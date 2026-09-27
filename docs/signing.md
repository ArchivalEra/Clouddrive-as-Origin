# Reading content with a signature

How a client gets bytes out of this origin when content auth is on
(`front_content_auth = true`, ADR-0027). If it is off — the default — a plain
URL works and nothing here applies; the switch and the rollout order are in
the runbook ("Content reads are presigned").

**In one breath:** a content read is a plain `GET`/`HEAD` of a plain URL that
carries six `X-Amz-*` parameters and a signature. That is standard S3
**query-string authentication** — the same thing `aws s3 presign` produces —
so any S3 SDK can mint it and no custom client code is needed. What the origin
adds is policy: who may sign (a credential), how long the URL lives, and a
per-session budget that bounds a flood.

## Who does what

| role | holds | does |
|---|---|---|
| **site backend** | an access key pair | mints one URL per viewing session (`presign.py` or any SDK) and hands it to the player |
| **player / browser** | nothing | requests the URL as-is; ranged reads, open-ended ranges, pipelining all unchanged |
| **CDN** | nothing | forwards URL and Host untouched; caches by URL-with-query-ignored (one console setting, see step 0 below) |
| **origin** | the credential store | verifies the signature, enforces expiry and budgets, serves or refuses |

A player never needs a secret, and a backend never needs to call the origin to
mint a URL — signing is local crypto. That is the point of staying standard.

## Minting one URL

```sh
python3 deploy/oracle/presign.py \
    --host cdn.example.com \              # the host the BROWSER will request
    --key googledrive1/film.mkv \         # object key, same path you already use
    --session viewer-17 \                 # opaque per-viewer id (see Budgets)
    --expires 3600 \                      # seconds; capped by the deployment
    --id AKIDEXAMPLE --secret SECRET      # or SIGV4_ACCESS_KEY_ID / _SECRET in env
# -> https://cdn.example.com/googledrive1/film.mkv?X-Amz-Algorithm=...&X-Amz-Signature=...
```

Add `--method HEAD` for a HEAD ticket (see "Method matters" below). `--session`
is required: the tool refuses to mint a URL without it, because the origin
refuses to serve one (`--no-session` exists to reproduce that refusal on
purpose, and is never a working ticket).

If the backend already speaks S3, an SDK can sign it too — but use the **S3**
query signer, not the generic one, and note that the high-level helpers
(`aws s3 presign`, `generate_presigned_url`) mint a URL with no extra query
parameters, while this origin requires the `session` marker:

```python
import boto3
from botocore.auth import S3SigV4QueryAuth          # the S3 variant: UNSIGNED-PAYLOAD
from botocore.awsrequest import AWSRequest

creds = boto3.Session(aws_access_key_id=ID, aws_secret_access_key=SECRET).get_credentials()
req = AWSRequest(method="GET",                       # or "HEAD", a separate ticket
                 url="https://cdn.example.com/googledrive1/film.mkv",
                 params={"session": "viewer-17"})    # signed like any other param
S3SigV4QueryAuth(creds, "s3", "us-east-1", expires=3600).add_auth(req)
url = req.prepare().url
```

The generic `SigV4QueryAuth` looks like it works and does not: it hashes the
empty body (`sha256("")`) as the payload, while S3 presigned URLs carry
`UNSIGNED-PAYLOAD`, which is what the origin verifies — the request comes back
`403 signature does not match` with nothing else wrong. `presign.py` exists
mostly so this does not have to be re-derived per language, and so the
parameter set lives in one place.

Three constraints, all of them things that silently produce `403`:

1. **The host must be the one the browser requests.** A presigned URL signs the
   `Host` header; the edge forwards the client's Host to the origin (spec §6).
   Sign `cdn.example.com` if that is what the viewer types, not the origin's
   address and not an internal pull hostname.
2. **The session marker is part of the signature.** It is an extra query
   parameter, and every query parameter is covered by the signature — so it
   cannot be added, changed or removed after minting without invalidating the
   URL. Mint a fresh URL instead of patching one.
3. **Expiry is capped by the deployment** (`front_content_auth_max_expiry_secs`,
   default 6 h, protocol maximum 7 days). Ask for longer and the origin refuses
   the URL rather than serving it.

## What the URL looks like

```
https://cdn.example.com/googledrive1/film.mkv
  ?X-Amz-Algorithm=AWS4-HMAC-SHA256
  &X-Amz-Credential=AKIDEXAMPLE%2F20260927%2Fus-east-1%2Fs3%2Faws4_request
  &X-Amz-Date=20260927T000000Z
  &X-Amz-Expires=3600
  &X-Amz-SignedHeaders=host
  &session=viewer-17                     <- the origin's budget key (signed like the rest)
  &X-Amz-Signature=48ee4d7a...           <- excluded from its own signature
```

The path is unchanged — the ticket rides in the query, so a URL that already
worked keeps its shape. Treat the whole URL as a credential for its lifetime:
it works for anyone who has it until it expires (same as any presigned S3
URL). Do not put it in a page that logs referrers if the content is sensitive;
prefer short `--expires`.

## Method matters (GET and HEAD are separate tickets)

SigV4 signs the HTTP method. A GET ticket refuses a HEAD request
(`403 signature does not match`), and vice versa. Players that probe with HEAD
before reading — several do — must be given a HEAD ticket for those probes
(`presign.py --method HEAD ...`), or the backend can hand the same signed
constructor both. `Range` is *not* part of the signature: a GET ticket
authorizes any range shape (closed, open-ended, multi-request sequences).

## Expiry and budgets

- **Expiry** is the hard gate: past `X-Amz-Date + X-Amz-Expires` the URL is
  dead (`403 request has expired`). Mint per viewing session rather than
  sharing one long-lived URL; a session that outlives its URL asks its backend
  for the next one.
- **`session`** is the budget key: an opaque string the backend chooses
  (per viewer, per device, per playback — its call), at most 128 printable
  characters. Two budgets are charged against it:
  - **requests/second** (default 30) — a player issuing a handful of
    concurrent ranged reads never notices; a flood is stopped after its first
    few requests in a second;
  - **bytes/minute** (default 1 GiB) — measured from the bytes the origin
    actually sent, so it cannot be gamed by asking for ranges you do not read.
- Over budget the origin answers **`503 SlowDown`** — the S3-standard back-off
  signal — and the session is served again once the window slides (a second
  for requests, a minute for bytes). Treat `503` as "retry with back-off",
  never as a permanent failure.
- **A caveat worth knowing:** with a CDN in front, the *viewer* may not see the
  503 — the edge can re-ask and hand its viewer a short `206` instead. The
  budget is a backstop for the origin and the provider, not a quality-of-service
  signal; keeping your own request shape sane is still your job.

## Tenants: credentials, confinement, rotation

The origin holds a table of credentials — one per site/app — in a `0600` JSON
file (`sigv4_credentials_path`):

```json
[
  {"id": "site-a", "secret": "...", "prefix": "googledrive1/",
   "session_rps": 30, "session_mib_per_min": 1024},
  {"id": "app-b",  "secret": "...", "prefix": "app-b-assets/"}
]
```

- `prefix` confines a tenant: with `"prefix": "googledrive1/"`, a key outside
  that prefix is refused (`403 outside key prefix`) before routing, so one
  site's key cannot read another's objects. Omit it to allow everything.
- `session_rps` / `session_mib_per_min` override the deployment defaults for
  that tenant.
- Add a tenant: append an entry, restart the unit. Rotate a secret: edit,
  restart. Revoke: delete the entry, restart. (The store loads once at boot;
  a restart is the rotation primitive today.)
- The file must not be readable by group or other, must parse, and must not be
  empty — otherwise the process refuses to start (fail-closed, like every
  other admission control here).

## When it says no

| response | reason (metric label / log field) | what it means | fix |
|---|---|---|---|
| 403 | `missing signature` | no `X-Amz-Signature` in the query | mint a URL; the plain object URL is not authorized |
| 403 | `signature does not match` | wrong secret, edited URL, wrong method, or a proxy rewrote something | re-mint; check the host and the method |
| 403 | `unknown access key` | the credential id is not in the store | the tenant is not provisioned, or the id is from another deployment |
| 403 | `request has expired` | past `Date + Expires` | mint a fresh URL |
| 403 | `expires too long` | asked for more than the deployment cap | ask for less |
| 403 | `missing session` / `malformed session` | no `session` parameter, or it is too long / not printable ASCII | always pass a session id |
| 403 | `outside key prefix` | this tenant's credential is confined to another prefix | sign the right key, or give the tenant a wider prefix |
| 403 | `host header is not signed` | a hand-rolled signer left `host` out of `SignedHeaders` | use `presign.py` or an SDK |
| 503 | `session rate exceeded` | more requests/second than the budget | back off and retry; batch reads if the client can |
| 503 | `session bytes exceeded` | more bytes/minute than the budget | same; check for a runaway reader |

Operators can watch the same decisions as a bounded metric:
`origin_content_auth_total{outcome="allow|deny", reason="..."}`.

Every reason above is a value of the contract's closed taxonomy
(`src/signing.rs::Reason`), and this table is checked against it by
`tests/signing_contract.rs` in both directions: a refusal that can reach a
caller but is missing here fails the suite, a row naming something the code
cannot produce fails it too, and the status column must equal what the code
answers. Adding a row here without adding a variant (or the reverse) is
therefore a test failure rather than a documentation bug waiting to be found.
The same suite runs the signer below against the origin's verifier, so the tool
cannot drift from this document either.

## The one CDN setting this depends on

Before content auth is switched on, the edge's cache key must **ignore the
query string** (spec §6, requirement 2). Without it, every session's URL is a
distinct cache key: the edge never hits, the origin sees every request, and
the flood comes back *authenticated*. Verify the setting by requesting two
different sessions' URLs for one object and checking that both report
`eo-cache-status: HIT` on the same entry.

Host rewriting and query stripping must stay off on signed routes — both break
the signature, because the URL the origin verifies must be the URL the client
signed.

## Where this is implemented

`deploy/oracle/presign.py` (the signer this document describes),
`src/content_auth.rs` (the gate: verification, expiry cap, prefix, budgets),
`front/src/lib.rs` (`ContentAuth` seam in `request_filter`),
`src/sigv4.rs` (the SigV4 verifier the gate calls), ADR-0027 (why), and the
runbook (rolling it out).
