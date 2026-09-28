# The manual

Everything needed to stand this up, point clients at it, sign reads, put a CDN in
front of it, and know what to run when it looks wrong. This page is the order of
operations; the deep material is linked, never duplicated.

| when you want… | read |
| --- | --- |
| the design and why it is shaped this way | `docs/spec.md`, `docs/adr/README.md` |
| the measurement behind a claim | `docs/runbook.md` |
| a trap somebody already paid for | `docs/pitfalls.md` |
| signed reads, in full | `docs/signing.md` |
| which instrument answers which question | `deploy/README.md` |
| the exposure and what closes it | `docs/security-hardening.md` |
| the same operations, written for an agent | `skills/clouddrive-origin/SKILL.md` |

---

## 0. What it is, and what it is not

A **pull-through origin cache**. It sits between a CDN and cloud drives (through
OpenList's WebDAV interface), answers ranged `GET`/`HEAD` from the bytes it holds
on local disk, and pulls the rest — streaming to the client while it writes. The
client namespace is flat (`/<upstream-id>/<key>`), so adding a drive or an
upstream never changes a URL.

It is **not a media server** (ADR-0026). It does not look at content types, parse
containers, build manifests or ship a player: a 200 GiB film, a 100 GiB tarball
and a VM image are the same object to it. What a viewer needs beyond bytes — a
manifest, MSE, a page — is the client's program. The exposure is **read-only**:
there is no upload path at all, and `PUT`/`POST`/`PATCH`/`DELETE`/`MKCOL` answer
`405`.

## 1. Who does what

| role | holds | does |
| --- | --- | --- |
| **operator** | the node's SSH, the env file (0600) | installs, configures, watches health, upgrades |
| **content provider** | the cloud drive, OpenList's web credentials | puts files in the drive; nothing about this program changes when they do |
| **client site / app** | the signing secret (once reads are signed) | mints one URL per viewing session, hands it to the player |
| **CDN** | — | pulls from the origin over TLS with a stamp header; caches what it can |

## 2. Five minutes, no cloud

Nothing below is needed to evaluate it; this is needed to change it.

```sh
cargo build --release --offline
bash deploy/lab/run-lab.sh --smoke      # ~1 minute, the core set
bash deploy/lab/run-lab.sh --quick      # ~10 minutes, everything but the 3 GB pull
```

The suite starts several loopback instances of the real binary against a local
WebDAV stand-in and prints one `PASS:`/`FAIL:` line per assertion; a healthy run
ends `PASS=45 FAIL=0` (`--smoke`) or `PASS=85 FAIL=0` (`--quick`) — the suite
prints its own totals, trust those over this page. It **refuses to start on a
busy box** (`load > 6`, or any live `chromium`) because its timing assertions
measure the machine as much as the code; a red line on a loaded box is not a
regression. Two sections need a browser: export `PW_DIR` (or `PW`) if
`run-lab.sh` cannot find `playwright-core` on its own.

Unit and integration tests: `cargo test --offline --workspace` (366 assertions
at the time of writing).

## 3. A node: prerequisites

- **Linux with systemd**, x86_64 or aarch64. Serving bytes is not CPU-bound
  work: the constraints are the magazine's disk and the network to the provider.
- **Disk** for the magazine: `max_size_bytes` plus reserve (the production node
  runs a 10 GiB magazine and keeps a few hundred MB of filesystem headroom).
- **A TLS certificate** for the public hostname the CDN will send in the `Host`
  header. The origin's certificate is validated by the CDN, so it must match.
- **Ports**: the front listens on 7777 (TLS, the CDN-facing plane) and 7778
  (a second, zero-disk plane, internal); the business plane listens on loopback
  (8080/8081) and metrics on 9090/9091. Nothing else needs to be reachable.
- **The upstream**: an OpenList instance on the same box, bound to **loopback**
  (`127.0.0.1:5244/dav`), with one mount per cloud drive. Create the mount in
  OpenList's web UI, then note its WebDAV **username and password** — those go
  into the origin's env file, never into a config file or the repository.

Keeping OpenList on loopback is not decoration: the origin is the only thing that
should be able to reach it, and the container's port must not be published.

## 4. Install and first start

On the node:

```sh
# first install (also lays down the units and writes the env template)
sudo bash deploy/oracle/install.sh <binary> [--keep-env] [--new-token]
bash deploy/oracle/accept.sh          # expect: VERDICT=PASS
```

`install.sh` calls `deploy/oracle/env-file.sh`, which writes
`/opt/origin-cache/origin-cache.env` (mode 0600) from a template. **Fill every
`REPLACE_ME`** before the first start; the origin refuses to boot when a config
names an environment variable that is not set.

| variable | what it is |
| --- | --- |
| `OPENLIST_USERNAME`, `OPENLIST_PASSWORD` | the WebDAV credentials of the OpenList mount |
| `ORIGIN_PREWARM_SECRET` | shared secret for the `/_internal/prewarm/` endpoint |
| `ORIGIN_TOKEN` | the value the CDN sets in `X-Origin-Token` on every pull (`install.sh` generates 64 hex chars) |
| `ORIGIN_TLS_CERT_PATH`, `ORIGIN_TLS_KEY_PATH` | certificate and key for the public hostname |
| `SIGV4_ACCESS_KEY_ID`, `SIGV4_SECRET_ACCESS_KEY` | optional; the single-tenant credential pair for signed reads |
| `SIGV4_CREDENTIALS_FILE` | optional; path to the multi-tenant 0600 JSON store (see §7) |

Four units run afterwards:

| unit | what it is |
| --- | --- |
| `origin-cache-efficient` | the cache: front on 7777, business on 8080, metrics on 9090 |
| `origin-cache-nocache` | the same binary with a zero-disk profile, internal (7778/8081/9091) |
| `origin-cache-watchdog.timer` + `.service` | health checks and the heartbeat; the service showing `inactive` between runs is normal |

Restarting: `sudo -n systemctl restart origin-cache-efficient origin-cache-nocache`
(a plain `systemctl` may hit interactive authentication). An idle restart takes
seconds; a busy one takes minutes, because staged bytes have to settle.

**Day-2 binary updates** — one command from source to a serving node:

```sh
bash deploy/oracle/deploy-node.sh --yes     # package -> cross-build -> gates -> backup -> install -> restart -> accept.sh -> roll back on failure
```

It needs an ssh alias or `COMPILE_HOST` for the cross-build machine (see the
runbook's "Building for the node"). It refuses to install a binary whose
architecture does not match, or one that cannot parse the node's own config with
a scratch cache directory.

Two things that look like details and are not:

- **`cache_dir` is never renamed.** It holds the live metadata database and every
  staged sidecar; renaming it starts you on an empty cache.
- **Never point a trial binary at the live `cache_dir`.** A valid-but-wrong
  config quarantines the store (pitfall 55).

## 5. Configuration reference

Config files are parsed **strictly**: an unknown key, or a key in the wrong
table, is a boot failure — keys never fail silently. `config.example.toml` is the
fully commented reference; `deploy/oracle/config-efficient.toml` is the shape the
production node runs. The knobs that matter to a client or an operator:

| knob | default | what it decides |
| --- | --- | --- |
| `front_listen` | — | the CDN-facing address; `[::]:7777` when the CDN pulls over IPv6 |
| `tls_cert_env`, `tls_key_env` | — | which env vars hold the certificate and key |
| `front_metrics_listen` | — | where Prometheus metrics are served (loopback) |
| `front_ip_block` / `front_ip_allow` | `[]` | connection-time refusal / rate-limit exemption |
| `front_rate_rps` | off | per-IP ceiling. Left **off** on the CDN-facing plane by measurement (the edge fans a burst across ten pull IPs, and it turns a `429` into a `206` with a short body) |
| `front_origin_token_header` / `_env` / `_exempt` | off | the admission stamp the CDN sets; exemption defaults to loopback so the node's own probes keep working |
| `listen_addr` | — | the business plane, loopback only |
| `cache_dir` | — | where the metadata database and staged sidecars live |
| `max_size_bytes` | — | the magazine's byte budget; objects larger than this get a bounded working window instead of a slot |
| `inactive_ttl_secs` / `revalidate_ttl_secs` / `negative_ttl_secs` | 1200 / 60 / 60 | how long a key survives idle, how often it is revalidated against the provider, how long a miss is remembered |
| `eviction_policy` | `lru` | which staged span goes first under pressure (`lru` or `heat`) |
| `session_window_bytes` | 64 MiB | how much one upstream `open` covers — the knob behind cost (§11) |
| `window_floor_bytes` | 8 MiB | how far a cold jump reads ahead before the window grows |
| `read_grace_secs` / `watch_idle_secs` / `watch_pin_bytes` | 300 / 900 / 128 MiB | how a viewed key is protected across pauses |
| `concurrency_per_upstream` / `retry_*` | 3 / 4 attempts | upstream parallelism and backoff |
| `prewarm_shared_secret_env` | — | the env var holding the prewarm secret |
| `front_content_auth*`, `sigv4_credentials_path` | off | signed reads (§7) |
| `[[upstreams]]` | — | one OpenList instance per entry: `id` (must equal the first URL segment), `type = "openlist"`, `base_url`, `root_path`, `username_env`, `password_env`, optional `cold_miss` and `cache_profile` |
| `[[routes]]` | — | prefix → upstream, longest prefix first; `""` is the catch-all |

Profiles: `efficient` is the built-in default (staged windows; the design in §9)
and `nocache` is the zero-disk water-pipe for tiny nodes. A
`[cache_profiles.<name>]` table tunes `min_file_size` and
`coverage_window_secs` and nothing else.

## 6. The client interface

### 6.1 Addressing

```
https://<public-host>/<upstream-id>/<key>
```

`<upstream-id>` is the `id` of an `[[upstreams]]` entry; `<key>` is the object's
path inside that mount, percent-encoded (spaces, `+`, CJK and `#` must be
encoded; `/` inside a key is fine). The path is flat: there is no notion of a
directory, and a key that collides with a reserved cache name (`redb.db`, a
`.hidden` name such as `.tmp.a.b.1234`) is refused with `400`.

### 6.2 Reads

| request | answer |
| --- | --- |
| `GET /<id>/<key>` | `200`, `Content-Length`, `Content-Type` (passed through, with a fallback table for the generic `application/octet-stream` many provider APIs return) |
| `HEAD /<id>/<key>` | the same headers, no body |
| `GET` + `Range: bytes=100-199` | `206` with `Content-Range: bytes 100-199/<total>` |
| `GET` + `Range: bytes=100-` (open-ended) | `206`; the origin promises a bounded span it can serve, e.g. `Content-Range: bytes 100-<100+2^31-2>/<total>` — it never has to read the object to the end |
| `GET` + `Range: bytes=-65536` (suffix) | `206` with the last 64 KiB (the size comes from one `stat`) |
| multi-range (`bytes=0-1,5-6`) | `416` — a deliberate AWS-compatible answer, not a silent whole-object reply |
| an unsatisfiable range, or `bytes=-0` | `416` with `Content-Range: bytes */<total>` |
| any write verb | `405` |

There is no CORS: no `Access-Control-*` header is emitted, so a browser page that
fetches objects must be served from the same origin as the objects. An object
that changed upstream is picked up by revalidation (`revalidate_ttl_secs`) — no
restart, no cache flush.

### 6.3 Listing

`GET /<id>/?list-type=2` returns an S3 `ListBucketResult`; supported parameters
are `prefix`, `delimiter`, `max-keys`, `continuation-token`, `start-after` and
`encoding-type`, so a client can walk a mount the way it walks a bucket.

### 6.4 Diagnostics

| surface | where | what |
| --- | --- | --- |
| `GET /_internal/healthz` | business plane (loopback), `:8080` and `:8081` | the node's state: entries, bytes, `segment_bytes`, `stray_bytes`, disk headroom, `store.state`, degradation reasons |
| `GET /_internal/healthz?key=<raw key>` | same | one key's view: installed metadata, spans on disk, ledger spans, pin, lease |
| `POST /_internal/prewarm/<key>` | same | pull a key into the cache ahead of a viewer; requires `x-prewarm-token: <ORIGIN_PREWARM_SECRET>`, body capped at 64 KiB (`413` beyond) |
| `GET /metrics` | metrics listener (loopback) | Prometheus text; §10.2 says which counter answers which question |

Everything under `/_internal/` is `404` on the public port by design — the guard
is the prefix, so nothing added there can leak by accident.

### 6.5 Errors

| code | meaning | who emits it |
| --- | --- | --- |
| `400` | the key cannot be named (reserved cache name, or too long after escaping) | front |
| `401` | the private surface needs a token that is not configured | business |
| `403` | no/incorrect CDN stamp (`X-Origin-Token`), or a signed read that failed verification (§7) | front |
| `404` | the object is absent upstream, or the path is a private surface | business / front |
| `405` | a write verb | business |
| `413` | a prewarm body larger than 64 KiB | front |
| `416` | an unsatisfiable or multi-part range | business |
| `429` | the per-IP ceiling (only if `front_rate_rps` is set; off on the CDN plane) | front |
| `502` | the business plane is unreachable (pooled connection reset); carries `Content-Length: 0` and `Cache-Control: private, no-store` | front |
| `503 SlowDown` | a signed session is over its request/byte budget (§7) | front |

### 6.6 What a client should do

- **Shard, do not scrape.** The origin and the CDN both behave best with bounded
  ranges (a viewer's shard walk of 1–5 MiB is the measured shape). An open-ended
  `bytes=N-` pull is legal and answered, but it pins a body for as long as the
  client keeps reading.
- **Seeking is cheap when it stays inside a window.** A jump to a cold offset
  pays one upstream `open` (measured 0.6–0.8 s) and then reads ahead; reads that
  land in the window that is already open pay nothing.
- **A paused viewer is not forgotten.** A key keeps being watched for
  `watch_idle_secs` after its last body, which is what makes "watch ten minutes,
  seek, watch again" cost one open rather than one per request.
- **Two cautions that are properties of content, not of this service:** an
  unindexed fragmented MP4 will not play in a bare `<video>` (it needs MSE plus a
  manifest), and any object handed to a media element that cannot demux it will
  look like an origin problem when it is not. Measure before blaming the path
  (pitfalls 27/30, and the runbook's "The real player on the real film").

## 7. Signed reads (anti-hotlink)

A public origin will be pointed at by scrapers. Ranged reads are cheap for a
viewer and expensive for the provider, so the origin can require every content
read to present a **standard S3 presigned URL** and charge a per-session budget
(ADR-0027). It is **off until you flip it**:

```toml
front_content_auth = true
sigv4_credentials_path = "/var/lib/origin-cache/content-auth.json"   # or the SIGV4_* pair
```

- The **client site's backend** mints one URL per viewing session with
  `deploy/oracle/presign.py` or any S3 SDK (`S3SigV4QueryAuth`, not the generic
  signer), and hands the player a plain URL. **GET and HEAD are separate
  tickets.**
- The `session` parameter is opaque, signed (only the secret holder can set it)
  and is the budget key: over budget answers `503 SlowDown`, which means "back
  off", never "gone".
- The credential store is a 0600 JSON file of tenants
  (`[{"id", "secret", "prefix", "session_rps", "session_mib_per_min"}]`); a
  group-readable, unparseable or empty file is a **boot failure**, and so is
  `front_content_auth = true` with no credentials at all.
- **Rollout order matters.** First make the CDN's cache key **ignore the whole
  query string** and verify two different sessions hit the same cache entry —
  without that, every session is a distinct cache key and the flood returns
  authenticated. Then deploy with auth off, confirm
  `origin_content_auth_total` appears, then flip the line and restart in a quiet
  window, coordinating with the site.

Minting, the budget rules, rotation, the symptom table and the rollout checklist
are all in **`docs/signing.md`** and the runbook's "Content reads are presigned".

## 8. Putting a CDN in front

Any origin-pull CDN works if it forwards ranges. For EdgeOne:

| setting | value, and why |
| --- | --- |
| origin | the node's address, port **7777**, `OriginProtocol=FOLLOW` |
| Host header | the public hostname (the origin's certificate is checked against it) |
| `RangeOriginPull` | **on** — the whole design assumes the edge forwards ranges |
| `UpstreamHTTP2` | **on** — one H2 connection carrying many range streams |
| a `ModifyRequestHeader` action | **sets** `X-Origin-Token` to the value in the env file |
| cache key | **ignore the query string** (required before signed reads are enabled) |

The stamp header is the admission gate (ADR-0025): the edge *sets* it, so a
request that reaches the port without it did not come from the CDN and gets
`403`. The viewer-facing URL is then `https://<public-host>/<upstream-id>/<key>`.

Three measured facts to keep in mind when reading CDN dashboards:

- **The edge's `eo-cache-status: HIT` does not mean the bytes are at the edge.**
  For ranged requests the same offsets were re-pulled from the origin twice while
  both passes were labelled `HIT`. Bytes, rate and cost come from the **origin's**
  counters or its access log, never from the client-side label.
- **The edge may hide origin errors.** A `429` or a `503` at the origin can reach
  a viewer as `206` with a short or empty body.
- **The edge appends to `X-Forwarded-For`.** For forensics read the **last**
  element: the earlier ones are whatever the client chose to claim, so any
  per-client control keyed on `xff` can be bypassed by editing a header.

Per-IP rate limiting on the CDN-facing port stays off, for the reasons in the
runbook's "Would rate limiting be safe to enable?".

## 9. The caching model in one page

- A ranged read that cannot be answered from disk starts a **run**: one upstream
  stream covering a **window** (`session_window_bytes`, 64 MiB by default).
  Every request that lands inside that window **rides it** — the metric calls
  those readers `attached`.
- What the run pulls is written to disk as staged spans (`.seg` sidecars), and
  later reads inside a staged span are answered from disk with **no upstream
  call at all**. There is no "promotion" step: the staged span *is* the cache.
- A cold jump reads the **floor** first (`window_floor_bytes`, 8 MiB) and the
  window grows as it is read out, so a jump costs little and a continuous walk
  converges on one `open` per window.
- A key that is being read is protected while its body lives; a key that is
  being **watched** keeps a neighbourhood around the viewer's position
  (`watch_pin_bytes`, 128 MiB) across pauses. Both are **preferences**: under
  disk pressure they can be spent, back end first.
- An object larger than the magazine is not refused. It gets a bounded working
  window, so a 200 GiB film on a 10 GiB disk is served with one upstream `open`
  per window instead of one per request — the ledger acts as a sliding window.

The consequence to remember: **upstream calls follow windows, not requests.**

## 10. Day two

### 10.1 Health

```sh
curl -s http://127.0.0.1:8080/_internal/healthz     # or :8081 for the nocache plane
```

`status` is `ok` or `degraded`, and `degraded_reasons` says why (a rebuilt
metadata store, disk below reserve) rather than lying. `store.state` reports the
metadata store's own state, `stray_bytes` the objects larger than the magazine
currently on disk, `disk_free_bytes` the headroom the reaper watches. The same
JSON carries one entry per upstream with the profile and cold-miss policy it is
running.

The watchdog posts a heartbeat every 5 minutes; the far side declares the node
offline after 15 silent ones. Its log is `/opt/origin-cache/watchdog.log`, and
`docs/status-reporting.md` describes the payload.

### 10.2 Metrics

```sh
curl -s http://127.0.0.1:9090/metrics | head       # :9091 for the nocache plane
```

| counter | the question it answers |
| --- | --- |
| `backend_call_duration_seconds_count{op="open"\|"stat"}` | what the **provider** sees: one `open` per window, one `stat` per cold ranged read |
| `cache_body_bytes_total{source="disk"\|"stage"\|"upstream"}` | how much was delivered, and whether we served it ourselves |
| `cache_body_ttfb_seconds{source}` | time to the first body byte, split the same way — the number a viewer feels on a seek |
| `cache_serve_source_total{source}` | responses by byte source |
| `cache_session_total{outcome="sealed"\|"chained"\|"open_failed"}` | runs, and how they ended |
| `cache_session_reader_total{result="attached"\|"standalone"}` | how many requests rode an open window, and how many escaped |
| `cache_watch_active`, `cache_watch_pinned_bytes` | viewers being remembered, and what their pins cost the budget |
| `cache_unkeepable_keys`, `cache_unkeepable_trim_bytes_total` | objects too big for the magazine and the bytes they churn |
| `front_requests_total{method,proto,status}` | the front's own answer codes |
| `front_upstream_*` | the front↔business hop |

Two commands do the accounting:

```sh
bash deploy/oracle/fill-account.sh 300 200G          # per-key asks, bytes, latency, clients
bash deploy/oracle/merge-account.sh mark run         # before a workload…
bash deploy/oracle/merge-account.sh report run       # …the bytes-per-open / calls-per-GiB table
bash deploy/metrics-report.sh http://127.0.0.1:9090/metrics 30   # percentiles
```

`deploy/README.md` indexes every instrument and the question it is authoritative
for.

### 10.3 Logs

```sh
journalctl -u origin-cache-efficient --since "1 hour ago" | sed 's/\x1b\[[0-9;]*m//g'
```

`front access` lines are one per request and carry `peer=` (who opened the
connection: the CDN's pull node) and `xff=` (the client the edge was serving),
plus `bytes=`, `duration_ms=` and the admission outcome (`auth=`). **The log is
ANSI-coloured** — strip it before grepping fields, or every field grep comes back
empty. Retention is capped (`deploy/oracle/journald-origin-cache.conf`, plus the
node's own journald drop-in — read them before assuming history is there): if the
log matters, ship it somewhere.

### 10.4 Upgrade, rollback, restart

```sh
bash deploy/oracle/deploy-node.sh --yes     # keeps a rollback copy at /home/opc/origin-cache.prev
```

The script backs up the running binary, installs the new one, restarts both
planes and runs `accept.sh`; a failure rolls back. A binary-only restart is
seconds when idle and minutes when busy. Configuration and unit changes go
through `install.sh` instead (and it will refuse a config that names an unset
environment variable). Certificate renewal is in the runbook's "Certificate
expiry / renewal".

## 11. Capacity and cost arithmetic

The three measured constants (§ runbook for each): one upstream `open` costs
**0.6–0.8 s** and covers one window; bytes already staged are served in
**~86 ms per MiB**; the provider hands over data at a rate that makes a 64 MiB
window about a second of transfer.

- **Upstream calls per GiB delivered ≈ 1 GiB / `session_window_bytes`** in the
  steady case (16 opens per GiB at the 64 MiB default, 128 per GiB if every read
  only ever pays the 8 MiB floor). Each cold ranged read also pays one `stat`
  (measured at ~6 ms on the real film).
- **Viewer load**: a film at 2.9 Mbps needs ~0.36 MB/s per viewer; ten
  concurrent viewers ≈ 3.6 MB/s, which is a fraction of what the node pulls from
  the provider and a small multiple of what one H2 connection to a CDN edge
  carries. The CDN's cache absorbs most of it when it can hold whole objects.
- **Random seeking is the worst case for merging**: with viewers jumping to
  unrelated offsets there is nothing to share, and the measured figure under ten
  viewers was **one `open` per ~17.6 MB delivered**. Continuous playback in one
  direction is the best case (one `open` per window).
- **Disk**: size `max_size_bytes` to the objects you want answered from disk. An
  object bigger than the magazine still streams; it just never becomes fast.

## 12. Troubleshooting

| symptom | first command | where the numbers are |
| --- | --- | --- |
| a client gets `403` from outside | `curl -k https://<node-ip>:7777/<id>/<key>` | that is the admission gate working (no CDN stamp) — §8 |
| a client gets `403` behind the CDN | `presign.py` mint, then `curl` the signed URL | signed reads: host mismatch, expired ticket, or a session edited after signing — `docs/signing.md` |
| a range request returns little or nothing | `deploy/oracle/big-object-probe.sh`, then look at the object itself | pitfall 27/30: it is usually the object (unindexed fMP4) or the client, not the path |
| the first byte is slow | `deploy/oracle/seek-attribution-probe.sh` | one `open` dominates and it is already at its floor — runbook "Is seeking smooth?" |
| viewers stall | `deploy/oracle/fill-account.sh`, then the client's own report | runbook "Ten viewers on the real film" — do the arithmetic before blaming the origin |
| bytes at the edge look wrong | `deploy/oracle/fill-account.sh` | the origin's counters, never `eo-cache-status` — §8 |
| healthz says `degraded` | the `degraded_reasons` field | metadata store rebuilt, or disk below reserve — runbook "Health checks" |
| the store will not open (boot loop) | `journalctl -u origin-cache-efficient -n 50` | runbook "Metadata store will not open"; never re-point a trial binary at the live `cache_dir` |
| a `502` from the front | `curl -s http://127.0.0.1:8080/_internal/healthz` | the business plane is down or restarting — runbook "Failure handling" |
| the node looks offline in the dashboard | `sudo tail /opt/origin-cache/watchdog.log` | heartbeat chain — runbook "The blog card says the node is offline" |

## 13. Limits and non-goals

- **Read-only.** No uploads, no delete, no rename. Content is managed in the
  cloud drive; this program only serves it.
- **No CORS.** A browser must be same-origin with the objects.
- **Not a media server.** No manifests, no transcoding, no container parsing, no
  player (ADR-0026).
- **One node is one node.** There is no replication and no cluster: the magazine
  is local, and losing the node means serving from the provider directly until it
  is back (ADR-0010 accepts this for the shape this was built for).
- **Cold reads are bound by the provider**, not by the box: an `open` is
  0.6–0.8 s and the provider's read rate is what it is. The origin's job is to
  make sure the *next* read does not pay it again.

## 14. Where the knowledge lives

`docs/spec.md` (the contract) · `docs/adr/README.md` (one line per decision) ·
`docs/pitfalls.md` (traps, with evidence) · `docs/runbook.md` (measurements and
procedures) · `docs/signing.md` (signed reads) · `docs/security-hardening.md`
(the exposure and its closures) · `docs/status-reporting.md` (the heartbeat
payload) · `docs/systemd-origin-cache.service.md` (the units) ·
`deploy/README.md` (every instrument, and what it is authoritative for) ·
`skills/clouddrive-origin/SKILL.md` (the same operations, written for an agent).
