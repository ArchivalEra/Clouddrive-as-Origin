# Content reads are presigned, not public

Settles how the origin decides who may READ content (ADR-0026's byte surface),
and closes the door the flood test pointed at: a hostile or buggy client
firing thousands of ranged reads per second at fresh offsets. Companions:
ADR-0025 (admission to the PORT is the edge's token — unchanged, a different
layer), `docs/security-hardening.md` R11 (superseded for the content surface),
`docs/signing.md` (how a backend mints a URL and what every refusal means),
`deploy/oracle/presign.py` (the signer site backends use), and the runbook's
"Content reads are presigned" section (rollout).

## Context

Three measured facts forced this:

- **The old answer made the content public on purpose** (R11, 2026-09-23):
  unsigned business route, unsigned S3 listing, anonymous-first. That was fine
  while the only callers were our own probes; a real player population made
  the exposure the product's main risk.
- **The merge machinery shares one upstream open per WINDOW** (ADR-0016/0024),
  so it survives ten viewers seeking around a film — but a client firing
  thousands of ranged reads per second at fresh offsets opens a window per
  read, and the provider sees every open. Rate limiting per IP cannot help
  (R8: the edge fans a burst over ~10 pull nodes and hides 429s from viewers),
  because per-IP cannot see the CLIENT. What can see the client is a request
  that names itself.
- **The edge is a shared cache keyed by URL**: once warm, it serves without
  the origin. Protection placed anywhere but the origin either cannot see the
  client (the edge sees a CDN-population of requests) or must be re-implemented
  per site on the CDN. The origin is where signing can be central, because
  this CDN will serve other sites and apps.

## Decision

**Content reads (GET/HEAD on the public route) require a SigV4 presigned URL
the credential store knows; each URL carries a session marker and lives under
per-session budgets.**

- **The ticket is standard S3 query auth** — `X-Amz-*` parameters, verified by
  the same ported s3s machinery that verifies the S3 API (`src/sigv4.rs`).
  No custom crypto, no custom endpoints: a site backend holds an access key
  pair, presigns locally (any SDK, or `deploy/oracle/presign.py`), and hands
  the player a plain URL. Extra query parameters are signed by construction,
  which is how the session travels: `session=<opaque>` costs the backend
  nothing and gives the origin a budget key.
- **Enforcement lives at the front, in `request_filter`, after the edge-token
  gate** — the same door, so one place answers "who may talk to us" and one
  access log carries the identity (`auth=` field). Unsigned reads are refused
  with 403 before the business plane, the cache or the ledger is touched. The
  seam is a one-method trait (`front::ContentAuth`) implemented in the core
  crate — the workspace's dependency direction (core → front) keeps the front
  ignorant of SigV4.
- **Budgets, not just identity.** A valid signature proves who; the budgets
  cap how fast: requests/second (sliding 1 s window) and bytes/minute
  (observed from the bytes each response actually sent — a range request's
  size is unknowable at the door). Over budget → `503 SlowDown`, the
  S3-standard back-off signal. The session table is capped and swept, so a
  flood of fresh session ids is itself bounded.
- **Multi-tenant by credential.** Each site/app gets its own id/secret pair
  (a 0600 JSON file), optionally confined to a key prefix — one tenant cannot
  read another's objects, and a leaked key is revocable per tenant. A
  single-tenant deployment may keep the two `SIGV4_*` env vars instead.
- **Fail-closed at boot**: `front_content_auth = true` with an empty or
  unreadable credential store refuses to start, like every other admission
  control here. The file being group-readable is a boot failure, not a
  warning.
- **Loopback stays exempt** (the node's own probes sign nothing), mirroring
  the origin-token exemption. The LAB arm runs the same gate with an empty
  list so the refusal is reachable on the test box.

## What it costs

- **The edge's cache key must ignore the query string** (operator console
  change, spec §6.2) — otherwise every session's URL is a distinct cache key,
  the hit rate collapses, and the origin sees every request: the flood would
  come back *authenticated*. This is a hard prerequisite of the rollout.
- **A lifted URL works against whatever the edge holds warm.** Residual
  exposure, accepted: unsigned callers cannot REFILL the edge (cold reads
  403 at the origin), the edge's cache is finite and TTL'd, and budgets bound
  how fast any session can refill it. The presigned system protects the
  origin and the provider — it cannot make a public CDN into a private file.
- **Budget trips are invisible to the viewer** (measured in R8 for 429s; the
  same masking applies to 503s: the edge may re-ask and serve its viewer a
  short 206). The budgets are a backstop for the origin and the provider, not
  a QoS mechanism; the site's own traffic shape remains its responsibility.
- **A presigned URL leaks like any URL** (logs, Referer): short expiry caps
  the damage, and the session budget bounds what a leaked URL can pull.
- Rotation is a restart (the store loads once at boot); a real reload path
  waits for a second tenant to actually need it.

## Evidence

Unit tests pin the signature chain against s3s vectors, and pin the python
presigner's output against the Rust verifier (a cross-language vector that
goes red if either side drifts). The LAB (config-i, empty exemptions, 3 req/s
ceiling, 300 s cap) walks the gate live: unsigned 403; presigned 206
byte-exact; wrong secret, missing session, expired date and over-long expiry
all 403; twelve rapid reads trip the budget (503 SlowDown) and the session is
served again once the window slides; a prefix-confined credential reads
inside `media/` and is refused outside it, before routing.
