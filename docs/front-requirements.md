# Front plane (`origin-front`) — requirements checklist

The front is the TLS/H2 edge of the single binary: it terminates client
(EdgeOne origin-pull) connections and forwards every request to the
business plane on loopback. **It owns no cache semantics, no auth, no
routing decisions — pure byte movement.** Anything smarter than that
belongs in `origin_cache`.

Status markers: `[x]` done & verified, `[~]` works but on defaults /
unverified, `[ ]` missing.

## 1. Core duties

- [x] TLS termination from cert/key path pair (env pointers, DNS-01
      renewed material; `TlsSettings::intermediate`)
- [x] ALPN advertises `h2` + `http/1.1` (`add_tls_with_settings` +
      `enable_h2`) — EdgeOne origin-pull negotiates HTTP/2
- [x] Plaintext mode when TLS env is absent (boot warning; edge is
      expected to carry public HTTPS)
- [x] Forward all methods, headers, and bodies verbatim to the business
      plane (Pingora default proxy path)
- [x] Streaming pass-through — no full-response buffering on the
      multi-GB cold-pull path (object routes are GET/HEAD only; the only
      request body in the surface is the tiny prewarm POST)
- [x] Graceful shutdown on SIGTERM/SIGINT via Pingora's own signal
      handling (drains in-flight connections; can hold the process — and
      therefore the business plane's redb lock — for up to 300 s; see
      known-behavior notes below)
- [x] Runs on a dedicated thread (Pingora manages its own runtime;
      `run_forever` panics inside a tokio runtime)

## 2. Hardening gaps (next work items, in priority order)

- [ ] **Timeouts**: no connect / read / write / idle timeouts are
      configured — a stalled client or a wedged business plane holds a
      connection indefinitely (slowloris surface). Set explicit
      upstream + downstream timeouts.
- [ ] **Connection limits**: no per-listener max-connections or
      per-client-IP cap. EdgeOne origin-pull volume is bounded, but the
      listener is also reachable directly.
- [ ] **Prewarm body bound**: `POST /_internal/prewarm/*key` reaches the
      front unbounded; the business plane checks the token, but the
      front should still cap request-body size and header count.
- [ ] **Client IP preservation**: no `X-Forwarded-For` insertion — the
      business plane (and its logs) only ever sees the front/loopback
      peer. Needed for any future per-IP throttling or log forensics.
- [ ] **TLS floor**: rustls defaults (TLS 1.2+ intermediate suite) are
      fine today; pin an explicit minimum version once Pingora exposes
      the knob cleanly.

## 3. Observability gaps

- [ ] **Access log**: requests are invisible at the front (only errors
      warn). Add one-line access logging (method, path, status, bytes,
      duration, h1/h2) behind the existing `tracing` stack.
- [ ] **Connection/protocol metrics**: h2 vs h1 counts, active
      connections, upstream reuse rate — needed to validate that the H2
      optimization is actually engaged in production traffic.

## 4. Non-goals (explicitly rejected)

- No caching at the front (see TinyUFO note below for why this survives
  every optimization proposal)
- No auth, no S3 semantics, no key routing — the front must stay
  replaceable (it already replaced a hand-rolled rustls proxy once)
- No direct-to-upstream logic; the front knows exactly one peer: the
  business plane

## 5. TinyUFO integration consideration

TinyUFO (Cloudflare's `tinyufo` crate, used inside `pingora-cache`) is a
concurrent in-memory object cache with S3-FIFO eviction: O(1) ops,
scan-resistant, higher hit ratio than LRU at a fixed memory budget.

**Where it must NOT go: the front.** A front-level cache would sit in
front of the SigV4 auth gate (business plane), so a cached hit would
serve unauthenticated bytes — a security hole, not an optimization. It
would also re-create cache-coherence problems (ETag versioning,
revalidation, the coverage ledger) that already live, correctly, one
layer down. The front stays pure byte movement.

**Where it can go: inside `origin_cache`, as a RAM tier in front of the
disk cache.** Two candidate placements, smallest first:

1. **List-results cache** — PROPFIND-to-XML responses are the most
   CPU-expensive per byte and already carry `Cache-Control: private,
   max-age=5`. A TinyUFO keyed by (bucket, canonical query) with a 5 s
   TTL absorbs directory-browsing storms. Least risk: metadata only,
   trivially invalidated, no byte-stream complexity.
2. **Hot small-object tier** — objects below a size cap (e.g. ≤ 1 MiB)
   cached in RAM keyed by (upstream, key, etag). Coherence rules must
   reuse the existing machinery: etag must match the disk/meta state,
   revalidation outcomes invalidate entries, single-flight stays the
   only cold-pull path. S3-FIFO's scan resistance fits CDN traffic
   (one-shot big files won't evict the hot small working set — exactly
   the failure mode LRU has here).

**Honest cost/benefit first.** The loopback disk-hit path already serves
~580 MB/s with ~15 ms TTFB on the oracle node; cold pulls are
network-bound on the cloud drive. A RAM tier only pays off when disk IO
or per-request metadata cost is the bottleneck under real concurrency —
measure that before writing any code, and start with placement (1),
which is a day of work and an easy revert.

**Not a replacement for anything existing**: redb stays the durable
metadata store, the disk stays the byte cache, TinyUFO would only be the
volatile fast tier between them.

## Known front behaviors worth remembering

- SIGTERM → Pingora graceful drain (up to 300 s default grace): the
  process stays alive and keeps the business plane's redb lock — don't
  start a second instance against the same cache_dir during drain.
- The lab suite's pre-flight (`pkill` + port-wait + liveness asserts)
  exists exactly because of the above.
