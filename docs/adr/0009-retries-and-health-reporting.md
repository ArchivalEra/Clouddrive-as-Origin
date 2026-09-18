# Upstream retries and health reporting

Decided while resolving tickets #58 and #59 of the production-readiness map
(#53). Two related decisions, recorded together because both concern how the
service behaves when a dependency is unhealthy.

## Retries: implement what the documents already promised

Three documents described retry behaviour that did not exist: `spec.md`
§Resilience ("`429` → honor `Retry-After` with bounded jitter"), ADR-0002
("`Retry-After` honored exactly, else jittered exponential capped ~30 s"), and
ADR-0003 (requiring a test double that can inject `429` with `Retry-After`).
Meanwhile the three `retry_*` config fields were never read and `Retry-After`
was never parsed, so every upstream 429 reached the client as a bare 503 with
no `retry-after` header.

**Decisions:**

1. `Retry-After` (delta-seconds) is parsed at the OpenList mapping and carried
   in `BackendError::RateLimited`, whose field already existed and already
   renders into the outbound 503.
2. `TimedBackend` — which already wrapped all five backend methods and is
   constructed once — applies the retry policy: retry `RateLimited` and
   `ServerError`; exponential from `retry_base_ms`, capped at `retry_max_ms`,
   additive jitter derived from the clock (no `rand` dependency).
3. Deterministic failures are **not** retried: `NotFound`, `AuthRequired`,
   `RangeNotSatisfiable` cannot change on retry and retrying them burns quota.
4. A `Retry-After` longer than `retry_max_ms` is **not truncated**. Truncating
   would call the upstream back sooner than it asked, which is how a client
   earns a longer ban. Instead the retry is declined, so neither the upstream
   nor a gate permit is abused.

**Accepted consequence:** retries run while the caller holds an upstream gate
permit, so `retry_max_ms` bounds how long a throttled upstream can occupy one.

## Healthz: report a verdict, not a constant

`healthz` returned 200 and `{"status":"ok"}` unconditionally — it checked
nothing. A node with a quarantined metadata store, a rebuilt cache or a full
disk reported perfect health, which made the endpoint useless as a monitor and
left the watchdog unable to see anything but a dead socket.

**Decisions:**

1. The **status code stays a liveness signal** (200 = the process serves).
   Proxies and systemd consume that, and overloading it would make liveness
   checks fail during a partial degradation.
2. The **verdict lives in the body**: `degraded` plus `degraded_reasons`, so a
   caller can distinguish "answering" from "healthy".
3. Reasons are drawn from state the service already had but never exposed:
   `metadata_store_quarantined` and `metadata_rows_rebuilt` (the ADR-0008
   recovery path, previously log-only), and `disk_below_reserve` (previously
   used only for admission control).
4. The watchdog consumes the verdict, so degradation reaches a human the same
   way an outage does. It reports **transitions plus a daily heartbeat**,
   because per-run logging on success made "healthy all along"
   indistinguishable from "the watchdog itself died".

**Superseded 2026-09-17:** an alerting channel does exist. The blog-side
status channel sends a heartbeat every five minutes and a death event on a
non-clean exit, riding the node's existing cloudflared tunnel, with the far
side calling the node offline after 15 minutes of silence
(`docs/status-reporting.md`). What remains true from the note above: the local
log is still the authoritative record and a human still reads it for anything
the card does not carry. Revisit with multiple nodes.

## Consequences

- An upstream that throttles us is now respected instead of hammered, and the
  client learns *when* to come back.
- Degradation is visible without SSH: healthz says what is wrong and why.
- A long silence in the watchdog log is now meaningful (it means healthy), and
  a missing daily heartbeat is itself a signal.
