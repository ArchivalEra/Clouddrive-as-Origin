# OneDrive Pull-Through Origin Cache — Specification

> Handoff spec: an implementer (human or agent) can complete the
> build without follow-up questions.
> Platform: Oracle VPS, Ubuntu, 200 GB disk, ~10 TB/month free egress.
> Deployment: single binary + systemd. Front is EdgeOne CDN
> (origin-pull to this service over HTTPS; see §6).
>
> Implementation: **Pingora (Rust) as front plane** — TLS termination,
> H2, connection management, reverse-proxy to localhost — plus
> **axum / hyper / tokio as business plane** on `127.0.0.1` which owns
> **all** cache semantics. Metadata store: **redb** (pure-Rust,
> embedded, ACID). Acceptance criteria are framework-agnostic.

## 1. Goal

A pull-through origin for `GET /<key>` (and `HEAD`): on hit, serve
from local disk; on cold miss, fetch from OneDrive via Microsoft
Graph and **stream to the client while writing to disk** (water-pipe).
Entries expire **20 minutes after last access**. Disk usage is bounded;
when `max_size` is exceeded the entries closest to expiry are evicted
first (LRU). No garbage is left on disk. OneDrive is the durable
source of truth; the VPS is a discardable hot cache.

The service is **multi-upstream from day one**: the config declares an
`upstreams` list (≥ 2 real accounts in v1, a dozen or more later). All
cache state lives in a single global pool; each entry records its
owning upstream.

The origin hostname that EdgeOne pulls from is **never stored in this
repo** — it is injected at runtime via an environment variable (e.g.
`ORIGIN_HOST`). Docs and examples use the placeholder
`${ORIGIN_HOST}`.

## 2. Interface contract

- `GET` / `HEAD` only. Path form `/<key>` where `key` is a flat
  relative path that may contain subdirectories (e.g.
  `2026/08/image.png`).
- Each `key` maps to exactly one upstream. The server-side TOML
  declares prefix rules that route a key to an upstream; clients see a
  **single flat namespace** — adding an upstream never changes existing
  URLs. Example shape (illustrative; actual TOML keys TBD):

  ```toml
  [[upstreams]]
  id = "primary"
  drive_root_path = "/drive/root:/assets"
  client_id = "${ONEDRIVE_PRIMARY_CLIENT_ID}"  # env reference, no literal

  [[routes]]
  prefix = ""          # default upstream
  upstream = "primary"
  ```

  At request time the service resolves `key → upstream`, then calls
  Graph `GET /me/drive/root:<drive_root_path>/<key>` to obtain
  `@microsoft.graph.downloadUrl` and metadata (`size`, `eTag`,
  `lastModifiedDateTime`), then fetches the `downloadUrl`.

- **Security hard constraints (acceptance gates):**
  - Outbound only to `https://`. Before every server-initiated request,
    validate the target host and reject `localhost`, loopback, private
    ranges (RFC 1918 / CGNAT / link-local) and other reserved ranges.
    `downloadUrl` is a dynamic Microsoft domain — it must still pass
    this check.
  - `key` must reject path traversal (`..`, backslash, absolute path,
    NUL byte, percent-encoded variants such as `%2e%2e%2f`) and must
    never resolve outside the configured `drive_root_path` of the
    selected upstream.
  - Credential handling: client secrets and refresh tokens are **only**
    supplied via environment variables (or a secret service) at boot.
    Refreshed tokens are persisted to the data directory with mode
    `0600`, never committed, never logged. Source, examples and tests
    **must not contain usable credential literals** — use placeholders
    or env references.

- `POST /_internal/prewarm/<key>`: requires a shared-secret header
  whose value is injected via env. On hit, no-op; on miss, fetch from
  the owning upstream and populate the cache without streaming to the
  caller. The upload pipeline calls this after uploading an image.

- `GET /_internal/healthz`: `200` with basic info — entry count, disk
  usage, token status per upstream. No secrets in the response.

## 3. Cache semantics (each item is an acceptance gate)

1. **Water-pipe:** on cold miss, start streaming to the client
   immediately (use `Content-Length` from Graph metadata up front);
   write to disk while sending. "Download-then-serve" is forbidden.

2. **Single-flight:** concurrent cold requests for the same `key`
   coalesce to a single origin fetch; others wait and reuse the
   result. Different keys do not block each other. Single-flight is
   per-key; token refresh has its own global single-flight (see §4).

3. **Inactive = 20 min:** an entry's last-access timestamp (a hit that
   reads from disk also counts as an access) is the clock. If 20
   minutes pass with no access, delete the file and its metadata.
   Any access resets the clock.

4. **max_size eviction (magazine):** when total cached bytes exceed
   `max_size` (default 100 GiB, configurable), evict entries in order
   of **earliest expiry = earliest last-access** until usage is back
   within the limit — i.e. LRU. Eviction must remove metadata and, if
   the parent directory becomes empty, prune it.

5. **Revalidation on access (no background polling):** an entry that is
   present but older than a short TTL (default 60 s, configurable)
   triggers a conditional Graph request (`If-None-Match` / `ETag` or
   `lastModified`) before serving from disk. Not modified → reset the
   20 min clock and serve from disk. Modified → treat as cold miss and
   atomically replace (temp file + `rename`; in-flight readers keep
   serving the old file).

   Hits within the TTL are served from disk with no upstream request.

6. **Negative cache:** upstream-confirmed `404` is cached as a `404`
   for 60 s (configurable); during that window, return `404` directly
   (stampede protection). After expiry, resume normal origin fetch.

7. **Resilience:** Graph / `downloadUrl` returning `429` → honor
   `Retry-After` with bounded jitter; the global per-upstream queue
   waits. `5xx` / network error → if a stale file exists, serve it
   as `stale-if-error` (with a `Warning` header); otherwise `502`.
   All backoff has an upper bound and jitter.

8. **Range:** clients may send `Range`. For cached files, slice bytes
   directly. For a cold miss that carries `Range`, fetching the whole
   file is acceptable (Graph `downloadUrl` supports `Range`; serving
   `Range` while streaming is optional — acceptance is lenient).

9. **Response headers:** pass through `Content-Type`, `ETag`,
   `Last-Modified`; overwrite `Cache-Control` to
   `public, max-age=31536000, immutable` (filenames carry a version
   timestamp; the upload pipeline enforces the naming discipline).

10. **Survives restart:** entries and their last-access timestamps are
    persisted in **redb** so the clock and eviction order survive a
    process restart. Partial-download temp files are always removed on
    startup.

## 4. Upstream details (Graph)

- Personal OneDrive requires **delegated auth** — a one-time device-code
  flow obtains a refresh token; thereafter the service refreshes it
  internally. Refresh is globally single-flight per upstream (concurrent
  `401`s trigger exactly one refresh). Proactive renewal 5 minutes
  before expiry.
- `downloadUrl` is short-lived and may redirect. Re-fetch metadata on
  every origin pull; never cache `downloadUrl` long-term. Requests to
  `downloadUrl` still pass the §2 host allow-check.
- `429` / throttling is normal, not exceptional. Per-upstream Graph
  concurrency ≤ 3 (configurable).

Per-upstream persisted state (all `0600`, under the data directory):

- `tokens/<upstream-id>.json` — refreshed token material
- redb database — entry metadata (see §3.10)

## 5. Multi-upstream routing

- `upstreams` is a list; each entry has its own `client_id` (env
  reference), `drive_root_path`, token file, and concurrency limit.
- Routing is a first-match prefix table over `key` (longest-prefix
  wins, empty prefix = default). Adding a new upstream is a config-only
  change — no URL migration.
- The global cache pool is shared; each entry's redb record stores
  `upstream_id` so revalidation and `prewarm` hit the correct upstream.

## 6. Deployment & TLS

- Single binary. Pingora front plane listens on `443` (and `80` for
  ACME challenge if used) and terminates TLS; it reverse-proxies to
  the axum business plane on `127.0.0.1` (plain HTTP, standard tokio
  runtime). This keeps the runtime seam clean — no dual-runtime shared
  mutable state.

- `ORIGIN_HOST` and TLS certificate/key paths are supplied via
  environment variables. The repo contains **no real hostname or
  certificate material**. EdgeOne origin-pull is **HTTPS**; the
  Pingora front presents a certificate for `${ORIGIN_HOST}` (e.g.
  via Let's Encrypt DNS-01 using credentials injected via env).
  Renewal is handled by a systemd timer outside this binary.

- OCI Security List / firewall allows inbound `443` only from EdgeOne
  origin-pull ranges (plus operator SSH). `/_internal/*` is still
  gated by its shared secret even though the listener is `0.0.0.0`.

## 7. Configuration (single TOML file)

All timeouts / limits have defaults. Secrets are **env references**
(never literals). Sketch:

```toml
listen_addr = "127.0.0.1:8080"          # axum business plane
front_listen = "0.0.0.0:443"            # Pingora front plane
cache_dir = "/var/lib/origin-cache"
max_size_bytes = 107374182400           # 100 GiB
inactive_ttl_secs = 1200               # 20 min
revalidate_ttl_secs = 60
negative_ttl_secs = 60
graph_concurrency_per_upstream = 3

[[upstreams]]
id = "primary"
drive_root_path = "/drive/root:/assets"
client_id_env = "ONEDRIVE_PRIMARY_CLIENT_ID"
client_secret_env = "ONEDRIVE_PRIMARY_CLIENT_SECRET"
refresh_token_env = "ONEDRIVE_PRIMARY_REFRESH_TOKEN"

[[routes]]
prefix = ""
upstream = "primary"
```

`prewarm_shared_secret_env`, TLS `cert_path` / `key_path` are likewise
env-backed.

## 8. Observability

- Structured log, one line per request: `key`, outcome
  (`hit` / `hit-refresh` / `miss` / `stale` / `negative` / `error`),
  bytes, upstream latency. No token, secret, or full `downloadUrl` in
  logs.
- Per-minute self-check log: entry count, cached bytes, per-category
  counts for the last minute.
- `/healthz` exposes the same counters plus per-upstream token state.

## 9. Non-goals (v1 explicitly out of scope)

- Upload (handled by a separate upload service / PicGo pipeline).
- OneDrive writes (prewarm is read-only).
- HTML / page caching (only image-class static assets).
- EdgeOne site provisioning and `${ORIGIN_HOST}` DNS creation (operator
  task, tracked separately).
- The upload pipeline that calls `prewarm` (ships later; the endpoint
  itself still ships in v1).

## 10. Acceptance checklist

- [ ] Cold miss TTFB < 1.5 s (typical domestic → VPS) and first byte
      arrives before download completes.
- [ ] 20 concurrent cold requests for the same `key` → exactly 1 Graph
      metadata call + 1 download.
- [ ] 20 min without access (test may use an accelerated clock) →
      file and metadata both gone, `du` returns to zero.
- [ ] Fill past `max_size` → eviction order = ascending last-access,
      total returns within limit.
- [ ] After modifying the file on OneDrive: first access outside the
      revalidation TTL returns the new content with no interruption.
- [ ] Upstream `500` → serve stale file with `Warning` when available.
- [ ] Traversal payloads (`..%2f` etc.) all `400`.
- [ ] Cache hits and eviction order survive a restart.
- [ ] Logs and `/healthz` satisfy §8 with zero secret leakage.
- [ ] Two-upstream routing: keys matching different prefixes hit
      different upstreams; adding a third upstream requires only a
      config change.
