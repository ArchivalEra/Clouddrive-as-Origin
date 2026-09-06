# Clouddrive-as-Origin — Multi-Backend Pull-Through Origin Shield

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
> embedded, ACID). Upstream storage: **OpenList** instances on loopback
> expose hundreds of cloud drives as WebDAV (native-proxy policy); this
> service is the pull-through disk cache in front of them via the
> StorageBackend trait (§5.1). Acceptance criteria are framework-agnostic.

## 1. Goal

A pull-through origin for `GET /<key>` (and `HEAD`): on hit, serve
from local disk; on cold miss, fetch from the owning upstream
(OpenList/WebDAV) and **stream to the client while writing to disk**
(water-pipe).
Entries expire **20 minutes after last access**. Disk usage is bounded;
when `max_size` is exceeded the entries closest to expiry are evicted
first (LRU). No garbage is left on disk. The cloud drives behind OpenList are
the durable source of truth; the VPS is a discardable hot cache.

The service is **multi-upstream from day one**: the config declares an
`upstreams` list — one per OpenList instance (or per WebDAV mount on
one instance). All cache state lives in a single global pool; each
entry records its owning upstream.

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
  id = "media"
  type = "openlist"
  base_url = "http://127.0.0.1:5244/dav"
  root_path = "music"
  username_env = "OPENLIST_USERNAME"
  password_env = "OPENLIST_PASSWORD"

  [[upstreams]]
  id = "archive"
  type = "openlist"
  base_url = "http://127.0.0.1:5245/dav"
  username_env = "OPENLIST2_USERNAME"
  password_env = "OPENLIST2_PASSWORD"

  [[routes]]
  prefix = "music/"     # -> media (OpenList #1)
  upstream = "media"
  [[routes]]
  prefix = "albums/"    # -> archive (OpenList #2)
  upstream = "archive"
  [[routes]]
  prefix = ""           # default (catch-all)
  upstream = "media"
  ```

  At request time the service resolves `key → upstream` (longest
  prefix first), then talks WebDAV to that OpenList: `PROPFIND
  Depth:0` for metadata and `GET` (with Range) for bytes. The business
  plane never knows which cloud drive sits behind the mount.
  At request time the service resolves `key → upstream` (longest
  prefix first), then calls the upstream's **StorageBackend** (§5.1)
  `stat` for metadata and `open` for bytes. The business plane never
  knows which provider answered.

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
  whose value is injected via env. On hit, no-op (200); on miss,
  answer `202 Accepted` immediately and fetch from the owning upstream
  in a background task (still passing single-flight and the
  per-upstream concurrency gate). CI/CD calls this after syncing new
  media so the first visitor is a 100% disk hit.

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

8. **Range (full 206 support, dual-channel cold miss):**
   - **Hit:** serve `206 Partial Content` from the cached file via
     file seek (zero upstream traffic) — audio/video seek must be
     milliseconds.
   - **Cold miss with `Range`:** NEVER download-then-serve, and never
     write a partial file into the cache. Dual-channel: the client
     stream passes through immediately from the backend at the
     requested offset (minimum TTFB), while a full-file fetch — sharing
     the same single-flight window — populates the complete cache in
     the background. The visitor hears the music while the cache
     quietly becomes complete; subsequent visitors are 100% disk hits.
   - Backend `open(key, range)` must pass the range through to the
     source when supported (Graph `downloadUrl` and Drive `alt=media`
     both honor `Range`).

9. **Response headers:** pass through `Content-Type`, `ETag`,
   `Last-Modified`; overwrite `Cache-Control` to
   `public, max-age=31536000, immutable` (filenames carry a version
   timestamp; the upload pipeline enforces the naming discipline).
   **MIME fallback:** many provider APIs return generic
   `application/octet-stream` for media (`.flac`, `.webp`, `.avif`,
   `.lrc`, ...). The business layer maintains a static extension →
   MIME table and overrides generic/missing values, so browsers stream
   audio/video in-player instead of popping a download dialog.

10. **Survives restart:** entries and their last-access timestamps are
    persisted in **redb** so the clock and eviction order survive a
    process restart. Partial-download temp files are always removed on
    startup.

## 4. Upstream details (OpenList WebDAV)

- Each OpenList instance exposes WebDAV at `http://<host>:5244/dav` with
  the web-UI credentials (basic auth, env-injected). The WebDAV policy on
  every mounted drive must be **native proxy** so byte streams flow
  through the instance with Range support (302 mode is not usable here).
- `stat` = `PROPFIND Depth:0` (via the `reqwest_dav` crate): maps
  `getcontentlength` → size, `getetag` → etag, `getlastmodified` →
  last-modified, `getcontenttype` → mime hint.
- `open` = `GET` with `Range` through our own streaming client
  (reqwest, `bytes_stream`): 200/206 accepted; full length parsed from
  the `Content-Range` total or `Content-Length`.
- Error mapping: 404 → `NotFound`, 401/403 → `AuthRequired` (surfaced
  in healthz), 429 → `RateLimited` (no Retry-After — jittered backoff),
  5xx → `ServerError` (stale-if-error applies).
- OpenList owns all provider credential rotation and per-drive quirks;
  our per-upstream concurrency gate (≤3) still applies to every
  PROPFIND/GET we issue.
- ETag semantics: OpenList reports per-driver etags; where a driver
  yields an unstable etag, `getlastmodified` is the revalidation
  fallback (stat-compare either field).

Per-upstream persisted state (all `0600`, under the data directory):

- redb database — entry metadata (see §3.10); OpenList credentials are
  env-only, nothing to persist.

## 5. Multi-upstream routing

- `upstreams` is a list; each entry has its own `client_id` (env
  reference), `drive_root_path`, token file, and concurrency limit.
- Routing is a first-match prefix table over `key` (longest-prefix
  wins, empty prefix = default). Adding a new upstream is a config-only
  change — no URL migration.
- The global cache pool is shared; each entry's redb record stores
  `upstream_id` so revalidation and `prewarm` hit the correct upstream.

### 5.1 StorageBackend trait (provider decoupling)

Upstream storage is abstracted as a unified trait; the business plane
only knows virtual keys and standard metadata — it never knows which
cloud answered:

```rust
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Standard remote-object metadata (size, etag/hash, mtime, mime hint).
    async fn stat(&self, key: &Key) -> Result<ObjectMeta, BackendError>;
    /// Read-only byte stream; passes Range through to the source when supported.
    async fn open(&self, key: &Key, range: Option<ByteRange>)
        -> Result<StreamSource, BackendError>;
    /// Credential rotation + health probe (OAuth refresh, quota check).
    async fn refresh_if_needed(&self) -> Result<(), BackendError>;
}
```

- `StreamSource` = a byte stream (`AsyncRead`) with a known-or-unknown
  length; `ByteRange = (offset, Option<length>)`.
- `BackendError` is a unified enum (NotFound / RateLimited / ServerError
  / Auth / Other) so cache semantics (negative cache, stale-if-error,
  backoff) are provider-agnostic.
- v1 provider: `src/backend/openlist.rs` — WebDAV against OpenList
  instances (native-proxy policy). The former hand-written cloud
  providers (GoogleDrive direct, OneDrive Graph) were dropped when the
  project pivoted to OpenList adaptation (2026-09-06); OpenList fronts
  hundreds of cloud drives, so per-provider code is unnecessary.
- Adding a non-OpenList source later is a new `impl StorageBackend`
  plus a config block — zero changes to the business plane.

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

All timeouts / limits have defaults. Secrets and hostnames are **env
references** (never literals). Canonical shape is `config.example.toml`:

```toml
# Planes
front_listen = "0.0.0.0:443"            # Pingora front plane
listen_addr = "127.0.0.1:8080"          # axum business plane (loopback only)
tls_cert_env = "ORIGIN_TLS_CERT_PATH"
tls_key_env = "ORIGIN_TLS_KEY_PATH"

# Cache & TTLs
cache_dir = "/var/lib/origin-cache"
max_size_bytes = 107374182400           # 100 GiB
inactive_ttl_secs = 1200               # 20 min
revalidate_ttl_secs = 60
negative_ttl_secs = 60
concurrency_per_upstream = 3
retry_max_attempts = 4
retry_base_ms = 200
retry_max_ms = 30000

[[upstreams]]
id = "primary"
drive_root_path = "/drive/root:/assets"
client_id_env = "ONEDRIVE_PRIMARY_CLIENT_ID"
client_secret_env = "ONEDRIVE_PRIMARY_CLIENT_SECRET"
refresh_token_env = "ONEDRIVE_PRIMARY_REFRESH_TOKEN"

[[routes]]
prefix = ""                            # default (catch-all)
upstream = "primary"
```

`prewarm_shared_secret_env` and `allowed_download_suffixes` are likewise
configurable; TLS/hostname material always via `*_env` indirection so the
repo never contains `${ORIGIN_HOST}`'s real value.

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
- Upstream writes (prewarm is read-only).
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
- [ ] Range: cached file serves `206` via file seek; cold-miss `Range`
      passes through from the backend at the requested offset (first
      byte before any full download completes) while the background
      full fetch lands a complete cache file — never a partial one.
- [ ] MIME fallback: `.flac` / `.webp` / `.avif` served with correct
      `Content-Type` even when the provider returns
      `application/octet-stream`.
- [ ] `prewarm` returns `202` immediately; the background fetch passes
      single-flight (concurrent prewarm + visitor = one upstream fetch)
      and `healthz` reports the prewarm queue depth.
- [ ] OpenList backend: PROPFIND stat mapping, ranged streaming GET,
      error taxonomy (404/401/429) — wiremock-tested end to end.
- [ ] Restart survival holds with redb (already in §3.10) for BOTH
      provider types.
