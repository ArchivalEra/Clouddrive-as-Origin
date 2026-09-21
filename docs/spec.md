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
    never resolve outside the configured `root_path` of the
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

4. **max_size eviction (magazine):** the cache is a magazine, not a
   hoard: it holds what fits and ejects the oldest to make room. When
   total cached bytes exceed `max_size` (default 100 GiB, configurable),
   evict entries in order of **earliest expiry = earliest last-access**
   until usage is back within the limit — i.e. LRU. Staged sidecars are
   in the same budget as entries (ADR-0007), so a partially-watched large
   object competes for room on the same terms as a complete one, and the
   magazine ejects whichever was touched longest ago. Eviction must remove
   metadata and, if the parent directory becomes empty, prune it.

   **Staged spans are evicted by span, under one of two policies
   (ADR-0015).** Staged bytes overshooting the budget are trimmed one
   `.seg` at a time, never by throwing away a whole key's window:
   `eviction_policy = "lru"` (default) takes the row's stalest span first,
   `"heat"` compares spans by read count inside the trailing 20 spans and
   widens one window back when more room is needed. Either way the rows are
   visited least-recently-touched first, so the choice *across* keys is LRU,
   and a row younger than 60 s is never touched. The ledger holds the read
   counts — `add_read` credits every span a request touches, including the
   staged head of a partial hit — so heat survives a merge as a sum. **The
   disk is the authority for what exists; the ledger is the policy's map of
   it (ADR-0016):** candidates are the key's real `.seg` files and the row is
   rebuilt from the survivors, so a ledger that merged (a sequential walk past
   the 4096-interval ceiling), decayed, or dropped its coldest records still
   evicts one file at a time.

   **An object larger than the whole magazine is resident, not evicted
   (ADR-0014).** Evicting others cannot bring it into budget — its own
   size is the overshoot — so the byte budget neither counts it nor picks
   it: the row is admitted with `oversize` set, it leaves on the 20-minute
   clock like anything else, and the reaper reclaims strays (oldest
   touched first) when free space falls below reserve + 2 GiB. Disk
   pressure takes strays only; the byte budget already governs the
   magazine's own members. `stray_bytes` in healthz reports the part of
   `bytes` the budget does not govern.

   **A request the cache cannot hold is served, not refused (ADR-0013).**
   The size is established by one coalesced `stat` before any flight
   exists, room is made by evicting strays, and a request that still does
   not fit is answered through the per-upstream pipe: no flight, no entry,
   no disk write, no error. The cache refuses to cache.

   **A staged-read run holds ONE stream permit for its window (ADR-0016)**, so
   the per-upstream budget is charged per window rather than per request: many
   viewers of the same window cost one permit. A cold seek still queues for
   that permit in the starting request's name.

   **A key being read is not evicted (ADR-0017).** Every clock here measures
   requests, so a body that streams for longer than a guard's window used to
   lose its bytes mid-flight (past 60 s it was budget-evictable, past
   `inactive_ttl` the sweep deleted it). A body served from local bytes now
   holds a **read lease** for its whole life — released when the body ends or
   the viewer disconnects — and policy eviction (byte/count budget, span trim,
   inactivity sweep) skips leased keys and keeps skipping them for
   `read_grace_secs` (default 300) after the last lease ends, so a pause inside
   one viewing session does not pay a re-fetch. Disk pressure is the
   exception: it may still reclaim a resident stray being streamed, because the
   disk is the last resort and an open descriptor survives the unlink.

   **A watched key is protected where it is being watched (ADR-0018).** A lease
   is anchored to a response body, and the real traffic shape is one 1 MiB shard
   per request: bodies last milliseconds and a viewing session lasts hours, so a
   300 s grace is the whole horizon a viewer gets — pause for six minutes and
   the window the chain already fetched is ordinary eviction material. A key
   that has been requested is therefore **watched** for `watch_idle_secs`
   (default 900) after its last body ended, remembering the byte range of the
   most recent response, and the protection is a bounded **neighbourhood**
   around that position: `watch_pin_bytes` (default 128 MiB, half behind and
   half ahead of it) rather than the whole key. That fixes both directions of
   the same mistake — the lease protected too little (protection lapsed
   mid-watch) and too much (a key being read could not be trimmed at all, so
   the byte budget stopped being enforceable against exactly the key a long
   watch was filling).

   **An object larger than the magazine gets a run on a bounded working window
   (ADR-0019).** What admission asks is whether the WRITE is affordable — a
   window no larger than the retention budget, and disk room for it — not
   whether the object fits: staged spans are served directly, so a bounded
   window of an object too large to keep whole is the sliding window its reader
   is walking through. Such a key may keep `watch_pin_bytes +
   session_window_bytes` staged (192 MiB by default) and its excess is spent
   before any keepable key's span, on every tick rather than only when the
   magazine is globally over budget. Measured on the node against a real 200 GiB
   object: a walk of 24 shards costs 1 upstream open (was 24), 400 shards cost 10
   (was 400), and the staged bytes settle at exactly the cap once the
   minimum-age guard has passed.

   **An object larger than the magazine is never cached WHOLE, and its reader
   still pays for random seeks.**
   Measured against a real 200 GiB video: admission logs "passthrough without
   staging: the magazine cannot hold this object" for every request, the cache
   holds nothing for the key (`segment_bytes`, `entries`, `stray_bytes` all
   zero), and every request is one provider `open` — 1.2 s per 1 MiB shard,
   0.82-0.92 s per random seek, while a stageable object walks at about one open
   per 64 MiB window. Playback survives on larger ranges (16 MiB = one open,
   1.5 s), scrubbing pays a provider round trip per gesture. `big-object-probe.sh`
   is the account; ADR-0019's run closes the walk half of it, and full coverage
   stays unreachable by construction (the magazine cannot hold the object).

   The pin is a preference, not an exemption: a trim takes bytes outside every
   pin first and spends a pin only when the rest of the cache cannot cover the
   need — and a pin that has to be spent is spent from its BACK (the spans the
   viewer has already watched) before the span it is about to need, because
   forward progress is continuous while a scrub back is deliberate. The chain's
   read-ahead also follows the watch rather than an attached body, so a pause
   does not throw away the window it already paid for. Every response holds its
   key's lease whatever its byte source: the longest streams are the upstream
   ones, and what they need protected is the key's own staged bytes.

5. **Revalidation on access (no background polling):** an entry that is
   present but older than a short TTL (default 60 s, configurable)
   triggers a revalidation before serving from disk: a `stat` is compared
   against the stored etag (or mtime). Providers behind OpenList are
   provider-uniform here — Drive answers no `304`, so a conditional request
   would buy nothing and cost a round trip. Unchanged → reset the
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
     write a partial file into the cache. Amended 2026-09-19 to match the
     implementation: the reader **converges on the flight's growing file**
     rather than opening its own upstream stream. Every upstream open pays
     a ~640 ms fixed cost and N concurrent ranges used to mean N opens
     (measured: 5 → 5), so the reader follows the one shared download and
     waits for the writer to reach its offset — normally zero wait, since
     EdgeOne delivers shards in ascending order. The wait is bounded by
     **inactivity, not total time**: every progress event re-arms the
     budget, so a reader far ahead of a flowing writer is not killed
     mid-body (ADR-0002 amendment).
     The `nocache` and `efficient` profiles serve a ranged miss straight
     from the backend instead (§3.11 C, §3.12).
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

11. **Outbound modes (A/B/C):** hits serve S3-shaped bytes from disk.
    Cold misses leave by one of three paths, chosen per upstream config:
    - **A (307 relief valve):** `cold_miss = "redirect"` + upstream-issued
      direct link available → `307 Temporary Redirect` to the link
      (`Cache-Control: no-store`, never edge-cached), bytes filled in
      background. Hits never redirect; every failure silently proxies.
    - **B (water-pipe, default):** stream + full-file fill simultaneously.
    - **C (passthrough):** ranged miss on a key with no durable entry →
      exact-Range origin bytes streamed straight through (no flight, no
      full fill). Full GETs and objects below the profile's `min_file_size`
      still water-pipe; all failures fall back to B (stale-if-error
      included).

12. **efficient cache fill policy (the default, per-upstream `cache_profile`):**
    orthogonal to §3.11's serve modes. Ranged misses stage a window of the
    served bytes as `.seg` sidecars and
    merge them into a per-key **coverage ledger** (etag-locked). Files
    below `min_file_size` (default 64 MiB) fill whole via B. Segments count
    separately (`segment_bytes` in healthz), age-sweep with `inactive_ttl`,
    rebuild from disk on restart.

    **Staged spans are directly servable, so the ledger IS the cache for
    objects the magazine cannot hold whole (ADR-0015).** A Range request is
    planned against the spans on disk: the covered parts are read from their
    sidecar files and the uncovered remainder is served by a **run**
    (ADR-0016). The ledger is thereby a sliding window sized by the byte
    budget, not by the object; promotion and the promotion hold are deleted,
    so there is no threshold, no merge task and no hold deadline.

    **A run is one upstream stream covering a window (ADR-0016).** The first
    ranged miss on a key opens `[frontier, frontier + session_window_bytes)`
    (default 64 MiB), pumps it through the flight machinery (watermark,
    inactivity-bounded waits, fsync + rename) and seals it as one `.seg` span.
    Every request whose range falls inside a live run's window is answered from
    its watermark with **no upstream open and no stream permit** — the ~640 ms
    open is paid once per window instead of once per request. A request no run
    covers takes its own exact Range (the escape, unchanged); a gap larger than
    the window widens it, so one response still costs one open; admission asks
    the disk about the window, since that is what gets written. When a run
    seals, the next window may start at once while a reader is still consuming
    and within one window of the boundary, so the open lands ahead of the
    playhead; a paused or departed reader stops the chain. `cache_session_total`
    and `cache_session_reader_total{result=attached|standalone}` are the
    account.

    **Version gate:** the etag the ledger last saw is compared with the stat
    the request already made; drift resets the key (spans, marker, row) and
    the request is served fresh. A missing sidecar for an interval the ledger
    claims means the request falls back to upstream rather than guessing at
    a mixed-version file.

    **Staging admission (ADR-0013):** an object larger than `max_size` is
    never staged (it is served as a resident stray instead when the disk can
    hold it, or through the pipe otherwise), and neither is an interval the
    disk has no room for. A refused staged request is served by the same
    pipe, byte for byte.

    **Ledger ceiling:** one key's ledger holds at most
    `MAX_INTERVALS_PER_KEY` (4096) intervals. Touching spans merge first
    (exact — `[a,b) + [b,c) = [a,c)`, and that is what a sequential scrub
    produces); if gaps remain, the coldest spans are dropped, because
    merging across a gap would claim bytes the node does not hold. A merge
    sums the read counts. Decay and the coverage count are one pass.

    **Coverage window:** each staged interval carries its read timestamp;
    intervals older than `coverage_window_secs` (default 3600, per-profile)
    decay out of the ledger, so the window tracks recently-active content
    rather than everything ever watched. Window expiry removes ledger
    entries only; disk sidecars stay for the natural sweep.

    **Viewer-disconnect sealing:** segmented downloads are
    separate connections; when the viewer disconnects, axum drops the
    body stream, so a detached watcher polls each `.segpart` and seals
    it once it stops growing (3 × 100 ms stable), finalizing coverage.
    The in-stream seal path stays for full-consumption transfers. Both
    paths go through one `seal_span`, so the ledger has exactly one writer
    shape. The read loop is bounded by the Range length — upstream 206
    streams may not signal EOF at Content-Length (e.g. rclone serve
    webdav).

    **Historical (retired with promotion, 2026-09-20):** five 600 MB ranged
    reads over a 3 GiB object used to reach 93% coverage, trigger a merge,
    and serve a later full GET from disk at **1.07 GB/s** against a 56 MB/s
    cold pull (~19×). The same node now serves those bytes from the staged
    spans directly — no merge, and the speedup applies to the first
    re-read rather than to the completed merge.

    **Upstream fetch is a single stream (corrected 2026-09-11):** cold
    misses open ONE upstream stream and pump it to disk while the client
    reads the growing file. Measured on the real upstream (OpenList →
    Google Drive, 3.1 GB file, raw curl bypassing this service):

    | upstream fetch | time | throughput |
    |---|---|---|
    | single stream | 71 s | 43.8 MB/s |
    | 100 MB segments × 31, conc 3 | 256 s | 11 MB/s |
    | 5 MB segments × 614, conc 3 | 292 s | 11 MB/s |

    Every segmented variant is ~4× slower: each upstream request pays a
    **~800 ms fixed stream-open cost** (a 1 KB Range request also takes
    ~820 ms — the cost is size-independent), and concurrency saturates
    near 15 MB/s. One stream pays that cost once.

    **Two hops, opposite optima — do not share one segment size.** An
    earlier revision fetched upstream in parallel 5 MB segments, applying
    a measurement taken on the **EdgeOne edge** (client → edge, where
    large single responses do degrade — see below) to the **upstream hop**,
    where the opposite holds. The edge still benefits from client-side
    Range requests; the upstream must not be segmented. Local lab after
    the fix: 3 GB cold pull 119 s → 52.5 s (27 → 61 MB/s).

    **EdgeOne edge behavior (live-measured 2026-09-09):** the edge
    serves 5 MB Range segments at full speed (16 MB/s aggregate) but
    degrades sharply for larger single responses — 10 MB segments drop
    to ~1.7 MB/s, 50 MB segments stall at ~700 KB/s and full 100 MB
    responses truncate around 70 MB. This is an edge-side platform
    behavior (verified from the origin node itself, ruling out client
    links). Real-world consumers (video seeking, resumable downloads,
    database clients) use Range requests natively. EdgeOne sharded
    origin-pull is enabled for `cdn-oracle.isui.ren/*` so the edge only
    origin-pulls missing shards. **This section governs the client↔edge
    hop only** — it does not describe the upstream hop above.

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
  our per-upstream concurrency gates (≤ `concurrency_per_upstream`, default
  3) still apply to every PROPFIND/GET we issue — as two independent
  budgets since ADR-0004: metadata lookups (stat/HEAD/list/link) and byte
  streams (cold-miss pumps, passthrough staging, staged reads) no
  longer share one pool, so a long transfer cannot starve a HEAD.
- ETag semantics: OpenList reports per-driver etags; where a driver
  yields an unstable etag, `getlastmodified` is the revalidation
  fallback (stat-compare either field).

Per-upstream persisted state (all `0600`, under the data directory):

- redb database — entry metadata (see §3.10); OpenList credentials are
  env-only, nothing to persist.

## 5. Multi-upstream routing

- `upstreams` is a list; each entry has its own `client_id` (env
  reference), `base_url` / `root_path`, credential env names, and
  concurrency limit.
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
    /// The provider's own link to the object, when it can offer one
    /// (the relief valve's 307 path); `Err` means "proxy instead".
    async fn direct_url(&self, key: &Key, viewer_ua: Option<&str>)
        -> Result<DirectUrl, BackendError>;
    /// One directory walk for the listing path (no cache semantics).
    async fn list(&self, folder: &str, recursive: bool)
        -> Result<Vec<ListEntry>, BackendError>;
    /// The configured upstream id this adapter serves.
    fn id(&self) -> &str;
}
```

- `StreamSource` = a byte stream (`AsyncRead`) with a known-or-unknown
  length; `ByteRange = (offset, Option<length>)`.
- `BackendError` is a unified enum (NotFound / RateLimited / ServerError /
  AuthRequired / RangeNotSatisfiable / Other) so cache semantics (negative
  cache, stale-if-error, backoff) are provider-agnostic. It carries upstream
  faults only: a client-side key error is answered at the resolve seam and
  has no variant here.
- v1 provider: `src/backend/openlist.rs` — WebDAV against OpenList.
  Its Tier-1 / Tier-2 / Tier-3 vocabulary for link handling lives in that
  module's comments, not here; `efficientcache` is the older name for the
  efficient profile and survives in code comments only.
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

### 6.1 S3-compatible inbound (ListObjectsV2 + SigV4)

The outbound speaks AWS S3 shapes.
Two add-on surfaces ride the same listener; both are additive to the
plain `GET /<key>` contract above.

- **ListObjectsV2** (`GET /?list-type=2...` or `GET /{bucket}?list-type=2`):
  S3-compatible listing backed by a live upstream PROPFIND — metadata
  only, never touches the object cache. Path-style bucket alias maps
  `/{bucket}` to the upstream id (virtual-host style unsupported).
  Full parameter contract, wire shapes, and measured edge cases are in
  this section (Contents-then-CommonPrefixes block order, form-urlencoded
  encoding set, `max-keys=0` -> empty non-truncated page). Responses
  carry `Cache-Control: private, max-age=5` so directory browsing also
  rides the CDN. The root path `/` is routed to the object handler via
  dedicated root handlers (matchit does not match `/` for the wildcard
  segment, and the `Path` extractor panics with no key segment).

- **SigV4 inbound verification** (optional verify-if-present): a request
  bearing `Authorization: AWS4-HMAC-SHA256 ...` or presigned query
  params (`X-Amz-Signature`) is verified against the credentials in
  `SIGV4_ACCESS_KEY_ID` / `SIGV4_SECRET_ACCESS_KEY`; unsigned requests
  pass through (D1 anonymous-first). Header form and presigned form
  share one verifier (s3s-derived): +-900 s clock skew, credential
  scope date consistency, X-Amz-Expires <= 604800, constant-time
  compare, raw-path retry. Failures are `403 AccessDenied` XML with
  `cache-control: no-store`. Both env vars unset = layer disabled.
  SigV4a (ECDSA-P256) is deferred — no Rust verifier crate exists.

- **Nocache profile** (built-in `cache_profile = "nocache"`): for
  small-footprint nodes (hundreds-of-MB RAM, GB-scale eMMC) that
  water-pipe without caching. Every GET/HEAD is stat + ranged open +
  stream-to-viewer; the cache directory receives **zero cache-object
  writes** — no entries, no segments, no negative tombstones, no
  coverage ledger, no access-clock writes; prewarm and the relief
  valve's background fill are no-ops. Only the redb metadata file
  exists on disk. Consequences: no stale-if-error (nothing to fall
  back to), no hit acceleration (every request walks the upstream), and
  `healthz` stays at zero entries — the CDN edge (EdgeOne) is expected
  to carry the caching duty for these nodes.

- **EdgeOne compatibility (operator requirement: edge caching
  must survive)**: EdgeOne forwards all client headers (Authorization
  included) and the full query string on origin-pull, and its cache
  key is the client URL + query — it does NOT include the
  Authorization header, so signed and anonymous requests share one
  cache entry. Two operator-side requirements:
  1. Do NOT add Authorization or signature params to the cache key
     (default behavior is already correct).
  2. If presigned URLs are used, configure the EdgeOne cache key to
     IGNORE the entire query string — origin-pull still carries the
     full query, so verification keeps working while every presigned
     request hits the same cache entry.
  3. Do NOT enable origin-pull URL rewriting / query stripping /
     Host rewriting rules on routes serving signed traffic — any of
     them breaks the signature.

  **Live-verified 2026-09-09 (oracle node, EdgeOne → apple.dib.l.cd:7777)**:
  | Behavior | Test | Result |
  |---|---|---|
  | Authorization forwarded to origin | SigV4-signed GET via EdgeOne domain | 200 (origin verified the signature — header arrived intact) |
  | Cache key excludes Authorization | Signed GET then anonymous GET, same URL | both `eo-cache-status: HIT` on the same entry (age identical) |
  | 403 not edge-cached | Bad-signature GET on a cold URL, twice | both 403 + `cache-control: no-store` + `eo-cache-status: MISS` (each hit the origin) |
  | Edge-cached URL + bad signature | Bad-signature GET on a warm URL | 200 HIT (edge serves without re-verifying — expected: auth is origin-side) |

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
id = "primary"                        # must equal the first URL path segment
type = "openlist"                     # v1 supports this one type
base_url = "http://127.0.0.1:5244/dav"
root_path = "assets"                  # provider-side path; keys append to it
username_env = "OPENLIST_USERNAME"
password_env = "OPENLIST_PASSWORD"
# cache_profile = "efficient"         # the default; or "nocache", or a name
                                     # declared in [cache_profiles.<name>]

[[routes]]
prefix = ""                            # default (catch-all)
upstream = "primary"
```

`prewarm_shared_secret_env` is likewise configurable; TLS/hostname material
always via `*_env` indirection so the repo never contains `${ORIGIN_HOST}`'s
real value. The redirect-target allow-list is a hardcoded predicate
(`backend::redirect_target_allowed`), not a config key.

## 8. Observability

Amended 2026-09-17 to describe what exists, with the two places the original
text no longer matched called out rather than dropped.

- Structured log, one line per request: `key`, outcome
  (`hit` / `hit-refresh` / `miss` / `stale` / `negative` / `error`),
  bytes. No token, secret, or full `downloadUrl` in logs. — holds: the front
  emits a `front access` line (method, path, status, bytes, duration, proto,
  xff) and the cache an outcome line (key, outcome, size). Upstream latency
  is a metric rather than a log field:
  `backend_call_duration_seconds{op}`.
- Per-minute self-check log: entry count, cached bytes, per-category
  counts for the last minute. — **replaced by design.** A per-minute line was
  noise next to the watchdog's transition-only logging plus one daily `HB`
  line (see `docs/status-reporting.md`); the same counters are available on
  demand from `/healthz` and continuously on the metrics endpoint. The quiet
  log is deliberate: silence means the verdict did not change, and a missing
  `HB` means the checker itself stopped.
- `/healthz` exposes the same counters plus per-upstream token state. —
  **counters yes, token state no.** healthz reports `entries`, `bytes`,
  `segment_bytes`, `stray_bytes`, `flights_active`,
  `dirty_access_flushes`, `coverage_keys`, `coverage_intervals`,
  `prewarm_inflight`, `store.state`, `rebuilt_rows`, `disk_free_bytes`,
  `disk_reserve_bytes`, and per upstream `id` / `profile` / `cold_miss` /
  `sigv4_layer`. **Known blind spot:** an upstream credential expiring does
  not move the verdict — healthz probes local serving, so every cold miss can
  fail with `AuthRequired` while healthz still says `ok` and the status card
  still says `active`. Today that is visible only in the logs and in
  `front_requests_total{status="502"}` / `backend_call_duration_seconds` on
  the metrics endpoint. Candidate ticket; not implemented here.
- `/_internal/healthz` additionally reports `segment_bytes` (staged,
  efficiently-staged sidecars). — holds.
- `/_internal/healthz` reports `stray_bytes` (2026-09-19): the part of
  `bytes` that belongs to resident strays, which the magazine's byte budget
  neither counts nor evicts (ADR-0014). It is the field that explains a
  `bytes` total above `max_size_bytes`.

### Can a seek be watched, not just guessed (2026-09-19)

`cache_serve_duration_seconds` observes functions that return an **unconsumed
body**, so it measures response construction and stops before the transfer —
it was documented as "time the cache layer spent serving a request", which
overstated it. The transfer now has its own instruments, on the shared
registry, visible at the front plane's `/metrics` (both planes register into
the same default registry):

- `cache_serve_source_total{source}` — responses by where their bytes came
  from (`disk` or `upstream`). The label is carried on the plan and the hit
  from the path that produced the bytes, not re-derived from the outcome: a
  `Miss` is upstream bytes while the flight is filling and disk bytes when a
  late attacher finds the sealed file.
- `cache_body_ttfb_seconds{source}` — request entry to the **first body
  byte**. This is the seek-smoothness number: a range served from this node's
  disk costs no upstream round trip, and a range served from upstream pays the
  measured per-open cost.
- `cache_body_bytes_total{source}` — bytes actually delivered, by source.

The label set is closed at those two values; no placeholder exists for a read
path that does not exist yet. The watchdog's report schema is a closed
whitelist agreed with the status page, so none of this is added there.


## 9. Non-goals (v1 explicitly out of scope)

- Upload (handled by a separate upload service / PicGo pipeline).
- Upstream writes (prewarm is read-only).
- HTML / page caching (only image-class static assets).
- EdgeOne site provisioning and `${ORIGIN_HOST}` DNS creation (operator
  task, tracked separately).
- The upload pipeline that calls `prewarm` (ships later; the endpoint
  itself still ships in v1).

## 10. Acceptance checklist

Verified 2026-09-17 against `eaec3f1`. Every line names evidence that can be
re-run (`cargo test <name>`, or the script path). Two lines were reworded to
match the implementation and one was retired as vacuous.

- [x] Cold miss TTFB < 1.5 s (typical domestic → VPS) and first byte
      arrives before download completes.
      — `deploy/measure-client-ttfb.sh`: measured 2026-09-16 from a domestic
      client through EdgeOne, 0.66–0.91 s cold, all inside the budget. The
      script states its own limit: it measures latency, not concurrency.
      Streaming: `cache::flight::tests::growing_reader_follows_writer`.
- [x] 20 concurrent cold requests for the same `key` → exactly one upstream
      metadata call + one download.
      — `tests/integration.rs::single_flight_20_concurrent_same_key_one_fetch`
      (asserts the upstream call count).
- [x] 20 min without access (test may use an accelerated clock) →
      file and metadata both gone, `du` returns to zero.
      — `inactive_ttl_expiry_removes_file_and_meta`,
      `inactive_expiry_via_tick`,
      `spawned_reaper_expires_entries_without_manual_tick`.
- [x] Fill past `max_size` → eviction order = ascending last-access,
      total returns within limit.
      — `max_size_evicts_lru_order`, `eviction_picks_lru_victims_in_order`,
      `entry_count_cap_evicts_lru_even_under_byte_budget`.
- [x] After modifying the file upstream: first access outside the
      revalidation TTL returns the new content with no interruption.
      — `revalidation_uses_stat_and_serves_updated_content`,
      `revalidation_not_modified_serves_revalidated`.
- [x] Upstream `500` → serve stale file with `Warning` when available.
      — `cache::cache::tests::stale_if_error_serves_cached`.
- [x] Traversal payloads (`..%2f` etc.) all `400`.
      — `tests/integration.rs::traversal_payloads_are_400_via_fetch_error`
      (typed since 2026-09-17, not text-matched),
      `business::tests::invalid_object_keys_are_bad_requests_for_get_and_head`
      (through the real router),
      `response::tests::every_key_error_maps_to_400_not_backend_failure`.
- [x] Cache hits and eviction order survive a restart.
      — `cache_entries_and_access_clock_survive_restart`; on the live node
      2026-09-17 a real restart re-served the same object with `entries=1,
      rebuilt_rows=0`, i.e. the rows came from the store, not a rebuild.
- [x] Logs and `/healthz` satisfy §8 with zero secret leakage.
      — §8 as amended below; the status report's field whitelist is asserted
      by `deploy/oracle/test-watchdog.sh` ("no healthz internals leak").
- [x] Two-upstream routing: keys matching different prefixes hit
      different upstreams; adding a third upstream requires only a
      config change.
      — `tests/integration.rs::two_upstream_routing_by_prefix`.
- [x] Range: cached file serves `206` via file seek; cold-miss `Range`
      passes through from the backend at the requested offset (first
      byte before any full download completes) while the background
      full fetch lands a complete cache file — never a partial one.
      — `range_on_cached_file_slices_and_reports_content_range`,
      `range_cold_miss_offset_zero_streams_full_with_content_range`,
      `range_cold_miss_dual_channel_passthrough_and_background_fill`,
      `unsatisfiable_range_rejected`, `short_upstream_body_is_never_sealed`.
- [x] MIME fallback: `.flac` / `.webp` / `.avif` served with correct
      `Content-Type` even when the provider returns
      `application/octet-stream`.
      — `tests/integration.rs::mime_fallback_overrides_octet_stream`.
- [x] `prewarm` returns `202` immediately; the background fetch passes
      single-flight (concurrent prewarm + visitor = one upstream fetch)
      and `healthz` reports the in-flight fetch count.
      — `prewarm_accepts_immediately_and_fetches_in_the_background`,
      `prewarm_reports_a_hit_without_queueing_anything`,
      `prewarm_secret_gate_blocks_anonymous`; healthz field
      `prewarm_inflight`. Reworded 2026-09-17: prewarm has no queue by
      design, it has fetches in flight, and that count is what healthz
      reports. Until `eaec3f1` the handler awaited the whole fetch and
      answered `200`, contradicting §2.
- [x] OpenList backend: PROPFIND stat mapping, ranged streaming GET,
      error taxonomy (404/401/429) — wiremock-tested end to end.
      — `tests/openlist.rs`: `stat_maps_propfind_to_object_meta`,
      `stat_missing_key_maps_to_not_found`,
      `stat_bad_credentials_map_to_auth_required`,
      `open_passes_range_header_and_streams`, `unknown_type_is_rejected`.
- [~] ~~Restart survival holds with redb (already in §3.10) for BOTH
      provider types.~~
      **Retired 2026-09-17**: only one backend type exists (`openlist`;
      `main.rs` refuses any other), so the cross-provider half of this line
      cannot fail. Reinstate it if a second type lands.

### Added 2026-09-19 (the admission round; each line was reverse-verified by
disabling its fix and watching the named test fail)

- [x] An object larger than the magazine is a **resident stray**: admitted,
      served from disk for later ranges, and it evicts nothing on the way in.
      — `an_object_larger_than_the_magazine_is_cached_as_a_stray`,
      `cache::cache::tests::a_resident_stray_never_evicts_the_magazine`.
- [x] An object that can never be promoted is **never staged**: no sidecar, no
      ledger row, no staged bytes, and the viewer still gets every byte.
      — `business::tests::an_object_larger_than_the_cache_is_never_staged`.
- [x] A request the cache cannot hold is **served, not refused**: no flight,
      no entry, no disk write, no 502.
      — `a_cold_pull_the_disk_cannot_hold_is_served_without_caching`.
- [x] Disk pressure reclaims resident strays, oldest-touched first, and
      nothing else.
      — `cache::cache::tests::strays_are_reclaimed_under_disk_pressure_oldest_first`.
- [x] A complete entry keeps serving ranges past the revalidate window: one
      stat, no bytes, no upstream open.
      — `business::tests::efficient_complete_entry_keeps_serving_ranges_after_the_revalidate_window`.
- [x] A reader parked ahead of a flowing writer survives a wait longer than
      one stall budget; a genuinely stalled pull still errors inside it.
      — `cache::flight::tests::far_ahead_reader_survives_a_flowing_writer_beyond_one_budget`,
      `stalled_flight_ends_reader_within_budget`.
- [x] The passthrough holds the **stream** gate for its transfer.
      — `tests/integration.rs::efficient_passthrough_waits_for_a_stream_permit`.
- [x] One key's ledger is bounded without losing coverage.
      — `cache::store::tests::a_sequential_walk_is_bounded_and_loses_no_coverage`,
      `a_gapped_ledger_is_bounded_by_dropping_the_coldest_spans`.
- [x] A response's byte source and its first-byte latency are measurable.
      — `cache::cache::tests::serve_labels_the_bytes_with_their_source`,
      `business::tests::http_get_records_its_byte_source`,
      `business::tests::instrument_body_delivers_every_byte_and_counts_it`.


### Added 2026-09-20 (the eviction round; each line was reverse-verified by
reverting the rule and watching the named test fail)

- [x] Staged spans are **directly servable**: a fully covered seek opens
      upstream zero times, and a partly covered one opens it exactly once.
      — `business::tests::a_fully_covered_seek_is_served_from_stage_without_upstream`,
      `a_partially_covered_range_needs_one_open_and_stages_the_rest`.
- [x] The two eviction policies **disagree** on the case heat exists for: a
      span that is stale by the clock but hot by reads survives under `heat`
      and is the first to go under `lru`. Reverting the policy flag on either
      test flips which span lives.
      — `business::tests::heat_eviction_keeps_the_hot_span_lru_would_eject`,
      `lru_eviction_ejects_the_stale_span_even_when_it_is_hot`.
- [x] Eviction is **span-level**: an overshoot trims the cold tail, the key's
      other spans and every other key's spans stay, and the byte account drops
      by the evicted span rather than by a whole window.
      — the same two tests (they assert the surviving span and
      `segment_bytes`), `tests/integration.rs::staged_segment_bytes_join_the_disk_budget`.
- [x] A read credits **every span it touches**: one straddling two spans
      credits both, and the staged head of a partial hit is credited.
      — `business::tests::a_read_credits_every_staged_span_it_touches`.

### Added 2026-09-20 (the eviction-granularity round; the fix was
reverse-verified by restoring the exact-bounds candidate rule and watching all
three named tests fail)

- [x] A **merged** ledger interval (a sequential walk past the 4096-interval
      ceiling) still evicts one file at a time, and the row is rebuilt from the
      survivors with their read times carried across.
      — `a_merged_interval_still_evicts_one_file_at_a_time`,
      `cache::store::tests::adopt_files_keeps_the_invariants_and_carries_policy`.
- [x] A row whose records **decayed** away (files still on disk) is still
      evictable, and the survivors are recorded again.
      — `a_decayed_interval_leaves_its_files_evictable`.
- [x] The ceiling's shape at scale: 4097 sequential 1-byte spans (compaction
      merges them, so no interval's bounds name a file) still bring the byte
      budget back under.
      — `a_sequential_walk_past_the_ledger_ceiling_stays_evictable`.

### Added 2026-09-20 (the one-stream round; each line was reverse-verified by
disabling the mechanism and watching the named test fail)

- [x] Seeks inside one live window share **one upstream open**, byte-exact, and
      are labelled stage-served.
      — `ranged_seeks_on_one_key_share_one_upstream_open` (reverting attach
      makes it a per-request open).
- [x] A seek the live run does not cover opens its **own** exact Range.
      — `a_seek_far_beyond_the_window_opens_its_own_range`.
- [x] A sealed run's spans serve later reads with no further upstream open.
      — `a_sealed_run_leaves_spans_that_serve_later_reads`.
- [x] A walk costs **one open per window**, not one per request.
      — `a_sequential_walk_opens_once_per_window`.
- [x] A failed open **errors its reader** rather than parking it.
      — `a_run_that_cannot_open_errors_its_reader_instead_of_hanging`.
- [x] A consuming reader **chains the next window**; a paused one buys at most
      one window of read-ahead and stops.
      — `cache::session::tests::a_consuming_reader_chains_the_next_window`,
      `a_paused_reader_buys_at_most_one_window_of_read_ahead` (reverting the
      chain turns both red).
- [x] The window is clamped to the object, and a bigger need widens it.
      — `cache::session::tests::the_window_is_clamped_to_the_object`,
      `a_need_larger_than_the_window_widens_it`.

### Added 2026-09-21 (the read-lease round; reverse-verified by making every
eviction path ignore the lease map and watching all three tests fail)

- [x] A key being streamed is not evicted by the **byte budget**, and stays
      protected for the grace after the body ends.
      — `a_leased_key_survives_the_budget_until_its_grace_expires`.
- [x] The **inactivity sweep** spares a key being read, even a stream that
      outlives `inactive_ttl`.
      — `the_age_sweep_spares_a_key_being_read`.
- [x] End to end: a response body that is still in flight keeps its bytes alive
      past the TTL, and the bytes go once the viewer is gone.
      — `business::tests::a_body_in_flight_keeps_its_bytes_alive_past_the_ttl`.
- [x] The lease map prunes itself rather than growing with every key ever read,
      and a zero grace protects only live bodies.
      — `cache::leases::tests::the_map_does_not_grow_with_every_key_ever_read`,
      `a_zero_grace_protects_only_while_the_body_lives`.
