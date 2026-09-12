use std::{collections::{HashMap, HashSet}, sync::Arc};
use tokio::sync::{Mutex, RwLock};

use crate::{
    backend::{BackendError, BackendRegistry, BackendSlot, ContentRange, DirectUrl, Key, ObjectMeta},
    cache::{
        flight::{self, BodyStream, FlightProgress, FlightShared},
        meta::EntryMeta,
        store,
    },
    clock::Clock,
    config::Config,
    inflight::Inflight,
    key::{ResolvedKey, resolve_key, validate_key},
    routing::RouteTable,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheOutcome {
    Hit,
    Miss,
    Negative,
    Stale,
    Revalidated,
}

/// Response metadata for the business plane's headers.
#[derive(Debug, Clone)]
pub struct HitMeta {
    pub size: u64,
    pub etag: Option<String>,
    pub content_type: Option<String>,
    pub last_modified: Option<String>,
}

impl From<&ObjectMeta> for HitMeta {
    fn from(m: &ObjectMeta) -> Self {
        Self {
            size: m.size_bytes,
            etag: m.etag.clone(),
            content_type: m.mime_hint.clone(),
            last_modified: m.last_modified.clone(),
        }
    }
}

impl From<&EntryMeta> for HitMeta {
    fn from(m: &EntryMeta) -> Self {
        Self {
            size: m.size_bytes,
            etag: m.etag.clone(),
            content_type: m.content_type.clone(),
            last_modified: m.last_modified.clone(),
        }
    }
}

/// Build response headers' metadata with MIME fallback applied (§3.9).
fn hit_meta_remote(key: &str, m: &ObjectMeta) -> HitMeta {
    HitMeta {
        size: m.size_bytes,
        etag: m.etag.clone(),
        content_type: crate::mime::resolve(key, &m.mime_hint),
        last_modified: m.last_modified.clone(),
    }
}

fn hit_meta_entry(key: &str, m: &EntryHeaders) -> HitMeta {
    HitMeta {
        size: m.size_bytes,
        etag: m.etag.clone(),
        content_type: crate::mime::resolve(key, &m.content_type),
        last_modified: m.last_modified.clone(),
    }
}

/// A cache response: headers' worth of metadata plus a streaming body
/// (water-pipe — the body may still be downloading from the backend).
/// `content_range` is Some for 206 responses (cached-file slices and
/// cold-miss Range passthrough per §3.8): pure data, rendered by the
/// response module (C3). `content_length` is the exact byte count the
/// body will deliver when known up front.
pub struct CacheHit {
    pub outcome: CacheOutcome,
    pub meta: HitMeta,
    pub content_range: Option<ContentRange>,
    pub content_length: Option<u64>,
    pub body: BodyStream,
}

/// C-path / nocache response: origin bytes with the streaming
/// coordinates attached. No outcome: these paths never touch entries,
/// flights, or revalidation.
pub struct PassthroughHit {
    pub meta: HitMeta,
    pub etag: Option<String>,
    pub total: u64,
    pub content_range: Option<ContentRange>,
    pub content_length: Option<u64>,
    pub body: BodyStream,
}

pub struct CacheState {
    pub entries: HashMap<String, EntryMeta>,
    pub total_bytes: u64,
    /// Bytes staged as `.seg` sidecars (efficientcache): served but not
    /// yet promoted. Swept by age, never evicted by LRU (separate counter
    /// so entry eviction math stays exact).
    pub segment_bytes: u64,
    pub segment_sweep_at_millis: u64,
}

impl Default for CacheState {
    fn default() -> Self {
        Self { entries: HashMap::new(), total_bytes: 0, segment_bytes: 0, segment_sweep_at_millis: 0 }
    }
}

/// Lock-free access clock: the per-hit alternative to taking the state
/// write lock to stamp `last_access_millis` (audit P1). Hits record into
/// sharded maps without any exclusive lock; the one-second flusher folds
/// the batch into `CacheState` (and redb) in a single write stretch.
///
/// Lag bound: a hit's timestamp is visible to the reaper/evictor within
/// one flush tick (<= ~1 s). Both readers use the value only against a
/// 1200 s TTL (reap) or for relative LRU ordering, so sub-second lag is
/// invisible — verified in the audit (readers: `reap_collect`,
/// `eligible_at`).
#[derive(Debug, Default)]
pub struct AccessClock {
    shards: [Mutex<HashMap<String, u64>>; ACCESS_SHARDS],
    pending: std::sync::atomic::AtomicUsize,
}

const ACCESS_SHARDS: usize = 16;

fn shard_of(key: &str) -> usize {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut h);
    (h.finish() as usize) % ACCESS_SHARDS
}

impl AccessClock {
    pub fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| Mutex::new(HashMap::new())),
            pending: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Record an access. Never takes the state lock; a key that is absent
    /// from the entry map is harmless (the flusher's fold is a no-op).
    pub async fn touch(&self, key: &str, now_millis: u64) {
        let mut s = self.shards[shard_of(key)].lock().await;
        s.insert(key.to_string(), now_millis);
        self.pending.store(s.len(), std::sync::atomic::Ordering::Relaxed);
    }

    /// Drain every shard for the flusher; returns the batch and shrinks the
    /// pending gauge.
    pub async fn drain(&self) -> Vec<(String, u64)> {
        let mut out = Vec::new();
        for shard in &self.shards {
            let mut g = shard.lock().await;
            out.extend(g.drain());
        }
        self.pending.store(0, std::sync::atomic::Ordering::Relaxed);
        out
    }

    pub fn pending(&self) -> usize {
        self.pending.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// The five entry fields the disk-hit path needs, copied out under one
/// read guard (P1).
#[derive(Debug, Clone)]
struct EntryHeaders {
    size_bytes: u64,
    etag: Option<String>,
    last_modified: Option<String>,
    content_type: Option<String>,
    negative: bool,
}

/// Owned healthz view of the cache machinery (C4).
#[derive(Debug, Clone)]
pub struct CacheSnapshot {
    pub entries: usize,
    pub total_bytes: u64,
    pub segment_bytes: u64,
    pub flights_active: usize,
    pub promotions_active: usize,
    pub dirty_access_pending: usize,
}

/// The Cache is the only seam between the HTTP layer and the cache
/// machinery (ADR-0002). Its interface is the request path (get / head /
/// resolve / per-profile serves), lifecycle (load_and_start / tick), the
/// relief-valve helpers (prefetch / direct_url_bounded), and the
/// operator view (snapshot). The pub fields below are the machinery's
/// working state — consumed by tests; production code goes through the
/// methods, never through them.
pub struct Cache<C: Clock> {
    pub config: Arc<Config>,
    pub clock: Arc<C>,
    pub backends: BackendRegistry,
    pub state: Arc<RwLock<CacheState>>,
    /// redb-backed metadata persistence (spec §3.10: entries + access clock
    /// + eviction order survive restarts).
    pub meta: Arc<crate::cache::persist::MetaStore>,
    /// Access-clock bumps awaiting the coalesced fold (R1: per-hit fsync
    /// would bottleneck; fold at most once per second). Lock-free on the
    /// hit path — see [`AccessClock`].
    pub dirty_access: Arc<AccessClock>,
    /// In-flight cold-miss downloads, keyed by cache key. The flight
    /// module owns joining, driver spawning, panic guarding and map
    /// hygiene; the Cache only supplies the driver policy.
    pub flights: crate::cache::flight::Flights,
    /// Coverage ledger (efficientcache): staged byte intervals per cache
    /// key. Segment files on disk are the source of truth; this map is
    /// the working view, rebuilt by scan on startup.
    pub coverage: Arc<Mutex<HashMap<String, store::Coverage>>>,
    /// Keys with a promotion task in flight (P2-b single-flight: threshold
    /// re-hits while promoting attach to nothing — the task re-verifies).
    pub promotions: Arc<Mutex<HashSet<String>>>,
    pub reval_inflight: Inflight<StatData, BackendError>,
    pub routes: RouteTable,
}

#[derive(Debug, Clone)]
struct StatData {
    meta: ObjectMeta,
}

impl<C: Clock + Clone> Cache<C> {
    pub fn new(config: Arc<Config>, clock: Arc<C>, backends: BackendRegistry) -> Self {
        let routes = config.routes.clone();
        let meta = Arc::new(crate::cache::persist::MetaStore::open(&config.cache_dir.join("redb.db")).expect("open redb metadata store"));
        let dirty_access = Arc::new(AccessClock::new());
        Self {
            config,
            clock,
            backends,
            state: Arc::new(RwLock::new(CacheState::default())),
            meta,
            dirty_access,
            flights: crate::cache::flight::Flights::new(crate::cache::flight::DEFAULT_STALL_BUDGET),
            coverage: Arc::new(Mutex::new(HashMap::new())),
            promotions: Arc::new(Mutex::new(HashSet::new())),
            reval_inflight: Inflight::new(),
            routes,
        }
    }

    /// Startup: load persisted entries (drop rows whose file vanished),
    /// sweep partial-download temps, and start the coalesced flush task
    /// plus the reaper loop (60 s — inactive-expiry and max-size LRU run
    /// in production, matching the ADR-0002 `tick()` interface instead of
    /// only ever being driven by tests).
    pub async fn load_and_start(self: &Arc<Self>) {
        self.load_and_start_with(std::time::Duration::from_secs(60)).await;
    }

    /// [`Cache::load_and_start`] with an injectable reaper interval (tests
    /// shorten it so expiry assertions don't wait on the production spacing).
    pub async fn load_and_start_with(self: &Arc<Self>, reaper_interval: std::time::Duration) {
        // Startup self-heal (spec §3.10): temp files from crashed downloads.
        let _ = store::cleanup_tmps(&self.config.cache_dir);

        // Coverage rebuild (P2-a): staged segments regroup into the ledger
        // (segment files authoritative; orphans already swept by scan).
        let (ledger, staged) = store::scan_segments(&self.config.cache_dir, self.clock.now_millis());
        *self.coverage.lock().await = ledger;

        let persisted = self.meta.load_all().await.unwrap_or_default();
        let mut state = self.state.write().await;
        state.segment_bytes = staged;
        for m in persisted {
            let path = store::file_path(&self.config.cache_dir, &m.key);
            if tokio::fs::metadata(&path).await.is_ok() {
                state.total_bytes += m.size_bytes;
                state.entries.insert(m.key.clone(), m);
            } else {
                // File lost while we were down — drop the row too.
                let _ = self.meta.remove(&m.key).await;
            }
        }

        // Coalesced access-clock flusher: at most one redb write per second
        // no matter the hit rate (R1: fsync is the bottleneck).
        let dirty = Arc::clone(&self.dirty_access);
        let meta = Arc::clone(&self.meta);
        let state = Arc::clone(&self.state);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(1000));
            loop {
                tick.tick().await;
                let batch = dirty.drain().await;
                if batch.is_empty() {
                    continue;
                }
                // One write stretch folds the whole batch into memory
                // (P1: the hit path no longer touches this lock) ...
                {
                    let mut s = state.write().await;
                    for (key, ms) in &batch {
                        if let Some(m) = s.entries.get_mut(key) {
                            m.last_access_millis = *ms;
                        }
                    }
                }
                // ... then ONE redb transaction for the whole batch (P5).
                if let Err(e) = meta.bump_last_access_batch(&batch).await {
                    tracing::warn!(error = %e, "access-clock flush failed");
                }
            }
        });

        // Reaper loop: `tick()` drives inactive expiry + max-size LRU, and
        // until now only tests ever called it — a deployed binary never
        // reaped anything. Run it on a fixed interval (ADR-0002: tick is
        // part of the Cache interface, not a test-only affordance).
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(reaper_interval);
            loop {
                tick.tick().await;
                this.tick().await;
            }
        });
    }

    /// Resolve a request path to serving coordinates (C2: the single
    /// upstream-resolution seam — bucket alias + prefix routes + validation).
    pub fn resolve(&self, raw_path: &str) -> Result<ResolvedKey, crate::key::KeyError> {
        resolve_key(raw_path, &self.routes, &self.backends.ids())
    }

    /// Owned view of the live machinery for healthz: operators read a
    /// snapshot, never the internals (C4).
    pub async fn snapshot(&self) -> CacheSnapshot {
        let (entries, total_bytes, segment_bytes) = {
            let s = self.state.read().await;
            (s.entries.len(), s.total_bytes, s.segment_bytes)
        };
        CacheSnapshot {
            entries,
            total_bytes,
            segment_bytes,
            flights_active: self.flights.active().await,
            promotions_active: self.promotions.lock().await.len(),
            dirty_access_pending: self.dirty_access.pending(),
        }
    }

    /// Whether any entry row (positive or negative tombstone) exists.
    pub async fn entry_exists(&self, key: &str) -> bool {
        self.state.read().await.entries.contains_key(key)
    }

    /// Relief-valve link lookup (A: redirect cold misses): bounded
    /// `direct_url` against the routed upstream. `None` = unknown upstream
    /// or the budget blew; the caller owns target validation.
    pub async fn direct_url_bounded(
        &self,
        upstream_id: &str,
        backend_key: &str,
        viewer_ua: Option<&str>,
        budget: std::time::Duration,
    ) -> Option<DirectUrl> {
        let slot = self.backends.get(upstream_id)?;
        let key = Key::from_validated(backend_key.to_string());
        tokio::time::timeout(budget, slot.backend.direct_url(&key, viewer_ua))
            .await
            .ok()?
            .ok()
    }

    /// HEAD-grade metadata lookup: memory entry when fresh, else a single
    /// upstream `stat` (a HEAD must be current — stale rows still cost one
    /// stat, but bytes never move: no flights, no file reads, no file-row
    /// installs; a confirmed absence installs a negative tombstone so
    /// HEAD 404s share the negative-cache window with GET).
    pub async fn head_meta(&self, raw_key: &str) -> Result<HitMeta, BackendError> {
        let rk = self.resolve(raw_key).map_err(|e| BackendError::Other(format!("invalid key: {e}")))?;
        self.head_resolved(&rk).await
    }

    /// [`Cache::head_meta`] for a pre-resolved key (no re-validation:
    /// [`ResolvedKey`] is valid by construction).
    pub async fn head_resolved(&self, rk: &ResolvedKey) -> Result<HitMeta, BackendError> {
        let key = rk.cache_key.clone();
        let backend_key = rk.backend_key.clone();
        let upstream_id = rk.upstream_id.clone();
        let now = self.clock.now_millis();
        // Nocache profile: pure stat, zero cache state (no memory rows, no
        // tombstones, no access-clock bumps).
        if self.config.cache_profile(&upstream_id).nocache {
            let slot = self
                .backends
                .get(&upstream_id)
                .ok_or_else(|| BackendError::Other(format!("unknown upstream {upstream_id}")))?;
            let _permit = slot.gate.acquire().await;
            let m = slot.backend.stat(&Key::from_validated(backend_key)).await?;
            return Ok(hit_meta_remote(&key, &m));
        }
        {
            let s = self.state.read().await;
            if let Some(meta) = s.entries.get(&key) {
                if meta.is_negative(now) {
                    return Err(BackendError::NotFound);
                }
                if meta.negative_until_millis.is_none() {
                    // Fresh enough to serve from memory: a HEAD must be
                    // current, so a stale row still costs one stat (bytes
                    // never move — flights and file reads stay untouched).
                    let age = now.saturating_sub(meta.last_revalidated_millis.unwrap_or(meta.created_at_millis));
                    if age <= self.config.revalidate_ttl_secs * 1000 {
                        let hdrs = EntryHeaders {
                            size_bytes: meta.size_bytes,
                            etag: meta.etag.clone(),
                            last_modified: meta.last_modified.clone(),
                            content_type: meta.content_type.clone(),
                            negative: false,
                        };
                        let hit = hit_meta_entry(&key, &hdrs);
                        drop(s);
                        self.bump_last_access(&key).await;
                        return Ok(hit);
                    }
                }
                // Stale row or expired tombstone: fall through to stat.
            }
        }
        let slot = self
            .backends
            .get(&upstream_id)
            .ok_or_else(|| BackendError::Other(format!("unknown upstream {upstream_id}")))?;
        let _permit = slot.gate.acquire().await;
        match slot.backend.stat(&Key::from_validated(backend_key)).await {
            Ok(m) => {
                self.bump_last_access(&key).await;
                Ok(hit_meta_remote(&key, &m))
            }
            Err(BackendError::NotFound) => {
                self.install_negative(&key, &upstream_id).await;
                Err(BackendError::NotFound)
            }
            Err(e) => Err(e),
        }
    }

    /// Pure memory peek at a cached entry's size: no upstream call, no
    /// state mutation. Used only for best-effort `Content-Range: bytes
    /// */size` hints on 416 responses (SHOULD-level per R1).
    pub(crate) async fn memory_size(&self, raw_key: &str) -> Option<u64> {
        let key = validate_key(raw_key).ok()?;
        let s = self.state.read().await;
        let m = s.entries.get(&key)?;
        if m.negative_until_millis.is_some() {
            return None;
        }
        Some(m.size_bytes)
    }

    /// Whether a fresh (no revalidation due) memory entry exists: the
    /// hit-first gate for the A relief valve. Pure peek — no upstream,
    /// no mutation, no flights.
    pub(crate) async fn memory_hit_fresh(&self, raw_key: &str) -> bool {
        let key = match validate_key(raw_key) {
            Ok(k) => k,
            Err(_) => return false,
        };
        let now = self.clock.now_millis();
        let s = self.state.read().await;
        match s.entries.get(&key) {
            Some(m) if m.negative_until_millis.is_none() => {
                let age = now.saturating_sub(m.last_revalidated_millis.unwrap_or(m.created_at_millis));
                age <= self.config.revalidate_ttl_secs * 1000
            }
            _ => false,
        }
    }

    /// Background fill: full fetch + drain, no client attached. Powers the
    /// A relief valve (307 now, bytes later) and shares the prewarm path —
    /// one primitive, two callers. Nocache upstreams have nothing to fill
    /// (zero-disk contract), so prefetch is a no-op there.
    pub async fn prefetch(&self, rk: &ResolvedKey) -> Result<(), BackendError> {
        if self.config.cache_profile(&rk.upstream_id).nocache {
            return Ok(());
        }
        let mut hit = self.get_resolved(rk, None).await?;
        flight::drain(&mut hit.body).await?;
        Ok(())
    }

    /// Nocache profile (small-footprint nodes): pure water-pipe — stat for
    /// headers, ranged open, bytes stream origin-to-viewer with **zero
    /// disk writes**: no entries, no segments, no redb rows, no negative
    /// tombstones, no coverage ledger. Every failure is the caller's
    /// fallback problem, exactly like `serve_passthrough`.
    pub async fn serve_nocache(
        &self,
        rk: &ResolvedKey,
        range: Option<crate::backend::ByteRange>,
    ) -> Result<PassthroughHit, BackendError> {
        let start = std::time::Instant::now();
        let out = self.serve_nocache_inner(rk, range).await;
        crate::metrics::observe_serve(if out.is_ok() { "nocache" } else { "nocache_error" }, start);
        out
    }

    async fn serve_nocache_inner(
        &self,
        rk: &ResolvedKey,
        range: Option<crate::backend::ByteRange>,
    ) -> Result<PassthroughHit, BackendError> {
        let slot = self
            .backends
            .get(&rk.upstream_id)
            .ok_or_else(|| BackendError::Other(format!("unknown upstream {}", rk.upstream_id)))?;
        let _permit = slot.gate.acquire().await;
        let bkey = Key::from_validated(rk.backend_key.clone());
        let meta = slot.backend.stat(&bkey).await?;
        let total = meta.size_bytes;
        let (start, end) = match range {
            None => (0, total),
            Some(r) => {
                if r.offset >= total {
                    return Err(BackendError::RangeNotSatisfiable);
                }
                (r.offset, r.length.map_or(total, |l| (r.offset + l).min(total)))
            }
        };
        let src = slot.backend.open(&bkey, range).await?;
        let content_range = range.map(|_| ContentRange { first: start, last: end.saturating_sub(1), total });
        let content_length = Some(end.saturating_sub(start));
        let mut src_stream = src.stream;
        let body: BodyStream = Box::pin(async_stream::try_stream! {
            use tokio::io::AsyncReadExt;
            let mut buf = vec![0u8; 256 * 1024];
            loop {
                let n = src_stream.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                yield bytes::Bytes::copy_from_slice(&buf[..n]);
            }
        });
        Ok(PassthroughHit {
            meta: hit_meta_remote(&rk.cache_key, &meta),
            etag: meta.etag,
            total,
            content_range,
            content_length,
            body,
        })
    }

    /// C-path response (efficientcache): origin bytes streamed straight to
    /// the viewer with zero cache machinery — no flight, no tmp/seal, no
    /// entry, no revalidation. The served interval is staged as a sidecar
    /// segment so coverage-triggered promotion (P2-b) can reuse it; the
    /// ledger merge happens only on successful exhaustion (aborts leave
    /// `.segpart` orphans for the sweeper). Small files (`size <
    /// min_file_size`) and every failure fall back to the B path in the
    /// caller — this method never serves from disk.
    pub async fn serve_passthrough(
        &self,
        rk: &ResolvedKey,
        range: Option<crate::backend::ByteRange>,
        min_file_size: u64,
    ) -> Result<PassthroughHit, BackendError> {
        let start = std::time::Instant::now();
        let out = self.serve_passthrough_inner(rk, range, min_file_size).await;
        crate::metrics::observe_serve(
            if out.is_ok() { "passthrough" } else { "passthrough_error" },
            start,
        );
        out
    }

    async fn serve_passthrough_inner(
        &self,
        rk: &ResolvedKey,
        range: Option<crate::backend::ByteRange>,
        min_file_size: u64,
    ) -> Result<PassthroughHit, BackendError> {
        let slot = self
            .backends
            .get(&rk.upstream_id)
            .ok_or_else(|| BackendError::Other(format!("unknown upstream {}", rk.upstream_id)))?;
        // Negative tombstones bind the C path too (no origin hammering).
        {
            let s = self.state.read().await;
            if let Some(m) = s.entries.get(&rk.cache_key) {
                if m.is_negative(self.clock.now_millis()) {
                    return Err(BackendError::NotFound);
                }
            }
        }
        // Stat single-flight: concurrent cold passthroughs for
        // the same key must coalesce to ONE upstream stat. The flight runs
        // OUTSIDE the gate: the gate (concurrency 3) would otherwise
        // serialize the stat calls and the flight cell would be removed
        // between permits — 50 concurrent requests would stat 50 times.
        let bkey = Key::from_validated(rk.backend_key.clone());
        let meta = match self
            .reval_inflight
            .run(format!("stat:{}", rk.cache_key), || {
                let slot = Arc::clone(&slot);
                let k = bkey.clone();
                async move {
                    slot.backend.stat(&k).await.map(|meta| StatData { meta })
                }
            })
            .await
        {
            Ok(s) => s.meta,
            Err(BackendError::NotFound) => {
                self.install_negative(&rk.cache_key, &rk.upstream_id).await;
                return Err(BackendError::NotFound);
            }
            Err(e) => return Err(e),
        };
        let _permit = slot.gate.acquire().await;
        // Version gate: a flipped object restarts staged history BEFORE
        // serving, so new bytes land on a clean ledger (finalize keeps a
        // same-file backstop for races).
        {
            let known = self.coverage.lock().await.get(&rk.cache_key).and_then(|e| e.etag.clone());
            let changed = match (known.as_deref(), meta.etag.as_deref()) {
                (Some(a), Some(b)) => a != b,
                _ => false,
            };
            if changed {
                reset_coverage(&self.coverage, &self.state, &self.config.cache_dir, &rk.cache_key).await;
            }
        }
        if meta.size_bytes < min_file_size {
            return Err(BackendError::Other("below min_file_size".into()));
        }
        let (start, end) = match range {
            None => (0, meta.size_bytes),
            Some(r) => {
                if r.offset >= meta.size_bytes {
                    return Err(BackendError::RangeNotSatisfiable);
                }
                (r.offset, r.length.map_or(meta.size_bytes, |l| (r.offset + l).min(meta.size_bytes)))
            }
        };
        let src = match slot.backend.open(&bkey, range).await {
            Ok(s) => s,
            Err(BackendError::NotFound) => {
                self.install_negative(&rk.cache_key, &rk.upstream_id).await;
                return Err(BackendError::NotFound);
            }
            Err(e) => return Err(e),
        };
        let total = meta.size_bytes;
        let etag = meta.etag.clone();
        let meta_out = hit_meta_remote(&rk.cache_key, &meta);
        let content_range = range.map(|_| ContentRange { first: start, last: end.saturating_sub(1), total });
        let content_length = Some(end.saturating_sub(start));

        // Staged streaming: chunk → segpart file → viewer. Ledger merge +
        // seal rename happen only on exhaustion, so aborts are sweep-safe.
        let coverage = Arc::clone(&self.coverage);
        let state = Arc::clone(&self.state);
        let clock = Arc::clone(&self.clock);
        let config = Arc::clone(&self.config);
        let backends = self.backends.clone();
        let meta_store = Arc::clone(&self.meta);
        let promotions = Arc::clone(&self.promotions);
        let cache_dir = self.config.cache_dir.clone();
        let cache_key = rk.cache_key.clone();
        let backend_key = rk.backend_key.clone();
        let upstream_id = rk.upstream_id.clone();
        let segpart = store::segpart_path(&cache_dir, &cache_key, start, end);
        let mut src_stream = src.stream;
        // The upstream 206 stream may not signal EOF at the Content-Length
        // boundary (keep-alive reuse, e.g. rclone serve webdav): read at
        // most `end - start` bytes, then seal. Waiting for EOF would hang
        // the staging loop and leave the segpart unsealed forever.
        let want = end.saturating_sub(start);
        // Viewer disconnect drops the whole body stream (axum drops the
        // async block), so the seal code below never runs on abort. A
        // detached watcher polls the segpart and seals it once it stops
        // growing — the served bytes still count toward coverage
        // (segmented downloads are separate connections).
        let watcher = {
            let segpart = segpart.clone();
            let seg = store::seg_path(&cache_dir, &cache_key, start, end);
            let coverage = Arc::clone(&coverage);
            let state = Arc::clone(&state);
            let clock = Arc::clone(&clock);
            let config = Arc::clone(&config);
            let backends = backends.clone();
            let meta_store = Arc::clone(&meta_store);
            let promotions = Arc::clone(&promotions);
            let cache_dir = cache_dir.clone();
            let cache_key = cache_key.clone();
            let backend_key = backend_key.clone();
            let upstream_id = upstream_id.clone();
            let etag = etag.clone();
            let total = total;
            let start = start;
            let end = end;
            tokio::spawn(async move {
                // Wait for the segpart to appear and stop growing (viewer
                // gone or transfer done), then seal it.
                let mut last = 0u64;
                let mut stable = 0u32;
                for _ in 0..600 {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    let size = tokio::fs::metadata(&segpart).await.map(|m| m.len()).unwrap_or(0);
                    if size == 0 {
                        continue; // not started yet
                    }
                    if size == last {
                        stable += 1;
                        if stable >= 3 {
                            // Sealed by the stream itself already?
                            if tokio::fs::metadata(&seg).await.is_ok() {
                                return;
                            }
                            // Seal it here.
                            let _ = tokio::fs::rename(&segpart, &seg).await;
                            let now = clock.now_millis();
                            let span = FinalizedSpan {
                                cache_dir: cache_dir.clone(),
                                key: cache_key.clone(),
                                backend_key: backend_key.clone(),
                                upstream_id: upstream_id.clone(),
                                etag: etag.clone(),
                                total,
                                start,
                                end: start + size,
                                bytes: size,
                                now_millis: now,
                            };
                            finalize_coverage(&coverage, &state, span, window_millis_for(&config, &upstream_id)).await;
                            maybe_promote(
                                &coverage,
                                &config,
                                &backends,
                                &meta_store,
                                &promotions,
                                &state,
                                &cache_dir,
                                &cache_key,
                                &upstream_id,
                                now,
                            )
                            .await;
                            return;
                        }
                    } else {
                        stable = 0;
                    }
                    last = size;
                }
            })
        };
        let body: BodyStream = Box::pin(async_stream::try_stream! {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = vec![0u8; 256 * 1024];
            let mut file: Option<tokio::fs::File> = None;
            let mut written: u64 = 0;
            while written < want {
                let n = src_stream.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                if file.is_none() {
                    if let Some(parent) = segpart.parent() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                    file = Some(tokio::fs::File::create(&segpart).await?);
                }
                file.as_mut().unwrap().write_all(&buf[..n]).await?;
                written += n as u64;
                yield bytes::Bytes::copy_from_slice(&buf[..n]);
            }
            if written > 0 {
                drop(file);
                let seg = store::seg_path(&cache_dir, &cache_key, start, start + written);
                let _ = tokio::fs::rename(&segpart, &seg).await;
                let now = clock.now_millis();
                let span = FinalizedSpan {
                    cache_dir: cache_dir.clone(),
                    key: cache_key.clone(),
                    backend_key: backend_key.clone(),
                    upstream_id: upstream_id.clone(),
                    etag,
                    total,
                    start,
                    end: start + written,
                    bytes: written,
                    now_millis: now,
                };
                finalize_coverage(&coverage, &state, span, window_millis_for(&config, &upstream_id)).await;
                // Coverage-triggered promotion (P2-b): threshold met →
                // background assemble + seal. Fire-and-forget by design.
                maybe_promote(
                    &coverage,
                    &config,
                    &backends,
                    &meta_store,
                    &promotions,
                    &state,
                    &cache_dir,
                    &cache_key,
                    &upstream_id,
                    now,
                )
                .await;
            } else if file.is_some() {
                let _ = tokio::fs::remove_file(&segpart).await;
            }
        });
        let _ = watcher;
        Ok(PassthroughHit { meta: meta_out, etag: meta.etag, total, content_range, content_length, body })
    }

    /// Main entry: `GET /<key>` — streaming response. Cold misses attach
    /// to a shared download flight; hits stream the cached file; revalidation
    /// stats the upstream (no bytes) and compares etags. A Range request on
    /// a cached file slices locally; on a cold miss with offset > 0 the
    /// reader converges on the flight's growing temp file and waits for the
    /// writer to reach its offset (single stream — every upstream open pays
    /// a ~800 ms fixed cost).
    pub async fn get(
        &self,
        raw_key: &str,
        range: Option<crate::backend::ByteRange>,
    ) -> Result<CacheHit, BackendError> {
        let rk = self.resolve(raw_key).map_err(|e| BackendError::Other(format!("invalid key: {e}")))?;
        self.get_resolved(&rk, range).await
    }

    /// [`Cache::get`] for a pre-resolved key (no re-validation).
    ///
    /// Two key namespaces: [`ResolvedKey::cache_key`] is the cache identity
    /// (full request path — state rows, flights, store paths, reval labels);
    /// [`ResolvedKey::backend_key`] is the provider-side object path. They
    /// coincide for legacy routing; they differ only for the bucket alias.
    /// Sharing one namespace would collide flights and entries across
    /// upstreams, so the split is load-bearing, not cosmetic.
    pub async fn get_resolved(
        &self,
        rk: &ResolvedKey,
        range: Option<crate::backend::ByteRange>,
    ) -> Result<CacheHit, BackendError> {
        let start = std::time::Instant::now();
        let out = self.get_resolved_inner(rk, range).await;
        let label = match &out {
            Ok(hit) => format!("{:?}", hit.outcome),
            Err(_) => "error".to_string(),
        };
        crate::metrics::observe_serve(&label, start);
        out
    }

    async fn get_resolved_inner(
        &self,
        rk: &ResolvedKey,
        range: Option<crate::backend::ByteRange>,
    ) -> Result<CacheHit, BackendError> {
        let key = rk.cache_key.clone();
        let backend_key = rk.backend_key.clone();
        let upstream_id = rk.upstream_id.clone();
        let slot = self
            .backends
            .get(&upstream_id)
            .ok_or_else(|| BackendError::Other(format!("unknown upstream {upstream_id}")))?;
        let now = self.clock.now_millis();

        // One read pass answers all three questions the hit path asks
        // (P1: negative tombstone? revalidate due? what etag is on file?)
        // instead of taking the read lock three separate times.
        let (needs_revalidate, cached_etag) = {
            let s = self.state.read().await;
            match s.entries.get(&key) {
                Some(meta) => {
                    if meta.is_negative(now) {
                        return Err(BackendError::NotFound);
                    }
                    if meta.negative_until_millis.is_some() {
                        (false, None)
                    } else {
                        let age = now
                            .saturating_sub(meta.last_revalidated_millis.unwrap_or(meta.created_at_millis));
                        (age > self.config.revalidate_ttl_secs * 1000, meta.etag.clone())
                    }
                }
                None => (false, None),
            }
        };

        if !needs_revalidate {
            match self.serve_from_disk(&key, CacheOutcome::Hit, range).await? {
                Some(hit) => {
                    self.bump_last_access(&key).await;
                    return Ok(hit);
                }
                None => {}
            }
        } else {
            // Revalidation = stat + etag compare (G2 #11: Drive has no 304;
            // stat-compare is provider-uniform and costs no bytes).
            let stat = self
                .reval_inflight
                .run(format!("reval:{key}"), || {
                    let slot = Arc::clone(&slot);
                    let k = Key::from_validated(backend_key.clone());
                    async move {
                        let _permit = slot.gate.acquire().await;
                        slot.backend.stat(&k).await.map(|meta| StatData { meta })
                    }
                })
                .await;
            match stat {
                Ok(stat) if cached_etag.is_some() && cached_etag == stat.meta.etag => {
                    self.bump_last_access(&key).await;
                    match self.serve_from_disk(&key, CacheOutcome::Revalidated, range).await? {
                        Some(hit) => return Ok(hit),
                        None => {}
                    }
                }
                Ok(_) => {
                    // Modified (or etag vanished): forced refetch below; the
                    // old file keeps serving other readers until the rename.
                    return self.forced_fetch(slot, key, backend_key, upstream_id, range).await;
                }
                Err(BackendError::NotFound) => {
                    self.install_negative(&key, &upstream_id).await;
                    return Err(BackendError::NotFound);
                }
                Err(e) => {
                    match self.serve_from_disk(&key, CacheOutcome::Stale, range).await? {
                        Some(hit) => return Ok(hit),
                        None => return Err(e),
                    }
                }
            }
        }

        // Cold miss: attach-or-create the shared download flight.
        // The flight is namespaced by cache key; the driver fetches the
        // provider-side object path.
        let backend_key = Key::from_validated(backend_key);
        let flight = self.attach_or_start(&key, backend_key.clone(), &upstream_id, Arc::clone(&slot)).await;
        self.await_flight(flight, &key, &upstream_id, range).await
    }

    /// Serve a complete cached file from disk, if both file and meta exist.
    /// Returns Err(RangeNotSatisfiable) when a Range cannot be satisfied.
    async fn serve_from_disk(
        &self,
        key: &str,
        outcome: CacheOutcome,
        range: Option<crate::backend::ByteRange>,
    ) -> Result<Option<CacheHit>, BackendError> {
        let m = match self.entry_meta(key).await {
            Some(m) if !m.negative => m,
            _ => return Ok(None),
        };
        let path = store::file_path(&self.config.cache_dir, key);
        if tokio::fs::metadata(&path).await.is_err() {
            return Ok(None);
        }
        let size = m.size_bytes;
        let (offset, len, content_range) = match range {
            None => (0, size, None),
            Some(r) => {
                if r.offset >= size {
                    return Err(BackendError::RangeNotSatisfiable);
                }
                let end = r.length.map_or(size, |l| (r.offset + l).min(size));
                (
                    r.offset,
                    end - r.offset,
                    Some(ContentRange { first: r.offset, last: end - 1, total: size }),
                )
            }
        };
        Ok(Some(CacheHit {
            outcome,
            meta: hit_meta_entry(key, &m),
            content_range,
            content_length: Some(len),
            body: flight::file_body(path, offset, len),
        }))
    }

    /// Attach to an existing flight for this key, or create one and spawn
    /// its driver. Joining (insert-before-await) and map hygiene live in
    /// the flight module; the driver closure is the Cache's policy.
    async fn attach_or_start(
        &self,
        key: &str,
        backend_key: Key,
        upstream_id: &str,
        slot: Arc<BackendSlot>,
    ) -> Arc<FlightShared> {
        let entry_key = key.to_string();
        let driver_up = upstream_id.to_string();
        let cfg = Arc::clone(&self.config);
        let state = Arc::clone(&self.state);
        let meta_store = Arc::clone(&self.meta);
        let clock = Arc::clone(&self.clock);
        self.flights
            .join_or_start(
                key,
                store::tmp_path(&self.config.cache_dir, key),
                store::file_path(&self.config.cache_dir, key),
                move |f| drive_flight(f, slot, backend_key, entry_key, driver_up, cfg, state, meta_store, clock),
            )
            .await
    }

    /// Wait for a flight's metadata, then return the streaming body.
    /// Range handling: offset 0 → the growing body itself (it starts at
    /// byte 0); offset > 0 → the reader follows the flight's growing temp
    /// file from that offset and waits for the writer to reach it (single
    /// stream, no second upstream open). Failed flights fall back to
    /// stale-if-error.
    async fn await_flight(
        &self,
        flight: Arc<FlightShared>,
        key: &str,
        upstream_id: &str,
        range: Option<crate::backend::ByteRange>,
    ) -> Result<CacheHit, BackendError> {
        let mut rx = flight.subscribe();
        loop {
            let st = rx.borrow().clone();
            match st {
                FlightProgress::Meta(meta) => {
                    self.bump_last_access(key).await;
                    let meta_out = hit_meta_remote(key, &meta);
                    match range {
                        None => {
                            return Ok(CacheHit {
                                outcome: CacheOutcome::Miss,
                                meta: meta_out,
                                content_range: None,
                                content_length: Some(meta.size_bytes),
                                body: flight::growing_reader(flight),
                            });
                        }
                        Some(r) => {
                            // Converge on the flight instead of opening a
                            // second upstream connection. Each upstream open
                            // costs ~800 ms and N concurrent ranges used to
                            // mean N opens (measured: 5 -> 5). The reader
                            // waits for the writer to reach this offset; a
                            // cold seek costs only the full pull it needed
                            // anyway, and EdgeOne delivers shards in
                            // ascending order so the wait is normally zero.
                            // (Offset 0 with a bounded length is the same
                            // path — `growing_reader_from(0, want)` — so a
                            // `bytes=0-N` shard never gets promised the
                            // whole file.)
                            if r.offset >= meta.size_bytes {
                                return Err(BackendError::RangeNotSatisfiable);
                            }
                            let end = r
                                .length
                                .map_or(meta.size_bytes.saturating_sub(1), |l| (r.offset + l - 1).min(meta.size_bytes - 1));
                            let want = end.saturating_sub(r.offset).saturating_add(1);
                            return Ok(CacheHit {
                                outcome: CacheOutcome::Miss,
                                meta: meta_out,
                                content_range: Some(ContentRange {
                                    first: r.offset,
                                    last: end,
                                    total: meta.size_bytes,
                                }),
                                content_length: Some(want),
                                body: flight::growing_reader_from(flight, r.offset, Some(want)),
                            });
                        }
                    }
                }
                FlightProgress::Done => {
                    // Late attacher: the whole flight finished before we
                    // subscribed (watch keeps only the latest value). The
                    // file is sealed and its meta installed — serve disk.
                    if let Some(hit) = self.serve_from_disk(key, CacheOutcome::Miss, range).await? {
                        return Ok(hit);
                    }
                    return Err(BackendError::Other("flight done but entry missing".into()));
                }
                FlightProgress::Failed(e) => {
                    match self.serve_from_disk(key, CacheOutcome::Stale, range).await? {
                        Some(hit) => return Ok(hit),
                        None => {}
                    }
                    if matches!(e, BackendError::NotFound) {
                        self.install_negative(key, upstream_id).await;
                    }
                    return Err(e);
                }
                _ => {
                    // A stalled driver (or a panic that never publishes a
                    // terminal state) must not park metadata-waiters
                    // forever: bounded by the flight's stall budget.
                    let budget = flight.stall_budget;
                    match tokio::time::timeout(budget, rx.changed()).await {
                        Ok(Ok(())) => {}
                        Ok(Err(_)) => {
                            return Err(BackendError::Other("flight ended without metadata".into()));
                        }
                        Err(_) => {
                            return Err(BackendError::Other(format!(
                                "flight stalled before metadata: no progress for {budget:?}"
                            )));
                        }
                    }
                }
            }
        }
    }

    /// Revalidation found a changed etag: refetch on a private flight (no
    /// map entry) while the old file keeps serving everyone else; the seal
    /// rename swaps it atomically.
    async fn forced_fetch(
        &self,
        slot: Arc<BackendSlot>,
        key: String,
        backend_key: String,
        upstream_id: String,
        range: Option<crate::backend::ByteRange>,
    ) -> Result<CacheHit, BackendError> {
        let driver_up = upstream_id.clone();
        let entry_key = key.clone();
        let backend_key = Key::from_validated(backend_key);
        let cfg = Arc::clone(&self.config);
        let state = Arc::clone(&self.state);
        let meta_store = Arc::clone(&self.meta);
        let clock = Arc::clone(&self.clock);
        let flight = self.flights.spawn_solo(
            store::tmp_path(&self.config.cache_dir, &key),
            store::file_path(&self.config.cache_dir, &key),
            move |f| drive_flight(f, slot, backend_key, entry_key, driver_up, cfg, state, meta_store, clock),
        );
        self.await_flight(flight, &key, &upstream_id, range).await
    }

    /// Header-relevant slice of an entry row (P1): avoids cloning the long
    /// `key`/`upstream_id` Strings on every disk hit.
    async fn entry_meta(&self, key: &str) -> Option<EntryHeaders> {
        self.state.read().await.entries.get(key).map(|m| EntryHeaders {
            size_bytes: m.size_bytes,
            etag: m.etag.clone(),
            last_modified: m.last_modified.clone(),
            content_type: m.content_type.clone(),
            negative: m.negative_until_millis.is_some(),
        })
    }

    /// Stamp an access without taking the state write lock (P1): the hit
    /// path only touches the sharded [`AccessClock`]; the flusher folds it
    /// into memory and redb within one tick.
    async fn bump_last_access(&self, key: &str) {
        self.dirty_access.touch(key, self.clock.now_millis()).await;
    }

    async fn install_negative(&self, key: &str, upstream_id: &str) {
        let now = self.clock.now_millis();
        let until = now + self.config.negative_ttl_secs * 1000;
        let entry = EntryMeta {
            version: 1,
            upstream_id: upstream_id.to_string(),
            key: key.to_string(),
            size_bytes: 0,
            etag: None,
            last_modified: None,
            content_type: None,
            created_at_millis: now,
            last_access_millis: now,
            last_revalidated_millis: None,
            negative_until_millis: Some(until),
        };
        let _ = self.meta.insert(&entry).await;
        let mut s = self.state.write().await;
        s.entries.insert(key.to_string(), entry);
    }

    /// Drive both reapers: inactive expiry + max_size LRU. Called by `tick()`.
    /// Lock discipline: the state guard and the coverage mutex are never
    /// held together (finalize takes them coverage→state; inverting here
    /// would deadlock).
    pub async fn tick(&self) {
        let now = self.clock.now_millis();
        let ttl_ms = self.config.inactive_ttl_secs * 1000;
        // 1. Decide expiry under each lock separately.
        let do_sweep = {
            let s = self.state.read().await;
            now.saturating_sub(s.segment_sweep_at_millis) >= ttl_ms
        };
        let expired: Vec<String> = if do_sweep {
            let cov = self.coverage.lock().await;
            cov.iter()
                .filter(|(_, c)| now.saturating_sub(c.last_touch_millis) >= ttl_ms)
                .map(|(k, _)| k.clone())
                .collect()
        } else {
            Vec::new()
        };
        // 2. Filesystem deletes hold no locks — and run off the async
        // runtime (blocking read_dir/remove_file in spawn_blocking).
        let freed = {
            let cache_dir = self.config.cache_dir.clone();
            let expired = expired.clone();
            tokio::task::spawn_blocking(move || {
                // One top-level index for the whole batch (P4): the old
                // shape walked the entire cache once per expired key.
                let index = store::segment_index(&cache_dir);
                let mut freed: u64 = 0;
                for key in &expired {
                    if let Some(paths) = index.get(key) {
                        for path in paths {
                            freed += std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
                            let _ = std::fs::remove_file(path);
                        }
                    }
                    let _ = std::fs::remove_file(store::segmeta_path(&cache_dir, key));
                }
                if do_sweep {
                    store::sweep_segparts(&cache_dir, ttl_ms, now);
                    store::sweep_orphan_metas(&cache_dir);
                }
                freed
            })
            .await
            .unwrap_or(0)
        };
        // 3. State mutation — memory only, no awaits under the write
        // guard (C3); deletes for reaped/evicted rows run guard-free.
        let reaped = {
            let mut s = self.state.write().await;
            let reaped = reap_collect(&mut s, ttl_ms, now);
            if do_sweep {
                s.segment_sweep_at_millis = now;
                s.segment_bytes = s.segment_bytes.saturating_sub(freed);
            }
            reaped
        };
        remove_entries(&self.config, &self.meta, &reaped).await;
        let evicted = {
            let mut s = self.state.write().await;
            evict_pick(&mut s, &self.config)
        };
        remove_entries(&self.config, &self.meta, &evicted).await;
        // 4. Ledger removal (coverage only).
        if do_sweep {
            let mut cov = self.coverage.lock().await;
            for key in &expired {
                cov.remove(key);
            }
        }
    }
}

/// Coverage window in millis for an upstream: 0 = no decay.
fn window_millis_for(config: &Config, upstream_id: &str) -> u64 {
    config.cache_profile(upstream_id).coverage_window_secs * 1000
}

/// Merge one completed staged interval into the coverage ledger (the only
/// writer besides the startup scan). Etag-locked: a version change with
/// history present resets (drops staged files + ledger) so promotion can
/// never assemble a mixed-version file. Unknown etags adopt; totals adopt
/// when known. Best-effort fs ops — the scan heals any gap.
struct FinalizedSpan {
    cache_dir: std::path::PathBuf,
    key: String,
    backend_key: String,
    upstream_id: String,
    etag: Option<String>,
    total: u64,
    start: u64,
    end: u64,
    bytes: u64,
    now_millis: u64,
}

async fn finalize_coverage(
    coverage: &Arc<Mutex<HashMap<String, store::Coverage>>>,
    state: &Arc<RwLock<CacheState>>,
    span: FinalizedSpan,
    window_millis: u64,
) {
    {
        let mut cov = coverage.lock().await;
        let entry = cov.entry(span.key.clone()).or_default();
        let version_changed = match (&entry.etag, &span.etag) {
            (Some(a), Some(b)) => a != b,
            _ => false,
        };
        if version_changed {
            // New bytes already sealed above: keep this file, drop the rest.
            let fresh = store::seg_path(&span.cache_dir, &span.key, span.start, span.end);
            remove_key_segments(&span.cache_dir, &span.key, Some(&fresh));
            *entry = store::Coverage::default();
        }
        if span.etag.is_some() {
            entry.etag = span.etag.clone();
        }
        if span.total != 0 {
            entry.total = span.total;
        }
        if !span.backend_key.is_empty() {
            entry.backend_key = span.backend_key.clone();
        }
        if !span.upstream_id.is_empty() {
            entry.upstream_id = span.upstream_id.clone();
        }
        entry.add_interval(span.start, span.end, span.now_millis);
        entry.last_touch_millis = span.now_millis;
        // Window decay: drop intervals whose last read is
        // older than the coverage window, so stale staged bytes stop
        // counting toward promotion. Disk sidecars stay for the sweep.
        if window_millis > 0 {
            entry.decay(span.now_millis, window_millis);
        }
    }
    let meta = store::SegMeta {
        etag: span.etag.clone(),
        total: span.total,
        backend_key: span.backend_key.clone(),
        upstream_id: span.upstream_id.clone(),
    };
    if let Ok(b) = serde_json::to_vec(&meta) {
        let _ = tokio::fs::write(store::segmeta_path(&span.cache_dir, &span.key), b).await;
    }
    state.write().await.segment_bytes += span.bytes;
}

/// Drop a key's completed segments + version marker (etag reset path).
/// In-flight `.segpart.*` files are left alone (concurrent transfers).
fn remove_key_segments(cache_dir: &std::path::Path, key: &str, keep: Option<&std::path::Path>) {
    let esc = store::escape_key(key);
    let prefix = format!(".seg.{esc}.");
    if let Ok(rd) = std::fs::read_dir(cache_dir) {
        for entry in rd.filter_map(|e| e.ok()) {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with(&prefix) {
                if keep.is_some_and(|k| entry.path() == k) {
                    continue;
                }
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }
    let _ = std::fs::remove_file(store::segmeta_path(cache_dir, key));
}

/// Full history reset for one key: drop staged files + version marker +
/// ledger row + accounting. Used on version drift (serve pre-check,
/// finalize backstop, promotion verify) — never assembles mixed versions.
async fn reset_coverage(
    coverage: &Arc<Mutex<HashMap<String, store::Coverage>>>,
    state: &Arc<RwLock<CacheState>>,
    cache_dir: &std::path::Path,
    key: &str,
) {
    let mut freed = 0u64;
    for path in store::key_segment_files(cache_dir, key) {
        freed += std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        let _ = std::fs::remove_file(&path);
    }
    let _ = std::fs::remove_file(store::segmeta_path(cache_dir, key));
    coverage.lock().await.remove(key);
    {
        let mut s = state.write().await;
        s.segment_bytes = s.segment_bytes.saturating_sub(freed);
    }
}

/// Coverage-triggered promotion check (P2-b): runs at the end of every
/// staged C transfer. Threshold met → spawn exactly one promotion task
/// per key (single-flight via `promotions`); anything else → no-op.
/// Lock discipline: no lock held across the spawn.
#[allow(clippy::too_many_arguments)]
async fn maybe_promote(
    coverage: &Arc<Mutex<HashMap<String, store::Coverage>>>,
    config: &Arc<Config>,
    backends: &BackendRegistry,
    meta_store: &Arc<crate::cache::persist::MetaStore>,
    promotions: &Arc<Mutex<HashSet<String>>>,
    state: &Arc<RwLock<CacheState>>,
    cache_dir: &std::path::Path,
    key: &str,
    upstream_id: &str,
    now_millis: u64,
) {
    let prof = config.cache_profile(upstream_id);
    if !prof.efficient {
        return;
    }
    let ready = {
        let mut cov = coverage.lock().await;
        // Window decay backstop: a key with no recent
        // writes must not promote on stale intervals.
        if let Some(entry) = cov.get_mut(key) {
            let window_ms = prof.coverage_window_secs * 1000;
            if window_ms > 0 {
                entry.decay(now_millis, window_ms);
            }
        }
        cov.get(key).and_then(|c| c.ratio()).is_some_and(|r| r >= prof.coverage_threshold)
    };
    if !ready {
        return;
    }
    {
        let mut p = promotions.lock().await;
        if !p.insert(key.to_string()) {
            return;
        }
    }
    let (backends, meta_store, state, coverage, config, cache_dir, key, upstream_id, promotions) = (
        backends.clone(),
        Arc::clone(meta_store),
        Arc::clone(state),
        Arc::clone(coverage),
        Arc::clone(config),
        cache_dir.to_path_buf(),
        key.to_string(),
        upstream_id.to_string(),
        Arc::clone(promotions),
    );
    tokio::spawn(async move {
        promote_key(&backends, &meta_store, &state, &coverage, &config, &cache_dir, &key, &upstream_id, now_millis).await;
        promotions.lock().await.remove(&key);
    });
}

/// Assemble a promoted entry: re-verify the version by fresh stat (abort +
/// reset on ANY drift — never a mixed-version file), copy covered slices
/// from sidecars, fetch gaps by exact Range, seal, install, clean staged
/// history. All failures abort silently (segments stay for a later retry).
#[allow(clippy::too_many_arguments)]
async fn promote_key(
    backends: &BackendRegistry,
    meta_store: &Arc<crate::cache::persist::MetaStore>,
    state: &Arc<RwLock<CacheState>>,
    coverage: &Arc<Mutex<HashMap<String, store::Coverage>>>,
    config: &Arc<Config>,
    cache_dir: &std::path::Path,
    key: &str,
    upstream_id: &str,
    now_millis: u64,
) {
    // Snapshot the ledger (unknown version/size or unmapped keys wait for
    // fresh transfers — conservative by design).
    let cov = {
        match coverage.lock().await.get(key).cloned() {
            Some(c) if !c.backend_key.is_empty() && c.total != 0 && c.etag.is_some() => c,
            _ => return,
        }
    };
    let etag = cov.etag.clone().unwrap();
    let total = cov.total;
    let slot = match backends.get(upstream_id) {
        Some(s) => s,
        None => return,
    };
    let bkey = Key::from_validated(cov.backend_key.clone());
    let _permit = slot.gate.acquire().await;
    let live = match slot.backend.stat(&bkey).await {
        Ok(m) => m,
        Err(_) => return,
    };
    if live.etag != Some(etag) || live.size_bytes != total {
        // Drifted under us: drop staged history, start over.
        reset_coverage(coverage, state, cache_dir, key).await;
        return;
    }
    let tmp = store::tmp_path(cache_dir, key);
    if !assemble_file(&slot, &bkey, cache_dir, key, &cov, &tmp).await {
        let _ = std::fs::remove_file(&tmp);
        return;
    }
    let dest = store::file_path(cache_dir, key);
    if store::install_tmp(&tmp, &dest).is_err() {
        let _ = std::fs::remove_file(&tmp);
        return;
    }
    insert_meta(state, config, meta_store, key, upstream_id, &live, now_millis).await;
    // History is now redundant: drop sidecars + ledger row.
    reset_coverage(coverage, state, cache_dir, key).await;
}

/// Fill `tmp` with the full object: covered slices copied from sidecars,
/// gaps fetched by exact Range. Walks the merged intervals in order; every
/// write is an absolute seek, so order is a courtesy, not a requirement.
async fn assemble_file(
    slot: &Arc<BackendSlot>,
    bkey: &Key,
    cache_dir: &std::path::Path,
    key: &str,
    cov: &store::Coverage,
    tmp: &std::path::Path,
) -> bool {
    use tokio::io::AsyncWriteExt;
    // Index segment files by interval (parsed names only).
    let mut segs: Vec<(u64, u64, std::path::PathBuf)> = Vec::new();
    for path in store::key_segment_files(cache_dir, key) {
        let name = path.file_name().unwrap_or_default().to_string_lossy().into_owned();
        if let Some((_, s, e)) = store::parse_seg_name(&name) {
            segs.push((s, e, path));
        }
    }
    segs.sort();
    let mut out = match tokio::fs::File::create(tmp).await {
        Ok(f) => f,
        Err(_) => return false,
    };
    let mut buf = vec![0u8; 256 * 1024];
    let mut pos = 0u64;
    for &(s, e, _) in cov.intervals.iter() {
        if pos < s && !fetch_gap(slot, bkey, &mut out, &mut buf, pos, s).await {
            return false;
        }
        if !copy_span(&mut out, &segs, &mut buf, s.max(pos), e).await {
            return false;
        }
        pos = pos.max(e);
    }
    if pos < cov.total && !fetch_gap(slot, bkey, &mut out, &mut buf, pos, cov.total).await {
        return false;
    }
    out.flush().await.is_ok()
}

/// Copy one covered span `[a, b)` from whichever sidecars hold it.
async fn copy_span(
    out: &mut tokio::fs::File,
    segs: &[(u64, u64, std::path::PathBuf)],
    buf: &mut [u8],
    mut a: u64,
    b: u64,
) -> bool {
    use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
    while a < b {
        let holder = segs.iter().find(|(s, e, _)| *s <= a && a < *e);
        let (fs, fe, path) = match holder {
            Some(h) => h,
            None => return false,
        };
        let n = (*fe).min(b) - a;
        let mut f = match tokio::fs::File::open(path).await {
            Ok(f) => f,
            Err(_) => return false,
        };
        if f.seek(std::io::SeekFrom::Start(a - fs)).await.is_err() {
            return false;
        }
        let mut remaining = n;
        out.seek(std::io::SeekFrom::Start(a)).await.ok();
        while remaining > 0 {
            let want = remaining.min(buf.len() as u64) as usize;
            let r = match f.read(&mut buf[..want]).await {
                Ok(0) => return false,
                Ok(r) => r,
                Err(_) => return false,
            };
            if out.write_all(&buf[..r]).await.is_err() {
                return false;
            }
            remaining -= r as u64;
            a += r as u64;
        }
    }
    true
}

/// Fetch one missing span by exact Range into `out` at absolute `start`.
async fn fetch_gap(
    slot: &Arc<BackendSlot>,
    bkey: &Key,
    out: &mut tokio::fs::File,
    buf: &mut [u8],
    start: u64,
    end: u64,
) -> bool {
    use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
    let src = match slot.backend.open(bkey, Some(crate::backend::ByteRange::bounded(start, end - start))).await {
        Ok(s) => s,
        Err(_) => return false,
    };
    let mut stream = src.stream;
    if out.seek(std::io::SeekFrom::Start(start)).await.is_err() {
        return false;
    }
    let mut remaining = end - start;
    while remaining > 0 {
        let want = remaining.min(buf.len() as u64) as usize;
        let n = match stream.read(&mut buf[..want]).await {
            Ok(0) => return false, // short backend read: do not seal a short file
            Ok(n) => n,
            Err(_) => return false,
        };
        if out.write_all(&buf[..n]).await.is_err() {
            return false;
        }
        remaining -= n as u64;
    }
    true
}

/// One cold-miss download driver: gate → stat → publish Meta → pump to
/// temp file → seal (rename) → install metadata row (disk + redb) → Done.
/// Detached from its creator so a disconnecting client never kills the
/// download.
async fn drive_flight<C: Clock>(
    flight: Arc<FlightShared>,
    slot: Arc<BackendSlot>,
    backend_key: Key,
    entry_key: String,
    upstream_id: String,
    config: Arc<Config>,
    state: Arc<RwLock<CacheState>>,
    meta_store: Arc<crate::cache::persist::MetaStore>,
    clock: Arc<C>,
) {
    let outcome = async {
        let _permit = slot.gate.acquire().await;
        let meta = slot.backend.stat(&backend_key).await?;
        let _ = flight.progress_tx.send(FlightProgress::Meta(meta.clone()));
        // Upstream fetch is ALWAYS a single stream. Measured on the real
        // upstream (OpenList -> Google Drive, 3.1 GB file):
        //   single stream         71 s   43.8 MB/s
        //   100 MB segments x31  256 s   11 MB/s
        //   5 MB segments x614   292 s   11 MB/s
        // Every segmented variant is ~4x slower because each upstream
        // request pays a ~800 ms fixed stream-open cost (a 1 KB Range
        // request also takes ~820 ms), and concurrency does not stack
        // past ~15 MB/s. One stream pays that cost once.
        //
        // The earlier parallel pump applied a measurement taken on the
        // EdgeOne edge (client -> edge, where large single responses do
        // degrade) to this hop, where the opposite holds. The client
        // still reads a growing stream; only the upstream fetch strategy
        // changed.
        let src = slot.backend.open(&backend_key, None).await?;
        flight::pump_and_seal(src, &flight.tmp_path, &flight.final_path, &flight.progress_tx).await?;
        Ok::<ObjectMeta, BackendError>(meta)
    }
    .await;

    match outcome {
        Ok(meta) => {
            insert_meta(&state, &config, &meta_store, &entry_key, &upstream_id, &meta, clock.now_millis()).await;
            let _ = flight.progress_tx.send(FlightProgress::Done);
        }
        Err(e) => {
            let _ = flight.progress_tx.send(FlightProgress::Failed(e));
        }
    }
}

async fn insert_meta(
    state: &RwLock<CacheState>,
    config: &Config,
    meta_store: &crate::cache::persist::MetaStore,
    key: &str,
    upstream_id: &str,
    meta: &ObjectMeta,
    now: u64,
) {
    // C3 lock discipline: the state write guard never spans a redb commit
    // or a file delete. Order: build the entry under a read, persist
    // guard-free, then one await-free write stretch for accounting +
    // victim selection; the deletes run after the guard drops. Persist
    // first: on crash between redb and memory, startup rebuilds memory
    // from redb; the reverse order would lose the row.
    let (old_size, entry) = {
        let s = state.read().await;
        let old_size = s.entries.get(key).map(|m| m.size_bytes).unwrap_or(0);
        let entry = EntryMeta {
            version: 1,
            upstream_id: upstream_id.to_string(),
            key: key.to_string(),
            size_bytes: meta.size_bytes,
            etag: meta.etag.clone(),
            last_modified: meta.last_modified.clone(),
            // Raw provider hint: MIME resolution happens once, at read time
            // (hit_meta_*), never at write. Old rows holding resolved
            // values re-resolve idempotently (resolve passes specifics through).
            content_type: meta.mime_hint.clone(),
            created_at_millis: s.entries.get(key).map(|m| m.created_at_millis).unwrap_or(now),
            last_access_millis: now,
            last_revalidated_millis: Some(now),
            negative_until_millis: None,
        };
        (old_size, entry)
    };
    if let Err(e) = meta_store.insert(&entry).await {
        tracing::error!(key = %key, error = %e, "redb insert failed");
    }
    let evicted = {
        let mut s = state.write().await;
        s.total_bytes = s.total_bytes.saturating_sub(old_size) + entry.size_bytes;
        s.entries.insert(key.to_string(), entry);
        evict_pick(&mut s, config)
    };
    remove_entries(config, meta_store, &evicted).await;
}

/// Inactive-expiry collection — memory only. Persistence and file
/// deletes happen guard-free via [`remove_entries`] (C3 lock discipline).
fn reap_collect(state: &mut CacheState, ttl_ms: u64, now: u64) -> Vec<(String, u64)> {
    let expired: Vec<String> = state
        .entries
        .iter()
        .filter(|(_, m)| {
            m.negative_until_millis.map_or_else(
                || now.saturating_sub(m.last_access_millis) >= ttl_ms,
                |until| now >= until,
            )
        })
        .map(|(k, _)| k.clone())
        .collect();
    let mut out = Vec::with_capacity(expired.len());
    for k in expired {
        if let Some(m) = state.entries.remove(&k) {
            state.total_bytes = state.total_bytes.saturating_sub(m.size_bytes);
            out.push((k, m.size_bytes));
        }
    }
    out
}

/// Max-size LRU victim selection — memory only; deletes happen guard-free
/// via [`remove_entries`] (C3 lock discipline).
fn evict_pick(state: &mut CacheState, config: &Config) -> Vec<(String, u64)> {
    let mut out = Vec::new();
    while state.total_bytes > config.max_size_bytes && !state.entries.is_empty() {
        let victim = state
            .entries
            .iter()
            .filter(|(_, m)| m.negative_until_millis.is_none())
            .min_by_key(|(_, m)| m.eligible_at(config.inactive_ttl_secs))
            .map(|(k, _)| k.clone());
        match victim {
            Some(k) => {
                if let Some(m) = state.entries.remove(&k) {
                    state.total_bytes = state.total_bytes.saturating_sub(m.size_bytes);
                    out.push((k, m.size_bytes));
                } else {
                    break;
                }
            }
            None => break,
        }
    }
    out
}

/// redb row removal + cache-file deletion for victims collected under a
/// state guard. Never call this while holding the guard (C3).
async fn remove_entries(
    config: &Config,
    meta_store: &crate::cache::persist::MetaStore,
    victims: &[(String, u64)],
) {
    if victims.is_empty() {
        return;
    }
    // One redb transaction for the whole victim batch (P5), then the file
    // deletes.
    let keys: Vec<String> = victims.iter().map(|(k, _)| k.clone()).collect();
    if let Err(e) = meta_store.remove_batch(&keys).await {
        tracing::warn!(error = %e, "batched redb remove failed");
    }
    for (k, _) in victims {
        let path = store::file_path(&config.cache_dir, k);
        let _ = tokio::fs::remove_file(&path).await;
        store::prune_empty_parents(&config.cache_dir, &path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{backend::StorageBackend, clock::MockClock, config::Config};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::tempdir;

    #[derive(Clone)]
    struct CountingBackend {
        bytes: Vec<u8>,
        etag: Option<String>,
        calls: Arc<AtomicUsize>,
        fail: Option<BackendError>,
    }

    #[async_trait::async_trait]
    impl StorageBackend for CountingBackend {
        async fn stat(&self, _key: &Key) -> Result<ObjectMeta, BackendError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(e) = &self.fail {
                return Err(e.clone());
            }
            Ok(ObjectMeta {
                size_bytes: self.bytes.len() as u64,
                etag: self.etag.clone(),
                last_modified: None,
                mime_hint: Some("image/png".into()),
            })
        }

        async fn open(&self, _key: &Key, _range: Option<crate::backend::ByteRange>) -> Result<crate::backend::StreamSource, BackendError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(e) = &self.fail {
                return Err(e.clone());
            }
            Ok(crate::backend::StreamSource {
                stream: Box::new(std::io::Cursor::new(self.bytes.clone())),
                total_len: Some(self.bytes.len() as u64),
            })
        }

        async fn refresh_if_needed(&self) -> Result<(), BackendError> {
            Ok(())
        }

        fn id(&self) -> &str {
            "test"
        }
    }

    async fn read_body(body: &mut BodyStream) -> Vec<u8> {
        use futures::StreamExt;
        let mut out = Vec::new();
        while let Some(chunk) = body.next().await {
            out.extend_from_slice(&chunk.unwrap());
        }
        out
    }

    fn test_cache(
        dir: std::path::PathBuf,
        bytes: &[u8],
        etag: Option<&str>,
        fail: Option<BackendError>,
    ) -> (Arc<Config>, Arc<MockClock>, Cache<MockClock>, Arc<AtomicUsize>) {
        let mut cfg = Config::default();
        cfg.cache_dir = dir;
        let cfg = Arc::new(cfg);
        let clock = Arc::new(MockClock::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let backend = CountingBackend {
            bytes: bytes.to_vec(),
            etag: etag.map(|s| s.to_string()),
            calls: Arc::clone(&calls),
            fail,
        };
        let mut slots = HashMap::new();
        slots.insert(
            "primary".to_string(),
            Arc::new(BackendSlot { backend: Arc::new(backend), gate: Arc::new(tokio::sync::Semaphore::new(3)) }),
        );
        let cache = Cache::new(Arc::clone(&cfg), Arc::clone(&clock), BackendRegistry::new(slots));
        (cfg, clock, cache, calls)
    }

    #[tokio::test]
    async fn miss_then_hit() {
        let dir = tempdir().unwrap();
        let (_cfg, _clock, cache, _calls) = test_cache(dir.path().to_path_buf(), b"hello", Some("v1"), None);
        let mut hit = cache.get("a.png", None).await.unwrap();
        assert_eq!(hit.outcome, CacheOutcome::Miss);
        let b = read_body(&mut hit.body).await;
        assert_eq!(b, b"hello");
        // wait for the driver to seal + install
        for _ in 0..100 {
            if cache.state.read().await.entries.contains_key("a.png") {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let mut hit2 = cache.get("a.png", None).await.unwrap();
        assert_eq!(hit2.outcome, CacheOutcome::Hit);
        let b2 = read_body(&mut hit2.body).await;
        assert_eq!(b2, b"hello");
    }

    #[tokio::test]
    async fn negative_cache_and_expiry_via_tick() {
        let dir = tempdir().unwrap();
        let mut cfg = Config::default();
        cfg.cache_dir = dir.path().to_path_buf();
        cfg.negative_ttl_secs = 2;
        let cfg = Arc::new(cfg);
        let clock = Arc::new(MockClock::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let backend = CountingBackend {
            bytes: vec![],
            etag: None,
            calls: Arc::clone(&calls),
            fail: Some(BackendError::NotFound),
        };
        let mut slots = HashMap::new();
        slots.insert(
            "primary".to_string(),
            Arc::new(BackendSlot { backend: Arc::new(backend), gate: Arc::new(tokio::sync::Semaphore::new(3)) }),
        );
        let cache = Cache::new(cfg, Arc::clone(&clock), BackendRegistry::new(slots));
        assert!(matches!(cache.get("missing.png", None).await, Err(BackendError::NotFound)));
        assert!(matches!(cache.get("missing.png", None).await, Err(BackendError::NotFound)));
        clock.advance(3000);
        cache.tick().await;
        assert!(matches!(cache.get("missing.png", None).await, Err(BackendError::NotFound)));
    }

    #[tokio::test]
    async fn stale_if_error_serves_cached() {
        let dir = tempdir().unwrap();
        let (cfg, clock, cache, _calls) = test_cache(dir.path().to_path_buf(), b"cached", Some("v1"), None);
        let mut hit = cache.get("a.png", None).await.unwrap();
        assert_eq!(read_body(&mut hit.body).await, b"cached");
        for _ in 0..100 {
            if cache.state.read().await.entries.contains_key("a.png") {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        // Swap state into a cache whose backend 500s (same dir + clock).
        let state = RwLock::new(std::mem::take(&mut *cache.state.write().await));
        let calls2 = Arc::new(AtomicUsize::new(0));
        let backend2 = CountingBackend {
            bytes: b"ignored".to_vec(),
            etag: None,
            calls: Arc::clone(&calls2),
            fail: Some(BackendError::ServerError("boom".into())),
        };
        let mut slots = HashMap::new();
        slots.insert(
            "primary".to_string(),
            Arc::new(BackendSlot { backend: Arc::new(backend2), gate: Arc::new(tokio::sync::Semaphore::new(3)) }),
        );
        let cache2 = Cache::new(Arc::clone(&cfg), Arc::clone(&clock), BackendRegistry::new(slots));
        {
            let mut s2 = cache2.state.write().await;
            *s2 = std::mem::take(&mut *state.write().await);
        }
        clock.advance(61_000);
        let mut hit2 = cache2.get("a.png", None).await.unwrap();
        assert_eq!(hit2.outcome, CacheOutcome::Stale);
        assert_eq!(read_body(&mut hit2.body).await, b"cached");
    }

    #[tokio::test]
    async fn inactive_expiry_via_tick() {
        let dir = tempdir().unwrap();
        let mut cfg = Config::default();
        cfg.cache_dir = dir.path().to_path_buf();
        cfg.inactive_ttl_secs = 1;
        let cfg = Arc::new(cfg);
        let clock = Arc::new(MockClock::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let backend = CountingBackend {
            bytes: b"x".to_vec(),
            etag: None,
            calls: Arc::clone(&calls),
            fail: None,
        };
        let mut slots = HashMap::new();
        slots.insert(
            "primary".to_string(),
            Arc::new(BackendSlot { backend: Arc::new(backend), gate: Arc::new(tokio::sync::Semaphore::new(3)) }),
        );
        let cache = Cache::new(cfg, Arc::clone(&clock), BackendRegistry::new(slots));
        let mut hit = cache.get("a.png", None).await.unwrap();
        read_body(&mut hit.body).await;
        for _ in 0..100 {
            if cache.state.read().await.entries.contains_key("a.png") {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(dir.path().join("a.png").exists());
        clock.advance(2000);
        cache.tick().await;
        assert!(!dir.path().join("a.png").exists());
    }
}
