use std::{collections::HashMap, sync::Arc};
use tokio::sync::{Mutex, RwLock};

use crate::{
    backend::{BackendError, BackendRegistry, BackendSlot, ByteRange, ContentRange, DirectUrl, Key, ObjectMeta},
    cache::{
        flight::{self, BodyStream, FlightProgress, FlightShared},
        magazine::{self, Magazine},
        meta::EntryMeta,
        session::{self, Sessions},
        staging::{self, FinalizedSpan, Staging},
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


/// Static metric label for an outcome (P7): keeps the hot path
/// allocation-free.
fn outcome_label(o: &CacheOutcome) -> &'static str {
    match o {
        CacheOutcome::Hit => "Hit",
        CacheOutcome::Miss => "Miss",
        CacheOutcome::Negative => "Negative",
        CacheOutcome::Stale => "Stale",
        CacheOutcome::Revalidated => "Revalidated",
    }
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
/// Resolve a client Range against an object of `total` bytes into
/// `[start, end)`, `end` exclusive. One construction site (C3): the same
/// arithmetic used to live in four places, and two of them had drifted.
pub(crate) fn resolve_range(
    range: Option<crate::backend::ByteRange>,
    total: u64,
) -> Result<(u64, u64), BackendError> {
    match range {
        None => Ok((0, total)),
        Some(r) => {
            if r.offset >= total {
                return Err(BackendError::RangeNotSatisfiable);
            }
            Ok((r.offset, r.length.map_or(total, |l| (r.offset + l).min(total))))
        }
    }
}

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
    /// Where the body's bytes come from (metric label).
    pub source: BodySource,
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
    /// Where the bytes came from: the metric label. "stage" means every byte
    /// came from this node's sidecars; any upstream open makes it
    /// "upstream", even when the first byte was served from stage.
    pub source: BodySource,
}

impl PassthroughHit {
    /// The same answer rendered as an ordinary cache hit, for the one caller
    /// that serves a cold miss WITHOUT caching it (ADR-0013). The headers
    /// and the body are unchanged; the outcome says "not from cache" and the
    /// source says where the bytes came from.
    fn into_cache_hit(self, outcome: CacheOutcome) -> CacheHit {
        CacheHit {
            outcome,
            meta: self.meta,
            content_range: self.content_range,
            content_length: self.content_length,
            body: self.body,
            source: BodySource::Upstream,
        }
    }
}

/// What one request should be answered with, decided in one place.
///
/// The serve-mode decision used to be spread over business.rs (`try_relief_valve`,
/// `try_nocache`, `try_passthrough`, then the water-pipe) while each mode's
/// implementation was a separate `Cache` method, so the profile concept had no
/// locality: a fourth profile meant four files. Here the order lives with the
/// modes; the caller renders.
///
/// The ORDER is load-bearing and is preserved verbatim: the relief valve
/// first, then the nocache passthrough, then the efficient passthrough, then
/// the ordinary cached path. Range validation and the 416 precedence stay in
/// the caller — they are request validation, not mode selection.
pub enum ServeOutcome {
    /// Hand the viewer the upstream's own signed link (307).
    Redirect { location: String },
    /// Stream these bytes, with the status and headers this path decided.
    Stream(StreamPlan),
}

/// The render facts for a streaming answer, taken from whichever path
/// produced it. Carried explicitly rather than re-derived, because the three
/// paths disagree on purpose: nocache and the cached path answer 206 only
/// when a content-range is present, while the efficient passthrough always
/// answers 206.
/// The one construction site for a streaming answer (C3): the status
/// follows the content-range — 206 when a range was honoured, 200 otherwise
/// — except where a path overrides it (the efficient passthrough always
/// answers 206, and its hit always carries one). Before this, `Cache::serve`
/// built the plan three times with the same fields reshuffled.
fn stream_plan(
    meta: HitMeta,
    content_range: Option<ContentRange>,
    content_length: Option<u64>,
    body: BodyStream,
    stale: bool,
    source: BodySource,
) -> StreamPlan {
    StreamPlan {
        status: if content_range.is_some() {
            axum::http::StatusCode::PARTIAL_CONTENT
        } else {
            axum::http::StatusCode::OK
        },
        meta,
        content_range,
        content_length,
        body,
        stale,
        source,
    }
}

/// Where a response's bytes actually come from. Carried on the plan (and on
/// every `CacheHit`) so the metric label is taken from the path that
/// produced the bytes rather than re-derived from the outcome — `Miss`, for
/// one, is upstream bytes when the flight is still filling and disk bytes
/// when a late attacher finds the sealed file.
///
/// The distinction is the product question this node exists to answer: a
/// range served from this node's disk costs no upstream round trip, and the
/// measured per-open cost is what makes a sequential shard walk slow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodySource {
    /// A complete object file on this node.
    Disk,
    /// Staged sidecars of this key (the efficient profile): the request was
    /// answered without opening upstream at all.
    Stage,
    /// A live upstream stream: a cold-miss flight, a nocache water-pipe, or
    /// an efficient passthrough that needed bytes.
    Upstream,
}

impl BodySource {
    pub fn label(self) -> &'static str {
        match self {
            BodySource::Disk => "disk",
            BodySource::Stage => "stage",
            BodySource::Upstream => "upstream",
        }
    }
}

pub struct StreamPlan {
    pub status: axum::http::StatusCode,
    pub meta: HitMeta,
    pub content_range: Option<ContentRange>,
    pub content_length: Option<u64>,
    pub body: BodyStream,
    /// Only the cached path reports this: a stale serve adds `Warning: 110`.
    pub stale: bool,
    /// Where the bytes come from — the label for the body metrics.
    pub source: BodySource,
}

impl StreamPlan {
    /// The byte range this response serves: where the viewer is on the object,
    /// which is the position a watch pins around (ADR-0018). A ranged response
    /// carries it in the content-range; a whole-object response starts at 0.
    pub fn served_span(&self) -> (u64, u64) {
        match &self.content_range {
            Some(cr) => (cr.first, cr.last.saturating_add(1)),
            None => (0, self.content_length.unwrap_or(0)),
        }
    }
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
/// invisible — verified in the audit (readers: the magazine's reaper and
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
    /// Of `total_bytes`, how much belongs to resident strays — entries the
    /// magazine's byte budget neither counts nor evicts (ADR-0014). This is
    /// the number that explains a `total_bytes` above `max_size_bytes`.
    pub stray_bytes: u64,
    pub flights_active: usize,
    pub dirty_access_pending: usize,
    /// Coverage-ledger rows and their total interval count (P10: the
    /// ledger is the other structure that grows with distinct scrubbed
    /// keys, so operators need to see it).
    pub coverage_keys: usize,
    pub coverage_intervals: usize,
    /// Whether the metadata store opened cleanly or was quarantined (C1).
    pub store: crate::cache::persist::StoreState,
    /// Entry rows rebuilt from the object tree after metadata loss (C1).
    pub rebuilt_rows: usize,
    /// Background prewarm fetches in flight (spec §10).
    pub prewarm_inflight: usize,
    /// Free bytes on the cache filesystem, when known (C1).
    pub disk_free_bytes: Option<u64>,
    /// The reserve floor cold pulls are held back from (C1).
    pub disk_reserve_bytes: u64,
}

/// The Cache is the only seam between the HTTP layer and the cache
/// machinery (ADR-0002). Its interface is the request path (get / head /
/// resolve / per-profile serves), lifecycle (load_and_start / tick), the
/// relief-valve helpers (prefetch / direct_url_bounded), and the
/// operator view (snapshot). The pub fields below are the machinery's
/// working state — consumed by tests; production code goes through the
/// methods, never through them.
/// How often the session chain is evaluated (ADR-0016). Sub-second because
/// the decision is only useful while a reader is still inside the sealed
/// window: at the measured 63 MB/s a 64 MiB window is ~1 s of transfer, so a
/// quarter of that leaves room to start the successor before the reader
/// reaches the boundary.
const SESSION_TICK_INTERVAL: std::time::Duration = std::time::Duration::from_millis(250);

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
    /// The magazine: every byte-budget decision (admission, eviction,
    /// pressure reclaim, install, delete) lives behind this receiver.
    pub(crate) magazine: Magazine,
    /// Read leases (ADR-0017): keys a viewer is streaming, which policy
    /// eviction must leave alone while the body lives and for a grace after.
    /// `pub` like the rest of the machinery's working state (tests drive it
    /// directly; production goes through the response path).
    pub leases: Arc<crate::cache::leases::Leases>,
    /// Watches (ADR-0018): keys being VIEWED, which outlives the bodies of one
    /// viewing session. Where a lease protects coarsely (the whole key, while a
    /// body lives), a watch protects precisely — a bounded neighbourhood
    /// around the viewer's position — and for as long as the session lasts.
    pub watches: Arc<crate::cache::watch::Watches>,
    /// Staged-read runs (ADR-0016): one upstream stream per key, shared by
    /// every reader inside its window. Public because the serve path and the
    /// tests both drive it; the module owns the invariants.
    pub(crate) sessions: Arc<Sessions<C>>,
    /// The staging ledger: the efficient profile's transfer history,
    /// promotion and assembly, behind this receiver.
    pub(crate) staging: Staging,
    pub(crate) reval_inflight: Inflight<StatData, BackendError>,
    /// Rows rebuilt from the object tree after metadata loss (C1). Read by
    /// healthz so a rebuild is visible without reading logs.
    pub rebuilt_rows: std::sync::atomic::AtomicUsize,
    /// Background prewarm fetches in flight (spec §10 asks healthz to report
    /// the queue depth; prewarm answers immediately and fetches behind the
    /// caller, so this is the count that stands in for a queue).
    pub prewarm_inflight: std::sync::atomic::AtomicUsize,
    pub routes: RouteTable,
}

#[derive(Debug, Clone)]
pub(crate) struct StatData {
    meta: ObjectMeta,
}

impl<C: Clock + Clone + 'static> Cache<C> {
    pub fn new(config: Arc<Config>, clock: Arc<C>, backends: BackendRegistry) -> Self {
        let routes = config.routes.clone();
        let meta = Arc::new(crate::cache::persist::MetaStore::open(&config.cache_dir.join(store::META_STORE_FILE)).expect("open redb metadata store"));
        let dirty_access = Arc::new(AccessClock::new());
        let state = Arc::new(RwLock::new(CacheState::default()));
        let coverage = Arc::new(Mutex::new(HashMap::new()));
        let leases = Arc::new(crate::cache::leases::Leases::new(
            config.read_grace_secs.saturating_mul(1000),
        ));
        // Watches (ADR-0018): the viewing session, which outlives its bodies.
        let watches = Arc::new(crate::cache::watch::Watches::new(
            config.watch_idle_secs.saturating_mul(1000),
            config.watch_pin_bytes,
        ));
        let magazine = Magazine::new(
            Arc::clone(&state),
            Arc::clone(&config),
            Arc::clone(&meta),
            Arc::clone(&leases),
            Arc::clone(&watches),
        );
        let staging = Staging::new(
            Arc::clone(&coverage),
            Arc::clone(&state),
            Arc::clone(&config),
            Arc::clone(&leases),
            Arc::clone(&watches),
        );
        let flights = crate::cache::flight::Flights::new(crate::cache::flight::DEFAULT_STALL_BUDGET);
        let sessions = Arc::new(Sessions::new(
            Arc::clone(&config),
            Arc::clone(&clock),
            staging.clone(),
            flights.clone(),
            Arc::clone(&watches),
        ));
        Self {
            config,
            clock,
            backends,
            state,
            meta,
            dirty_access,
            flights,
            coverage,
            staging,
            sessions,
            leases,
            watches,
            reval_inflight: Inflight::new(),
            rebuilt_rows: std::sync::atomic::AtomicUsize::new(0),
            prewarm_inflight: std::sync::atomic::AtomicUsize::new(0),
            routes,
            magazine,
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

        // Metadata load failures used to be swallowed by
        // `unwrap_or_default()` (ticket #57): a corrupt store started the
        // node with ZERO entries while every cached file sat on disk as an
        // orphan — served by nothing, reaped by nothing, and reported by
        // nothing. Log it, then rebuild from the object tree below.
        let persisted = match self.meta.load_all().await {
            Ok(rows) => rows,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "metadata load failed; rebuilding entry rows from the object tree"
                );
                Vec::new()
            }
        };
        let loaded = persisted.len();
        // Resolve file existence OUTSIDE the state guard (O4): the loop
        // used to await `metadata` and `redb remove` while holding
        // `state.write()`, which is exactly the shape the C3 lock
        // discipline forbids. Startup is single-threaded so it was
        // harmless in practice, but a rule with an exception is not a rule.
        let mut live_rows = Vec::with_capacity(persisted.len());
        let mut lost_rows: Vec<String> = Vec::new();
        for m in persisted {
            // A row naming the metadata store is leftovers from the rebuild
            // bug: the scanner used to adopt it as a cached object. Drop the
            // ROW and keep the FILE. `is_reserved_key` is the same rule the
            // request path applies, so only keys no valid request could have
            // produced are dropped here; a nested `bucket/redb.db` is an
            // ordinary object and stays.
            if store::is_reserved_key(&m.key) {
                lost_rows.push(m.key);
                continue;
            }
            let path = store::file_path(&self.config.cache_dir, &m.key);
            if tokio::fs::metadata(&path).await.is_ok() {
                live_rows.push(m);
            } else {
                // File lost while we were down — drop the row too.
                lost_rows.push(m.key);
            }
        }
        if !lost_rows.is_empty() {
            if let Err(e) = self.meta.remove_batch(&lost_rows).await {
                tracing::warn!(error = %e, "startup: dropping rows for vanished files failed");
            }
        }
        let mut state = self.state.write().await;
        state.segment_bytes = staged;
        for m in live_rows {
            state.total_bytes += m.size_bytes;
            state.entries.insert(m.key.clone(), m);
        }
        // Rebuild path (ticket #57): rows missing but bytes present means
        // metadata was lost (corrupt store, manual `rm redb.db`). Recreate
        // an entry per surviving object file so the bytes stay served and
        // reaped instead of leaking. ETag/mtime are unknown, so the rows
        // carry none: the first access re-stats the upstream and installs
        // the real metadata (the same revalidation path as any stale row).
        let now = self.clock.now_millis();
        let needs_rebuild = state.entries.is_empty();
        // Release the state guard before touching redb (C3 lock discipline):
        // the rebuild below persists rows.
        drop(state);
        if needs_rebuild {
            let found = store::scan_object_files(&self.config.cache_dir);
            if !found.is_empty() {
                tracing::warn!(
                    files = found.len(),
                    "no metadata rows but object files exist; rebuilding entries from disk"
                );
                let mut rebuilt: Vec<EntryMeta> = Vec::with_capacity(found.len());
                for (key, size) in found {
                    let upstream_id = self.routes.resolve(&key).to_string();
                    rebuilt.push(EntryMeta {
                        version: 1,
                        upstream_id,
                        key,
                        size_bytes: size,
                        etag: None,
                        last_modified: None,
                        content_type: None,
                        created_at_millis: now,
                        last_access_millis: now,
                        last_revalidated_millis: None,
                        negative_until_millis: None,
                        // Metadata loss leaves nothing but the size, so the
                        // size rule is the only information available: a
                        // rebuilt row larger than the budget is admitted as
                        // a stray, exactly as it would be on a cold pull.
                        oversize: !self.magazine.fits(size),
                    });
                }
                let n = rebuilt.len();
                self.magazine.rebuild(rebuilt, loaded).await;
                self.rebuilt_rows.store(n, std::sync::atomic::Ordering::Relaxed);
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

        // Session ticker: the chain decision (ADR-0016) has to run while a
        // reader is still consuming a sealed window — a sub-second window of
        // opportunity that the reaper's 60 s spacing is far too coarse for.
        // Cheap: a map scan and a few counter loads per run.
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(SESSION_TICK_INTERVAL);
            loop {
                tick.tick().await;
                this.sessions.tick().await;
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
        resolve_key(raw_path, &self.routes, self.backends.ids_slice())
    }

    /// Owned view of the live machinery for healthz: operators read a
    /// snapshot, never the internals (C4).
    pub async fn snapshot(&self) -> CacheSnapshot {
        let (entries, total_bytes, segment_bytes) = {
            let s = self.state.read().await;
            (s.entries.len(), s.total_bytes, s.segment_bytes)
        };
        let stray_bytes = self.magazine.strays_bytes().await;
        let (coverage_keys, coverage_intervals) = self.staging.summary().await;
        CacheSnapshot {
            entries,
            total_bytes,
            segment_bytes,
            stray_bytes,
            flights_active: self.flights.active().await,
            dirty_access_pending: self.dirty_access.pending(),
            coverage_keys,
            coverage_intervals,
            store: self.meta.state().clone(),
            rebuilt_rows: self.rebuilt_rows.load(std::sync::atomic::Ordering::Relaxed),
            prewarm_inflight: self.prewarm_inflight.load(std::sync::atomic::Ordering::Relaxed),
            disk_free_bytes: store::free_bytes(&self.config.cache_dir),
            disk_reserve_bytes: magazine::DISK_RESERVE_BYTES,
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
            let m = {
                let _permit = slot.gate.acquire().await;
                slot.backend.stat(&Key::from_validated(backend_key)).await?
            };
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
                self.magazine
                    .install_negative(&key, &upstream_id, self.clock.now_millis())
                    .await;
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

    /// Whether a fresh (no revalidation due) memory entry exists. A pure
    /// peek — no upstream, no mutation, no flights.
    ///
    /// This is the relief valve's gate and only its gate, and the freshness
    /// clock is the point there: the valve exists for upstreams whose
    /// operator asked for `cold_miss = redirect`, i.e. "hand the viewer to
    /// my CDN rather than proxy the bytes". Something we hold but have not
    /// revalidated is exactly what that operator wants redirected. The
    /// efficient profile deliberately does NOT use this gate — see
    /// [`Cache::has_durable_entry`].
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

    /// Whether we already hold this object at all: any row for the key, a
    /// negative tombstone included (an answer is an answer — the ordinary
    /// path renders it 404).
    ///
    /// Deliberately NOT a freshness check. The efficient profile's whole
    /// purpose is to serve bytes it holds, and gating that on the 60 s
    /// revalidate clock meant a complete promoted entry stopped serving
    /// ranged reads a minute after its promotion — which also capped the
    /// promotion hold's value at that same minute. Whether the bytes need
    /// revalidating is the ordinary path's business: it stats, compares
    /// etags and either serves the file or refetches it.
    ///
    /// Pure peek: no upstream call, no mutation, no flights, and no
    /// filesystem call — a row whose file vanished falls through the
    /// ordinary path's own disk check.
    pub(crate) async fn has_durable_entry(&self, raw_key: &str) -> bool {
        let key = match validate_key(raw_key) {
            Ok(k) => k,
            Err(_) => return false,
        };
        self.state.read().await.entries.contains_key(&key)
    }

    /// One coalesced upstream stat per key, holding the METADATA gate inside
    /// the coalescer: the permit is taken by whichever caller actually runs
    /// the PROPFIND, not by every caller that wants the result. Gating
    /// outside the coalescer would queue a stampede on three permits and
    /// each straggler would then find the shared cell already gone — the
    /// measured 50-stats-for-50-requests shape.
    async fn stat_coalesced(
        &self,
        slot: &Arc<BackendSlot>,
        cache_key: &str,
        bkey: &Key,
    ) -> Result<ObjectMeta, BackendError> {
        self.reval_inflight
            .run(format!("stat:{cache_key}"), || {
                let slot = Arc::clone(slot);
                let k = bkey.clone();
                async move {
                    let _permit = slot.gate.acquire().await;
                    slot.backend.stat(&k).await.map(|meta| StatData { meta })
                }
            })
            .await
            .map(|s| s.meta)
    }

    /// Whether `want` bytes can be written to the cache disk, making room
    /// first if needed: it evicts resident strays (oldest-touched first) and
    /// then asks again. Returns false when even that is not enough — the
    /// caller then serves the request WITHOUT caching it rather than
    /// refusing to serve it (ADR-0013).
    ///
    /// Only strays yield here. They sit outside the magazine's byte budget
    /// by definition (ADR-0014), so removing one is the least destructive
    /// way to make room; the magazine's own members leave on their clock.
    /// The tick reclaims strays down to the same floor on its own schedule,
    /// which keeps this path's eviction step rare.
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

    /// Stream a byte range straight from the origin: the one water pipe
    /// behind every path that serves bytes without holding them.
    ///
    /// It owns the per-upstream STREAM gate (ADR-0004) and holds it for as
    /// long as the body can move bytes — that budget is what bounds
    /// bandwidth-bound upstream work, and a passthrough that skipped it left
    /// upstream pressure set by the client count: the same range requested
    /// twice opened two upstream streams. The permit is moved INTO the body
    /// rather than held in this scope, so it lives exactly as long as the
    /// transfer does and not one request longer.
    ///
    /// Callers own the metadata protocol (single-flight stat, negative
    /// tombstones, whether a refused request may still be served): this owns
    /// the gate, the open, and the read loop.
    async fn serve_upstream_range(
        &self,
        slot: &Arc<BackendSlot>,
        rk: &ResolvedKey,
        range: Option<crate::backend::ByteRange>,
        meta: crate::backend::ObjectMeta,
    ) -> Result<PassthroughHit, BackendError> {
        let bkey = Key::from_validated(rk.backend_key.clone());
        let total = meta.size_bytes;
        let (start, end) = resolve_range(range, total)?;
        let stream_permit = Arc::clone(&slot.stream_gate).acquire_owned().await;
        let src = slot.backend.open(&bkey, range).await?;
        let content_range = range.map(|_| ContentRange { first: start, last: end.saturating_sub(1), total });
        let content_length = Some(end.saturating_sub(start));
        let mut src_stream = src.stream;
        let body: BodyStream = Box::pin(async_stream::try_stream! {
            // The gate is held by the transfer itself.
            let _stream_permit = stream_permit;
            // Read straight into a fresh BytesMut and freeze it (P8): the
            // served bytes are handed over with no copy step.
            use tokio::io::AsyncReadExt;
            let mut buf = bytes::BytesMut::with_capacity(256 * 1024);
            loop {
                buf.clear();
                let n = src_stream.read_buf(&mut buf).await?;
                if n == 0 {
                    break;
                }
                yield buf.split().freeze();
            }
        });
        Ok(PassthroughHit {
            meta: hit_meta_remote(&rk.cache_key, &meta),
            etag: meta.etag,
            total,
            content_range,
            content_length,
            body,
            source: BodySource::Upstream,
        })
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
        let bkey = Key::from_validated(rk.backend_key.clone());
        let meta = {
            // stat is metadata; the transfer below is a stream (B1).
            let _permit = slot.gate.acquire().await;
            slot.backend.stat(&bkey).await?
        };
        self.serve_upstream_range(&slot, rk, range, meta).await
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
        self: &Arc<Self>,
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
        self: &Arc<Self>,
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
        // Stat single-flight: concurrent requests for the same key coalesce
        // to ONE upstream stat, and the METADATA gate is taken inside the
        // coalescer (see `stat_coalesced`) so the gate is paid once per
        // batched call rather than once per stampeding request.
        let bkey = Key::from_validated(rk.backend_key.clone());
        let meta = match self.stat_coalesced(&slot, &rk.cache_key, &bkey).await {
            Ok(m) => m,
            Err(BackendError::NotFound) => {
                self.magazine
                    .install_negative(&rk.cache_key, &rk.upstream_id, self.clock.now_millis())
                    .await;
                return Err(BackendError::NotFound);
            }
            Err(e) => return Err(e),
        };
        if meta.size_bytes < min_file_size {
            return Err(BackendError::Other("below min_file_size".into()));
        }
        let (start, end) = resolve_range(range, meta.size_bytes)?;
        // Version gate: a flipped object restarts staged history BEFORE
        // serving, so new bytes land on a clean ledger (finalize keeps a
        // same-file backstop for races). It gates the staged READ below too:
        // a drifted ledger must not serve old bytes.
        {
            let known = self.staging.known_etag(&rk.cache_key).await;
            let changed = match (known.as_deref(), meta.etag.as_deref()) {
                (Some(a), Some(b)) => a != b,
                _ => false,
            };
            if changed {
                // A reset deletes every `.seg` of the key, and a response body
                // serves its staged pieces LAZILY (`pieces_then` opens each one
                // as it reaches it), so a reset under a live body cuts the
                // viewer off mid-stream on the piece it has not opened yet.
                // The reset is right — mixed versions must never be served —
                // but it does not have to happen while somebody is reading:
                // answer this request from upstream and let the drift be
                // settled by the next request that arrives when nobody is.
                let now = self.clock.now_millis();
                if self.watches.live(&rk.cache_key, now) || self.leases.is_protected(&rk.cache_key, now) {
                    tracing::info!(
                        key = %rk.cache_key,
                        "version drifted while this key is being read: serving upstream, settling the ledger later"
                    );
                    return self.serve_upstream_range(&slot, rk, range, meta).await;
                }
                self.staging.reset(&rk.cache_key).await;
            }
        }
        // Staged reads: plan `[start, end)` against the ledger AND
        // a fresh disk scan. The scan is the authority - a span whose file
        // vanished simply is not covered, and the gap falls back upstream
        // instead of trusting the ledger over disk.
        let segs = store::segments_for_key(&self.config.cache_dir, &rk.cache_key);
        let plan = staging::plan_staged(&segs, start, end);
        if plan.frontier >= end {
            // Fully covered: answered from this node's sidecars with no
            // upstream open, no stream gate and no disk write.
            tracing::info!(key = %rk.cache_key, from = start, to = end, "served from staged sidecars");
            self.staging.touch(&rk.cache_key, self.clock.now_millis()).await;
            // The read count is the heat policy's input: one serve of this
            // range adds one count to the span that holds it.
            self.staging.add_read(&rk.cache_key, start, end).await;
            return Ok(PassthroughHit {
                meta: hit_meta_remote(&rk.cache_key, &meta),
                etag: meta.etag,
                total: meta.size_bytes,
                content_range: range
                    .map(|_| ContentRange { first: start, last: end - 1, total: meta.size_bytes }),
                content_length: Some(end - start),
                body: staging::staged_body(plan.pieces),
                source: BodySource::Stage,
            });
        }
        let staged_prefix = !plan.pieces.is_empty();
        if staged_prefix {
            // The staged head of this response is being re-served, so the
            // spans it came from are read — credit them. Cold sequential
            // playback is made of nothing but these partial hits, and the
            // heat policy would otherwise see zero reads for the whole walk.
            self.staging.add_read(&rk.cache_key, start, plan.frontier).await;
        }
        // Staging admission (ADR-0013). Staged segments have exactly one
        // reader - promotion - and promotion is refused for an object the
        // magazine cannot hold, so staging such an object writes bytes
        // nobody can ever read back: pure cost, paid on every seek. The disk
        // gets the same question the cold-pull path asks, because a staged
        // stream is a disk write like any other. A staged PREFIX changes the
        // fetch to `[frontier, end)`, so that is the write the disk is asked
        // about.
        // A run writes a WINDOW, not this request's remainder (it is the
        // read-ahead that makes one open serve many shards), so the disk is
        // asked about what will actually be written — clamped to the object,
        // exactly as `Sessions::start` computes it. The request's own
        // remainder is the floor.
        let run_len = self
            .config
            .session_window_bytes
            .max(1)
            .max(end - plan.frontier)
            .min(meta.size_bytes.saturating_sub(plan.frontier));
        if !self.magazine.fits(meta.size_bytes)
            || !store::has_room_for(&self.config.cache_dir, run_len, magazine::DISK_RESERVE_BYTES)
        {
            tracing::info!(
                key = %rk.cache_key,
                size = meta.size_bytes,
                "passthrough without staging: the magazine cannot hold this object"
            );
            return self.serve_upstream_range(&slot, rk, range, meta).await;
        }
        // One upstream stream per key (ADR-0016). An upstream `open` costs a
        // fixed ~640 ms, so paying it per ranged request is the whole cost of a
        // scrub: a live run whose window covers this request answers it from
        // the watermark with NO upstream open and NO stream permit, and when no
        // run covers it this request starts one (paying the permit itself, as
        // it always did) and rides its watermark rather than fetching its own
        // Range. Either way the bytes land in the same staged span the disk
        // ledger plans against afterwards.
        //
        // A run one key cannot serve is the escape the design pins: the live
        // run does not cover this offset (a far seek) or one is already
        // starting, so the request takes its own exact Range below — never a
        // wait on somebody else's window.
        let session_run = match self.sessions.covering(&rk.cache_key, plan.frontier, end).await {
            Some(run) => Some(run),
            None => {
                self.sessions
                    .start(
                        &slot,
                        &rk.cache_key,
                        bkey.clone(),
                        &rk.upstream_id,
                        meta.etag.clone(),
                        meta.size_bytes,
                        plan.frontier,
                        end - plan.frontier,
                        // This request's reader IS the viewer's position; there
                        // is nothing to inherit.
                        None,
                    )
                    .await
            }
        };
        if let Some(run) = session_run {
            crate::metrics::observe_session_reader("attached");
            tracing::info!(
                key = %rk.cache_key,
                from = start,
                to = end,
                run = %format!("{}-{}", run.start, run.end),
                "served from a staged-read run"
            );
            let total = meta.size_bytes;
            let content_range =
                range.map(|_| ContentRange { first: start, last: end.saturating_sub(1), total });
            return Ok(PassthroughHit {
                meta: hit_meta_remote(&rk.cache_key, &meta),
                etag: meta.etag,
                total,
                content_range,
                content_length: Some(end - start),
                body: staging::pieces_then(
                    plan.pieces,
                    session::Sessions::<C>::reader(run, plan.frontier, end),
                ),
                source: BodySource::Stage,
            });
        }
        crate::metrics::observe_session_reader("standalone");
        // The open and the transfer below are a stream, not metadata (B1):
        // this used to take the METADATA gate, which is the head-of-line
        // class ADR-0004 split off, and it released it before a byte moved.
        let stream_permit = Arc::clone(&slot.stream_gate).acquire_owned().await;
        // Fetch only the uncovered remainder, at ONE exact Range: a staged
        // prefix rides along in the response and the open count stays at one
        // per request regardless of how many sidecars it rode on.
        let fetch_start = plan.frontier;
        let src = match slot
            .backend
            .open(&bkey, Some(ByteRange::bounded(fetch_start, end - fetch_start)))
            .await
        {
            Ok(s) => s,
            Err(BackendError::NotFound) => {
                self.magazine
                    .install_negative(&rk.cache_key, &rk.upstream_id, self.clock.now_millis())
                    .await;
                return Err(BackendError::NotFound);
            }
            Err(e) => return Err(e),
        };
        let total = meta.size_bytes;
        let etag = meta.etag.clone();
        let meta_out = hit_meta_remote(&rk.cache_key, &meta);
        let content_range = range.map(|_| ContentRange { first: start, last: end.saturating_sub(1), total });
        let content_length = Some(end.saturating_sub(start));
        // First byte from a staged prefix counts as stage-sourced; any
        // upstream open still shows up in the bytes ledger as upstream.
        let source = if staged_prefix { BodySource::Stage } else { BodySource::Upstream };

        // Staged streaming: chunk → segpart file → viewer. Ledger merge +
        // seal rename happen only on exhaustion, so aborts are sweep-safe.
        let clock = Arc::clone(&self.clock);
        let staging = self.staging.clone();
        let cache_dir = self.config.cache_dir.clone();
        let cache_key = rk.cache_key.clone();
        let backend_key = rk.backend_key.clone();
        let upstream_id = rk.upstream_id.clone();
        let segpart = store::segpart_path(&cache_dir, &cache_key, fetch_start, end);
        let mut src_stream = src.stream;
        // The upstream 206 stream may not signal EOF at the Content-Length
        // boundary (keep-alive reuse, e.g. rclone serve webdav): read at
        // most `end - start` bytes, then seal. Waiting for EOF would hang
        // the staging loop and leave the segpart unsealed forever.
        let want = end.saturating_sub(fetch_start);
        // Viewer disconnect drops the whole body stream (axum drops the
        // async block), so the seal code below never runs on abort. A
        // detached watcher polls the segpart and seals it once it stops
        // growing — the served bytes still count toward coverage
        // (segmented downloads are separate connections).
        let watcher = {
            let segpart = segpart.clone();
            let fetch_start = fetch_start;
            let seg = store::seg_path(&cache_dir, &cache_key, fetch_start, end);
            let clock = Arc::clone(&clock);
            let staging = self.staging.clone();
            let cache_dir = cache_dir.clone();
            let cache_key = cache_key.clone();
            let backend_key = backend_key.clone();
            let upstream_id = upstream_id.clone();
            let etag = etag.clone();
            let total = total;
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
                            // Seal it here — through the same rename-and-claim
                            // step the stream's own tail uses.
                            staging
                                .seal_renamed(
                                    &segpart,
                                    &seg,
                                    FinalizedSpan {
                                        cache_dir,
                                        key: cache_key,
                                        backend_key,
                                        upstream_id,
                                        etag,
                                        total,
                                        start: fetch_start,
                                        end: fetch_start + size,
                                        bytes: size,
                                        now_millis: clock.now_millis(),
                                    },
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
            // The upstream stream gate is held by the transfer itself, not
            // by the request that built it.
            let _stream_permit = stream_permit;
            use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
            // The staged prefix goes first: the viewer starts on local bytes
            // while the upstream remainder is fetched. It is served from the
            // planned sidecars, exactly as the staged-read path would.
            for (path, off, len) in plan.pieces {
                let mut f = tokio::fs::File::open(&path).await?;
                f.seek(std::io::SeekFrom::Start(off)).await?;
                let mut remaining = len;
                while remaining > 0 {
                    let want_read = (256 * 1024).min(remaining as usize);
                    let mut piece = bytes::BytesMut::with_capacity(want_read);
                    let n = f.read_buf(&mut piece).await?;
                    if n == 0 {
                        Err(std::io::Error::other(
                            "staged segment is shorter than the ledger's range",
                        ))?;
                    }
                    remaining -= n as u64;
                    yield piece.freeze();
                }
            }
            let mut file: Option<tokio::fs::File> = None;
            let mut written: u64 = 0;
            while written < want {
                // The staged copy must reach the sidecar file AND the
                // viewer, so this is the one path that needs both a file
                // write and a served buffer (P8: the serve side is the
                // frozen bytes, no extra copy).
                let mut chunk = bytes::BytesMut::with_capacity(256 * 1024);
                let n = src_stream.read_buf(&mut chunk).await?;
                if n == 0 {
                    break;
                }
                if file.is_none() {
                    if let Some(parent) = segpart.parent() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                    file = Some(tokio::fs::File::create(&segpart).await?);
                }
                file.as_mut().unwrap().write_all(&chunk).await?;
                written += n as u64;
                yield chunk.freeze();
            }
            if written > 0 {
                drop(file);
                let seg = store::seg_path(&cache_dir, &cache_key, fetch_start, fetch_start + written);
                // The rename and the claim are one step: a swallowed failure
                // here left the ledger and `segment_bytes` describing a span
                // with no file behind it (see [`Staging::seal_renamed`]).
                staging
                    .seal_renamed(
                        &segpart,
                        &seg,
                        FinalizedSpan {
                            cache_dir,
                            key: cache_key,
                            backend_key,
                            upstream_id,
                            etag,
                            total,
                            start: fetch_start,
                            end: fetch_start + written,
                            bytes: written,
                            now_millis: clock.now_millis(),
                        },
                    )
                    .await;
            } else if file.is_some() {
                let _ = tokio::fs::remove_file(&segpart).await;
            }
        });
        let _ = watcher;
        Ok(PassthroughHit {
            meta: meta_out,
            etag: meta.etag,
            total,
            content_range,
            content_length,
            body,
            source,
        })
    }

    /// Main entry: `GET /<key>` — streaming response. Cold misses attach
    /// to a shared download flight; hits stream the cached file; revalidation
    /// stats the upstream (no bytes) and compares etags. A Range request on
    /// a cached file slices locally; on a cold miss with offset > 0 the
    /// reader converges on the flight's growing temp file and waits for the
    /// writer to reach its offset (single stream — every upstream open pays
    /// a ~800 ms fixed cost).

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
        // Static outcome labels (P7): the set is closed, so a per-request
        // `format!` only produced garbage.
        let label = match &out {
            Ok(hit) => outcome_label(&hit.outcome),
            Err(_) => "error",
        };
        crate::metrics::observe_serve(label, start);
        out
    }

    /// Decide how to answer one GET — see [`ServeOutcome`] for the order and
    /// why the decision lives here rather than in the handler.
    pub async fn serve(
        self: &Arc<Self>,
        rk: &ResolvedKey,
        range: Option<ByteRange>,
        viewer_ua: Option<&str>,
    ) -> Result<ServeOutcome, BackendError> {
        let prof = self.config.cache_profile(&rk.upstream_id);

        // 1. Relief valve: a redirect-capable upstream hands the viewer its own
        // signed link and we fill the cache in the background. Hit-first (a
        // fresh memory entry never redirects), and every failure falls through
        // -- the valve can only save bandwidth, never break a fetch. The link
        // must also be a legal header value, or the viewer would receive a
        // redirect it cannot follow.
        let redirect_capable = self
            .config
            .upstream(&rk.upstream_id)
            .map(|u| u.cold_miss == crate::config::ColdMiss::Redirect)
            .unwrap_or(false);
        if redirect_capable && !self.memory_hit_fresh(&rk.cache_key).await {
            let link = self
                .direct_url_bounded(
                    &rk.upstream_id,
                    &rk.backend_key,
                    viewer_ua,
                    std::time::Duration::from_secs(8),
                )
                .await;
            if let Some(link) = link {
                if crate::backend::redirect_target_allowed(&link.url)
                    && link.url.parse::<axum::http::HeaderValue>().is_ok()
                {
                    let cache = Arc::clone(self);
                    let rk_owned = rk.clone();
                    tokio::spawn(async move {
                        let _ = cache.prefetch(&rk_owned).await;
                    });
                    return Ok(ServeOutcome::Redirect { location: link.url });
                }
            }
        }

        // 2. Nocache profile: pure water-pipe, no disk, no stale-if-error.
        if prof.nocache {
            if let Ok(hit) = self.serve_nocache(rk, range).await {
                tracing::info!(key = %rk.cache_key, size = hit.meta.size, "nocache passthrough response");
                return Ok(ServeOutcome::Stream(stream_plan(
                    hit.meta,
                    hit.content_range,
                    hit.content_length,
                    hit.body,
                    false,
                    BodySource::Upstream,
                )));
            }
        }

        // 3. Efficient profile: a ranged miss streams origin bytes while the
        // served interval is staged. Always 206 (this path only runs with a
        // range). An object we already hold goes to the ordinary path
        // instead, so a promoted entry keeps serving ranges for its whole
        // life rather than only for the revalidate window.
        if prof.efficient && range.is_some() && !self.has_durable_entry(&rk.cache_key).await {
            if let Ok(hit) = self.serve_passthrough(rk, range, prof.min_file_size).await {
                tracing::info!(key = %rk.cache_key, size = hit.meta.size, "passthrough response");
                // Always 206: this path only runs with a range, and its hit
                // always carries the honoured content-range. The source is
                // the hit's own: stage when every byte was local, upstream
                // when any open happened.
                return Ok(ServeOutcome::Stream(stream_plan(
                    hit.meta,
                    hit.content_range,
                    hit.content_length,
                    hit.body,
                    false,
                    hit.source,
                )));
            }
        }

        // 4. The ordinary cached path.
        let hit = self.get_resolved(rk, range).await?;
        tracing::info!(
            key = %rk.cache_key,
            outcome = ?hit.outcome,
            source = hit.source.label(),
            size = hit.meta.size,
            "cache response"
        );
        Ok(ServeOutcome::Stream(stream_plan(
            hit.meta,
            hit.content_range,
            hit.content_length,
            hit.body,
            hit.outcome == CacheOutcome::Stale,
            hit.source,
        )))
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
                Ok(stat) => {
                    // Modified (or etag vanished): forced refetch below; the
                    // old file keeps serving other readers until the rename.
                    // The refetch is a cold pull in every respect, so it gets
                    // the same admission: an object that has grown past what
                    // the disk can hold must not be fetched into nothing.
                    let live = stat.meta;
                    if !self.magazine.make_room(live.size_bytes).await {
                        tracing::warn!(
                            key = %key,
                            want = live.size_bytes,
                            "serving without caching: the disk cannot hold this object"
                        );
                        return Ok(self
                            .serve_upstream_range(&slot, rk, range, live)
                            .await?
                            .into_cache_hit(CacheOutcome::Miss));
                    }
                    let oversize = !self.magazine.fits(live.size_bytes);
                    return self.forced_fetch(slot, rk, range, live, oversize).await;
                }
                Err(BackendError::NotFound) => {
                    self.magazine
                    .install_negative(&key, &upstream_id, self.clock.now_millis())
                    .await;
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

        // Cold miss. The object's size decides whether the cache may hold it
        // at all, so the stat comes first — coalesced, and one PROPFIND per
        // key however many requests stampede here. The decision must be made
        // BEFORE the flight exists: a flight can only refuse by failing its
        // readers, and a cache that cannot hold an object must still serve it
        // (ADR-0013).
        let bkey = Key::from_validated(backend_key);
        let meta = match self.stat_coalesced(&slot, &key, &bkey).await {
            Ok(m) => m,
            Err(BackendError::NotFound) => {
                self.magazine
                    .install_negative(&key, &upstream_id, self.clock.now_millis())
                    .await;
                return Err(BackendError::NotFound);
            }
            Err(e) => return Err(e),
        };
        if !self.magazine.make_room(meta.size_bytes).await {
            tracing::warn!(
                key = %key,
                want = meta.size_bytes,
                "serving without caching: the disk cannot hold this object"
            );
            return Ok(self
                .serve_upstream_range(&slot, rk, range, meta)
                .await?
                .into_cache_hit(CacheOutcome::Miss));
        }
        // Too big for the magazine is not too big to keep: it is admitted as
        // a resident stray, which is the only way a large object gets a
        // single upstream stream instead of one open per shard (ADR-0014).
        let oversize = !self.magazine.fits(meta.size_bytes);
        let flight = self
            .attach_or_start(&key, bkey, &upstream_id, Arc::clone(&slot), meta, oversize)
            .await;
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
        let (offset, end) = resolve_range(range, size)?;
        let len = end - offset;
        let content_range = range.map(|_| ContentRange { first: offset, last: end - 1, total: size });
        Ok(Some(CacheHit {
            outcome,
            meta: hit_meta_entry(key, &m),
            content_range,
            content_length: Some(len),
            body: flight::file_body(path, offset, len),
            source: BodySource::Disk,
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
        meta: ObjectMeta,
        oversize: bool,
    ) -> Arc<FlightShared> {
        let entry_key = key.to_string();
        let driver_up = upstream_id.to_string();
        let cfg = Arc::clone(&self.config);
        let magazine = self.magazine.clone();
        let clock = Arc::clone(&self.clock);
        self.flights
            .join_or_start(
                key,
                store::tmp_path(&self.config.cache_dir, key),
                store::file_path(&self.config.cache_dir, key),
                move |f| {
                    drive_flight(ColdPullDriver {
                        flight: f,
                        slot,
                        backend_key,
                        entry_key,
                        upstream_id: driver_up,
                        config: cfg,
                        magazine,
                        clock,
                        meta,
                        oversize,
                    })
                },
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
                                source: BodySource::Upstream,
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
                                source: BodySource::Upstream,
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
                        self.magazine
                            .install_negative(key, upstream_id, self.clock.now_millis())
                            .await;
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
        rk: &ResolvedKey,
        range: Option<crate::backend::ByteRange>,
        meta: ObjectMeta,
        oversize: bool,
    ) -> Result<CacheHit, BackendError> {
        let key = rk.cache_key.clone();
        let entry_key = key.clone();
        let backend_key = Key::from_validated(rk.backend_key.clone());
        let upstream_id = rk.upstream_id.clone();
        let cfg = Arc::clone(&self.config);
        let magazine = self.magazine.clone();
        let clock = Arc::clone(&self.clock);
        let flight = self.flights.spawn_solo(
            store::tmp_path(&self.config.cache_dir, &key),
            store::file_path(&self.config.cache_dir, &key),
            move |f| {
                drive_flight(ColdPullDriver {
                    flight: f,
                    slot,
                    backend_key,
                    entry_key,
                    upstream_id,
                    config: cfg,
                    magazine,
                    clock,
                    meta,
                    oversize,
                })
            },
        );
        self.await_flight(flight, &key, &rk.upstream_id, range).await
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

    /// Drive both reapers: inactive expiry + max_size LRU. Called by `tick()`.
    /// Lock discipline is ORDERING, not exclusion: the coverage mutex is
    /// always taken BEFORE any state guard, and never the reverse. Every
    /// site follows it (finalize 1959→2001, reset 2020→2022, and the
    /// staged-victim block below, which holds coverage across a state READ).
    /// Inverting the order deadlocks. Nothing enforces this but the layout —
    /// see ADR-0008 and the magazine/staging module split that is meant to
    /// make it structural.
    pub async fn tick(&self) {
        let now = self.clock.now_millis();
        let ttl_ms = self.config.inactive_ttl_secs * 1000;
        // 1. Decide expiry under each lock separately.
        let do_sweep = {
            let s = self.state.read().await;
            now.saturating_sub(s.segment_sweep_at_millis) >= ttl_ms
        };
        let expired: Vec<String> = if do_sweep {
            self.staging.expired(ttl_ms, now).await
        } else {
            Vec::new()
        };
        // 1b. Staged bytes share the disk budget (P56): `segment_bytes`
        //     used to be bounded only by time (inactive_ttl), so an
        //     efficient-profile scrub session could stage far more than
        //     max_size_bytes while total_bytes stayed at zero. The ledger's
        //     own evictor owns the whole operation — picking spans, deleting
        //     their files, subtracting their bytes (ADR-0015) — so this call
        //     site never holds a span list it could route into the row-level
        //     delete path below; that routing deleted every remaining span
        //     of an evicted key and charged its bytes twice.
        let overrun = self.magazine.staged_overrun(self.state.read().await.segment_bytes).await;
        let (staged_keys, stage_freed) = if overrun > 0 {
            self.staging.evict_staged(overrun, now).await
        } else {
            (0, 0)
        };
        if staged_keys > 0 {
            tracing::info!(
                keys = staged_keys,
                bytes = stage_freed,
                policy = ?self.config.eviction_policy,
                "evicted staged spans to stay inside the magazine"
            );
        }

        // 2. Filesystem deletes hold no locks — and run off the async
        // runtime (blocking read_dir/remove_file in spawn_blocking).
        // Only age-expired rows reach this path: staged-byte eviction is
        // span-level and has already deleted its own files.
        let victims: Vec<String> = expired.clone();
        // `segment_bytes` is accounted from the LEDGER (finalize_coverage
        // adds to it, scan_segments rebuilds it), so the age sweep subtracts
        // what the disk scan finds for the rows it drops.
        let freed = {
            let cache_dir = self.config.cache_dir.clone();
            let victims = victims.clone();
            tokio::task::spawn_blocking(move || {
                // One top-level index for the whole batch (P4): the old
                // shape walked the entire cache once per expired key.
                let index = store::segment_index(&cache_dir);
                let mut freed: u64 = 0;
                for key in &victims {
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
                // Abandoned cold-pull temps are swept every tick, not only
                // at boot (P56): a mid-write failure leaves the tmp behind
                // and the next restart could be days away.
                let _ = store::cleanup_stale_tmps(&cache_dir, ttl_ms, now);
                freed
            })
            .await
            .unwrap_or(0)
        };
        // 3. State mutation — memory only, no awaits under the write
        // guard (C3); deletes for reaped/evicted rows run guard-free.
        let reaped = self.magazine.reap(ttl_ms, now).await;
        // `freed` is what the age sweep removed from disk. Span eviction
        // subtracted its own bytes inside the staging module, so this is the
        // only place the two accounts could ever meet — keep them added, not
        // max-ed: they are disjoint (expired rows vs. evicted spans of live
        // rows) and a max would silently forgive a real shortage.
        if do_sweep || freed > 0 {
            let mut s = self.state.write().await;
            if do_sweep {
                s.segment_sweep_at_millis = now;
            }
            s.segment_bytes = s.segment_bytes.saturating_sub(freed);
        }
        self.magazine.delete(&reaped).await;
        let evicted = self.magazine.evict_budget(now).await;
        self.magazine.delete(&evicted).await;
        // 3b. Disk pressure (ADR-0014). Resident strays sit outside the byte
        //     budget, so nothing else bounds how much of the disk they take;
        //     without this the only signal would be a cold pull that cannot
        //     find room. Reclaim them down to the working floor, oldest
        //     touched first. Strays only: the byte budget already governs
        //     the magazine's own members, and freeing resident bytes would
        //     make the disk a second, silent eviction budget for them.
        let pressure_victims = self.magazine.reclaim_under_pressure().await;
        self.magazine.delete(&pressure_victims).await;
        // 4. Ledger removal (coverage only) for the age sweep. Staged-byte
        //    eviction removes the rows it empties itself, inside the module
        //    that owns the ledger.
        if do_sweep && !expired.is_empty() {
            self.staging.drop_rows(&expired).await;
        }
        // 5. Publish what the cache is holding for viewers (ADR-0018). These
        //    are gauges, not counters: the question they answer is "how much
        //    of the budget is currently spoken for by viewing sessions", and a
        //    pin whose owner never comes back must fall on its own.
        crate::metrics::set_watch(self.watches.active(now), self.watches.pinned_bytes(now));
    }
}
/// Everything one cold-miss driver needs, as one receiver: the flight it
/// pumps, where the bytes go, and the metadata the caller's stat produced
/// (admission is decided before a flight exists, ADR-0013, so the GET is
/// issued exactly once).
struct ColdPullDriver<C: Clock> {
    flight: Arc<FlightShared>,
    slot: Arc<BackendSlot>,
    backend_key: Key,
    entry_key: String,
    upstream_id: String,
    config: Arc<Config>,
    magazine: Magazine,
    clock: Arc<C>,
    meta: ObjectMeta,
    oversize: bool,
}

/// One cold-miss download, driven to completion: capacity backstop → pump →
/// install.
async fn drive_flight<C: Clock>(
    ColdPullDriver { flight, slot, backend_key, entry_key, upstream_id, config, magazine, clock, meta, oversize }: ColdPullDriver<C>,
) {
    let outcome = async {
        let _stream_permit = slot.stream_gate.acquire().await;
        // Last-resort capacity backstop (P56): the caller has already made
        // room (`magazine::make_room`) against this same probe, so this only
        // fires when the disk filled in the window between the two. Kept
        // because the alternative — a mid-pull write failure — leaves a
        // truncated body AND a leaked temp file; a flight cannot switch to
        // serving-without-caching after it has promised bytes.
        if !store::has_room_for(&config.cache_dir, meta.size_bytes, magazine::DISK_RESERVE_BYTES) {
            let free = store::free_bytes(&config.cache_dir).unwrap_or(0);
            tracing::warn!(
                key = %entry_key,
                want = meta.size_bytes,
                free,
                "cold pull refused: not enough free space"
            );
            return Err(BackendError::ServerError(format!(
                "insufficient disk space: need {} bytes, {free} free",
                meta.size_bytes
            )));
        }
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
            magazine
                .install(&entry_key, &upstream_id, &meta, clock.now_millis(), oversize)
                .await;
            let _ = flight.progress_tx.send(FlightProgress::Done);
        }
        Err(e) => {
            let _ = flight.progress_tx.send(FlightProgress::Failed(e));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{clock::MockClock, config::Config};
    use std::sync::atomic::AtomicUsize;
    use crate::testsupport::CacheTestExt;
    use tempfile::tempdir;




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
        let backend =
            crate::testsupport::MockBackend::counting(bytes.to_vec(), etag.map(|s| s.to_string()), Arc::clone(&calls), fail);
        let mut slots = HashMap::new();
        slots.insert(
            "primary".to_string(),
            Arc::new(BackendSlot::new(Arc::new(backend), 3)),
        );
        let cache = Cache::new(Arc::clone(&cfg), Arc::clone(&clock), BackendRegistry::new(slots));
        (cfg, clock, cache, calls)
    }

    /// A store row left behind by the rebuild bug -- including the quarantine
    /// archive, whose name the layout derives from the store -- must be
    /// dropped with the FILE intact. The reaper turns a key back into a
    /// deletion, so keeping one of these rows is what let the node destroy
    /// its own metadata store.
    ///
    /// A NESTED row that merely shares the name is a real object and must
    /// survive: the node's log showed `googledrive1/redb.db` being treated as
    /// the store.
    #[tokio::test]
    async fn load_drops_store_rows_and_never_deletes_the_file() {
        let dir = tempdir().unwrap();
        let store = crate::cache::persist::MetaStore::open(&dir.path().join(store::META_STORE_FILE)).unwrap();
        for key in [store::META_STORE_FILE, "redb.db.corrupt-1789556382", "bucket/redb.db"] {
            let path = dir.path().join(key);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, b"database bytes").unwrap();
            store
                .insert(&EntryMeta {
                    version: 1,
                    upstream_id: "primary".into(),
                    key: key.into(),
                    size_bytes: 14,
                    etag: None,
                    last_modified: None,
                    content_type: None,
                    created_at_millis: 0,
                    last_access_millis: 0,
                    last_revalidated_millis: None,
                    negative_until_millis: None,
                    oversize: false,
                })
                .await
                .unwrap();
        }

        let (_cfg, _clock, cache, _calls) = test_cache(dir.path().to_path_buf(), b"x", None, None);
        Arc::new(cache).load_and_start().await;

        let reloaded = crate::cache::persist::MetaStore::open(&dir.path().join(store::META_STORE_FILE))
            .unwrap()
            .load_all()
            .await
            .unwrap();
        assert!(
            reloaded.iter().all(|m| !store::is_reserved_key(&m.key)),
            "store rows must be dropped at load: {:?}",
            reloaded.iter().map(|m| &m.key).collect::<Vec<_>>()
        );
        assert!(
            reloaded.iter().any(|m| m.key == "bucket/redb.db"),
            "a nested object that shares the store's name must survive: {:?}",
            reloaded.iter().map(|m| &m.key).collect::<Vec<_>>()
        );
        for key in [store::META_STORE_FILE, "redb.db.corrupt-1789556382", "bucket/redb.db"] {
            assert!(dir.path().join(key).exists(), "{key} must survive the load");
        }
    }

    #[tokio::test]
    async fn miss_then_hit() {
        let dir = tempdir().unwrap();
        let (_cfg, _clock, cache, _calls) = test_cache(dir.path().to_path_buf(), b"hello", Some("v1"), None);
        let mut hit = cache.get_by_key("a.png", None).await.unwrap();
        assert_eq!(hit.outcome, CacheOutcome::Miss);
        let b = crate::testsupport::collect(&mut hit.body).await;
        assert_eq!(b, b"hello");
        // wait for the driver to seal + install
        for _ in 0..100 {
            if cache.state.read().await.entries.contains_key("a.png") {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let mut hit2 = cache.get_by_key("a.png", None).await.unwrap();
        assert_eq!(hit2.outcome, CacheOutcome::Hit);
        let b2 = crate::testsupport::collect(&mut hit2.body).await;
        assert_eq!(b2, b"hello");
    }

    /// The byte-source label comes from the path that produced the bytes,
    /// not from the outcome: a cold miss is a live upstream stream, and the
    /// same key's next read is this node's disk. This is the label the body
    /// metrics carry, and the only fact a "how much do we serve ourselves"
    /// question needs.
    #[tokio::test]
    async fn serve_labels_the_bytes_with_their_source() {
        let dir = tempdir().unwrap();
        let (_cfg, _clock, cache, _calls) = test_cache(dir.path().to_path_buf(), b"hello", Some("v1"), None);
        let cache = Arc::new(cache);
        let rk = cache.resolve("a.png").unwrap();
        assert!(
            !cache.state.read().await.entries.contains_key("a.png"),
            "the first serve must really be a miss, or this test proves nothing"
        );
        let plan = match cache.serve(&rk, None, None).await.unwrap() {
            ServeOutcome::Stream(plan) => plan,
            _ => panic!("a cold miss must stream, not redirect"),
        };
        assert_eq!(
            plan.source,
            BodySource::Upstream,
            "the first read is pulled through the flight"
        );
        let mut body = plan.body;
        assert_eq!(crate::testsupport::collect(&mut body).await, b"hello");
        for _ in 0..100 {
            if cache.state.read().await.entries.contains_key("a.png") {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let plan2 = match cache.serve(&rk, None, None).await.unwrap() {
            ServeOutcome::Stream(plan) => plan,
            _ => panic!("a hit must stream"),
        };
        assert_eq!(
            plan2.source,
            BodySource::Disk,
            "the second read is served from the file on this node"
        );
    }

    #[tokio::test]
    async fn stale_if_error_serves_cached() {
        let dir = tempdir().unwrap();
        let (cfg, clock, cache, _calls) = test_cache(dir.path().to_path_buf(), b"cached", Some("v1"), None);
        let mut hit = cache.get_by_key("a.png", None).await.unwrap();
        assert_eq!(crate::testsupport::collect(&mut hit.body).await, b"cached");
        for _ in 0..100 {
            if cache.state.read().await.entries.contains_key("a.png") {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        // Swap state into a cache whose backend 500s (same dir + clock).
        let state = RwLock::new(std::mem::take(&mut *cache.state.write().await));
        let calls2 = Arc::new(AtomicUsize::new(0));
        let backend2 = crate::testsupport::MockBackend::counting(b"ignored".to_vec(), None, Arc::clone(&calls2), Some(BackendError::ServerError("boom".into())));
        let mut slots = HashMap::new();
        slots.insert(
            "primary".to_string(),
            Arc::new(BackendSlot::new(Arc::new(backend2), 3)),
        );
        let cache2 = Cache::new(Arc::clone(&cfg), Arc::clone(&clock), BackendRegistry::new(slots));
        {
            let mut s2 = cache2.state.write().await;
            *s2 = std::mem::take(&mut *state.write().await);
        }
        clock.advance(61_000);
        let mut hit2 = cache2.get_by_key("a.png", None).await.unwrap();
        assert_eq!(hit2.outcome, CacheOutcome::Stale);
        assert_eq!(crate::testsupport::collect(&mut hit2.body).await, b"cached");
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
        let backend = crate::testsupport::MockBackend::counting(b"x".to_vec(), None, Arc::clone(&calls), None);
        let mut slots = HashMap::new();
        slots.insert(
            "primary".to_string(),
            Arc::new(BackendSlot::new(Arc::new(backend), 3)),
        );
        let cache = Cache::new(cfg, Arc::clone(&clock), BackendRegistry::new(slots));
        let mut hit = cache.get_by_key("a.png", None).await.unwrap();
        crate::testsupport::collect(&mut hit.body).await;
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
