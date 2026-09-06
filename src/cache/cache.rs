use std::{collections::HashMap, sync::Arc};
use tokio::sync::{Mutex, RwLock};

use crate::{
    backend::{BackendError, BackendRegistry, BackendSlot, Key, ObjectMeta},
    cache::{
        flight::{self, BodyStream, FlightProgress, FlightShared},
        meta::EntryMeta,
        store,
    },
    clock::Clock,
    config::Config,
    inflight::Inflight,
    key::validate_key,
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

fn hit_meta_entry(key: &str, m: &EntryMeta) -> HitMeta {
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
/// cold-miss Range passthrough per §3.8). `content_length` is the exact
/// byte count the body will deliver when known up front (S3 shape: the
/// business plane renders it as `Content-Length` instead of chunked).
pub struct CacheHit {
    pub outcome: CacheOutcome,
    pub meta: HitMeta,
    pub content_range: Option<String>,
    pub content_length: Option<u64>,
    pub body: BodyStream,
}

pub struct CacheState {
    pub entries: HashMap<String, EntryMeta>,
    pub total_bytes: u64,
}

impl Default for CacheState {
    fn default() -> Self {
        Self { entries: HashMap::new(), total_bytes: 0 }
    }
}

pub struct Cache<C: Clock> {
    pub config: Arc<Config>,
    pub clock: Arc<C>,
    pub backends: BackendRegistry,
    pub state: Arc<RwLock<CacheState>>,
    /// redb-backed metadata persistence (spec §3.10: entries + access clock
    /// + eviction order survive restarts).
    pub meta: Arc<crate::cache::persist::MetaStore>,
    /// Access-clock bumps awaiting the coalesced flush (R1: per-hit fsync
    /// would bottleneck; flush at most once per second).
    pub dirty_access: Arc<Mutex<HashMap<String, u64>>>,
    /// In-flight cold-miss downloads, keyed by cache key. Inserted
    /// synchronously before any await so every concurrent caller attaches
    /// to the same flight (no TOCTOU stampede window).
    pub flights: Arc<Mutex<HashMap<String, Arc<FlightShared>>>>,
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
        let dirty_access = Arc::new(Mutex::new(HashMap::new()));
        Self {
            config,
            clock,
            backends,
            state: Arc::new(RwLock::new(CacheState::default())),
            meta,
            dirty_access,
            flights: Arc::new(Mutex::new(HashMap::new())),
            reval_inflight: Inflight::new(),
            routes,
        }
    }

    /// Startup: load persisted entries (drop rows whose file vanished),
    /// sweep partial-download temps, and start the coalesced flush task.
    pub async fn load_and_start(&self) {
        // Startup self-heal (spec §3.10): temp files from crashed downloads.
        let _ = store::cleanup_tmps(&self.config.cache_dir);

        let persisted = self.meta.load_all().await.unwrap_or_default();
        let mut state = self.state.write().await;
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
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(1000));
            loop {
                tick.tick().await;
                let batch: Vec<(String, u64)> = {
                    let mut d = dirty.lock().await;
                    d.drain().collect()
                };
                for (key, ms) in batch {
                    if let Err(e) = meta.bump_last_access(&key, ms).await {
                        tracing::warn!(key = %key, error = %e, "access-clock flush failed");
                    }
                }
            }
        });
    }

    pub fn resolve_upstream(&self, raw_key: &str) -> Result<(String, String), crate::key::KeyError> {
        let key = validate_key(raw_key)?;
        let upstream = self.routes.resolve(&key).to_string();
        Ok((key, upstream))
    }

    /// HEAD-grade metadata lookup: memory entry when fresh, else a single
    /// upstream `stat` (a HEAD must be current — stale rows still cost one
    /// stat, but bytes never move: no flights, no file reads, no file-row
    /// installs; a confirmed absence installs a negative tombstone so
    /// HEAD 404s share the negative-cache window with GET).
    pub async fn head_meta(&self, raw_key: &str) -> Result<HitMeta, BackendError> {
        let (key, upstream_id) =
            self.resolve_upstream(raw_key).map_err(|e| BackendError::Other(format!("invalid key: {e}")))?;
        self.head_meta_pinned(key.clone(), key, upstream_id).await
    }

    /// Same as [`Cache::head_meta`] with the upstream pre-pinned (S3
    /// bucket alias + suffix-range size resolution). Same two-namespace
    /// split as [`Cache::get_pinned`]: `cache_key` for memory rows,
    /// `backend_key` for the upstream stat.
    pub async fn head_meta_pinned(
        &self,
        cache_key: String,
        backend_key: String,
        upstream_id: String,
    ) -> Result<HitMeta, BackendError> {
        let key = validate_key(&cache_key).map_err(|e| BackendError::Other(format!("invalid key: {e}")))?;
        let backend_key = validate_key(&backend_key).map_err(|e| BackendError::Other(format!("invalid key: {e}")))?;
        let now = self.clock.now_millis();
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
                        let hit = hit_meta_entry(&key, meta);
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
    /// one primitive, two callers.
    pub async fn prefetch(&self, full: String, pinned: Option<(String, String)>) -> Result<(), BackendError> {
        let mut hit = match pinned {
            Some((backend, upstream)) => self.get_pinned(full, backend, upstream, None).await?,
            None => self.get(&full, None).await?,
        };
        flight::drain(&mut hit.body).await?;
        Ok(())
    }

    /// Main entry: `GET /<key>` — streaming response. Cold misses attach
    /// to a shared download flight; hits stream the cached file; revalidation
    /// stats the upstream (no bytes) and compares etags. A Range request on
    /// a cached file slices locally; on a cold miss with offset > 0 the
    /// backend stream passes through at that offset while the flight fills
    /// the cache in the background (dual-channel, spec §3.8).
    pub async fn get(
        &self,
        raw_key: &str,
        range: Option<crate::backend::ByteRange>,
    ) -> Result<CacheHit, BackendError> {
        let (key, upstream_id) =
            self.resolve_upstream(raw_key).map_err(|e| BackendError::Other(format!("invalid key: {e}")))?;
        self.get_pinned(key.clone(), key, upstream_id, range).await
    }

    /// Same as [`Cache::get`] but with the upstream pre-pinned: skips route
    /// resolution. Used by the S3 path-style `/{bucket}/{key}` alias, where
    /// the bucket names the upstream directly (bucket namespace = upstream
    /// ids, zero new config).
    ///
    /// Two key namespaces: `key` is the cache identity (full request path,
    /// bucket included — state rows, flights, store paths, reval labels);
    /// `backend_key` is the provider-side object path (bucket stripped).
    /// They coincide for legacy routing; they differ only for the alias.
    /// Sharing one namespace would collide flights and entries across
    /// upstreams, so the split is load-bearing, not cosmetic.
    pub async fn get_pinned(
        &self,
        key: String,
        backend_key: String,
        upstream_id: String,
        range: Option<crate::backend::ByteRange>,
    ) -> Result<CacheHit, BackendError> {
        let key = validate_key(&key).map_err(|e| BackendError::Other(format!("invalid key: {e}")))?;
        let backend_key = validate_key(&backend_key).map_err(|e| BackendError::Other(format!("invalid key: {e}")))?;
        let slot = self
            .backends
            .get(&upstream_id)
            .ok_or_else(|| BackendError::Other(format!("unknown upstream {upstream_id}")))?;
        let now = self.clock.now_millis();

        // Negative cache (cheap read, not single-flighted).
        {
            let s = self.state.read().await;
            if let Some(meta) = s.entries.get(&key) {
                if meta.is_negative(now) {
                    return Err(BackendError::NotFound);
                }
            }
        }

        let needs_revalidate = {
            let s = self.state.read().await;
            if let Some(meta) = s.entries.get(&key) {
                if meta.negative_until_millis.is_some() {
                    false
                } else {
                    let age =
                        now.saturating_sub(meta.last_revalidated_millis.unwrap_or(meta.created_at_millis));
                    age > self.config.revalidate_ttl_secs * 1000
                }
            } else {
                false
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
            let cached_etag = {
                let s = self.state.read().await;
                s.entries.get(&key).and_then(|m| m.etag.clone())
            };
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
        self.await_flight(flight, &key, backend_key.as_str(), &upstream_id, slot, range).await
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
            Some(m) if m.negative_until_millis.is_none() => m,
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
                    Some(format!("bytes {}-{}/{}", r.offset, end - 1, size)),
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
    /// its driver. Map insertion happens before any await.
    /// `key` namespaces the flight/map entry and store paths;
    /// `backend_key` is what the driver stats/opens upstream.
    async fn attach_or_start(
        &self,
        key: &str,
        backend_key: Key,
        upstream_id: &str,
        slot: Arc<BackendSlot>,
    ) -> Arc<FlightShared> {
        let mut map = self.flights.lock().await;
        if let Some(f) = map.get(key) {
            return f.clone();
        }
        let f = Arc::new(FlightShared::new(
            store::tmp_path(&self.config.cache_dir, key),
            store::file_path(&self.config.cache_dir, key),
        ));
        map.insert(key.to_string(), f.clone());
        drop(map);
        // Driver is detached: the creator's client may disconnect without
        // affecting the download other readers are attached to.
        let driver_f = f.clone();
        let driver_slot = Arc::clone(&slot);
        let driver_up = upstream_id.to_string();
        let entry_key = key.to_string();
        let cfg = Arc::clone(&self.config);
        let state = Arc::clone(&self.state);
        let meta_store = Arc::clone(&self.meta);
        let flights = Arc::clone(&self.flights);
        let clock = Arc::clone(&self.clock);
        tokio::spawn(async move {
            drive_flight(driver_f, driver_slot, backend_key, entry_key, driver_up, cfg, state, meta_store, flights, clock).await;
        });
        f
    }

    /// Wait for a flight's metadata, then return the streaming body.
    /// Range handling (§3.8): offset 0 → the growing body itself (it starts
    /// at byte 0); offset > 0 → dual-channel passthrough from the backend at
    /// that offset while the flight fills the cache. Failed flights fall
    /// back to stale-if-error.
    async fn await_flight(
        &self,
        flight: Arc<FlightShared>,
        key: &str,
        backend_key: &str,
        upstream_id: &str,
        slot: Arc<BackendSlot>,
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
                        Some(r) if r.offset == 0 => {
                            // The growing reader already streams from byte 0.
                            let last = meta.size_bytes.saturating_sub(1);
                            return Ok(CacheHit {
                                outcome: CacheOutcome::Miss,
                                meta: meta_out,
                                content_range: Some(format!("bytes 0-{last}/{}", meta.size_bytes)),
                                content_length: Some(meta.size_bytes),
                                body: flight::growing_reader(flight),
                            });
                        }
                        Some(r) => {
                            // Dual-channel: client stream passes through at
                            // the offset; the flight keeps filling the cache.
                            if r.offset >= meta.size_bytes {
                                return Err(BackendError::RangeNotSatisfiable);
                            }
                            let _permit = slot.gate.acquire().await;
                            let src = slot.backend.open(&Key::from_validated(backend_key.to_string()), Some(r)).await?;
                            let end = r
                                .length
                                .map_or(meta.size_bytes.saturating_sub(1), |l| (r.offset + l - 1).min(meta.size_bytes - 1));
                            return Ok(CacheHit {
                                outcome: CacheOutcome::Miss,
                                meta: meta_out,
                                content_range: Some(format!("bytes {}-{}/{}", r.offset, end, meta.size_bytes)),
                                content_length: Some(end.saturating_sub(r.offset).saturating_add(1)),
                                body: flight::passthrough_body(src),
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
                    if rx.changed().await.is_err() {
                        return Err(BackendError::Other("flight ended without metadata".into()));
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
        let flight = Arc::new(FlightShared::new(
            store::tmp_path(&self.config.cache_dir, &key),
            store::file_path(&self.config.cache_dir, &key),
        ));
        let driver_f = flight.clone();
        let driver_slot = Arc::clone(&slot);
        let driver_up = upstream_id.clone();
        let backend_key = Key::from_validated(backend_key);
        let entry_key = key.clone();
        let driver_backend_key = backend_key.clone();
        let cfg = Arc::clone(&self.config);
        let state = Arc::clone(&self.state);
        let meta_store = Arc::clone(&self.meta);
        let clock = Arc::clone(&self.clock);
        tokio::spawn(async move {
            drive_flight(driver_f, driver_slot, driver_backend_key, entry_key, driver_up, cfg, state, meta_store, flights_none(), clock).await;
        });
        self.await_flight(flight, &key, backend_key.as_str(), &upstream_id, slot, range).await
    }

    async fn try_serve_cached(&self, key: &str) -> Option<Vec<u8>> {
        let path = store::file_path(&self.config.cache_dir, key);
        tokio::fs::read(&path).await.ok()
    }

    async fn entry_meta(&self, key: &str) -> Option<EntryMeta> {
        self.state.read().await.entries.get(key).cloned()
    }

    async fn bump_last_access(&self, key: &str) {
        let now = self.clock.now_millis();
        {
            let mut s = self.state.write().await;
            if let Some(m) = s.entries.get_mut(key) {
                m.last_access_millis = now;
            }
        }
        // Coalesced redb flush (R1): mark dirty; the flusher writes at most
        // once per second across all keys.
        self.dirty_access.lock().await.insert(key.to_string(), now);
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
    pub async fn tick(&self) {
        let now = self.clock.now_millis();
        let mut s = self.state.write().await;
        reap(&mut s, &self.config, &self.meta, now).await;
    }

    /// Serve a byte range from the cached file (spec §3.8).
    pub async fn range(&self, key: &str, start: u64, end: Option<u64>) -> Option<Vec<u8>> {
        let bytes = self.try_serve_cached(key).await?;
        let end = end.unwrap_or(bytes.len() as u64);
        if start >= bytes.len() as u64 || end > bytes.len() as u64 || start >= end {
            return None;
        }
        Some(bytes[start as usize..end as usize].to_vec())
    }
}

fn flights_none() -> Arc<Mutex<HashMap<String, Arc<FlightShared>>>> {
    // Forced refetches run on a private flight: no map registration needed.
    Arc::new(Mutex::new(HashMap::new()))
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
    flights: Arc<Mutex<HashMap<String, Arc<FlightShared>>>>,
    clock: Arc<C>,
) {
    let outcome = async {
        let _permit = slot.gate.acquire().await;
        let meta = slot.backend.stat(&backend_key).await?;
        let _ = flight.progress_tx.send(FlightProgress::Meta(meta.clone()));
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
    flights.lock().await.remove(entry_key.as_str());
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
    let mut s = state.write().await;
    let old_size = s.entries.get(key).map(|m| m.size_bytes).unwrap_or(0);
    let entry = EntryMeta {
        version: 1,
        upstream_id: upstream_id.to_string(),
        key: key.to_string(),
        size_bytes: meta.size_bytes,
        etag: meta.etag.clone(),
        last_modified: meta.last_modified.clone(),
        content_type: crate::mime::resolve(key, &meta.mime_hint),
        created_at_millis: s.entries.get(key).map(|m| m.created_at_millis).unwrap_or(now),
        last_access_millis: now,
        last_revalidated_millis: Some(now),
        negative_until_millis: None,
    };
    // Persist first: on crash between redb and memory, startup rebuilds
    // memory from redb; the reverse order would lose the row.
    if let Err(e) = meta_store.insert(&entry).await {
        tracing::error!(key = %key, error = %e, "redb insert failed");
    }
    s.total_bytes = s.total_bytes.saturating_sub(old_size) + entry.size_bytes;
    s.entries.insert(key.to_string(), entry);
    evict_if_needed(&mut s, config, meta_store).await;
}

/// Inactive expiry + max_size LRU over one state lock hold.
async fn reap(state: &mut CacheState, config: &Config, meta_store: &crate::cache::persist::MetaStore, now: u64) {
    let ttl_ms = config.inactive_ttl_secs * 1000;
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
    for k in expired {
        if let Some(m) = state.entries.remove(&k) {
            state.total_bytes = state.total_bytes.saturating_sub(m.size_bytes);
            let _ = meta_store.remove(&k).await;
            let path = store::file_path(&config.cache_dir, &k);
            let _ = tokio::fs::remove_file(&path).await;
        }
    }
    evict_if_needed(state, config, meta_store).await;
}

async fn evict_if_needed(state: &mut CacheState, config: &Config, meta_store: &crate::cache::persist::MetaStore) {
    while state.total_bytes > config.max_size_bytes && !state.entries.is_empty() {
        let victim = state
            .entries
            .iter()
            .filter(|(_, m)| m.negative_until_millis.is_none())
            .min_by_key(|(_, m)| m.eligible_at(config.inactive_ttl_secs))
            .map(|(k, _)| k.clone());
        if let Some(k) = victim {
            if let Some(m) = state.entries.remove(&k) {
                state.total_bytes = state.total_bytes.saturating_sub(m.size_bytes);
                let _ = meta_store.remove(&k).await;
                let path = store::file_path(&config.cache_dir, &k);
                let _ = tokio::fs::remove_file(&path).await;
                store::prune_empty_parents(&config.cache_dir, &path);
            } else {
                break;
            }
        } else {
            break;
        }
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
