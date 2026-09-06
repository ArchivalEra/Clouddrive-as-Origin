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
/// cold-miss Range passthrough per §3.8).
pub struct CacheHit {
    pub outcome: CacheOutcome,
    pub meta: HitMeta,
    pub content_range: Option<String>,
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
        Self {
            config,
            clock,
            backends,
            state: Arc::new(RwLock::new(CacheState::default())),
            flights: Arc::new(Mutex::new(HashMap::new())),
            reval_inflight: Inflight::new(),
            routes,
        }
    }

    pub fn resolve_upstream(&self, raw_key: &str) -> Result<(String, String), crate::key::KeyError> {
        let key = validate_key(raw_key)?;
        let upstream = self.routes.resolve(&key).to_string();
        Ok((key, upstream))
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
                    let k = Key::from_validated(key.clone());
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
                    return self.forced_fetch(slot, key, upstream_id, range).await;
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
        let flight = self.attach_or_start(&key, &upstream_id, Arc::clone(&slot)).await;
        self.await_flight(flight, &key, &upstream_id, CacheOutcome::Miss, slot, range).await
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
            body: flight::file_body(path, offset, len),
        }))
    }

    /// Attach to an existing flight for this key, or create one and spawn
    /// its driver. Map insertion happens before any await.
    async fn attach_or_start(
        &self,
        key: &str,
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
        let driver_key = Key::from_validated(key.to_string());
        let driver_up = upstream_id.to_string();
        let cfg = Arc::clone(&self.config);
        let state = Arc::clone(&self.state);
        let flights = Arc::clone(&self.flights);
        let clock = Arc::clone(&self.clock);
        tokio::spawn(async move {
            drive_flight(driver_f, driver_slot, driver_key, driver_up, cfg, state, flights, clock).await;
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
        upstream_id: &str,
        outcome: CacheOutcome,
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
                                outcome,
                                meta: meta_out,
                                content_range: None,
                                body: flight::growing_reader(flight),
                            });
                        }
                        Some(r) if r.offset == 0 => {
                            // The growing reader already streams from byte 0.
                            let last = meta.size_bytes.saturating_sub(1);
                            return Ok(CacheHit {
                                outcome,
                                meta: meta_out,
                                content_range: Some(format!("bytes 0-{last}/{}", meta.size_bytes)),
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
                            let src = slot.backend.open(&Key::from_validated(key.to_string()), Some(r)).await?;
                            let end = r
                                .length
                                .map_or(meta.size_bytes.saturating_sub(1), |l| (r.offset + l - 1).min(meta.size_bytes - 1));
                            return Ok(CacheHit {
                                outcome,
                                meta: meta_out,
                                content_range: Some(format!("bytes {}-{}/{}", r.offset, end, meta.size_bytes)),
                                body: flight::passthrough_body(src),
                            });
                        }
                    }
                }
                FlightProgress::Done => {
                    // Late attacher: the whole flight finished before we
                    // subscribed (watch keeps only the latest value). The
                    // file is sealed and its meta installed — serve disk.
                    if let Some(hit) = self.serve_from_disk(key, outcome, range).await? {
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
        upstream_id: String,
        range: Option<crate::backend::ByteRange>,
    ) -> Result<CacheHit, BackendError> {
        let flight = Arc::new(FlightShared::new(
            store::tmp_path(&self.config.cache_dir, &key),
            store::file_path(&self.config.cache_dir, &key),
        ));
        let driver_f = flight.clone();
        let driver_slot = Arc::clone(&slot);
        let driver_key = Key::from_validated(key.clone());
        let driver_up = upstream_id.clone();
        let cfg = Arc::clone(&self.config);
        let state = Arc::clone(&self.state);
        let clock = Arc::clone(&self.clock);
        tokio::spawn(async move {
            drive_flight(driver_f, driver_slot, driver_key, driver_up, cfg, state, flights_none(), clock).await;
        });
        self.await_flight(flight, &key, &upstream_id, CacheOutcome::Miss, slot, range).await
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
        let mut s = self.state.write().await;
        if let Some(m) = s.entries.get_mut(key) {
            m.last_access_millis = now;
        }
    }

    async fn install_negative(&self, key: &str, upstream_id: &str) {
        let now = self.clock.now_millis();
        let until = now + self.config.negative_ttl_secs * 1000;
        let mut s = self.state.write().await;
        s.entries.insert(
            key.to_string(),
            EntryMeta {
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
            },
        );
    }

    /// Drive both reapers: inactive expiry + max_size LRU. Called by `tick()`.
    pub async fn tick(&self) {
        let now = self.clock.now_millis();
        let mut s = self.state.write().await;
        reap(&mut s, &self.config, now).await;
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
/// temp file → seal (rename) → install metadata row → Done. Detached from
/// its creator so a disconnecting client never kills the download.
async fn drive_flight<C: Clock>(
    flight: Arc<FlightShared>,
    slot: Arc<BackendSlot>,
    k: Key,
    upstream_id: String,
    config: Arc<Config>,
    state: Arc<RwLock<CacheState>>,
    flights: Arc<Mutex<HashMap<String, Arc<FlightShared>>>>,
    clock: Arc<C>,
) {
    let outcome = async {
        let _permit = slot.gate.acquire().await;
        let meta = slot.backend.stat(&k).await?;
        let _ = flight.progress_tx.send(FlightProgress::Meta(meta.clone()));
        let src = slot.backend.open(&k, None).await?;
        flight::pump_and_seal(src, &flight.tmp_path, &flight.final_path, &flight.progress_tx).await?;
        Ok::<ObjectMeta, BackendError>(meta)
    }
    .await;

    match outcome {
        Ok(meta) => {
            insert_meta(&state, &config, k.as_str(), &upstream_id, &meta, clock.now_millis()).await;
            let _ = flight.progress_tx.send(FlightProgress::Done);
        }
        Err(e) => {
            let _ = flight.progress_tx.send(FlightProgress::Failed(e));
        }
    }
    flights.lock().await.remove(k.as_str());
}

async fn insert_meta(
    state: &RwLock<CacheState>,
    config: &Config,
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
    s.total_bytes = s.total_bytes.saturating_sub(old_size) + entry.size_bytes;
    s.entries.insert(key.to_string(), entry);
    evict_if_needed(&mut s, config).await;
}

/// Inactive expiry + max_size LRU over one state lock hold.
async fn reap(state: &mut CacheState, config: &Config, now: u64) {
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
            let path = store::file_path(&config.cache_dir, &k);
            let _ = tokio::fs::remove_file(&path).await;
        }
    }
    evict_if_needed(state, config).await;
}

async fn evict_if_needed(state: &mut CacheState, config: &Config) {
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
