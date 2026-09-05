use std::{collections::HashMap, sync::Arc};
use tokio::sync::{RwLock, Semaphore};

use crate::{
    backend::{BackendError, BackendRegistry, ByteRange, Key, ObjectMeta, StorageBackend},
    cache::{meta::EntryMeta, store},
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

/// Data returned by a winning cold-miss flight: bytes (P2 interim —
/// becomes a streaming ticket in the next increment) + provider metadata.
#[derive(Debug, Clone)]
pub struct Fetched {
    pub bytes: Vec<u8>,
    pub meta: ObjectMeta,
}

/// Data produced by a winning revalidation flight (stat only).
#[derive(Debug, Clone)]
pub struct StatData {
    pub meta: ObjectMeta,
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
    pub state: RwLock<CacheState>,
    pub inflight: Inflight<Fetched, BackendError>,
    pub reval_inflight: Inflight<StatData, BackendError>,
    pub routes: RouteTable,
}

impl<C: Clock + Clone> Cache<C> {
    pub fn new(config: Arc<Config>, clock: Arc<C>, backends: BackendRegistry) -> Self {
        let routes = config.routes.clone();
        Self {
            config,
            clock,
            backends,
            state: RwLock::new(CacheState::default()),
            inflight: Inflight::new(),
            reval_inflight: Inflight::new(),
            routes,
        }
    }

    pub fn resolve_upstream(&self, raw_key: &str) -> Result<(String, String), crate::key::KeyError> {
        let key = validate_key(raw_key)?;
        let upstream = self.routes.resolve(&key).to_string();
        Ok((key, upstream))
    }

    /// Main entry: `GET /<key>` — returns bytes + outcome for observability.
    /// Cold misses and revalidation stats are coalesced per-key (spec §3.2);
    /// upstream concurrency is gated per upstream slot (spec §4).
    pub async fn get(&self, raw_key: &str) -> Result<(Vec<u8>, CacheOutcome), BackendError> {
        let (key, upstream_id) =
            self.resolve_upstream(raw_key).map_err(|e| BackendError::Other(format!("invalid key: {e}")))?;
        let slot = self
            .backends
            .get(&upstream_id)
            .ok_or_else(|| BackendError::Other(format!("unknown upstream {upstream_id}")))?;
        let now = self.clock.now_millis();

        // Negative cache check (cheap read, not single-flighted).
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
        let mut force_fetch = false;

        if !needs_revalidate {
            if let Some(bytes) = self.try_serve_cached(&key).await {
                self.bump_last_access(&key).await;
                return Ok((bytes, CacheOutcome::Hit));
            }
        } else {
            // Revalidation = stat + etag compare (G2 #11: Drive has no 304,
            // Graph could use If-None-Match but stat-compare is uniform).
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
                    if let Some(bytes) = self.try_serve_cached(&key).await {
                        return Ok((bytes, CacheOutcome::Revalidated));
                    }
                }
                Ok(_) => {
                    // Modified (or etag vanished) → full fetch path below,
                    // forced (the in-flight disk double-check must not
                    // serve the stale bytes we just detected as changed).
                    force_fetch = true;
                }
                Err(BackendError::NotFound) => {
                    // Upstream lost the file: install a negative tombstone.
                    self.install_negative(&key, &upstream_id).await;
                    return Err(BackendError::NotFound);
                }
                Err(e) => {
                    if let Some(bytes) = self.try_serve_cached(&key).await {
                        return Ok((bytes, CacheOutcome::Stale));
                    }
                    return Err(e);
                }
            }
        }

        // Cold miss (or stale revalidate that found modification) — single-
        // flight full fetch, gated by the upstream semaphore. The closure
        // re-checks the disk cache first: under a sequential stampede the
        // previous flight may have installed the file between this caller's
        // outer check and its flight entry (double-check-under-lock).
        // Install happens INSIDE the flight so the cell outlives the disk
        // write — no window where the cell is gone but the file missing.
        // Metadata is fetched first (spec §3.1: Content-Length goes out up
        // front).
        let key_for_run = key.clone();
        let upstream_for_run = upstream_id.clone();
        let fetched = self
            .inflight
            .run(key.clone(), move || {
                let slot = Arc::clone(&slot);
                let k = Key::from_validated(key_for_run.clone());
                let this = self;
                let up = upstream_for_run.clone();
                async move {
                    if !force_fetch {
                        if let Some(bytes) = this.try_serve_cached(k.as_str()).await {
                            if let Some(m) = this.entry_meta(k.as_str()).await {
                                return Ok(Fetched {
                                    bytes,
                                    meta: ObjectMeta {
                                        size_bytes: m.size_bytes,
                                        etag: m.etag,
                                        last_modified: m.last_modified,
                                        mime_hint: m.content_type,
                                    },
                                });
                            }
                        }
                    }
                    let _permit = slot.gate.acquire().await;
                    let meta = slot.backend.stat(&k).await?;
                    let src = slot.backend.open(&k, None).await?;
                    let mut bytes = Vec::new();
                    let mut stream = src.stream;
                    tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut bytes)
                        .await
                        .map_err(|e| BackendError::ServerError(format!("read stream: {e}")))?;
                    let fetched = Fetched { bytes, meta };
                    this.install(k.as_str(), &up, &fetched).await;
                    Ok(fetched)
                }
            })
            .await;

        match fetched {
            Ok(fetched) => Ok((fetched.bytes, CacheOutcome::Miss)),
            Err(BackendError::NotFound) => {
                self.install_negative(&key, &upstream_id).await;
                Err(BackendError::NotFound)
            }
            Err(e) => {
                if let Some(bytes) = self.try_serve_cached(&key).await {
                    Ok((bytes, CacheOutcome::Stale))
                } else {
                    Err(e)
                }
            }
        }
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

    async fn install(&self, key: &str, upstream_id: &str, fetched: &Fetched) {
        let now = self.clock.now_millis();
        let path = store::file_path(&self.config.cache_dir, key);
        let tmp = store::tmp_path(&self.config.cache_dir, key);
        if let Err(e) = async {
            if let Some(parent) = tmp.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            tokio::fs::write(&tmp, &fetched.bytes).await?;
            store::install_tmp(&tmp, &path).map_err(|e| std::io::Error::other(e.to_string()))?;
            Ok::<(), std::io::Error>(())
        }
        .await
        {
            tracing::error!(key = %key, error = %e, "cache install failed");
        }
        let mut s = self.state.write().await;
        let old_size = s.entries.get(key).map(|m| m.size_bytes).unwrap_or(0);
        let meta = EntryMeta {
            version: 1,
            upstream_id: upstream_id.to_string(),
            key: key.to_string(),
            size_bytes: fetched.bytes.len() as u64,
            etag: fetched.meta.etag.clone(),
            last_modified: fetched.meta.last_modified.clone(),
            content_type: fetched.meta.mime_hint.clone(),
            created_at_millis: s.entries.get(key).map(|m| m.created_at_millis).unwrap_or(now),
            last_access_millis: now,
            last_revalidated_millis: Some(now),
            negative_until_millis: None,
        };
        s.total_bytes = s.total_bytes.saturating_sub(old_size) + meta.size_bytes;
        s.entries.insert(key.to_string(), meta);
        self.evict_if_needed(&mut s).await;
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

    async fn evict_if_needed(&self, state: &mut CacheState) {
        while state.total_bytes > self.config.max_size_bytes && !state.entries.is_empty() {
            let victim = state
                .entries
                .iter()
                .filter(|(_, m)| m.negative_until_millis.is_none())
                .min_by_key(|(_, m)| m.eligible_at(self.config.inactive_ttl_secs))
                .map(|(k, _)| k.clone());
            if let Some(k) = victim {
                if let Some(m) = state.entries.remove(&k) {
                    state.total_bytes = state.total_bytes.saturating_sub(m.size_bytes);
                    let path = store::file_path(&self.config.cache_dir, &k);
                    let _ = tokio::fs::remove_file(&path).await;
                    store::prune_empty_parents(&self.config.cache_dir, &path);
                } else {
                    break;
                }
            } else {
                break;
            }
        }
    }

    pub async fn tick(&self) {
        let now = self.clock.now_millis();
        let ttl_ms = self.config.inactive_ttl_secs * 1000;
        let mut s = self.state.write().await;
        let expired: Vec<String> = s
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
            if let Some(m) = s.entries.remove(&k) {
                s.total_bytes = s.total_bytes.saturating_sub(m.size_bytes);
                let path = store::file_path(&self.config.cache_dir, &k);
                let _ = tokio::fs::remove_file(&path).await;
            }
        }
        self.evict_if_needed(&mut s).await;
    }

    pub async fn range(&self, key: &str, start: u64, end: Option<u64>) -> Option<Vec<u8>> {
        let bytes = self.try_serve_cached(key).await?;
        let end = end.unwrap_or(bytes.len() as u64);
        if start >= bytes.len() as u64 || end > bytes.len() as u64 || start >= end {
            return None;
        }
        Some(bytes[start as usize..end as usize].to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{clock::MockClock, config::Config};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::tempdir;

    /// Test backend: fixed bytes, injectable failures, call counter.
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

        async fn open(&self, _key: &Key, _range: Option<ByteRange>) -> Result<crate::backend::StreamSource, BackendError> {
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
            Arc::new(crate::backend::BackendSlot {
                backend: Arc::new(backend),
                gate: Arc::new(Semaphore::new(3)),
            }),
        );
        let cache = Cache::new(Arc::clone(&cfg), Arc::clone(&clock), BackendRegistry::new(slots));
        (cfg, clock, cache, calls)
    }

    #[tokio::test]
    async fn miss_then_hit() {
        let dir = tempdir().unwrap();
        let (_cfg, _clock, cache, _calls) = test_cache(dir.path().to_path_buf(), b"hello", Some("v1"), None);
        let (b, out) = cache.get("a.png").await.unwrap();
        assert_eq!(out, CacheOutcome::Miss);
        assert_eq!(b, b"hello");
        let (b2, out2) = cache.get("a.png").await.unwrap();
        assert_eq!(out2, CacheOutcome::Hit);
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
            Arc::new(crate::backend::BackendSlot {
                backend: Arc::new(backend),
                gate: Arc::new(Semaphore::new(3)),
            }),
        );
        let cache = Cache::new(cfg, Arc::clone(&clock), BackendRegistry::new(slots));
        assert!(matches!(cache.get("missing.png").await, Err(BackendError::NotFound)));
        assert!(matches!(cache.get("missing.png").await, Err(BackendError::NotFound)));
        clock.advance(3000);
        cache.tick().await;
        assert!(matches!(cache.get("missing.png").await, Err(BackendError::NotFound)));
    }

    #[tokio::test]
    async fn stale_if_error_serves_cached() {
        let dir = tempdir().unwrap();
        let (cfg, clock, cache, _calls) = test_cache(dir.path().to_path_buf(), b"cached", Some("v1"), None);
        cache.get("a.png").await.unwrap();
        // Swap state into a cache whose backend 500s (same dir + clock),
        // advance past revalidate ttl, then expect stale-if-error.
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
            Arc::new(crate::backend::BackendSlot {
                backend: Arc::new(backend2),
                gate: Arc::new(Semaphore::new(3)),
            }),
        );
        let cache2 = Cache::new(Arc::clone(&cfg), Arc::clone(&clock), BackendRegistry::new(slots));
        {
            let mut s2 = cache2.state.write().await;
            *s2 = std::mem::take(&mut *state.write().await);
        }
        clock.advance(61_000);
        let (b, out) = cache2.get("a.png").await.unwrap();
        assert_eq!(out, CacheOutcome::Stale);
        assert_eq!(b, b"cached");
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
            Arc::new(crate::backend::BackendSlot {
                backend: Arc::new(backend),
                gate: Arc::new(Semaphore::new(3)),
            }),
        );
        let cache = Cache::new(cfg, Arc::clone(&clock), BackendRegistry::new(slots));
        cache.get("a.png").await.unwrap();
        assert!(dir.path().join("a.png").exists());
        clock.advance(2000);
        cache.tick().await;
        assert!(!dir.path().join("a.png").exists());
    }
}
