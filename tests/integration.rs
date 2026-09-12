use std::collections::HashMap;
use std::sync::{atomic::AtomicUsize, atomic::Ordering, Arc};
use tokio::sync::Semaphore;

use tempfile::tempdir;

use origin_cache::{
    backend::{BackendError, BackendRegistry, BackendSlot, ByteRange, Key, ListEntry, ObjectMeta, StreamSource, StorageBackend},
    cache::cache::{Cache, CacheOutcome},
    cache::flight::BodyStream,
    clock::MockClock,
    config::Config,
    routing::{RouteRule, RouteTable},
};

/// Test backend: fixed bytes, injectable failures, call counter on open+stat.
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
            mime_hint: Some("application/octet-stream".into()),
        })
    }

    async fn open(&self, _key: &Key, range: Option<ByteRange>) -> Result<StreamSource, BackendError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(e) = &self.fail {
            return Err(e.clone());
        }
        let bytes: Vec<u8> = match range {
            None => self.bytes.clone(),
            Some(r) => {
                let start = r.offset as usize;
                if start > self.bytes.len() {
                    return Err(BackendError::RangeNotSatisfiable);
                }
                match r.length {
                    None => self.bytes[start..].to_vec(),
                    Some(len) => {
                        let end = (start + len as usize).min(self.bytes.len());
                        self.bytes[start..end].to_vec()
                    }
                }
            }
        };
        Ok(StreamSource {
            stream: Box::new(std::io::Cursor::new(bytes)),
            // total_len = FULL object length (trait contract), not the slice.
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

fn test_config(dir: std::path::PathBuf) -> Arc<Config> {
    let mut cfg = Config::default();
    cfg.cache_dir = dir;
    Arc::new(cfg)
}

fn registry_with(backend: Arc<dyn StorageBackend>) -> BackendRegistry {
    let mut slots = HashMap::new();
    slots.insert(
        "primary".to_string(),
        Arc::new(BackendSlot::new(backend, 3)),
    );
    BackendRegistry::new(slots)
}

async fn read_body(body: &mut BodyStream) -> Vec<u8> {
    use futures::StreamExt;
    let mut out = Vec::new();
    while let Some(chunk) = body.next().await {
        out.extend_from_slice(&chunk.unwrap());
    }
    out
}

/// Wait until the driver task has installed the metadata row for `key`.
async fn wait_installed(cache: &Cache<MockClock>, key: &str) {
    for _ in 0..200 {
        if cache.state.read().await.entries.contains_key(key) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    panic!("entry {key} never installed");
}

#[tokio::test]
async fn single_flight_20_concurrent_same_key_one_fetch() {
    let dir = tempdir().unwrap();
    let cfg = test_config(dir.path().to_path_buf());
    let clock = Arc::new(MockClock::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let backend = CountingBackend {
        bytes: b"payload".to_vec(),
        etag: Some("v1".into()),
        calls: Arc::clone(&calls),
        fail: None,
    };
    let cache = Arc::new(Cache::new(cfg, Arc::clone(&clock), registry_with(Arc::new(backend))));

    let mut handles = Vec::new();
    for _ in 0..20 {
        let c = Arc::clone(&cache);
        handles.push(tokio::spawn(async move {
            let mut hit = c.get("same.png", None).await?;
            let mut body = hit.body;
            origin_cache::cache::flight::drain(&mut body)
                .await
                .map(|_| hit.outcome)
        }));
    }
    for h in handles {
        assert_eq!(h.await.unwrap().unwrap(), CacheOutcome::Miss);
    }
    // Spec §10: exactly ONE metadata call + ONE download for the stampede.
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn two_upstream_routing_by_prefix() {
    let dir = tempdir().unwrap();
    let mut cfg = Config::default();
    cfg.cache_dir = dir.path().to_path_buf();
    cfg.upstreams.push(origin_cache::config::UpstreamConfig {
        id: "archive".into(),
        backend_type: "openlist".into(),
        base_url: "http://127.0.0.1:5245/dav".into(),
        root_path: None,
        username_env: "ARCHIVE_USER".into(),
        password_env: "ARCHIVE_PASS".into(),
        accept_invalid_certs: false,
        cold_miss: origin_cache::config::ColdMiss::Proxy,
        link_api_token_env: None,
        cache_profile: "standard".into(),
    });
    cfg.routes = RouteTable::new(vec![
        RouteRule { prefix: "archive/".into(), upstream: "archive".into() },
        RouteRule { prefix: "".into(), upstream: "primary".into() },
    ]);
    let cfg = Arc::new(cfg);
    assert_eq!(cfg.routes.resolve("archive/a.png"), "archive");
    assert_eq!(cfg.routes.resolve("2026/b.png"), "primary");
}

#[tokio::test]
async fn inactive_ttl_expiry_removes_file_and_meta() {
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
    let cache = Cache::new(cfg, Arc::clone(&clock), registry_with(Arc::new(backend)));
    let mut hit = cache.get("a.png", None).await.unwrap();
    read_body(&mut hit.body).await;
    wait_installed(&cache, "a.png").await;
    assert!(dir.path().join("a.png").exists());
    clock.advance(2000);
    cache.tick().await;
    assert!(!dir.path().join("a.png").exists());
    assert!(cache.state.read().await.entries.is_empty());
}

#[tokio::test]
async fn max_size_evicts_lru_order() {
    let dir = tempdir().unwrap();
    let mut cfg = Config::default();
    cfg.cache_dir = dir.path().to_path_buf();
    cfg.max_size_bytes = 10;
    cfg.inactive_ttl_secs = 3600;
    let cfg = Arc::new(cfg);
    let clock = Arc::new(MockClock::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let backend = CountingBackend {
        bytes: b"12345".to_vec(),
        etag: None,
        calls: Arc::clone(&calls),
        fail: None,
    };
    let cache = Cache::new(Arc::clone(&cfg), Arc::clone(&clock), registry_with(Arc::new(backend)));
    for k in ["a.png", "b.png", "c.png"] {
        let mut hit = cache.get(k, None).await.unwrap();
        read_body(&mut hit.body).await;
        wait_installed(&cache, k).await;
        clock.advance(10);
    }
    let remaining = cache.state.read().await.entries.len();
    assert!(remaining < 3);
    assert!(!cache.state.read().await.entries.contains_key("a.png"));
}

#[tokio::test]
async fn revalidation_uses_stat_and_serves_updated_content() {
    // v1 cached; backend version flips to v2; after the revalidate ttl the
    // next get must stat-compare (v1 != v2), refetch, and serve v2 bytes.
    let dir = tempdir().unwrap();
    let mut cfg = Config::default();
    cfg.cache_dir = dir.path().to_path_buf();
    cfg.revalidate_ttl_secs = 1;
    let cfg = Arc::new(cfg);
    let clock = Arc::new(MockClock::new(0));
    let version = Arc::new(AtomicUsize::new(1));

    struct VersionedBackend {
        version: Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl StorageBackend for VersionedBackend {
        async fn stat(&self, _key: &Key) -> Result<ObjectMeta, BackendError> {
            let v = self.version.load(Ordering::SeqCst);
            let bytes = format!("bytes-v{v}").into_bytes();
            Ok(ObjectMeta {
                size_bytes: bytes.len() as u64,
                etag: Some(format!("v{v}")),
                last_modified: None,
                mime_hint: Some("image/png".into()),
            })
        }
        async fn open(&self, _key: &Key, _range: Option<ByteRange>) -> Result<StreamSource, BackendError> {
            let v = self.version.load(Ordering::SeqCst);
            let bytes = format!("bytes-v{v}").into_bytes();
            Ok(StreamSource {
                stream: Box::new(std::io::Cursor::new(bytes)),
                total_len: Some(7),
            })
        }
        async fn refresh_if_needed(&self) -> Result<(), BackendError> {
            Ok(())
        }
        fn id(&self) -> &str {
            "versioned"
        }
    }

    let cache = Cache::new(
        Arc::clone(&cfg),
        Arc::clone(&clock),
        registry_with(Arc::new(VersionedBackend { version: Arc::clone(&version) })),
    );

    let hit = cache.get("a.png", None).await.unwrap();
    assert_eq!(hit.outcome, CacheOutcome::Miss);
    let mut hit = hit;
    assert_eq!(read_body(&mut hit.body).await, b"bytes-v1");
    wait_installed(&cache, "a.png").await;

    version.store(2, Ordering::SeqCst);
    clock.advance(2000); // past revalidate ttl

    let mut hit2 = cache.get("a.png", None).await.unwrap();
    assert_eq!(hit2.outcome, CacheOutcome::Miss); // stat: v2 != cached v1 -> refetch
    assert_eq!(read_body(&mut hit2.body).await, b"bytes-v2");
    wait_installed(&cache, "a.png").await;

    // Third get within ttl: fresh hit, no upstream.
    let mut hit3 = cache.get("a.png", None).await.unwrap();
    assert_eq!(hit3.outcome, CacheOutcome::Hit);
    assert_eq!(read_body(&mut hit3.body).await, b"bytes-v2");
}

#[tokio::test]
async fn revalidation_not_modified_serves_revalidated() {
    // Same etag: stat says unmodified -> serve from disk (Revalidated).
    let dir = tempdir().unwrap();
    let mut cfg = Config::default();
    cfg.cache_dir = dir.path().to_path_buf();
    cfg.revalidate_ttl_secs = 1;
    let cfg = Arc::new(cfg);
    let clock = Arc::new(MockClock::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let backend = CountingBackend {
        bytes: b"stable".to_vec(),
        etag: Some("same".into()),
        calls: Arc::clone(&calls),
        fail: None,
    };
    let cache = Cache::new(cfg, Arc::clone(&clock), registry_with(Arc::new(backend)));
    let mut hit = cache.get("a.png", None).await.unwrap();
    assert_eq!(hit.outcome, CacheOutcome::Miss);
    assert_eq!(read_body(&mut hit.body).await, b"stable");
    wait_installed(&cache, "a.png").await;
    clock.advance(2000);
    let mut hit2 = cache.get("a.png", None).await.unwrap();
    assert_eq!(hit2.outcome, CacheOutcome::Revalidated);
    assert_eq!(read_body(&mut hit2.body).await, b"stable");
}

#[tokio::test]
async fn traversal_payloads_are_400_via_fetch_error() {
    let dir = tempdir().unwrap();
    let cfg = test_config(dir.path().to_path_buf());
    let clock = Arc::new(MockClock::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let backend = CountingBackend {
        bytes: vec![],
        etag: None,
        calls: Arc::clone(&calls),
        fail: None,
    };
    let cache = Cache::new(cfg, clock, registry_with(Arc::new(backend)));
    let err = match cache.get("../etc/passwd", None).await {
        Err(e) => e,
        Ok(_) => panic!("traversal key must not resolve"),
    };
    assert!(matches!(err, BackendError::Other(_)));
    let err2 = match cache.get("%2e%2e%2fetc/passwd", None).await {
        Err(e) => e,
        Ok(_) => panic!("encoded traversal key must not resolve"),
    };
    assert!(matches!(err2, BackendError::Other(_)));
}

#[tokio::test]
async fn range_on_cached_file_slices_and_reports_content_range() {
    let dir = tempdir().unwrap();
    let cfg = test_config(dir.path().to_path_buf());
    let clock = Arc::new(MockClock::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let backend = CountingBackend {
        bytes: b"0123456789".to_vec(),
        etag: Some("v1".into()),
        calls: Arc::clone(&calls),
        fail: None,
    };
    let cache = Cache::new(cfg, clock, registry_with(Arc::new(backend)));
    let mut full = cache.get("a.png", None).await.unwrap();
    read_body(&mut full.body).await;
    wait_installed(&cache, "a.png").await;

    // Cached-file Range: sliced via file seek, no upstream traffic.
    let before = calls.load(Ordering::SeqCst);
    let mut part = cache
        .get("a.png", Some(ByteRange::bounded(2, 4)))
        .await
        .unwrap();
    assert_eq!(part.outcome, CacheOutcome::Hit);
    assert_eq!(part.content_range.as_ref().map(|c| c.header_value()).as_deref(), Some("bytes 2-5/10"));
    assert_eq!(read_body(&mut part.body).await, b"2345");
    assert_eq!(calls.load(Ordering::SeqCst), before);
}

#[tokio::test]
async fn range_cold_miss_offset_zero_streams_full_with_content_range() {
    let dir = tempdir().unwrap();
    let cfg = test_config(dir.path().to_path_buf());
    let clock = Arc::new(MockClock::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let backend = CountingBackend {
        bytes: b"0123456789".to_vec(),
        etag: None,
        calls: Arc::clone(&calls),
        fail: None,
    };
    let cache = Cache::new(cfg, clock, registry_with(Arc::new(backend)));
    let mut hit = cache
        .get("a.png", Some(ByteRange::from_offset(0)))
        .await
        .unwrap();
    assert_eq!(hit.outcome, CacheOutcome::Miss);
    assert_eq!(hit.content_range.as_ref().map(|c| c.header_value()).as_deref(), Some("bytes 0-9/10"));
    assert_eq!(read_body(&mut hit.body).await, b"0123456789");
    wait_installed(&cache, "a.png").await;
}

#[tokio::test]
async fn range_cold_miss_dual_channel_passthrough_and_background_fill() {
    let dir = tempdir().unwrap();
    let cfg = test_config(dir.path().to_path_buf());
    let clock = Arc::new(MockClock::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let backend = CountingBackend {
        bytes: b"0123456789".to_vec(),
        etag: None,
        calls: Arc::clone(&calls),
        fail: None,
    };
    let cache = Cache::new(cfg, clock, registry_with(Arc::new(backend)));

    // Cold miss seeking to byte 3: client gets bytes 3.. immediately
    // (passthrough), while the full flight fills the cache in background.
    let mut hit = cache
        .get("a.png", Some(ByteRange::from_offset(3)))
        .await
        .unwrap();
    assert_eq!(hit.outcome, CacheOutcome::Miss);
    assert_eq!(hit.content_range.as_ref().map(|c| c.header_value()).as_deref(), Some("bytes 3-9/10"));
    assert_eq!(read_body(&mut hit.body).await, b"3456789");
    wait_installed(&cache, "a.png").await;
    assert_eq!(
        std::fs::read(dir.path().join("a.png")).unwrap(),
        b"0123456789".to_vec(),
        "background flight must land a COMPLETE cache file"
    );

    // Next access is a full disk hit.
    let mut hit2 = cache.get("a.png", None).await.unwrap();
    assert_eq!(hit2.outcome, CacheOutcome::Hit);
    assert_eq!(read_body(&mut hit2.body).await, b"0123456789");
}

#[tokio::test]
async fn unsatisfiable_range_rejected() {
    let dir = tempdir().unwrap();
    let cfg = test_config(dir.path().to_path_buf());
    let clock = Arc::new(MockClock::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let backend = CountingBackend {
        bytes: b"short".to_vec(),
        etag: None,
        calls: Arc::clone(&calls),
        fail: None,
    };
    let cache = Cache::new(cfg, clock, registry_with(Arc::new(backend)));
    let mut hit = cache.get("a.png", None).await.unwrap();
    read_body(&mut hit.body).await;
    wait_installed(&cache, "a.png").await;

    let err = match cache
        .get("a.png", Some(ByteRange::from_offset(99)))
        .await
    {
        Err(e) => e,
        Ok(_) => panic!("out-of-bounds range must be rejected"),
    };
    assert!(matches!(err, BackendError::RangeNotSatisfiable));
}

#[tokio::test]
async fn mime_fallback_overrides_octet_stream() {
    let dir = tempdir().unwrap();
    let cfg = test_config(dir.path().to_path_buf());
    let clock = Arc::new(MockClock::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    // CountingBackend always hints application/octet-stream.
    let backend = CountingBackend {
        bytes: b"id3".to_vec(),
        etag: None,
        calls: Arc::clone(&calls),
        fail: None,
    };
    let cache = Cache::new(cfg, clock, registry_with(Arc::new(backend)));
    let mut hit = cache.get("music/dazbee.flac", None).await.unwrap();
    read_body(&mut hit.body).await;
    assert_eq!(hit.meta.content_type.as_deref(), Some("audio/flac"));
}

#[tokio::test]
async fn cache_entries_and_access_clock_survive_restart() {
    // Spec §10: entries and last-access survive a restart. Simulate by
    // building a cache, filling it, dropping it, then reopening the same
    // cache_dir with a never-seen-the-data backend.
    let dir = tempdir().unwrap();
    let cfg = test_config(dir.path().to_path_buf());
    let clock = Arc::new(MockClock::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let backend = CountingBackend {
        bytes: b"persisted".to_vec(),
        etag: Some("v1".into()),
        calls: Arc::clone(&calls),
        fail: None,
    };
    let cache = Arc::new(Cache::new(Arc::clone(&cfg), Arc::clone(&clock), registry_with(Arc::new(backend))));
    cache.load_and_start().await;
    let mut hit = cache.get("a.png", None).await.unwrap();
    assert_eq!(read_body(&mut hit.body).await, b"persisted");
    wait_installed(&cache, "a.png").await;
    clock.advance(5000); // last_access moved; eviction order must persist too
    // flush the coalesced access-clock bump to redb
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;

    // "Restart": new Cache instance, fresh clock, backend that would 500 if
    // ever contacted (proving the restart hit is served from disk+redb).
    drop(cache);
    let calls2 = Arc::new(AtomicUsize::new(0));
    let backend2 = CountingBackend {
        bytes: vec![],
        etag: None,
        calls: Arc::clone(&calls2),
        fail: Some(BackendError::ServerError("must not be contacted".into())),
    };
    let clock2 = Arc::new(MockClock::new(5000));
    let cache2 = Arc::new(Cache::new(
        test_config(dir.path().to_path_buf()),
        Arc::clone(&clock2),
        registry_with(Arc::new(backend2)),
    ));
    cache2.load_and_start().await;

    // Entry reloaded from redb: fresh clock (now=5000) vs last_access —
    // still within revalidate ttl, so a plain disk hit with zero upstream.
    let mut hit2 = cache2.get("a.png", None).await.unwrap();
    assert_eq!(hit2.outcome, CacheOutcome::Hit);
    assert_eq!(read_body(&mut hit2.body).await, b"persisted");
    assert_eq!(calls2.load(Ordering::SeqCst), 0, "restart hit must not touch upstream");
}

/// B1 regression guard: the reaper loop spawned by `load_and_start` must
/// collect expired entries on its own — no manual `tick()` call. Until this
/// wiring existed, a deployed binary never reaped anything (audit finding).
#[tokio::test]
async fn spawned_reaper_expires_entries_without_manual_tick() {
    let dir = tempdir().unwrap();
    let cfg = test_config(dir.path().to_path_buf());
    let clock = Arc::new(MockClock::new(0));
    let backend = CountingBackend {
        bytes: b"ttl".to_vec(),
        etag: Some("v1".into()),
        calls: Arc::new(AtomicUsize::new(0)),
        fail: None,
    };
    let cache = Arc::new(Cache::new(
        Arc::clone(&cfg),
        Arc::clone(&clock),
        registry_with(Arc::new(backend)),
    ));
    // 50 ms production-style reaper loop instead of the 60 s default.
    cache.load_and_start_with(std::time::Duration::from_millis(50)).await;

    let mut hit = cache.get("old.png", None).await.unwrap();
    assert_eq!(read_body(&mut hit.body).await, b"ttl");
    wait_installed(&cache, "old.png").await;

    // Default inactive_ttl is 1200 s; step past it and give the reaper a
    // real-time moment to fire (the loop interval is wall time, the TTL is
    // mock-clock time).
    clock.advance(1_201_000);
    for _ in 0..200 {
        if cache.state.read().await.entries.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(cache.state.read().await.entries.is_empty(), "spawned reaper did not expire the entry");
    assert!(!cache.config.cache_dir.join("old.png").exists(), "expired file must be deleted");
}

// ---------------------------------------------------------------------------
// Storm suite: multi-client concurrent seeks on one cold key — the surface
// whose breakdown motivated the refactor (audit findings C1/B2/B3).
// ---------------------------------------------------------------------------

/// Storm-suite backend: counts stat+open calls, delays a real open so
/// readers join an in-flight download, and can be switched to fail / panic
/// / undershoot on open to exercise flight failure propagation.
struct StormBackend {
    payload: Vec<u8>,
    calls: Arc<AtomicUsize>,
    mode: Arc<std::sync::Mutex<StormMode>>,
}

#[derive(Clone, Copy, PartialEq)]
enum StormMode {
    Good,
    FailOpen,
    PanicOpen,
    ShortBody,
}

#[async_trait::async_trait]
impl StorageBackend for StormBackend {
    async fn stat(&self, _key: &Key) -> Result<ObjectMeta, BackendError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(ObjectMeta {
            size_bytes: self.payload.len() as u64,
            etag: Some("v1".into()),
            last_modified: None,
            mime_hint: None,
        })
    }
    async fn open(&self, _key: &Key, _range: Option<ByteRange>) -> Result<StreamSource, BackendError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mode = *self.mode.lock().unwrap();
        match mode {
            StormMode::FailOpen => Err(BackendError::ServerError("open refused".into())),
            StormMode::PanicOpen => panic!("storm open boom"),
            StormMode::ShortBody => {
                let cut = self.payload.len() / 2;
                Ok(StreamSource {
                    stream: Box::new(std::io::Cursor::new(self.payload[..cut].to_vec())),
                    total_len: Some(self.payload.len() as u64),
                })
            }
            StormMode::Good => {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                let len = self.payload.len() as u64;
                Ok(StreamSource {
                    stream: Box::new(std::io::Cursor::new(self.payload.clone())),
                    total_len: Some(len),
                })
            }
        }
    }
    async fn refresh_if_needed(&self) -> Result<(), BackendError> {
        Ok(())
    }
    async fn list(&self, _prefix: &str, _recursive: bool) -> Result<Vec<origin_cache::backend::ListEntry>, BackendError> {
        Ok(vec![])
    }
    fn id(&self) -> &str {
        "storm"
    }
}

/// Collect a body, allowing a terminal Err: returns (bytes, errored).
async fn read_body_allow_error(body: BodyStream) -> (Vec<u8>, bool) {
    use futures::StreamExt;
    let mut body = body;
    let mut out = Vec::new();
    let mut errored = false;
    while let Some(chunk) = body.next().await {
        match chunk {
            Ok(b) => out.extend_from_slice(&b),
            Err(_) => {
                errored = true;
                break;
            }
        }
    }
    (out, errored)
}

async fn wait_map_empty(cache: &Cache<MockClock>) {
    for _ in 0..200 {
        if cache.flights.active().await == 0 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    panic!("flight map never emptied");
}

/// The storm core: 20 concurrent ranged cold misses on ONE key must share
/// exactly one upstream stat + one open (ADR-0003 acceptance, ranged), and
/// every seek must receive its exact byte slice. A second wave serves the
/// same slices as disk hits.
#[tokio::test]
async fn ranged_cold_misses_on_one_key_coalesce() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let payload: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
    let calls = Arc::new(AtomicUsize::new(0));
    let backend = StormBackend {
        payload: payload.clone(),
        calls: Arc::clone(&calls),
        mode: Arc::new(std::sync::Mutex::new(StormMode::Good)),
    };
    let cache = Arc::new(Cache::new(
        test_config(dir.path().to_path_buf()),
        Arc::clone(&clock),
        registry_with(Arc::new(backend)),
    ));

    // 20 distinct offsets (i*173 % 4096), 64 bytes each, all cold.
    let mut tasks = Vec::new();
    for i in 0..20u64 {
        let cache = Arc::clone(&cache);
        tasks.push(tokio::spawn(async move {
            let offset = (i * 173) % 4096;
            let mut hit = cache
                .get("storm.bin", Some(ByteRange::bounded(offset, 64)))
                .await
                .expect("ranged cold miss must succeed");
            let body = read_body(&mut hit.body).await;
            (offset, body)
        }));
    }
    for t in tasks {
        let (offset, body) = t.await.unwrap();
        assert_eq!(body, payload[offset as usize..(offset + 64) as usize], "seek @{offset} bytes must be exact");
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2, "1 stat + 1 open for 20 ranged cold misses");
    wait_installed(&cache, "storm.bin").await;

    // Second wave: same seeks served from disk as hits, bytes still exact.
    for i in 0..20u64 {
        let offset = (i * 173) % 4096;
        let mut hit = cache.get("storm.bin", Some(ByteRange::bounded(offset, 64))).await.unwrap();
        assert_eq!(hit.outcome, CacheOutcome::Hit);
        assert_eq!(read_body(&mut hit.body).await, payload[offset as usize..(offset + 64) as usize]);
    }
}

/// A failed open must reach every attached reader as a clean body error,
/// release the flight map entry, and leave the key retryable.
#[tokio::test]
async fn flight_failure_reaches_attached_readers_and_clears_map() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let payload: Vec<u8> = (0..100u32).map(|i| (i % 251) as u8).collect();
    let mode = Arc::new(std::sync::Mutex::new(StormMode::FailOpen));
    let backend = StormBackend {
        payload: payload.clone(),
        calls: Arc::new(AtomicUsize::new(0)),
        mode: Arc::clone(&mode),
    };
    let cache = Arc::new(Cache::new(
        test_config(dir.path().to_path_buf()),
        Arc::clone(&clock),
        registry_with(Arc::new(backend)),
    ));

    let mut tasks = Vec::new();
    for _ in 0..3 {
        let cache = Arc::clone(&cache);
        tasks.push(tokio::spawn(async move {
            // A fast-failing flight may publish Failed before Meta is ever
            // observed — then get() itself errors. Either surfacing is the
            // failure reaching the client; neither may hang.
            match cache.get("flaky.bin", None).await {
                Err(_) => (Vec::new(), true),
                Ok(mut hit) => read_body_allow_error(hit.body).await,
            }
        }));
    }
    for t in tasks {
        let (out, errored) = t.await.unwrap();
        assert!(errored, "failed flight must surface as an error");
        assert!(out.is_empty() || out.len() <= 100);
    }
    wait_map_empty(&cache).await;

    *mode.lock().unwrap() = StormMode::Good;
    let mut hit = cache.get("flaky.bin", None).await.unwrap();
    assert_eq!(read_body(&mut hit.body).await, payload, "retry after failure must succeed");
}

/// A driver panic must not leave a zombie flight: readers error out, the
/// map entry is released, and the key is immediately retryable (B3).
#[tokio::test]
async fn panicked_driver_fails_flight_and_releases_key() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let payload: Vec<u8> = (0..100u32).map(|i| (i % 251) as u8).collect();
    let mode = Arc::new(std::sync::Mutex::new(StormMode::PanicOpen));
    let backend = StormBackend {
        payload: payload.clone(),
        calls: Arc::new(AtomicUsize::new(0)),
        mode: Arc::clone(&mode),
    };
    let cache = Arc::new(Cache::new(
        test_config(dir.path().to_path_buf()),
        Arc::clone(&clock),
        registry_with(Arc::new(backend)),
    ));

    let cache2 = Arc::clone(&cache);
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), async move {
        cache2.get("panic.bin", None).await
    })
    .await
    .expect("must not hang on a panicking driver");
    // Fast panic: Failed may beat Meta — get() errors directly. Either
    // surfacing is fine; hanging forever is not.
    let errored = match outcome {
        Err(_) => true,
        Ok(mut hit) => read_body_allow_error(hit.body).await.1,
    };
    assert!(errored, "panic must surface as an error");
    wait_map_empty(&cache).await;
    assert!(cache.state.read().await.entries.is_empty(), "a panicked flight installs nothing");

    *mode.lock().unwrap() = StormMode::Good;
    let mut hit = cache.get("panic.bin", None).await.unwrap();
    assert_eq!(read_body(&mut hit.body).await, payload, "key must be retryable after a panic");
}

/// A short upstream body must never be sealed into the cache: the flight
/// fails, no entry is installed, and a retry pulls the full object (B2).
#[tokio::test]
async fn short_upstream_body_is_never_sealed() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let payload: Vec<u8> = (0..100u32).map(|i| (i % 251) as u8).collect();
    let mode = Arc::new(std::sync::Mutex::new(StormMode::ShortBody));
    let backend = StormBackend {
        payload: payload.clone(),
        calls: Arc::new(AtomicUsize::new(0)),
        mode: Arc::clone(&mode),
    };
    let cache = Arc::new(Cache::new(
        test_config(dir.path().to_path_buf()),
        Arc::clone(&clock),
        registry_with(Arc::new(backend)),
    ));

    let (out, errored) = match cache.get("short.bin", None).await {
        Err(_) => (Vec::new(), true),
        Ok(mut hit) => read_body_allow_error(hit.body).await,
    };
    assert!(errored, "short body must surface as an error");
    assert!(out.len() <= 50, "at most the bytes that did land reach the reader");
    wait_map_empty(&cache).await;
    assert!(cache.state.read().await.entries.is_empty(), "short read must not install an entry");
    assert!(
        !cache.config.cache_dir.join("short.bin").exists(),
        "short read must not be renamed into the cache"
    );

    *mode.lock().unwrap() = StormMode::Good;
    let mut hit = cache.get("short.bin", None).await.unwrap();
    assert_eq!(read_body(&mut hit.body).await, payload, "retry must pull the full object");
}

// ---------------------------------------------------------------------------
// P1: hit-path lock discipline — the access stamp must not take the state
// write lock, and concurrent hits must not serialize on it.
// ---------------------------------------------------------------------------

/// Concurrent hits must all complete while the access clock stays lock-free
/// on the state: with N readers hitting one hot key, every response must be
/// byte-exact and no hit may block on a writer lock the hit path no longer
/// takes. The flusher folds the batch later; the value's lag is invisible to
/// the reaper (1200 s TTL).
#[tokio::test]
async fn concurrent_hits_stamp_access_without_state_write_lock() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let payload: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
    let backend = CountingBackend {
        bytes: payload.clone(),
        etag: Some("v1".into()),
        calls: Arc::new(AtomicUsize::new(0)),
        fail: None,
    };
    let cache = Arc::new(Cache::new(
        test_config(dir.path().to_path_buf()),
        Arc::clone(&clock),
        registry_with(Arc::new(backend)),
    ));
    cache.load_and_start().await;

    // Move the clock so the access stamps carry a distinguishable value,
    // then cold-fill and hammer the hot key concurrently.
    clock.advance(5000);
    let mut first = cache.get("hot.bin", None).await.unwrap();
    assert_eq!(read_body(&mut first.body).await, payload);
    wait_installed(&cache, "hot.bin").await;

    let mut tasks = Vec::new();
    for _ in 0..32 {
        let cache = Arc::clone(&cache);
        let expect = payload.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..8 {
                let mut hit = cache.get("hot.bin", None).await.unwrap();
                assert_eq!(hit.outcome, CacheOutcome::Hit);
                assert_eq!(read_body(&mut hit.body).await, expect);
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    // The access clock is pending a fold (not yet written into the row) —
    // that is the design, and the reaper must not treat it as stale.
    assert!(cache.dirty_access.pending() > 0, "hits must register in the access clock");

    // After a real-time flush tick the row carries the stamp the hit wrote
    // (5000), proving the flusher folds the lock-free records into state.
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    let last = cache.state.read().await.entries.get("hot.bin").unwrap().last_access_millis;
    assert_eq!(last, 5000, "flusher must fold the access stamp into the entry row");
}

// ---------------------------------------------------------------------------
// B1: metadata and stream gates are independent — a long transfer must not
// starve a HEAD. Measured on the node before the split: an idle HEAD took
// 22 ms, and 14.2 s while three cold pulls held the single shared gate.
// ---------------------------------------------------------------------------

/// Backend whose `open` blocks until released, so a transfer can be held
/// open while a metadata call is attempted.
struct BlockingOpenBackend {
    bytes: Vec<u8>,
    release: Arc<tokio::sync::Notify>,
    opened: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl StorageBackend for BlockingOpenBackend {
    async fn stat(&self, _key: &Key) -> Result<ObjectMeta, BackendError> {
        Ok(ObjectMeta {
            size_bytes: self.bytes.len() as u64,
            etag: Some("v1".into()),
            last_modified: None,
            mime_hint: None,
        })
    }
    async fn open(&self, _key: &Key, _range: Option<ByteRange>) -> Result<StreamSource, BackendError> {
        self.opened.fetch_add(1, Ordering::SeqCst);
        // Hold the transfer open until the test releases it.
        self.release.notified().await;
        Ok(StreamSource {
            stream: Box::new(std::io::Cursor::new(self.bytes.clone())),
            total_len: Some(self.bytes.len() as u64),
        })
    }
    async fn refresh_if_needed(&self) -> Result<(), BackendError> {
        Ok(())
    }
    fn id(&self) -> &str {
        "blocking"
    }
}

/// With the stream gate saturated, a metadata stat must still complete
/// promptly (B1). Under the old single gate it queued behind the transfer.
#[tokio::test]
async fn head_not_starved_by_saturated_stream_gate() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let cfg = test_config(dir.path().to_path_buf());
    let release = Arc::new(tokio::sync::Notify::new());
    let opened = Arc::new(AtomicUsize::new(0));
    let backend = Arc::new(BlockingOpenBackend {
        bytes: vec![7u8; 4096],
        release: Arc::clone(&release),
        opened: Arc::clone(&opened),
    });

    // Deliberately saturate the STREAM gate (2 permits) with two held
    // transfers on distinct keys.
    let mut slots = HashMap::new();
    let slot = Arc::new(BackendSlot::new(backend, 2));
    slots.insert("primary".to_string(), Arc::clone(&slot));
    let cache = Arc::new(Cache::new(cfg, Arc::clone(&clock), BackendRegistry::new(slots)));

    for key in ["a.bin", "b.bin"] {
        let c = Arc::clone(&cache);
        tokio::spawn(async move {
            let _ = c.get(key, None).await; // blocks in open()
        });
    }
    // Wait until both transfers have reached open() (both stream permits held).
    for _ in 0..200 {
        if opened.load(Ordering::SeqCst) >= 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert!(opened.load(Ordering::SeqCst) >= 2, "both transfers must hold stream permits");
    assert_eq!(slot.stream_gate.available_permits(), 0, "stream gate must be saturated");

    // A metadata call on a third key must still be fast: the metadata
    // gate is separate.
    let started = std::time::Instant::now();
    let meta = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        cache.head_meta("c.bin"),
    )
    .await
    .expect("HEAD must not queue behind the saturated stream gate")
    .expect("HEAD must succeed");
    assert_eq!(meta.size, 4096);
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
        "HEAD took {:?} — metadata is sharing the stream gate again",
        started.elapsed()
    );

    release.notify_waiters();
}


// ---------------------------------------------------------------------------
// P9: listing snapshots let later pages reuse one upstream walk.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn listing_snapshot_is_reused_then_expires() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let cache = Arc::new(Cache::new(
        test_config(dir.path().to_path_buf()),
        Arc::clone(&clock),
        registry_with(Arc::new(CountingBackend {
            bytes: b"x".to_vec(),
            etag: None,
            calls: Arc::new(AtomicUsize::new(0)),
            fail: None,
        })),
    ));

    let entries = vec![
        ListEntry { key: "a/1.bin".into(), size: 1, etag: None, last_modified: None, is_dir: false },
        ListEntry { key: "a/2.bin".into(), size: 2, etag: None, last_modified: None, is_dir: false },
    ];
    assert!(cache.list_snapshot("up", "a/", true).await.is_none(), "cold snapshot is empty");
    cache.store_list_snapshot("up", "a/", true, &entries).await;

    let got = cache.list_snapshot("up", "a/", true).await.expect("snapshot present");
    assert_eq!(got, entries, "the second page sees the same walk");

    // Different selector = different snapshot.
    assert!(cache.list_snapshot("up", "a/", false).await.is_none());

    // After the TTL it is gone.
    clock.advance(6_000);
    assert!(cache.list_snapshot("up", "a/", true).await.is_none(), "snapshot expired");
}
