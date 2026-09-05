use std::collections::HashMap;
use std::sync::{atomic::AtomicUsize, atomic::Ordering, Arc};
use tokio::sync::Semaphore;

use tempfile::tempdir;

use origin_cache::{
    backend::{BackendError, BackendRegistry, BackendSlot, ByteRange, Key, ObjectMeta, StreamSource, StorageBackend},
    cache::cache::{Cache, CacheOutcome},
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
            mime_hint: Some("image/png".into()),
        })
    }

    async fn open(&self, _key: &Key, _range: Option<ByteRange>) -> Result<StreamSource, BackendError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(e) = &self.fail {
            return Err(e.clone());
        }
        Ok(StreamSource {
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

fn test_config(dir: std::path::PathBuf) -> Arc<Config> {
    let mut cfg = Config::default();
    cfg.cache_dir = dir;
    Arc::new(cfg)
}

fn registry_with(backend: Arc<dyn StorageBackend>) -> BackendRegistry {
    let mut slots = HashMap::new();
    slots.insert(
        "primary".to_string(),
        Arc::new(BackendSlot { backend, gate: Arc::new(Semaphore::new(3)) }),
    );
    BackendRegistry::new(slots)
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
        handles.push(tokio::spawn(async move { c.get("same.png").await.map(|(b, _)| b) }));
    }
    for h in handles {
        assert_eq!(h.await.unwrap().unwrap(), b"payload");
    }
    // Spec §10: exactly ONE metadata call + ONE download for the whole
    // stampede (the winner's flight); everyone else serves from disk via
    // the double-check inside the flight.
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn two_upstream_routing_by_prefix() {
    let dir = tempdir().unwrap();
    let mut cfg = Config::default();
    cfg.cache_dir = dir.path().to_path_buf();
    cfg.upstreams.push(origin_cache::config::UpstreamConfig {
        id: "archive".into(),
        drive_root_path: "/drive/root:/archive".into(),
        client_id_env: "ARCHIVE_ID".into(),
        client_secret_env: "ARCHIVE_SECRET".into(),
        refresh_token_env: "ARCHIVE_TOKEN".into(),
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
    cache.get("a.png").await.unwrap();
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
    cache.get("a.png").await.unwrap();
    clock.advance(10);
    cache.get("b.png").await.unwrap();
    clock.advance(10);
    cache.get("c.png").await.unwrap();
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
            Ok(ObjectMeta {
                size_bytes: 7,
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

    let (b1, o1) = cache.get("a.png").await.unwrap();
    assert_eq!(o1, CacheOutcome::Miss);
    assert_eq!(b1, b"bytes-v1");

    version.store(2, Ordering::SeqCst);
    clock.advance(2000); // past revalidate ttl

    let (b2, o2) = cache.get("a.png").await.unwrap();
    assert_eq!(o2, CacheOutcome::Miss); // stat: v2 != cached v1 -> refetch
    assert_eq!(b2, b"bytes-v2");

    // Third get within ttl: fresh hit, no upstream.
    let (b3, o3) = cache.get("a.png").await.unwrap();
    assert_eq!(o3, CacheOutcome::Hit);
    assert_eq!(b3, b"bytes-v2");
}

#[tokio::test]
async fn revalidation_not_modified_serves_revalidated() {
    // Same etag: stat says unmodified → serve from disk (Revalidated).
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
    let (b1, o1) = cache.get("a.png").await.unwrap();
    assert_eq!(o1, CacheOutcome::Miss);
    assert_eq!(b1, b"stable");
    clock.advance(2000);
    let (b2, o2) = cache.get("a.png").await.unwrap();
    assert_eq!(o2, CacheOutcome::Revalidated);
    assert_eq!(b2, b"stable");
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
    let err = cache.get("../etc/passwd").await.unwrap_err();
    assert!(matches!(err, BackendError::Other(_)));
    let err2 = cache.get("%2e%2e%2fetc/passwd").await.unwrap_err();
    assert!(matches!(err2, BackendError::Other(_)));
}
