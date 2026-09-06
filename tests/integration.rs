use std::collections::HashMap;
use std::sync::{atomic::AtomicUsize, atomic::Ordering, Arc};
use tokio::sync::Semaphore;

use tempfile::tempdir;

use origin_cache::{
    backend::{BackendError, BackendRegistry, BackendSlot, ByteRange, Key, ObjectMeta, StreamSource, StorageBackend},
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
        Arc::new(BackendSlot { backend, gate: Arc::new(Semaphore::new(3)) }),
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
    assert_eq!(part.content_range.as_deref(), Some("bytes 2-5/10"));
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
    assert_eq!(hit.content_range.as_deref(), Some("bytes 0-9/10"));
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
    assert_eq!(hit.content_range.as_deref(), Some("bytes 3-9/10"));
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
