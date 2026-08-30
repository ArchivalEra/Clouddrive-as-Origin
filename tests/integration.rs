use std::sync::{atomic::AtomicUsize, atomic::Ordering, Arc};

use tempfile::tempdir;

use origin_cache::{
    cache::cache::{Cache, Fetcher, Fetched, FetchError},
    clock::MockClock,
    config::Config,
};

#[derive(Clone)]
struct CountingFetcher {
    bytes: Vec<u8>,
    etag: Option<String>,
    calls: Arc<AtomicUsize>,
    fail: Option<FetchError>,
}

#[async_trait::async_trait]
impl Fetcher for CountingFetcher {
    async fn fetch(&self, _key: &str, _upstream: &str) -> Result<Fetched, FetchError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(e) = &self.fail {
            return Err(e.clone());
        }
        Ok(Fetched {
            bytes: self.bytes.clone(),
            etag: self.etag.clone(),
            content_type: Some("image/png".into()),
            last_modified: None,
        })
    }
}

fn test_config(dir: std::path::PathBuf) -> Arc<Config> {
    let mut cfg = Config::default();
    cfg.cache_dir = dir;
    Arc::new(cfg)
}

#[tokio::test]
async fn single_flight_20_concurrent_same_key_one_fetch() {
    let dir = tempdir().unwrap();
    let cfg = test_config(dir.path().to_path_buf());
    let clock = Arc::new(MockClock::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let fetcher = Arc::new(CountingFetcher {
        bytes: b"payload".to_vec(),
        etag: Some("v1".into()),
        calls: Arc::clone(&calls),
        fail: None,
    });
    let cache = Arc::new(Cache::new(Arc::clone(&cfg), Arc::clone(&clock), fetcher));

    let mut handles = Vec::new();
    for _ in 0..20 {
        let c = Arc::clone(&cache);
        handles.push(tokio::spawn(async move { c.get("same.png").await.map(|(b, _)| b) }));
    }
    for h in handles {
        assert_eq!(h.await.unwrap().unwrap(), b"payload");
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
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
    cfg.routes = origin_cache::routing::RouteTable::new(vec![
        origin_cache::routing::RouteRule { prefix: "archive/".into(), upstream: "archive".into() },
        origin_cache::routing::RouteRule { prefix: "".into(), upstream: "primary".into() },
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
    let fetcher = Arc::new(CountingFetcher {
        bytes: b"x".to_vec(),
        etag: None,
        calls: Arc::new(AtomicUsize::new(0)),
        fail: None,
    });
    let cache = Cache::new(Arc::clone(&cfg), Arc::clone(&clock), fetcher);
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
    let fetcher = Arc::new(CountingFetcher {
        bytes: b"12345".to_vec(),
        etag: None,
        calls: Arc::new(AtomicUsize::new(0)),
        fail: None,
    });
    let cache = Cache::new(Arc::clone(&cfg), Arc::clone(&clock), Arc::clone(&fetcher));
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
async fn traversal_payloads_are_400_via_fetch_error() {
    let dir = tempdir().unwrap();
    let cfg = test_config(dir.path().to_path_buf());
    let clock = Arc::new(MockClock::new(0));
    let fetcher = Arc::new(CountingFetcher {
        bytes: vec![],
        etag: None,
        calls: Arc::new(AtomicUsize::new(0)),
        fail: None,
    });
    let cache = Cache::new(cfg, clock, fetcher);
    let err = cache.get("../etc/passwd").await.unwrap_err();
    assert!(matches!(err, FetchError::Other(_)));
    let err2 = cache.get("%2e%2e%2fetc/passwd").await.unwrap_err();
    assert!(matches!(err2, FetchError::Other(_)));
}
