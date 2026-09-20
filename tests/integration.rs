use std::collections::HashMap;
use std::sync::{atomic::AtomicUsize, atomic::Ordering, Arc};

use tempfile::tempdir;

// The mock lives in the lib now: one implementation for the whole tree.
use origin_cache::testsupport::{
    collect, collect_allow_error, BlockingOpenBackend, CacheTestExt, MockBackend as CountingBackend,
    SizedBackend, StormBackend, StormMode, VersionedBackend, wait_entry,
};

use origin_cache::{
    backend::{BackendError, BackendRegistry, BackendSlot, ByteRange, ListEntry, StreamSource, StorageBackend},
    cache::cache::{Cache, CacheOutcome},
    cache::flight::FlightProgress,
    clock::{Clock, MockClock},
    config::Config,
    key::KeyError,
    routing::{RouteRule, RouteTable},
};


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

/// Wait until every flight has left the map (an admission round keeps the
/// map as its coalescing key-set, and these tests assert on its drain).
async fn wait_map_empty(cache: &Cache<MockClock>) {
    for _ in 0..200 {
        if cache.flights.active().await == 0 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    panic!("flight map never emptied");
}

#[tokio::test]
async fn single_flight_20_concurrent_same_key_one_fetch() {
    let dir = tempdir().unwrap();
    let cfg = test_config(dir.path().to_path_buf());
    let clock = Arc::new(MockClock::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let backend = CountingBackend::counting(b"payload".to_vec(), Some("v1".into()), Arc::clone(&calls), None);
    let cache = Arc::new(Cache::new(cfg, Arc::clone(&clock), registry_with(Arc::new(backend))));

    let mut handles = Vec::new();
    for _ in 0..20 {
        let c = Arc::clone(&cache);
        handles.push(tokio::spawn(async move {
            let hit = c.get_by_key("same.png", None).await?;
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
    let backend = CountingBackend::counting(b"x".to_vec(), None, Arc::clone(&calls), None);
    let cache = Cache::new(cfg, Arc::clone(&clock), registry_with(Arc::new(backend)));
    let mut hit = cache.get_by_key("a.png", None).await.unwrap();
    collect(&mut hit.body).await;
    wait_entry(&cache, "a.png").await;
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
    let backend = CountingBackend::counting(b"12345".to_vec(), None, Arc::clone(&calls), None);
    let cache = Cache::new(Arc::clone(&cfg), Arc::clone(&clock), registry_with(Arc::new(backend)));
    for k in ["a.png", "b.png", "c.png"] {
        let mut hit = cache.get_by_key(k, None).await.unwrap();
        collect(&mut hit.body).await;
        wait_entry(&cache, k).await;
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

    let cache = Cache::new(
        Arc::clone(&cfg),
        Arc::clone(&clock),
        registry_with(Arc::new(VersionedBackend { version: Arc::clone(&version), mime: None })),
    );

    let hit = cache.get_by_key("a.png", None).await.unwrap();
    assert_eq!(hit.outcome, CacheOutcome::Miss);
    let mut hit = hit;
    assert_eq!(collect(&mut hit.body).await, b"bytes-v1");
    wait_entry(&cache, "a.png").await;

    version.store(2, Ordering::SeqCst);
    clock.advance(2000); // past revalidate ttl

    let mut hit2 = cache.get_by_key("a.png", None).await.unwrap();
    assert_eq!(hit2.outcome, CacheOutcome::Miss); // stat: v2 != cached v1 -> refetch
    assert_eq!(collect(&mut hit2.body).await, b"bytes-v2");
    wait_entry(&cache, "a.png").await;

    // Third get within ttl: fresh hit, no upstream.
    let mut hit3 = cache.get_by_key("a.png", None).await.unwrap();
    assert_eq!(hit3.outcome, CacheOutcome::Hit);
    assert_eq!(collect(&mut hit3.body).await, b"bytes-v2");
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
    let backend = CountingBackend::counting(b"stable".to_vec(), Some("same".into()), Arc::clone(&calls), None);
    let cache = Cache::new(cfg, Arc::clone(&clock), registry_with(Arc::new(backend)));
    let mut hit = cache.get_by_key("a.png", None).await.unwrap();
    assert_eq!(hit.outcome, CacheOutcome::Miss);
    assert_eq!(collect(&mut hit.body).await, b"stable");
    wait_entry(&cache, "a.png").await;
    clock.advance(2000);
    let mut hit2 = cache.get_by_key("a.png", None).await.unwrap();
    assert_eq!(hit2.outcome, CacheOutcome::Revalidated);
    assert_eq!(collect(&mut hit2.body).await, b"stable");
}

#[tokio::test]
async fn traversal_payloads_are_400_via_the_resolve_seam() {
    let dir = tempdir().unwrap();
    let cfg = test_config(dir.path().to_path_buf());
    let clock = Arc::new(MockClock::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let backend = CountingBackend::counting(
        vec![],
        None,
        Arc::clone(&calls),
        None,
    );
    let cache = Cache::new(cfg, clock, registry_with(Arc::new(backend)));

    // Traversal never reaches the cache: the resolve seam rejects it, which is
    // now the ONLY way a key error leaves this layer, because BackendError can
    // no longer carry a client fault at all.
    for key in ["../etc/passwd", "%2e%2e%2fetc/passwd"] {
        match cache.resolve(key) {
            Err(KeyError::Traversal) => {}
            other => panic!("{key} must be rejected as traversal, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn range_on_cached_file_slices_and_reports_content_range() {
    let dir = tempdir().unwrap();
    let cfg = test_config(dir.path().to_path_buf());
    let clock = Arc::new(MockClock::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let backend = CountingBackend::counting(b"0123456789".to_vec(), Some("v1".into()), Arc::clone(&calls), None);
    let cache = Cache::new(cfg, clock, registry_with(Arc::new(backend)));
    let mut full = cache.get_by_key("a.png", None).await.unwrap();
    collect(&mut full.body).await;
    wait_entry(&cache, "a.png").await;

    // Cached-file Range: sliced via file seek, no upstream traffic.
    let before = calls.load(Ordering::SeqCst);
    let mut part = cache
        .get_by_key("a.png", Some(ByteRange::bounded(2, 4)))
        .await
        .unwrap();
    assert_eq!(part.outcome, CacheOutcome::Hit);
    assert_eq!(part.content_range.as_ref().map(|c| c.header_value()).as_deref(), Some("bytes 2-5/10"));
    assert_eq!(collect(&mut part.body).await, b"2345");
    assert_eq!(calls.load(Ordering::SeqCst), before);
}

#[tokio::test]
async fn range_cold_miss_offset_zero_streams_full_with_content_range() {
    let dir = tempdir().unwrap();
    let cfg = test_config(dir.path().to_path_buf());
    let clock = Arc::new(MockClock::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let backend = CountingBackend::counting(b"0123456789".to_vec(), None, Arc::clone(&calls), None);
    let cache = Cache::new(cfg, clock, registry_with(Arc::new(backend)));
    let mut hit = cache
        .get_by_key("a.png", Some(ByteRange::from_offset(0)))
        .await
        .unwrap();
    assert_eq!(hit.outcome, CacheOutcome::Miss);
    assert_eq!(hit.content_range.as_ref().map(|c| c.header_value()).as_deref(), Some("bytes 0-9/10"));
    assert_eq!(collect(&mut hit.body).await, b"0123456789");
    wait_entry(&cache, "a.png").await;
}

#[tokio::test]
async fn range_cold_miss_dual_channel_passthrough_and_background_fill() {
    let dir = tempdir().unwrap();
    let cfg = test_config(dir.path().to_path_buf());
    let clock = Arc::new(MockClock::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let backend = CountingBackend::counting(b"0123456789".to_vec(), None, Arc::clone(&calls), None);
    let cache = Cache::new(cfg, clock, registry_with(Arc::new(backend)));

    // Cold miss seeking to byte 3: client gets bytes 3.. immediately
    // (passthrough), while the full flight fills the cache in background.
    let mut hit = cache
        .get_by_key("a.png", Some(ByteRange::from_offset(3)))
        .await
        .unwrap();
    assert_eq!(hit.outcome, CacheOutcome::Miss);
    assert_eq!(hit.content_range.as_ref().map(|c| c.header_value()).as_deref(), Some("bytes 3-9/10"));
    assert_eq!(collect(&mut hit.body).await, b"3456789");
    wait_entry(&cache, "a.png").await;
    assert_eq!(
        std::fs::read(dir.path().join("a.png")).unwrap(),
        b"0123456789".to_vec(),
        "background flight must land a COMPLETE cache file"
    );

    // Next access is a full disk hit.
    let mut hit2 = cache.get_by_key("a.png", None).await.unwrap();
    assert_eq!(hit2.outcome, CacheOutcome::Hit);
    assert_eq!(collect(&mut hit2.body).await, b"0123456789");
}

#[tokio::test]
async fn unsatisfiable_range_rejected() {
    let dir = tempdir().unwrap();
    let cfg = test_config(dir.path().to_path_buf());
    let clock = Arc::new(MockClock::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let backend = CountingBackend::counting(b"short".to_vec(), None, Arc::clone(&calls), None);
    let cache = Cache::new(cfg, clock, registry_with(Arc::new(backend)));
    let mut hit = cache.get_by_key("a.png", None).await.unwrap();
    collect(&mut hit.body).await;
    wait_entry(&cache, "a.png").await;

    let err = match cache
        .get_by_key("a.png", Some(ByteRange::from_offset(99)))
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
    let backend = CountingBackend::counting(b"id3".to_vec(), None, Arc::clone(&calls), None);
    let cache = Cache::new(cfg, clock, registry_with(Arc::new(backend)));
    let mut hit = cache.get_by_key("music/dazbee.flac", None).await.unwrap();
    collect(&mut hit.body).await;
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
    let backend = CountingBackend::counting(b"persisted".to_vec(), Some("v1".into()), Arc::clone(&calls), None);
    let cache = Arc::new(Cache::new(Arc::clone(&cfg), Arc::clone(&clock), registry_with(Arc::new(backend))));
    cache.load_and_start().await;
    let mut hit = cache.get_by_key("a.png", None).await.unwrap();
    assert_eq!(collect(&mut hit.body).await, b"persisted");
    wait_entry(&cache, "a.png").await;
    clock.advance(5000); // last_access moved; eviction order must persist too
    // flush the coalesced access-clock bump to redb
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;

    // "Restart": new Cache instance, fresh clock, backend that would 500 if
    // ever contacted (proving the restart hit is served from disk+redb).
    drop(cache);
    let calls2 = Arc::new(AtomicUsize::new(0));
    let backend2 = CountingBackend::counting(vec![], None, Arc::clone(&calls2), Some(BackendError::ServerError("must not be contacted".into())));
    let clock2 = Arc::new(MockClock::new(5000));
    let cache2 = Arc::new(Cache::new(
        test_config(dir.path().to_path_buf()),
        Arc::clone(&clock2),
        registry_with(Arc::new(backend2)),
    ));
    cache2.load_and_start().await;

    // Entry reloaded from redb: fresh clock (now=5000) vs last_access —
    // still within revalidate ttl, so a plain disk hit with zero upstream.
    let mut hit2 = cache2.get_by_key("a.png", None).await.unwrap();
    assert_eq!(hit2.outcome, CacheOutcome::Hit);
    assert_eq!(collect(&mut hit2.body).await, b"persisted");
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
    let backend = CountingBackend::counting(b"ttl".to_vec(), Some("v1".into()), Arc::new(AtomicUsize::new(0)), None);
    let cache = Arc::new(Cache::new(
        Arc::clone(&cfg),
        Arc::clone(&clock),
        registry_with(Arc::new(backend)),
    ));
    // 50 ms production-style reaper loop instead of the 60 s default.
    cache.load_and_start_with(std::time::Duration::from_millis(50)).await;

    let mut hit = cache.get_by_key("old.png", None).await.unwrap();
    assert_eq!(collect(&mut hit.body).await, b"ttl");
    wait_entry(&cache, "old.png").await;

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
    let backend = StormBackend::new(&payload, Arc::clone(&calls), Arc::new(std::sync::Mutex::new(StormMode::Good)));
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
                .get_by_key("storm.bin", Some(ByteRange::bounded(offset, 64)))
                .await
                .expect("ranged cold miss must succeed");
            let body = collect(&mut hit.body).await;
            (offset, body)
        }));
    }
    for t in tasks {
        let (offset, body) = t.await.unwrap();
        assert_eq!(body, payload[offset as usize..(offset + 64) as usize], "seek @{offset} bytes must be exact");
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2, "1 stat + 1 open for 20 ranged cold misses");
    wait_entry(&cache, "storm.bin").await;

    // Second wave: same seeks served from disk as hits, bytes still exact.
    for i in 0..20u64 {
        let offset = (i * 173) % 4096;
        let mut hit = cache.get_by_key("storm.bin", Some(ByteRange::bounded(offset, 64))).await.unwrap();
        assert_eq!(hit.outcome, CacheOutcome::Hit);
        assert_eq!(collect(&mut hit.body).await, payload[offset as usize..(offset + 64) as usize]);
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
    let backend = StormBackend::new(&payload, Arc::new(AtomicUsize::new(0)), Arc::clone(&mode));
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
            match cache.get_by_key("flaky.bin", None).await {
                Err(_) => (Vec::new(), true),
                Ok(hit) => collect_allow_error(hit.body).await,
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
    let mut hit = cache.get_by_key("flaky.bin", None).await.unwrap();
    assert_eq!(collect(&mut hit.body).await, payload, "retry after failure must succeed");
}

/// A driver panic must not leave a zombie flight: readers error out, the
/// map entry is released, and the key is immediately retryable (B3).
#[tokio::test]
async fn panicked_driver_fails_flight_and_releases_key() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let payload: Vec<u8> = (0..100u32).map(|i| (i % 251) as u8).collect();
    let mode = Arc::new(std::sync::Mutex::new(StormMode::PanicOpen));
    let backend = StormBackend::new(&payload, Arc::new(AtomicUsize::new(0)), Arc::clone(&mode));
    let cache = Arc::new(Cache::new(
        test_config(dir.path().to_path_buf()),
        Arc::clone(&clock),
        registry_with(Arc::new(backend)),
    ));

    let cache2 = Arc::clone(&cache);
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), async move {
        cache2.get_by_key("panic.bin", None).await
    })
    .await
    .expect("must not hang on a panicking driver");
    // Fast panic: Failed may beat Meta — get() errors directly. Either
    // surfacing is fine; hanging forever is not.
    let errored = match outcome {
        Err(_) => true,
        Ok(hit) => collect_allow_error(hit.body).await.1,
    };
    assert!(errored, "panic must surface as an error");
    wait_map_empty(&cache).await;
    assert!(cache.state.read().await.entries.is_empty(), "a panicked flight installs nothing");

    *mode.lock().unwrap() = StormMode::Good;
    let mut hit = cache.get_by_key("panic.bin", None).await.unwrap();
    assert_eq!(collect(&mut hit.body).await, payload, "key must be retryable after a panic");
}

/// A short upstream body must never be sealed into the cache: the flight
/// fails, no entry is installed, and a retry pulls the full object (B2).
#[tokio::test]
async fn short_upstream_body_is_never_sealed() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let payload: Vec<u8> = (0..100u32).map(|i| (i % 251) as u8).collect();
    let mode = Arc::new(std::sync::Mutex::new(StormMode::ShortBody));
    let backend = StormBackend::new(&payload, Arc::new(AtomicUsize::new(0)), Arc::clone(&mode));
    let cache = Arc::new(Cache::new(
        test_config(dir.path().to_path_buf()),
        Arc::clone(&clock),
        registry_with(Arc::new(backend)),
    ));

    let (out, errored) = match cache.get_by_key("short.bin", None).await {
        Err(_) => (Vec::new(), true),
        Ok(hit) => collect_allow_error(hit.body).await,
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
    let mut hit = cache.get_by_key("short.bin", None).await.unwrap();
    assert_eq!(collect(&mut hit.body).await, payload, "retry must pull the full object");
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
    let backend = CountingBackend::counting(payload.clone(), Some("v1".into()), Arc::new(AtomicUsize::new(0)), None);
    let cache = Arc::new(Cache::new(
        test_config(dir.path().to_path_buf()),
        Arc::clone(&clock),
        registry_with(Arc::new(backend)),
    ));
    cache.load_and_start().await;

    // Move the clock so the access stamps carry a distinguishable value,
    // then cold-fill and hammer the hot key concurrently.
    clock.advance(5000);
    let mut first = cache.get_by_key("hot.bin", None).await.unwrap();
    assert_eq!(collect(&mut first.body).await, payload);
    wait_entry(&cache, "hot.bin").await;

    let mut tasks = Vec::new();
    for _ in 0..32 {
        let cache = Arc::clone(&cache);
        let expect = payload.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..8 {
                let mut hit = cache.get_by_key("hot.bin", None).await.unwrap();
                assert_eq!(hit.outcome, CacheOutcome::Hit);
                assert_eq!(collect(&mut hit.body).await, expect);
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


/// With the stream gate saturated, a metadata stat must still complete
/// promptly (B1). Under the old single gate it queued behind the transfer.
#[tokio::test]
async fn head_not_starved_by_saturated_stream_gate() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let cfg = test_config(dir.path().to_path_buf());
    let release = Arc::new(tokio::sync::Notify::new());
    let opened = Arc::new(AtomicUsize::new(0));
    let backend = Arc::new(BlockingOpenBackend::new(&vec![7u8; 4096], Arc::clone(&release), Arc::clone(&opened)));

    // Deliberately saturate the STREAM gate (2 permits) with two held
    // transfers on distinct keys.
    let mut slots = HashMap::new();
    let slot = Arc::new(BackendSlot::new(backend, 2));
    slots.insert("primary".to_string(), Arc::clone(&slot));
    let cache = Arc::new(Cache::new(cfg, Arc::clone(&clock), BackendRegistry::new(slots)));

    for key in ["a.bin", "b.bin"] {
        let c = Arc::clone(&cache);
        tokio::spawn(async move {
            let _ = c.get_by_key(key, None).await; // blocks in open()
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
        cache.head_by_key("c.bin"),
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


/// An object larger than the magazine is admitted as a RESIDENT STRAY
/// (ADR-0014): cached, because one upstream stream then serves every later
/// range, and harmless on the way in — the magazine's own members are not
/// evicted for it, and the byte budget neither counts nor picks it.
#[tokio::test]
async fn an_object_larger_than_the_magazine_is_cached_as_a_stray() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let opens = Arc::new(AtomicUsize::new(0));
    let backend = Arc::new(SizedBackend::new(&[("member.bin", 100), ("stray.bin", 4_000)], Arc::clone(&opens)));
    let mut slots = HashMap::new();
    slots.insert("primary".to_string(), Arc::new(BackendSlot::new(backend, 3)));
    let mut cfg = Config { cache_dir: dir.path().to_path_buf(), ..Config::default() };
    cfg.max_size_bytes = 1_000;
    let cache = Arc::new(Cache::new(Arc::new(cfg), Arc::clone(&clock), BackendRegistry::new(slots)));

    let mut hit = cache.get_by_key("member.bin", None).await.unwrap();
    collect(&mut hit.body).await;
    wait_entry(&cache, "member.bin").await;

    let mut hit = cache.get_by_key("stray.bin", None).await.unwrap();
    collect(&mut hit.body).await;
    wait_entry(&cache, "stray.bin").await;

    {
        let s = cache.state.read().await;
        assert!(
            s.entries.contains_key("member.bin"),
            "a stray must not evict the magazine on its way in"
        );
        assert!(
            s.entries.get("stray.bin").expect("stray installed").oversize,
            "the row records that it was admitted outside the budget"
        );
        assert_eq!(s.total_bytes, 4_100, "both objects are on disk");
    }
    let snap = cache.snapshot().await;
    assert_eq!(snap.stray_bytes, 4_000, "and healthz reports the part the budget does not govern");
    assert!(snap.total_bytes > 1_000, "a stray is not bounded by the magazine's byte budget");

    // Its bytes are served like any cached object: no upstream open at all.
    let before = opens.load(Ordering::SeqCst);
    let mut hit = cache
        .get_by_key("stray.bin", Some(ByteRange::bounded(0, 10)))
        .await
        .unwrap();
    let body = collect(&mut hit.body).await;
    assert_eq!(body, (0..10u64).map(|i| (i % 251) as u8).collect::<Vec<u8>>());
    assert_eq!(
        opens.load(Ordering::SeqCst),
        before,
        "a cached stray serves ranges from this node's disk"
    );
}

/// The cache never refuses to SERVE, only to cache (ADR-0013). An object no
/// disk could hold — and with no stray to evict for it — is answered through
/// the pipe: no flight, no entry, no bytes on disk, no 502.
#[tokio::test]
async fn a_cold_pull_the_disk_cannot_hold_is_served_without_caching() {
    use origin_cache::cache::cache::{BodySource, ServeOutcome};

    let dir = tempdir().unwrap();
    let opens = Arc::new(AtomicUsize::new(0));
    let backend = Arc::new(SizedBackend::new(&[("huge.bin", u64::MAX / 4)], Arc::clone(&opens)));
    let mut slots = HashMap::new();
    slots.insert("primary".to_string(), Arc::new(BackendSlot::new(backend, 3)));
    let clock = Arc::new(MockClock::new(0));
    let cache = Arc::new(Cache::new(test_config(dir.path().to_path_buf()), Arc::clone(&clock), BackendRegistry::new(slots)));

    let rk = cache.resolve("huge.bin").unwrap();
    let out = cache
        .serve(&rk, Some(ByteRange::bounded(0, 8)), None)
        .await
        .expect("a cache that cannot hold an object must still serve it");
    let plan = match out {
        ServeOutcome::Stream(plan) => plan,
        _ => panic!("a ranged cold miss must stream"),
    };
    assert_eq!(plan.source, BodySource::Upstream, "the bytes came straight from upstream");
    let mut body = plan.body;
    assert_eq!(
        collect(&mut body).await,
        (0..8u64).map(|i| (i % 251) as u8).collect::<Vec<u8>>()
    );
    assert_eq!(cache.flights.active().await, 0, "no flight for an object that cannot be kept");
    assert!(cache.state.read().await.entries.is_empty(), "and nothing was installed");
    assert_eq!(opens.load(Ordering::SeqCst), 1, "one ranged upstream open, no fill");
    assert!(
        !dir.path().join("huge.bin").exists() && !dir.path().join("huge.bin").is_file(),
        "nothing was written to the cache disk"
    );
}

/// The efficient passthrough MOVES BYTES, so it must hold the STREAM gate
/// (B1 / ADR-0004) — for the transfer, not just until the response is
/// built. Its old shape took the metadata gate (the head-of-line class
/// ADR-0004 split off) and released it when the response was constructed,
/// so two viewers of the same range opened two upstream streams and the
/// stream budget saw neither.
#[tokio::test]
async fn efficient_passthrough_waits_for_a_stream_permit() {
    use origin_cache::cache::cache::ServeOutcome;
    use origin_cache::config::CacheProfile;

    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let mut cfg = Config { cache_dir: dir.path().to_path_buf(), ..Config::default() };
    cfg.upstreams[0].cache_profile = "efficient".into();
    cfg.cache_profiles.insert(
        "efficient".into(),
        CacheProfile { min_file_size: 1, coverage_window_secs: 3600 },
    );
    let cfg = Arc::new(cfg);

    let release = Arc::new(tokio::sync::Notify::new());
    let opened = Arc::new(AtomicUsize::new(0));
    let backend = Arc::new(BlockingOpenBackend::new(&vec![7u8; 4096], Arc::clone(&release), Arc::clone(&opened)));
    let mut slots = HashMap::new();
    // Two stream permits; the test holds both, so no transfer can start.
    let slot = Arc::new(BackendSlot::new(backend, 2));
    slots.insert("primary".to_string(), Arc::clone(&slot));
    let cache = Arc::new(Cache::new(cfg, Arc::clone(&clock), BackendRegistry::new(slots)));

    let mut held = Vec::new();
    for _ in 0..2 {
        held.push(Arc::clone(&slot.stream_gate).acquire_owned().await.unwrap());
    }
    assert_eq!(slot.stream_gate.available_permits(), 0, "stream gate saturated");

    let rk = cache.resolve("a.bin").unwrap();
    let passthrough = {
        let cache = Arc::clone(&cache);
        let rk = rk.clone();
        tokio::spawn(async move { cache.serve(&rk, Some(ByteRange::bounded(0, 64)), None).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(
        opened.load(Ordering::SeqCst),
        0,
        "a passthrough must take a stream permit BEFORE opening upstream"
    );

    // Free the gate: the transfer proceeds, and the permit it takes must
    // stay held for the body rather than being dropped when `serve` returns.
    drop(held);
    for _ in 0..200 {
        if opened.load(Ordering::SeqCst) >= 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert_eq!(opened.load(Ordering::SeqCst), 1, "the passthrough must now open upstream");
    assert_eq!(
        slot.stream_gate.available_permits(),
        1,
        "the transfer holds the permit it took"
    );

    release.notify_one();
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), passthrough)
        .await
        .expect("the passthrough must finish once the upstream opens")
        .unwrap()
        .unwrap();
    let plan = match outcome {
        ServeOutcome::Stream(plan) => plan,
        _ => panic!("a ranged efficient miss must stream"),
    };
    assert_eq!(
        slot.stream_gate.available_permits(),
        1,
        "the permit must be held by the body, not released when the response was built"
    );

    let mut body = plan.body;
    assert_eq!(collect(&mut body).await, vec![7u8; 64]);
    // The permit belongs to the RUN, not to this response (ADR-0016): it is
    // held while the run's window transfers — which is what lets one open serve
    // every request inside that window — and released when the window ends,
    // possibly after this body is already consumed. So poll rather than sample.
    for _ in 0..400 {
        if slot.stream_gate.available_permits() == 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert_eq!(
        slot.stream_gate.available_permits(),
        2,
        "the run releases its window's permit when the window is done"
    );
}


// ---------------------------------------------------------------------------
// P9: listing snapshots let later pages reuse one upstream walk.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn listing_snapshot_is_reused_then_expires() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let _cache = Arc::new(Cache::new(
        test_config(dir.path().to_path_buf()),
        Arc::clone(&clock),
        registry_with(Arc::new(CountingBackend::counting(b"x".to_vec(), None, Arc::new(AtomicUsize::new(0)), None))),
    ));

    let entries = vec![
        ListEntry { key: "a/1.bin".into(), size: 1, etag: None, last_modified: None, is_dir: false },
        ListEntry { key: "a/2.bin".into(), size: 2, etag: None, last_modified: None, is_dir: false },
    ];
    // The memo lives beside the listing logic now, not on the cache, and it
    // takes `now` from its caller rather than holding a clock.
    let listings = origin_cache::list::ListingCache::default();
    let at = |clock: &MockClock| clock.now_millis();

    assert!(listings.get("up", "a/", true, at(&clock)).await.is_none(), "cold snapshot is empty");
    listings.put("up", "a/", true, &entries, at(&clock)).await;

    let got = listings.get("up", "a/", true, at(&clock)).await.expect("snapshot present");
    assert_eq!(*got, entries, "the second page sees the same walk");
    // The snapshot is shared, not copied (O5): the returned handle must
    // alias the stored one, not a fresh vector.
    let again = listings.get("up", "a/", true, at(&clock)).await.unwrap();
    assert!(Arc::ptr_eq(&got, &again), "pages must share one snapshot allocation");

    // Different selector = different snapshot.
    assert!(listings.get("up", "a/", false, at(&clock)).await.is_none());

    // After the TTL it is gone.
    clock.advance(6_000);
    assert!(listings.get("up", "a/", true, at(&clock)).await.is_none(), "snapshot expired");
}

// ---------------------------------------------------------------------------
// P10: entry-count eviction budget.
// ---------------------------------------------------------------------------

/// A byte-only budget lets many small objects exhaust RAM; the entry-count
/// cap must evict the least-recently-used rows even when bytes are far
/// under max_size_bytes.
#[tokio::test]
async fn entry_count_cap_evicts_lru_even_under_byte_budget() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let mut cfg = Config::default();
    cfg.cache_dir = dir.path().to_path_buf();
    cfg.max_size_bytes = 1 << 40; // huge: bytes never trigger
    cfg.max_entries = 3; // the count cap is the only active budget
    let cfg = Arc::new(cfg);

    let backend = CountingBackend::counting(b"tiny".to_vec(), Some("v1".into()), Arc::new(AtomicUsize::new(0)), None);
    let cache = Arc::new(Cache::new(Arc::clone(&cfg), Arc::clone(&clock), registry_with(Arc::new(backend))));

    // Fill 5 distinct keys, advancing the clock so recency is ordered.
    for (i, key) in ["k1", "k2", "k3", "k4", "k5"].iter().enumerate() {
        clock.advance(1000);
        let mut hit = cache.get_by_key(key, None).await.unwrap();
        let _ = collect(&mut hit.body).await;
        wait_entry(&cache, key).await;
        let _ = i;
    }

    let s = cache.state.read().await;
    assert!(
        s.entries.len() <= 3,
        "entry-count cap must bound the map (got {} entries)",
        s.entries.len()
    );
    assert!(s.entries.contains_key("k5"), "the most recent key must survive");
    drop(s);
}

// ---------------------------------------------------------------------------
// P56: disk capacity is bounded — staged sidecars join the budget, stale
// temps are swept periodically, and a cold pull is refused when the disk
// is too full.
// ---------------------------------------------------------------------------

/// Staged (segment) bytes must count against max_size_bytes, not only the
/// entry byte total. Before this, an efficient-profile scrub could stage
/// unbounded sidecar bytes while total_bytes stayed at zero.
#[tokio::test]
async fn staged_segment_bytes_join_the_disk_budget() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let mut cfg = Config::default();
    cfg.cache_dir = dir.path().to_path_buf();
    cfg.inactive_ttl_secs = 1_200;
    cfg.max_size_bytes = 4096; // tiny: staged bytes alone exceed it
    let cfg = Arc::new(cfg);

    let backend = CountingBackend::counting(b"x".to_vec(), Some("v1".into()), Arc::new(AtomicUsize::new(0)), None);
    let cache = Arc::new(Cache::new(Arc::clone(&cfg), Arc::clone(&clock), registry_with(Arc::new(backend))));

    // Two staged sidecars of 8 KiB each, touched well in the past so the
    // min-age guard (60s) allows eviction. The eviction is span-level and
    // only touches a span with a sidecar file behind it (a merged ledger
    // interval owns no single file), so the files are written for real: a
    // ledger row with nothing on disk is not evictable by design, and
    // asserting on one would test nothing.
    let touched = 10_000u64;
    clock.advance(120_000);
    let mut paths = Vec::new();
    {
        let mut cov = cache.coverage.lock().await;
        for key in ["s1.bin", "s2.bin"].iter() {
            let path = origin_cache::cache::store::seg_path(&cfg.cache_dir, key, 0, 8192);
            std::fs::write(&path, vec![0u8; 8192]).unwrap();
            paths.push(path);
            let mut c = origin_cache::cache::store::Coverage {
                total: 8192,
                last_touch_millis: touched,
                ..Default::default()
            };
            c.add_interval(0, 8192, touched);
            cov.insert(key.to_string(), c);
        }
    }
    cache.state.write().await.segment_bytes = 16_384; // > max_size_bytes

    cache.tick().await;

    let s = cache.state.read().await;
    let cov = cache.coverage.lock().await;
    assert!(
        s.total_bytes.saturating_add(s.segment_bytes) <= cfg.max_size_bytes,
        "staged bytes must be brought back under the shared budget (got {} + {})",
        s.total_bytes,
        s.segment_bytes
    );
    assert!(cov.is_empty(), "evicted ledger rows must be dropped");
    assert!(paths.iter().all(|p| !p.exists()), "the evicted sidecar files must be gone");
}

// ---------------------------------------------------------------------------
// Staged-read runs (ADR-0016): one upstream stream per key, shared by every
// reader inside its window.
// ---------------------------------------------------------------------------

/// An efficient-profile cache with a small window over a paced synthetic
/// object. Returns the upstream open counter the fake keeps.
fn run_fixture(
    dir: &std::path::Path,
    object_bytes: u64,
    window: u64,
    pace_ms: u64,
) -> (Arc<Cache<MockClock>>, Arc<AtomicUsize>) {
    let clock = Arc::new(MockClock::new(0));
    let mut cfg = Config {
        cache_dir: dir.to_path_buf(),
        ..Config::default()
    };
    cfg.upstreams[0].cache_profile = "efficient".into();
    cfg.cache_profiles.insert(
        "efficient".into(),
        origin_cache::config::CacheProfile { min_file_size: 1, coverage_window_secs: 3600 },
    );
    cfg.session_window_bytes = window;
    cfg.max_size_bytes = 64 * 1024 * 1024;
    let cfg = Arc::new(cfg);
    let opens = Arc::new(AtomicUsize::new(0));
    let backend = Arc::new(
        SizedBackend::new(&[("a.bin", object_bytes)], Arc::clone(&opens))
            .paced(1024, std::time::Duration::from_millis(pace_ms)),
    );
    let mut slots = HashMap::new();
    slots.insert("primary".to_string(), Arc::new(BackendSlot::new(backend, 3)));
    let cache = Arc::new(Cache::new(cfg, clock, BackendRegistry::new(slots)));
    (cache, opens)
}

/// Bytes of the synthetic object (`byte = offset % 251`), so a body can be
/// checked without materializing the object.
fn synthetic(offset: u64, len: u64) -> Vec<u8> {
    (offset..offset + len).map(|i| (i % 251) as u8).collect()
}

/// One ranged GET, returning the plan and asserting it streamed.
async fn get_range(cache: &Arc<Cache<MockClock>>, offset: u64, len: u64) -> origin_cache::cache::cache::StreamPlan {
    use origin_cache::cache::cache::ServeOutcome;
    let rk = cache.resolve("a.bin").unwrap();
    match cache.serve(&rk, Some(ByteRange::bounded(offset, len)), None).await.unwrap() {
        ServeOutcome::Stream(p) => p,
        _ => panic!("expected a streamed passthrough from a ranged efficient miss"),
    }
}

/// Wait until the key has exactly `bytes` staged (the seal lands after the
/// body's last byte, so staged state is polled, never sampled).
async fn wait_staged(cache: &Arc<Cache<MockClock>>, bytes: u64) {
    for _ in 0..400 {
        if cache.state.read().await.segment_bytes == bytes {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    panic!(
        "segment_bytes never became {bytes}; got {}",
        cache.state.read().await.segment_bytes
    );
}

/// The headline of ADR-0016: seeks that land inside a live run's window ride
/// its watermark instead of opening upstream. Three requests, ONE open — the
/// per-request open cost (~640 ms measured) is what this removes from a scrub.
#[tokio::test]
async fn ranged_seeks_on_one_key_share_one_upstream_open() {
    let dir = tempdir().unwrap();
    // 1 MiB object, 8 KiB window, body paced so the run is still live while
    // the later seeks arrive — the overlap this mechanism exists for.
    let (cache, opens) = run_fixture(dir.path(), 1 << 20, 8192, 10);

    for offset in [0u64, 2048, 4096] {
        let plan = get_range(&cache, offset, 1024).await;
        assert_eq!(
            plan.source,
            origin_cache::cache::cache::BodySource::Stage,
            "a run-served response reads staged bytes, not a fresh upstream stream"
        );
        let mut body = plan.body;
        assert_eq!(collect(&mut body).await, synthetic(offset, 1024), "seek at {offset}");
    }
    assert_eq!(
        opens.load(Ordering::SeqCst),
        1,
        "three seeks inside one window must share one upstream open"
    );
}

/// A seek the live run does not cover is the escape the design pins: it opens
/// its own exact Range instead of waiting on somebody else's window, and both
/// bodies are byte-exact.
#[tokio::test]
async fn a_seek_far_beyond_the_window_opens_its_own_range() {
    let dir = tempdir().unwrap();
    let (cache, opens) = run_fixture(dir.path(), 1 << 20, 8192, 10);

    for offset in [0u64, 65536] {
        let plan = get_range(&cache, offset, 1024).await;
        let mut body = plan.body;
        assert_eq!(collect(&mut body).await, synthetic(offset, 1024));
    }
    assert_eq!(
        opens.load(Ordering::SeqCst),
        2,
        "a far seek takes its own Range rather than riding the window"
    );
}

/// Once a run has sealed, its window is a normal staged span: a later read
/// inside it is served from disk with no upstream open and no run.
#[tokio::test]
async fn a_sealed_run_leaves_spans_that_serve_later_reads() {
    let dir = tempdir().unwrap();
    let (cache, opens) = run_fixture(dir.path(), 1 << 20, 8192, 10);

    let plan = get_range(&cache, 0, 1024).await;
    let mut body = plan.body;
    assert_eq!(collect(&mut body).await, synthetic(0, 1024));
    wait_staged(&cache, 8192).await;

    // The next read sits inside the sealed window.
    let plan = get_range(&cache, 1024, 1024).await;
    assert_eq!(plan.source, origin_cache::cache::cache::BodySource::Stage);
    let mut body = plan.body;
    assert_eq!(collect(&mut body).await, synthetic(1024, 1024));
    assert_eq!(
        opens.load(Ordering::SeqCst),
        1,
        "the sealed span answers the later read without touching upstream"
    );
}

/// A window per open, across a walk: four windows of a 32 KiB object are four
/// opens, not one per request (which is what a shard walk used to cost).
#[tokio::test]
async fn a_sequential_walk_opens_once_per_window() {
    let dir = tempdir().unwrap();
    let (cache, opens) = run_fixture(dir.path(), 32 * 1024, 8192, 0);

    for i in 0..4u64 {
        let offset = i * 8192;
        let plan = get_range(&cache, offset, 1024).await;
        let mut body = plan.body;
        assert_eq!(collect(&mut body).await, synthetic(offset, 1024));
    }
    assert_eq!(
        opens.load(Ordering::SeqCst),
        4,
        "one open per window, not one per request"
    );
}

/// The run's own failure reaches its readers instead of parking them: a body
/// that already promised bytes fails rather than hanging (the flight's
/// contract, inherited).
#[tokio::test]
async fn a_run_that_cannot_open_errors_its_reader_instead_of_hanging() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let mut cfg = Config {
        cache_dir: dir.path().to_path_buf(),
        ..Config::default()
    };
    cfg.upstreams[0].cache_profile = "efficient".into();
    cfg.cache_profiles.insert(
        "efficient".into(),
        origin_cache::config::CacheProfile { min_file_size: 1, coverage_window_secs: 3600 },
    );
    cfg.session_window_bytes = 8192;
    let cfg = Arc::new(cfg);
    let backend = Arc::new(StormBackend::new(
        b"payload",
        Arc::new(AtomicUsize::new(0)),
        Arc::new(std::sync::Mutex::new(StormMode::FailOpen)),
    ));
    let mut slots = HashMap::new();
    slots.insert("primary".to_string(), Arc::new(BackendSlot::new(backend, 3)));
    let cache = Arc::new(Cache::new(cfg, clock, BackendRegistry::new(slots)));

    let plan = get_range(&cache, 0, 1024).await;
    let (bytes, errored) = collect_allow_error(plan.body).await;
    assert!(bytes.is_empty(), "no bytes can come from a failed open");
    assert!(errored, "the reader must see the failure, not wait forever");
}

/// The evictor takes its candidates from DISK and re-derives the row from the
/// survivors, so a ledger that has stopped being 1:1 with the files still
/// evicts. This is the post-`compact` shape: one merged interval covering four
/// real spans. Before that change the evictor looked for a sidecar named
/// after the *interval* (`.seg.m.bin.0-4096`, which was never written), found
/// no candidate and freed nothing — the key's bytes were unreclaimable until
/// the inactivity sweep.
#[tokio::test]
async fn a_merged_interval_still_evicts_one_file_at_a_time() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let mut cfg = Config::default();
    cfg.cache_dir = dir.path().to_path_buf();
    cfg.inactive_ttl_secs = 1_200;
    cfg.max_size_bytes = 3072; // four spans staged, budget for three
    let cfg = Arc::new(cfg);
    let backend = CountingBackend::counting(b"x".to_vec(), Some("v1".into()), Arc::new(AtomicUsize::new(0)), None);
    let cache = Arc::new(Cache::new(Arc::clone(&cfg), Arc::clone(&clock), registry_with(Arc::new(backend))));

    let touched = 10_000u64;
    clock.advance(120_000);
    let mut paths = Vec::new();
    for i in 0..4u64 {
        let path = origin_cache::cache::store::seg_path(&cfg.cache_dir, "m.bin", i * 1024, (i + 1) * 1024);
        std::fs::write(&path, vec![0u8; 1024]).unwrap();
        paths.push(path);
    }
    {
        let mut cov = cache.coverage.lock().await;
        let mut c = origin_cache::cache::store::Coverage {
            total: 4096,
            last_touch_millis: touched,
            ..Default::default()
        };
        // One interval for the whole walk: what `compact` leaves behind.
        c.add_interval(0, 4096, touched);
        assert_eq!(c.intervals.len(), 1, "the premise is a merged ledger");
        cov.insert("m.bin".to_string(), c);
    }
    cache.state.write().await.segment_bytes = 4096;

    cache.tick().await;

    let s = cache.state.read().await;
    let cov = cache.coverage.lock().await;
    assert_eq!(s.segment_bytes, 3072, "one span's worth must be freed");
    assert!(paths[0].exists() == false, "the stalest span (lowest offset) goes first");
    assert!(paths[1].exists() && paths[2].exists() && paths[3].exists(), "the rest stay");
    assert_eq!(
        cov.get("m.bin").unwrap().intervals,
        vec![(1024, 2048, touched, 0), (2048, 3072, touched, 0), (3072, 4096, touched, 0)],
        "the rebuilt ledger describes exactly the surviving files, carrying their read time"
    );
}

/// The same hole from the other two entrances: `decay` (and the ceiling's
/// `drop_coldest`) leaves files on disk with no interval naming them. A row
/// that has lost every record is still evictable, and the rebuild gives the
/// survivors a fresh stamp rather than pretending they were never staged.
#[tokio::test]
async fn a_decayed_interval_leaves_its_files_evictable() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let mut cfg = Config::default();
    cfg.cache_dir = dir.path().to_path_buf();
    cfg.inactive_ttl_secs = 1_200;
    cfg.max_size_bytes = 1024; // two spans staged, budget for one
    let cfg = Arc::new(cfg);
    let backend = CountingBackend::counting(b"x".to_vec(), Some("v1".into()), Arc::new(AtomicUsize::new(0)), None);
    let cache = Arc::new(Cache::new(Arc::clone(&cfg), Arc::clone(&clock), registry_with(Arc::new(backend))));

    let touched = 10_000u64;
    clock.advance(120_000);
    let mut paths = Vec::new();
    for i in 0..2u64 {
        let path = origin_cache::cache::store::seg_path(&cfg.cache_dir, "d.bin", i * 1024, (i + 1) * 1024);
        std::fs::write(&path, vec![0u8; 1024]).unwrap();
        paths.push(path);
    }
    {
        let mut cov = cache.coverage.lock().await;
        // No intervals at all: every record of these bytes decayed away.
        cov.insert(
            "d.bin".to_string(),
            origin_cache::cache::store::Coverage {
                total: 2048,
                last_touch_millis: touched,
                ..Default::default()
            },
        );
    }
    cache.state.write().await.segment_bytes = 2048;

    cache.tick().await;

    let s = cache.state.read().await;
    let cov = cache.coverage.lock().await;
    assert_eq!(s.segment_bytes, 1024);
    assert!(!paths[0].exists() && paths[1].exists(), "one file goes, one stays");
    let iv = &cov.get("d.bin").unwrap().intervals;
    assert_eq!(iv.len(), 1, "the survivor is recorded again");
    assert_eq!((iv[0].0, iv[0].1), (1024, 2048));
}

/// The scale shape: a sequential 1-byte-per-span walk past the ledger ceiling.
/// `compact` merges touching intervals (4097 files -> 2049 two-byte intervals),
/// so no interval's bounds ever equal a file's bounds — and the budget must
/// still be brought back under. This is the workload the node cannot test with
/// a real 200 GB object (disk 183 GB, upstream max 3.2 GB), so it is pinned
/// here at the level where the ceiling actually fires.
#[tokio::test]
async fn a_sequential_walk_past_the_ledger_ceiling_stays_evictable() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let mut cfg = Config::default();
    cfg.cache_dir = dir.path().to_path_buf();
    cfg.inactive_ttl_secs = 1_200;
    cfg.max_size_bytes = 1000;
    let cfg = Arc::new(cfg);
    let backend = CountingBackend::counting(b"x".to_vec(), Some("v1".into()), Arc::new(AtomicUsize::new(0)), None);
    let cache = Arc::new(Cache::new(Arc::clone(&cfg), Arc::clone(&clock), registry_with(Arc::new(backend))));

    let touched = 10_000u64;
    clock.advance(120_000);
    let spans = origin_cache::cache::store::MAX_INTERVALS_PER_KEY + 1;
    for i in 0..spans as u64 {
        let path = origin_cache::cache::store::seg_path(&cfg.cache_dir, "walk.bin", i, i + 1);
        std::fs::write(&path, vec![7u8; 1]).unwrap();
    }
    {
        let mut cov = cache.coverage.lock().await;
        let mut c = origin_cache::cache::store::Coverage {
            total: spans as u64,
            last_touch_millis: touched,
            ..Default::default()
        };
        for i in 0..spans as u64 {
            c.add_interval(i, i + 1, touched);
        }
        assert!(
            c.intervals.len() < spans,
            "the ceiling must have merged the walk (got {} intervals)",
            c.intervals.len()
        );
        cov.insert("walk.bin".to_string(), c);
    }
    cache.state.write().await.segment_bytes = spans as u64;

    cache.tick().await;

    let s = cache.state.read().await;
    let files = origin_cache::cache::store::segments_for_key(&cfg.cache_dir, "walk.bin").len();
    let cov = cache.coverage.lock().await;
    assert_eq!(s.segment_bytes, 1000, "the budget is respected");
    assert_eq!(files, 1000, "exactly one file per byte of the budget remains");
    let covered: u64 = cov.get("walk.bin").unwrap().intervals.iter().map(|(s, e, ..)| e - s).sum();
    assert_eq!(covered, 1000, "the rebuilt ledger covers exactly the surviving files");
}

/// A mid-write failure must not leave the temp file behind: the leak used
/// to persist until the next restart.
#[tokio::test]
async fn failed_pump_removes_its_temp_file() {
    use origin_cache::cache::flight::pump_and_seal;
    let dir = tempdir().unwrap();
    let tmp = dir.path().join(".tmp.leak");
    let finalp = dir.path().join("leak.bin");
    let (tx, _rx) = tokio::sync::watch::channel(FlightProgress::Pending);

    // A stream that errors after yielding a little data.
    struct FailingStream;
    impl tokio::io::AsyncRead for FailingStream {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            buf.put_slice(&[1u8; 16]);
            std::task::Poll::Ready(Err(std::io::Error::other("upstream died")))
        }
    }
    let src = StreamSource {
        stream: Box::new(FailingStream),
        total_len: Some(1024),
    };

    let res = pump_and_seal(src, &tmp, &finalp, &tx).await;
    assert!(res.is_err(), "a failing stream must fail the pump");
    assert!(!tmp.exists(), "the temp file must be removed on failure");
}

/// The free-space guard helper must refuse a transfer that would cross the
/// reserve floor, and must never block when free space is unknown.
#[test]
fn disk_room_check_respects_the_reserve() {
    use origin_cache::cache::store::{free_bytes, has_room_for};
    let dir = tempfile::tempdir().unwrap();
    let free = free_bytes(dir.path()).expect("statvfs must work on a real dir");
    assert!(free > 0, "a real filesystem reports free space");

    // Absurdly large request must be refused.
    assert!(!has_room_for(dir.path(), u64::MAX, 0));
    // A tiny request with no reserve fits.
    assert!(has_room_for(dir.path(), 1, 0));
    // A reserve larger than the disk must refuse even a tiny request.
    assert!(!has_room_for(dir.path(), 1, u64::MAX));
    // Unknown path must not block serving (None -> true).
    assert!(has_room_for(std::path::Path::new("/nonexistent/probe/path"), 1, 0));
}

// ---------------------------------------------------------------------------
// #57: metadata loss must degrade, not crash-loop, and the bytes on disk
// must not become orphans.
// ---------------------------------------------------------------------------

/// An unopenable metadata file must not kill the process: the store
/// quarantines it and opens fresh (before this it propagated into a boot
/// panic, and with Restart=always a crash loop).
#[test]
fn unopenable_metadata_is_quarantined_not_fatal() {
    use origin_cache::cache::persist::MetaStore;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("redb.db");
    // A directory where the db file belongs: redb cannot open it.
    std::fs::create_dir_all(path.join("child")).unwrap();
    let store = MetaStore::open(&path).expect("open must degrade, not error out");
    drop(store);
    assert!(path.is_file(), "a real db file must exist after the quarantine");
    let stray: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().contains("corrupt"))
        .collect();
    assert!(!stray.is_empty(), "the bad path should be moved aside, not silently dropped");
}

/// A corrupt-but-openable store yields no rows; a cache started on it must
/// rebuild entry rows from the object files so the bytes stay served.
#[tokio::test]
async fn metadata_loss_rebuilds_rows_from_the_object_tree() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let cfg = test_config(dir.path().to_path_buf());
    let backend = CountingBackend::counting(b"payload".to_vec(), Some("v1".into()), Arc::new(AtomicUsize::new(0)), None);

    // First cache: store an object normally.
    {
        let cache = Arc::new(Cache::new(Arc::clone(&cfg), Arc::clone(&clock), registry_with(Arc::new(backend.clone()))));
        cache.load_and_start().await;
        let mut hit = cache.get_by_key("kept.bin", None).await.unwrap();
        assert_eq!(collect(&mut hit.body).await, b"payload");
        wait_entry(&cache, "kept.bin").await;
    }

    // Simulate metadata loss: wipe the store's rows by removing redb.db
    // while leaving the object file in place (the runbook's recovery step).
    std::fs::remove_file(cfg.cache_dir.join("redb.db")).unwrap();
    assert!(cfg.cache_dir.join("kept.bin").exists(), "object file survived");

    // Second cache: no rows exist, but the object does -- rebuild it.
    let cache2 = Arc::new(Cache::new(Arc::clone(&cfg), Arc::clone(&clock), registry_with(Arc::new(backend))));
    cache2.load_and_start().await;

    {
        let s = cache2.state.read().await;
        assert!(
            s.entries.contains_key("kept.bin"),
            "the row must be rebuilt from the object tree (got {:?})",
            s.entries.keys().collect::<Vec<_>>()
        );
        assert_eq!(s.entries.get("kept.bin").unwrap().size_bytes, 7);
    }
}

/// O2: eviction must pick the same victims, in the same order, as the
/// original repeated-minimum scan — and do it in one pass.
#[tokio::test]
async fn eviction_picks_lru_victims_in_order() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let mut cfg = Config::default();
    cfg.cache_dir = dir.path().to_path_buf();
    cfg.max_entries = 3; // count budget drives the sweep
    let cfg = Arc::new(cfg);

    let backend = CountingBackend::counting(b"x".to_vec(), Some("v".into()), Arc::new(AtomicUsize::new(0)), None);
    let cache = Arc::new(Cache::new(Arc::clone(&cfg), Arc::clone(&clock), registry_with(Arc::new(backend))));

    // Five keys, each accessed later than the last, so recency is strict.
    for (i, k) in ["k1", "k2", "k3", "k4", "k5"].iter().enumerate() {
        clock.advance(1000 * (i as u64 + 1));
        let mut hit = cache.get_by_key(k, None).await.unwrap();
        let _ = collect(&mut hit.body).await;
        wait_entry(&cache, k).await;
    }

    let s = cache.state.read().await;
    assert!(s.entries.len() <= 3, "count budget must bound the map (got {})", s.entries.len());
    // The three most recent survive; the two oldest are gone.
    for k in ["k3", "k4", "k5"] {
        assert!(s.entries.contains_key(k), "{k} (recent) must survive eviction");
    }
    for k in ["k1", "k2"] {
        assert!(!s.entries.contains_key(k), "{k} (oldest) must be evicted");
    }
}
