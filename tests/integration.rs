use std::collections::HashMap;
use std::sync::{atomic::AtomicUsize, atomic::Ordering, Arc};

use tempfile::tempdir;

// The mock lives in the lib now: one implementation for the whole tree.
use origin_cache::testsupport::{
    collect, collect_allow_error, install_staged, install_staged_aged, install_staged_decayed,
    install_staged_merged, wait_entry, wait_until, LEDGER_INTERVAL_CEILING, WAIT_TRIES,
    BlockingOpenBackend, CacheTestExt, MockBackend as CountingBackend, SizedBackend, StormBackend,
    StormMode, VersionedBackend,
};

use origin_cache::{
    cache::cache::KeyState,
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
    wait_until("the flight map to empty", || async { cache.flights.active().await == 0 }).await;
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
        cache_profile: "efficient".into(),
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
    assert!(cache.snapshot().await.entries == 0);
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
    let remaining = cache.snapshot().await.entries;
    assert!(remaining < 3);
    assert!(!cache.entry_exists("a.png").await);
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
    wait_until("the spawned reaper to expire the entry", || async {
        cache.snapshot().await.entries == 0
    })
    .await;
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
    assert!(cache.snapshot().await.entries == 0, "a panicked flight installs nothing");

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
    assert!(cache.snapshot().await.entries == 0, "short read must not install an entry");
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
    let last = cache.inspect("hot.bin").await.last_access_millis.unwrap();
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
    wait_until("both transfers to hold stream permits", || async {
        opened.load(Ordering::SeqCst) >= 2
    })
    .await;
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
        assert!(
            cache.entry_exists("member.bin").await,
            "a stray must not evict the magazine on its way in"
        );
        assert!(
            cache.inspect("stray.bin").await.oversize,
            "the row records that it was admitted outside the budget"
        );
        assert_eq!(cache.snapshot().await.total_bytes, 4_100, "both objects are on disk");
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
/// the passthrough path and nothing is written.
#[tokio::test]
async fn a_window_bigger_than_the_retention_budget_is_not_written() {
    use origin_cache::cache::cache::{BodySource, ServeOutcome};

    let dir = tempdir().unwrap();
    let opens = Arc::new(AtomicUsize::new(0));
    let backend = Arc::new(SizedBackend::new(&[("huge.bin", u64::MAX / 4)], Arc::clone(&opens)));
    let mut slots = HashMap::new();
    slots.insert("primary".to_string(), Arc::new(BackendSlot::new(backend, 3)));
    let clock = Arc::new(MockClock::new(0));
    let mut cfg = Config { cache_dir: dir.path().to_path_buf(), ..Config::default() };
    // A window this large could never be KEPT (it is bigger than the magazine),
    // so the run is refused and the request streams through: that is the rule
    // that stops a whole-object request on a huge object from writing a huge
    // file, and it is asked about the WRITE, not about the object (ADR-0019).
    cfg.session_window_bytes = cfg.max_size_bytes.saturating_add(1 << 30);
    let cache = Arc::new(Cache::new(Arc::new(cfg), Arc::clone(&clock), BackendRegistry::new(slots)));

    let rk = cache.resolve("huge.bin").unwrap();
    let out = cache
        .serve(&rk, Some(ByteRange::bounded(0, 8)), None)
        .await
        .expect("a cache that cannot keep this window must still serve the range");
    let served = match out {
        ServeOutcome::Stream(served) => served,
        _ => panic!("a ranged miss must stream"),
    };
    assert_eq!(served.plan.source, BodySource::Upstream, "the bytes came straight from upstream");
    let mut body = served.plan.body;
    assert_eq!(
        collect(&mut body).await,
        (0..8u64).map(|i| (i % 251) as u8).collect::<Vec<u8>>()
    );
    assert_eq!(opens.load(Ordering::SeqCst), 1, "one upstream open, no fill");
    assert_eq!(cache.snapshot().await.segment_bytes, 0, "nothing was staged");
    assert!(cache.snapshot().await.entries == 0, "and nothing was installed");
}

/// The other half of the same decision: when the window IS affordable, a ranged
/// request on an object the disk can never hold stages exactly ONE WINDOW — the
/// sliding window its reader walks through — and never the object. Byte-exactness
/// and the read-ahead story for this shape live in the run tests.
#[tokio::test]
async fn an_unkeepable_object_stages_one_window_and_never_the_object() {
    use origin_cache::cache::cache::{BodySource, ServeOutcome};

    let dir = tempdir().unwrap();
    let opens = Arc::new(AtomicUsize::new(0));
    let backend = Arc::new(SizedBackend::new(&[("huge.bin", u64::MAX / 4)], Arc::clone(&opens)));
    let mut slots = HashMap::new();
    slots.insert("primary".to_string(), Arc::new(BackendSlot::new(backend, 3)));
    let clock = Arc::new(MockClock::new(0));
    let mut cfg = Config { cache_dir: dir.path().to_path_buf(), ..Config::default() };
    cfg.session_window_bytes = 1 << 20; // one mebibyte of window, and quick
    let cache = Arc::new(Cache::new(Arc::new(cfg), Arc::clone(&clock), BackendRegistry::new(slots)));

    let rk = cache.resolve("huge.bin").unwrap();
    let out = cache
        .serve(&rk, Some(ByteRange::bounded(0, 8)), None)
        .await
        .expect("a ranged read of an un-keepable object must still serve");
    let served = match out {
        ServeOutcome::Stream(served) => served,
        _ => panic!("a ranged miss must stream"),
    };
    assert_eq!(served.plan.source, BodySource::Stage, "the run's watermark served it");
    let mut body = served.plan.body;
    assert_eq!(
        collect(&mut body).await,
        (0..8u64).map(|i| (i % 251) as u8).collect::<Vec<u8>>()
    );
    assert_eq!(opens.load(Ordering::SeqCst), 1, "one run, one upstream open");
    wait_staged(&cache, 1 << 20).await;
    assert!(
        !dir.path().join("huge.bin").exists(),
        "the OBJECT is never written; only its window is"
    );
    assert!(cache.snapshot().await.entries == 0);
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
    wait_until("the passthrough to open upstream", || async {
        opened.load(Ordering::SeqCst) >= 1
    })
    .await;
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
    let served = match outcome {
        ServeOutcome::Stream(served) => served,
        _ => panic!("a ranged efficient miss must stream"),
    };
    assert_eq!(
        slot.stream_gate.available_permits(),
        1,
        "the permit must be held by the body, not released when the response was built"
    );

    let mut body = served.plan.body;
    assert_eq!(collect(&mut body).await, vec![7u8; 64]);
    // The permit belongs to the RUN, not to this response (ADR-0016): it is
    // held while the run's window transfers — which is what lets one open serve
    // every request inside that window — and released when the window ends,
    // possibly after this body is already consumed. So poll rather than sample.
    wait_until("the stream gate to be released", || async {
        slot.stream_gate.available_permits() == 2
    })
    .await;
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

    let entries = cache.snapshot().await.entries;
    assert!(
        entries <= 3,
        "entry-count cap must bound the map (got {entries} entries)"
    );
    // The three most recent survive, the two oldest are gone: the cap evicts
    // in LRU order, not merely "something".
    for k in ["k3", "k4", "k5"] {
        assert!(cache.entry_exists(k).await, "{k} (recent) must survive eviction");
    }
    for k in ["k1", "k2"] {
        assert!(!cache.entry_exists(k).await, "{k} (oldest) must be evicted");
    }
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
    // Staged now, then aged: the ledger stamps what a scan sees with the
    // clock's now, and the min-age guard needs the spans to be older.
    install_staged(&cache, &[("s1.bin", 0, 8192), ("s2.bin", 0, 8192)]).await;
    clock.advance(120_000);
    let paths: Vec<_> = ["s1.bin", "s2.bin"]
        .iter()
        .map(|key| origin_cache::cache::store::seg_path(&cfg.cache_dir, key, 0, 8192))
        .collect();

    cache.tick().await;

    let snap = cache.snapshot().await;
    assert!(
        snap.total_bytes.saturating_add(snap.segment_bytes) <= cfg.max_size_bytes,
        "staged bytes must be brought back under the shared budget (got {} + {})",
        snap.total_bytes,
        snap.segment_bytes
    );
    assert_eq!(snap.coverage_keys, 0, "evicted ledger rows must be dropped");
    assert!(paths.iter().all(|p| !p.exists()), "the evicted sidecar files must be gone");
}

// ---------------------------------------------------------------------------
// Watches (ADR-0018): the viewing SESSION, which outlives its bodies.
// ---------------------------------------------------------------------------

/// The horizon a lease cannot give. A viewing session lasts hours while its
/// bodies last milliseconds (EdgeOne asks for ascending 1 MiB shards, so the
/// origin sees one request per shard), and protection anchored to the last body
/// plus `read_grace_secs` therefore lapses MID-WATCH: the viewer's own window
/// becomes ordinary eviction material while the viewer is still there. A watch
/// says the session is still alive and pins a bounded neighbourhood of where
/// the viewer is, so the budget takes somebody else's bytes first.
#[tokio::test]
async fn a_watch_keeps_the_viewers_window_while_the_budget_takes_other_keys() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let mut cfg = Config::default();
    cfg.cache_dir = dir.path().to_path_buf();
    cfg.max_size_bytes = 3_072; // 4 KiB staged = 1 KiB over the cap
    cfg.inactive_ttl_secs = 3_600; // long enough that the sweep is not the cause
    cfg.read_grace_secs = 300;
    cfg.watch_idle_secs = 900;
    cfg.watch_pin_bytes = 4_096;
    let cfg = Arc::new(cfg);
    let backend =
        CountingBackend::counting(b"x".to_vec(), Some("v1".into()), Arc::new(AtomicUsize::new(0)), None);
    let cache = Arc::new(Cache::new(Arc::clone(&cfg), Arc::clone(&clock), registry_with(Arc::new(backend))));

    clock.advance(120_000);
    let watched = origin_cache::cache::store::seg_path(&cfg.cache_dir, "live.bin", 0, 2048);
    let other = origin_cache::cache::store::seg_path(&cfg.cache_dir, "other.bin", 0, 2048);
    // The watched key is the OLDER row, so the cross-key LRU visits it first:
    // without the pin it is the one the budget takes.
    install_staged_aged(
        &cache,
        &[("live.bin", 0, 2048, 10_000), ("other.bin", 0, 2048, 120_000)],
    )
    .await;

    // The viewer watched [0, 2048) and is between two requests of the same
    // session: no body holds the key, the watch does.
    let watch = cache.watches.acquire_at("live.bin", (0, 2048), Arc::clone(&clock));
    drop(watch);
    // Past the read grace (300 s), inside the watch budget (900 s).
    clock.advance(400_000);
    cache.tick().await;

    assert!(watched.exists(), "the viewer's window survives the pause");
    assert!(!other.exists(), "the budget took the other key instead");
    assert_eq!(cache.snapshot().await.segment_bytes, 2_048, "accounting moved with the file");

    // Past the watch budget the key is ordinary material again: the pin is a
    // deadline, not an exemption (ADR-0012's rule, third application).
    clock.advance(900_001);
    install_staged_aged(&cache, &[("other.bin", 0, 2048, clock.now_millis())]).await;
    cache.tick().await;
    assert!(!watched.exists(), "after its budget a watch protects nothing");
    assert_eq!(cache.snapshot().await.segment_bytes, 2_048);
}

// ---------------------------------------------------------------------------
// Read leases (ADR-0017): what a viewer is streaming is not evicted.
// ---------------------------------------------------------------------------

/// The cache's clocks measure REQUESTS, so a stream longer than the guards'
/// windows would otherwise lose its bytes while it is still being read: past
/// `STAGE_MIN_AGE_MS` the spans become budget-evictable, past the TTL the sweep
/// deletes them. A lease outranks both, and keeps outranking them for
/// `read_grace_secs` after the body ends, which is what stops a pause between
/// two requests of one viewing session from costing a re-fetch.
#[tokio::test]
async fn a_leased_key_survives_the_budget_until_its_grace_expires() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let mut cfg = Config::default();
    cfg.cache_dir = dir.path().to_path_buf();
    cfg.inactive_ttl_secs = 1_200;
    cfg.read_grace_secs = 300;
    cfg.max_size_bytes = 1_024; // one 2 KiB span is over budget on its own
    let cfg = Arc::new(cfg);
    let backend = CountingBackend::counting(b"x".to_vec(), Some("v1".into()), Arc::new(AtomicUsize::new(0)), None);
    let cache = Arc::new(Cache::new(Arc::clone(&cfg), Arc::clone(&clock), registry_with(Arc::new(backend))));

    clock.advance(120_000);
    install_staged(&cache, &[("live.bin", 0, 2048)]).await;
    let path = origin_cache::cache::store::seg_path(&cfg.cache_dir, "live.bin", 0, 2048);

    // A viewer is streaming this key right now.
    let lease = cache.leases.acquire_at("live.bin", Arc::clone(&clock));
    cache.tick().await;
    assert!(path.exists(), "a key being read must not be evicted for budget");
    assert_eq!(cache.snapshot().await.segment_bytes, 2048);

    // The body ends, but the grace still covers the pause before the next
    // request of the same session.
    drop(lease);
    cache.tick().await;
    assert!(path.exists(), "the grace covers the pause after a stream ends");

    // Past the grace the budget applies again.
    clock.advance(300_001);
    cache.tick().await;
    assert!(!path.exists(), "after the grace the key is ordinary budget material");
    assert_eq!(cache.snapshot().await.segment_bytes, 0);
}

/// The same rule against the inactivity sweep: idleness is measured from the
/// last request, so a download longer than the TTL used to be swept
/// mid-flight.
#[tokio::test]
async fn the_age_sweep_spares_a_key_being_read() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let mut cfg = Config::default();
    cfg.cache_dir = dir.path().to_path_buf();
    cfg.inactive_ttl_secs = 1_200;
    cfg.read_grace_secs = 300;
    let cfg = Arc::new(cfg);
    let backend = CountingBackend::counting(b"x".to_vec(), Some("v1".into()), Arc::new(AtomicUsize::new(0)), None);
    let cache = Arc::new(Cache::new(Arc::clone(&cfg), Arc::clone(&clock), registry_with(Arc::new(backend))));

    // Staged and never touched again: idle since the clock's epoch.
    install_staged_aged(&cache, &[("long.bin", 0, 4096, 0)]).await;
    let path = origin_cache::cache::store::seg_path(&cfg.cache_dir, "long.bin", 0, 4096);

    let lease = cache.leases.acquire_at("long.bin", Arc::clone(&clock));
    clock.advance(1_201_000); // far past the TTL, mid-download
    cache.tick().await;
    assert!(path.exists(), "a stream in flight outlives the inactivity TTL");
    assert_eq!(cache.snapshot().await.segment_bytes, 4096);

    // The sweep is TTL-paced, not per-tick: the next one arrives a full
    // `inactive_ttl` after the pass that spared the key. (The grace itself is
    // what protects against the BUDGET, which runs on every tick — pinned by
    // `a_leased_key_survives_the_budget_until_its_grace_expires`.)
    drop(lease);
    clock.advance(1_200_001);
    cache.tick().await;
    assert!(!path.exists(), "once the viewer is gone, the next sweep takes it");
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
    run_fixture_capped(dir, object_bytes, window, pace_ms, 64 * 1024 * 1024)
}

/// The same, with a magazine the object does NOT fit (ADR-0019): the object can
/// never be retained whole, which is the case a run has to work in too.
fn run_fixture_capped(
    dir: &std::path::Path,
    object_bytes: u64,
    window: u64,
    pace_ms: u64,
    max_size_bytes: u64,
) -> (Arc<Cache<MockClock>>, Arc<AtomicUsize>) {
    run_fixture_with(dir, object_bytes, window, pace_ms, max_size_bytes, false)
}

/// The same fixture with an upstream that ignores the END of the range it was
/// asked for: the 206 keeps streaming to the end of the object, the shape a
/// reused connection without an EOF at its Content-Length boundary produces.
fn run_fixture_overlong(
    dir: &std::path::Path,
    object_bytes: u64,
    window: u64,
    pace_ms: u64,
) -> (Arc<Cache<MockClock>>, Arc<AtomicUsize>) {
    run_fixture_with(dir, object_bytes, window, pace_ms, 64 * 1024 * 1024, true)
}

fn run_fixture_with(
    dir: &std::path::Path,
    object_bytes: u64,
    window: u64,
    pace_ms: u64,
    max_size_bytes: u64,
    overlong: bool,
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
    cfg.max_size_bytes = max_size_bytes;
    let cfg = Arc::new(cfg);
    let opens = Arc::new(AtomicUsize::new(0));
    let sized = SizedBackend::new(&[("a.bin", object_bytes)], Arc::clone(&opens))
        .paced(1024, std::time::Duration::from_millis(pace_ms));
    let backend = Arc::new(if overlong { sized.overlong() } else { sized });
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
async fn get_range(
    cache: &Arc<Cache<MockClock>>,
    offset: u64,
    len: u64,
) -> origin_cache::cache::cache::Served {
    use origin_cache::cache::cache::ServeOutcome;
    let rk = cache.resolve("a.bin").unwrap();
    match cache.serve(&rk, Some(ByteRange::bounded(offset, len)), None).await.unwrap() {
        // Returned WITH its protection, exactly as production receives it: a
        // test that drops the guards would be testing a shape nobody runs.
        ServeOutcome::Stream(served) => served,
        _ => panic!("expected a streamed passthrough from a ranged efficient miss"),
    }
}

/// Wait until the key has exactly `bytes` staged (the seal lands after the
/// body's last byte, so staged state is polled, never sampled).
async fn wait_staged(cache: &Arc<Cache<MockClock>>, bytes: u64) {
    wait_until(&format!("segment_bytes to reach {bytes}"), || async {
        cache.snapshot().await.segment_bytes == bytes
    })
    .await;
}

/// The PROFILE NAME no longer decides whether a ranged request gets a run
/// (ADR-0020), and since ADR-0022 there is only one fill profile left to name.
/// What decides is the request (ranged?), whether the key already has a durable
/// entry (then the ordinary path owns it, and only it revalidates), and the
/// object's size against the profile's `min_file_size`.
#[tokio::test]
async fn the_default_profile_gets_runs_for_ranged_reads() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let mut cfg = Config { cache_dir: dir.path().to_path_buf(), ..Config::default() };
    cfg.session_window_bytes = 1 << 20;
    let cfg = Arc::new(cfg);
    let opens = Arc::new(AtomicUsize::new(0));
    // 128 MiB, generated rather than stored, so the fixture costs nothing.
    let backend = Arc::new(SizedBackend::new(&[("big.bin", 128 << 20)], Arc::clone(&opens)));
    let mut slots = HashMap::new();
    slots.insert("primary".to_string(), Arc::new(BackendSlot::new(backend, 3)));
    let cache = Arc::new(Cache::new(cfg, clock, BackendRegistry::new(slots)));

    let rk = cache.resolve("big.bin").unwrap();
    let out = cache
        .serve(&rk, Some(ByteRange::bounded(0, 65536)), None)
        .await
        .unwrap();
    let served = match out {
        origin_cache::cache::cache::ServeOutcome::Stream(p) => p,
        _ => panic!("a ranged miss must stream"),
    };
    assert_eq!(
        served.plan.source,
        origin_cache::cache::cache::BodySource::Stage,
        "a ranged read of a large object stages its window whatever the profile is named"
    );
    let mut body = served.plan.body;
    let _ = collect(&mut body).await;
    assert_eq!(opens.load(Ordering::SeqCst), 1, "one run, one upstream open");
    wait_until("one window to be staged", || async {
        cache.snapshot().await.segment_bytes == 1 << 20
    })
    .await;
    assert_eq!(cache.snapshot().await.segment_bytes, 1 << 20, "one window staged");
}

/// A read that JUMPS to a cold offset stages the window decision's floor, not
/// a whole window, and the next read inside it stages nothing new.
///
/// Measured on the node before this rule (2026-09-22, a real 200 GiB object):
/// one 5 MiB cold jump took `segment_bytes` up by exactly 67,108,864 — the
/// configured window — a 12.8x amplification, while the next 5 MiB inside that
/// window cost 0.04 s. The floor is the read-ahead a jump actually uses; a
/// sequential walk still climbs to one open per window (`window::window_for`
/// doubles a window its readers read out).
#[tokio::test]
async fn a_jump_stages_the_floor_and_a_second_read_inside_it_stages_nothing() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let mut cfg = Config { cache_dir: dir.path().to_path_buf(), ..Config::default() };
    cfg.session_window_bytes = 1 << 20;
    cfg.window_floor_bytes = 1 << 18;
    let cfg = Arc::new(cfg);
    let opens = Arc::new(AtomicUsize::new(0));
    let backend = Arc::new(SizedBackend::new(&[("big.bin", 64 << 20)], Arc::clone(&opens)));
    let mut slots = HashMap::new();
    slots.insert("primary".to_string(), Arc::new(BackendSlot::new(backend, 3)));
    let cache = Arc::new(Cache::new(cfg, clock, BackendRegistry::new(slots)));
    let rk = cache.resolve("big.bin").unwrap();

    // A cold jump: 1 MiB in, and read the floor out (256 KiB) so the next
    // request at its frontier is a continuation, not a jump.
    let out = cache
        .serve(&rk, Some(ByteRange::bounded(1 << 20, 1 << 18)), None)
        .await
        .unwrap();
    let mut served = match out {
        origin_cache::cache::cache::ServeOutcome::Stream(p) => p,
        _ => panic!("a ranged miss must stream"),
    };
    // The body is borrowed, not moved out: `Served`'s guards (lease, watch)
    // drop with it, and a partially moved value cannot be dropped whole.
    let _ = collect(&mut served.plan.body).await;
    wait_until("the floor to be staged", || async {
        cache.snapshot().await.segment_bytes >= 1 << 18
    })
    .await;
    assert_eq!(
        cache.snapshot().await.segment_bytes,
        1 << 18,
        "a jump stages the floor (256 KiB), not the 1 MiB window"
    );

    // The next read at that frontier continues the run it replaces, which was
    // read out: the window doubles. This is the request-driven half of the
    // ramp (the tick-driven half is pinned in `cache::session`).
    let out = cache
        .serve(&rk, Some(ByteRange::bounded((1 << 20) + (1 << 18), 65536)), None)
        .await
        .unwrap();
    let mut served = match out {
        origin_cache::cache::cache::ServeOutcome::Stream(p) => p,
        _ => panic!("a staged read must stream"),
    };
    let _ = collect(&mut served.plan.body).await;
    wait_until("three windows to be staged", || async {
        cache.snapshot().await.segment_bytes >= (1 << 18) * 3
    })
    .await;
    assert_eq!(
        cache.snapshot().await.segment_bytes,
        (1 << 18) * 3,
        "a read-out window ramps the next one to twice the floor (256 KiB + 512 KiB)"
    );
    assert_eq!(opens.load(Ordering::SeqCst), 2, "two runs, two upstream opens");
}

/// An object the magazine can NEVER hold whole still gets a run (ADR-0019).
///
/// Before this, `magazine.fits` refused every request on such a key, so each
/// one opened its own upstream Range — for the product's object (a video, a
/// tarball, an image, any object larger than the magazine) that means one
/// ~1.2 s provider open per request instead of one per window.
#[tokio::test]
async fn a_run_starts_for_an_object_larger_than_the_magazine() {
    let dir = tempdir().unwrap();
    // 4 MiB object, 1 MiB magazine (so it can never be held), 256 KiB window.
    let (cache, opens) = run_fixture_capped(dir.path(), 4 << 20, 256 << 10, 0, 1 << 20);

    // Three seeks inside the first window: one run, one open. The bodies are
    // drained, not just planned: a plan only says where the bytes WILL come
    // from, and the run's own open happens in its driver task.
    for offset in [0u64, 65_536, 131_072] {
        let served = get_range(&cache, offset, 65_536).await;
        assert_eq!(
            served.plan.source,
            origin_cache::cache::cache::BodySource::Stage,
            "a window of an object too large to keep is still staged and served"
        );
        let mut body = served.plan.body;
        assert_eq!(collect(&mut body).await, synthetic(offset, 65_536), "seek at {offset}");
    }
    assert_eq!(
        opens.load(Ordering::SeqCst),
        1,
        "three seeks inside one window of an un-keepable object must share one upstream open"
    );
}

/// A run is a WRITE, and the write is what has to be affordable: a window
/// larger than the retention budget can never be kept (it would be evicted the
/// moment it sealed), so the request streams through instead. This is the
/// question that replaced the old object-size test, and it is what keeps a
/// whole-object request on a huge object from writing a huge file.
#[tokio::test]
async fn a_range_larger_than_the_magazine_streams_through_without_a_run() {
    let dir = tempdir().unwrap();
    let (cache, opens) = run_fixture_capped(dir.path(), 4 << 20, 256 << 10, 0, 1 << 20);
    // The whole object in one request: run_len = 4 MiB > 1 MiB magazine.
    let served = get_range(&cache, 0, 4 << 20).await;
    assert_eq!(
        served.plan.source,
        origin_cache::cache::cache::BodySource::Upstream,
        "a window bigger than the retention budget is not written"
    );
    assert_eq!(opens.load(Ordering::SeqCst), 1, "one open, straight through");
    assert_eq!(
        cache.snapshot().await.segment_bytes,
        0,
        "and nothing was staged for it"
    );
}

/// The escape must not re-fetch what it already holds. When a run is refused
/// (the window is bigger than the retention budget, or the disk cannot afford
/// it) the request still streams through — but the staged prefix rides along
/// and only the gap is opened upstream. Before this the escape handed the
/// provider the ORIGINAL range, so every byte of the prefix was fetched twice.
#[tokio::test]
async fn an_admission_escape_does_not_refetch_the_staged_prefix() {
    let dir = tempdir().unwrap();
    let payload: Vec<u8> = (0..(4u64 << 20)).map(|i| (i % 251) as u8).collect();
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
    cfg.session_window_bytes = 256 << 10;
    cfg.max_size_bytes = 1 << 20;
    cfg.read_grace_secs = 0;
    cfg.watch_idle_secs = 0;
    cfg.watch_pin_bytes = 0;
    let cfg = Arc::new(cfg);
    let open_calls = Arc::new(AtomicUsize::new(0));
    let opens_log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let calls = Arc::new(AtomicUsize::new(0));
    let backend = Arc::new(
        CountingBackend::counting(payload.clone(), Some("v1".into()), Arc::clone(&calls), None)
            .counters(
                Arc::clone(&open_calls),
                Arc::clone(&open_calls),
                Arc::clone(&open_calls),
                Arc::clone(&opens_log),
            ),
    );
    let mut slots = HashMap::new();
    slots.insert("primary".to_string(), Arc::new(BackendSlot::new(backend, 3)));
    let cache = Arc::new(Cache::new(cfg, clock, BackendRegistry::new(slots)));

    // Stage the first window with an ordinary small read.
    get_range(&cache, 0, 65_536).await;
    wait_staged(&cache, 262_144).await;

    // Ask for the whole object: the run would write 4 MiB against a 1 MiB
    // budget, so it streams through — with the prefix served locally.
    let served = get_range(&cache, 0, 4 << 20).await;
    let opens = opens_log.lock().unwrap().clone();
    let last = *opens.last().expect("an upstream open happened");
    assert_eq!(
        last.0, 262_144,
        "the open starts at the staged frontier, not at the requested start: {opens:?}"
    );
    assert_eq!(served.plan.content_length, Some(4 << 20), "the response still promises the whole range");
    let mut body = served.plan.body;
    let bytes = collect(&mut body).await;
    assert_eq!(bytes.len() as u64, 4 << 20, "and delivers it");
    assert_eq!(&bytes[..262_144], &payload[..262_144], "byte-exact across the prefix boundary");
}

/// A key being walked trims itself to a bounded working window even while the
/// magazine is globally under budget: the object can never be kept, so what a
/// reader may hold is the neighbourhood it is reading plus one window — not
/// everything it has written since the session began.
#[tokio::test]
async fn an_unkeepable_key_trims_itself_to_its_working_window() {
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
    cfg.session_window_bytes = 256 << 10;
    cfg.max_size_bytes = 1 << 20; // the 4 MiB object cannot fit
    cfg.read_grace_secs = 0;
    cfg.watch_idle_secs = 0;
    cfg.watch_pin_bytes = 65_536; // cap = pin + window = 320 KiB
    let cfg = Arc::new(cfg);
    let opens = Arc::new(AtomicUsize::new(0));
    let backend = Arc::new(SizedBackend::new(&[("a.bin", 4 << 20)], Arc::clone(&opens)));
    let mut slots = HashMap::new();
    slots.insert("primary".to_string(), Arc::new(BackendSlot::new(backend, 3)));
    let cache = Arc::new(Cache::new(cfg, clock.clone(), BackendRegistry::new(slots)));

    // Walk all four windows of the magazine-sized first MiB.
    for offset in [0u64, 262_144, 524_288, 786_432] {
        get_range(&cache, offset, 65_536).await;
        let want = offset / 262_144 + 1;
        wait_staged(&cache, want * 262_144).await;
    }
    // Exactly at the global limit, not over it: 1 MiB staged against a 1 MiB
    // cap with no durable entries is `resident + segment_bytes - max_size = 0`.
    // The trim asserted below is therefore the PER-KEY rule, not the global one.
    assert_eq!(cache.snapshot().await.segment_bytes, 1 << 20, "four windows staged");

    // The spans are older than the minimum age, so they are candidates.
    clock.advance(120_000);
    cache.tick().await;

    let staged = cache.snapshot().await.segment_bytes;
    assert!(
        staged <= 320 << 10,
        "the key holds its working window (pin + one window = 320 KiB), not the whole walk: {staged}"
    );
    assert!(staged > 0, "and it holds something: the newest window is the read-ahead");
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
        let served = get_range(&cache, offset, 1024).await;
        assert_eq!(
            served.plan.source,
            origin_cache::cache::cache::BodySource::Stage,
            "a run-served response reads staged bytes, not a fresh upstream stream"
        );
        let mut body = served.plan.body;
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
        let served = get_range(&cache, offset, 1024).await;
        let mut body = served.plan.body;
        assert_eq!(collect(&mut body).await, synthetic(offset, 1024));
    }
    assert_eq!(
        opens.load(Ordering::SeqCst),
        2,
        "a far seek takes its own Range rather than riding the window"
    );
}

/// An upstream 206 that does NOT stop at the length it was asked for must not
/// leak the rest of the object into the response, and must not let the sealed
/// span claim bytes nobody asked for: the standalone escape reads at most its
/// own remainder. The other shaped paths need no such cap because nothing is
/// written after their read; here a read that trusted EOF would stage the rest
/// of the object and hand the viewer a body longer than the promise.
#[tokio::test]
async fn an_overlong_upstream_range_is_cut_at_the_remainder_it_asked_for() {
    let dir = tempdir().unwrap();
    let (cache, opens) = run_fixture_overlong(dir.path(), 1 << 20, 8192, 10);

    // The run at 0 stays live (paced), so the far seek below is outside every
    // run and no new one can start: that is the escape this test is about.
    let served = get_range(&cache, 0, 1024).await;
    let mut body = served.plan.body;
    let first = collect(&mut body).await;

    let served = get_range(&cache, 65536, 1024).await;
    assert_eq!(served.plan.content_length, Some(1024));
    let mut body = served.plan.body;
    let escaped = collect(&mut body).await;

    assert_eq!(first.len(), 1024, "the window's reader got its bytes");
    assert_eq!(
        escaped,
        synthetic(65536, 1024),
        "the escape stops at the length the request asked for, not at EOF"
    );
    assert_eq!(
        opens.load(Ordering::SeqCst),
        2,
        "one open for the window and one for the escape"
    );
    // The window is a window and the escape is its remainder: an upstream that
    // streams past both would stage the rest of the object instead.
    wait_staged(&cache, 8192 + 1024).await;
    assert_eq!(cache.snapshot().await.segment_bytes, 8192 + 1024);
}

/// Once a run has sealed, its window is a normal staged span: a later read
/// inside it is served from disk with no upstream open and no run.
#[tokio::test]
async fn a_sealed_run_leaves_spans_that_serve_later_reads() {
    let dir = tempdir().unwrap();
    let (cache, opens) = run_fixture(dir.path(), 1 << 20, 8192, 10);

    let served = get_range(&cache, 0, 1024).await;
    let mut body = served.plan.body;
    assert_eq!(collect(&mut body).await, synthetic(0, 1024));
    wait_staged(&cache, 8192).await;

    // The next read sits inside the sealed window.
    let served = get_range(&cache, 1024, 1024).await;
    assert_eq!(served.plan.source, origin_cache::cache::cache::BodySource::Stage);
    let mut body = served.plan.body;
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
        let served = get_range(&cache, offset, 1024).await;
        let mut body = served.plan.body;
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

    let served = get_range(&cache, 0, 1024).await;
    let (bytes, errored) = collect_allow_error(served.plan.body).await;
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
    let files: Vec<(u64, u64)> = (0..4u64).map(|i| (i * 1024, (i + 1) * 1024)).collect();
    let paths: Vec<std::path::PathBuf> = files
        .iter()
        .map(|(s, e)| origin_cache::cache::store::seg_path(&cfg.cache_dir, "m.bin", *s, *e))
        .collect();
    // One interval for the whole walk: touching intervals stay separate in the
    // ledger by design (each keeps its own window), so this bridged history is
    // what the walk's compaction leaves behind — and adoption records it because
    // the union of the files covers it. The BYTES still come off the disk.
    install_staged_merged(&cache, "m.bin", &files, (0, 4096), 4096, touched).await;
    assert_eq!(
        cache.inspect("m.bin").await.ledger_spans.len(),
        1,
        "the premise is a merged ledger"
    );

    cache.tick().await;

    assert_eq!(cache.snapshot().await.segment_bytes, 3072, "one span's worth must be freed");
    assert!(paths[0].exists() == false, "the stalest span (lowest offset) goes first");
    assert!(paths[1].exists() && paths[2].exists() && paths[3].exists(), "the rest stay");
    let spans = cache.inspect("m.bin").await.ledger_spans;
    assert_eq!(
        spans.iter().map(|s| (s.start, s.end, s.last_read_millis, s.reads)).collect::<Vec<_>>(),
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
    let files: Vec<(u64, u64)> = (0..2u64).map(|i| (i * 1024, (i + 1) * 1024)).collect();
    let paths: Vec<std::path::PathBuf> = files
        .iter()
        .map(|(s, e)| origin_cache::cache::store::seg_path(&cfg.cache_dir, "d.bin", *s, *e))
        .collect();
    // No intervals at all: every record of these bytes decayed away while the
    // files themselves are still here. A state only the decay path produces, so
    // it is built by decaying — which removes ledger records, never bytes.
    install_staged_decayed(&cache, "d.bin", &files, 2048, touched).await;

    cache.tick().await;

    assert_eq!(cache.snapshot().await.segment_bytes, 1024);
    assert!(!paths[0].exists() && paths[1].exists(), "one file goes, one stays");
    let iv = cache.inspect("d.bin").await.ledger_spans;
    assert_eq!(iv.len(), 1, "the survivor is recorded again");
    assert_eq!((iv[0].start, iv[0].end), (1024, 2048));
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
    let spans = LEDGER_INTERVAL_CEILING + 1;
    // Staged at `touched`, which is older than the min-age guard (`now` is
    // 120_000, the guard is 60_000): the trim refuses to evict a span that was
    // JUST sealed, and the row's object size puts the key in the un-keepable
    // class (ADR-0019), which is the premise this test has always had.
    let plan: Vec<(&str, u64, u64, u64)> =
        (0..spans as u64).map(|i| ("walk.bin", i, i + 1, touched)).collect();
    install_staged_aged(&cache, &plan).await;
    // The ceiling's own compaction, fired by adoption: touching spans merge
    // losslessly, so the ledger collapses a walk this long and no interval's
    // bounds are a file's bounds any more.
    let intervals = cache.inspect("walk.bin").await.ledger_spans.len();
    assert!(
        intervals < spans,
        "the ceiling must have merged the walk (got {intervals} intervals)"
    );

    cache.tick().await;

    let key = cache.inspect("walk.bin").await;
    assert_eq!(cache.snapshot().await.segment_bytes, 1000, "the budget is respected");
    assert_eq!(key.staged_spans.len(), 1000, "exactly one file per byte of the budget remains");
    let covered: u64 = key.ledger_spans.iter().map(|s| s.end - s.start).sum();
    assert_eq!(covered, 1000, "the rebuilt ledger covers exactly the surviving files");
}

/// Sealing IS adoption. The two ways a span enters the record — the live path
/// (a rename landed, so the ledger measures the file it named) and the installer
/// (a test states "this node holds these bytes at this moment") — leave the same
/// record: same bytes, same read time, same count. They differ only in how they
/// learn that the disk holds the bytes, which is why one rule can serve both.
#[tokio::test]
async fn a_sealed_span_and_an_adopted_span_agree() {
    let stamp = 50_000u64;
    let build = |dir: &std::path::Path| {
        let mut cfg = Config { cache_dir: dir.to_path_buf(), ..Config::default() };
        cfg.session_window_bytes = 1 << 20;
        cfg.window_floor_bytes = 1 << 18;
        let cfg = Arc::new(cfg);
        let backend =
            Arc::new(SizedBackend::new(&[("big.bin", 64 << 20)], Arc::new(AtomicUsize::new(0))));
        let mut slots = HashMap::new();
        slots.insert("primary".to_string(), Arc::new(BackendSlot::new(backend, 3)));
        Arc::new(Cache::new(cfg, Arc::new(MockClock::new(stamp)), BackendRegistry::new(slots)))
    };

    // The live path: one cold ranged read, exactly the floor, sealed as it lands.
    let dir = tempdir().unwrap();
    let cache = build(dir.path());
    let rk = cache.resolve("big.bin").unwrap();
    let out = cache.serve(&rk, Some(ByteRange::bounded(0, 1 << 18)), None).await.unwrap();
    let mut served = match out {
        origin_cache::cache::cache::ServeOutcome::Stream(p) => p,
        _ => panic!("a ranged miss must stream"),
    };
    let _ = collect(&mut served.plan.body).await;
    // Wait for the LEDGER row, not the disk view. The seal renames the file
    // first and records the row after, so sampling the ledger the moment the
    // staged view appears races the seal (pitfall 50). Measured: waiting on
    // `staged_spans` then reading `ledger_spans` failed two full-suite runs out
    // of two under load, while the same test passed 3/3 in isolation. Wait for
    // the thing this test actually asserts.
    wait_until("the ledger row to appear", || async {
        !cache.inspect("big.bin").await.ledger_spans.is_empty()
    })
    .await;
    let view = cache.inspect("big.bin").await;
    let sealed = view.ledger_spans;
    // Say what was there when it was not: a bare "staged nothing" on a box under
    // load is not actionable (this test failed twice in two full-suite runs
    // before it waited on the ledger, and it is still rare after that).
    assert!(
        !sealed.is_empty(),
        "the read staged something to compare: ledger={} staged={}",
        sealed.len(),
        view.staged_spans.len()
    );

    // The installer path, over the same bytes at the same moment.
    let dir2 = tempdir().unwrap();
    let cache2 = build(dir2.path());
    let plan: Vec<(&str, u64, u64, u64)> =
        sealed.iter().map(|s| ("big.bin", s.start, s.end, stamp)).collect();
    install_staged_aged(&cache2, &plan).await;
    let adopted = cache2.inspect("big.bin").await.ledger_spans;

    let shape = |v: &[origin_cache::cache::cache::SpanReads]| {
        v.iter().map(|s| (s.start, s.end, s.last_read_millis, s.reads)).collect::<Vec<_>>()
    };
    assert_eq!(
        shape(&sealed),
        shape(&adopted),
        "a sealed span and an adopted span are the same record"
    );
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
        promised_len: Some(1024),
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
        // Through the read-only view (ADR-0022), not the machinery's own state:
        // callers and tests cross the same seam.
        let state = cache2.inspect("kept.bin").await;
        assert!(state.installed, "the row must be rebuilt from the object tree");
        assert_eq!(state.entry_bytes, Some(7));
    }
}


// ---------------------------------------------------------------------------
// The key-state view (ADR-0021): one read-only seam that production's operator
// surface and the tests share, replacing direct projections of the machinery's
// fields.
// ---------------------------------------------------------------------------

/// A key nobody touched is empty; a ranged read stages a window, takes a lease
/// while the body lives and a watch that outlives it; a whole-file pull
/// installs the row the ranged path never does. The view keeps the DISK's
/// answer (what exists) apart from the LEDGER's (what the policy believes).
#[tokio::test]
async fn inspect_reports_one_key_from_disk_and_ledger() {
    let dir = tempdir().unwrap();
    let (cache, _opens) = run_fixture(dir.path(), 1 << 20, 8192, 0);

    let untouched = cache.inspect("a.bin").await;
    assert!(!untouched.installed);
    assert!(untouched.staged_spans.is_empty());
    assert_eq!(untouched.staged_bytes, 0);
    assert!(untouched.ledger_spans.is_empty());
    assert_eq!(untouched.ledger_total, None);
    assert_eq!(untouched.pin, None);
    assert!(!untouched.leased);

    let served = get_range(&cache, 0, 1024).await;
    let mut body = served.plan.body;
    assert_eq!(collect(&mut body).await, synthetic(0, 1024));
    wait_staged(&cache, 8192).await;

    let staged = cache.inspect("a.bin").await;
    assert!(!staged.installed, "a run stages a window; it installs no entry");
    assert_eq!(staged.staged_spans, vec![(0, 8192)], "the disk's answer");
    assert_eq!(staged.staged_bytes, 8192);
    assert_eq!(staged.ledger_spans.len(), 1, "the ledger's map of the same bytes");
    assert_eq!((staged.ledger_spans[0].start, staged.ledger_spans[0].end), (0, 8192));
    assert_eq!(staged.ledger_total, Some(1 << 20), "the object's size, as staged");
    assert!(staged.ledger_last_touch_millis.is_some());
    // The response's own guard is still in this scope, so the key reports as
    // leased; the watch is what outlives the body (ADR-0017 vs ADR-0018), and
    // the healthz test pins the other side of it — once the plane drops the
    // body, `leased` goes false while the pin stays.
    assert!(staged.leased, "the guard this scope holds is visible");
    assert!(staged.pin.is_some(), "the viewer's neighbourhood is still pinned");

    // A whole-file pull installs the row the ranged path never does.
    let mut hit = cache.get_by_key("a.bin", None).await.unwrap();
    assert_eq!(collect(&mut hit.body).await.len(), 1 << 20);
    wait_entry(&cache, "a.bin").await;
    let installed = cache.inspect("a.bin").await;
    assert!(installed.installed);
    assert!(!installed.tombstone);
    assert_eq!(installed.entry_bytes, Some(1 << 20));
    assert!(!installed.oversize);
}

/// A negative row is a row: `installed` says something is known about the key,
/// `tombstone` says what — and the tombstone expires with its window.
#[tokio::test]
async fn inspect_reports_a_negative_row_as_a_tombstone() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let mut cfg = Config {
        cache_dir: dir.path().to_path_buf(),
        ..Config::default()
    };
    cfg.negative_ttl_secs = 60;
    let cfg = Arc::new(cfg);
    let calls = Arc::new(AtomicUsize::new(0));
    let backend =
        CountingBackend::counting(b"x".to_vec(), None, Arc::clone(&calls), Some(BackendError::NotFound));
    let mut slots = HashMap::new();
    slots.insert("primary".to_string(), Arc::new(BackendSlot::new(Arc::new(backend), 3)));
    let cache = Arc::new(Cache::new(cfg, Arc::clone(&clock), BackendRegistry::new(slots)));

    assert!(cache.get_by_key("gone.bin", None).await.is_err(), "404 from upstream");
    wait_entry(&cache, "gone.bin").await;
    let k = cache.inspect("gone.bin").await;
    assert!(k.installed, "a tombstone is a row");
    assert!(k.tombstone, "and it says so");
    assert!(k.staged_spans.is_empty(), "nothing was written for it");

    // Past the negative window the row is no longer a tombstone.
    clock.advance(61_000);
    let k = cache.inspect("gone.bin").await;
    assert!(k.installed);
    assert!(!k.tombstone);
}

// ---------------------------------------------------------------------------
// Protection vs the budget: why a per-key pin needs no global cap.
// ---------------------------------------------------------------------------

/// A pin is spent only after the bytes OUTSIDE it. Three spans per key against a
/// magazine that holds half of them: the reaper has enough outside the pins to
/// meet the budget, so every pinned span survives — the ordering a global pin
/// ceiling would otherwise have to enforce by arithmetic.
#[tokio::test]
async fn a_pin_is_spent_only_after_the_bytes_outside_it() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let mut cfg = Config {
        cache_dir: dir.path().to_path_buf(),
        ..Config::default()
    };
    // 6 KiB staged, a 4 KiB magazine: exactly the two spans that lie entirely
    // OUTSIDE the pins have to go, which is the ordering under test.
    cfg.max_size_bytes = 4_096;
    cfg.watch_pin_bytes = 1_024; // a neighbourhood around the viewer, both sides
    cfg.watch_idle_secs = 3_600; // the watches stay live across the tick
    cfg.read_grace_secs = 0; // leases are not part of this question
    let cfg = Arc::new(cfg);
    let backend =
        CountingBackend::counting(b"x".to_vec(), Some("v1".into()), Arc::new(AtomicUsize::new(0)), None);
    let cache = Arc::new(Cache::new(Arc::clone(&cfg), Arc::clone(&clock), registry_with(Arc::new(backend))));

    let mut spans = Vec::new();
    for key in ["a.bin", "b.bin"] {
        for i in 0..3u64 {
            spans.push((key, i * 1024, (i + 1) * 1024));
        }
    }
    install_staged(&cache, &spans).await;
    assert_eq!(cache.snapshot().await.segment_bytes, 6_144, "the premise is an overrun");

    for key in ["a.bin", "b.bin"] {
        // The viewer sits in the SECOND span, so the pin has bytes on both sides.
        let watch = cache.watches.acquire_at(key, (1024, 2048), Arc::clone(&clock));
        drop(watch);
    }
    clock.advance(120_000); // past the min-age guard, inside the watch budget
    cache.tick().await;

    assert_eq!(
        cache.snapshot().await.segment_bytes,
        4_096,
        "the magazine must come back inside its budget"
    );
    for key in ["a.bin", "b.bin"] {
        let k = cache.inspect(key).await;
        let (ps, pe, anchor) = k.pin.expect("a live watch has a pin");
        // Exactly the span the viewer has already passed went; the two spans the
        // pin touches stayed — a pin is spent only once nothing outside it can
        // cover the need.
        assert_eq!(
            k.staged_spans,
            vec![(1024, 2048), (2048, 3072)],
            "{key}: only the span outside the pin [{ps}, {pe}) may go"
        );
        assert!(
            k.staged_spans.iter().any(|(s, e)| *s <= anchor && anchor < *e),
            "{key}: the byte under the viewer survives"
        );
    }
}

/// EVERY byte pinned and the budget overrun: the magazine still comes back
/// inside it. This is the property that makes a per-key pin safe with no global
/// ceiling — N watched keys cannot RESERVE N × `watch_pin_bytes`, they only order
/// the eviction, and when nothing outside a pin is left the reaper spends the
/// pins themselves (ADR-0012: a deadline, not an exemption). The two passes in
/// `evict_staged` are the mechanism; a rule that skipped everything protected
/// would free nothing here and leave the budget overrun.
#[tokio::test]
async fn protection_orders_eviction_and_never_exempts_it() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(MockClock::new(0));
    let mut cfg = Config {
        cache_dir: dir.path().to_path_buf(),
        ..Config::default()
    };
    cfg.max_size_bytes = 2_048; // half of the 4 KiB staged below
    cfg.watch_pin_bytes = 1 << 20; // the pin covers whole rows: nothing is outside
    cfg.watch_idle_secs = 3_600;
    cfg.read_grace_secs = 0;
    let cfg = Arc::new(cfg);
    let backend =
        CountingBackend::counting(b"x".to_vec(), Some("v1".into()), Arc::new(AtomicUsize::new(0)), None);
    let cache = Arc::new(Cache::new(Arc::clone(&cfg), Arc::clone(&clock), registry_with(Arc::new(backend))));

    let spans = [
        ("a.bin", 0u64, 1024u64),
        ("a.bin", 1024, 2048),
        ("b.bin", 0, 1024),
        ("b.bin", 1024, 2048),
    ];
    install_staged(&cache, &spans).await;
    assert_eq!(cache.snapshot().await.segment_bytes, 4_096, "the premise is an overrun");

    for key in ["a.bin", "b.bin"] {
        let watch = cache.watches.acquire_at(key, (0, 1024), Arc::clone(&clock));
        drop(watch);
    }
    clock.advance(120_000);
    cache.tick().await;

    assert_eq!(
        cache.snapshot().await.segment_bytes,
        2_048,
        "protection must not turn into an exemption: the budget is the budget"
    );
    let (a, b) = (cache.inspect("a.bin").await, cache.inspect("b.bin").await);
    assert_eq!(
        a.staged_bytes + b.staged_bytes,
        2_048,
        "the accounting moved with the files"
    );
    // Which of the two rows paid is the age order's business; that a watched row
    // paid WHILE EVERYTHING WAS PINNED is the property. It is also what the
    // second pass exists for: with `inside_pins` reduced to one pass, nothing
    // here would be freed and the assertion above would read 4096.
}

// ---------------------------------------------------------------------------
// The invariant everything else rests on: the account, the ledger and the DISK
// describe the same bytes.
// ---------------------------------------------------------------------------

/// A tiny deterministic PRNG. No dependency, and a fixed seed means a failure
/// is reproducible: the same walk, every time.
struct Xorshift(u64);

impl Xorshift {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

/// Wait until the staged-byte account stops moving. Sealing is asynchronous (the
/// run's driver renames the span after the body's last byte), so a sample taken
/// the instant a response returns would read a half-settled state — the same
/// discipline the wait helpers document, in miniature.
async fn wait_settled(cache: &Arc<Cache<MockClock>>) {
    let mut last = cache.snapshot().await.segment_bytes;
    let mut stable = 0;
    for _ in 0..WAIT_TRIES {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let now = cache.snapshot().await.segment_bytes;
        if now == last {
            stable += 1;
            if stable >= 3 {
                return;
            }
        } else {
            stable = 0;
            last = now;
        }
    }
    panic!("the staged-byte account never settled");
}

/// Is every ledger interval covered by spans that exist on disk? The ledger is
/// allowed to FORGET bytes (window decay drops the interval while the file
/// stays), but it must never claim bytes that are not there.
fn ledger_is_backed_by_disk(k: &KeyState) -> bool {
    for iv in &k.ledger_spans {
        let mut at = iv.start;
        for (s, e) in &k.staged_spans {
            if *s <= at && at < *e {
                at = *e;
            }
        }
        if at < iv.end {
            return false;
        }
    }
    true
}

/// A seeded random walk over the real request path — reads, evictions and a
/// restart — with the three invariants checked as it goes:
///
/// 1. `segment_bytes` equals the sum of what `inspect` finds ON DISK;
/// 2. every ledger interval is backed by disk spans;
/// 3. after a tick, the magazine is inside its budget.
///
/// The failure this is written against is the one no targeted test covers: the
/// account and the disk drifting apart over an arbitrary interleaving of reads,
/// seals and evictions. The seed makes a failure reproducible; the checks name
/// which invariant broke.
#[tokio::test]
async fn the_account_the_ledger_and_the_disk_never_disagree() {
    let dir = tempdir().unwrap();
    // A 1 MiB object with an 8 KiB window and a magazine that holds three
    // windows: the walk stages constantly and evicts as it goes.
    let (cache, _opens) = run_fixture_capped(dir.path(), 1 << 20, 8192, 0, 24 * 1024);

    let mut rng = Xorshift(0xC0FFEE_1234_5678);
    for round in 0..120u64 {
        // One 1 KiB read at a random window boundary.
        let off = (rng.next() % 1024) * 1024;
        let mut served = get_range(&cache, off, 1024).await;
        // Borrowed, not moved: the lease and the watch live in `served` (their
        // fields are private on purpose — `Cache::protect` is the only way to
        // get them), and holding either one keeps this key out of the reaper's
        // reach. Dropping the whole response here is what lets the walk evict.
        assert_eq!(
            collect(&mut served.plan.body).await,
            synthetic(off, 1024),
            "round {round}: bytes served"
        );
        drop(served);

        wait_settled(&cache).await;
        let snap = cache.snapshot().await;
        let k: KeyState = cache.inspect("a.bin").await;
        assert_eq!(
            snap.segment_bytes, k.staged_bytes,
            "round {round}: the account must equal what is on disk"
        );
        assert!(
            ledger_is_backed_by_disk(&k),
            "round {round}: the ledger claims bytes the disk does not have: ledger={:?} disk={:?}",
            k.ledger_spans,
            k.staged_spans
        );

        // Every few rounds, run the reaper — the budget must come back inside.
        if round % 20 == 19 {
            cache.tick().await;
            wait_settled(&cache).await;
            let snap = cache.snapshot().await;
            let budget = cache.config.max_size_bytes;
            assert!(
                snap.total_bytes + snap.segment_bytes <= budget,
                "round {round}: after a tick the magazine is over budget ({} + {} > {budget})",
                snap.total_bytes,
                snap.segment_bytes
            );
            let k = cache.inspect("a.bin").await;
            assert_eq!(
                snap.segment_bytes, k.staged_bytes,
                "round {round}: the account drifted across an eviction"
            );
        }
    }

    // And the restart: a second Cache over the same directory rebuilds the
    // account from the FILES (the disk is the authority), so the invariants must
    // hold in the new process's books too.
    let before = {
        let k = cache.inspect("a.bin").await;
        (cache.snapshot().await.segment_bytes, k.staged_spans)
    };
    let clock2 = Arc::new(MockClock::new(cache.clock.now_millis()));
    let cache2 = Arc::new(Cache::new(
        Arc::clone(&cache.config),
        clock2,
        BackendRegistry::new(HashMap::new()),
    ));
    cache2.load_and_start_with(std::time::Duration::from_secs(3_600)).await;
    let snap2 = cache2.snapshot().await;
    assert_eq!(
        snap2.segment_bytes, before.0,
        "the account must survive a restart (the disk is the authority)"
    );
    assert_eq!(
        cache2.inspect("a.bin").await.staged_spans, before.1,
        "and the spans must be rediscovered, not re-invented"
    );
}

