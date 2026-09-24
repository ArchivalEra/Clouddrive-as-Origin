//! The business plane's tests.
//!
//! Split out of `business.rs` so that file reads as the plane: it was 72%
//! tests, and a reader looking for the request path had to scroll past them.
//! It stays a CHILD MODULE of `business` (declared with `#[path]` in
//! `business.rs`) rather than moving to `tests/http.rs`, because the fixture
//! and helpers here are `#[cfg(test)]` and depend on `tempfile` — a
//! dev-dependency that must not be dragged into the shipped lib — and because
//! an integration test cannot see `pub(crate)` items at all.

use super::*;
use std::collections::HashMap;
use std::sync::atomic::Ordering;

use crate::{
    backend::{BackendRegistry, BackendSlot},
    clock::MockClock,
};

use crate::testsupport::CacheTestExt;
use crate::testsupport::{
    assert_no_backend_calls, body_text, headers, reset, staged_segments, stray_cache_files,
    wait_installed, wait_until, Fixture, FixtureBuilder, DEFAULT_TEST_URI,
};

/// The mock's fixed last-modified, as the backend it replaced returned.
const FIXTURE_LAST_MODIFIED: &str = "Wed, 01 Jan 2025 00:00:00 GMT";

/// The knobs every fixture shares, set to what the replaced mocks
/// returned: an octet-stream hint and a fixed last-modified.
fn base(bytes: &[u8]) -> FixtureBuilder {
    FixtureBuilder::new(bytes)
        .mime(Some("application/octet-stream"))
        .last_modified(Some(FIXTURE_LAST_MODIFIED))
}

/// Single-upstream ("primary") fixture. `extra` adds more upstreams
/// (used for the bucket-alias test). `missing` makes every key absent.
///
/// The two positional constructors that used to sit here (`fixture`, with four
/// arguments, and `fixture_full`, with six) are gone: a call site that read
/// `fixture_full(b, None, vec![], false, Some(url), true)` said nothing about
/// which of those was which, so those sites now chain the builder instead
/// (`base(b).direct(Some(url)).redirect().build()`). What remains below are the
/// two NAMED SHAPES, where the argument IS the shape and a chain would only be
/// longer: the session window an efficient fixture stages, and the no-disk
/// profile.
///
/// Efficient-profile fixture with the session window pinned to the span
/// the test means to stage. A run fetches its whole WINDOW (ADR-0016), so
/// the production 64 MiB default would stage this whole ten-byte fixture
/// on the first request and every span-count assertion here would be
/// about a different shape; the window's own behaviour is pinned by
/// `a_run_stages_its_window_not_just_the_requested_bytes`.
fn fixture_efficient(bytes: &[u8], session_window: u64) -> Fixture {
    base(bytes)
        .etag(Some("v1"))
        .coverage(4)
        .session_window(session_window)
        .build()
}

/// Nocache-profile fixture: primary serves `cache_profile = "nocache"`
/// (built-in pure water-pipe, zero disk writes).
fn fixture_nocache(bytes: &[u8]) -> Fixture {
    base(bytes).etag(Some("v1")).profile("nocache").build()
}

/// Prime the cache via GET miss + full drain, then wait for install.
async fn prime(fx: &Fixture, key: &str) {
    let resp = get_key(State(fx.state.clone()), Path(key.to_string()), headers(&[]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
    let (status, _, _) = body_text(resp).await;
    assert_eq!(status, StatusCode::OK);
    wait_installed(fx, key).await;
}

/// Prime below the business plane (bypasses the relief valve): for
/// redirect-enabled fixtures where GET would 307 instead of filling.
async fn prime_cache(fx: &Fixture, key: &str) {
    let mut hit = fx.state.cache.get_by_key(key, None).await.unwrap();
    crate::cache::flight::drain(&mut hit.body).await.unwrap();
    wait_installed(fx, key).await;
}

#[test]
fn range_parser_shapes() {
    assert!(matches!(client_range::parse(&headers(&[])).unwrap(), ClientRange::Absent));
    assert!(matches!(
        client_range::parse(&headers(&[("range", "bytes=10-20")])).unwrap(),
        ClientRange::Single(_)
    ));
    assert!(matches!(
        client_range::parse(&headers(&[("range", "bytes=-30")])).unwrap(),
        ClientRange::Suffix(30)
    ));
    assert!(matches!(
        client_range::parse(&headers(&[("range", "bytes=0-1,3-4")])).unwrap(),
        ClientRange::Multi
    ));
    assert!(client_range::parse(&headers(&[("range", "bytes=100-50")])).is_err());
    assert!(client_range::parse(&headers(&[("range", "bytes=-0")])).is_err());
    assert!(client_range::parse(&headers(&[("range", "items=0-1")])).is_err());
}

#[tokio::test]
async fn get_hit_s3_shape() {
    let fx = base(b"0123456789").etag(Some("abc123")).build();
    prime(&fx, "a.bin").await;
    let resp = get_key(State(fx.state.clone()), Path("a.bin".into()), headers(&[]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
    let (status, h, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "0123456789");
    assert_eq!(h.get("etag").unwrap(), "\"abc123\"");
    assert_eq!(h.get("accept-ranges").unwrap(), "bytes");
    assert_eq!(h.get("content-length").unwrap(), "10");
    assert_eq!(h.get("content-type").unwrap(), "binary/octet-stream");
    assert_eq!(h.get("last-modified").unwrap(), "Wed, 01 Jan 2025 00:00:00 GMT");
    assert!(h.get("x-amz-request-id").is_some());
    assert!(h.get("x-amz-id-2").is_some());
}

#[tokio::test]
async fn request_ids_unique_per_response() {
    let fx = base(b"0123456789").build();
    prime(&fx, "a.bin").await;
    let r1 = get_key(State(fx.state.clone()), Path("a.bin".into()), headers(&[]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
    let r2 = get_key(State(fx.state.clone()), Path("a.bin".into()), headers(&[]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
    assert_ne!(
        r1.headers().get("x-amz-request-id").unwrap(),
        r2.headers().get("x-amz-request-id").unwrap()
    );
}

#[tokio::test]
async fn get_missing_is_nosuchkey_xml() {
    let fx = base(b"0123456789").missing(true).build();
    // Missing on a fresh cache: stat the backend once to confirm absence.
    let resp = get_key(State(fx.state.clone()), Path("nope.bin".into()), headers(&[]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
    let (status, h, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(h.get("content-type").unwrap(), "application/xml");
    assert!(body.contains("<Code>NoSuchKey</Code>"), "{body}");
    let req_id = h.get("x-amz-request-id").unwrap().to_str().unwrap().to_string();
    assert!(body.contains(&format!("<RequestId>{req_id}</RequestId>")), "{body}");
    assert!(body.contains("<Resource>/nope.bin</Resource>"), "{body}");
}

#[tokio::test]
async fn get_unsatisfiable_is_invalidrange_xml() {
    let fx = base(b"0123456789").build();
    prime(&fx, "a.bin").await;
    let resp = get_key(
        State(fx.state.clone()),
        Path("a.bin".into()),
        headers(&[("range", "bytes=20-30")]),
        RawQuery(None),
        OriginalUri(DEFAULT_TEST_URI.clone()),
    )
    .await;
    let (status, h, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(h.get("content-range").unwrap(), "bytes */10");
    assert!(body.contains("<Code>InvalidRange</Code>"), "{body}");
}

#[tokio::test]
async fn get_multi_range_rejected() {
    let fx = base(b"0123456789").build();
    prime(&fx, "a.bin").await;
    let resp = get_key(
        State(fx.state.clone()),
        Path("a.bin".into()),
        headers(&[("range", "bytes=0-1,3-4")]),
        RawQuery(None),
        OriginalUri(DEFAULT_TEST_URI.clone()),
    )
    .await;
    let (status, _, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::RANGE_NOT_SATISFIABLE);
    assert!(body.contains("<Code>InvalidRange</Code>"), "{body}");
}

#[tokio::test]
async fn get_suffix_ranges() {
    let fx = base(b"0123456789").build();
    prime(&fx, "a.bin").await;
    // Last 3 bytes.
    let resp = get_key(
        State(fx.state.clone()),
        Path("a.bin".into()),
        headers(&[("range", "bytes=-3")]),
        RawQuery(None),
        OriginalUri(DEFAULT_TEST_URI.clone()),
    )
    .await;
    let (status, h, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(h.get("content-range").unwrap(), "bytes 7-9/10");
    assert_eq!(body, "789");
    // Suffix longer than the object → whole object, still 206.
    let resp = get_key(
        State(fx.state.clone()),
        Path("a.bin".into()),
        headers(&[("range", "bytes=-100")]),
        RawQuery(None),
        OriginalUri(DEFAULT_TEST_URI.clone()),
    )
    .await;
    let (status, h, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(h.get("content-range").unwrap(), "bytes 0-9/10");
    assert_eq!(body, "0123456789");
}

#[tokio::test]
async fn head_hit_no_backend_no_body() {
    let fx = base(b"0123456789").etag(Some("v1")).build();
    prime(&fx, "a.bin").await;
    reset(&fx);
    let resp = head_key(State(fx.state.clone()), Path("a.bin".into()), headers(&[]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone()))
        .await;
    let (status, h, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.is_empty());
    assert_eq!(h.get("content-length").unwrap(), "10");
    assert_eq!(h.get("etag").unwrap(), "\"v1\"");
    assert_eq!(h.get("accept-ranges").unwrap(), "bytes");
    // Extreme HEAD: fresh hit costs zero backend calls, zero flights.
    assert_eq!(fx.stat_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fx.open_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn head_ranged_returns_200_with_range_length() {
    let fx = base(b"0123456789").build();
    prime(&fx, "a.bin").await;
    reset(&fx);
    let resp = head_key(
        State(fx.state.clone()),
        Path("a.bin".into()),
        headers(&[("range", "bytes=2-5")]),
        RawQuery(None),
        OriginalUri(DEFAULT_TEST_URI.clone()),
    )
    .await;
    let (status, h, body) = body_text(resp).await;
    // R1: ranged HEAD is 200 (not 206), Content-Length = range length.
    assert_eq!(status, StatusCode::OK);
    assert!(body.is_empty());
    assert_eq!(h.get("content-length").unwrap(), "4");
    assert_eq!(h.get("content-range").unwrap(), "bytes 2-5/10");
    assert_eq!(fx.open_calls.load(Ordering::SeqCst), 0);
}

/// HEAD never takes the relief valve, and this is the one difference
/// between the two entry points that no test pinned: a redirect-capable
/// upstream that makes GET answer 307 leaves HEAD answering 200, because
/// a HEAD cannot follow a redirect and still report the object's shape.
/// Pinned deliberately -- if the two paths are ever unified, the choice
/// has to be made on purpose rather than by accident.
#[tokio::test]
async fn head_never_redirects_even_when_get_does() {
    let fx = base(b"0123456789")
        .direct(Some("https://cdn.example.com/f?sign=x"))
        .redirect()
        .build();
    let get = get_key(
        State(fx.state.clone()),
        Path("a.bin".into()),
        headers(&[]),
        RawQuery(None),
        OriginalUri(DEFAULT_TEST_URI.clone()),
    )
    .await;
    let (status, _, _) = body_text(get).await;
    assert_eq!(status, StatusCode::TEMPORARY_REDIRECT, "GET uses the valve");
    let probes_after_get = fx.direct_calls.load(Ordering::SeqCst);
    assert_eq!(probes_after_get, 1, "the valve probed the link once");

    let head = head_key(
        State(fx.state.clone()),
        Path("a.bin".into()),
        headers(&[]),
        RawQuery(None),
        OriginalUri(DEFAULT_TEST_URI.clone()),
    )
    .await;
    let (status, h, body) = body_text(head).await;
    assert_eq!(status, StatusCode::OK, "HEAD does not redirect");
    assert!(body.is_empty());
    assert_eq!(h.get("content-length").unwrap(), "10");
    // And it does not even ask the backend for a link.
    assert_eq!(
        fx.direct_calls.load(Ordering::SeqCst),
        probes_after_get,
        "HEAD adds no link probe"
    );
}

/// HEAD on a nocache profile does not go through the GET passthrough: it
/// reports the object's shape from a stat, writing nothing to disk. Pinned
/// because the GET path for this profile is a different code path
/// (`serve_nocache`), and the two must stay behaviourally consistent
/// without sharing an implementation.
#[tokio::test]
async fn head_on_nocache_reports_shape_without_touching_disk() {
    let fx = fixture_nocache(b"0123456789");
    let resp = head_key(
        State(fx.state.clone()),
        Path("a.bin".into()),
        headers(&[]),
        RawQuery(None),
        OriginalUri(DEFAULT_TEST_URI.clone()),
    )
    .await;
    let (status, h, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.is_empty());
    assert_eq!(h.get("content-length").unwrap(), "10");
    assert_eq!(fx.stat_calls.load(Ordering::SeqCst), 1, "one stat, no bytes");
    assert_eq!(fx.open_calls.load(Ordering::SeqCst), 0);
    assert!(staged_segments(&fx, "a.bin").await.is_empty(), "nocache stages nothing");
    assert_eq!(stray_cache_files(&fx), 0, "and writes nothing");
}

#[tokio::test]
async fn head_missing_404_empty_shares_negative_cache() {
    let fx = base(b"0123456789").missing(true).build();
    let resp = head_key(State(fx.state.clone()), Path("gone.bin".into()), headers(&[]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone()))
        .await;
    let (status, _, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.is_empty());
    assert_eq!(fx.stat_calls.load(Ordering::SeqCst), 1);
    // Second HEAD: negative tombstone, no second stat.
    let resp = head_key(State(fx.state.clone()), Path("gone.bin".into()), headers(&[]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone()))
        .await;
    let (status, _, _) = body_text(resp).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(fx.stat_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn head_stale_costs_one_stat_no_open() {
    let fx = base(b"0123456789").build();
    prime(&fx, "a.bin").await;
    // Age past revalidate_ttl (60 s default): HEAD must re-stat (fresh),
    // but still never opens a flight or reads bytes.
    fx.state.cache.clock.advance(61_000);
    reset(&fx);
    let resp = head_key(State(fx.state.clone()), Path("a.bin".into()), headers(&[]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone()))
        .await;
    let (status, h, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.is_empty());
    assert_eq!(h.get("content-length").unwrap(), "10");
    assert_eq!(fx.stat_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fx.open_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn bucket_alias_pins_upstream() {
    let fx = base(b"AAA").extra(vec![("archive", b"BBB".to_vec())]).build();
    // Legacy path routes "" → primary.
    let resp = get_key(State(fx.state.clone()), Path("f.bin".into()), headers(&[]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
    let (_, _, body) = body_text(resp).await;
    assert_eq!(body, "AAA");
    // Bucket alias pins the archive upstream regardless of routes.
    let resp = get_key(State(fx.state.clone()), Path("archive/f.bin".into()), headers(&[]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
    let (_, _, body) = body_text(resp).await;
    assert_eq!(body, "BBB");
}

#[tokio::test]
async fn redirect_cold_307_and_background_fill() {
    let fx = base(b"0123456789").direct(Some("https://cdn.example.com/f?sign=x")).redirect().build();
    let resp = get_key(State(fx.state.clone()), Path("new.bin".into()), headers(&[]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
    let (status, h, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(h.get("location").unwrap(), "https://cdn.example.com/f?sign=x");
    assert_eq!(h.get("cache-control").unwrap(), "no-store");
    assert!(body.is_empty());
    assert!(h.get("x-amz-request-id").is_some());
    assert_eq!(fx.direct_calls.load(Ordering::SeqCst), 1);
    // Background fill installs the entry without any viewer attached.
    wait_until("the background fill to install the entry", || async {
        fx.state.cache.entry_exists("new.bin").await
    })
    .await;
}

#[tokio::test]
async fn redirect_hit_serves_cache_never_redirects() {
    let fx = base(b"0123456789").direct(Some("https://cdn.example.com/f")).redirect().build();
    prime_cache(&fx, "a.bin").await;
    reset(&fx);
    // fresh hit → 200 from cache even though redirect is enabled.
    let resp = get_key(State(fx.state.clone()), Path("a.bin".into()), headers(&[]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
    let (status, _, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "0123456789");
    assert_eq!(fx.direct_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn redirect_unavailable_silently_proxies() {
    // Tier 3 (no link): normal water-pipe, viewer unaffected.
    let fx = base(b"0123456789").redirect().build();
    let resp = get_key(State(fx.state.clone()), Path("new.bin".into()), headers(&[]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
    let (status, _, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "0123456789");
    assert_eq!(fx.direct_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn redirect_rejected_target_silently_proxies() {
    // Foreign http is not an allowed redirect target → proxy.
    let fx = base(b"0123456789").direct(Some("http://cdn.example.com/f")).redirect().build();
    let resp = get_key(State(fx.state.clone()), Path("new.bin".into()), headers(&[]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
    let (status, _, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "0123456789");
}

#[tokio::test]
async fn redirect_disabled_never_consults_backend() {
    // Default proxy mode: direct_url untouched even when offered.
    let fx = base(b"0123456789").direct(Some("https://cdn.example.com/f")).build();
    let resp = get_key(State(fx.state.clone()), Path("new.bin".into()), headers(&[]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
    let (status, _, _) = body_text(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fx.direct_calls.load(Ordering::SeqCst), 0);
}

/// Spec §10: prewarm answers at once and fetches behind the caller, so
/// the row appears after the response, not before it.
#[tokio::test]
async fn prewarm_accepts_immediately_and_fetches_in_the_background() {
    let fx = base(b"0123456789").build();
    let resp = prewarm(State(fx.state.clone()), Path("w.bin".into()), headers(&[])).await.into_response();
    let (status, _, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(body.contains("accepted"), "{body}");
    wait_until("the prewarm to install the entry", || async {
        fx.state.cache.entry_exists("w.bin").await
    })
    .await;
    // The in-flight count must come back down on its own, or healthz
    // would report a queue that never drains.
    wait_until("the prewarm queue to drain", || async {
        fx.state.cache.snapshot().await.prewarm_inflight == 0
    })
    .await;
}

/// A second prewarm for an object already cached is a synchronous hit;
/// nothing new is fetched and nothing is counted as in flight.
#[tokio::test]
async fn prewarm_reports_a_hit_without_queueing_anything() {
    let fx = base(b"0123456789").build();
    prime(&fx, "w.bin").await;
    let resp = prewarm(State(fx.state.clone()), Path("w.bin".into()), headers(&[])).await.into_response();
    let (status, _, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("hit"), "{body}");
    assert_eq!(fx.state.cache.snapshot().await.prewarm_inflight, 0);
}

#[tokio::test]
async fn efficient_ranged_miss_passthrough_and_stages() {
    let fx = fixture_efficient(b"0123456789", 4);          // window = the 4-byte range: span (2,6)
    let resp = get_key(
        State(fx.state.clone()),
        Path("a.bin".into()),
        headers(&[("range", "bytes=2-5")]),
        RawQuery(None),
        OriginalUri(DEFAULT_TEST_URI.clone()),
    )
    .await;
    let (status, h, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(body, "2345");
    assert_eq!(h.get("content-range").unwrap(), "bytes 2-5/10");
    // No flight, no entry: pure passthrough.
    assert!(!fx.state.cache.entry_exists("a.bin").await);
    assert_eq!(fx.open_calls.load(Ordering::SeqCst), 1);
    // The run's window was staged as one sidecar (the seal lands after the
    // last byte, hence the wait); ledger merged.
    wait_ledger(&fx, "a.bin", &[(2, 6)]).await;
    assert_eq!(staged_segments(&fx, "a.bin").await, vec![(2, 6)]);
    let k = fx.state.cache.inspect("a.bin").await;
    assert_eq!(k.ledger_spans.len(), 1);
    assert_eq!((k.ledger_spans[0].start, k.ledger_spans[0].end), (2, 6));
    assert_eq!(k.ledger_total, Some(10));
    assert_eq!(k.ledger_etag.as_deref(), Some("v1"));
    assert_eq!(fx.state.cache.snapshot().await.segment_bytes, 4);
}

#[tokio::test]
async fn efficient_second_pull_merges_ledger() {
    let fx = fixture_efficient(b"0123456789", 2);          // window = 2: spans (0,2) and (4,6)
    for range in ["bytes=0-1", "bytes=4-5"] {
        let resp = get_key(State(fx.state.clone()), Path("a.bin".into()), headers(&[("range", range)]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
        let (status, _, _) = body_text(resp).await;
        assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    }
    wait_ledger(&fx, "a.bin", &[(0, 2), (4, 6)]).await;
    assert_eq!(staged_segments(&fx, "a.bin").await, vec![(0, 2), (4, 6)]);
    {
        let k = fx.state.cache.inspect("a.bin").await;
        assert_eq!(k.ledger_spans.len(), 2);
        assert_eq!((k.ledger_spans[0].start, k.ledger_spans[0].end), (0, 2));
        assert_eq!((k.ledger_spans[1].start, k.ledger_spans[1].end), (4, 6));
    }
    wait_segment_bytes(&fx, 4).await;
    assert_eq!(fx.state.cache.snapshot().await.segment_bytes, 4);
    // Still no cache entry: staging is not filling.
    assert!(!fx.state.cache.entry_exists("a.bin").await);
}

#[tokio::test]
async fn efficient_full_get_still_waterpipes() {
    let fx = fixture_efficient(b"0123456789", 4);          // full GET: the water-pipe path
    let resp = get_key(State(fx.state.clone()), Path("a.bin".into()), headers(&[]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
    let (status, _, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "0123456789");
    // Full GETs fill normally (a whole file needs no promotion dance).
    wait_installed(&fx, "a.bin").await;
    assert!(staged_segments(&fx, "a.bin").await.is_empty());
}

/// Staged reads: a seek whose bytes are already staged is answered from the
/// sidecars with NO upstream open. This is the seek-back case - the
/// reader returns to bytes an earlier request already paid for - and the
/// reason staged bytes exist at all once promotion is off the table.
#[tokio::test]
async fn a_fully_covered_seek_is_served_from_stage_without_upstream() {
    let fx = fixture_efficient(b"0123456789", 4);          // fully covered read
    // Stage the whole object in two pulls.
    for range in ["bytes=0-4", "bytes=5-9"] {
        let resp = get_key(State(fx.state.clone()), Path("a.bin".into()), headers(&[("range", range)]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
        let (status, _, _) = body_text(resp).await;
        assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    }
    // Both seals must be in the ledger before the seek: on a real disk the
    // seal lags the response it belongs to, and a seek that arrives first
    // finds no coverage and opens upstream (pitfalls 50/51).
    wait_ledger(&fx, "a.bin", &[(0, 5), (5, 10)]).await;
    reset(&fx);
    fx.state.cache.clock.advance(5_000);
    let resp = get_key(
        State(fx.state.clone()),
        Path("a.bin".into()),
        headers(&[("range", "bytes=2-7")]),
        RawQuery(None),
        OriginalUri(DEFAULT_TEST_URI.clone()),
    )
    .await;
    let (status, h, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(body, "234567");
    assert_eq!(h.get("content-range").unwrap(), "bytes 2-7/10");
    assert_eq!(
        fx.state.cache.inspect("a.bin").await.ledger_last_touch_millis,
        Some(5_000),
        "a staged read refreshes the row's age, so a watched window is not swept"
    );
    assert_eq!(
        fx.open_calls.load(Ordering::SeqCst),
        0,
        "a covered seek must not open upstream"
    );
    let before = source_total("stage");
    let resp = get_key(
        State(fx.state.clone()),
        Path("a.bin".into()),
        headers(&[("range", "bytes=3-4")]),
        RawQuery(None),
        OriginalUri(DEFAULT_TEST_URI.clone()),
    )
    .await;
    let (status, _, _) = body_text(resp).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert!(
        source_total("stage") - before >= 1.0,
        "the response must be labelled stage"
    );
}

/// A partially covered range serves its covered prefix from stage and
/// opens upstream ONCE for the remainder - one open regardless of how
/// many sidecars rode along - and the remainder is staged, so the ledger
/// ends fully covered, which is what a later read is served from.
#[tokio::test]
async fn a_partially_covered_range_needs_one_open_and_stages_the_rest() {
    let fx = fixture_efficient(b"0123456789", 5);          // window = 5: spans (0,5) and (5,10)
    let resp = get_key(State(fx.state.clone()), Path("a.bin".into()), headers(&[("range", "bytes=0-4")]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
    let (status, _, _) = body_text(resp).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    // The first window must be ON DISK before the second request asks for
    // it, or this stops being a test about a partially covered range: a
    // request that arrives while the first run is still unsealed finds no
    // coverage and no live run to ride, so it takes the standalone escape
    // (one span the length of the request, not one window).
    wait_spans(&fx, "a.bin", 1).await;
    reset(&fx);

    let resp = get_key(
        State(fx.state.clone()),
        Path("a.bin".into()),
        headers(&[("range", "bytes=0-9")]),
        RawQuery(None),
        OriginalUri(DEFAULT_TEST_URI.clone()),
    )
    .await;
    let (status, _, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(body, "0123456789");
    assert_eq!(
        fx.open_calls.load(Ordering::SeqCst),
        1,
        "the remainder is one exact-Range open, not one per gap"
    );
    // The seal of the tail span lands after the response it belongs to (the
    // body's last byte does not carry the ledger with it) — on a tmpfs /tmp
    // that race is invisible, on a real disk it is not. Wait for the record,
    // never assert on the response (pitfalls 50/51).
    wait_ledger(&fx, "a.bin", &[(0, 5), (5, 10)]).await;
    assert_eq!(
        staged_segments(&fx, "a.bin").await,
        vec![(0, 5), (5, 10)],
        "both spans stay staged: the ledger is now fully covered"
    );
    assert!(
        fx.state.cache.snapshot().await.entries == 0,
        "no promotion: the staged spans ARE the cache"
    );
}

/// An object we already hold must keep serving ranges after the
/// revalidate window. The efficient gate used to be the 60 s freshness
/// clock, so a complete entry stopped being served a minute after it was
/// filled and every ranged request went back upstream for bytes already
/// on disk, which is why the gate is on the entry and not on its age.
/// minute.
#[tokio::test]
async fn efficient_complete_entry_keeps_serving_ranges_after_the_revalidate_window() {
    let fx = fixture_efficient(b"0123456789", 4);          // complete entry, later range
    prime(&fx, "a.bin").await;
    fx.state.cache.clock.advance(61_000);
    reset(&fx);
    let resp = get_key(
        State(fx.state.clone()),
        Path("a.bin".into()),
        headers(&[("range", "bytes=2-5")]),
        RawQuery(None),
        OriginalUri(DEFAULT_TEST_URI.clone()),
    )
    .await;
    let (status, h, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(body, "2345");
    assert_eq!(h.get("content-range").unwrap(), "bytes 2-5/10");
    assert_eq!(
        fx.open_calls.load(Ordering::SeqCst),
        0,
        "a complete entry must serve the range it already holds, not re-pull it"
    );
    assert_eq!(
        fx.stat_calls.load(Ordering::SeqCst),
        1,
        "stale means one revalidation stat and no bytes"
    );
    assert!(
        staged_segments(&fx, "a.bin").await.is_empty(),
        "nothing to stage: the bytes are already in one file"
    );
}

#[tokio::test]
async fn efficient_min_size_bypass_goes_waterpipe() {
    let fx = base(b"0123456789")
        .etag(Some("v1"))
        .coverage(64) // below min_file_size: the miss goes the water-pipe
        .session_window(4)
        .build();
    let resp = get_key(
        State(fx.state.clone()),
        Path("a.bin".into()),
        headers(&[("range", "bytes=2-5")]),
        RawQuery(None),
        OriginalUri(DEFAULT_TEST_URI.clone()),
    )
    .await;
    let (status, _, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(body, "2345");
    // Bypass: B path installs the entry, stages nothing.
    wait_installed(&fx, "a.bin").await;
    assert!(staged_segments(&fx, "a.bin").await.is_empty());
}

#[tokio::test]
async fn a_ranged_miss_stages_its_window_on_the_default_profile() {
    // The default profile is `efficient`: a ranged read stages the window
    // it served, so the next request inside that window costs no upstream
    // open. It used to be `standard`, which water-piped the whole file and
    // installed an entry — that shape is still what a FULL GET does, and is
    // pinned by `miss_then_hit` and the cold-miss tests.
    let bytes: Vec<u8> = (0..40u8).collect();
    let fx = base(&bytes).session_window(8).max_size_bytes(1024).build();
    let resp = get_key(
        State(fx.state.clone()),
        Path("a.bin".into()),
        headers(&[("range", "bytes=2-5")]),
        RawQuery(None),
        OriginalUri(DEFAULT_TEST_URI.clone()),
    )
    .await;
    let (status, _, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(body.as_bytes(), &bytes[2..6]);
    // The run covers the request's own window FROM WHERE THE REQUEST
    // STARTS (nothing was staged before it), so the span is [2, 10).
    wait_ledger(&fx, "a.bin", &[(2, 10)]).await;
    assert_eq!(
        fx.state.cache.snapshot().await.entries,
        0,
        "a ranged read stages a window; it does not install an entry"
    );
}

#[tokio::test]
async fn tick_sweeps_old_segments() {
    let fx = fixture_efficient(b"0123456789", 4);          // window = the 4-byte range
    let resp = get_key(
        State(fx.state.clone()),
        Path("a.bin".into()),
        headers(&[("range", "bytes=2-5")]),
        RawQuery(None),
        OriginalUri(DEFAULT_TEST_URI.clone()),
    )
    .await;
    let (status, _, _) = body_text(resp).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    wait_ledger(&fx, "a.bin", &[(2, 6)]).await;
    assert_eq!(staged_segments(&fx, "a.bin").await, vec![(2, 6)]);
    // Age past inactive_ttl: tick sweeps segments, zeroes accounting.
    fx.state.cache.clock.advance(1_201_000);
    fx.state.cache.tick().await;
    assert!(staged_segments(&fx, "a.bin").await.is_empty());
    assert_eq!(fx.state.cache.snapshot().await.segment_bytes, 0);
}

#[tokio::test]
async fn healthz_reports_segment_bytes() {
    let fx = fixture_efficient(b"0123456789", 4);          // segment_bytes 4
    let resp = get_key(
        State(fx.state.clone()),
        Path("a.bin".into()),
        headers(&[("range", "bytes=2-5")]),
        RawQuery(None),
        OriginalUri(DEFAULT_TEST_URI.clone()),
    )
    .await;
    body_text(resp).await;
    wait_segment_bytes(&fx, 4).await;
    let resp = healthz(State(fx.state.clone()), RawQuery(None)).await.into_response();
    let (_, _, body) = body_text(resp).await;
    assert!(body.contains("\"segment_bytes\":4"), "{body}");
    assert!(body.contains("\"flights_active\":"), "{body}");
    assert!(body.contains("\"dirty_access_flushes\":"), "{body}");
    assert!(body.contains("\"sigv4_enabled\":false"), "{body}");
    assert!(body.contains("\"profile\":\"efficient\""), "{body}");
    assert!(body.contains("\"id\":\"primary\""), "{body}");
}

/// C1: healthz must carry a real verdict, not a hardcoded "ok". A fresh
/// node is healthy; the response must say so explicitly (so a monitor
/// can tell "answering" from "healthy") and expose the disk numbers.
#[tokio::test]
async fn healthz_reports_a_verdict_and_disk_state() {
    let fx = base(b"x").build();
    let resp = healthz(State(fx.state.clone()), RawQuery(None)).await.into_response();
    assert_eq!(resp.status(), StatusCode::OK, "liveness stays 200");
    let (_, _, body) = body_text(resp).await;
    assert!(body.contains("\"degraded\":false"), "{body}");
    assert!(body.contains("\"status\":\"ok\""), "{body}");
    assert!(body.contains("\"store\":{\"state\":\"ready\"}"), "{body}");
    assert!(body.contains("\"disk_free_bytes\":"), "{body}");
    assert!(body.contains("\"disk_reserve_bytes\":"), "{body}");
    assert!(body.contains("\"rebuilt_rows\":0"), "{body}");
}

/// healthz on a nocache upstream reports the profile so an operator
/// can spot a node misconfigured into zero-disk mode.
#[tokio::test]
async fn healthz_reports_nocache_profile() {
    let fx = fixture_nocache(b"0123456789");
    let resp = healthz(State(fx.state.clone()), RawQuery(None)).await.into_response();
    let (_, _, body) = body_text(resp).await;
    assert!(body.contains("\"profile\":\"nocache\""), "{body}");
    assert!(body.contains("\"entries\":0"), "{body}");
}

/// `?key=` answers the questions an investigation actually starts with: is
/// this object installed, which spans are staged for it, who is watching
/// it. It rides the same seam the tests read state through
/// (`Cache::inspect`), so "what is this key's state" has one answer rather
/// than one per consumer.
#[tokio::test]
async fn healthz_answers_for_one_key_on_demand() {
    let fx = fixture_efficient(b"0123456789", 4);
    let resp = get_key(
        State(fx.state.clone()),
        Path("a.bin".into()),
        headers(&[("range", "bytes=2-5")]),
        RawQuery(None),
        OriginalUri(DEFAULT_TEST_URI.clone()),
    )
    .await;
    body_text(resp).await;
    wait_spans(&fx, "a.bin", 1).await;

    // A key with nothing on this node, and one with a staged span. The
    // parameter is percent-encoded here, because that is how it arrives.
    let resp = healthz(State(fx.state.clone()), RawQuery(Some("key=absent.bin".into())))
        .await
        .into_response();
    let (_, _, body) = body_text(resp).await;
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let key = v.get("key").expect("the key section");
    assert_eq!(key["installed"], json!(false));
    assert_eq!(key["staged_bytes"], json!(0));
    assert_eq!(key["staged_spans"], json!([]));

    let resp = healthz(State(fx.state.clone()), RawQuery(Some("key=a.bin".into())))
        .await
        .into_response();
    let (_, _, body) = body_text(resp).await;
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let key = v.get("key").expect("the key section");
    // The disk's view and the ledger's view of the same span, kept apart.
    assert_eq!(key["installed"], json!(false), "no entry was installed");
    assert_eq!(key["staged_spans"], json!([{"start": 2, "end": 6}]));
    assert_eq!(key["staged_bytes"], json!(4));
    assert_eq!(key["ledger_spans"][0]["start"], json!(2));
    assert_eq!(key["ledger_spans"][0]["end"], json!(6));
    assert_eq!(key["ledger_total"], json!(10));
    assert_eq!(key["ledger_etag"], json!("v1"));

    // No parameter: the body is what it always was.
    let resp = healthz(State(fx.state.clone()), RawQuery(None)).await.into_response();
    let (_, _, body) = body_text(resp).await;
    assert!(!body.contains("\"key\":"), "no key section without the query: {body}");
}

/// A key is arbitrary bytes, so the query is percent-decoded rather than
/// split on `=`: `media/a b.bin` has to reach the same key the object
/// plane would route.
#[test]
fn healthz_key_parameter_decodes() {
    assert_eq!(key_param("key=media%2Fa+b.bin").as_deref(), Some("media/a b.bin"));
    assert_eq!(key_param("key=a.bin&other=1").as_deref(), Some("a.bin"));
    assert_eq!(key_param("other=1"), None);
    assert_eq!(key_param("key="), None);
}

/// Version flip between transfers: history resets, no mixed-version entry
/// is ever installed. The flip is settled on a request that arrives once
/// nobody is watching the key — while a viewer is still there the reset is
/// deferred (`a_version_drift_waits_for_a_watcher_to_leave`).
#[tokio::test]
async fn etag_flip_resets_staged_history() {
    let bytes: Vec<u8> = (0..100u8).collect();
    let fx = fixture_efficient(&bytes, 50);                // window = the 50-byte range
    let resp = get_key(State(fx.state.clone()), Path("f.bin".into()), headers(&[("range", "bytes=0-49")]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
    body_text(resp).await;
    wait_ledger(&fx, "f.bin", &[(0, 50)]).await;
    assert_eq!(staged_segments(&fx, "f.bin").await, vec![(0, 50)]);
    // The viewer is gone and its watch has lapsed, so the drifted ledger is
    // settled where it stands rather than deferred.
    fx.state.cache.clock.advance(1_000_000);
    // Object replaced upstream: next transfer restarts history. Wait
    // out the stat single-flight cooldown so the flip is
    // observed on a fresh stat.
    *fx.etag.lock().unwrap() = Some("v2".into());
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    let resp = get_key(State(fx.state.clone()), Path("f.bin".into()), headers(&[("range", "bytes=50-79")]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
    body_text(resp).await;
    wait_ledger(&fx, "f.bin", &[(50, 100)]).await;
    // Old segments dropped, ledger re-anchored on v2, no entry yet. The
    // span is the request's WINDOW (50 bytes here), not its own 30 bytes:
    // the seek starts a run (ADR-0016).
    assert_eq!(staged_segments(&fx, "f.bin").await, vec![(50, 100)]);
    assert_eq!(
        fx.state.cache.inspect("f.bin").await.ledger_etag.as_deref(),
        Some("v2")
    );
    assert!(!fx.state.cache.entry_exists("f.bin").await);
}

/// A drift discovered WHILE the key is being watched must not cut the
/// viewer off: the reset deletes every `.seg` of the key, and a response
/// body opens its staged pieces lazily, so a reset under a live body breaks
/// it on the first piece it has not opened yet. The reset is right — mixed
/// versions must never be served — but it can wait: this request is
/// answered from upstream (never from a mix), and the next request that
/// arrives when nobody is watching settles the drift.
#[tokio::test]
async fn a_version_drift_waits_for_a_watcher_to_leave() {
    let bytes: Vec<u8> = (0..100u8).collect();
    let fx = fixture_efficient(&bytes, 50);
    let resp = get_key(State(fx.state.clone()), Path("f.bin".into()), headers(&[("range", "bytes=0-49")]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
    body_text(resp).await;
    wait_ledger(&fx, "f.bin", &[(0, 50)]).await;

    *fx.etag.lock().unwrap() = Some("v2".into());
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    let before = source_total("upstream");
    let resp = get_key(State(fx.state.clone()), Path("f.bin".into()), headers(&[("range", "bytes=50-79")]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
    let (status, _, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(body.as_bytes(), &bytes[50..80], "the bytes are the provider's, not the stale span's");
    assert!(
        source_total("upstream") - before >= 1.0,
        "a drifted read is answered from upstream while a watcher holds the key"
    );
    assert_eq!(
        staged_segments(&fx, "f.bin").await,
        vec![(0, 50)],
        "the old version's spans are still on disk, untouched"
    );
    assert_eq!(
        fx.state.cache.inspect("f.bin").await.ledger_etag.as_deref(),
        Some("v1"),
        "the ledger was not re-anchored under the viewer: nothing was mixed"
    );

    // The viewer leaves; the next request settles the drift.
    fx.state.cache.clock.advance(1_000_000);
    let resp = get_key(State(fx.state.clone()), Path("f.bin".into()), headers(&[("range", "bytes=50-79")]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
    body_text(resp).await;
    wait_ledger(&fx, "f.bin", &[(50, 100)]).await;
    assert_eq!(staged_segments(&fx, "f.bin").await, vec![(50, 100)], "the old spans went once nobody was reading");
    assert_eq!(
        fx.state.cache.inspect("f.bin").await.ledger_etag.as_deref(),
        Some("v2")
    );
}

#[tokio::test]
async fn prewarm_secret_gate_blocks_anonymous() {
    // Endpoint is open when prewarm_shared_secret_env is unset, but
    // when set and wrong token sent, it must 401.
    let fx = base(b"0123456789").build();
    std::env::set_var("TEST_PW_SECRET", "right-token");
    let mut cfg = fx.state.config.as_ref().clone();
    cfg.prewarm_shared_secret_env = Some("TEST_PW_SECRET".into());
    let state = AppState {
        cache: fx.state.cache.clone(),
        config: Arc::new(cfg),
        sigv4_config: None,
        listings: Default::default(),
    };
    // Wrong token: rejected.
    let resp = prewarm(State(state.clone()), Path("w.bin".into()), headers(&[("x-prewarm-token", "wrong")]))
        .await
        .into_response();
    let (status, _, _) = body_text(resp).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    // Right token: proceeds.
    let resp = prewarm(State(state.clone()), Path("w.bin".into()), headers(&[("x-prewarm-token", "right-token")]))
        .await
        .into_response();
    let (status, _, _) = body_text(resp).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    std::env::remove_var("TEST_PW_SECRET");
}

/// The branch the token gate takes when the CONFIG names a secret but the
/// environment does not define it: `expected` is empty, and an empty expected
/// secret must refuse everything rather than wave it through — a node that lost
/// its env file would otherwise expose an unauthenticated upstream fetcher.
/// `main` bails on that case at startup; this is the handler's own backstop, and
/// it had no test.
#[tokio::test]
async fn prewarm_401s_when_the_named_secret_is_missing() {
    let fx = base(b"0123456789").build();
    std::env::remove_var("TEST_PW_MISSING");
    let mut cfg = fx.state.config.as_ref().clone();
    cfg.prewarm_shared_secret_env = Some("TEST_PW_MISSING".into());
    let state = AppState {
        cache: fx.state.cache.clone(),
        config: Arc::new(cfg),
        sigv4_config: None,
        listings: Default::default(),
    };
    // Even a caller who guesses the (empty) secret cannot get through.
    let resp = prewarm(State(state.clone()), Path("m.bin".into()), headers(&[("x-prewarm-token", "")]))
        .await
        .into_response();
    assert_eq!(body_text(resp).await.0, StatusCode::UNAUTHORIZED);
    let resp = prewarm(State(state), Path("m.bin".into()), headers(&[("x-prewarm-token", "anything")]))
        .await
        .into_response();
    assert_eq!(body_text(resp).await.0, StatusCode::UNAUTHORIZED);
}

/// SigV4 gate end-to-end at the business seam: anonymous passes, a
/// correctly-signed request passes, a tampered signature 403s with a
/// no-store XML error.
#[tokio::test]
async fn sigv4_gate_anonymous_passes_and_bad_signature_403s() {
    let fx = base(b"0123456789").build();
    let cfg = crate::sigv4::SigV4Config {
        access_key_id: "AKIDEXAMPLE".into(),
        secret_access_key: "s3cr3t".into(),
    };
    let mut state = fx.state.clone();
    state.sigv4_config = Some(cfg.clone());

    // Anonymous request: no gate response (proceeds to the cache).
    let gate = sigv4_gate(
        Some(&cfg),
        SigV4Request { method: "GET", raw_uri_path: "/a.bin", query: None, headers: &headers(&[]) },
        "r",
        "h",
        0,
    );
    assert!(gate.is_none());

    // Tampered signature: 403 AccessDenied XML, no-store.
    let amz_date = "20130524T000000Z";
    let bad_sig = "0".repeat(64);
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20130524/us-east-1/s3/aws4_request, \
         SignedHeaders=host;x-amz-content-sha256;x-amz-date, \
         Signature={bad_sig}"
    );
    let h = headers(&[
        ("host", "origin.example.com"),
        ("x-amz-date", amz_date),
        ("x-amz-content-sha256", "UNSIGNED-PAYLOAD"),
        ("authorization", auth.as_str()),
    ]);
    // Skewed-clock check fires first; to reach the signature mismatch
    // we set "now" to the request's own time (2013-05-24T00:00:00Z).
    let now: i64 = 1_369_353_600;
    let resp = sigv4_gate(
        Some(&cfg),
        SigV4Request { method: "GET", raw_uri_path: "/a.bin", query: None, headers: &h },
        "r",
        "h",
        now,
    )
    .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert_eq!(resp.headers().get("cache-control").unwrap(), "no-store");
    let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
    let body = String::from_utf8_lossy(&body).into_owned();
    assert!(body.contains("<Code>AccessDenied</Code>"), "{body}");
}

/// Nocache full GET: correct bytes, exact stat+open calls, and the
/// cache directory stays EMPTY (no entry, no segment, no tmp).
#[tokio::test]
async fn nocache_full_get_zero_disk() {
    let fx = fixture_nocache(b"0123456789");
    let resp = get_key(State(fx.state.clone()), Path("a.bin".into()), headers(&[]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
    let (status, h, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "0123456789");
    assert_eq!(h.get("content-length").unwrap(), "10");
    assert_eq!(h.get("etag").unwrap(), "\"v1\"");
    // One stat (headers) + one open (bytes). No flight machinery.
    assert_eq!(fx.stat_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fx.open_calls.load(Ordering::SeqCst), 1);
    // Zero disk: no entries, no segment bytes, no stray cache files.
    assert!(!fx.state.cache.entry_exists("a.bin").await);
    assert_eq!(fx.state.cache.snapshot().await.total_bytes, 0);
    assert_eq!(fx.state.cache.snapshot().await.segment_bytes, 0);
    assert_eq!(stray_cache_files(&fx), 0);
}

/// Nocache ranged GET: exact slice, 206, still zero disk.
#[tokio::test]
async fn nocache_ranged_get_slices_and_zero_disk() {
    let fx = fixture_nocache(b"0123456789");
    let resp = get_key(State(fx.state.clone()), Path("a.bin".into()), headers(&[("range", "bytes=2-5")]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
    let (status, h, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(body, "2345");
    assert_eq!(h.get("content-range").unwrap(), "bytes 2-5/10");
    assert_eq!(stray_cache_files(&fx), 0);
    assert_eq!(fx.state.cache.snapshot().await.total_bytes, 0);
    assert_eq!(fx.state.cache.snapshot().await.segment_bytes, 0);
}

/// Nocache HEAD: one stat, no open, no disk state.
#[tokio::test]
async fn nocache_head_is_pure_stat() {
    let fx = fixture_nocache(b"0123456789");
    let resp = head_key(State(fx.state.clone()), Path("a.bin".into()), headers(&[]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
    let (status, h, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.is_empty());
    assert_eq!(h.get("content-length").unwrap(), "10");
    assert_eq!(fx.stat_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fx.open_calls.load(Ordering::SeqCst), 0);
    // HEAD on a nocache upstream writes nothing (no tombstones either).
    assert_eq!(stray_cache_files(&fx), 0);
}

/// Nocache prewarm: accepted like any other, but the background fetch is
/// a no-op (there is nothing to fill); it never opens the backend and
/// installs no row.
#[tokio::test]
async fn nocache_prewarm_is_noop() {
    let fx = fixture_nocache(b"0123456789");
    let resp = prewarm(State(fx.state.clone()), Path("w.bin".into()), headers(&[])).await.into_response();
    let (status, _, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(body.contains("accepted"), "{body}");
    // Let the background task run before judging what it did.
    wait_until("the rejected prewarm to settle", || async {
        fx.state.cache.snapshot().await.prewarm_inflight == 0
    })
    .await;
    assert_eq!(fx.open_calls.load(Ordering::SeqCst), 0);
    assert!(!fx.state.cache.entry_exists("w.bin").await);
}

/// A malformed object key is a client error: 400 with the S3
/// `InvalidRequest` envelope on GET, and the same status with an empty
/// body on HEAD.
async fn assert_invalid_request_400(resp: Response, what: &str) {
    let head = what.starts_with("HEAD");
    let (status, h, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{what}");
    assert_eq!(h.get("content-type").unwrap(), "application/xml", "{what}");
    assert!(h.contains_key("x-amz-request-id"), "{what}");
    assert!(h.contains_key("x-amz-id-2"), "{what}");
    if head {
        assert!(body.is_empty(), "HEAD must not carry a body: {what} {body}");
    } else {
        assert!(body.contains("<Code>InvalidRequest</Code>"), "{what} {body}");
    }
}

#[tokio::test]
async fn root_handlers_reject_empty_object_keys_without_backend_calls() {
    let fx = base(b"unused").build();
    for query in [None, Some(""), Some("download=1")] {
        let resp = get_key_root(
            State(fx.state.clone()),
            headers(&[]),
            RawQuery(query.map(str::to_owned)),
            OriginalUri("/".parse().unwrap()),
        ).await;
        assert_invalid_request_400(resp, &format!("GET / query={query:?}")).await;
    }
    for query in [None, Some("list-type=2")] {
        let resp = head_key_root(
            State(fx.state.clone()),
            headers(&[]),
            RawQuery(query.map(str::to_owned)),
            OriginalUri("/".parse().unwrap()),
        ).await;
        assert_invalid_request_400(resp, &format!("HEAD / query={query:?}")).await;
    }
    assert_no_backend_calls(&fx, "root handlers");
}

/// Every way a key can be rejected that a URI can actually express, driven
/// through the real axum Router so path extraction and percent-decoding
/// are part of the test rather than only direct handler calls.
#[tokio::test]
async fn invalid_object_keys_are_bad_requests_for_get_and_head() {
    use tower::ServiceExt;

    let fx = base(b"unused").build();
    let keys = [
        "/../outside",              // traversal
        "/googledrive1/../outside", // traversal under a bucket alias
        "/nul%00key",               // NUL, percent-decoded by axum
        "/back%5Cslash",            // backslash, percent-decoded
        "/bad%",                    // malformed percent encoding
        "/redb.db",                 // reserved: the metadata store
        "/.tmp.a.b.1234",           // reserved: an ephemeral artifact
    ];
    for key in keys {
        for method in ["GET", "HEAD"] {
            let request = axum::http::Request::builder()
                .method(method)
                .uri(key)
                .body(Body::empty())
                .unwrap();
            let resp = router(fx.state.clone()).oneshot(request).await.unwrap();
            assert_invalid_request_400(resp, &format!("{method} {key}")).await;
        }
    }
    assert_no_backend_calls(&fx, "invalid keys via the router");
}

/// The rejections a URI cannot express, so the handler seam is the only
/// place they can be reached: an empty key, and an absolute key (a
/// leading slash in a URI is consumed by the URL, so `/absolute` routed
/// for real yields the perfectly valid key `absolute` and a 200).
#[tokio::test]
async fn invalid_object_keys_unreachable_by_uri_are_rejected_at_the_seam() {
    let fx = base(b"unused").build();
    for key in ["", "/absolute"] {
        let get = get_key(
            State(fx.state.clone()), Path(key.into()), headers(&[]), RawQuery(None),
            OriginalUri(DEFAULT_TEST_URI.clone()),
        ).await;
        assert_invalid_request_400(get, &format!("GET key={key:?}")).await;
        let head = head_key(
            State(fx.state.clone()), Path(key.into()), headers(&[]), RawQuery(None),
            OriginalUri(DEFAULT_TEST_URI.clone()),
        ).await;
        assert_invalid_request_400(head, &format!("HEAD key={key:?}")).await;
    }
    assert_no_backend_calls(&fx, "seam-only invalid keys");
    // Proof of the claim above: the same text through real routing is a
    // valid key, not an error. It legitimately reaches the backend, so
    // the no-call assertion sits before it.
    use tower::ServiceExt;
    let request = axum::http::Request::builder().uri("/absolute").body(Body::empty()).unwrap();
    let (status, _, _) = body_text(router(fx.state.clone()).oneshot(request).await.unwrap()).await;
    assert_ne!(status, StatusCode::BAD_REQUEST, "routed /absolute is the key `absolute`");
}

/// V1 and V2 listing must keep working: the invalid-key fix must not
/// turn a legitimate bucket listing into an error.
#[tokio::test]
async fn root_router_preserves_v2_and_v1_listing() {
    use crate::backend::ListEntry;
    use tower::ServiceExt;

    let fx = base(b"unused").build();
    let backend = crate::testsupport::MockBackend::new(b"unused", None, None).with_listing(vec![ListEntry {
        key: "listed.bin".into(),
        size: 6,
        etag: None,
        last_modified: None,
        is_dir: false,
    }]);
    let slots = HashMap::from([(
        "primary".into(),
        Arc::new(BackendSlot::new(Arc::new(backend), 3)),
    )]);
    let state = AppState {
        cache: Arc::new(Cache::new(
            Arc::clone(&fx.state.config), Arc::new(MockClock::new(0)), BackendRegistry::new(slots),
        )),
        config: Arc::clone(&fx.state.config),
        sigv4_config: None,
        listings: Default::default(),
    };
    for (uri, v2) in [("/?list-type=2&delimiter=/", true), ("/?delimiter=/", false)] {
        let request = axum::http::Request::builder().uri(uri).body(Body::empty()).unwrap();
        let (status, h, body) = body_text(router(state.clone()).oneshot(request).await.unwrap()).await;
        assert_eq!(status, StatusCode::OK, "{uri}");
        assert_eq!(h.get("content-type").unwrap(), "application/xml");
        assert!(body.contains("<ListBucketResult"), "{body}");
        assert!(body.contains("<Name>primary</Name>"), "{body}");
        assert!(body.contains("<Key>listed.bin</Key>"), "{body}");
        assert_eq!(body.contains("<KeyCount>1</KeyCount>"), v2);
    }
}

/// The root route itself answers 400 through real routing, for both
/// methods. Kept separate from the listing test: one is the invalid-key
/// contract, the other is the list contract, and they change for
/// different reasons.
#[tokio::test]
async fn root_router_answers_400_for_root_get_and_head() {
    use tower::ServiceExt;

    let fx = base(b"unused").build();
    for method in ["GET", "HEAD"] {
        let request = axum::http::Request::builder()
            .method(method).uri("/").body(Body::empty()).unwrap();
        let resp = router(fx.state.clone()).oneshot(request).await.unwrap();
        assert_invalid_request_400(resp, &format!("{method} / via router")).await;
    }
    assert_no_backend_calls(&fx, "root via router");
}

/// The HEAT policy must diverge from LRU on exactly the case it exists
/// for: a span that is STALE by the clock but HOT by reads stays, and the
/// fresher-but-never-re-read span goes. With lru (the default) the same
/// shape ejects the hot one, which is why the policy is a choice.
///
/// Two keys are needed to overshoot at all: admission (ADR-0013) refuses
/// to stage an object bigger than the magazine, so one key's staged bytes
/// can never exceed the budget on their own.
#[tokio::test]
async fn heat_eviction_keeps_the_hot_span_lru_would_eject() {
    let bytes: Vec<u8> = (0..10u8).collect();
    let fx = base(&bytes)
        .etag(Some("v1"))
        .coverage(10)
        .session_window(5) // two 5-byte spans, so a policy has a choice
        .max_size_bytes(10)
        .eviction(crate::config::EvictionPolicy::Heat)
        // The viewer protections are OFF here, exactly as they are in the
        // LAB's eviction account (config-d): what this test measures is the
        // POLICY's choice between two spans, so a pin must not be part of
        // the answer (ADR-0018).
        .read_grace(0)
        .watch_pin(0)
        .build();

    // a.bin stages two spans ([0,5) then [5,10)) — its whole 10 bytes,
    // which is exactly the budget, so staging is admitted.
    stage(&fx, "a.bin", "bytes=0-4").await;
    stage(&fx, "a.bin", "bytes=5-9").await;
    // [0,5) is re-read twice: staged FIRST, so stalest by the clock, yet
    // hot. [5,10) was staged later and never read again.
    stage(&fx, "a.bin", "bytes=0-4").await;
    stage(&fx, "a.bin", "bytes=0-4").await;
    // Both spans on disk before the policy is asked anything: the seal is
    // asynchronous (see `wait_ledger`), and a missing span here would be
    // read as a policy answer rather than a timing artifact.
    wait_spans(&fx, "a.bin", 2).await;
    assert_eq!(staged_segments(&fx, "a.bin").await, vec![(0, 5), (5, 10)]);
    // b.bin stages 5 more bytes, and its row is the NEWER one: the 5-byte
    // overshoot must come out of a.bin, under both policies.
    fx.state.cache.clock.advance(1_000);
    stage(&fx, "b.bin", "bytes=0-4").await;
    wait_segment_bytes(&fx, 15).await;
    assert_eq!(fx.state.cache.snapshot().await.segment_bytes, 15);

    // Age both rows past the eviction guard so they are evictable.
    fx.state.cache.clock.advance(120_000);
    fx.state.cache.tick().await;

    assert_eq!(
        staged_segments(&fx, "a.bin").await,
        vec![(0, 5)],
        "heat keeps the span that was re-read; the colder fresh one goes"
    );
    assert_eq!(staged_segments(&fx, "b.bin").await, vec![(0, 5)], "the newer row is untouched");
    assert_eq!(fx.state.cache.snapshot().await.segment_bytes, 10);
}

/// The same shape under lru (the default): the stalest span goes, reads
/// irrelevant — which here means the HOT one. The two policies must
/// therefore disagree on this shape, and that disagreement is the whole
/// point of the knob. Reverse-verification for the test above.
#[tokio::test]
async fn lru_eviction_ejects_the_stale_span_even_when_it_is_hot() {
    let bytes: Vec<u8> = (0..10u8).collect();
    let fx = base(&bytes)
        .etag(Some("v1"))
        .coverage(10)
        .session_window(5) // two 5-byte spans, so a policy has a choice
        .max_size_bytes(10)
        .eviction(crate::config::EvictionPolicy::Lru)
        // Protections off: this measures the policy (see the heat twin).
        .read_grace(0)
        .watch_pin(0)
        .build();

    stage(&fx, "a.bin", "bytes=0-4").await;
    stage(&fx, "a.bin", "bytes=5-9").await;
    stage(&fx, "a.bin", "bytes=0-4").await;
    stage(&fx, "a.bin", "bytes=0-4").await;
    fx.state.cache.clock.advance(1_000);
    stage(&fx, "b.bin", "bytes=0-4").await;
    wait_segment_bytes(&fx, 15).await;
    assert_eq!(fx.state.cache.snapshot().await.segment_bytes, 15);

    fx.state.cache.clock.advance(120_000);
    fx.state.cache.tick().await;

    assert_eq!(
        staged_segments(&fx, "a.bin").await,
        vec![(5, 10)],
        "lru ejects by the clock: the older span goes even though it is hot"
    );
    assert_eq!(staged_segments(&fx, "b.bin").await, vec![(0, 5)], "the newer row is untouched");
    // Span-level: the row survives with its other span, and the byte
    // account drops by one span rather than by a whole window.
    assert_eq!(fx.state.cache.snapshot().await.segment_bytes, 10);
}

/// Sealing is asynchronous: the run's driver renames the span and merges
/// the ledger entry after the body's last byte reaches the viewer, so a
/// test that samples staged state the instant a response returns races it
/// (the same discipline the lab's `wait_segment_bytes` uses). Poll for the
/// ledger to describe `expect` instead.
async fn wait_ledger(fx: &Fixture, key: &str, expect: &[(u64, u64)]) {
    wait_until(&format!("the ledger for {key} to become {expect:?}"), || async {
        ledger_spans(fx, key).await == expect
    })
    .await;
}

/// The ledger's spans as `(start, end)`, off the same view an operator reads.
async fn ledger_spans(fx: &Fixture, key: &str) -> Vec<(u64, u64)> {
    fx.state
        .cache
        .inspect(key)
        .await
        .ledger_spans
        .iter()
        .map(|s| (s.start, s.end))
        .collect()
}

/// The same wait for the staged-byte counter (healthz-level assertions).
async fn wait_segment_bytes(fx: &Fixture, expect: u64) {
    wait_until(&format!("segment_bytes to reach {expect}"), || async {
        fx.state.cache.snapshot().await.segment_bytes == expect
    })
    .await;
}

/// One ranged GET, asserting it was served as a partial response. The
/// staging tests below all need the same three lines.
async fn stage(fx: &Fixture, key: &str, range: &str) {
    let resp = get_key(
        State(fx.state.clone()),
        Path(key.into()),
        headers(&[("range", range)]),
        RawQuery(None),
        OriginalUri(DEFAULT_TEST_URI.clone()),
    )
    .await;
    let (status, _, _) = body_text(resp).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT, "{key} {range}");
}

/// The ledger's `(start, end, reads)` triples, for the heat assertions.
async fn span_reads(fx: &Fixture, key: &str) -> Vec<(u64, u64, u64)> {
    fx.state
        .cache
        .inspect(key)
        .await
        .ledger_spans
        .iter()
        .map(|s| (s.start, s.end, s.reads))
        .collect()
}

/// Heat must see the reads a scrub actually makes. Two rules, both of
/// which the first cut got wrong by requiring one span to CONTAIN the
/// whole read: a read crossing two staged spans credits both, and a read
/// whose head is served from the stage — the partial-coverage path, which
/// is what every request in a cold sequential walk takes — credits the
/// staged head. With the old rule both recorded nothing, so heat stayed
/// at zero for exactly the workload the policy exists for.
#[tokio::test]
async fn a_read_credits_every_staged_span_it_touches() {
    let bytes: Vec<u8> = (0..20u8).collect();
    let fx = base(&bytes)
        .etag(Some("v1"))
        .coverage(4)
        .session_window(5) // one 5-byte span per request, as the walk reads
        .max_size_bytes(4096)
        .build();

    stage(&fx, "a.bin", "bytes=0-4").await;
    // A seal lands asynchronously (the body's tail, or the disconnect watcher),
    // so the next request waits for the span to be recorded first: otherwise it
    // plans against a disk that does not have it yet and takes the standalone
    // escape instead of being served from the stage, which is a different read
    // than the one this test is about.
    wait_ledger(&fx, "a.bin", &[(0, 5)]).await;
    stage(&fx, "a.bin", "bytes=5-9").await;
    wait_ledger(&fx, "a.bin", &[(0, 5), (5, 10)]).await;
    // Straddles the [0,5)/[5,10) boundary: fully covered, so it is served
    // from the stage, and it touches two spans.
    stage(&fx, "a.bin", "bytes=3-7").await;
    assert_eq!(span_reads(&fx, "a.bin").await, vec![(0, 5, 1), (5, 10, 1)]);

    // A partial hit: [0,10) comes from the stage, [10,20) from upstream.
    stage(&fx, "a.bin", "bytes=0-19").await;
    wait_ledger(&fx, "a.bin", &[(0, 5), (5, 10), (10, 20)]).await;
    assert_eq!(
        span_reads(&fx, "a.bin").await,
        vec![(0, 5, 2), (5, 10, 2), (10, 20, 0)],
        "the staged head both spans contributed is credited; the freshly fetched tail is not"
    );
}

/// End to end through the response path: a body that is still in flight
/// holds the key's read lease, so neither the budget nor the idle sweep
/// takes its bytes while the viewer is watching — and the lease ends with
/// the body (ADR-0017).
#[tokio::test]
async fn a_body_in_flight_keeps_its_bytes_alive_past_the_ttl() {
    let bytes: Vec<u8> = (0..40u8).collect();
    let fx = base(&bytes)
        .etag(Some("v1"))
        .coverage(4)
        .session_window(8)
        .max_size_bytes(1024)
        .build();
    let resp = get_key(State(fx.state.clone()), Path("a.bin".into()), headers(&[("range", "bytes=0-7")]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
    // Hold the body WITHOUT polling it: that is the state a viewer is in
    // while the bytes are queued on their side of the connection.
    let body = resp.into_body();
    wait_ledger(&fx, "a.bin", &[(0, 8)]).await;
    assert_eq!(staged_segments(&fx, "a.bin").await, vec![(0, 8)]);

    // Idle long past the TTL: the sweep must spare what is being read.
    fx.state.cache.clock.advance(1_201_000);
    fx.state.cache.tick().await;
    assert_eq!(
        staged_segments(&fx, "a.bin").await,
        vec![(0, 8)],
        "a body in flight keeps its bytes past the inactivity TTL"
    );

    // The viewer goes away (the body is dropped), and the next sweep
    // window takes the bytes.
    drop(body);
    fx.state.cache.clock.advance(1_200_001);
    fx.state.cache.tick().await;
    assert!(staged_segments(&fx, "a.bin").await.is_empty(), "with no reader, idleness applies");
    assert_eq!(fx.state.cache.snapshot().await.segment_bytes, 0);
}

/// Every response holds its key's lease, whatever end of the pipe filled
/// it (ADR-0018's correction to ADR-0017). The longest streams in the
/// system are the upstream ones, and what they need protected is the KEY's
/// own staged bytes: a concurrent eviction pass neither knows nor cares
/// which end of the pipe filled this particular body.
#[tokio::test]
async fn an_upstream_sourced_body_holds_its_key() {
    let bytes: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
    // Grace 0, so the lease's own life is what is being asserted.
    let fx = base(&bytes).etag(Some("v1")).read_grace(0).build();
    // Through the read-only view (ADR-0022) rather than the lease table itself.
    assert!(!fx.state.cache.inspect("a.bin").await.leased, "nothing read yet");

    // A cold miss: the body IS the provider's stream, which is exactly the
    // case the source test used to exclude from protection.
    let resp = get_key(
        State(fx.state.clone()),
        Path("a.bin".into()),
        headers(&[("range", "bytes=0-2047")]),
        RawQuery(None),
        OriginalUri(DEFAULT_TEST_URI.clone()),
    )
    .await;
    let body = resp.into_body();
    assert!(
        fx.state.cache.inspect("a.bin").await.leased,
        "an upstream-served body must hold its key's lease"
    );

    // And the lease ends with the body: no grace here, so the drop is the
    // whole difference.
    drop(body);
    assert!(
        !fx.state.cache.inspect("a.bin").await.leased,
        "the lease ends when the body does"
    );
}

/// A request arriving at a live watch with no body on the key is counted:
/// that is the instrument that says whether the watch bought anything
/// (ADR-0018). The response it measures here is a fully staged one, so it
/// cost no upstream open — `hit`.
#[tokio::test]
async fn a_request_inside_the_watch_budget_is_counted_as_a_watch_resume() {
    let bytes: Vec<u8> = (0..40u8).collect();
    let fx = base(&bytes)
        .etag(Some("v1"))
        .coverage(4)
        .session_window(8)
        .max_size_bytes(1024)
        .build();
    let resp = get_key(
        State(fx.state.clone()),
        Path("a.bin".into()),
        headers(&[("range", "bytes=0-7")]),
        RawQuery(None),
        OriginalUri(DEFAULT_TEST_URI.clone()),
    )
    .await;
    body_text(resp).await;
    wait_ledger(&fx, "a.bin", &[(0, 8)]).await;

    let before = watch_resume_total("hit");
    // Five seconds later, same viewing session: nothing is streaming the
    // key, the watch is.
    fx.state.cache.clock.advance(5_000);
    let resp = get_key(
        State(fx.state.clone()),
        Path("a.bin".into()),
        headers(&[("range", "bytes=0-7")]),
        RawQuery(None),
        OriginalUri(DEFAULT_TEST_URI.clone()),
    )
    .await;
    let (status, _, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(body.as_bytes(), &bytes[0..8]);
    assert!(
        watch_resume_total("hit") - before >= 1.0,
        "the watch, not a live body, answered this request"
    );
}

/// Wait until a key has staged exactly `n` spans (the run's seal is
/// asynchronous, so the count is polled rather than sampled).
async fn wait_spans(fx: &Fixture, key: &str, n: usize) {
    for _ in 0..crate::testsupport::WAIT_TRIES {
        if staged_segments(fx, key).await.len() == n {
            // The seal lands before the driver marks its run terminal, and
            // a request that arrives in between finds a live run that does
            // not cover it and takes the standalone escape (one span of the
            // request's own length instead of a window). A real walk has the
            // same gap; here it is a few milliseconds wide.
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    panic!("{key} never reached {n} staged spans: {:?}", staged_segments(fx, key).await);
}

/// The LAB's watch account with a movable clock: two keys stage four
/// windows each against a magazine that holds one window's worth of four,
/// and one of them is being watched at its first window. Which windows does
/// the trim take?
///
/// This is the deterministic form of LAB section 12, and it exists because
/// the LAB's own numbers came back wrong: the trim took the window the
/// viewer was on. The pin is asserted here so the answer is not guesswork.
#[tokio::test]
async fn the_trim_takes_the_tail_and_leaves_the_window_the_viewer_is_on() {
    let bytes: Vec<u8> = (0..1_048_576u32).map(|i| (i % 251) as u8).collect();
    let fx = base(&bytes)
        .etag(Some("v1"))
        .coverage(4)
        .session_window(262_144)
        .max_size_bytes(1_048_576)
        .watch_pin(524_288)
        .watch_idle(900)
        .read_grace(0)
        .build();
    // Four windows for the watched key, one run at a time.
    for off in [0u64, 262_144, 524_288, 786_432] {
        stage(&fx, "a.bin", &format!("bytes={off}-{}", off + 65_535)).await;
        wait_spans(&fx, "a.bin", (off / 262_144 + 1) as usize).await;
    }
    // The viewer sits on the whole first window: the pin must be [0, 512 KiB).
    stage(&fx, "a.bin", "bytes=0-262143").await;
    let now = fx.state.cache.clock.now_millis();
    let pin = fx
        .state
        .cache
        .watches
        .pin("a.bin", now)
        .expect("a watched key has a pin");
    assert_eq!(
        (pin.start, pin.end, pin.anchor),
        (0, 524_288, 262_144),
        "the pin follows the last response, and that response was the whole first window"
    );

    // A second key fills the magazine.
    for off in [0u64, 262_144, 524_288, 786_432] {
        stage(&fx, "b.bin", &format!("bytes={off}-{}", off + 65_535)).await;
        wait_spans(&fx, "b.bin", (off / 262_144 + 1) as usize).await;
    }
    assert_eq!(
        fx.state.cache.snapshot().await.segment_bytes,
        2_097_152,
        "a={:?} b={:?}",
        staged_segments(&fx, "a.bin").await,
        staged_segments(&fx, "b.bin").await
    );

    // Past the minimum age, so the trim is allowed to look at both rows.
    fx.state.cache.clock.advance(120_000);
    fx.state.cache.tick().await;

    let a = staged_segments(&fx, "a.bin").await;
    let seg = fx.state.cache.snapshot().await.segment_bytes;
    assert!(
        seg <= 1_048_576,
        "the budget must be enforced while a viewer is watching (segment_bytes={seg})"
    );
    assert!(
        a.contains(&(262_144, 524_288)),
        "the window the viewer is about to need must survive the trim (a={a:?})"
    );
    assert!(
        !a.contains(&(524_288, 786_432)) && !a.contains(&(786_432, 1_048_576)),
        "the tail beyond the pin goes first (a={a:?})"
    );
    // (The order in which a pin is spent is pinned by the staging unit test
    // `a_spent_pin_gives_up_the_back_before_the_playhead`: this fixture has
    // no resident bytes, so its overshoot is exactly what the tail holds and
    // pass two never runs.)
}

/// A nocache node must not write anything, not even when a request fails.
/// A 404 used to fall out of the nocache arm into the ordinary path, which
/// installed a negative tombstone in redb — the exact thing this profile is
/// chosen to avoid (config's "nothing persists" contract).
#[tokio::test]
async fn a_nocache_404_writes_no_tombstone() {
    let fx = base(b"0123456789")
        .profile("nocache")
        .missing(true)
        .build();
    let resp = get_key(
        State(fx.state.clone()),
        Path("gone.bin".into()),
        headers(&[]),
        RawQuery(None),
        OriginalUri(DEFAULT_TEST_URI.clone()),
    )
    .await;
    let (status, _, _) = body_text(resp).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let snap = fx.state.cache.snapshot().await;
    assert_eq!(
        snap.entries, 0,
        "a nocache 404 must leave no row, not even a tombstone"
    );
    assert_eq!(snap.total_bytes, 0);
    assert_eq!(snap.segment_bytes, 0);
}

/// Router construction smoke test: every route path must survive
/// matchit's pattern compiler at runtime. The integration suite
/// calls handlers directly and never builds the Router — a blind
/// spot that let route-syntax panics surface only at deploy boot.
#[tokio::test]
async fn router_constructs_without_panic() {
    let fx = base(b"0123456789").build();
    let _app = router(fx.state.clone());
}

/// A lease and a watch for tests that drive `instrument_body` directly:
/// production takes both from the serve path, where the request's own
/// response range is known.
fn test_guards() -> (crate::cache::leases::LeaseGuard, crate::cache::watch::WatchGuard) {
    let clock = std::sync::Arc::new(MockClock::new(0));
    let leases = std::sync::Arc::new(crate::cache::leases::Leases::new(0));
    let watches = std::sync::Arc::new(crate::cache::watch::Watches::new(0, 0));
    (
        leases.acquire_at("test.bin", std::sync::Arc::clone(&clock)),
        watches.acquire_at("test.bin", (0, 1), clock),
    )
}

/// Read one counter's current value for a source label. The registry is
/// process-global and the tests share it, so callers assert on the
/// INCREASE, which concurrent increments can only make larger.
fn source_total(source: &str) -> f64 {
    prometheus::gather()
        .iter()
        .find(|f| f.get_name() == "cache_serve_source_total")
        .and_then(|f| {
            f.get_metric().iter().find(|m| {
                m.get_label()
                    .iter()
                    .any(|l| l.get_name() == "source" && l.get_value() == source)
            })
        })
        .map(|m| m.get_counter().get_value())
        .unwrap_or(0.0)
}

/// One `cache_watch_resume_total` outcome's current value. Process-global
/// registry, so callers assert on the increase.
fn watch_resume_total(outcome: &str) -> f64 {
    prometheus::gather()
        .iter()
        .find(|f| f.get_name() == "cache_watch_resume_total")
        .and_then(|f| {
            f.get_metric().iter().find(|m| {
                m.get_label()
                    .iter()
                    .any(|l| l.get_name() == "outcome" && l.get_value() == outcome)
            })
        })
        .map(|m| m.get_counter().get_value())
        .unwrap_or(0.0)
}

fn body_bytes(source: &str) -> f64 {
    prometheus::gather()
        .iter()
        .find(|f| f.get_name() == "cache_body_bytes_total")
        .and_then(|f| {
            f.get_metric().iter().find(|m| {
                m.get_label()
                    .iter()
                    .any(|l| l.get_name() == "source" && l.get_value() == source)
            })
        })
        .map(|m| m.get_counter().get_value())
        .unwrap_or(0.0)
}

/// The body wrapper must not change a byte, and it must count what it
/// delivered. `cache_serve_duration_seconds` stops at response
/// construction, so without this wrapper a viewer's actual transfer —
/// and its time to first byte — is invisible.
#[tokio::test]
async fn instrument_body_delivers_every_byte_and_counts_it() {
    use futures::StreamExt;

    let before = body_bytes("upstream");
    let chunks: Vec<Result<bytes::Bytes, std::io::Error>> = vec![
        Ok(bytes::Bytes::from_static(b"abc")),
        Ok(bytes::Bytes::from_static(b"de")),
    ];
    let body: crate::cache::flight::BodyStream = Box::pin(futures::stream::iter(chunks));
    let (lease, watch) = test_guards();
    let mut wrapped = instrument_body(body, "upstream", std::time::Instant::now(), lease, watch);

    let mut out = Vec::new();
    while let Some(chunk) = wrapped.next().await {
        out.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(out, b"abcde", "the wrapper must be transparent");
    assert!(
        body_bytes("upstream") - before >= 5.0,
        "every delivered byte must be counted against its source"
    );
}

/// hyper stops polling a body once its declared length is satisfied, so a
/// length-known response is DROPPED rather than driven to completion.
/// Counting at the end of the stream therefore never ran, and the counter
/// was absent from the node's /metrics while its sibling histogram was
/// there. Bytes are counted as they are yielded.
#[tokio::test]
async fn instrument_body_counts_bytes_even_when_the_stream_is_dropped() {
    use futures::StreamExt;

    let before = body_bytes("disk");
    let chunks: Vec<Result<bytes::Bytes, std::io::Error>> = vec![
        Ok(bytes::Bytes::from_static(b"abcd")),
        Ok(bytes::Bytes::from_static(b"efgh")),
    ];
    let body: crate::cache::flight::BodyStream = Box::pin(futures::stream::iter(chunks));
    let (lease, watch) = test_guards();
    let mut wrapped = instrument_body(body, "disk", std::time::Instant::now(), lease, watch);
    let first = wrapped.next().await.unwrap().unwrap();
    assert_eq!(&first[..], b"abcd");
    drop(wrapped); // exactly what hyper does once content-length is met
    assert!(
        body_bytes("disk") - before >= 4.0,
        "bytes delivered before the drop must already be counted"
    );
}

/// And the HTTP path must actually label its responses: the wrapper
/// only reports what `stream_response` told it. Only the positive claim
/// is asserted — the registry is process-global, so any "and not that
/// other source" assertion would race with the tests running beside
/// this one. Exact attribution is pinned by
/// `cache::cache::tests::serve_labels_the_bytes_with_their_source`,
/// which reads the plan instead of the counters.
#[tokio::test]
async fn http_get_records_its_byte_source() {
    let fx = base(b"0123456789").build();
    let before_disk = body_bytes("disk");
    prime(&fx, "a.bin").await;
    reset(&fx);
    let resp = get_key(
        State(fx.state.clone()),
        Path("a.bin".into()),
        headers(&[]),
        RawQuery(None),
        OriginalUri(DEFAULT_TEST_URI.clone()),
    )
    .await;
    let (status, _, body) = body_text(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "0123456789");
    assert!(
        body_bytes("disk") - before_disk >= 10.0,
        "a hit is disk bytes and must be counted as such"
    );
}
