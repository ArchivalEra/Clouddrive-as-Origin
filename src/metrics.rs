//! Process-wide metrics for the business plane (map #47 T2).
//!
//! These register into the prometheus default registry, which is
//! process-global — so the front crate's `prometheus_http_service()`
//! serves them on the same `/metrics` endpoint with no extra listener
//! and no cross-crate plumbing (both crates live in one binary and
//! resolve to the same prometheus 0.13.4).
//!
//! Cardinality is bounded by construction: labels are operation names
//! and cache outcomes only — never keys, paths, or IPs.

use std::sync::LazyLock;
use std::time::Instant;

use prometheus::{
    register_histogram_vec, register_int_counter, register_int_counter_vec, register_int_gauge,
    HistogramVec, IntCounter, IntCounterVec, IntGauge,
};

/// Same bucket span as the front plane (1 ms through 3 min).
pub const LATENCY_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 180.0,
];

/// Time spent inside one upstream backend call, by operation. `open`
/// covers both full and ranged GETs — a cold parallel pull contributes
/// one sample per 5 MB segment, so per-segment slowness is visible
/// rather than hidden in a total.
pub static BACKEND_CALL: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "backend_call_duration_seconds",
        "upstream backend call duration by operation",
        &["op"],
        LATENCY_BUCKETS.to_vec()
    )
    .expect("register backend_call_duration_seconds")
});

/// Time the cache layer spent BUILDING a response, by outcome. `outcome`
/// values are the `CacheOutcome` variants plus the two non-cache serve
/// modes (`nocache`, `passthrough`).
///
/// Read this as "how long until the headers were ready", not as how long
/// the viewer waited: every observed function returns an unconsumed body,
/// so the byte transfer is not inside the sample. [`BODY_TTFB`] is the
/// instrument for the viewer's actual wait.
pub static CACHE_SERVE: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "cache_serve_duration_seconds",
        "cache response construction duration by outcome",
        &["outcome"],
        LATENCY_BUCKETS.to_vec()
    )
    .expect("register cache_serve_duration_seconds")
});

/// Responses by where their bytes come from (`disk` or `upstream`), counted
/// when the response is built. This is the counter that answers "how much of
/// this workload are we serving ourselves?" — the question that decides
/// whether a bigger local read path is worth building.
pub static SERVE_SOURCE: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "cache_serve_source_total",
        "cache responses by byte source",
        &["source"]
    )
    .expect("register cache_serve_source_total")
});

/// Time from the request entering the handler to the FIRST body byte
/// reaching the viewer, by source. This — not `cache_serve_duration_seconds`
/// — is what says whether seeking is smooth: a seek served from disk or from
/// staged sidecars costs no upstream round trip, while one served from
/// upstream pays the per-open cost.
pub static BODY_TTFB: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "cache_body_ttfb_seconds",
        "time from request entry to the first body byte, by source",
        &["source"],
        LATENCY_BUCKETS.to_vec()
    )
    .expect("register cache_body_ttfb_seconds")
});

/// Bytes actually delivered to viewers, by source: the bandwidth ledger that
/// pairs with [`SERVE_SOURCE`].
pub static BODY_BYTES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "cache_body_bytes_total",
        "body bytes delivered to viewers, by source",
        &["source"]
    )
    .expect("register cache_body_bytes_total")
});

/// Staged-read runs by outcome (ADR-0016). One run is one upstream `open`
/// covering a whole window, shared by however many readers fall inside it, so
/// this counter divided by `backend_call_duration_seconds_count{op="open"}`
/// is the shaping account: how many requests each paid open bought.
pub static SESSION: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "cache_session_total",
        "staged-read runs by outcome",
        &["outcome"]
    )
    .expect("register cache_session_total")
});

/// How each ranged request was served: `attached` (rode a live run's
/// watermark, no upstream open and no stream permit) or `standalone` (opened
/// its own exact Range — no admission, a far seek, or a run already starting).
pub static SESSION_READER: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "cache_session_reader_total",
        "ranged requests by how the run served them",
        &["result"]
    )
    .expect("register cache_session_reader_total")
});

/// Keys being watched right now, and the bytes their pins cover (ADR-0018).
/// A watch is a viewing session that outlives its response bodies, so
/// `cache_watch_active` is the number of viewers the cache is holding a
/// neighbourhood for, and `cache_watch_pinned_bytes` is what that costs the
/// magazine's budget. Read them together: pinning that grows while the active
/// count falls is how a budget goes soft.
pub static WATCH_ACTIVE: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "cache_watch_active",
        "cache keys currently watched (a viewing session inside its idle budget)"
    )
    .expect("register cache_watch_active")
});

pub static WATCH_PINNED_BYTES: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!("cache_watch_pinned_bytes", "bytes covered by live watch pins")
        .expect("register cache_watch_pinned_bytes")
});

/// Responses arriving at a watched key with no body streaming it — the watch,
/// not a live body, is what the cache answered from. `hit` = answered from
/// this node's bytes with no upstream open; `miss` = it still cost a fetch.
/// An EdgeOne shard walk contributes one per shard (the origin's bodies for
/// consecutive shards do not overlap), so read this as "how often the watch
/// bought something", not as a pause counter.
pub static WATCH_RESUME: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "cache_watch_resume_total",
        "responses arriving at a watched key with no body streaming it, by whether they cost an upstream open",
        &["outcome"]
    )
    .expect("register cache_watch_resume_total")
});

/// Keys whose object the magazine can never hold whole (ADR-0019), i.e. the
/// keys on a bounded working window, and the bytes that rule has freed.
///
/// The pair answers "what is this class of object costing us": the count says
/// how many large objects have staged bytes right now, the counter says how
/// much the cap has reclaimed. There is deliberately no byte GAUGE — the ledger
/// under-reports staged bytes by design (`segment_bytes` is the exact total,
/// and it is already published), so a per-class byte gauge would be a number
/// that disagrees with the truth.
pub static TRANSIENT_KEYS: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "cache_unkeepable_keys",
        "keys whose object is larger than the retention budget (bounded working window)"
    )
    .expect("register cache_unkeepable_keys")
});

pub static TRANSIENT_TRIMMED_BYTES: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter!(
        "cache_unkeepable_trim_bytes_total",
        "bytes reclaimed from un-keepable keys by their working-window cap"
    )
    .expect("register cache_unkeepable_trim_bytes_total")
});

/// Publish how many keys are on a bounded working window (the tick's account).
pub fn set_unkeepable_keys(n: usize) {
    TRANSIENT_KEYS.set(n as i64);
}

/// Record bytes reclaimed by a working-window cap.
pub fn observe_transient_trim(bytes: u64) {
    TRANSIENT_TRIMMED_BYTES.inc_by(bytes);
}

/// Observe one backend call. The closure runs the actual call; we time
/// around it so callers stay one-line.
pub async fn observe_backend<T, F, Fut>(op: &'static str, f: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let start = Instant::now();
    let out = f().await;
    BACKEND_CALL
        .with_label_values(&[op])
        .observe(start.elapsed().as_secs_f64());
    out
}

/// Record a cache-serve duration sample.
pub fn observe_serve(outcome: &str, start: Instant) {
    CACHE_SERVE
        .with_label_values(&[outcome])
        .observe(start.elapsed().as_secs_f64());
}

/// Record one built response against its byte source.
pub fn observe_source(source: &str) {
    SERVE_SOURCE.with_label_values(&[source]).inc();
}

/// Record the wait for the first body byte of one response.
pub fn observe_body_ttfb(source: &str, start: Instant) {
    BODY_TTFB
        .with_label_values(&[source])
        .observe(start.elapsed().as_secs_f64());
}

/// Record bytes delivered to a viewer.
pub fn observe_body_bytes(source: &str, bytes: u64) {
    BODY_BYTES.with_label_values(&[source]).inc_by(bytes);
}

/// Record one staged-read run's outcome.
pub fn observe_session(outcome: &str) {
    SESSION.with_label_values(&[outcome]).inc();
}

/// Record how one ranged request was served by the run machinery.
pub fn observe_session_reader(result: &str) {
    SESSION_READER.with_label_values(&[result]).inc();
}

/// Publish the watch gauges (the cache tick is their single writer).
pub fn set_watch(active: usize, pinned_bytes: u64) {
    WATCH_ACTIVE.set(active as i64);
    WATCH_PINNED_BYTES.set(pinned_bytes as i64);
}

/// Record how a response that arrived at an idle watch was served: `hit` when
/// this node's own bytes answered it, `miss` when it cost an upstream open.
pub fn observe_watch_resume(outcome: &str) {
    WATCH_RESUME.with_label_values(&[outcome]).inc();
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::Encoder;

    fn rendered() -> String {
        let families = prometheus::gather();
        let mut buf = vec![];
        prometheus::TextEncoder::new()
            .encode(&families, &mut buf)
            .unwrap();
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn backend_and_cache_histograms_exposed() {
        BACKEND_CALL.with_label_values(&["stat"]).observe(0.012);
        BACKEND_CALL.with_label_values(&["open"]).observe(1.4);
        let t = Instant::now();
        observe_serve("Hit", t);
        let text = rendered();
        // Presence only — exact counts race with other tests sharing the
        // process-global registry.
        assert!(text.contains(r#"backend_call_duration_seconds_count{op="stat"}"#));
        assert!(text.contains(r#"backend_call_duration_seconds_count{op="open"}"#));
        assert!(text.contains(r#"cache_serve_duration_seconds_count{outcome="Hit"}"#));
        assert!(text.contains("backend_call_duration_seconds_bucket"));
        assert!(text.contains("cache_serve_duration_seconds_sum"));
    }

    #[test]
    fn labels_are_bounded() {
        let text = rendered();
        for line in text.lines().filter(|l| l.starts_with("backend_") || l.starts_with("cache_")) {
            for forbidden in ["key=", "path=", "ip=", "addr="] {
                assert!(!line.contains(forbidden), "high-cardinality {forbidden} in {line}");
            }
        }
    }

    /// The working-window account (ADR-0019) exists and is named for the
    /// mechanism, not for any kind of content.
    #[test]
    fn unkeepable_account_is_exposed() {
        set_unkeepable_keys(3);
        observe_transient_trim(4096);
        let text = rendered();
        assert!(text.contains("cache_unkeepable_keys 3"), "{text}");
        assert!(text.contains("cache_unkeepable_trim_bytes_total 4096"), "{text}");
        for forbidden in ["video", "media", "mp4", "player"] {
            assert!(
                !text.contains(forbidden),
                "the metric names must stay general-purpose, found {forbidden}"
            );
        }
    }

    /// The body metrics exist and carry only the closed source label set.
    #[test]
    fn session_metrics_are_exposed() {
        observe_session("sealed");
        observe_session_reader("attached");
        observe_watch_resume("hit");
        set_watch(2, 4096);
        let text = rendered();
        assert!(text.contains("cache_session_total"), "{text}");
        assert!(text.contains("cache_session_reader_total"), "{text}");
        assert!(text.contains("cache_watch_resume_total"), "{text}");
        assert!(text.contains(r#"cache_watch_active 2"#), "{text}");
        assert!(text.contains(r#"cache_watch_pinned_bytes 4096"#), "{text}");
    }

    #[test]
    fn body_metrics_are_exposed_by_source() {
        observe_source("disk");
        observe_body_bytes("upstream", 4096);
        observe_body_ttfb("upstream", Instant::now());
        let text = rendered();
        assert!(text.contains(r#"cache_serve_source_total{source="disk"}"#));
        assert!(text.contains(r#"cache_body_bytes_total{source="upstream"}"#));
        assert!(text.contains(r#"cache_body_ttfb_seconds_count{source="upstream"}"#));
        assert!(text.contains("cache_body_ttfb_seconds_bucket"));
    }
}
