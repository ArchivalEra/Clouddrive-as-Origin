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
    register_histogram_vec, register_int_counter_vec, HistogramVec, IntCounterVec,
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

    /// The body metrics exist and carry only the closed source label set.
    #[test]
    fn session_metrics_are_exposed() {
        observe_session("sealed");
        observe_session_reader("attached");
        let text = prometheus::gather()
            .iter()
            .map(|f| f.get_name().to_string() + "\n")
            .collect::<String>();
        assert!(text.contains("cache_session_total"), "{text}");
        assert!(text.contains("cache_session_reader_total"), "{text}");
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
