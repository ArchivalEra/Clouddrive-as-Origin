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

use prometheus::{register_histogram_vec, HistogramVec};

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

/// Time the cache layer spent serving a request, by outcome. `outcome`
/// values are the `CacheOutcome` variants plus the two non-cache serve
/// modes (`nocache`, `passthrough`).
pub static CACHE_SERVE: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "cache_serve_duration_seconds",
        "cache serve duration by outcome",
        &["outcome"],
        LATENCY_BUCKETS.to_vec()
    )
    .expect("register cache_serve_duration_seconds")
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
}
