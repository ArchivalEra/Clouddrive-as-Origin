pub mod openlist;

use async_trait::async_trait;
use tokio::io::AsyncRead;

pub use openlist::OpenListBackend;

/// A validated cache key (produced only by `crate::key::validate_key`).
/// Wrapping prevents backends from receiving raw, unvalidated paths.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Key(String);

impl Key {
    pub fn from_validated(validated: String) -> Self {
        Self(validated)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Byte range for ranged open: start offset + optional length.
/// `None` length = read to end of object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    pub offset: u64,
    pub length: Option<u64>,
}

impl ByteRange {
    pub fn from_offset(offset: u64) -> Self {
        Self { offset, length: None }
    }

    pub fn bounded(offset: u64, length: u64) -> Self {
        Self { offset, length: Some(length) }
    }

    /// Render as an HTTP `Range` header value (bytes=offset-).
    /// If `length` is set: bytes=offset-(offset+length-1).
    pub fn http_header_value(&self) -> String {
        match self.length {
            None => format!("bytes={}-", self.offset),
            Some(len) => format!("bytes={}-{}", self.offset, self.offset + len - 1),
        }
    }
}

/// Standardized remote-object metadata. Provider-agnostic: the business
/// plane never sees Drive/Graph shapes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub size_bytes: u64,
    /// ETag or content hash (Drive v3 has no etag: md5Checksum per G2 #11).
    pub etag: Option<String>,
    /// RFC 2822 / HTTP-date last-modified when the provider offers one.
    pub last_modified: Option<String>,
    /// Provider MIME hint — often generic (`application/octet-stream`);
    /// the business layer's extension table overrides generic values.
    pub mime_hint: Option<String>,
}

/// A viewer-facing direct download URL issued by the upstream (A relief
/// valve). The business plane 307s cold viewers here instead of proxying
/// bytes; any failure silently falls back to the water-pipe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectUrl {
    pub url: String,
}

/// A satisfiable byte range over a known total: pure data, no HTTP. The
/// single construction site for `Content-Range` rendering (C3) — cache
/// builds values, the response module formats them. Invariant: `first <=
/// last < total` (empty objects never carry a range).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentRange {
    pub first: u64,
    pub last: u64,
    pub total: u64,
}

/// One entry from a backend listing (ListObjectsV2 support).
/// `key` is the full key path relative to the upstream root: no leading
/// slash, never percent-encoded. Directory entries end with `/` and carry
/// `is_dir = true`, `size = 0`; file entries never end with `/`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListEntry {
    pub key: String,
    pub size: u64,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub is_dir: bool,
}

impl ContentRange {
    /// Render `bytes first-last/total` (206 responses).
    pub fn header_value(&self) -> String {
        format!("bytes {}-{}/{}", self.first, self.last, self.total)
    }

    /// Render `bytes */total` (416 responses).
    pub fn unsatisfiable(total: u64) -> String {
        format!("bytes */{total}")
    }
}

/// A readable byte stream with a known-or-unknown total length.
/// `total_len` is `Some` when the provider returned it (stat or
/// Content-Range total); the water-pipe uses it for `Content-Length`.
pub struct StreamSource {
    pub stream: Box<dyn AsyncRead + Send + Unpin>,
    pub total_len: Option<u64>,
}

/// Whether a URL may be handed to a viewer as a redirect target (A relief
/// valve choke point). Rules: absolute http(s) only, no userinfo
/// credentials (`user:pass@` must never reach a `Location` header), https
/// everywhere except loopback http (same trust argument as the upstream
/// URL policy — loopback plaintext is fine). Relative URLs fail.
pub fn redirect_target_allowed(url: &str) -> bool {
    let (scheme, rest) = match url.split_once("://") {
        Some(p) => p,
        None => return false,
    };
    let https = scheme.eq_ignore_ascii_case("https");
    let http = scheme.eq_ignore_ascii_case("http");
    if !https && !http {
        return false;
    }
    let auth = rest.split('/').next().unwrap_or("");
    if auth.is_empty() || auth.contains('@') {
        return false;
    }
    if https {
        return true;
    }
    let host = if let Some(s) = auth.strip_prefix('[') {
        s.split(']').next().unwrap_or("")
    } else {
        auth.split(':').next().unwrap_or("")
    };
    crate::net::is_loopback_host(host)
}

/// Unified backend error taxonomy — cache semantics (negative cache,
/// stale-if-error, backoff) key off these variants only.
#[derive(Debug, Clone, thiserror::Error)]
pub enum BackendError {
    #[error("object not found")]
    NotFound,
    /// Provider throttle. `retry_after_millis` when the provider sends
    /// Retry-After (Graph does; Drive does not — jittered backoff then).
    #[error("rate limited (retry_after_millis={retry_after_millis:?})")]
    RateLimited { retry_after_millis: Option<u64> },
    #[error("upstream server error: {0}")]
    ServerError(String),
    /// Credential invalid/revoked — needs re-auth; surfaced in healthz.
    #[error("authentication required (re-auth needed)")]
    AuthRequired,
    /// Client requested a Range beyond the object size (HTTP 416).
    #[error("range not satisfiable")]
    RangeNotSatisfiable,
    /// Includes SSRF allow-list rejections and malformed responses.
    #[error("backend error: {0}")]
    Other(String),
}

/// Unified storage-source abstraction (spec §5.1). The business plane
/// only knows virtual keys and `ObjectMeta` — never which cloud answered.
/// v1 providers: `googledrive` (first) and `onedrive` (Graph port).
#[async_trait]
pub trait StorageBackend: Send + Sync + 'static {
    /// Standard remote-object metadata (size, etag/hash, mtime, mime hint).
    async fn stat(&self, key: &Key) -> Result<ObjectMeta, BackendError>;

    /// Read-only byte stream; passes Range through to the source when
    /// the provider supports it (both Drive alt=media and Graph
    /// downloadUrl do).
    async fn open(&self, key: &Key, range: Option<ByteRange>) -> Result<StreamSource, BackendError>;

    /// Credential rotation + health probe (OAuth refresh, quota check).
    async fn refresh_if_needed(&self) -> Result<(), BackendError>;

    /// Resolve a viewer-facing direct URL for `key` (Tier 1 link-first:
    /// issue, normalize, and 1-byte-probe the upstream's direct link).
    /// Any `Err` — unsupported, self-referential, unprobed — means
    /// "proxy instead" (Tier 3); the caller never surfaces it.
    /// `viewer_ua` mimics the viewer on server-side probes. Default:
    /// unsupported (zero changes for existing backends).
    async fn direct_url(&self, _key: &Key, _viewer_ua: Option<&str>) -> Result<DirectUrl, BackendError> {
        Err(BackendError::Other("direct links not supported".into()))
    }

    /// List entries under `folder` — a validated directory path ending
    /// with `/`, or empty for the mount root. `recursive = false` returns
    /// the immediate children (files AND directories); `recursive = true`
    /// returns every file in the subtree and omits directories. The
    /// caller filters by prefix, sorts, folds, and paginates — the
    /// backend only does transport + href→key reconstruction. Default:
    /// unsupported (zero changes for existing backends).
    async fn list(&self, _folder: &str, _recursive: bool) -> Result<Vec<ListEntry>, BackendError> {
        Err(BackendError::Other("listing not supported by this backend".into()))
    }

    /// Upstream id this backend serves (for logs and redb records).
    fn id(&self) -> &str;
}

/// Retry policy for upstream calls (ticket #58). Derived from the config's
/// `retry_*` fields, which existed since the beginning but were never read
/// while three documents described the behaviour they were supposed to
/// drive (spec §Resilience, ADR-0002).
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_ms: u64,
    pub max_ms: u64,
}

impl RetryPolicy {
    /// No retries: one attempt. Used by tests and by callers that want the
    /// raw behaviour.
    pub fn none() -> Self {
        Self { max_attempts: 1, base_ms: 0, max_ms: 0 }
    }

    /// Whether another attempt is allowed after `attempt` (1-based).
    fn allows(&self, attempt: u32) -> bool {
        attempt < self.max_attempts.max(1)
    }

    /// Delay before attempt number `attempt + 1`. `Retry-After` is honored
    /// exactly when present; otherwise exponential backoff from `base_ms`,
    /// capped at `max_ms`, with additive jitter so a fleet of concurrent
    /// callers does not resynchronize onto the same retry instant.
    ///
    /// A `Retry-After` longer than `max_ms` is NOT truncated. Truncating
    /// would call the upstream back sooner than it asked, which is how a
    /// client earns a longer ban -- and the docs are explicit that the
    /// header is honored exactly. Instead `delay` reports it and
    /// [`Self::allows`-adjacent logic] declines to retry at all when the
    /// wait would exceed our ceiling: the upstream's wish is respected, and
    /// no permit is parked for an hour holding a caller hostage.
    fn delay(&self, attempt: u32, retry_after_millis: Option<u64>) -> std::time::Duration {
        if let Some(ms) = retry_after_millis {
            return std::time::Duration::from_millis(ms);
        }
        let exp = self.base_ms.saturating_mul(1u64 << attempt.saturating_sub(1).min(16));
        let capped = exp.min(self.max_ms);
        std::time::Duration::from_millis(capped.saturating_add(jitter_ms(capped)))
    }

    /// Whether a retry is worth attempting at all, given how long we would
    /// have to wait. Declining here is the alternative to truncating an
    /// upstream's long `Retry-After`: we neither hammer it early nor hold a
    /// gate permit for the whole wait.
    fn worth_waiting(&self, retry_after_millis: Option<u64>) -> bool {
        match retry_after_millis {
            // A ceiling of 0 means "no ceiling configured"; accept the wait.
            Some(ms) => self.max_ms == 0 || ms <= self.max_ms,
            None => true,
        }
    }
}

/// Additive jitter: up to 25% of `ms`, derived from the clock. Hand-rolled
/// deliberately -- one call site does not justify a `rand` dependency.
fn jitter_ms(ms: u64) -> u64 {
    if ms == 0 {
        return 0;
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    nanos % (ms / 4).max(1)
}

/// Whether an error is worth retrying. `NotFound`, `AuthRequired` and
/// `RangeNotSatisfiable` are deterministic -- a retry cannot change them --
/// so only throttling and provider failures are retried.
fn is_retryable(e: &BackendError) -> bool {
    matches!(e, BackendError::RateLimited { .. } | BackendError::ServerError(_))
}

/// Timing decorator (map #47 T2): wraps any `StorageBackend`, records each
/// call's duration into `backend_call_duration_seconds{op}`, and applies the
/// retry policy. Wired once at construction so all 14 call sites stay
/// untouched.
///
/// Note the cost, accepted deliberately: the per-upstream gate is acquired
/// by CALLERS, outside this decorator, so a retry loop holds one permit for
/// the whole backoff. `max_ms` is therefore the bound on how long a
/// throttled upstream can park a permit.
pub struct TimedBackend {
    inner: std::sync::Arc<dyn StorageBackend>,
    retry: RetryPolicy,
}

impl TimedBackend {
    pub fn new(inner: std::sync::Arc<dyn StorageBackend>) -> Self {
        Self { inner, retry: RetryPolicy::none() }
    }

    pub fn with_retry(inner: std::sync::Arc<dyn StorageBackend>, retry: RetryPolicy) -> Self {
        Self { inner, retry }
    }

    /// One observable operation with the retry policy applied.
    async fn attempt<T, F, Fut>(&self, op: &'static str, mut call: F) -> Result<T, BackendError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, BackendError>>,
    {
        let mut attempt = 1u32;
        loop {
            let out = crate::metrics::observe_backend(op, &mut call).await;
            match out {
                Ok(v) => return Ok(v),
                Err(e) => {
                    if !is_retryable(&e) || !self.retry.allows(attempt) {
                        return Err(e);
                    }
                    let retry_after = match &e {
                        BackendError::RateLimited { retry_after_millis } => *retry_after_millis,
                        _ => None,
                    };
                    if !self.retry.worth_waiting(retry_after) {
                        tracing::warn!(
                            op,
                            retry_after_ms = retry_after.unwrap_or(0),
                            max_ms = self.retry.max_ms,
                            error = %e,
                            "upstream asked to wait longer than our ceiling; not retrying"
                        );
                        return Err(e);
                    }
                    let delay = self.retry.delay(attempt, retry_after);
                    tracing::warn!(
                        op,
                        attempt,
                        delay_ms = delay.as_millis() as u64,
                        error = %e,
                        "upstream call failed; retrying"
                    );
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
            }
        }
    }
}

#[async_trait]
impl StorageBackend for TimedBackend {
    async fn stat(&self, key: &Key) -> Result<ObjectMeta, BackendError> {
        self.attempt("stat", || self.inner.stat(key)).await
    }

    async fn open(&self, key: &Key, range: Option<ByteRange>) -> Result<StreamSource, BackendError> {
        self.attempt("open", || self.inner.open(key, range)).await
    }

    async fn refresh_if_needed(&self) -> Result<(), BackendError> {
        self.attempt("refresh", || self.inner.refresh_if_needed()).await
    }

    async fn direct_url(&self, key: &Key, viewer_ua: Option<&str>) -> Result<DirectUrl, BackendError> {
        self.attempt("direct_url", || self.inner.direct_url(key, viewer_ua)).await
    }

    async fn list(&self, folder: &str, recursive: bool) -> Result<Vec<ListEntry>, BackendError> {
        self.attempt("list", || self.inner.list(folder, recursive)).await
    }

    fn id(&self) -> &str {
        self.inner.id()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_header_rendering() {
        assert_eq!(ByteRange::from_offset(0).http_header_value(), "bytes=0-");
        assert_eq!(ByteRange::from_offset(100).http_header_value(), "bytes=100-");
        assert_eq!(ByteRange::bounded(100, 50).http_header_value(), "bytes=100-149");
    }

    #[test]
    fn content_range_rendering() {
        assert_eq!(
            ContentRange { first: 0, last: 9, total: 443 }.header_value(),
            "bytes 0-9/443"
        );
        assert_eq!(ContentRange::unsatisfiable(443), "bytes */443");
    }

    #[test]
    fn redirect_target_policy() {
        // Foreign https: always fine.
        assert!(redirect_target_allowed("https://cdn.example.com/f.bin?sign=x"));
        // Credentials in URL: never (must not reach Location).
        assert!(!redirect_target_allowed("https://user:pass@cdn.example.com/f"));
        assert!(!redirect_target_allowed("https://user@cdn.example.com/f"));
        // Relative / non-http: never.
        assert!(!redirect_target_allowed("/p/f?d&sign=s"));
        assert!(!redirect_target_allowed("ftp://cdn.example.com/f"));
        assert!(!redirect_target_allowed("https://"));
        // Remote http: never (credentials and signatures in cleartext).
        assert!(!redirect_target_allowed("http://cdn.example.com/f"));
        // Loopback http: fine (same trust argument as upstream policy).
        assert!(redirect_target_allowed("http://127.0.0.1:8080/f"));
        assert!(redirect_target_allowed("http://localhost:5244/f"));
        assert!(redirect_target_allowed("http://[::1]:8080/f"));
    }

    /// A host that only STARTS with a loopback name is a host the upstream
    /// controls, and allowing plaintext http to it would leak the query
    /// string's signature. The upstream policy has the mirror of this test,
    /// and both hold because they call one shared predicate rather than
    /// two copies of the check.
    #[test]
    fn redirect_target_rejects_hosts_that_merely_look_loopback() {
        for url in [
            "http://127.evil.com/f",
            "http://localhost.evil.com/f",
            "http://127.0.0.1.evil.com/f",
            "http://localhosts/f",
        ] {
            assert!(!redirect_target_allowed(url), "{url} must not be an allowed target");
        }
    }

    #[tokio::test]
    async fn mock_backend_roundtrip() {
        let b = crate::testsupport::MockBackend::new(b"hello", Some("abc".into()), Some("text/plain".into()));
        let key = Key::from_validated("a.txt".into());
        let meta = b.stat(&key).await.unwrap();
        assert_eq!(meta.size_bytes, 5);
        assert_eq!(meta.etag.as_deref(), Some("abc"));
        let src = b.open(&key, Some(ByteRange::from_offset(1))).await.unwrap();
        assert_eq!(src.total_len, Some(5));
        let mut s = src.stream;
        let mut buf = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut s, &mut buf).await.unwrap();
        assert_eq!(buf, b"ello");
    }

}

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Semaphore;

/// One configured upstream's runtime pieces: the provider backend plus its
/// per-upstream concurrency gates (spec §4: per-upstream ≤ N).
///
/// Two gates, deliberately separate (B1). The measurement that justified
/// the split: with a single shared gate, a HEAD on one key took **14.2 s**
/// while three long cold pulls held the permits, versus **22 ms** idle —
/// metadata operations were queueing behind byte-moving transfers. Metadata
/// (stat/HEAD/list/direct_url) and streams (cold-miss pumps, passthrough
/// staging, promotion assembly) now have independent budgets, so a long
/// download can never starve a HEAD.
pub struct BackendSlot {
    pub backend: Arc<dyn StorageBackend>,
    /// Metadata gate: concurrent stat / HEAD / list / link lookups.
    pub gate: Arc<Semaphore>,
    /// Stream gate: concurrent byte-moving transfers.
    pub stream_gate: Arc<Semaphore>,
}

impl BackendSlot {
    /// Both gates start at the configured per-upstream concurrency.
    pub fn new(backend: Arc<dyn StorageBackend>, concurrency: usize) -> Self {
        Self {
            backend,
            gate: Arc::new(Semaphore::new(concurrency)),
            stream_gate: Arc::new(Semaphore::new(concurrency)),
        }
    }
}

/// Registry of constructed backends, keyed by upstream id (from config).
/// Built once at boot from `[[upstreams]]`; business code resolves
/// `key → upstream id (routing) → BackendSlot (this registry)`.
#[derive(Default, Clone)]
pub struct BackendRegistry {
    slots: HashMap<String, Arc<BackendSlot>>,
    /// Precomputed id list (the hot path only needs membership tests).
    ids: Arc<[String]>,
}

impl BackendRegistry {
    pub fn new(slots: HashMap<String, Arc<BackendSlot>>) -> Self {
        let ids: Arc<[String]> = slots.keys().cloned().collect::<Vec<_>>().into();
        Self { slots, ids }
    }

    pub fn get(&self, upstream_id: &str) -> Option<Arc<BackendSlot>> {
        self.slots.get(upstream_id).cloned()
    }

    /// Upstream ids, cached at construction (P7): resolution tests bucket
    /// membership on every request, so building a fresh `Vec<String>` each
    /// time was a per-request allocation proportional to the pool size.
    pub fn ids(&self) -> Arc<[String]> {
        self.ids.clone()
    }

    /// Borrowed form for the hot resolution path (no refcount traffic).
    pub fn ids_slice(&self) -> &[String] {
        &self.ids
    }
}

#[cfg(test)]
mod retry_tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A backend that fails a scripted number of times, then succeeds.
    struct FlakyBackend {
        fails_left: AtomicU32,
        calls: AtomicU32,
        error: BackendError,
    }

    #[async_trait::async_trait]
    impl StorageBackend for FlakyBackend {
        async fn stat(&self, _k: &Key) -> Result<ObjectMeta, BackendError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fails_left.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1)).is_ok() {
                return Err(self.error.clone());
            }
            Ok(ObjectMeta { size_bytes: 1, etag: None, last_modified: None, mime_hint: None })
        }
        async fn open(&self, _k: &Key, _r: Option<ByteRange>) -> Result<StreamSource, BackendError> {
            unreachable!("not exercised")
        }
        async fn refresh_if_needed(&self) -> Result<(), BackendError> {
            Ok(())
        }
        fn id(&self) -> &str {
            "flaky"
        }
    }

    fn key() -> Key {
        Key::from_validated("k".into())
    }

    #[tokio::test]
    async fn retries_throttling_then_succeeds() {
        let inner = std::sync::Arc::new(FlakyBackend {
            fails_left: AtomicU32::new(2),
            calls: AtomicU32::new(0),
            error: BackendError::RateLimited { retry_after_millis: Some(1) },
        });
        let backend = TimedBackend::with_retry(
            inner.clone(),
            RetryPolicy { max_attempts: 4, base_ms: 1, max_ms: 10 },
        );
        let out = backend.stat(&key()).await;
        assert!(out.is_ok(), "throttling must be retried until it succeeds");
        assert_eq!(inner.calls.load(Ordering::SeqCst), 3, "2 failures + 1 success");
    }

    #[tokio::test]
    async fn retries_provider_errors_up_to_the_attempt_cap() {
        let inner = std::sync::Arc::new(FlakyBackend {
            fails_left: AtomicU32::new(100), // always fails
            calls: AtomicU32::new(0),
            error: BackendError::ServerError("boom".into()),
        });
        let backend = TimedBackend::with_retry(
            inner.clone(),
            RetryPolicy { max_attempts: 3, base_ms: 1, max_ms: 5 },
        );
        assert!(backend.stat(&key()).await.is_err());
        assert_eq!(
            inner.calls.load(Ordering::SeqCst),
            3,
            "must stop at max_attempts, not loop forever"
        );
    }

    /// Deterministic failures must NOT be retried: a retry cannot change
    /// them and only burns time and upstream quota.
    #[tokio::test]
    async fn deterministic_errors_are_not_retried() {
        for err in [
            BackendError::NotFound,
            BackendError::AuthRequired,
            BackendError::RangeNotSatisfiable,
        ] {
            let inner = std::sync::Arc::new(FlakyBackend {
                fails_left: AtomicU32::new(100),
                calls: AtomicU32::new(0),
                error: err.clone(),
            });
            let backend = TimedBackend::with_retry(
                inner.clone(),
                RetryPolicy { max_attempts: 4, base_ms: 1, max_ms: 5 },
            );
            assert!(backend.stat(&key()).await.is_err());
            assert_eq!(
                inner.calls.load(Ordering::SeqCst),
                1,
                "{err:?} must not be retried"
            );
        }
    }

    /// An upstream asking for a longer wait than our ceiling must not be
    /// retried at all: no permit parked, no early callback.
    #[tokio::test]
    async fn oversized_retry_after_declines_the_retry() {
        let inner = std::sync::Arc::new(FlakyBackend {
            fails_left: AtomicU32::new(100),
            calls: AtomicU32::new(0),
            error: BackendError::RateLimited { retry_after_millis: Some(3_600_000) },
        });
        let backend = TimedBackend::with_retry(
            inner.clone(),
            RetryPolicy { max_attempts: 4, base_ms: 1, max_ms: 5_000 },
        );
        let started = std::time::Instant::now();
        assert!(backend.stat(&key()).await.is_err());
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1, "must not wait out an hour");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(1),
            "must return immediately, not sleep the Retry-After"
        );
    }

    /// The default policy (no explicit config) makes exactly one attempt,
    /// so existing behaviour is unchanged where retry is not configured.
    #[tokio::test]
    async fn default_policy_makes_one_attempt() {
        let inner = std::sync::Arc::new(FlakyBackend {
            fails_left: AtomicU32::new(100),
            calls: AtomicU32::new(0),
            error: BackendError::ServerError("boom".into()),
        });
        let backend = TimedBackend::new(inner.clone());
        assert!(backend.stat(&key()).await.is_err());
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
    }

    /// `Retry-After` is honored exactly when we retry, and a value beyond
    /// our ceiling makes us decline the retry instead of truncating it.
    #[test]
    fn retry_after_is_honored_and_oversized_waits_are_declined() {
        let p = RetryPolicy { max_attempts: 4, base_ms: 100, max_ms: 5_000 };
        assert_eq!(p.delay(1, Some(250)).as_millis(), 250, "honored exactly");
        assert_eq!(
            p.delay(1, Some(3_600_000)).as_millis(),
            3_600_000,
            "not truncated: truncating would call back sooner than asked"
        );
        // The ceiling is enforced by declining the retry, not by shortening
        // the wait: no permit is parked for an hour.
        assert!(p.worth_waiting(Some(250)));
        assert!(p.worth_waiting(None));
        assert!(!p.worth_waiting(Some(3_600_000)), "a 1h Retry-After must not be waited out");
        assert!(p.worth_waiting(Some(5_000)), "the boundary itself is allowed");
        // Without Retry-After: exponential from base, jittered, capped.
        let d1 = p.delay(1, None).as_millis();
        assert!((100..=125).contains(&d1), "base + up to 25% jitter, got {d1}");
        let capped = p.delay(20, None).as_millis();
        assert!((5_000..=6_250).contains(&capped), "capped at max_ms + jitter, got {capped}");
    }
}
