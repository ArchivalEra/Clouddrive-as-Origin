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
    }
    .to_ascii_lowercase();
    host == "localhost" || host.starts_with("localhost.") || host.starts_with("127.") || host == "::1"
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

impl From<crate::key::KeyError> for BackendError {
    fn from(e: crate::key::KeyError) -> Self {
        BackendError::Other(format!("invalid key: {e}"))
    }
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

    /// Upstream id this backend serves (for logs and redb records).
    fn id(&self) -> &str;
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

    #[tokio::test]
    async fn mock_backend_roundtrip() {
        let b = MockBackend::new(b"hello", Some("abc".into()), Some("text/plain".into()));
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

    /// Reference mock used by cache tests until wiremock integration
    /// (also serves as the documentation example for implementors).
    pub struct MockBackend {
        bytes: Vec<u8>,
        etag: Option<String>,
        mime: Option<String>,
    }

    impl MockBackend {
        pub fn new(bytes: &[u8], etag: Option<String>, mime: Option<String>) -> Self {
            Self { bytes: bytes.to_vec(), etag, mime }
        }
    }

    #[async_trait]
    impl StorageBackend for MockBackend {
        async fn stat(&self, _key: &Key) -> Result<ObjectMeta, BackendError> {
            Ok(ObjectMeta {
                size_bytes: self.bytes.len() as u64,
                etag: self.etag.clone(),
                last_modified: None,
                mime_hint: self.mime.clone(),
            })
        }

        async fn open(&self, _key: &Key, range: Option<ByteRange>) -> Result<StreamSource, BackendError> {
            let slice: Vec<u8> = match range {
                None => self.bytes.clone(),
                Some(r) => {
                    let start = r.offset as usize;
                    if start > self.bytes.len() {
                        return Err(BackendError::Other("range out of bounds".into()));
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
                stream: Box::new(std::io::Cursor::new(slice)),
                total_len: Some(self.bytes.len() as u64),
            })
        }

        async fn refresh_if_needed(&self) -> Result<(), BackendError> {
            Ok(())
        }

        fn id(&self) -> &str {
            "mock"
        }
    }
}

// Re-export the mock for integration tests (cfg(test) modules in other
// crates cannot see it; integration tests use the lib target).
#[doc(hidden)]
#[cfg(test)]
pub use self::tests::MockBackend as TestMockBackend;

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Semaphore;

/// One configured upstream's runtime pieces: the provider backend plus
/// its Graph/Drive concurrency gate (spec §4: per-upstream ≤ N).
pub struct BackendSlot {
    pub backend: Arc<dyn StorageBackend>,
    pub gate: Arc<Semaphore>,
}

/// Registry of constructed backends, keyed by upstream id (from config).
/// Built once at boot from `[[upstreams]]`; business code resolves
/// `key → upstream id (routing) → BackendSlot (this registry)`.
#[derive(Default, Clone)]
pub struct BackendRegistry {
    slots: HashMap<String, Arc<BackendSlot>>,
}

impl BackendRegistry {
    pub fn new(slots: HashMap<String, Arc<BackendSlot>>) -> Self {
        Self { slots }
    }

    pub fn get(&self, upstream_id: &str) -> Option<Arc<BackendSlot>> {
        self.slots.get(upstream_id).cloned()
    }

    pub fn ids(&self) -> Vec<String> {
        self.slots.keys().cloned().collect()
    }
}
