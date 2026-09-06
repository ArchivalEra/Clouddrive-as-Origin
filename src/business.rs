use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde_json::json;
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::{
    backend::{BackendError, ByteRange},
    cache::cache::{Cache, CacheHit, CacheOutcome, HitMeta},
    clock::Clock,
    config::Config,
    key::validate_key,
};

#[derive(Clone)]
pub struct AppState<C: Clock + Clone> {
    pub cache: Arc<Cache<C>>,
    pub config: Arc<Config>,
}

// ---------------------------------------------------------------------------
// S3 response shaping (R1 table). The outbound speaks AWS S3 shapes over the
// disk cache: quoted ETags, S3 XML error envelope, Accept-Ranges, request
// ids mirrored into errors, path-style bucket alias. Deliberate deviation:
// Cache-Control stays `public, max-age=..., immutable` (no per-object stored
// value exists) because edge caching matters more here than S3 purity.
// ---------------------------------------------------------------------------

static REQUEST_SEQ: AtomicU64 = AtomicU64::new(1);

/// Per-request opaque ids: 16-hex-upper request id + 76-char extended id.
/// Uniqueness per request is sufficient; formats only need opaque ASCII.
fn request_ids() -> (String, String) {
    const B64: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789/+:";
    let n = REQUEST_SEQ.fetch_add(1, Ordering::Relaxed);
    let t = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(n as u128);
    let mut x = (n as u128).wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(t);
    let req_id = format!("{:016X}", (x & (u64::MAX as u128)) as u64);
    let mut host = String::with_capacity(76);
    for _ in 0..76 {
        // xorshift128-style stir; low 6 bits index the alphabet.
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        host.push(B64[(x & 63) as usize] as char);
    }
    (req_id, host)
}

/// S3 ETags are always double-quoted opaque tags, never `W/`-prefixed.
/// Multipart `-N` suffixes pass through verbatim.
fn quote_etag(etag: &str) -> String {
    let t = etag.trim();
    let bare = t.strip_prefix("W/").unwrap_or(t);
    if bare.len() >= 2 && bare.starts_with('"') && bare.ends_with('"') {
        bare.to_string()
    } else {
        format!("\"{bare}\"")
    }
}

fn s3_error_xml(code: &str, message: &str, resource: &str, req_id: &str, host_id: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <Error><Code>{code}</Code><Message>{message}</Message>\
         <Resource>{resource}</Resource><RequestId>{req_id}</RequestId>\
         <HostId>{host_id}</HostId></Error>"
    )
}

/// S3 resource path for error envelopes: the full request path
/// (`/{bucket}/{key}` in alias form, `/<key>` legacy).
fn resource_path(key: &str) -> String {
    format!("/{key}")
}

// ---------------------------------------------------------------------------
// Range parsing (R1: single 206, suffix support, multi/malformed → 416).
// ---------------------------------------------------------------------------

enum ClientRange {
    Absent,
    Single(ByteRange),
    /// Suffix request `bytes=-N` (N >= 1; `bytes=-0` is malformed → 416).
    Suffix(u64),
    Multi,
}

/// Parse the Range header. `Err` = syntactically malformed → 416
/// InvalidRange (AWS behavior; a server MAY ignore Range, S3 does not).
fn parse_client_range(headers: &HeaderMap) -> Result<ClientRange, ()> {
    let raw = match headers.get("range").and_then(|v| v.to_str().ok()) {
        None => return Ok(ClientRange::Absent),
        Some(r) => r,
    };
    let parsed = http_range_header::parse_range_header(raw).map_err(|_| ())?;
    if parsed.ranges.len() > 1 {
        // S3 has no multipart/byteranges: reject, do not coalesce.
        return Ok(ClientRange::Multi);
    }
    let r = &parsed.ranges[0];
    match (r.start, r.end) {
        (http_range_header::StartPosition::FromLast(n), _) => Ok(ClientRange::Suffix(n)),
        (http_range_header::StartPosition::Index(s), http_range_header::EndPosition::LastByte) => {
            Ok(ClientRange::Single(ByteRange::from_offset(s)))
        }
        (http_range_header::StartPosition::Index(s), http_range_header::EndPosition::Index(e)) => {
            if s > e {
                // Reversed (`bytes=100-50`): unsatisfiable. ByteRange cannot
                // express it (length would underflow), so reject here.
                return Err(());
            }
            Ok(ClientRange::Single(ByteRange::bounded(s, e - s + 1)))
        }
    }
}

// ---------------------------------------------------------------------------
// Bucket alias: bucket namespace = upstream ids, zero new config.
// `/{bucket}/{rest}` with a known bucket pins the upstream; anything else
// falls through to legacy prefix-table routing.
// ---------------------------------------------------------------------------

fn split_bucket<'a>(path: &'a str, ids: &[String]) -> Option<(&'a str, &'a str)> {
    let (first, rest) = path.split_once('/')?;
    if rest.is_empty() || !ids.iter().any(|id| id == first) {
        return None;
    }
    Some((first, rest))
}

// ---------------------------------------------------------------------------
// Shared response builders.
// ---------------------------------------------------------------------------

/// S3 metadata headers shared by GET and HEAD.
fn s3_meta_headers(
    builder: axum::http::response::Builder,
    meta: &HitMeta,
    req_id: &str,
    host_id: &str,
) -> axum::http::response::Builder {
    let mut b = builder
        .header("accept-ranges", "bytes")
        .header("x-amz-request-id", req_id)
        .header("x-amz-id-2", host_id)
        // Deviation (documented): no per-object stored value exists, and
        // edge caching outranks S3 purity here.
        .header("cache-control", "public, max-age=31536000, immutable");
    if let Some(ct) = &meta.content_type {
        b = b.header("content-type", ct);
    } else {
        b = b.header("content-type", "binary/octet-stream");
    }
    if let Some(et) = &meta.etag {
        b = b.header("etag", quote_etag(et));
    }
    if let Some(lm) = &meta.last_modified {
        b = b.header("last-modified", lm);
    }
    b
}

#[allow(clippy::too_many_arguments)]
fn error_response(
    e: BackendError,
    key: &str,
    req_id: &str,
    host_id: &str,
    head_only: bool,
    size_hint: Option<u64>,
) -> Response {
    let resource = resource_path(key);
    let xml = |code: &str, message: &str| s3_error_xml(code, message, &resource, req_id, host_id);
    let with_ids = |status: StatusCode, body: Body| {
        let mut resp = Response::builder()
            .status(status)
            .header("x-amz-request-id", req_id)
            .header("x-amz-id-2", host_id)
            .body(body)
            .unwrap();
        resp.headers_mut().insert("content-type", "application/xml".parse().unwrap());
        resp
    };
    match e {
        BackendError::NotFound => {
            let body = if head_only { Body::empty() } else { Body::from(xml("NoSuchKey", "The specified key does not exist.")) };
            with_ids(StatusCode::NOT_FOUND, body)
        }
        BackendError::RangeNotSatisfiable => {
            let mut resp = with_ids(
                StatusCode::RANGE_NOT_SATISFIABLE,
                if head_only { Body::empty() } else { Body::from(xml("InvalidRange", "The requested range is not satisfiable.")) },
            );
            if let Some(size) = size_hint {
                resp.headers_mut().insert(
                    "content-range",
                    format!("bytes */{size}").parse().unwrap(),
                );
            }
            resp
        }
        BackendError::RateLimited { retry_after_millis } => {
            let mut resp = (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "upstream rate limited"})),
            )
                .into_response();
            if let Some(ms) = retry_after_millis {
                if let Ok(v) = axum::http::HeaderValue::from_str(&(ms / 1000).to_string()) {
                    resp.headers_mut().insert("retry-after", v);
                }
            }
            resp
        }
        BackendError::Other(msg)
            if msg.contains("traversal") || msg.contains("Empty") || msg.contains("Absolute") =>
        {
            let body = if head_only { Body::empty() } else { Body::from(xml("InvalidRequest", "The request key is invalid.")) };
            with_ids(StatusCode::BAD_REQUEST, body)
        }
        other => {
            warn!(key = %key, error = %other, "cache fetch error");
            (StatusCode::BAD_GATEWAY, Json(json!({"error": "upstream error"}))).into_response()
        }
    }
}

/// Resolve the fetch target: bucket alias (pinned upstream + stripped
/// backend path) or legacy full path. Returns the full request path plus
/// the alias split when the first segment names a known upstream.
/// Cache identity is always the full path (prevents cross-upstream flight
/// and entry collisions); the backend sees the stripped path.
fn resolve_target<C: Clock + Clone>(
    state: &AppState<C>,
    path_key: &str,
) -> (String, Option<(String, String)>) {
    let ids = state.cache.backends.ids();
    if let Some((bucket, rest)) = split_bucket(path_key, &ids) {
        return (path_key.to_string(), Some((rest.to_string(), bucket.to_string())));
    }
    (path_key.to_string(), None)
}

async fn fetch_key<C: Clock + Clone>(
    state: &AppState<C>,
    full: String,
    pinned: Option<(String, String)>,
    range: Option<ByteRange>,
) -> Result<CacheHit, BackendError> {
    match pinned {
        Some((backend, upstream)) => state.cache.get_pinned(full, backend, upstream, range).await,
        None => state.cache.get(&full, range).await,
    }
}

async fn head_lookup<C: Clock + Clone>(
    state: &AppState<C>,
    full: &str,
    pinned: &Option<(String, String)>,
) -> Result<HitMeta, BackendError> {
    match pinned {
        Some((backend, upstream)) => {
            state.cache.head_meta_pinned(full.to_string(), backend.clone(), upstream.clone()).await
        }
        None => state.cache.head_meta(full).await,
    }
}

async fn get_key<C>(State(state): State<AppState<C>>, Path(path_key): Path<String>, headers: HeaderMap) -> Response
where
    C: Clock + Clone,
{
    let (req_id, host_id) = request_ids();
    let (key, pinned) = resolve_target(&state, &path_key);

    // Suffix ranges need the object size up front: one lightweight stat
    // (memory or single PROPFIND — never a flight).
    let range = match parse_client_range(&headers) {
        Err(()) => {
            let hint = state.cache.memory_size(&key).await;
            return error_response(BackendError::RangeNotSatisfiable, &key, &req_id, &host_id, false, hint);
        }
        Ok(ClientRange::Multi) => {
            let hint = state.cache.memory_size(&key).await;
            return error_response(BackendError::RangeNotSatisfiable, &key, &req_id, &host_id, false, hint);
        }
        Ok(ClientRange::Absent) => None,
        Ok(ClientRange::Single(r)) => Some(r),
        Ok(ClientRange::Suffix(n)) => {
            let size = match head_lookup(&state, &key, &pinned).await {
                Ok(m) => m.size,
                Err(e) => {
                    return error_response(e, &key, &req_id, &host_id, false, None);
                }
            };
            if size == 0 || n == 0 {
                return error_response(BackendError::RangeNotSatisfiable, &key, &req_id, &host_id, false, Some(size));
            }
            // RFC 9110: suffix longer than the representation → whole object,
            // still 206 (not 200).
            Some(if n >= size { ByteRange::bounded(0, size) } else { ByteRange::bounded(size - n, n) })
        }
    };

    match fetch_key(&state, key.clone(), pinned, range).await {
        Ok(hit) => {
            let status =
                if hit.content_range.is_some() { StatusCode::PARTIAL_CONTENT } else { StatusCode::OK };
            info!(key = %key, outcome = ?hit.outcome, size = hit.meta.size, "cache response");

            let mut builder = Response::builder().status(status);
            if let Some(cr) = &hit.content_range {
                builder = builder.header("content-range", cr);
            }
            if let Some(len) = hit.content_length {
                builder = builder.header("content-length", len);
            }
            builder = s3_meta_headers(builder, &hit.meta, &req_id, &host_id);
            if hit.outcome == CacheOutcome::Stale {
                builder = builder.header("warning", "110 - \"Response is Stale\"");
            }
            builder.body(Body::from_stream(hit.body)).unwrap()
        }
        Err(e) => {
            // Size hint for 416 Content-Range: best-effort memory peek, no
            // upstream call (SHOULD-level per R1).
            let hint = state.cache.memory_size(&key).await;
            error_response(e, &key, &req_id, &host_id, false, hint)
        }
    }
}

/// HEAD: headers identical to GET, always 200 on success (even when ranged),
/// always an empty body. Served from memory meta or a single stat — never a
/// flight, never file bytes.
async fn head_key<C>(State(state): State<AppState<C>>, Path(path_key): Path<String>, headers: HeaderMap) -> Response
where
    C: Clock + Clone,
{
    let (req_id, host_id) = request_ids();
    let (key, pinned) = resolve_target(&state, &path_key);

    // One lightweight stat up front (memory or a single PROPFIND — never a
    // flight, never file bytes), then resolve any range against its size.
    let parsed = match parse_client_range(&headers) {
        Err(()) | Ok(ClientRange::Multi) => {
            let hint = state.cache.memory_size(&key).await;
            return error_response(BackendError::RangeNotSatisfiable, &key, &req_id, &host_id, true, hint);
        }
        Ok(r) => r,
    };
    // Suffix form needs no size yet; single/absent neither. The stat below
    // serves both freshness and size — exactly one upstream call at most.
    let meta = match head_lookup(&state, &key, &pinned).await {
        Ok(m) => m,
        Err(e) => {
            return error_response(e, &key, &req_id, &host_id, true, None);
        }
    };
    let (len, content_range) = match parsed {
        ClientRange::Absent => (meta.size, None),
        ClientRange::Multi => unreachable!("rejected above"),
        ClientRange::Single(r) => {
            if r.offset >= meta.size {
                return error_response(BackendError::RangeNotSatisfiable, &key, &req_id, &host_id, true, Some(meta.size));
            }
            let end = r.length.map_or(meta.size, |l| (r.offset + l).min(meta.size));
            (end - r.offset, Some(format!("bytes {}-{}/{}", r.offset, end - 1, meta.size)))
        }
        ClientRange::Suffix(n) => {
            if meta.size == 0 || n == 0 {
                return error_response(BackendError::RangeNotSatisfiable, &key, &req_id, &host_id, true, Some(meta.size));
            }
            let (offset, len) = if n >= meta.size { (0, meta.size) } else { (meta.size - n, n) };
            (len, Some(format!("bytes {}-{}/{}", offset, offset + len - 1, meta.size)))
        }
    };

    let mut builder = Response::builder().status(StatusCode::OK);
    if let Some(cr) = content_range {
        builder = builder.header("content-range", cr);
    }
    builder = builder.header("content-length", len);
    builder = s3_meta_headers(builder, &meta, &req_id, &host_id);
    builder.body(Body::empty()).unwrap()
}

async fn healthz<C>(State(state): State<AppState<C>>) -> impl IntoResponse
where
    C: Clock + Clone,
{
    let (count, bytes) = {
        let s = state.cache.state.read().await;
        (s.entries.len(), s.total_bytes)
    };
    (
        StatusCode::OK,
        Json(json!({
            "status": "ok",
            "plane": "business",
            "version": env!("CARGO_PKG_VERSION"),
            "entries": count,
            "bytes": bytes,
        })),
    )
}

async fn prewarm<C>(
    State(state): State<AppState<C>>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse
where
    C: Clock + Clone,
{
    if let Some(env_name) = &state.config.prewarm_shared_secret_env {
        let expected = std::env::var(env_name).unwrap_or_default();
        let got = headers
            .get("x-prewarm-token")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if expected.is_empty() || got != expected {
            return (StatusCode::UNAUTHORIZED, Json(json!({"error": "unauthorized"}))).into_response();
        }
    }
    if validate_key(&key).is_err() {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "invalid key"}))).into_response();
    }
    let s = state.cache.state.read().await;
    let already = s.entries.contains_key(&key);
    drop(s);
    if already {
        return (StatusCode::OK, Json(json!({"status": "hit"}))).into_response();
    }
    match state.cache.get(&key, None).await {
        Ok(mut hit) => {
            // Prewarm fetches without streaming to a client: drain the body.
            match crate::cache::flight::drain(&mut hit.body).await {
                Ok(_) => (StatusCode::OK, Json(json!({"status": "fetched"}))).into_response(),
                Err(e) => {
                    warn!(key = %key, error = %e, "prewarm drain failed");
                    (StatusCode::BAD_GATEWAY, Json(json!({"error": "upstream error"}))).into_response()
                }
            }
        }
        Err(BackendError::NotFound) => {
            (StatusCode::NOT_FOUND, Json(json!({"error": "not found"}))).into_response()
        }
        Err(_) => (StatusCode::BAD_GATEWAY, Json(json!({"error": "upstream error"}))).into_response(),
    }
}

async fn not_found() -> impl IntoResponse {
    (StatusCode::NOT_FOUND, Json(json!({"error": "not found"})))
}

pub fn router<C>(state: AppState<C>) -> Router
where
    C: Clock + Clone,
{
    Router::new()
        .route("/_internal/healthz", get(healthz::<C>))
        .route("/_internal/prewarm/{key}", post(prewarm::<C>))
        .route("/{*key}", get(get_key::<C>).head(head_key::<C>))
        .fallback(not_found)
        .with_state(state)
}

pub async fn serve<C>(
    addr: SocketAddr,
    state: AppState<C>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()>
where
    C: Clock + Clone,
{
    let listener = TcpListener::bind(addr).await?;
    info!(%addr, "business plane listening");
    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicUsize;
    use tokio::sync::Semaphore;

    use crate::{
        backend::{BackendRegistry, BackendSlot, Key, ObjectMeta, StreamSource, StorageBackend},
        clock::MockClock,
    };

    /// Counting backend: fixed bytes per upstream id, call counters on
    /// stat/open — proves HEAD never opens flights and alias pins upstreams.
    struct ProbeBackend {
        id: String,
        bytes: Vec<u8>,
        etag: Option<String>,
        always_missing: bool,
        stat_calls: Arc<AtomicUsize>,
        open_calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl StorageBackend for ProbeBackend {
        async fn stat(&self, _key: &Key) -> Result<ObjectMeta, BackendError> {
            self.stat_calls.fetch_add(1, Ordering::SeqCst);
            if self.always_missing {
                return Err(BackendError::NotFound);
            }
            Ok(ObjectMeta {
                size_bytes: self.bytes.len() as u64,
                etag: self.etag.clone(),
                last_modified: Some("Wed, 01 Jan 2025 00:00:00 GMT".into()),
                mime_hint: Some("application/octet-stream".into()),
            })
        }

        async fn open(&self, _key: &Key, range: Option<ByteRange>) -> Result<StreamSource, BackendError> {
            self.open_calls.fetch_add(1, Ordering::SeqCst);
            if self.always_missing {
                return Err(BackendError::NotFound);
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
                total_len: Some(self.bytes.len() as u64),
            })
        }

        async fn refresh_if_needed(&self) -> Result<(), BackendError> {
            Ok(())
        }

        fn id(&self) -> &str {
            &self.id
        }
    }

    struct Fixture {
        _dir: tempfile::TempDir,
        state: AppState<MockClock>,
        stat_calls: Arc<AtomicUsize>,
        open_calls: Arc<AtomicUsize>,
    }

    /// Single-upstream ("primary") fixture. `extra` adds more upstreams
    /// (used for the bucket-alias test). `missing` makes every key absent.
    fn fixture(bytes: &[u8], etag: Option<&str>, extra: Vec<(&str, Vec<u8>)>, missing: bool) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config { cache_dir: dir.path().to_path_buf(), ..Config::default() };
        let stat_calls = Arc::new(AtomicUsize::new(0));
        let open_calls = Arc::new(AtomicUsize::new(0));
        let mut slots = HashMap::new();
        let mk = |id: &str, b: Vec<u8>| ProbeBackend {
            id: id.into(),
            bytes: b,
            etag: etag.map(|s| s.into()),
            always_missing: missing,
            stat_calls: Arc::clone(&stat_calls),
            open_calls: Arc::clone(&open_calls),
        };
        slots.insert(
            "primary".to_string(),
            Arc::new(BackendSlot {
                backend: Arc::new(mk("primary", bytes.to_vec())),
                gate: Arc::new(Semaphore::new(3)),
            }),
        );
        for (id, b) in extra {
            slots.insert(
                id.to_string(),
                Arc::new(BackendSlot {
                    backend: Arc::new(mk(id, b)),
                    gate: Arc::new(Semaphore::new(3)),
                }),
            );
        }
        let cache = Arc::new(Cache::new(Arc::new(cfg.clone()), Arc::new(MockClock::new(0)), BackendRegistry::new(slots)));
        Fixture {
            _dir: dir,
            state: AppState { cache, config: Arc::new(cfg) },
            stat_calls,
            open_calls,
        }
    }

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        use axum::http::HeaderName;
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(k.parse::<HeaderName>().unwrap(), v.parse().unwrap());
        }
        h
    }

    async fn body_text(resp: Response) -> (StatusCode, HeaderMap, String) {
        let (mut parts, body) = resp.into_parts();
        let bytes = axum::body::to_bytes(body, 64 * 1024 * 1024).await.unwrap();
        let headers = std::mem::take(&mut parts.headers);
        (parts.status, headers, String::from_utf8_lossy(&bytes).into_owned())
    }

    /// Prime the cache via GET miss + full drain, then wait for install.
    async fn prime(fx: &Fixture, key: &str) {
        let resp = get_key(State(fx.state.clone()), Path(key.to_string()), headers(&[])).await;
        let (status, _, _) = body_text(resp).await;
        assert_eq!(status, StatusCode::OK);
        for _ in 0..200 {
            if fx.state.cache.state.read().await.entries.contains_key(key) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        panic!("entry {key} never installed");
    }

    fn reset(fx: &Fixture) {
        fx.stat_calls.store(0, Ordering::SeqCst);
        fx.open_calls.store(0, Ordering::SeqCst);
    }

    #[test]
    fn etag_quoting_rules() {
        assert_eq!(quote_etag("abc123"), "\"abc123\"");
        assert_eq!(quote_etag("\"abc123\""), "\"abc123\"");
        assert_eq!(quote_etag("d41d8cd98f00b204e9800998ecf8427e-2"), "\"d41d8cd98f00b204e9800998ecf8427e-2\"");
        assert_eq!(quote_etag("W/\"abc\""), "\"abc\"");
    }

    #[test]
    fn range_parser_shapes() {
        assert!(matches!(parse_client_range(&headers(&[])).unwrap(), ClientRange::Absent));
        assert!(matches!(
            parse_client_range(&headers(&[("range", "bytes=10-20")])).unwrap(),
            ClientRange::Single(_)
        ));
        assert!(matches!(
            parse_client_range(&headers(&[("range", "bytes=-30")])).unwrap(),
            ClientRange::Suffix(30)
        ));
        assert!(matches!(
            parse_client_range(&headers(&[("range", "bytes=0-1,3-4")])).unwrap(),
            ClientRange::Multi
        ));
        assert!(parse_client_range(&headers(&[("range", "bytes=100-50")])).is_err());
        assert!(parse_client_range(&headers(&[("range", "bytes=-0")])).is_err());
        assert!(parse_client_range(&headers(&[("range", "items=0-1")])).is_err());
    }

    #[test]
    fn bucket_split_rules() {
        let ids = vec!["primary".to_string(), "archive".to_string()];
        assert_eq!(split_bucket("archive/f.bin", &ids), Some(("archive", "f.bin")));
        assert_eq!(split_bucket("a.bin", &ids), None);
        assert_eq!(split_bucket("unknown/f.bin", &ids), None);
        assert_eq!(split_bucket("archive/", &ids), None);
    }

    #[tokio::test]
    async fn get_hit_s3_shape() {
        let fx = fixture(b"0123456789", Some("abc123"), vec![], false);
        prime(&fx, "a.bin").await;
        let resp = get_key(State(fx.state.clone()), Path("a.bin".into()), headers(&[])).await;
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
        let fx = fixture(b"0123456789", None, vec![], false);
        prime(&fx, "a.bin").await;
        let r1 = get_key(State(fx.state.clone()), Path("a.bin".into()), headers(&[])).await;
        let r2 = get_key(State(fx.state.clone()), Path("a.bin".into()), headers(&[])).await;
        assert_ne!(
            r1.headers().get("x-amz-request-id").unwrap(),
            r2.headers().get("x-amz-request-id").unwrap()
        );
    }

    #[tokio::test]
    async fn get_missing_is_nosuchkey_xml() {
        let fx = fixture(b"0123456789", None, vec![], true);
        // Missing on a fresh cache: stat the backend once to confirm absence.
        let resp = get_key(State(fx.state.clone()), Path("nope.bin".into()), headers(&[])).await;
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
        let fx = fixture(b"0123456789", None, vec![], false);
        prime(&fx, "a.bin").await;
        let resp = get_key(
            State(fx.state.clone()),
            Path("a.bin".into()),
            headers(&[("range", "bytes=20-30")]),
        )
        .await;
        let (status, h, body) = body_text(resp).await;
        assert_eq!(status, StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(h.get("content-range").unwrap(), "bytes */10");
        assert!(body.contains("<Code>InvalidRange</Code>"), "{body}");
    }

    #[tokio::test]
    async fn get_multi_range_rejected() {
        let fx = fixture(b"0123456789", None, vec![], false);
        prime(&fx, "a.bin").await;
        let resp = get_key(
            State(fx.state.clone()),
            Path("a.bin".into()),
            headers(&[("range", "bytes=0-1,3-4")]),
        )
        .await;
        let (status, _, body) = body_text(resp).await;
        assert_eq!(status, StatusCode::RANGE_NOT_SATISFIABLE);
        assert!(body.contains("<Code>InvalidRange</Code>"), "{body}");
    }

    #[tokio::test]
    async fn get_suffix_ranges() {
        let fx = fixture(b"0123456789", None, vec![], false);
        prime(&fx, "a.bin").await;
        // Last 3 bytes.
        let resp = get_key(
            State(fx.state.clone()),
            Path("a.bin".into()),
            headers(&[("range", "bytes=-3")]),
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
        )
        .await;
        let (status, h, body) = body_text(resp).await;
        assert_eq!(status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(h.get("content-range").unwrap(), "bytes 0-9/10");
        assert_eq!(body, "0123456789");
    }

    #[tokio::test]
    async fn head_hit_no_backend_no_body() {
        let fx = fixture(b"0123456789", Some("v1"), vec![], false);
        prime(&fx, "a.bin").await;
        reset(&fx);
        let resp = head_key(State(fx.state.clone()), Path("a.bin".into()), headers(&[])).await;
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
        let fx = fixture(b"0123456789", None, vec![], false);
        prime(&fx, "a.bin").await;
        reset(&fx);
        let resp = head_key(
            State(fx.state.clone()),
            Path("a.bin".into()),
            headers(&[("range", "bytes=2-5")]),
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

    #[tokio::test]
    async fn head_missing_404_empty_shares_negative_cache() {
        let fx = fixture(b"0123456789", None, vec![], true);
        let resp = head_key(State(fx.state.clone()), Path("gone.bin".into()), headers(&[])).await;
        let (status, _, body) = body_text(resp).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body.is_empty());
        assert_eq!(fx.stat_calls.load(Ordering::SeqCst), 1);
        // Second HEAD: negative tombstone, no second stat.
        let resp = head_key(State(fx.state.clone()), Path("gone.bin".into()), headers(&[])).await;
        let (status, _, _) = body_text(resp).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(fx.stat_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn head_stale_costs_one_stat_no_open() {
        let fx = fixture(b"0123456789", None, vec![], false);
        prime(&fx, "a.bin").await;
        // Age past revalidate_ttl (60 s default): HEAD must re-stat (fresh),
        // but still never opens a flight or reads bytes.
        fx.state.cache.clock.advance(61_000);
        reset(&fx);
        let resp = head_key(State(fx.state.clone()), Path("a.bin".into()), headers(&[])).await;
        let (status, h, body) = body_text(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.is_empty());
        assert_eq!(h.get("content-length").unwrap(), "10");
        assert_eq!(fx.stat_calls.load(Ordering::SeqCst), 1);
        assert_eq!(fx.open_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn bucket_alias_pins_upstream() {
        let fx = fixture(b"AAA", None, vec![("archive", b"BBB".to_vec())], false);
        // Legacy path routes "" → primary.
        let resp = get_key(State(fx.state.clone()), Path("f.bin".into()), headers(&[])).await;
        let (_, _, body) = body_text(resp).await;
        assert_eq!(body, "AAA");
        // Bucket alias pins the archive upstream regardless of routes.
        let resp = get_key(State(fx.state.clone()), Path("archive/f.bin".into()), headers(&[])).await;
        let (_, _, body) = body_text(resp).await;
        assert_eq!(body, "BBB");
    }
}
