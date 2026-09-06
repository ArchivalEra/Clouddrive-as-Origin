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
    sync::Arc,
    time::Duration,
};
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::{
    backend::{BackendError, ByteRange, ContentRange, Key},
    cache::cache::{Cache, CacheOutcome},
    clock::Clock,
    config::{ColdMiss, Config},
    key::ResolvedKey,
    response::{error_response, request_ids, s3_meta_headers},
};

#[derive(Clone)]
pub struct AppState<C: Clock + Clone> {
    pub cache: Arc<Cache<C>>,
    pub config: Arc<Config>,
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

/// A relief valve (P1): cold + redirect-capable upstream → 307 to the
/// upstream-issued direct link, bytes filled in background. Returns
/// `Some(response)` only on the 307 path; `None` means "serve from cache
/// / proxy as usual". Hit-first: a fresh memory entry never redirects.
/// Every failure (disabled, unsupported, rejected target, slow link)
/// silently falls through to the water-pipe — the valve can only save
/// bandwidth, never break a fetch.
async fn try_relief_valve<C: Clock + Clone>(
    state: &AppState<C>,
    rk: &ResolvedKey,
    headers: &HeaderMap,
) -> Option<Response> {
    let redirect = state
        .config
        .upstream(&rk.upstream_id)
        .map(|u| u.cold_miss == ColdMiss::Redirect)
        .unwrap_or(false);
    if !redirect {
        return None;
    }
    if state.cache.memory_hit_fresh(&rk.cache_key).await {
        return None;
    }
    let slot = state.cache.backends.get(&rk.upstream_id)?;
    let ua = headers.get("user-agent").and_then(|v| v.to_str().ok());
    // Bound the link round-trips: a slow link source must not stall the
    // viewer before the proxy fallback engages.
    let link = tokio::time::timeout(
        Duration::from_secs(8),
        slot.backend.direct_url(&Key::from_validated(rk.backend_key.clone()), ua),
    )
    .await
    .ok()?
    .ok()?;
    if !crate::backend::redirect_target_allowed(&link.url) {
        return None;
    }
    // Fill in background; the viewer leaves now.
    let cache = Arc::clone(&state.cache);
    let rk_owned = rk.clone();
    tokio::spawn(async move {
        let _ = cache.prefetch(&rk_owned).await;
    });
    let (req_id, host_id) = request_ids();
    let location: axum::http::HeaderValue = link.url.parse().ok()?;
    Some(
        Response::builder()
            .status(StatusCode::TEMPORARY_REDIRECT)
            .header("location", location)
            // The 307 itself must never be edge-cached: the signed target
            // expires while the URL stays the same.
            .header("cache-control", "no-store")
            .header("x-amz-request-id", req_id)
            .header("x-amz-id-2", host_id)
            .body(Body::empty())
            .unwrap(),
    )
}

async fn get_key<C>(State(state): State<AppState<C>>, Path(path_key): Path<String>, headers: HeaderMap) -> Response
where
    C: Clock + Clone,
{
    let (req_id, host_id) = request_ids();
    // Single resolution seam (C2): routing + bucket alias + validation,
    // once. Everything below takes `rk` — no re-resolution, no re-validation.
    let rk = match state.cache.resolve(&path_key) {
        Ok(rk) => rk,
        Err(e) => {
            return error_response(
                BackendError::Other(format!("invalid key: {e}")),
                &path_key,
                &req_id,
                &host_id,
                false,
                None,
            );
        }
    };
    let key = &rk.cache_key;

    // AWS error precedence: malformed/multi Ranges 416 before anything else.
    let parsed = match parse_client_range(&headers) {
        Err(()) | Ok(ClientRange::Multi) => {
            let hint = state.cache.memory_size(key).await;
            return error_response(BackendError::RangeNotSatisfiable, key, &req_id, &host_id, false, hint);
        }
        Ok(r) => r,
    };
    // A relief valve: cold + redirect-capable upstreams leave via 307
    // (hits never redirect — checked inside). Falls through to proxy.
    if let Some(redirect) = try_relief_valve(&state, &rk, &headers).await {
        return redirect;
    }
    // Suffix ranges need the object size up front: one lightweight stat
    // (memory or single PROPFIND — never a flight).
    let range = match parsed {
        ClientRange::Absent => None,
        ClientRange::Multi => unreachable!("rejected above"),
        ClientRange::Single(r) => Some(r),
        ClientRange::Suffix(n) => {
            let size = match state.cache.head_resolved(&rk).await {
                Ok(m) => m.size,
                Err(e) => {
                    return error_response(e, key, &req_id, &host_id, false, None);
                }
            };
            if size == 0 || n == 0 {
                return error_response(BackendError::RangeNotSatisfiable, key, &req_id, &host_id, false, Some(size));
            }
            // RFC 9110: suffix longer than the representation → whole object,
            // still 206 (not 200).
            Some(if n >= size { ByteRange::bounded(0, size) } else { ByteRange::bounded(size - n, n) })
        }
    };

    match state.cache.get_resolved(&rk, range).await {
        Ok(hit) => {
            let status =
                if hit.content_range.is_some() { StatusCode::PARTIAL_CONTENT } else { StatusCode::OK };
            info!(key = %key, outcome = ?hit.outcome, size = hit.meta.size, "cache response");

            let mut builder = Response::builder().status(status);
            if let Some(cr) = &hit.content_range {
                builder = builder.header("content-range", cr.header_value());
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
            let hint = state.cache.memory_size(key).await;
            error_response(e, key, &req_id, &host_id, false, hint)
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
    let rk = match state.cache.resolve(&path_key) {
        Ok(rk) => rk,
        Err(e) => {
            return error_response(
                BackendError::Other(format!("invalid key: {e}")),
                &path_key,
                &req_id,
                &host_id,
                true,
                None,
            );
        }
    };
    let key = &rk.cache_key;

    // One lightweight stat up front (memory or a single PROPFIND — never a
    // flight, never file bytes), then resolve any range against its size.
    let parsed = match parse_client_range(&headers) {
        Err(()) | Ok(ClientRange::Multi) => {
            let hint = state.cache.memory_size(key).await;
            return error_response(BackendError::RangeNotSatisfiable, key, &req_id, &host_id, true, hint);
        }
        Ok(r) => r,
    };
    // Suffix form needs no size yet; single/absent neither. The stat below
    // serves both freshness and size — exactly one upstream call at most.
    let meta = match state.cache.head_resolved(&rk).await {
        Ok(m) => m,
        Err(e) => {
            return error_response(e, key, &req_id, &host_id, true, None);
        }
    };
    let (len, content_range) = match parsed {
        ClientRange::Absent => (meta.size, None),
        ClientRange::Multi => unreachable!("rejected above"),
        ClientRange::Single(r) => {
            if r.offset >= meta.size {
                return error_response(BackendError::RangeNotSatisfiable, key, &req_id, &host_id, true, Some(meta.size));
            }
            let end = r.length.map_or(meta.size, |l| (r.offset + l).min(meta.size));
            let cr = ContentRange { first: r.offset, last: end - 1, total: meta.size };
            (end - r.offset, Some(cr))
        }
        ClientRange::Suffix(n) => {
            if meta.size == 0 || n == 0 {
                return error_response(BackendError::RangeNotSatisfiable, key, &req_id, &host_id, true, Some(meta.size));
            }
            let (offset, len) = if n >= meta.size { (0, meta.size) } else { (meta.size - n, n) };
            let cr = ContentRange { first: offset, last: offset + len - 1, total: meta.size };
            (len, Some(cr))
        }
    };

    let mut builder = Response::builder().status(StatusCode::OK);
    if let Some(cr) = content_range {
        builder = builder.header("content-range", cr.header_value());
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
    let rk = match state.cache.resolve(&key) {
        Ok(rk) => rk,
        Err(_) => {
            return (StatusCode::BAD_REQUEST, Json(json!({"error": "invalid key"}))).into_response();
        }
    };
    let s = state.cache.state.read().await;
    let already = s.entries.contains_key(&rk.cache_key);
    drop(s);
    if already {
        return (StatusCode::OK, Json(json!({"status": "hit"}))).into_response();
    }
    // Same primitive as the relief valve's background fill: full fetch, no
    // client attached.
    match state.cache.prefetch(&rk).await {
        Ok(()) => (StatusCode::OK, Json(json!({"status": "fetched"}))).into_response(),
        Err(BackendError::NotFound) => {
            (StatusCode::NOT_FOUND, Json(json!({"error": "not found"}))).into_response()
        }
        Err(e) => {
            warn!(key = %key, error = %e, "prewarm fetch failed");
            (StatusCode::BAD_GATEWAY, Json(json!({"error": "upstream error"}))).into_response()
        }
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Semaphore;

    use crate::{
        backend::{BackendRegistry, BackendSlot, DirectUrl, Key, ObjectMeta, StreamSource, StorageBackend},
        clock::MockClock,
    };

    /// Counting backend: fixed bytes per upstream id, call counters on
    /// stat/open — proves HEAD never opens flights and alias pins upstreams.
    struct ProbeBackend {
        id: String,
        bytes: Vec<u8>,
        etag: Option<String>,
        always_missing: bool,
        direct: Option<String>,
        stat_calls: Arc<AtomicUsize>,
        open_calls: Arc<AtomicUsize>,
        direct_calls: Arc<AtomicUsize>,
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

        async fn direct_url(&self, _key: &Key, _viewer_ua: Option<&str>) -> Result<DirectUrl, BackendError> {
            self.direct_calls.fetch_add(1, Ordering::SeqCst);
            self.direct
                .clone()
                .map(|url| DirectUrl { url })
                .ok_or_else(|| BackendError::Other("no link".into()))
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
        direct_calls: Arc<AtomicUsize>,
    }

    /// Single-upstream ("primary") fixture. `extra` adds more upstreams
    /// (used for the bucket-alias test). `missing` makes every key absent.
    fn fixture(bytes: &[u8], etag: Option<&str>, extra: Vec<(&str, Vec<u8>)>, missing: bool) -> Fixture {
        fixture_full(bytes, etag, extra, missing, None, false)
    }

    /// Full fixture: `direct` is the Tier 1 link the backend offers
    /// (None = Tier 3 fallback); `redirect` flips primary to
    /// `cold_miss = "redirect"`.
    fn fixture_full(
        bytes: &[u8],
        etag: Option<&str>,
        extra: Vec<(&str, Vec<u8>)>,
        missing: bool,
        direct: Option<&str>,
        redirect: bool,
    ) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config { cache_dir: dir.path().to_path_buf(), ..Config::default() };
        if redirect {
            cfg.upstreams[0].cold_miss = ColdMiss::Redirect;
        }
        let stat_calls = Arc::new(AtomicUsize::new(0));
        let open_calls = Arc::new(AtomicUsize::new(0));
        let direct_calls = Arc::new(AtomicUsize::new(0));
        let mut slots = HashMap::new();
        let mk = |id: &str, b: Vec<u8>| ProbeBackend {
            id: id.into(),
            bytes: b,
            etag: etag.map(|s| s.into()),
            always_missing: missing,
            direct: direct.map(|s| s.into()),
            stat_calls: Arc::clone(&stat_calls),
            open_calls: Arc::clone(&open_calls),
            direct_calls: Arc::clone(&direct_calls),
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
            direct_calls,
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
        wait_installed(fx, key).await;
    }

    /// Prime below the business plane (bypasses the relief valve): for
    /// redirect-enabled fixtures where GET would 307 instead of filling.
    async fn prime_cache(fx: &Fixture, key: &str) {
        let mut hit = fx.state.cache.get(key, None).await.unwrap();
        crate::cache::flight::drain(&mut hit.body).await.unwrap();
        wait_installed(fx, key).await;
    }

    async fn wait_installed(fx: &Fixture, key: &str) {
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

    #[tokio::test]
    async fn redirect_cold_307_and_background_fill() {
        let fx = fixture_full(b"0123456789", None, vec![], false, Some("https://cdn.example.com/f?sign=x"), true);
        let resp = get_key(State(fx.state.clone()), Path("new.bin".into()), headers(&[])).await;
        let (status, h, body) = body_text(resp).await;
        assert_eq!(status, StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(h.get("location").unwrap(), "https://cdn.example.com/f?sign=x");
        assert_eq!(h.get("cache-control").unwrap(), "no-store");
        assert!(body.is_empty());
        assert!(h.get("x-amz-request-id").is_some());
        assert_eq!(fx.direct_calls.load(Ordering::SeqCst), 1);
        // Background fill installs the entry without any viewer attached.
        for _ in 0..200 {
            if fx.state.cache.state.read().await.entries.contains_key("new.bin") {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(fx.state.cache.state.read().await.entries.contains_key("new.bin"));
    }

    #[tokio::test]
    async fn redirect_hit_serves_cache_never_redirects() {
        let fx = fixture_full(b"0123456789", None, vec![], false, Some("https://cdn.example.com/f"), true);
        prime_cache(&fx, "a.bin").await;
        reset(&fx);
        // fresh hit → 200 from cache even though redirect is enabled.
        let resp = get_key(State(fx.state.clone()), Path("a.bin".into()), headers(&[])).await;
        let (status, _, body) = body_text(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "0123456789");
        assert_eq!(fx.direct_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn redirect_unavailable_silently_proxies() {
        // Tier 3 (no link): normal water-pipe, viewer unaffected.
        let fx = fixture_full(b"0123456789", None, vec![], false, None, true);
        let resp = get_key(State(fx.state.clone()), Path("new.bin".into()), headers(&[])).await;
        let (status, _, body) = body_text(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "0123456789");
        assert_eq!(fx.direct_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn redirect_rejected_target_silently_proxies() {
        // Foreign http is not an allowed redirect target → proxy.
        let fx = fixture_full(b"0123456789", None, vec![], false, Some("http://cdn.example.com/f"), true);
        let resp = get_key(State(fx.state.clone()), Path("new.bin".into()), headers(&[])).await;
        let (status, _, body) = body_text(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "0123456789");
    }

    #[tokio::test]
    async fn redirect_disabled_never_consults_backend() {
        // Default proxy mode: direct_url untouched even when offered.
        let fx = fixture_full(b"0123456789", None, vec![], false, Some("https://cdn.example.com/f"), false);
        let resp = get_key(State(fx.state.clone()), Path("new.bin".into()), headers(&[])).await;
        let (status, _, _) = body_text(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(fx.direct_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn prewarm_fetches_via_prefetch() {
        let fx = fixture(b"0123456789", None, vec![], false);
        let resp = prewarm(State(fx.state.clone()), Path("w.bin".into()), headers(&[])).await.into_response();
        let (status, _, body) = body_text(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("fetched"), "{body}");
        assert!(fx.state.cache.state.read().await.entries.contains_key("w.bin"));
    }
}
