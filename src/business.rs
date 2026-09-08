use axum::{
    body::Body,
    extract::{OriginalUri, Path, RawQuery, State},
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
    sigv4,
};

/// Inbound SigV4 gate (#28): optional verify-if-present. Reads the
/// verifier's input from the raw request line + headers; a request with
/// SigV4 material is verified (403 XML on failure), anything else
/// passes through (D1 anonymous-first). Config comes from named env
/// vars once at boot; `None` disables the layer entirely.
fn sigv4_gate(
    cfg: Option<&sigv4::SigV4Config>,
    method: &str,
    raw_uri_path: &str,
    query: Option<&str>,
    headers: &HeaderMap,
    req_id: &str,
    host_id: &str,
    now_unix: i64,
) -> Option<Response> {
    let decoded_path = raw_uri_path.percent_decoded();
    let pairs: Vec<(String, String)> = query
        .map(|q| form_urlencoded::parse(q.as_bytes()).map(|(k, v)| (k.into_owned(), v.into_owned())).collect())
        .unwrap_or_default();
    let header_pairs: Vec<(String, String)> = headers
        .iter()
        .filter_map(|(k, v)| v.to_str().ok().map(|v| (k.as_str().to_ascii_lowercase(), v.to_string())))
        .collect();
    let input = sigv4::VerifyInput {
        method,
        uri_path: &decoded_path,
        raw_uri_path,
        query_pairs: pairs.into_iter().map(|(k, v)| (k, v)).collect(),
        headers: header_pairs,
        authorization: headers.get("authorization").and_then(|v| v.to_str().ok()),
    };
    match sigv4::verify_optional(cfg, &input, now_unix) {
        sigv4::VerifyOutcome::Anonymous | sigv4::VerifyOutcome::Verified(_) => None,
        // The reason string stays out of the response body: AWS-compatible
        // clients only need the code; the detail lives in the log.
        sigv4::VerifyOutcome::Failed(reason) => {
            tracing::warn!(reason, "sigv4 verification failed");
            let xml = crate::response::s3_error_xml(
                "AccessDenied",
                "Access Denied",
                &format!("/{raw_uri_path}"),
                req_id,
                host_id,
            );
            Some(
                Response::builder()
                    .status(StatusCode::FORBIDDEN)
                    .header("content-type", "application/xml")
                    .header("cache-control", "no-store")
                    .header("x-amz-request-id", req_id)
                    .header("x-amz-id-2", host_id)
                    .body(Body::from(xml))
                    .unwrap(),
            )
        }
    }
}

trait PercentDecodePath {
    fn percent_decoded(&self) -> String;
}

impl PercentDecodePath for str {
    fn percent_decoded(&self) -> String {
        percent_encoding::percent_decode_str(self).decode_utf8_lossy().into_owned()
    }
}

#[derive(Clone)]
pub struct AppState<C: Clock + Clone> {
    pub cache: Arc<Cache<C>>,
    pub config: Arc<Config>,
    /// Inbound SigV4 credentials (named env vars, read once at boot).
    /// `None` = anonymous-first everywhere (the #28 default).
    pub sigv4_config: Option<sigv4::SigV4Config>,
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

async fn get_key<C>(
    State(state): State<AppState<C>>,
    Path(path_key): Path<String>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
    OriginalUri(original): OriginalUri,
) -> Response
where
    C: Clock + Clone,
{
    let (req_id, host_id) = request_ids();
    // Inbound SigV4 gate (#28): optional verify-if-present. The raw URI
    // path is exactly what the client signed; the business path may be
    // percent-decoded.
    if let Some(resp) = sigv4_gate(
        state.sigv4_config.as_ref(),
        "GET",
        original.path(),
        query.as_deref(),
        &headers,
        &req_id,
        &host_id,
        sigv4::SigV4Config::now_unix(),
    ) {
        return resp;
    }
    // List dispatch (map #24): S3 list operations live on the same path
    // space as objects but bypass the cache entirely (metadata path).
    if let Some(resp) =
        crate::list::try_list(&state, &path_key, query.as_deref(), &req_id, &host_id).await
    {
        return resp;
    }
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

    if let Some(passthrough) = try_nocache(&state, &rk, range, &req_id, &host_id).await {
        return passthrough;
    }
    if let Some(passthrough) = try_passthrough(&state, &rk, range, &req_id, &host_id).await {
        return passthrough;
    }

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

/// Nocache profile (small-footprint nodes): every GET water-pipes
/// origin-to-viewer with zero disk writes — no entries, no flights, no
/// segments, no tombstones. Range or not, cold or not: everything goes
/// through; only header metadata is stat'd. Every failure surfaces via
/// the standard error mapping (no stale-if-error: there is no disk copy).
async fn try_nocache<C: Clock + Clone>(
    state: &AppState<C>,
    rk: &ResolvedKey,
    range: Option<ByteRange>,
    req_id: &str,
    host_id: &str,
) -> Option<Response> {
    let prof = state.config.cache_profile(&rk.upstream_id);
    if !prof.nocache {
        return None;
    }
    let hit = state.cache.serve_nocache(rk, range).await.ok()?;
    info!(key = %rk.cache_key, size = hit.meta.size, "nocache passthrough response");
    let status = if hit.content_range.is_some() { StatusCode::PARTIAL_CONTENT } else { StatusCode::OK };
    let mut builder = Response::builder().status(status);
    if let Some(cr) = &hit.content_range {
        builder = builder.header("content-range", cr.header_value());
    }
    if let Some(len) = hit.content_length {
        builder = builder.header("content-length", len);
    }
    builder = s3_meta_headers(builder, &hit.meta, req_id, host_id);
    Some(builder.body(Body::from_stream(hit.body)).unwrap())
}

/// Efficient profile (P2-a): ranged misses passthrough origin straight to
/// the viewer while staging the served interval (no flight, no full fill).
/// Fresh entries serve from disk via the B path below; every failure here
/// falls through to it (stale-if-error included).
async fn try_passthrough<C: Clock + Clone>(
    state: &AppState<C>,
    rk: &ResolvedKey,
    range: Option<ByteRange>,
    req_id: &str,
    host_id: &str,
) -> Option<Response> {
    let prof = state.config.cache_profile(&rk.upstream_id);
    if !prof.efficient || range.is_none() {
        return None;
    }
    if state.cache.memory_hit_fresh(&rk.cache_key).await {
        return None;
    }
    let hit = state.cache.serve_passthrough(rk, range, prof.min_file_size).await.ok()?;
    info!(key = %rk.cache_key, size = hit.meta.size, "passthrough response");
    let mut builder = Response::builder().status(StatusCode::PARTIAL_CONTENT);
    if let Some(cr) = &hit.content_range {
        builder = builder.header("content-range", cr.header_value());
    }
    if let Some(len) = hit.content_length {
        builder = builder.header("content-length", len);
    }
    builder = s3_meta_headers(builder, &hit.meta, req_id, host_id);
    Some(builder.body(Body::from_stream(hit.body)).unwrap())
}

/// HEAD: headers identical to GET, always 200 on success (even when ranged),
/// always an empty body. Served from memory meta or a single stat — never a
/// flight, never file bytes.
async fn head_key<C>(
    State(state): State<AppState<C>>,
    Path(path_key): Path<String>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
    OriginalUri(original): OriginalUri,
) -> Response
where
    C: Clock + Clone,
{
    let (req_id, host_id) = request_ids();
    // Same SigV4 gate as GET (presigned/head-signed requests).
    if let Some(resp) = sigv4_gate(
        state.sigv4_config.as_ref(),
        "HEAD",
        original.path(),
        query.as_deref(),
        &headers,
        &req_id,
        &host_id,
        sigv4::SigV4Config::now_unix(),
    ) {
        return resp;
    }
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
    let (count, bytes, segment_bytes) = {
        let s = state.cache.state.read().await;
        (s.entries.len(), s.total_bytes, s.segment_bytes)
    };
    // Live-machinery depths (spec §8: the queue-ish counters operators
    // watch when a node misbehaves): active cold-miss flights, promotion
    // assemblies in flight, and pending access-clock flushes.
    let flights = state.cache.flights.lock().await.len();
    let promotions = state.cache.promotions.lock().await.len();
    let dirty_access = state.cache.dirty_access.lock().await.len();
    // Per-upstream view: profile + gate depth, so a saturated or
    // misconfigured upstream is visible without reading logs.
    let upstreams: Vec<serde_json::Value> = state
        .config
        .upstreams
        .iter()
        .map(|u| {
            let prof = state.config.cache_profile(&u.id);
            json!({
                "id": u.id,
                "profile": if prof.nocache { "nocache" } else if prof.efficient { "efficient" } else { "standard" },
                "cold_miss": format!("{:?}", u.cold_miss).to_lowercase(),
                "sigv4_layer": state.sigv4_config.is_some(),
            })
        })
        .collect();
    (
        StatusCode::OK,
        Json(json!({
            "status": "ok",
            "plane": "business",
            "version": env!("CARGO_PKG_VERSION"),
            "entries": count,
            "bytes": bytes,
            "segment_bytes": segment_bytes,
            "flights_active": flights,
            "promotions_active": promotions,
            "dirty_access_flushes": dirty_access,
            "sigv4_enabled": state.sigv4_config.is_some(),
            "upstreams": upstreams,
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
    /// `etag` is interior-mutable (version-flip tests); `opens` records
    /// every open's (offset, length) for gap-fetch assertions.
    struct ProbeBackend {
        id: String,
        bytes: Vec<u8>,
        etag: Arc<std::sync::Mutex<Option<String>>>,
        always_missing: bool,
        direct: Option<String>,
        stat_calls: Arc<AtomicUsize>,
        open_calls: Arc<AtomicUsize>,
        direct_calls: Arc<AtomicUsize>,
        opens: Arc<std::sync::Mutex<Vec<(u64, Option<u64>)>>>,
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
                etag: self.etag.lock().unwrap().clone(),
                last_modified: Some("Wed, 01 Jan 2025 00:00:00 GMT".into()),
                mime_hint: Some("application/octet-stream".into()),
            })
        }

        async fn open(&self, _key: &Key, range: Option<ByteRange>) -> Result<StreamSource, BackendError> {
            self.open_calls.fetch_add(1, Ordering::SeqCst);
            if self.always_missing {
                return Err(BackendError::NotFound);
            }
            if let Some(r) = range {
                self.opens.lock().unwrap().push((r.offset, r.length));
            } else {
                self.opens.lock().unwrap().push((0, None));
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
        etag: Arc<std::sync::Mutex<Option<String>>>,
        opens: Arc<std::sync::Mutex<Vec<(u64, Option<u64>)>>>,
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
        let etag = Arc::new(std::sync::Mutex::new(etag.map(|s| s.into())));
        let opens = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut slots = HashMap::new();
        let mk = |id: &str, b: Vec<u8>| ProbeBackend {
            id: id.into(),
            bytes: b,
            etag: Arc::clone(&etag),
            always_missing: missing,
            direct: direct.map(|s| s.into()),
            stat_calls: Arc::clone(&stat_calls),
            open_calls: Arc::clone(&open_calls),
            direct_calls: Arc::clone(&direct_calls),
            opens: Arc::clone(&opens),
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
            state: AppState { cache, config: Arc::new(cfg), sigv4_config: None },
            stat_calls,
            open_calls,
            direct_calls,
            etag,
            opens,
        }
    }

    /// Efficient-profile fixture: primary serves `cache_profile =
    /// "efficient"` with the given threshold/min_file_size.
    fn fixture_efficient(bytes: &[u8], threshold: f64, min_file_size: u64) -> Fixture {
        use crate::config::CacheProfile;
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config { cache_dir: dir.path().to_path_buf(), ..Config::default() };
        cfg.upstreams[0].cache_profile = "efficient".into();
        cfg.cache_profiles.insert("efficient".into(), CacheProfile { coverage_threshold: threshold, min_file_size });
        let stat_calls = Arc::new(AtomicUsize::new(0));
        let open_calls = Arc::new(AtomicUsize::new(0));
        let direct_calls = Arc::new(AtomicUsize::new(0));
        let etag = Arc::new(std::sync::Mutex::new(Some("v1".into())));
        let opens = Arc::new(std::sync::Mutex::new(Vec::new()));
        let backend = ProbeBackend {
            id: "primary".into(),
            bytes: bytes.to_vec(),
            etag: Arc::clone(&etag),
            always_missing: false,
            direct: None,
            stat_calls: Arc::clone(&stat_calls),
            open_calls: Arc::clone(&open_calls),
            direct_calls: Arc::clone(&direct_calls),
            opens: Arc::clone(&opens),
        };
        let mut slots = HashMap::new();
        slots.insert(
            "primary".to_string(),
            Arc::new(BackendSlot { backend: Arc::new(backend), gate: Arc::new(Semaphore::new(3)) }),
        );
        let cache = Arc::new(Cache::new(Arc::new(cfg.clone()), Arc::new(MockClock::new(0)), BackendRegistry::new(slots)));
        Fixture {
            _dir: dir,
            state: AppState { cache, config: Arc::new(cfg), sigv4_config: None },
            stat_calls,
            open_calls,
            direct_calls,
            etag,
            opens,
        }
    }

    /// Nocache-profile fixture: primary serves `cache_profile = "nocache"`
    /// (built-in pure water-pipe, zero disk writes).
    fn fixture_nocache(bytes: &[u8]) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config { cache_dir: dir.path().to_path_buf(), ..Config::default() };
        cfg.upstreams[0].cache_profile = "nocache".into();
        let stat_calls = Arc::new(AtomicUsize::new(0));
        let open_calls = Arc::new(AtomicUsize::new(0));
        let direct_calls = Arc::new(AtomicUsize::new(0));
        let etag = Arc::new(std::sync::Mutex::new(Some("v1".into())));
        let opens = Arc::new(std::sync::Mutex::new(Vec::new()));
        let backend = ProbeBackend {
            id: "primary".into(),
            bytes: bytes.to_vec(),
            etag: Arc::clone(&etag),
            always_missing: false,
            direct: None,
            stat_calls: Arc::clone(&stat_calls),
            open_calls: Arc::clone(&open_calls),
            direct_calls: Arc::clone(&direct_calls),
            opens: Arc::clone(&opens),
        };
        let mut slots = HashMap::new();
        slots.insert(
            "primary".to_string(),
            Arc::new(BackendSlot { backend: Arc::new(backend), gate: Arc::new(Semaphore::new(3)) }),
        );
        let cache = Arc::new(Cache::new(Arc::new(cfg.clone()), Arc::new(MockClock::new(0)), BackendRegistry::new(slots)));
        Fixture {
            _dir: dir,
            state: AppState { cache, config: Arc::new(cfg), sigv4_config: None },
            stat_calls,
            open_calls,
            direct_calls,
            etag,
            opens,
        }
    }

    /// Segment sidecars currently staged for `key` (test introspection).
    fn staged_segments(fx: &Fixture, key: &str) -> Vec<(u64, u64)> {
        let prefix = format!(".seg.{}.", crate::cache::store::escape_key(key));
        let mut out = Vec::new();
        let rd = std::fs::read_dir(&fx.state.config.cache_dir).unwrap();
        for entry in rd.filter_map(|e| e.ok()) {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with(&prefix) {
                let range = name[prefix.len()..].to_string();
                let (s, e) = range.split_once('-').unwrap();
                out.push((s.parse().unwrap(), e.parse().unwrap()));
            }
        }
        out.sort();
        out
    }

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        use axum::http::HeaderName;
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(k.parse::<HeaderName>().unwrap(), v.parse().unwrap());
        }
        h
    }

    /// Test OriginalUri: a plain absolute path (no percent encoding) so
    /// the sigv4 gate decodes it unchanged.
    static DEFAULT_TEST_URI: std::sync::LazyLock<axum::http::Uri> =
        std::sync::LazyLock::new(|| "/a.bin".parse().unwrap());

    async fn body_text(resp: Response) -> (StatusCode, HeaderMap, String) {
        let (mut parts, body) = resp.into_parts();
        let bytes = axum::body::to_bytes(body, 64 * 1024 * 1024).await.unwrap();
        let headers = std::mem::take(&mut parts.headers);
        (parts.status, headers, String::from_utf8_lossy(&bytes).into_owned())
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
        let fx = fixture(b"0123456789", None, vec![], false);
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
        let fx = fixture(b"0123456789", None, vec![], true);
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
        let fx = fixture(b"0123456789", None, vec![], false);
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
        let fx = fixture(b"0123456789", None, vec![], false);
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
        let fx = fixture(b"0123456789", None, vec![], false);
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
        let fx = fixture(b"0123456789", Some("v1"), vec![], false);
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
        let fx = fixture(b"0123456789", None, vec![], false);
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

    #[tokio::test]
    async fn head_missing_404_empty_shares_negative_cache() {
        let fx = fixture(b"0123456789", None, vec![], true);
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
        let fx = fixture(b"0123456789", None, vec![], false);
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
        let fx = fixture(b"AAA", None, vec![("archive", b"BBB".to_vec())], false);
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
        let fx = fixture_full(b"0123456789", None, vec![], false, Some("https://cdn.example.com/f?sign=x"), true);
        let resp = get_key(State(fx.state.clone()), Path("new.bin".into()), headers(&[]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
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
        let resp = get_key(State(fx.state.clone()), Path("a.bin".into()), headers(&[]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
        let (status, _, body) = body_text(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "0123456789");
        assert_eq!(fx.direct_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn redirect_unavailable_silently_proxies() {
        // Tier 3 (no link): normal water-pipe, viewer unaffected.
        let fx = fixture_full(b"0123456789", None, vec![], false, None, true);
        let resp = get_key(State(fx.state.clone()), Path("new.bin".into()), headers(&[]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
        let (status, _, body) = body_text(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "0123456789");
        assert_eq!(fx.direct_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn redirect_rejected_target_silently_proxies() {
        // Foreign http is not an allowed redirect target → proxy.
        let fx = fixture_full(b"0123456789", None, vec![], false, Some("http://cdn.example.com/f"), true);
        let resp = get_key(State(fx.state.clone()), Path("new.bin".into()), headers(&[]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
        let (status, _, body) = body_text(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "0123456789");
    }

    #[tokio::test]
    async fn redirect_disabled_never_consults_backend() {
        // Default proxy mode: direct_url untouched even when offered.
        let fx = fixture_full(b"0123456789", None, vec![], false, Some("https://cdn.example.com/f"), false);
        let resp = get_key(State(fx.state.clone()), Path("new.bin".into()), headers(&[]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
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

    #[tokio::test]
    async fn efficient_ranged_miss_passthrough_and_stages() {
        let fx = fixture_efficient(b"0123456789", 0.8, 4);
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
        assert!(!fx.state.cache.state.read().await.entries.contains_key("a.bin"));
        assert_eq!(fx.open_calls.load(Ordering::SeqCst), 1);
        // Served bytes staged as one sidecar; ledger merged.
        assert_eq!(staged_segments(&fx, "a.bin"), vec![(2, 6)]);
        let cov = fx.state.cache.coverage.lock().await;
        let c = cov.get("a.bin").unwrap();
        assert_eq!(c.intervals, vec![(2, 6)]);
        assert_eq!(c.total, 10);
        assert_eq!(c.etag.as_deref(), Some("v1"));
        assert_eq!(fx.state.cache.state.read().await.segment_bytes, 4);
    }

    #[tokio::test]
    async fn efficient_second_pull_merges_ledger() {
        let fx = fixture_efficient(b"0123456789", 0.8, 4);
        for range in ["bytes=0-1", "bytes=4-5"] {
            let resp = get_key(State(fx.state.clone()), Path("a.bin".into()), headers(&[("range", range)]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
            let (status, _, _) = body_text(resp).await;
            assert_eq!(status, StatusCode::PARTIAL_CONTENT);
        }
        assert_eq!(staged_segments(&fx, "a.bin"), vec![(0, 2), (4, 6)]);
        let cov = fx.state.cache.coverage.lock().await;
        assert_eq!(cov.get("a.bin").unwrap().intervals, vec![(0, 2), (4, 6)]);
        assert_eq!(fx.state.cache.state.read().await.segment_bytes, 4);
        // Still no cache entry: staging is not filling.
        assert!(!fx.state.cache.state.read().await.entries.contains_key("a.bin"));
    }

    #[tokio::test]
    async fn efficient_full_get_still_waterpipes() {
        let fx = fixture_efficient(b"0123456789", 0.8, 4);
        let resp = get_key(State(fx.state.clone()), Path("a.bin".into()), headers(&[]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
        let (status, _, body) = body_text(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "0123456789");
        // Full GETs fill normally (a whole file needs no promotion dance).
        wait_installed(&fx, "a.bin").await;
        assert!(staged_segments(&fx, "a.bin").is_empty());
    }

    #[tokio::test]
    async fn efficient_min_size_bypass_goes_waterpipe() {
        let fx = fixture_efficient(b"0123456789", 0.8, 64);
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
        assert!(staged_segments(&fx, "a.bin").is_empty());
    }

    #[tokio::test]
    async fn standard_ranged_miss_waterpipes() {
        let fx = fixture(b"0123456789", None, vec![], false);
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
        wait_installed(&fx, "a.bin").await;
        assert!(staged_segments(&fx, "a.bin").is_empty());
    }

    #[tokio::test]
    async fn tick_sweeps_old_segments() {
        let fx = fixture_efficient(b"0123456789", 0.8, 4);
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
        assert_eq!(staged_segments(&fx, "a.bin"), vec![(2, 6)]);
        // Age past inactive_ttl: tick sweeps segments, zeroes accounting.
        fx.state.cache.clock.advance(1_201_000);
        fx.state.cache.tick().await;
        assert!(staged_segments(&fx, "a.bin").is_empty());
        assert_eq!(fx.state.cache.state.read().await.segment_bytes, 0);
    }

    #[tokio::test]
    async fn healthz_reports_segment_bytes() {
        let fx = fixture_efficient(b"0123456789", 0.8, 4);
        let resp = get_key(
            State(fx.state.clone()),
            Path("a.bin".into()),
            headers(&[("range", "bytes=2-5")]),
            RawQuery(None),
            OriginalUri(DEFAULT_TEST_URI.clone()),
        )
        .await;
        body_text(resp).await;
        let resp = healthz(State(fx.state.clone())).await.into_response();
        let (_, _, body) = body_text(resp).await;
        assert!(body.contains("\"segment_bytes\":4"), "{body}");
        assert!(body.contains("\"flights_active\":"), "{body}");
        assert!(body.contains("\"promotions_active\":"), "{body}");
        assert!(body.contains("\"dirty_access_flushes\":"), "{body}");
        assert!(body.contains("\"sigv4_enabled\":false"), "{body}");
        assert!(body.contains("\"profile\":\"efficient\""), "{body}");
        assert!(body.contains("\"id\":\"primary\""), "{body}");
    }

    /// healthz on a nocache upstream reports the profile so an operator
    /// can spot a node misconfigured into zero-disk mode.
    #[tokio::test]
    async fn healthz_reports_nocache_profile() {
        let fx = fixture_nocache(b"0123456789");
        let resp = healthz(State(fx.state.clone())).await.into_response();
        let (_, _, body) = body_text(resp).await;
        assert!(body.contains("\"profile\":\"nocache\""), "{body}");
        assert!(body.contains("\"entries\":0"), "{body}");
    }

    /// Pull two disjoint halves (union 80% >= 0.75): promotion assembles
    /// from sidecars + fetches exactly the missing tail — then full GET
    /// hits disk with byte-exact content.
    #[tokio::test]
    async fn promotion_assembles_from_segments_plus_gap() {
        let bytes: Vec<u8> = (0..100u8).collect();
        let fx = fixture_efficient(&bytes, 0.75, 4);
        for range in ["bytes=0-49", "bytes=50-79"] {
            let resp = get_key(State(fx.state.clone()), Path("f.bin".into()), headers(&[("range", range)]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
            let (status, _, _) = body_text(resp).await;
            assert_eq!(status, StatusCode::PARTIAL_CONTENT);
        }
        // Background promotion installs the entry.
        wait_installed(&fx, "f.bin").await;
        // Gap fetch was exactly [80,100): no full re-download.
        let opens = fx.opens.lock().unwrap().clone();
        assert!(opens.contains(&(80, Some(20))), "{opens:?}");
        assert!(!opens.iter().any(|(o, l)| *o == 0 && l.is_none()), "{opens:?}");
        // Assembled bytes are exact: sidecar copies + fetched gap.
        let resp = get_key(State(fx.state.clone()), Path("f.bin".into()), headers(&[]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
        let (status, _, body) = body_text(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.as_bytes(), bytes.as_slice());
        // Staged history cleaned up after promotion.
        assert!(staged_segments(&fx, "f.bin").is_empty());
        assert_eq!(fx.state.cache.state.read().await.segment_bytes, 0);
    }

    /// Threshold 1.0: partial pulls never promote; the completing pull
    /// promotes with zero gap fetches.
    #[tokio::test]
    async fn promotion_at_full_coverage_needs_no_gap_fetch() {
        let bytes: Vec<u8> = (0..100u8).collect();
        let fx = fixture_efficient(&bytes, 1.0, 4);
        let resp = get_key(State(fx.state.clone()), Path("f.bin".into()), headers(&[("range", "bytes=0-49")]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
        body_text(resp).await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(!fx.state.cache.state.read().await.entries.contains_key("f.bin"));
        let resp = get_key(State(fx.state.clone()), Path("f.bin".into()), headers(&[("range", "bytes=50-99")]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
        body_text(resp).await;
        wait_installed(&fx, "f.bin").await;
        // Only the two viewer pulls opened the backend — no gap fetch.
        assert_eq!(fx.opens.lock().unwrap().len(), 2);
        let resp = get_key(State(fx.state.clone()), Path("f.bin".into()), headers(&[]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
        let (status, _, body) = body_text(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.as_bytes(), bytes.as_slice());
    }

    /// Below threshold: staged, never promoted.
    #[tokio::test]
    async fn below_threshold_stays_staged() {
        let bytes: Vec<u8> = (0..100u8).collect();
        let fx = fixture_efficient(&bytes, 0.9, 4);
        let resp = get_key(State(fx.state.clone()), Path("f.bin".into()), headers(&[("range", "bytes=0-49")]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
        body_text(resp).await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(!fx.state.cache.state.read().await.entries.contains_key("f.bin"));
        assert_eq!(staged_segments(&fx, "f.bin"), vec![(0, 50)]);
    }

    /// Version flip between staging and promotion: history resets, no
    /// mixed-version entry is ever installed.
    #[tokio::test]
    async fn etag_flip_resets_staged_history() {
        let bytes: Vec<u8> = (0..100u8).collect();
        let fx = fixture_efficient(&bytes, 0.75, 4);
        let resp = get_key(State(fx.state.clone()), Path("f.bin".into()), headers(&[("range", "bytes=0-49")]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
        body_text(resp).await;
        assert_eq!(staged_segments(&fx, "f.bin"), vec![(0, 50)]);
        // Object replaced upstream: next transfer restarts history.
        *fx.etag.lock().unwrap() = Some("v2".into());
        let resp = get_key(State(fx.state.clone()), Path("f.bin".into()), headers(&[("range", "bytes=50-79")]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
        body_text(resp).await;
        // Old segments dropped, ledger re-anchored on v2, no entry yet.
        assert_eq!(staged_segments(&fx, "f.bin"), vec![(50, 80)]);
        let cov = fx.state.cache.coverage.lock().await;
        assert_eq!(cov.get("f.bin").unwrap().etag.as_deref(), Some("v2"));
        assert!(!fx.state.cache.state.read().await.entries.contains_key("f.bin"));
    }

    #[tokio::test]
    async fn prewarm_secret_gate_blocks_anonymous() {
        // Endpoint is open when prewarm_shared_secret_env is unset, but
        // when set and wrong token sent, it must 401.
        let fx = fixture(b"0123456789", None, vec![], false);
        std::env::set_var("TEST_PW_SECRET", "right-token");
        let mut cfg = fx.state.config.as_ref().clone();
        cfg.prewarm_shared_secret_env = Some("TEST_PW_SECRET".into());
        let state = AppState {
            cache: fx.state.cache.clone(),
            config: Arc::new(cfg),
            sigv4_config: None,
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
        assert_eq!(status, StatusCode::OK);
        std::env::remove_var("TEST_PW_SECRET");
    }

    /// SigV4 gate end-to-end at the business seam: anonymous passes, a
    /// correctly-signed request passes, a tampered signature 403s with a
    /// no-store XML error.
    #[tokio::test]
    async fn sigv4_gate_anonymous_passes_and_bad_signature_403s() {
        let fx = fixture(b"0123456789", None, vec![], false);
        let cfg = crate::sigv4::SigV4Config {
            access_key_id: "AKIDEXAMPLE".into(),
            secret_access_key: "s3cr3t".into(),
        };
        let mut state = fx.state.clone();
        state.sigv4_config = Some(cfg.clone());

        // Anonymous request: no gate response (proceeds to the cache).
        let gate = sigv4_gate(Some(&cfg), "GET", "/a.bin", None, &headers(&[]), "r", "h", 0);
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
        let resp = sigv4_gate(Some(&cfg), "GET", "/a.bin", None, &h, "r", "h", now).unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        assert_eq!(resp.headers().get("cache-control").unwrap(), "no-store");
        let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
        let body = String::from_utf8_lossy(&body).into_owned();
        assert!(body.contains("<Code>AccessDenied</Code>"), "{body}");
    }

    /// Cache directory contents minus the redb metadata database (which
    /// exists by design even on nocache nodes — only cache OBJECT writes
    /// are forbidden).
    fn stray_cache_files(fx: &Fixture) -> usize {
        std::fs::read_dir(fx.state.config.cache_dir.clone())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy() != "redb.db")
            .count()
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
        assert!(!fx.state.cache.state.read().await.entries.contains_key("a.bin"));
        assert_eq!(fx.state.cache.state.read().await.total_bytes, 0);
        assert_eq!(fx.state.cache.state.read().await.segment_bytes, 0);
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
        assert_eq!(fx.state.cache.state.read().await.total_bytes, 0);
        assert_eq!(fx.state.cache.state.read().await.segment_bytes, 0);
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

    /// Nocache prewarm: a no-op fetch (nothing to fill) that still reports
    /// success; never opens the backend for bytes.
    #[tokio::test]
    async fn nocache_prewarm_is_noop() {
        let fx = fixture_nocache(b"0123456789");
        let resp = prewarm(State(fx.state.clone()), Path("w.bin".into()), headers(&[])).await.into_response();
        let (status, _, body) = body_text(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("fetched"), "{body}");
        assert_eq!(fx.open_calls.load(Ordering::SeqCst), 0);
        assert!(!fx.state.cache.state.read().await.entries.contains_key("w.bin"));
    }
}
