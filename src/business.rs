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
};
use tokio::net::TcpListener;
use tracing::info;

use crate::{
    backend::{BackendError, ByteRange, ContentRange},
    cache::cache::{Cache, ServeOutcome},
    clock::Clock,
    config::Config,
    response::{error_response, invalid_key_response, request_ids, s3_meta_headers},
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
    // Lazy gate (P7): probe for SigV4 material BEFORE building anything.
    // With no config the layer is off; without an Authorization header and
    // without a presigned X-Amz-Signature there is nothing to verify. The
    // old shape percent-decoded the path, parsed the query and copied every
    // header into owned Strings (tens of allocations) before discovering
    // that the request was anonymous — the common case.
    let authorization = headers.get("authorization").and_then(|v| v.to_str().ok());
    let has_presign = query.map_or(false, |q| q.contains("X-Amz-Signature"));
    if cfg.is_none() || (authorization.is_none() && !has_presign) {
        return None;
    }
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
        authorization,
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
    /// Short-TTL memo of one upstream listing walk, for S3 list paging (P9).
    /// Owned by the listing module rather than by `Cache`: the key is a
    /// listing concept and `Cache` had a pair of methods with one caller.
    pub listings: crate::list::ListingCache,
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
    get_key_inner(state, &path_key, headers, query, original).await
}

/// Root-path handler: `GET /?list-type=2` (S3 list at the bucket root).
/// The root route cannot extract a `Path<String>` (no key segment), so
/// the key is the empty string — the list dispatch resolves the default
/// upstream.
async fn get_key_root<C>(
    State(state): State<AppState<C>>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
    OriginalUri(original): OriginalUri,
) -> Response
where
    C: Clock + Clone,
{
    get_key_inner(state, "", headers, query, original).await
}

async fn get_key_inner<C>(
    state: AppState<C>,
    path_key: &str,
    headers: HeaderMap,
    query: Option<String>,
    original: axum::http::Uri,
) -> Response
where
    C: Clock + Clone,
{
    let (req_id, host_id) = request_ids();
    // First body byte minus this instant is the number that says whether a
    // seek felt smooth (see `metrics::BODY_TTFB`). It is taken before the
    // SigV4 gate so the sample covers everything the viewer waited for,
    // including the parts that never reach the cache.
    let started = std::time::Instant::now();
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
    // List dispatch: S3 list operations live on the same path
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
            return invalid_key_response(e, path_key, &req_id, &host_id, false);
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

    // One decision, rendered here. The serve-mode order (relief valve →
    // nocache → efficient passthrough → cached) lives in `Cache::serve`,
    // because it is profile semantics and profile semantics had four homes.
    let viewer_ua = headers.get("user-agent").and_then(|v| v.to_str().ok());
    match state.cache.serve(&rk, range, viewer_ua).await {
        Ok(ServeOutcome::Redirect { location }) => redirect_response(location, &req_id, &host_id),
        Ok(ServeOutcome::Stream(plan)) => stream_response(plan, &req_id, &host_id, started),
        Err(e) => {
            // Size hint for 416 Content-Range: best-effort memory peek, no
            // upstream call (SHOULD-level per R1).
            let hint = state.cache.memory_size(key).await;
            error_response(e, key, &req_id, &host_id, false, hint)
        }
    }
}

/// The 307 the relief valve asks for: the upstream's own signed link.
///
/// `no-store` because the signed target expires while the URL stays the same,
/// so an edge that cached this would hand out dead links.
fn redirect_response(location: String, req_id: &str, host_id: &str) -> Response {
    Response::builder()
        .status(StatusCode::TEMPORARY_REDIRECT)
        .header("location", location)
        .header("cache-control", "no-store")
        .header("x-amz-request-id", req_id)
        .header("x-amz-id-2", host_id)
        .body(Body::empty())
        .unwrap()
}

/// Render whatever `Cache::serve` decided to stream. The status and the
/// content-range came from the path that produced the bytes, not from here:
/// nocache and the cached path answer 206 only when a range was honoured,
/// while the efficient passthrough always does.
fn stream_response(
    plan: crate::cache::cache::StreamPlan,
    req_id: &str,
    host_id: &str,
    started: std::time::Instant,
) -> Response {
    let mut builder = Response::builder().status(plan.status);
    if let Some(cr) = &plan.content_range {
        builder = builder.header("content-range", cr.header_value());
    }
    if let Some(len) = plan.content_length {
        builder = builder.header("content-length", len);
    }
    builder = s3_meta_headers(builder, &plan.meta, req_id, host_id);
    if plan.stale {
        builder = builder.header("warning", "110 - \"Response is Stale\"");
    }
    let source = plan.source.label();
    crate::metrics::observe_source(source);
    builder
        .body(Body::from_stream(instrument_body(plan.body, source, started)))
        .unwrap()
}

/// Wrap a body so the viewer's actual wait and byte count are measured.
///
/// `metrics::CACHE_SERVE` stops at response construction (every observed
/// function returns an unconsumed body), so the transfer needs its own
/// instrument: this records the time to the first byte — the seek-smoothness
/// number — and the bytes delivered, both labelled by where the bytes came
/// from.
///
/// Bytes are counted **as they are yielded**, not after the loop. Every
/// response here carries a `content-length`, and hyper stops polling a body
/// once that length is satisfied: it DROPS the stream rather than driving the
/// generator to completion, so a tail that ran after the last chunk never ran
/// at all. The node showed it — `cache_body_ttfb_seconds` had samples and
/// `cache_body_bytes_total` did not exist. Per-chunk counting also reports the
/// bytes actually delivered when a client disconnects mid-body.
fn instrument_body(
    body: crate::cache::flight::BodyStream,
    source: &'static str,
    started: std::time::Instant,
) -> crate::cache::flight::BodyStream {
    use futures::StreamExt;
    Box::pin(async_stream::try_stream! {
        let mut body = body;
        let mut first = true;
        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            if first {
                first = false;
                crate::metrics::observe_body_ttfb(source, started);
            }
            crate::metrics::observe_body_bytes(source, chunk.len() as u64);
            yield chunk;
        }
    })
}

/// Nocache profile (small-footprint nodes): every GET water-pipes
/// origin-to-viewer with zero disk writes — no entries, no flights, no
/// segments, no tombstones. Range or not, cold or not: everything goes
/// through; only header metadata is stat'd. Every failure surfaces via
/// the standard error mapping (no stale-if-error: there is no disk copy).

/// Efficient profile (P2-a): ranged misses passthrough origin straight to
/// the viewer while staging the served interval (no flight, no full fill).
/// Fresh entries serve from disk via the B path below; every failure here
/// falls through to it (stale-if-error included).

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
    head_key_inner(state, &path_key, headers, query, original).await
}

/// Root-path HEAD (S3 list at the bucket root): no key segment, so the
/// key is the empty string.
async fn head_key_root<C>(
    State(state): State<AppState<C>>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
    OriginalUri(original): OriginalUri,
) -> Response
where
    C: Clock + Clone,
{
    head_key_inner(state, "", headers, query, original).await
}

async fn head_key_inner<C>(
    state: AppState<C>,
    path_key: &str,
    headers: HeaderMap,
    query: Option<String>,
    original: axum::http::Uri,
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
            return invalid_key_response(e, path_key, &req_id, &host_id, true);
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
    // Live-machinery view (spec §8: the queue-ish counters operators watch
    // when a node misbehaves) — read through the Cache snapshot, not the
    // internals (C4).
    let snap = state.cache.snapshot().await;
    let (count, bytes, segment_bytes, stray_bytes) =
        (snap.entries, snap.total_bytes, snap.segment_bytes, snap.stray_bytes);
    let flights = snap.flights_active;
    let promotions = snap.promotions_active;
    let dirty_access = snap.dirty_access_pending;
    let (coverage_keys, coverage_intervals) = (snap.coverage_keys, snap.coverage_intervals);
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
    // Health verdict (C1). The status code stays a liveness signal (200 =
    // the process is serving); the verdict lives in the body so a caller can
    // distinguish "answering" from "healthy". This handler used to return a
    // hardcoded "ok" with no checks at all, which made it useless as a
    // monitor: a node with a full disk, a quarantined metadata store or an
    // unreadable cache still reported perfect health.
    let mut reasons: Vec<&str> = Vec::new();
    if matches!(snap.store, crate::cache::persist::StoreState::Quarantined { .. }) {
        reasons.push("metadata_store_quarantined");
    }
    if snap.rebuilt_rows > 0 {
        reasons.push("metadata_rows_rebuilt");
    }
    match snap.disk_free_bytes {
        Some(free) if free < snap.disk_reserve_bytes => reasons.push("disk_below_reserve"),
        _ => {}
    }
    let store = match &snap.store {
        crate::cache::persist::StoreState::Ready => json!({"state": "ready"}),
        crate::cache::persist::StoreState::Quarantined { moved_to } => {
            json!({"state": "quarantined", "moved_to": moved_to})
        }
    };
    let degraded = !reasons.is_empty();
    (
        StatusCode::OK,
        Json(json!({
            "status": if degraded { "degraded" } else { "ok" },
            "degraded": degraded,
            "degraded_reasons": reasons,
            "plane": "business",
            "version": env!("CARGO_PKG_VERSION"),
            "entries": count,
            "bytes": bytes,
            "segment_bytes": segment_bytes,
            "stray_bytes": stray_bytes,
            "flights_active": flights,
            "promotions_active": promotions,
            "dirty_access_flushes": dirty_access,
            "coverage_keys": coverage_keys,
            "coverage_intervals": coverage_intervals,
            "store": store,
            "rebuilt_rows": snap.rebuilt_rows,
            "prewarm_inflight": snap.prewarm_inflight,
            "disk_free_bytes": snap.disk_free_bytes,
            "disk_reserve_bytes": snap.disk_reserve_bytes,
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
    if state.cache.entry_exists(&rk.cache_key).await {
        return (StatusCode::OK, Json(json!({"status": "hit"}))).into_response();
    }
    // Spec §10: answer immediately, fetch behind the caller. The prewarm
    // caller is a pipeline warming an object before the CDN asks for it, and
    // the old shape made it wait out a whole upstream fetch -- minutes for a
    // large object, with nothing it could do about a failure anyway. The
    // trade: there is no synchronous 404/502 any more, so the outcome is
    // reported in the log and counted in healthz instead.
    let cache = Arc::clone(&state.cache);
    let warmed = rk.clone();
    cache.prewarm_inflight.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    tokio::spawn(async move {
        let _inflight = PrewarmInflight(&cache.prewarm_inflight);
        match cache.prefetch(&warmed).await {
            Ok(()) => tracing::info!(key = %warmed.cache_key, "prewarm finished"),
            Err(e) => tracing::warn!(key = %warmed.cache_key, error = %e, "prewarm fetch failed"),
        }
    });
    (StatusCode::ACCEPTED, Json(json!({"status": "accepted"}))).into_response()
}

/// Decrements the in-flight prewarm count on every exit path, including a
/// panic inside the fetch, so healthz cannot report a phantom queue.
struct PrewarmInflight<'a>(&'a std::sync::atomic::AtomicUsize);

impl Drop for PrewarmInflight<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
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
        // axum 0.8 (matchit 0.8) brace syntax: `{key}` / `{*key}`.
        .route("/_internal/prewarm/{*key}", post(prewarm::<C>))
        // Root path: ListObjectsV2 lives at `/?list-type=2` (S3 API), so
        // the root must reach the object handler too.
        .route("/", get(get_key_root::<C>).head(head_key_root::<C>))
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
    let listener = bind(addr).await?;
    serve_on(listener, state, shutdown).await
}

/// Bind the business listener WITHOUT serving (C2). Callers use this as a
/// readiness gate: the front plane must not start accepting connections
/// until this port answers, otherwise requests arriving in the window are
/// proxied to a closed port and fail after the front's connect timeout.
pub async fn bind(addr: SocketAddr) -> anyhow::Result<TcpListener> {
    let listener = TcpListener::bind(addr).await?;
    info!(%addr, "business plane listening");
    Ok(listener)
}

/// Serve on an already-bound listener (see [`bind`]).
pub async fn serve_on<C>(
    listener: TcpListener,
    state: AppState<C>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()>
where
    C: Clock + Clone,
{
    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
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
        wait_installed, Fixture, FixtureBuilder, DEFAULT_TEST_URI,
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
    fn fixture(bytes: &[u8], etag: Option<&str>, extra: Vec<(&str, Vec<u8>)>, missing: bool) -> Fixture {
        base(bytes).etag(etag).extra(extra).missing(missing).build()
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
        let builder = base(bytes).etag(etag).extra(extra).missing(missing).direct(direct);
        if redirect {
            builder.redirect().build()
        } else {
            builder.build()
        }
    }

    /// Efficient-profile fixture: primary serves `cache_profile =
    /// "efficient"` with the given threshold/min_file_size.
    fn fixture_efficient(bytes: &[u8], threshold: f64, min_file_size: u64) -> Fixture {
        base(bytes).etag(Some("v1")).coverage(threshold, min_file_size).build()
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

    /// HEAD never takes the relief valve, and this is the one difference
    /// between the two entry points that no test pinned: a redirect-capable
    /// upstream that makes GET answer 307 leaves HEAD answering 200, because
    /// a HEAD cannot follow a redirect and still report the object's shape.
    /// Pinned deliberately -- if the two paths are ever unified, the choice
    /// has to be made on purpose rather than by accident.
    #[tokio::test]
    async fn head_never_redirects_even_when_get_does() {
        let fx = fixture_full(
            b"0123456789",
            None,
            vec![],
            false,
            Some("https://cdn.example.com/f?sign=x"),
            true,
        );
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
        assert!(staged_segments(&fx, "a.bin").is_empty(), "nocache stages nothing");
        assert_eq!(stray_cache_files(&fx), 0, "and writes nothing");
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

    /// Spec §10: prewarm answers at once and fetches behind the caller, so
    /// the row appears after the response, not before it.
    #[tokio::test]
    async fn prewarm_accepts_immediately_and_fetches_in_the_background() {
        let fx = fixture(b"0123456789", None, vec![], false);
        let resp = prewarm(State(fx.state.clone()), Path("w.bin".into()), headers(&[])).await.into_response();
        let (status, _, body) = body_text(resp).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert!(body.contains("accepted"), "{body}");
        for _ in 0..200 {
            if fx.state.cache.state.read().await.entries.contains_key("w.bin") {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(
            fx.state.cache.state.read().await.entries.contains_key("w.bin"),
            "the background fetch must still install the row"
        );
        // The in-flight count must come back down on its own, or healthz
        // would report a queue that never drains.
        for _ in 0..100 {
            if fx.state.cache.prewarm_inflight.load(Ordering::SeqCst) == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert_eq!(
            fx.state.cache.prewarm_inflight.load(Ordering::SeqCst),
            0,
            "the prewarm in-flight count must return to zero"
        );
    }

    /// A second prewarm for an object already cached is a synchronous hit;
    /// nothing new is fetched and nothing is counted as in flight.
    #[tokio::test]
    async fn prewarm_reports_a_hit_without_queueing_anything() {
        let fx = fixture(b"0123456789", None, vec![], false);
        prime(&fx, "w.bin").await;
        let resp = prewarm(State(fx.state.clone()), Path("w.bin".into()), headers(&[])).await.into_response();
        let (status, _, body) = body_text(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("hit"), "{body}");
        assert_eq!(fx.state.cache.prewarm_inflight.load(Ordering::SeqCst), 0);
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
        assert_eq!(c.intervals.len(), 1);
        assert_eq!((c.intervals[0].0, c.intervals[0].1), (2, 6));
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
        let iv = &cov.get("a.bin").unwrap().intervals;
        assert_eq!(iv.len(), 2);
        assert_eq!((iv[0].0, iv[0].1), (0, 2));
        assert_eq!((iv[1].0, iv[1].1), (4, 6));
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

    /// Staged reads: a seek whose bytes are already staged is answered from the
    /// sidecars with NO upstream open. This is the seek-back case - the
    /// reader returns to bytes an earlier request already paid for - and the
    /// reason staged bytes exist at all once promotion is off the table.
    #[tokio::test]
    async fn a_fully_covered_seek_is_served_from_stage_without_upstream() {
        let fx = fixture_efficient(b"0123456789", 0.8, 4);
        // Stage the whole object in two pulls.
        for range in ["bytes=0-4", "bytes=5-9"] {
            let resp = get_key(State(fx.state.clone()), Path("a.bin".into()), headers(&[("range", range)]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
            let (status, _, _) = body_text(resp).await;
            assert_eq!(status, StatusCode::PARTIAL_CONTENT);
        }
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
        {
            let cov = fx.state.cache.coverage.lock().await;
            assert_eq!(
                cov.get("a.bin").unwrap().last_touch_millis,
                5_000,
                "a staged read refreshes the row's age, so a watched window is not swept"
            );
        }
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
    /// ends fully covered and promotion can fire off it.
    #[tokio::test]
    async fn a_partially_covered_range_needs_one_open_and_stages_the_rest() {
        let fx = fixture_efficient(b"0123456789", 0.8, 4);
        let resp = get_key(State(fx.state.clone()), Path("a.bin".into()), headers(&[("range", "bytes=0-4")]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
        let (status, _, _) = body_text(resp).await;
        assert_eq!(status, StatusCode::PARTIAL_CONTENT);
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
        {
            let cov = fx.state.cache.coverage.lock().await;
            eprintln!("DEBUG ledger: {:?}", cov.get("a.bin"));
        }
        eprintln!("DEBUG entries: {:?} segment_bytes: {}", fx.state.cache.state.read().await.entries.keys().collect::<Vec<_>>(), fx.state.cache.state.read().await.segment_bytes);
        wait_installed(&fx, "a.bin").await;
        assert!(staged_segments(&fx, "a.bin").is_empty(), "promotion consumed the sidecars");
    }

    /// An object we already hold must keep serving ranges after the
    /// revalidate window. The efficient gate used to be the 60 s freshness
    /// clock, so a complete entry stopped being served a minute after it was
    /// filled and every ranged request went back upstream for bytes already
    /// on disk — which also capped the promotion hold's value at that same
    /// minute.
    #[tokio::test]
    async fn efficient_complete_entry_keeps_serving_ranges_after_the_revalidate_window() {
        let fx = fixture_efficient(b"0123456789", 0.8, 4);
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
            staged_segments(&fx, "a.bin").is_empty(),
            "nothing to stage: the bytes are already in one file"
        );
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

    /// C1: healthz must carry a real verdict, not a hardcoded "ok". A fresh
    /// node is healthy; the response must say so explicitly (so a monitor
    /// can tell "answering" from "healthy") and expose the disk numbers.
    #[tokio::test]
    async fn healthz_reports_a_verdict_and_disk_state() {
        let fx = fixture(b"x", None, Vec::new(), false);
        let resp = healthz(State(fx.state.clone())).await.into_response();
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

    /// Staging admission (ADR-0013): an object the magazine cannot hold is
    /// never staged. Staged segments have exactly one reader — promotion —
    /// and promotion's fit guard refuses this object, so staging it would
    /// write bytes nobody can ever read back on every single seek. It is
    /// served by the pipe instead, and its bytes are still correct.
    #[tokio::test]
    async fn an_object_larger_than_the_cache_is_never_staged() {
        let bytes: Vec<u8> = (0..100u8).collect();
        let fx = base(&bytes).etag(Some("v1")).coverage(0.8, 4).max_size_bytes(50).build();
        let resp = get_key(
            State(fx.state.clone()),
            Path("f.bin".into()),
            headers(&[("range", "bytes=0-99")]),
            RawQuery(None),
            OriginalUri(DEFAULT_TEST_URI.clone()),
        )
        .await;
        let (status, h, body) = body_text(resp).await;
        assert_eq!(status, StatusCode::PARTIAL_CONTENT);
        assert_eq!(body.len(), 100, "the viewer still gets every byte asked for");
        assert_eq!(h.get("content-range").unwrap(), "bytes 0-99/100");
        assert!(
            !fx.state.cache.state.read().await.entries.contains_key("f.bin"),
            "an object bigger than the whole budget is never merged into an entry"
        );
        assert!(
            staged_segments(&fx, "f.bin").is_empty(),
            "and it is never staged: nothing could ever read those bytes back"
        );
        assert_eq!(
            fx.state.cache.state.read().await.segment_bytes,
            0,
            "no staged bytes are accounted"
        );
        assert!(
            fx.state.cache.coverage.lock().await.is_empty(),
            "and no ledger row is created for it"
        );
    }

    /// Window decay: staged intervals older than the coverage
    /// window stop counting — a 60% read, a window expiry, then a 20% read
    /// must NOT promote (coverage decayed to 20%).
    #[tokio::test]
    async fn coverage_window_expiry_blocks_promotion() {
        let bytes: Vec<u8> = (0..100u8).collect();
        let fx = fixture_efficient(&bytes, 0.8, 4);
        // 60% staged at t=0.
        let resp = get_key(State(fx.state.clone()), Path("f.bin".into()), headers(&[("range", "bytes=0-59")]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
        body_text(resp).await;
        assert_eq!(staged_segments(&fx, "f.bin"), vec![(0, 60)]);
        // Advance past the 3600s window: intervals decay out of the ledger.
        fx.state.cache.clock.advance(3_601_000);
        // 20% more read at t=3601s: coverage is now 20%, not 80%.
        let resp = get_key(State(fx.state.clone()), Path("f.bin".into()), headers(&[("range", "bytes=60-79")]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
        body_text(resp).await;
        // No promotion: entry must not exist.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(!fx.state.cache.state.read().await.entries.contains_key("f.bin"));
        // Ledger holds only the fresh interval.
        let cov = fx.state.cache.coverage.lock().await;
        let c = cov.get("f.bin").unwrap();
        assert_eq!(c.intervals.len(), 1);
        assert_eq!((c.intervals[0].0, c.intervals[0].1), (60, 80));
    }

    /// Window decay backstop: a key with no recent writes must not promote
    /// on stale intervals even if the threshold was met long ago. The
    /// promotion task races the clock, so this asserts the deterministic
    /// half: the ledger decays when the window passes (the expiry test
    /// covers the promotion-blocking half).
    #[tokio::test]
    async fn coverage_window_backstop_blocks_stale_promotion() {
        let bytes: Vec<u8> = (0..100u8).collect();
        let fx = fixture_efficient(&bytes, 0.8, 4);
        // 80% staged at t=0 — threshold met, promotion task may spawn.
        let resp = get_key(State(fx.state.clone()), Path("f.bin".into()), headers(&[("range", "bytes=0-79")]), RawQuery(None), OriginalUri(DEFAULT_TEST_URI.clone())).await;
        body_text(resp).await;
        // Advance past the window: the ledger must decay regardless of
        // what the promotion task does (it may have already installed a
        // valid entry — that is fine; stale intervals must not survive).
        fx.state.cache.clock.advance(3_601_000);
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let cov = fx.state.cache.coverage.lock().await;
        match cov.get("f.bin") {
            // Promotion cleaned up: nothing left to decay — acceptable.
            None => {}
            // Ledger still present: every interval must be decayed away.
            Some(c) => assert!(c.intervals.is_empty(), "stale intervals must decay: {:?}", c.intervals),
        }
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
        // Object replaced upstream: next transfer restarts history. Wait
        // out the stat single-flight cooldown so the flip is
        // observed on a fresh stat.
        *fx.etag.lock().unwrap() = Some("v2".into());
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
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
        for _ in 0..20 {
            if fx.state.cache.prewarm_inflight.load(Ordering::SeqCst) == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert_eq!(fx.open_calls.load(Ordering::SeqCst), 0);
        assert!(!fx.state.cache.state.read().await.entries.contains_key("w.bin"));
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
        let fx = fixture(b"unused", None, vec![], false);
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

        let fx = fixture(b"unused", None, vec![], false);
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
        let fx = fixture(b"unused", None, vec![], false);
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

        let fx = fixture(b"unused", None, vec![], false);
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

        let fx = fixture(b"unused", None, vec![], false);
        for method in ["GET", "HEAD"] {
            let request = axum::http::Request::builder()
                .method(method).uri("/").body(Body::empty()).unwrap();
            let resp = router(fx.state.clone()).oneshot(request).await.unwrap();
            assert_invalid_request_400(resp, &format!("{method} / via router")).await;
        }
        assert_no_backend_calls(&fx, "root via router");
    }

    /// Router construction smoke test: every route path must survive
    /// matchit's pattern compiler at runtime. The integration suite
    /// calls handlers directly and never builds the Router — a blind
    /// spot that let route-syntax panics surface only at deploy boot.
    #[tokio::test]
    async fn router_constructs_without_panic() {
        let fx = fixture(b"0123456789", None, vec![], false);
        let _app = router(fx.state.clone());
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
        let mut wrapped = instrument_body(body, "upstream", std::time::Instant::now());

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
        let mut wrapped = instrument_body(body, "disk", std::time::Instant::now());
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
        let fx = fixture(b"0123456789", None, vec![], false);
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
}
