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
    client_range::{self, ClientRange},
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

    // A malformed or multi Range is refused here; everything else is decided by
    // `client_range`. Note what this does NOT say: the 416 arms below are reached
    // only when `Cache::resolve` has already accepted the key, so a bad range on
    // an invalid key answers about the key. (The comment here used to claim the
    // opposite.)
    let parsed = match client_range::parse(&headers) {
        Err(()) | Ok(ClientRange::Multi) => {
            let hint = state.cache.memory_size(key).await;
            return error_response(BackendError::RangeNotSatisfiable, key, &req_id, &host_id, false, hint);
        }
        Ok(r) => r,
    };
    let range = match parsed {
        ClientRange::Absent => None,
        // A single range goes down unresolved: the serve path clamps it against
        // the size its own version gate just confirmed.
        ClientRange::Single(r) => Some(r),
        ClientRange::Multi => unreachable!("rejected above"),
        // A suffix has to become a range before it can be served, so the size
        // comes first: one lightweight stat (memory or a single PROPFIND — never
        // a flight).
        ClientRange::Suffix(_) => {
            let size = match state.cache.head_resolved(&rk).await {
                Ok(m) => m.size,
                Err(e) => {
                    return error_response(e, key, &req_id, &host_id, false, None);
                }
            };
            match client_range::resolve(&parsed, size) {
                client_range::RequestedSpan::Span { offset, len } => {
                    Some(ByteRange::bounded(offset, len))
                }
                _ => {
                    return error_response(
                        BackendError::RangeNotSatisfiable,
                        key,
                        &req_id,
                        &host_id,
                        false,
                        Some(size),
                    );
                }
            }
        }
    };

    // One decision, rendered here. The serve-mode order (relief valve →
    // nocache → efficient passthrough → cached) lives in `Cache::serve`,
    // because it is profile semantics and profile semantics had four homes.
    let viewer_ua = headers.get("user-agent").and_then(|v| v.to_str().ok());
    match state.cache.serve(&rk, range, viewer_ua).await {
        Ok(ServeOutcome::Redirect { location }) => redirect_response(location, &req_id, &host_id),
        Ok(ServeOutcome::Stream(served)) => {
            // The read protection travels WITH the response: `serve` takes the
            // key's lease and its watch when it builds the plan — the span is the
            // response's own byte range, and only there is the byte source known
            // — so this handler cannot forget to hold them and the body cannot
            // forget to drop them.
            //
            // It used to be a protocol between two files: this one reached into
            // `cache.leases` and `cache.watches`, built both guards, and passed
            // them down for `instrument_body` to hold, while the cache itself
            // depended on them (the version gate defers a ledger reset only if a
            // guard exists). An invariant the interface cannot express is one a
            // new caller loses silently.
            stream_response(served, &req_id, &host_id, started)
        }
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
    served: crate::cache::cache::Served,
    req_id: &str,
    host_id: &str,
    started: std::time::Instant,
) -> Response {
    let crate::cache::cache::Served { plan, lease, watch } = served;
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
        .body(Body::from_stream(instrument_body(plan.body, source, started, lease, watch)))
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
    lease: crate::cache::leases::LeaseGuard,
    watch: crate::cache::watch::WatchGuard,
) -> crate::cache::flight::BodyStream {
    use futures::StreamExt;
    Box::pin(async_stream::try_stream! {
        // The lease lives exactly as long as this body: hyper drops the stream
        // when the viewer goes away (or once `content-length` is satisfied),
        // and the guard's drop is what ends the protection (ADR-0017). The
        // watch is the same shape with a longer memory — it survives this body
        // for its idle budget (ADR-0018).
        let _lease = lease;
        let _watch = watch;
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
    let parsed = match client_range::parse(&headers) {
        Err(()) | Ok(ClientRange::Multi) => {
            let hint = state.cache.memory_size(key).await;
            return error_response(BackendError::RangeNotSatisfiable, key, &req_id, &host_id, true, hint);
        }
        Ok(r) => r,
    };
    // The stat serves both freshness and size — exactly one upstream call at
    // most — and HEAD resolves every form against it, because the response needs
    // the length and the content-range rather than a body.
    let meta = match state.cache.head_resolved(&rk).await {
        Ok(m) => m,
        Err(e) => {
            return error_response(e, key, &req_id, &host_id, true, None);
        }
    };
    let (len, content_range) = match client_range::resolve(&parsed, meta.size) {
        client_range::RequestedSpan::Whole => (meta.size, None),
        client_range::RequestedSpan::Span { offset, len } => {
            (len, ContentRange::for_span(meta.size, offset, offset + len, true))
        }
        client_range::RequestedSpan::Unsatisfiable => {
            return error_response(BackendError::RangeNotSatisfiable, key, &req_id, &host_id, true, Some(meta.size));
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

async fn healthz<C>(State(state): State<AppState<C>>, RawQuery(query): RawQuery) -> impl IntoResponse
where
    C: Clock + Clone,
{
    // One key on demand (`?key=<raw key>`): the operator view of a single
    // object — is it installed, what is staged for it, who is watching it —
    // so answering "why is this thing being refetched?" does not need a
    // debugger. Absent the parameter the body is unchanged.
    let key_view = match query.as_deref().and_then(key_param) {
        Some(key) => Some(key_state_json(&state.cache.inspect(&key).await)),
        None => None,
    };
    // Live-machinery view (spec §8: the queue-ish counters operators watch
    // when a node misbehaves) — read through the Cache snapshot, not the
    // internals (C4).
    let snap = state.cache.snapshot().await;
    let (count, bytes, segment_bytes, stray_bytes) =
        (snap.entries, snap.total_bytes, snap.segment_bytes, snap.stray_bytes);
    let flights = snap.flights_active;
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
                // Two states, because there are two profiles (ADR-0022): the
                // retired full-file water-pipe had no flag of its own, and no
                // resolution path can produce it any more.
                "profile": if prof.nocache { "nocache" } else { "efficient" },
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
    let mut body = json!({
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
    });
    if let Some(k) = key_view {
        body["key"] = k;
    }
    (StatusCode::OK, Json(body))
}

/// The `key=` parameter of a query string, percent-decoded (`media/a b.bin`
/// arrives as `media%2Fa+b.bin`). `form_urlencoded` rather than a split on `=`
/// for exactly that reason: a key is arbitrary bytes and the query is not.
fn key_param(query: &str) -> Option<String> {
    form_urlencoded::parse(query.as_bytes())
        .find(|(k, _)| k == "key")
        .map(|(_, v)| v.into_owned())
        .filter(|v| !v.is_empty())
}

/// [`crate::cache::cache::KeyState`] as JSON: the operator view, with the
/// disk's view and the ledger's view kept apart because they answer different
/// questions (what exists vs. what the policy believes).
fn key_state_json(k: &crate::cache::cache::KeyState) -> serde_json::Value {
    let spans = |v: &[(u64, u64)]| -> Vec<serde_json::Value> {
        v.iter().map(|(s, e)| json!({"start": s, "end": e})).collect()
    };
    json!({
        "installed": k.installed,
        "tombstone": k.tombstone,
        "entry_bytes": k.entry_bytes,
        "oversize": k.oversize,
        "last_access_millis": k.last_access_millis,
        "staged_spans": spans(&k.staged_spans),
        "staged_bytes": k.staged_bytes,
        "ledger_spans": k
            .ledger_spans
            .iter()
            .map(|s| json!({
                "start": s.start,
                "end": s.end,
                "last_read_millis": s.last_read_millis,
                "reads": s.reads,
            }))
            .collect::<Vec<_>>(),
        "ledger_total": k.ledger_total,
        "ledger_etag": k.ledger_etag,
        "ledger_last_touch_millis": k.ledger_last_touch_millis,
        "pin": k.pin.map(|(s, e, a)| json!({"start": s, "end": e, "anchor": a})),
        "leased": k.leased,
    })
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
        // Compared in constant time: the token is a shared secret sent on every
        // request, and `==` on a String stops at the first differing byte.
        // Equal-length inputs only is fine here — `constant_time_eq` returns
        // false for a length mismatch, which is the answer anyway.
        if expected.is_empty() || !crate::sigv4::constant_time_eq(got.as_bytes(), expected.as_bytes()) {
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
#[path = "business/tests.rs"]
mod tests;
