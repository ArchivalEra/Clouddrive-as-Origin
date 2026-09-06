use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde_json::json;
use std::{net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::{
    backend::{BackendError, ByteRange},
    cache::cache::{Cache, CacheOutcome},
    clock::Clock,
    config::Config,
    key::validate_key,
};

#[derive(Clone)]
pub struct AppState<C: Clock + Clone> {
    pub cache: Arc<Cache<C>>,
    pub config: Arc<Config>,
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

fn client_range(headers: &HeaderMap) -> Option<ByteRange> {
    let raw = headers.get("range").and_then(|v| v.to_str().ok())?;
    // Strict RFC-9110 parsing (wheel reuse: http-range-header, same crate
    // tower-http's range logic is built around). Multi-range requests are
    // served whole with 200 — a server MAY ignore Range per the spec.
    let parsed = http_range_header::parse_range_header(raw).ok()?;
    parsed.validate(u64::MAX).ok()?.into_iter().next().map(|r| {
        let start = *r.start();
        let end_excl = *r.end() + 1;
        ByteRange::bounded(start, end_excl - start)
    })
}

async fn get_key<C>(State(state): State<AppState<C>>, Path(key): Path<String>, headers: HeaderMap) -> Response
where
    C: Clock + Clone,
{
    let range = client_range(&headers);
    match state.cache.get(&key, range).await {
        Ok(hit) => {
            let status =
                if hit.content_range.is_some() { StatusCode::PARTIAL_CONTENT } else { StatusCode::OK };
            info!(key = %key, outcome = ?hit.outcome, size = hit.meta.size, "cache response");

            let mut builder = Response::builder().status(status);
            if let Some(cr) = &hit.content_range {
                builder = builder.header("content-range", cr);
            }
            builder = builder.header("cache-control", "public, max-age=31536000, immutable");
            if let Some(ct) = &hit.meta.content_type {
                builder = builder.header("content-type", ct);
            }
            if let Some(et) = &hit.meta.etag {
                builder = builder.header("etag", et);
            }
            if hit.outcome == CacheOutcome::Stale {
                builder = builder.header("warning", "110 - \"Response is Stale\"");
            }
            builder.body(Body::from_stream(hit.body)).unwrap()
        }
        Err(BackendError::NotFound) => {
            (StatusCode::NOT_FOUND, Json(json!({"error": "not found"}))).into_response()
        }
        Err(BackendError::RangeNotSatisfiable) => {
            (StatusCode::RANGE_NOT_SATISFIABLE, Json(json!({"error": "range not satisfiable"}))).into_response()
        }
        Err(BackendError::RateLimited { retry_after_millis }) => {
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
        Err(BackendError::Other(msg))
            if msg.contains("traversal") || msg.contains("Empty") || msg.contains("Absolute") =>
        {
            (StatusCode::BAD_REQUEST, Json(json!({"error": msg}))).into_response()
        }
        Err(e) => {
            warn!(key = %key, error = %e, "cache fetch error");
            (StatusCode::BAD_GATEWAY, Json(json!({"error": "upstream error"}))).into_response()
        }
    }
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
        .route("/{*key}", get(get_key::<C>).head(get_key::<C>))
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
