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
    cache::cache::{Cache, Fetcher},
    clock::Clock,
    config::Config,
    key::validate_key,
};

#[derive(Clone)]
pub struct AppState<C: Clock + Clone, F: Fetcher + Clone> {
    pub cache: Arc<Cache<C, F>>,
    pub config: Arc<Config>,
}

async fn healthz<C, F>(State(state): State<AppState<C, F>>) -> impl IntoResponse
where
    C: Clock + Clone,
    F: Fetcher + Clone,
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

async fn get_key<C, F>(
    State(state): State<AppState<C, F>>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> Response
where
    C: Clock + Clone,
    F: Fetcher + Clone,
{
    let is_head = false;
    match state.cache.get(&key).await {
        Ok((bytes, outcome)) => {
            let range = headers.get("range").and_then(|v| v.to_str().ok());
            let (body_bytes, status, content_range) = if let Some(range) = range {
                if let Some((start, end)) = parse_range(range, bytes.len() as u64) {
                    let end = end.unwrap_or(bytes.len() as u64);
                    let slice = bytes[start as usize..end as usize].to_vec();
                    let cr = format!("bytes {}-{}/{}", start, end - 1, bytes.len());
                    (slice, StatusCode::PARTIAL_CONTENT, Some(cr))
                } else {
                    (bytes, StatusCode::OK, None)
                }
            } else {
                (bytes, StatusCode::OK, None)
            };

            info!(key = %key, outcome = ?outcome, bytes = body_bytes.len(), "cache response");

            let mut builder = Response::builder()
                .status(status)
                .header("cache-control", "public, max-age=31536000, immutable");
            if let Some(cr) = content_range {
                builder = builder.header("content-range", cr);
            }
            let content_type = {
                let s = state.cache.state.read().await;
                s.entries.get(&key).and_then(|m| m.content_type.clone())
            };
            if let Some(ct) = content_type {
                builder = builder.header("content-type", ct);
            }
            let etag = {
                let s = state.cache.state.read().await;
                s.entries.get(&key).and_then(|m| m.etag.clone())
            };
            if let Some(et) = etag {
                builder = builder.header("etag", et);
            }
            if outcome == crate::cache::cache::CacheOutcome::Stale {
                builder = builder.header("warning", "110 - \"Response is Stale\"");
            }
            let body = if is_head { Body::empty() } else { Body::from(body_bytes) };
            builder.body(body).unwrap()
        }
        Err(crate::cache::cache::FetchError::NotFound) => {
            (StatusCode::NOT_FOUND, Json(json!({"error": "not found"}))).into_response()
        }
        Err(crate::cache::cache::FetchError::Other(msg))
            if msg.contains("traversal") || msg.contains("Empty") || msg.contains("Absolute") =>
        {
            (StatusCode::BAD_REQUEST, Json(json!({"error": msg}))).into_response()
        }
        Err(e) => {
            warn!(key = %key, error = ?format!("{e:?}"), "cache fetch error");
            (StatusCode::BAD_GATEWAY, Json(json!({"error": "upstream error"}))).into_response()
        }
    }
}

async fn prewarm<C, F>(
    State(state): State<AppState<C, F>>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse
where
    C: Clock + Clone,
    F: Fetcher + Clone,
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
    match state.cache.get(&key).await {
        Ok(_) => (StatusCode::OK, Json(json!({"status": "fetched"}))).into_response(),
        Err(crate::cache::cache::FetchError::NotFound) => {
            (StatusCode::NOT_FOUND, Json(json!({"error": "not found"}))).into_response()
        }
        Err(_) => (StatusCode::BAD_GATEWAY, Json(json!({"error": "upstream error"}))).into_response(),
    }
}

async fn not_found() -> impl IntoResponse {
    (StatusCode::NOT_FOUND, Json(json!({"error": "not found"})))
}

fn parse_range(header: &str, total: u64) -> Option<(u64, Option<u64>)> {
    let h = header.trim();
    let bytes = h.strip_prefix("bytes=")?;
    let (start_s, end_s) = bytes.split_once('-')?;
    let start: u64 = start_s.parse().ok()?;
    let end: Option<u64> = if end_s.is_empty() {
        None
    } else {
        Some(end_s.parse::<u64>().ok()? + 1)
    };
    if start >= total {
        return None;
    }
    Some((start, end))
}

pub fn router<C, F>(state: AppState<C, F>) -> Router
where
    C: Clock + Clone,
    F: Fetcher + Clone,
{
    Router::new()
        .route("/_internal/healthz", get(healthz::<C, F>))
        .route("/_internal/prewarm/{key}", post(prewarm::<C, F>))
        .route("/{*key}", get(get_key::<C, F>).head(get_key::<C, F>))
        .fallback(not_found)
        .with_state(state)
}

pub async fn serve<C, F>(
    addr: SocketAddr,
    state: AppState<C, F>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()>
where
    C: Clock + Clone,
    F: Fetcher + Clone,
{
    let listener = TcpListener::bind(addr).await?;
    info!(%addr, "business plane listening");
    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown)
        .await?;
    Ok(())
}

pub fn skeleton_router() -> Router {
    Router::new()
        .route("/_internal/healthz", get(skeleton_healthz))
        .fallback(not_found)
}

async fn skeleton_healthz() -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(json!({
            "status": "ok",
            "plane": "business",
            "version": env!("CARGO_PKG_VERSION"),
        })),
    )
}
