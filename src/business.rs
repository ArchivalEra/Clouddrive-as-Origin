use axum::{http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use serde_json::json;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::info;

async fn healthz() -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(json!({
            "status": "ok",
            "plane": "business",
            "version": env!("CARGO_PKG_VERSION"),
        })),
    )
}

async fn not_found() -> impl IntoResponse {
    (StatusCode::NOT_FOUND, Json(json!({"error": "not found"})))
}

/// Build the axum business-plane router.
/// In the final service this also mounts `GET /<key>`, `HEAD /<key>`,
/// `POST /_internal/prewarm/<key>` and the cache machinery.
/// The P1 skeleton only needs `/_internal/healthz` to prove the seam.
pub fn router() -> Router {
    Router::new()
        .route("/_internal/healthz", get(healthz))
        .fallback(not_found)
}

/// Run the business plane on `addr` until `shutdown` completes.
pub async fn serve(
    addr: SocketAddr,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!(%addr, "business plane listening");
    axum::serve(listener, router())
        .with_graceful_shutdown(shutdown)
        .await?;
    Ok(())
}
