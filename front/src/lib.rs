//! Front plane: Pingora-based TLS termination + HTTP/2 reverse proxy to
//! the business plane on loopback. Owns no cache semantics — it only
//! moves bytes. TLS material comes from `tls_cert_env` / `tls_key_env`
//! paths; absent material = plaintext proxy + boot warning (EdgeOne is
//! expected to carry public HTTPS in that deployment).
//!
//! Pingora (Cloudflare's proxy framework) provides native HTTP/2
//! multiplexing, TLS termination, and graceful shutdown — replacing the
//! hand-rolled rustls byte proxy.
//!
//! Observability: an optional Prometheus listener reports three static
//! metrics (requests by proto/method/status, active downstream
//! connections, upstream reuse) via `pingora::apps`' PrometheusHttpApp,
//! which serves the crate-wide default registry.

use std::net::SocketAddr;
use std::time::Instant;

use anyhow::Context;
use pingora::{
    proxy::{ProxyHttp, Session},
    server::Server,
    upstreams::peer::HttpPeer,
};
use pingora_error::Result as ProxyResult;
use prometheus::{
    register_int_counter_vec, register_int_gauge, IntCounterVec, IntGauge,
};
use tracing::{info, warn};

/// Per-request front state. `start` exists for the access-log duration
/// (map ticket 00); the connection gauge lives on `new_ctx`/`logging`
/// bookends which are both guaranteed to run.
pub struct FrontCtx {
    pub start: Instant,
}

// Static metrics land in the global default registry — the same registry
// PrometheusHttpApp serves (prometheus 0.13, same version pingora uses).
pub static REQUESTS_TOTAL: std::sync::LazyLock<IntCounterVec> = std::sync::LazyLock::new(|| {
    register_int_counter_vec!(
        "front_requests_total",
        "front requests by protocol, method and response status",
        &["proto", "method", "status"]
    )
    .expect("register front_requests_total")
});
pub static CONNECTIONS_ACTIVE: std::sync::LazyLock<IntGauge> = std::sync::LazyLock::new(|| {
    register_int_gauge!("front_connections_active", "active downstream connections")
        .expect("register front_connections_active")
});
pub static UPSTREAM_REUSED_TOTAL: std::sync::LazyLock<IntCounterVec> =
    std::sync::LazyLock::new(|| {
        register_int_counter_vec!(
            "front_upstream_reused_total",
            "upstream connections by reuse state",
            &["reused"]
        )
        .expect("register front_upstream_reused_total")
    });

fn proto_of(session: &Session) -> &'static str {
    // RequestHeader derefs to http::request::Parts, which carries the
    // negotiated version.
    if session.req_header().version == http::Version::HTTP_2 {
        "h2"
    } else {
        "h1"
    }
}

fn status_of(session: &Session) -> String {
    session
        .response_written()
        .map(|h| h.status.as_u16().to_string())
        .unwrap_or_else(|| "0".to_string())
}

/// Resolve TLS material from config env pointers. `Ok(None)` = plaintext
/// proxy (warned, not failed — loopback/edge-terminated deployments).
pub fn acceptor_from_env(
    tls_cert_env: Option<&str>,
    tls_key_env: Option<&str>,
) -> anyhow::Result<Option<(String, String)>> {
    match (tls_cert_env, tls_key_env) {
        (Some(cert_env), Some(key_env)) => {
            let cert_path = std::env::var(cert_env)
                .with_context(|| format!("env {cert_env} not set"))?;
            let key_path = std::env::var(key_env)
                .with_context(|| format!("env {key_env} not set"))?;
            info!(cert = %cert_path, "front plane TLS enabled");
            Ok(Some((cert_path, key_path)))
        }
        (None, None) => {
            warn!("front plane without TLS (plaintext proxy) — terminate HTTPS at the edge");
            Ok(None)
        }
        _ => anyhow::bail!("tls_cert_env and tls_key_env must be set together"),
    }
}

/// The reverse proxy: forwards every request to the business plane on
/// loopback. No cache semantics here — pure byte movement.
pub struct BusinessProxy {
    pub business: SocketAddr,
}

#[async_trait::async_trait]
impl ProxyHttp for BusinessProxy {
    type CTX = FrontCtx;

    fn new_ctx(&self) -> Self::CTX {
        CONNECTIONS_ACTIVE.inc();
        FrontCtx {
            start: Instant::now(),
        }
    }

    /// Forward to the business plane (loopback, plain HTTP).
    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> ProxyResult<Box<HttpPeer>> {
        Ok(Box::new(HttpPeer::new(
            self.business.to_string(),
            false,
            String::new(),
        )))
    }

    /// Connection accounting: new vs reused upstream connections.
    async fn connected_to_upstream(
        &self,
        _session: &mut Session,
        reused: bool,
        _peer: &HttpPeer,
        #[cfg(unix)] _fd: std::os::unix::io::RawFd,
        #[cfg(windows)] _sock: std::os::windows::io::RawSocket,
        _digest: Option<&pingora::protocols::Digest>,
        _ctx: &mut Self::CTX,
    ) -> ProxyResult<()> {
        UPSTREAM_REUSED_TOTAL
            .with_label_values(&[if reused { "true" } else { "false" }])
            .inc();
        Ok(())
    }

    /// Guaranteed final per-request hook (runs even on failures) — the
    /// only place it is safe to decrement the connection gauge, count
    /// the request outcome, and emit the one-line access log.
    async fn logging(
        &self,
        session: &mut Session,
        e: Option<&pingora::Error>,
        ctx: &mut Self::CTX,
    ) where
        Self::CTX: Send + Sync,
    {
        REQUESTS_TOTAL
            .with_label_values(&[proto_of(session), session.req_header().method.as_str(), &status_of(session)])
            .inc();
        CONNECTIONS_ACTIVE.dec();
        let xff = session
            .req_header()
            .headers
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("-");
        tracing::info!(
            method = %session.req_header().method,
            path = %session.req_header().uri.path(),
            status = status_of(session),
            bytes = session.body_bytes_sent(),
            duration_ms = ctx.start.elapsed().as_millis(),
            proto = proto_of(session),
            xff = %xff,
            err = ?e.as_ref().map(|e| e.to_string()),
            "front access",
        );
    }
}

/// Run the Pingora front plane. `tls` is `Some((cert, key))` for TLS
/// termination, `None` for plaintext. `metrics` is the Prometheus
/// listener address (loopback), `None` disables the endpoint.
/// Synchronous: Pingora manages its own runtime and signal handling
/// (SIGTERM/SIGINT graceful shutdown) — must NOT be called from inside a
/// tokio runtime (run_forever panics with "Cannot start a runtime from
/// within a runtime"). Call from a dedicated std::thread.
pub fn run_front(
    front: SocketAddr,
    business: SocketAddr,
    tls: Option<(String, String)>,
    metrics: Option<String>,
) -> anyhow::Result<()> {
    let mut server = Server::new(None).context("create pingora server")?;
    server.bootstrap();

    let proxy = BusinessProxy { business };
    let mut service = pingora::proxy::http_proxy_service(&server.configuration, proxy);
    match &tls {
        Some((cert, key)) => {
            // add_tls_with_settings + enable_h2: ALPN advertises h2 +
            // http/1.1 so EdgeOne origin-pull negotiates HTTP/2.
            let mut tls_settings = pingora::listeners::tls::TlsSettings::intermediate(cert, key)
                .with_context(|| format!("load tls material {cert} / {key}"))?;
            tls_settings.enable_h2();
            service.add_tls_with_settings(&front.to_string(), None, tls_settings);
        }
        None => {
            service.add_tcp(&front.to_string());
        }
    }
    server.add_service(service);

    if let Some(metrics_addr) = metrics {
        let mut metrics_service =
            pingora::services::listening::Service::prometheus_http_service();
        metrics_service.add_tcp(&metrics_addr);
        server.add_service(metrics_service);
        info!(metrics = %metrics_addr, "front plane metrics enabled");
    }

    server.run_forever();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rendered() -> String {
        let families = prometheus::gather();
        let mut buf = vec![];
        prometheus::TextEncoder::new()
            .encode(&families, &mut buf)
            .unwrap();
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn metrics_exposed_and_incremented() {
        REQUESTS_TOTAL
            .with_label_values(&["h1", "GET", "200"])
            .inc();
        UPSTREAM_REUSED_TOTAL.with_label_values(&["true"]).inc();
        CONNECTIONS_ACTIVE.inc();
        CONNECTIONS_ACTIVE.dec();
        let text = rendered();
        assert!(text.contains(r#"front_requests_total{method="GET",proto="h1",status="200"}"#));
        assert!(text.contains(r#"front_upstream_reused_total{reused="true"}"#));
        assert!(text.contains("front_connections_active"));
    }
}
