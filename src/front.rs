//! Front plane: Pingora-based TLS termination + HTTP/2 reverse proxy to
//! the business plane on loopback. Owns no cache semantics — it only
//! moves bytes. TLS material comes from `tls_cert_env` / `tls_key_env`
//! paths; absent material = plaintext proxy + boot warning (EdgeOne is
//! expected to carry public HTTPS in that deployment).
//!
//! Pingora (Cloudflare's proxy framework) provides native HTTP/2
//! multiplexing, TLS termination, and graceful shutdown — replacing the
//! hand-rolled rustls byte proxy (map #31 H2 optimization).

use std::net::SocketAddr;

use anyhow::Context;
use pingora::{
    proxy::{ProxyHttp, Session},
    server::Server,
    upstreams::peer::HttpPeer,
};
use pingora_error::Result as ProxyResult;
use tracing::{info, warn};

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
    type CTX = ();

    fn new_ctx(&self) -> Self::CTX {}

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
}

/// Run the Pingora front plane. `tls` is `Some((cert, key))` for TLS
/// termination, `None` for plaintext. Synchronous: Pingora manages its
/// own runtime and signal handling (SIGTERM/SIGINT graceful shutdown) —
/// must NOT be called from inside a tokio runtime (run_forever panics
/// with "Cannot start a runtime from within a runtime"). Call from a
/// dedicated std::thread.
pub fn run_front(
    front: SocketAddr,
    business: SocketAddr,
    tls: Option<(String, String)>,
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
    server.run_forever();
    Ok(())
}
