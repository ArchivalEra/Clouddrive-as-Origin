mod business;
mod config;
mod key;
mod routing;

use anyhow::Context;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg_path = std::env::args().nth(1);
    let cfg = config::Config::from_file_or_default(cfg_path.as_deref())
        .context("load config")?;

    info!(
        front_listen = %cfg.front_listen,
        listen_addr = %cfg.listen_addr,
        "origin-cache skeleton starting (single binary, two planes)"
    );

    // Business plane on loopback owns healthz; front plane reverse-proxies to it.
    // Both live in the same tokio runtime here. Production will replace the
    // front's plain-TCP proxy with Pingora (TLS/H2) while keeping this seam.
    let business_addr = cfg.listen_addr;
    let front_addr = cfg.front_listen;

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let business_shutdown = async move {
        shutdown_rx.changed().await.ok();
    };

    let business_handle = tokio::spawn(async move {
        if let Err(e) = business::serve(business_addr, business_shutdown).await {
            warn!(error = %e, "business plane exited with error");
        }
    });

    // Tiny reverse-proxy front: accept on front_listen, proxy HTTP to business.
    // Enough to prove the B-seam (shared binary, clean shutdown) before Pingora.
    let front_handle = tokio::spawn(async move {
        if let Err(e) = run_front(front_addr, business_addr).await {
            warn!(error = %e, "front plane exited with error");
        }
    });

    wait_shutdown().await;
    let _ = shutdown_tx.send(true);
    // Give planes a moment to drain, then abort front listener.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    front_handle.abort();
    let _ = business_handle.await;
    info!("origin-cache skeleton shut down cleanly");
    Ok(())
}

async fn run_front(front: SocketAddr, business: SocketAddr) -> anyhow::Result<()> {
    let listener = TcpListener::bind(front).await?;
    info!(%front, business = %business, "front plane (plain-TCP proxy) listening");
    loop {
        let (mut inbound, peer) = listener.accept().await?;
        let business = business.clone();
        tokio::spawn(async move {
            if let Err(e) = proxy_one(&mut inbound, business).await {
                warn!(%peer, error = %e, "proxy error");
            }
        });
    }
}

async fn proxy_one(inbound: &mut TcpStream, business: SocketAddr) -> anyhow::Result<()> {
    // Read the inbound request (simple: read until blank line, then forward).
    // This skeleton handles one request per connection — enough for the prototype.
    let mut buf = vec![0u8; 8192];
    let n = inbound.read(&mut buf).await?;
    if n == 0 {
        return Ok(());
    }
    // Basic host header rewrite: forward as-is to business plane.
    // In production Pingora will handle Host/SNI properly.
    let mut outbound = TcpStream::connect(business).await?;
    outbound.write_all(&buf[..n]).await?;
    // Pipe response back.
    tokio::io::copy_bidirectional(inbound, &mut outbound).await?;
    Ok(())
}

async fn wait_shutdown() {
    #[cfg(unix)]
    {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("sigterm");
        let mut sigint =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                .expect("sigint");
        tokio::select! {
            _ = sigterm.recv() => info!("received SIGTERM"),
            _ = sigint.recv() => info!("received SIGINT"),
            _ = tokio::signal::ctrl_c() => info!("received ctrl_c"),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        info!("received ctrl_c");
    }
}
