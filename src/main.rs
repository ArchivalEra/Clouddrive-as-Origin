use anyhow::Context;
use std::{collections::HashMap, sync::Arc};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tracing::{info, warn};

use origin_cache::{
    backend::{BackendRegistry, BackendSlot, OpenListBackend, StorageBackend},
    cache::cache::Cache,
    clock::SystemClock,
    config,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg_path = std::env::args().nth(1);
    let cfg = Arc::new(config::Config::from_file_or_default(cfg_path.as_deref()).context("load config")?);

    info!(
        front_listen = %cfg.front_listen,
        listen_addr = %cfg.listen_addr,
        cache_dir = %cfg.cache_dir.display(),
        "origin-cache starting (single binary, two planes)"
    );

    let clock = Arc::new(SystemClock);
    let mut slots = HashMap::new();
    for u in &cfg.upstreams {
        let backend: Arc<dyn StorageBackend> = match u.backend_type.as_str() {
            "openlist" => Arc::new(
                OpenListBackend::from_config(u).map_err(|e| anyhow::anyhow!(e))?,
            ),
            other => anyhow::bail!("upstream {}: unknown type {other:?} (v1 supports \"openlist\")", u.id),
        };
        slots.insert(
            u.id.clone(),
            Arc::new(BackendSlot {
                backend,
                gate: Arc::new(Semaphore::new(cfg.concurrency_per_upstream)),
            }),
        );
    }
    let cache = Arc::new(Cache::new(Arc::clone(&cfg), clock, BackendRegistry::new(slots)));
    cache.load_and_start().await;
    let app_state = origin_cache::business::AppState { cache, config: Arc::clone(&cfg) };

    let business_addr = cfg.listen_addr;
    let front_addr = cfg.front_listen;

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let business_shutdown = async move {
        shutdown_rx.changed().await.ok();
    };

    let business_handle = tokio::spawn({
        let state = app_state.clone();
        async move {
            if let Err(e) = origin_cache::business::serve(business_addr, state, business_shutdown).await {
                warn!(error = %e, "business plane exited with error");
            }
        }
    });

    let front_handle = tokio::spawn(async move {
        if let Err(e) = run_front(front_addr, business_addr).await {
            warn!(error = %e, "front plane exited with error");
        }
    });

    wait_shutdown().await;
    let _ = shutdown_tx.send(true);
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    front_handle.abort();
    let _ = business_handle.await;
    info!("origin-cache shut down cleanly");
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
    let mut buf = vec![0u8; 8192];
    let n = inbound.read(&mut buf).await?;
    if n == 0 {
        return Ok(());
    }
    let mut outbound = TcpStream::connect(business).await?;
    outbound.write_all(&buf[..n]).await?;
    tokio::io::copy_bidirectional(inbound, &mut outbound).await?;
    Ok(())
}

async fn wait_shutdown() {
    #[cfg(unix)]
    {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("sigterm");
        let mut sigint =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).expect("sigint");
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
