use anyhow::Context;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Semaphore;
use tracing::{info, warn};

use origin_cache::{
    backend::{BackendRegistry, BackendSlot, OpenListBackend, StorageBackend},
    cache::cache::Cache,
    clock::SystemClock,
    config,
    front,
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

    if cfg.prewarm_shared_secret_env.is_none() {
        warn!("prewarm endpoint is open (no prewarm_shared_secret_env set) — fine behind EdgeOne, risky if directly exposed");
    }

    let business_addr = cfg.listen_addr;
    let front_addr = cfg.front_listen;

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let business_shutdown = {
        let mut rx = shutdown_rx.clone();
        async move { rx.changed().await.ok(); }
    };

    let business_handle = tokio::spawn({
        let state = app_state.clone();
        async move {
            if let Err(e) = origin_cache::business::serve(business_addr, state, business_shutdown).await {
                warn!(error = %e, "business plane exited with error");
            }
        }
    });

    let tls = front::acceptor_from_env(cfg.tls_cert_env.as_deref(), cfg.tls_key_env.as_deref())
        .context("load front TLS material")?;
    let front_handle = tokio::spawn({
        let mut rx = shutdown_rx.clone();
        async move {
            if let Err(e) = front::run_front(front_addr, business_addr, tls, async move {
                rx.changed().await.ok();
            })
            .await
            {
                warn!(error = %e, "front plane exited with error");
            }
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
