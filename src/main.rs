use anyhow::Context;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

use origin_cache::{
    backend::{BackendRegistry, BackendSlot, OpenListBackend, StorageBackend},
    cache::cache::Cache,
    clock::SystemClock,
    config,
};
use origin_front as front;

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
        // Timing decorator: every backend call is measured (map #47 T2) and
        // retried per the configured policy (#58). Wired once, so all call
        // sites are covered.
        let backend: Arc<dyn StorageBackend> = Arc::new(origin_cache::backend::TimedBackend::with_retry(
            backend,
            origin_cache::backend::RetryPolicy {
                max_attempts: cfg.retry_max_attempts as u32,
                base_ms: cfg.retry_base_ms,
                max_ms: cfg.retry_max_ms,
            },
        ));
        slots.insert(
            u.id.clone(),
            Arc::new(BackendSlot::new(backend, cfg.concurrency_per_upstream)),
        );
    }
    let cache = Arc::new(Cache::new(Arc::clone(&cfg), clock, BackendRegistry::new(slots)));
    cache.load_and_start().await;
    let app_state = origin_cache::business::AppState {
        cache,
        config: Arc::clone(&cfg),
        sigv4_config: origin_cache::sigv4::SigV4Config::from_env(),
        listings: Default::default(),
    };

    if cfg.prewarm_shared_secret_env.is_none() {
        // The handler skips its token check entirely when no env var is
        // named, which makes prewarm an OPEN bandwidth amplifier: any caller
        // can force a full upstream fetch. A warning was not enough (#54's
        // fail-open finding). Refuse to start unless it is configured, so
        // this cannot be deployed open by accident.
        anyhow::bail!(
            "prewarm_shared_secret_env is not set: the prewarm endpoint would be              unauthenticated and any caller could force upstream fetches. Name an              env var holding the shared secret, or remove the endpoint."
        );
    }

    let business_addr = cfg.listen_addr;

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let business_shutdown = {
        let mut rx = shutdown_rx.clone();
        async move { rx.changed().await.ok(); }
    };

    // Readiness gate (C2): bind the business listener NOW, before the front
    // plane starts accepting. Serving is spawned below on this listener.
    // Previously bind happened inside the spawned task, so the front thread
    // could accept connections in the window where the loopback port was
    // still closed -- those requests failed after the front's 3s connect
    // timeout for no reason the client could see.
    let business_listener = origin_cache::business::bind(business_addr)
        .await
        .context("bind business plane")?;

    let business_handle = tokio::spawn({
        let state = app_state.clone();
        async move {
            if let Err(e) =
                origin_cache::business::serve_on(business_listener, state, business_shutdown).await
            {
                warn!(error = %e, "business plane exited with error");
            }
        }
    });

    let tls = front::acceptor_from_env(cfg.tls_cert_env.as_deref(), cfg.tls_key_env.as_deref())
        .context("load front TLS material")?;
    // Pingora manages its own runtime + signal handling; run it on a
    // dedicated thread (run_forever panics inside a tokio runtime).
    let front_opts = front::FrontOptions {
        front: cfg.front_listen,
        business: cfg.listen_addr,
        tls,
        metrics: cfg.front_metrics_listen.clone(),
        ip_block: cfg.front_ip_block.clone(),
        ip_allow: cfg.front_ip_allow.clone(),
        rate_rps: cfg.front_rate_rps,
        // The box has 2 cores; the business plane already runs its own
        // workers on them. Pingora's default of 1 thread serializes every
        // TLS/H2/byte-move on one core (P6). Two proxy threads let TLS and
        // framing overlap; sized to the box, not unbounded.
        threads: cfg.front_threads.or(Some(2)),
    };
    // A front plane that dies must take the process with it (A1). This used
    // to warn and continue, so a failure here (a bad `front_threads`, a
    // bind error, a panic inside pingora) left the process ALIVE with NO
    // listener: systemd sees an active unit, the watchdog sees a healthy
    // systemd unit, and nothing serves. Dying loudly is the only honest
    // outcome -- restart policy then does its job.
    //
    // `front_threads = 0` is the concrete case found in review: pingora
    // asserts threads != 0 and panicked inside that thread, and the warn
    // below swallowed it.
    let (front_panic_tx, front_panic_rx) = std::sync::mpsc::channel::<String>();
    let front_thread = std::thread::spawn(move || {
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            front::run_front(front_opts)
        }));
        let msg = match outcome {
            Ok(Ok(())) => "front plane exited without error (unexpected)".to_string(),
            Ok(Err(e)) => format!("front plane exited with error: {e}"),
            Err(_) => "front plane panicked".to_string(),
        };
        let _ = front_panic_tx.send(msg);
    });

    // Wait for a shutdown signal OR a front-plane death (A1), whichever
    // comes first.
    let front_failure = tokio::task::spawn_blocking({
        let rx = front_panic_rx;
        move || rx.recv().ok()
    });
    tokio::select! {
        _ = wait_shutdown() => {
            // Adaptive stop (see origin_cache::shutdown): the front plane has
            // already closed its listener, and Pingora would otherwise sleep
            // its full 300s grace period even with nothing to drain -- five
            // minutes of an origin serving nothing, on every deploy. If the
            // in-flight count stays at zero for a settle window there is
            // nothing to protect, so exit now and let the supervisor's
            // TimeoutStopSec=320s remain the budget for the busy path.
            let action = origin_cache::shutdown::observe_settle(|| {
                origin_front::CONNECTIONS_ACTIVE.get()
            })
            .await;
            if action == origin_cache::shutdown::StopAction::FastExit {
                // Note the accepted trade: this skips the business plane's
                // drain. With no front connections there is no client work in
                // flight -- only loopback health probes, which a probe client
                // treats as a miss either way. The settle window also exceeds
                // the access-clock flush cadence, so pending LRU timestamps
                // are already committed.
                info!("no connections in flight; exiting without the drain window");
                std::process::exit(0);
            }
            info!("connections in flight; draining gracefully");
        }
        msg = front_failure => {
            if let Ok(Some(msg)) = msg {
                error!(reason = %msg, "front plane is gone; shutting down so the supervisor restarts us");
            }
        }
    }
    let _ = shutdown_tx.send(true);
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let _ = business_handle.await;
    let _ = front_thread.join();
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
