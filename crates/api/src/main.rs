//! API binary entrypoint: config, migrations, maintenance loop, graceful serve.

use std::sync::Arc;

use anyhow::Context;
use djq_api::{build_router, ApiConfig, AppState};
use djq_queue::maintenance::{self, MaintenanceConfig};
use djq_queue::QueueService;
use djq_storage::PgStore;
use djq_telemetry::Metrics;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = ApiConfig::from_env()?;
    djq_telemetry::init_tracing("djq-api", config.json_logs);

    let store = PgStore::connect(&config.database_url, config.db_max_connections)
        .await
        .context("failed to connect to Postgres")?;
    store.migrate().await.context("migrations failed")?;
    tracing::info!("database connected and migrated");

    let metrics = Metrics::new();
    let service = QueueService::new(Arc::new(store), metrics.clone());

    let shutdown = CancellationToken::new();

    // Background maintenance: lease recovery, worker pruning, gauge refresh.
    let maint = tokio::spawn(maintenance::run(
        service.clone(),
        MaintenanceConfig::default(),
        shutdown.clone(),
    ));

    let app = build_router(AppState { service }, config.max_body_bytes);

    let listener = tokio::net::TcpListener::bind(config.bind_addr)
        .await
        .with_context(|| format!("failed to bind {}", config.bind_addr))?;
    tracing::info!(addr = %config.bind_addr, "API listening");

    let serve_shutdown = shutdown.clone();
    let server = axum::serve(listener, app).with_graceful_shutdown(async move {
        wait_for_signal().await;
        tracing::info!("shutdown signal received");
        serve_shutdown.cancel();
    });

    server.await.context("server error")?;

    // Let maintenance finish its current sweep.
    let _ = maint.await;
    tracing::info!("API stopped");
    Ok(())
}

#[cfg(unix)]
async fn wait_for_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    let mut int = signal(SignalKind::interrupt()).expect("install SIGINT handler");
    tokio::select! {
        _ = term.recv() => {}
        _ = int.recv() => {}
    }
}

#[cfg(not(unix))]
async fn wait_for_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
