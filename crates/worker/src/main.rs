//! Worker binary entrypoint.
//!
//! Connects to Postgres, registers the example handlers, exposes a small
//! metrics/health endpoint, and runs the worker runtime until SIGINT/SIGTERM.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use axum::{extract::State, http::StatusCode, routing::get, Router};
use djq_queue::QueueService;
use djq_storage::PgStore;
use djq_telemetry::Metrics;
use djq_worker::{HandlerRegistry, WorkerConfig, WorkerRuntime};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let json_logs = std::env::var("LOG_JSON")
        .map(|v| v == "true")
        .unwrap_or(false);
    djq_telemetry::init_tracing("djq-worker", json_logs);

    let database_url =
        std::env::var("DATABASE_URL").context("DATABASE_URL environment variable is required")?;
    let max_conn = std::env::var("DB_MAX_CONNECTIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);

    let store = PgStore::connect(&database_url, max_conn)
        .await
        .context("failed to connect to Postgres")?;
    store.migrate().await.context("migrations failed")?;

    let metrics = Metrics::new();
    let service = QueueService::new(Arc::new(store), metrics.clone());

    let config = WorkerConfig::from_env();
    let registry = HandlerRegistry::new().with_examples();

    let shutdown = CancellationToken::new();

    // Optional metrics/health server.
    let metrics_addr: SocketAddr = std::env::var("WORKER_METRICS_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:9091".to_string())
        .parse()
        .context("invalid WORKER_METRICS_ADDR")?;
    let metrics_server = tokio::spawn(serve_metrics(metrics, metrics_addr, shutdown.clone()));

    // Trigger shutdown on SIGINT / SIGTERM.
    let signal_token = shutdown.clone();
    tokio::spawn(async move {
        wait_for_signal().await;
        tracing::info!("shutdown signal received");
        signal_token.cancel();
    });

    let runtime = WorkerRuntime::new(service, registry, config);
    runtime.run(shutdown).await?;

    let _ = metrics_server.await;
    Ok(())
}

#[derive(Clone)]
struct MetricsState {
    metrics: Metrics,
}

async fn serve_metrics(metrics: Metrics, addr: SocketAddr, shutdown: CancellationToken) {
    let app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/readyz", get(|| async { "ok" }))
        .route("/metrics", get(render_metrics))
        .with_state(MetricsState { metrics });

    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(error = %e, %addr, "failed to bind worker metrics server");
            return;
        }
    };
    tracing::info!(%addr, "worker metrics server listening");
    let _ = axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await;
}

async fn render_metrics(State(state): State<MetricsState>) -> (StatusCode, String) {
    (StatusCode::OK, state.metrics.render())
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
