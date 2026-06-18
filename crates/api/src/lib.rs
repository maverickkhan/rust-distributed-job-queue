//! `djq-api` — the HTTP surface for the distributed job queue.
//!
//! [`build_router`] wires the [`routes`] handlers onto an Axum [`Router`] with
//! tracing, a body-size limit and a correlation-id middleware. The binary in
//! `main.rs` owns process lifecycle (config, migrations, maintenance loop and
//! graceful shutdown).

pub mod config;
pub mod error;
pub mod routes;
pub mod sse;

use axum::body::Body;
use axum::extract::Request;
use axum::http::{HeaderValue, Response};
use axum::middleware::{self, Next};
use axum::routing::{get, post};
use axum::Router;
use djq_queue::QueueService;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::trace::TraceLayer;
use tracing::Instrument;
use uuid::Uuid;

pub use config::ApiConfig;

/// Shared application state handed to every handler.
#[derive(Clone)]
pub struct AppState {
    pub service: QueueService,
}

/// Build the full application router.
pub fn build_router(state: AppState, max_body_bytes: usize) -> Router {
    let api = Router::new()
        .route("/jobs", post(routes::submit_job).get(routes::list_jobs))
        .route("/jobs/{id}", get(routes::get_job))
        .route("/jobs/{id}/cancel", post(routes::cancel_job))
        .route("/jobs/{id}/retry", post(routes::retry_job))
        .route("/jobs/{id}/attempts", get(routes::job_attempts))
        .route("/jobs/{id}/events", get(routes::job_events))
        .route(
            "/queues",
            get(routes::list_queues).post(routes::create_queue),
        )
        .route("/queues/{name}/pause", post(routes::pause_queue))
        .route("/queues/{name}/resume", post(routes::resume_queue))
        .route("/queues/{name}/stats", get(routes::queue_stats))
        .route("/dlq", get(routes::list_dlq))
        .route("/maintenance/purge", post(routes::purge_finished));

    Router::new()
        .route("/healthz", get(routes::healthz))
        .route("/readyz", get(routes::readyz))
        .route("/metrics", get(routes::metrics))
        .nest("/api/v1", api)
        .layer(middleware::from_fn(correlation_id))
        .layer(RequestBodyLimitLayer::new(max_body_bytes))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// Attach a correlation id to every request span and echo it back in the
/// response, generating one when the client does not supply it.
async fn correlation_id(req: Request, next: Next) -> Response<Body> {
    let cid = req
        .headers()
        .get("x-correlation-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let span = tracing::info_span!("http_request", correlation_id = %cid, %method, path = %path);

    let mut res = next.run(req).instrument(span).await;
    if let Ok(value) = HeaderValue::from_str(&cid) {
        res.headers_mut().insert("x-correlation-id", value);
    }
    res
}
