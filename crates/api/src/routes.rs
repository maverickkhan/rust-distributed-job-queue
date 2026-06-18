//! HTTP handlers. Each maps cleanly onto a [`djq_queue::QueueService`] call and
//! converts domain errors into HTTP responses via [`crate::error::ApiError`].

use crate::error::{ApiError, ApiResult};
use crate::sse::job_event_stream;
use crate::AppState;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::Sse;
use axum::response::IntoResponse;
use axum::Json;
use djq_core::{DeadLetterJob, Job, JobAttempt, JobFilter, NewJob, Queue, QueueStats};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize)]
pub struct JobListResponse {
    pub jobs: Vec<Job>,
    pub count: usize,
}

#[derive(Deserialize)]
pub struct CreateQueue {
    pub name: String,
}

#[derive(Deserialize)]
pub struct DlqQuery {
    pub queue: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Deserialize)]
pub struct PurgeRequest {
    /// Delete finished jobs older than this many seconds (default 7 days).
    #[serde(default = "default_purge_age")]
    pub older_than_secs: i64,
}

fn default_purge_age() -> i64 {
    7 * 24 * 3600
}

#[derive(Serialize)]
pub struct PurgeResponse {
    pub deleted: u64,
}

// ---- jobs -----------------------------------------------------------------

/// `POST /api/v1/jobs` — submit a job. Honours an `Idempotency-Key` header when
/// the body does not carry one. Returns 201 for a fresh job, 200 on an
/// idempotent hit.
pub async fn submit_job(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(mut new): Json<NewJob>,
) -> ApiResult<impl IntoResponse> {
    if new.idempotency_key.is_none() {
        if let Some(key) = headers.get("idempotency-key").and_then(|v| v.to_str().ok()) {
            if !key.is_empty() {
                new.idempotency_key = Some(key.to_string());
            }
        }
    }
    let submission = state.service.submit(new).await?;
    let code = if submission.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((code, Json(submission.job)))
}

/// `GET /api/v1/jobs/{id}` — fetch a single job.
pub async fn get_job(State(state): State<AppState>, Path(id): Path<Uuid>) -> ApiResult<Json<Job>> {
    Ok(Json(state.service.get_job(id).await?))
}

/// `GET /api/v1/jobs` — filter + paginate jobs.
pub async fn list_jobs(
    State(state): State<AppState>,
    Query(filter): Query<JobFilter>,
) -> ApiResult<Json<JobListResponse>> {
    let jobs = state.service.list_jobs(&filter).await?;
    let count = jobs.len();
    Ok(Json(JobListResponse { jobs, count }))
}

/// `POST /api/v1/jobs/{id}/cancel`.
pub async fn cancel_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<Job>> {
    Ok(Json(state.service.cancel_job(id).await?))
}

/// `POST /api/v1/jobs/{id}/retry`.
pub async fn retry_job(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<Job>> {
    Ok(Json(state.service.retry_job(id).await?))
}

/// `GET /api/v1/jobs/{id}/attempts`.
pub async fn job_attempts(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<Vec<JobAttempt>>> {
    Ok(Json(state.service.list_attempts(id).await?))
}

/// `GET /api/v1/jobs/{id}/events` — SSE status stream.
pub async fn job_events(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Sse<impl futures::Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>>
{
    job_event_stream(state.service, id)
}

// ---- queues ---------------------------------------------------------------

/// `GET /api/v1/queues`.
pub async fn list_queues(State(state): State<AppState>) -> ApiResult<Json<Vec<Queue>>> {
    Ok(Json(state.service.list_queues().await?))
}

/// `POST /api/v1/queues`.
pub async fn create_queue(
    State(state): State<AppState>,
    Json(body): Json<CreateQueue>,
) -> ApiResult<impl IntoResponse> {
    if body.name.trim().is_empty() {
        return Err(ApiError::bad_request("queue name must not be empty"));
    }
    let queue = state.service.ensure_queue(&body.name).await?;
    Ok((StatusCode::CREATED, Json(queue)))
}

/// `POST /api/v1/queues/{name}/pause`.
pub async fn pause_queue(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<Json<Queue>> {
    Ok(Json(state.service.set_queue_paused(&name, true).await?))
}

/// `POST /api/v1/queues/{name}/resume`.
pub async fn resume_queue(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<Json<Queue>> {
    Ok(Json(state.service.set_queue_paused(&name, false).await?))
}

/// `GET /api/v1/queues/{name}/stats`.
pub async fn queue_stats(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<Json<QueueStats>> {
    Ok(Json(state.service.queue_stats(&name).await?))
}

// ---- dead-letter + maintenance -------------------------------------------

/// `GET /api/v1/dlq`.
pub async fn list_dlq(
    State(state): State<AppState>,
    Query(q): Query<DlqQuery>,
) -> ApiResult<Json<Vec<DeadLetterJob>>> {
    let limit = q.limit.unwrap_or(50);
    Ok(Json(
        state
            .service
            .list_dead_letter(q.queue.as_deref(), limit)
            .await?,
    ))
}

/// `POST /api/v1/maintenance/purge`.
pub async fn purge_finished(
    State(state): State<AppState>,
    Json(body): Json<PurgeRequest>,
) -> ApiResult<Json<PurgeResponse>> {
    let deleted = state.service.purge_finished(body.older_than_secs).await?;
    Ok(Json(PurgeResponse { deleted }))
}

// ---- health + metrics -----------------------------------------------------

/// `GET /healthz` — liveness (process is up).
pub async fn healthz() -> &'static str {
    "ok"
}

/// `GET /readyz` — readiness (database reachable).
pub async fn readyz(State(state): State<AppState>) -> impl IntoResponse {
    match state.service.store().get_queue("__readyz__").await {
        Ok(_) => (StatusCode::OK, "ready"),
        Err(e) => {
            tracing::warn!(error = %e, "readiness check failed");
            (StatusCode::SERVICE_UNAVAILABLE, "not ready")
        }
    }
}

/// `GET /metrics` — Prometheus exposition.
pub async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let body = state.service.metrics().render();
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        body,
    )
}
