//! `djq-queue` — the orchestration layer over a [`djq_core::Store`].
//!
//! [`QueueService`] is the single entry point the API and worker binaries use.
//! It applies request-time defaults (injecting `now`), records Prometheus
//! metrics, and exposes the operator-facing operations. [`maintenance`] holds
//! the background reaper that recovers abandoned jobs and refreshes gauges.

pub mod maintenance;

use djq_core::{
    DeadLetterJob, ExecutionOutcome, Job, JobAttempt, JobFilter, NewJob, Queue, QueueError,
    QueueStats, Result, Store, Submission,
};
use djq_telemetry::Metrics;
use serde_json::Value as Json;
use std::sync::Arc;
use uuid::Uuid;

/// Default lease duration applied when a worker does not specify one.
pub const DEFAULT_LEASE_SECS: i64 = 30;

/// Thin, metrics-aware facade over the storage layer.
#[derive(Clone)]
pub struct QueueService {
    store: Arc<dyn Store>,
    metrics: Metrics,
}

impl QueueService {
    pub fn new(store: Arc<dyn Store>, metrics: Metrics) -> Self {
        Self { store, metrics }
    }

    /// Access the underlying store (e.g. for readiness checks).
    pub fn store(&self) -> &Arc<dyn Store> {
        &self.store
    }

    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    // ---- submission -------------------------------------------------------

    /// Validate, apply defaults and persist a new job.
    pub async fn submit(&self, new: NewJob) -> Result<Submission> {
        let normalized = new.normalized(chrono::Utc::now())?;
        let queue = normalized.queue.clone();
        let submission = self.store.submit(normalized).await?;
        if submission.created {
            self.metrics
                .jobs_submitted
                .with_label_values(&[queue.as_str()])
                .inc();
        }
        Ok(submission)
    }

    // ---- inspection -------------------------------------------------------

    pub async fn get_job(&self, id: Uuid) -> Result<Job> {
        self.store
            .get_job(id)
            .await?
            .ok_or(QueueError::JobNotFound(id))
    }

    pub async fn list_jobs(&self, filter: &JobFilter) -> Result<Vec<Job>> {
        let (limit, offset) = filter.bounded();
        let status = filter.status.map(|s| s.as_str());
        self.store
            .list_jobs(
                filter.queue.as_deref(),
                status,
                filter.job_type.as_deref(),
                limit,
                offset,
            )
            .await
    }

    pub async fn list_attempts(&self, job_id: Uuid) -> Result<Vec<JobAttempt>> {
        // Surface a clear 404 when the job itself is gone.
        if self.store.get_job(job_id).await?.is_none() {
            return Err(QueueError::JobNotFound(job_id));
        }
        self.store.list_attempts(job_id).await
    }

    pub async fn list_dead_letter(
        &self,
        queue: Option<&str>,
        limit: i64,
    ) -> Result<Vec<DeadLetterJob>> {
        self.store
            .list_dead_letter(queue, limit.clamp(1, 500))
            .await
    }

    // ---- operator actions -------------------------------------------------

    pub async fn cancel_job(&self, id: Uuid) -> Result<Job> {
        self.store.cancel_job(id).await
    }

    pub async fn retry_job(&self, id: Uuid) -> Result<Job> {
        self.store.retry_job(id).await
    }

    pub async fn purge_finished(&self, older_than_secs: i64) -> Result<u64> {
        let cutoff = chrono::Utc::now() - chrono::Duration::seconds(older_than_secs.max(0));
        self.store.purge_finished(cutoff).await
    }

    // ---- queues -----------------------------------------------------------

    pub async fn ensure_queue(&self, name: &str) -> Result<Queue> {
        self.store.ensure_queue(name).await
    }

    pub async fn list_queues(&self) -> Result<Vec<Queue>> {
        self.store.list_queues().await
    }

    pub async fn set_queue_paused(&self, name: &str, paused: bool) -> Result<Queue> {
        self.store.set_queue_paused(name, paused).await
    }

    pub async fn queue_stats(&self, name: &str) -> Result<QueueStats> {
        self.store.queue_stats(name).await
    }

    // ---- worker-facing operations ----------------------------------------

    pub async fn lease_next(
        &self,
        worker_id: Uuid,
        queues: &[String],
        lease_secs: i64,
    ) -> Result<Option<Job>> {
        self.store.lease_next(worker_id, queues, lease_secs).await
    }

    pub async fn renew_lease(&self, id: Uuid, worker_id: Uuid, lease_secs: i64) -> Result<()> {
        self.store.renew_lease(id, worker_id, lease_secs).await
    }

    /// Report an execution outcome and record the relevant metrics.
    pub async fn report_outcome(
        &self,
        job: &Job,
        worker_id: Uuid,
        outcome: ExecutionOutcome,
        duration_secs: f64,
    ) -> Result<Job> {
        self.metrics.processing_duration.observe(duration_secs);
        match outcome {
            ExecutionOutcome::Success(result) => {
                let updated = self.store.complete_job(job.id, worker_id, result).await?;
                self.metrics
                    .jobs_completed
                    .with_label_values(&[job.queue.as_str()])
                    .inc();
                Ok(updated)
            }
            ExecutionOutcome::Failure(err) => {
                self.record_failure(job, worker_id, &err, false).await
            }
            ExecutionOutcome::Timeout => {
                self.metrics.jobs_timed_out.inc();
                self.record_failure(job, worker_id, "execution timed out", true)
                    .await
            }
        }
    }

    async fn record_failure(
        &self,
        job: &Job,
        worker_id: Uuid,
        err: &str,
        timed_out: bool,
    ) -> Result<Job> {
        self.metrics
            .jobs_failed
            .with_label_values(&[job.queue.as_str()])
            .inc();
        let updated = self
            .store
            .fail_job(job.id, worker_id, err, timed_out)
            .await?;
        match updated.status {
            djq_core::JobStatus::Retrying => self
                .metrics
                .jobs_retried
                .with_label_values(&[job.queue.as_str()])
                .inc(),
            djq_core::JobStatus::DeadLettered => self
                .metrics
                .jobs_dead_lettered
                .with_label_values(&[job.queue.as_str()])
                .inc(),
            _ => {}
        }
        Ok(updated)
    }

    /// Persist a raw result without going through the outcome enum (used in
    /// some tests and tooling).
    pub async fn complete_job(
        &self,
        id: Uuid,
        worker_id: Uuid,
        result: Option<Json>,
    ) -> Result<Job> {
        self.store.complete_job(id, worker_id, result).await
    }
}
