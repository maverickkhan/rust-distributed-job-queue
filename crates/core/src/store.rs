//! The storage-abstraction seam.
//!
//! [`Store`] is the single trait that separates all queue/worker domain logic
//! from a concrete persistence backend. The production implementation lives in
//! `djq-storage` (Postgres); tests can substitute an alternative. Every method
//! is expected to be transaction-safe and to respect the [`crate::JobStatus`]
//! state machine.

use crate::model::{DeadLetterJob, Job, JobAttempt, NormalizedJob, Queue, QueueStats, Worker};
use crate::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value as Json;
use uuid::Uuid;

/// Result of a submission, distinguishing a fresh insert from an idempotent
/// hit on an existing `(queue, idempotency_key)`.
#[derive(Debug, Clone)]
pub struct Submission {
    pub job: Job,
    /// `false` when an existing job was returned because of an idempotency key.
    pub created: bool,
}

/// Backend-agnostic persistence + leasing operations for the queue.
///
/// Implementations must be `Send + Sync` so they can be shared as
/// `Arc<dyn Store>` across Tokio tasks.
#[async_trait]
pub trait Store: Send + Sync + 'static {
    // ---- queues -----------------------------------------------------------

    /// Create the queue if absent and return it (idempotent).
    async fn ensure_queue(&self, name: &str) -> Result<Queue>;

    /// Fetch a queue by name.
    async fn get_queue(&self, name: &str) -> Result<Option<Queue>>;

    /// List all queues ordered by name.
    async fn list_queues(&self) -> Result<Vec<Queue>>;

    /// Pause or resume a queue. Errors if the queue is unknown.
    async fn set_queue_paused(&self, name: &str, paused: bool) -> Result<Queue>;

    /// Aggregate per-status counts for a queue.
    async fn queue_stats(&self, name: &str) -> Result<QueueStats>;

    // ---- job submission / inspection -------------------------------------

    /// Submit a job, honouring idempotency keys. Ensures the queue exists.
    async fn submit(&self, job: NormalizedJob) -> Result<Submission>;

    /// Fetch a single job by id.
    async fn get_job(&self, id: Uuid) -> Result<Option<Job>>;

    /// List jobs matching a filter with pagination already clamped by caller.
    async fn list_jobs(
        &self,
        queue: Option<&str>,
        status: Option<&str>,
        job_type: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<Job>>;

    /// Per-job attempt history, newest first.
    async fn list_attempts(&self, job_id: Uuid) -> Result<Vec<JobAttempt>>;

    /// Dead-letter entries, optionally filtered by queue.
    async fn list_dead_letter(&self, queue: Option<&str>, limit: i64)
        -> Result<Vec<DeadLetterJob>>;

    // ---- operator actions -------------------------------------------------

    /// Cancel a job if it is in a cancellable state.
    async fn cancel_job(&self, id: Uuid) -> Result<Job>;

    /// Re-queue a `failed` or `dead_lettered` job for another run.
    async fn retry_job(&self, id: Uuid) -> Result<Job>;

    /// Delete completed/cancelled jobs older than `older_than`. Returns count.
    async fn purge_finished(&self, older_than: DateTime<Utc>) -> Result<u64>;

    // ---- worker leasing protocol -----------------------------------------

    /// Atomically lease the next eligible job for `worker_id` from any of
    /// `queues`, using `FOR UPDATE SKIP LOCKED`. Returns `None` when no job is
    /// available. Sets status to `processing`, increments `attempts`, records a
    /// running attempt row and sets a lease expiring `lease_secs` from now.
    async fn lease_next(
        &self,
        worker_id: Uuid,
        queues: &[String],
        lease_secs: i64,
    ) -> Result<Option<Job>>;

    /// Mark a leased job complete. The lease must still be held by `worker_id`.
    async fn complete_job(&self, id: Uuid, worker_id: Uuid, result: Option<Json>) -> Result<Job>;

    /// Report a failed (or timed-out) attempt. Decides retry vs dead-letter
    /// transactionally using the job's stored backoff policy and attempt count.
    async fn fail_job(
        &self,
        id: Uuid,
        worker_id: Uuid,
        error: &str,
        timed_out: bool,
    ) -> Result<Job>;

    /// Extend the lease on an in-flight job (worker keep-alive for long jobs).
    async fn renew_lease(&self, id: Uuid, worker_id: Uuid, lease_secs: i64) -> Result<()>;

    /// Reaper: move expired-lease `processing` jobs back to `retrying`
    /// (or dead-letter when attempts are exhausted). Returns affected job ids.
    async fn recover_expired_leases(&self, limit: i64) -> Result<Vec<Uuid>>;

    // ---- worker registry --------------------------------------------------

    /// Register (or refresh) a worker row.
    async fn register_worker(&self, worker: &Worker) -> Result<()>;

    /// Record a heartbeat. Returns `false` if the worker row no longer exists.
    async fn heartbeat(&self, worker_id: Uuid) -> Result<bool>;

    /// Remove a worker from the registry (clean shutdown).
    async fn deregister_worker(&self, worker_id: Uuid) -> Result<()>;

    /// Workers whose last heartbeat is within `within_secs`.
    async fn list_active_workers(&self, within_secs: i64) -> Result<Vec<Worker>>;

    /// Delete worker rows whose heartbeat is older than `older_secs`.
    async fn prune_dead_workers(&self, older_secs: i64) -> Result<u64>;
}
