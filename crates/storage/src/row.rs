//! Mapping between Postgres rows and the pure `djq-core` domain types.

use chrono::{DateTime, Utc};
use djq_core::{DeadLetterJob, Job, JobAttempt, JobStatus, QueueError, Worker};
use serde_json::Value as Json;
use sqlx::FromRow;
use std::str::FromStr;
use uuid::Uuid;

/// Direct image of a `jobs` row; `status` is read as text and parsed.
#[derive(Debug, FromRow)]
pub struct JobRow {
    pub id: Uuid,
    pub queue: String,
    pub job_type: String,
    pub payload: Json,
    pub status: String,
    pub priority: i32,
    pub idempotency_key: Option<String>,
    pub max_attempts: i32,
    pub attempts: i32,
    pub run_at: DateTime<Utc>,
    pub timeout_secs: i32,
    pub backoff_base_secs: i32,
    pub backoff_max_secs: i32,
    pub dead_letter: bool,
    pub locked_by: Option<Uuid>,
    pub locked_at: Option<DateTime<Utc>>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub result: Option<Json>,
    pub metadata: Json,
    pub correlation_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

impl JobRow {
    /// Convert into the domain [`Job`], parsing the status string. A status the
    /// DB CHECK constraint should make impossible is reported as a storage error.
    pub fn into_job(self) -> Result<Job, QueueError> {
        let status = JobStatus::from_str(&self.status)
            .map_err(|e| QueueError::Storage(format!("invalid status in db: {e}")))?;
        Ok(Job {
            id: self.id,
            queue: self.queue,
            job_type: self.job_type,
            payload: self.payload,
            status,
            priority: self.priority,
            idempotency_key: self.idempotency_key,
            max_attempts: self.max_attempts,
            attempts: self.attempts,
            run_at: self.run_at,
            timeout_secs: self.timeout_secs,
            backoff_base_secs: self.backoff_base_secs,
            backoff_max_secs: self.backoff_max_secs,
            dead_letter: self.dead_letter,
            locked_by: self.locked_by,
            locked_at: self.locked_at,
            lease_expires_at: self.lease_expires_at,
            last_error: self.last_error,
            result: self.result,
            metadata: self.metadata,
            correlation_id: self.correlation_id,
            created_at: self.created_at,
            updated_at: self.updated_at,
            completed_at: self.completed_at,
        })
    }
}

/// The list of `jobs` columns, in `JobRow` order, for `RETURNING` / `SELECT`.
pub const JOB_COLUMNS: &str = "id, queue, job_type, payload, status, priority, \
    idempotency_key, max_attempts, attempts, run_at, timeout_secs, \
    backoff_base_secs, backoff_max_secs, dead_letter, locked_by, locked_at, \
    lease_expires_at, last_error, result, metadata, correlation_id, \
    created_at, updated_at, completed_at";

#[derive(Debug, FromRow)]
pub struct QueueRow {
    pub name: String,
    pub paused: bool,
    pub max_concurrency: Option<i32>,
    pub created_at: DateTime<Utc>,
}

impl From<QueueRow> for djq_core::Queue {
    fn from(r: QueueRow) -> Self {
        djq_core::Queue {
            name: r.name,
            paused: r.paused,
            max_concurrency: r.max_concurrency,
            created_at: r.created_at,
        }
    }
}

#[derive(Debug, FromRow)]
pub struct AttemptRow {
    pub id: i64,
    pub job_id: Uuid,
    pub attempt: i32,
    pub worker_id: Option<Uuid>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub status: String,
    pub error: Option<String>,
}

impl From<AttemptRow> for JobAttempt {
    fn from(r: AttemptRow) -> Self {
        JobAttempt {
            id: r.id,
            job_id: r.job_id,
            attempt: r.attempt,
            worker_id: r.worker_id,
            started_at: r.started_at,
            finished_at: r.finished_at,
            status: r.status,
            error: r.error,
        }
    }
}

#[derive(Debug, FromRow)]
pub struct WorkerRow {
    pub id: Uuid,
    pub hostname: String,
    pub queues: Vec<String>,
    pub concurrency: i32,
    pub registered_at: DateTime<Utc>,
    pub last_heartbeat: DateTime<Utc>,
}

impl From<WorkerRow> for Worker {
    fn from(r: WorkerRow) -> Self {
        Worker {
            id: r.id,
            hostname: r.hostname,
            queues: r.queues,
            concurrency: r.concurrency,
            registered_at: r.registered_at,
            last_heartbeat: r.last_heartbeat,
        }
    }
}

#[derive(Debug, FromRow)]
pub struct DeadLetterRow {
    pub id: Uuid,
    pub queue: String,
    pub job_type: String,
    pub payload: Json,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub dead_lettered_at: DateTime<Utc>,
    pub original_created_at: Option<DateTime<Utc>>,
}

impl From<DeadLetterRow> for DeadLetterJob {
    fn from(r: DeadLetterRow) -> Self {
        DeadLetterJob {
            id: r.id,
            queue: r.queue,
            job_type: r.job_type,
            payload: r.payload,
            attempts: r.attempts,
            last_error: r.last_error,
            dead_lettered_at: r.dead_lettered_at,
            original_created_at: r.original_created_at,
        }
    }
}
