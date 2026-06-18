//! Strongly typed domain models for jobs, queues, workers and attempts.
//!
//! These types are storage-agnostic. The `djq-storage` crate maps them to and
//! from Postgres rows; the API crate (de)serializes them at the HTTP boundary.

use crate::status::JobStatus;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use uuid::Uuid;

/// A named queue. Pausing a queue stops new leases without affecting in-flight
/// jobs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Queue {
    pub name: String,
    pub paused: bool,
    /// Optional advisory cap surfaced to workers; `None` means unbounded.
    pub max_concurrency: Option<i32>,
    pub created_at: DateTime<Utc>,
}

/// A unit of background work and its full lifecycle state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: Uuid,
    pub queue: String,
    pub job_type: String,
    pub payload: Json,
    pub status: JobStatus,
    /// Higher values are leased first.
    pub priority: i32,
    pub idempotency_key: Option<String>,
    pub max_attempts: i32,
    pub attempts: i32,
    /// Earliest time the job may be leased (delayed/scheduled execution).
    pub run_at: DateTime<Utc>,
    /// Per-attempt execution timeout in seconds.
    pub timeout_secs: i32,
    pub backoff_base_secs: i32,
    pub backoff_max_secs: i32,
    /// When `true`, exhausting attempts moves the job to `dead_lettered`;
    /// when `false` it terminates as `failed` (inspectable, manually retryable).
    pub dead_letter: bool,
    /// Worker currently holding the lease, if any.
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

/// Caller-supplied job submission request, before defaults are applied.
#[derive(Debug, Clone, Deserialize)]
pub struct NewJob {
    pub queue: String,
    pub job_type: String,
    #[serde(default = "default_payload")]
    pub payload: Json,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    /// Defaults applied in [`NewJob::normalized`] when absent.
    #[serde(default)]
    pub max_attempts: Option<i32>,
    /// Seconds to delay before the job becomes eligible.
    #[serde(default)]
    pub delay_secs: Option<i64>,
    /// Absolute time to run at (overrides `delay_secs` when both are set).
    #[serde(default)]
    pub run_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub timeout_secs: Option<i32>,
    #[serde(default)]
    pub backoff_base_secs: Option<i32>,
    #[serde(default)]
    pub backoff_max_secs: Option<i32>,
    /// Defaults to `true` (dead-letter on exhaustion).
    #[serde(default)]
    pub dead_letter: Option<bool>,
    #[serde(default = "default_metadata")]
    pub metadata: Json,
    #[serde(default)]
    pub correlation_id: Option<String>,
}

fn default_payload() -> Json {
    Json::Object(Default::default())
}
fn default_metadata() -> Json {
    Json::Object(Default::default())
}

/// Validated, defaults-applied submission ready for persistence.
#[derive(Debug, Clone)]
pub struct NormalizedJob {
    pub queue: String,
    pub job_type: String,
    pub payload: Json,
    pub priority: i32,
    pub idempotency_key: Option<String>,
    pub max_attempts: i32,
    pub run_at: DateTime<Utc>,
    pub status: JobStatus,
    pub timeout_secs: i32,
    pub backoff_base_secs: i32,
    pub backoff_max_secs: i32,
    pub dead_letter: bool,
    pub metadata: Json,
    pub correlation_id: Option<String>,
}

impl NewJob {
    /// Validate the request and apply defaults. `now` is injected so the
    /// computation is deterministic and unit-testable.
    pub fn normalized(self, now: DateTime<Utc>) -> crate::Result<NormalizedJob> {
        use crate::QueueError;

        if self.queue.trim().is_empty() {
            return Err(QueueError::Validation("queue must not be empty".into()));
        }
        if self.job_type.trim().is_empty() {
            return Err(QueueError::Validation("job_type must not be empty".into()));
        }
        if self.queue.len() > 128 {
            return Err(QueueError::Validation(
                "queue name too long (max 128)".into(),
            ));
        }
        if let Some(key) = &self.idempotency_key {
            if key.len() > 255 {
                return Err(QueueError::Validation(
                    "idempotency_key too long (max 255)".into(),
                ));
            }
        }

        let max_attempts = self.max_attempts.unwrap_or(5);
        if !(1..=100).contains(&max_attempts) {
            return Err(QueueError::Validation(
                "max_attempts must be between 1 and 100".into(),
            ));
        }

        let timeout_secs = self.timeout_secs.unwrap_or(300);
        if !(1..=86_400).contains(&timeout_secs) {
            return Err(QueueError::Validation(
                "timeout_secs must be between 1 and 86400".into(),
            ));
        }

        let backoff_base_secs = self.backoff_base_secs.unwrap_or(2).max(1);
        let backoff_max_secs = self.backoff_max_secs.unwrap_or(300).max(backoff_base_secs);

        // Resolve scheduling: explicit run_at wins, else delay, else now.
        let run_at = match (self.run_at, self.delay_secs) {
            (Some(t), _) => t,
            (None, Some(d)) if d > 0 => now + chrono::Duration::seconds(d),
            _ => now,
        };
        let status = if run_at > now {
            JobStatus::Scheduled
        } else {
            JobStatus::Queued
        };

        Ok(NormalizedJob {
            queue: self.queue,
            job_type: self.job_type,
            payload: self.payload,
            priority: self.priority,
            idempotency_key: self.idempotency_key,
            max_attempts,
            run_at,
            status,
            timeout_secs,
            backoff_base_secs,
            backoff_max_secs,
            dead_letter: self.dead_letter.unwrap_or(true),
            metadata: self.metadata,
            correlation_id: self.correlation_id,
        })
    }
}

/// A single execution attempt against a job (for the attempt-history view).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobAttempt {
    pub id: i64,
    pub job_id: Uuid,
    pub attempt: i32,
    pub worker_id: Option<Uuid>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    /// `succeeded` | `failed` | `timeout`.
    pub status: String,
    pub error: Option<String>,
}

/// A registered worker process and its liveness state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Worker {
    pub id: Uuid,
    pub hostname: String,
    pub queues: Vec<String>,
    pub concurrency: i32,
    pub registered_at: DateTime<Utc>,
    pub last_heartbeat: DateTime<Utc>,
}

/// A job that exhausted its attempts and was moved to the dead-letter table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetterJob {
    pub id: Uuid,
    pub queue: String,
    pub job_type: String,
    pub payload: Json,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub dead_lettered_at: DateTime<Utc>,
    pub original_created_at: Option<DateTime<Utc>>,
}

/// Aggregate counts for a queue, grouped by status.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueueStats {
    pub queue: String,
    pub paused: bool,
    pub queued: i64,
    pub scheduled: i64,
    pub processing: i64,
    pub completed: i64,
    pub failed: i64,
    pub retrying: i64,
    pub cancelled: i64,
    pub dead_lettered: i64,
}

/// Filter + pagination parameters for the job-listing endpoint.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct JobFilter {
    pub queue: Option<String>,
    pub status: Option<JobStatus>,
    pub job_type: Option<String>,
    #[serde(default)]
    pub limit: Option<i64>,
    #[serde(default)]
    pub offset: Option<i64>,
}

impl JobFilter {
    /// Clamp pagination to safe bounds (max page size 200).
    pub fn bounded(&self) -> (i64, i64) {
        let limit = self.limit.unwrap_or(50).clamp(1, 200);
        let offset = self.offset.unwrap_or(0).max(0);
        (limit, offset)
    }
}

/// Outcome reported by a worker after executing a job.
#[derive(Debug, Clone)]
pub enum ExecutionOutcome {
    /// Success, with an optional JSON result to persist.
    Success(Option<Json>),
    /// Failure with an error message; the queue decides retry vs dead-letter.
    Failure(String),
    /// The per-attempt timeout elapsed before the handler returned.
    Timeout,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    }

    fn base() -> NewJob {
        NewJob {
            queue: "emails".into(),
            job_type: "send".into(),
            payload: serde_json::json!({"to": "a@b.c"}),
            priority: 0,
            idempotency_key: None,
            max_attempts: None,
            delay_secs: None,
            run_at: None,
            timeout_secs: None,
            backoff_base_secs: None,
            backoff_max_secs: None,
            dead_letter: None,
            metadata: default_metadata(),
            correlation_id: None,
        }
    }

    #[test]
    fn applies_sensible_defaults() {
        let n = base().normalized(now()).unwrap();
        assert_eq!(n.max_attempts, 5);
        assert_eq!(n.timeout_secs, 300);
        assert_eq!(n.status, JobStatus::Queued);
        assert_eq!(n.run_at, now());
    }

    #[test]
    fn delay_makes_it_scheduled() {
        let n = NewJob {
            delay_secs: Some(60),
            ..base()
        }
        .normalized(now())
        .unwrap();
        assert_eq!(n.status, JobStatus::Scheduled);
        assert_eq!(n.run_at, now() + chrono::Duration::seconds(60));
    }

    #[test]
    fn explicit_run_at_overrides_delay() {
        let target = now() + chrono::Duration::seconds(3600);
        let n = NewJob {
            run_at: Some(target),
            delay_secs: Some(10),
            ..base()
        }
        .normalized(now())
        .unwrap();
        assert_eq!(n.run_at, target);
        assert_eq!(n.status, JobStatus::Scheduled);
    }

    #[test]
    fn rejects_empty_queue() {
        let n = NewJob {
            queue: "  ".into(),
            ..base()
        }
        .normalized(now());
        assert!(n.is_err());
    }

    #[test]
    fn rejects_bad_max_attempts() {
        let n = NewJob {
            max_attempts: Some(0),
            ..base()
        }
        .normalized(now());
        assert!(n.is_err());
    }

    #[test]
    fn backoff_max_is_floored_to_base() {
        let n = NewJob {
            backoff_base_secs: Some(30),
            backoff_max_secs: Some(5),
            ..base()
        }
        .normalized(now())
        .unwrap();
        assert_eq!(n.backoff_max_secs, 30);
    }
}
