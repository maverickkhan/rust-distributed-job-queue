//! Reusable domain errors shared across the storage, queue and worker crates.
//!
//! These use [`thiserror`] so callers can match on variants. Application
//! boundaries (the API and worker binaries) wrap these in `anyhow` for
//! reporting, but library code never does.

use thiserror::Error;

/// Errors that can arise from the persistence/queue domain.
#[derive(Debug, Error)]
pub enum QueueError {
    /// The requested job does not exist.
    #[error("job not found: {0}")]
    JobNotFound(uuid::Uuid),

    /// The requested queue does not exist.
    #[error("queue not found: {0}")]
    QueueNotFound(String),

    /// A job with the same `(queue, idempotency_key)` already exists.
    /// Carries the id of the pre-existing job so callers can return it.
    #[error("duplicate idempotency key for queue {queue}")]
    DuplicateIdempotencyKey {
        queue: String,
        existing_job_id: uuid::Uuid,
    },

    /// An attempted status change violates the [`crate::JobStatus`] machine.
    #[error("illegal state transition from {from} to {to}")]
    IllegalTransition {
        from: crate::JobStatus,
        to: crate::JobStatus,
    },

    /// The operation is not valid for the job's current status.
    #[error("operation '{op}' not permitted while job is {status}")]
    InvalidOperation {
        op: &'static str,
        status: crate::JobStatus,
    },

    /// Caller-supplied input failed validation.
    #[error("validation error: {0}")]
    Validation(String),

    /// An underlying storage/database failure.
    #[error("storage error: {0}")]
    Storage(String),

    /// JSON (de)serialization failure on a payload or result.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

impl QueueError {
    /// Coarse category used to map errors onto HTTP status codes and metrics.
    pub fn category(&self) -> ErrorCategory {
        match self {
            QueueError::JobNotFound(_) | QueueError::QueueNotFound(_) => ErrorCategory::NotFound,
            QueueError::DuplicateIdempotencyKey { .. } => ErrorCategory::Conflict,
            QueueError::IllegalTransition { .. } | QueueError::InvalidOperation { .. } => {
                ErrorCategory::Conflict
            }
            QueueError::Validation(_) | QueueError::Serialization(_) => ErrorCategory::BadRequest,
            QueueError::Storage(_) => ErrorCategory::Internal,
        }
    }
}

/// Stable, transport-agnostic error categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    BadRequest,
    NotFound,
    Conflict,
    Internal,
}

/// Convenience alias for fallible domain operations.
pub type Result<T> = std::result::Result<T, QueueError>;
