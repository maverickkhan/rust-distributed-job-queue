//! Job lifecycle status and the legal state-transition machine.
//!
//! The queue guarantees that jobs only ever move between states along the
//! edges encoded in [`JobStatus::can_transition_to`]. Every storage mutation
//! that changes a job's status must respect this machine; the integration
//! tests assert the invariants hold under concurrency.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// The lifecycle status of a job.
///
/// ```text
///                 ┌───────────► cancelled
///                 │
///  queued ───► processing ───► completed
///    ▲  ▲          │
///    │  │          ├──► retrying ──► processing
/// scheduled        │
///                  └──► failed ──► dead_lettered
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    /// Ready to be leased by a worker (`run_at <= now`).
    Queued,
    /// Accepted but not yet eligible (`run_at` in the future).
    Scheduled,
    /// Leased by a worker and currently executing.
    Processing,
    /// Finished successfully; a result may be attached.
    Completed,
    /// Terminal failure after the final attempt (kept for inspection).
    Failed,
    /// Failed but eligible for another attempt after a backoff delay.
    Retrying,
    /// Cancelled by an operator before completion.
    Cancelled,
    /// Exhausted all attempts and moved to the dead-letter queue.
    DeadLettered,
}

impl JobStatus {
    /// String form used in the Postgres `job_status` enum and the REST API.
    pub fn as_str(&self) -> &'static str {
        match self {
            JobStatus::Queued => "queued",
            JobStatus::Scheduled => "scheduled",
            JobStatus::Processing => "processing",
            JobStatus::Completed => "completed",
            JobStatus::Failed => "failed",
            JobStatus::Retrying => "retrying",
            JobStatus::Cancelled => "cancelled",
            JobStatus::DeadLettered => "dead_lettered",
        }
    }

    /// A terminal status can never transition again.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            JobStatus::Completed | JobStatus::Cancelled | JobStatus::DeadLettered
        )
    }

    /// True when a job in this status is eligible to be leased by a worker
    /// (subject to `run_at` and the queue not being paused).
    pub fn is_leaseable(&self) -> bool {
        matches!(
            self,
            JobStatus::Queued | JobStatus::Scheduled | JobStatus::Retrying
        )
    }

    /// Whether a job in this status may be cancelled by an operator.
    pub fn is_cancellable(&self) -> bool {
        matches!(
            self,
            JobStatus::Queued | JobStatus::Scheduled | JobStatus::Retrying | JobStatus::Processing
        )
    }

    /// The authoritative legal-transition table for the queue.
    pub fn can_transition_to(&self, next: JobStatus) -> bool {
        use JobStatus::*;
        matches!(
            (self, next),
            (Queued, Processing)
                | (Queued, Scheduled)
                | (Queued, Cancelled)
                | (Scheduled, Queued)
                | (Scheduled, Processing)
                | (Scheduled, Cancelled)
                | (Processing, Completed)
                | (Processing, Failed)
                | (Processing, Retrying)
                | (Processing, Queued) // lease expiry → re-queue
                | (Processing, Cancelled)
                | (Retrying, Processing)
                | (Retrying, Queued)
                | (Retrying, Cancelled)
                | (Retrying, DeadLettered)
                | (Failed, DeadLettered)
                | (Failed, Retrying)
        )
    }
}

impl fmt::Display for JobStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Error returned when parsing an unknown status string.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown job status: {0}")]
pub struct ParseStatusError(pub String);

impl FromStr for JobStatus {
    type Err = ParseStatusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "queued" => JobStatus::Queued,
            "scheduled" => JobStatus::Scheduled,
            "processing" => JobStatus::Processing,
            "completed" => JobStatus::Completed,
            "failed" => JobStatus::Failed,
            "retrying" => JobStatus::Retrying,
            "cancelled" => JobStatus::Cancelled,
            "dead_lettered" => JobStatus::DeadLettered,
            other => return Err(ParseStatusError(other.to_string())),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips_through_string() {
        for s in [
            JobStatus::Queued,
            JobStatus::Scheduled,
            JobStatus::Processing,
            JobStatus::Completed,
            JobStatus::Failed,
            JobStatus::Retrying,
            JobStatus::Cancelled,
            JobStatus::DeadLettered,
        ] {
            assert_eq!(JobStatus::from_str(s.as_str()).unwrap(), s);
        }
    }

    #[test]
    fn terminal_states_have_no_outgoing_edges() {
        for s in [
            JobStatus::Completed,
            JobStatus::Cancelled,
            JobStatus::DeadLettered,
        ] {
            assert!(s.is_terminal());
            for next in [
                JobStatus::Queued,
                JobStatus::Processing,
                JobStatus::Completed,
                JobStatus::Retrying,
            ] {
                assert!(
                    !s.can_transition_to(next),
                    "terminal {s} must not transition to {next}"
                );
            }
        }
    }

    #[test]
    fn happy_path_is_legal() {
        assert!(JobStatus::Queued.can_transition_to(JobStatus::Processing));
        assert!(JobStatus::Processing.can_transition_to(JobStatus::Completed));
    }

    #[test]
    fn retry_path_is_legal() {
        assert!(JobStatus::Processing.can_transition_to(JobStatus::Retrying));
        assert!(JobStatus::Retrying.can_transition_to(JobStatus::Processing));
        assert!(JobStatus::Retrying.can_transition_to(JobStatus::DeadLettered));
    }

    #[test]
    fn cannot_complete_a_queued_job_directly() {
        assert!(!JobStatus::Queued.can_transition_to(JobStatus::Completed));
    }

    #[test]
    fn lease_expiry_requeue_is_legal() {
        assert!(JobStatus::Processing.can_transition_to(JobStatus::Queued));
    }
}
