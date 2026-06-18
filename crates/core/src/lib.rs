//! `djq-core` — storage-agnostic domain for the distributed job queue.
//!
//! This crate is deliberately free of I/O, databases and async runtimes
//! (beyond the `async_trait` definitions). It holds:
//!
//! * [`model`] — strongly typed jobs, queues, workers and attempts.
//! * [`status`] — the [`JobStatus`] lifecycle and its legal-transition machine.
//! * [`backoff`] — pure exponential-backoff arithmetic.
//! * [`error`] — reusable [`QueueError`] domain errors via `thiserror`.
//! * [`store`] — the [`Store`] trait that every backend implements.
//!
//! Keeping these concerns pure makes the bulk of the business logic unit
//! testable without a database.

pub mod backoff;
pub mod error;
pub mod model;
pub mod status;
pub mod store;

pub use backoff::BackoffPolicy;
pub use error::{ErrorCategory, QueueError, Result};
pub use model::{
    DeadLetterJob, ExecutionOutcome, Job, JobAttempt, JobFilter, NewJob, NormalizedJob, Queue,
    QueueStats, Worker,
};
pub use status::{JobStatus, ParseStatusError};
pub use store::{Store, Submission};
