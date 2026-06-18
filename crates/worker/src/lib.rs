//! `djq-worker` — the worker runtime.
//!
//! A worker registers itself, sends heartbeats, leases jobs one at a time up to
//! a configured concurrency, executes the matching [`JobHandler`] under a
//! per-job timeout, reports the outcome, and drains in-flight work on shutdown.
//!
//! The [`HandlerRegistry`] maps a job's `job_type` to an implementation, so the
//! same binary can serve many job kinds.

pub mod config;
pub mod handlers;
pub mod runtime;

use async_trait::async_trait;
use djq_core::Job;
use serde_json::Value as Json;
use std::collections::HashMap;
use std::sync::Arc;

pub use config::WorkerConfig;
pub use runtime::WorkerRuntime;

/// The result a handler returns: `Ok(Some(json))` to attach a result,
/// `Ok(None)` for success without a payload, or `Err(message)` to fail.
pub type HandlerResult = std::result::Result<Option<Json>, String>;

/// A unit of executable work, keyed by `job_type`.
///
/// Handlers must be cancellation-friendly: the runtime enforces the per-job
/// timeout by dropping the future, so any blocking work must run via
/// [`tokio::task::spawn_blocking`].
#[async_trait]
pub trait JobHandler: Send + Sync + 'static {
    /// Execute the job. The `job.payload` carries the typed input.
    async fn handle(&self, job: &Job) -> HandlerResult;
}

/// Maps `job_type` strings to handlers.
#[derive(Clone, Default)]
pub struct HandlerRegistry {
    handlers: HashMap<String, Arc<dyn JobHandler>>,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a handler for a `job_type`, replacing any existing one.
    pub fn register(mut self, job_type: impl Into<String>, handler: Arc<dyn JobHandler>) -> Self {
        self.handlers.insert(job_type.into(), handler);
        self
    }

    /// Look up the handler for a job type.
    pub fn get(&self, job_type: &str) -> Option<Arc<dyn JobHandler>> {
        self.handlers.get(job_type).cloned()
    }

    /// The set of registered job types (for diagnostics).
    pub fn job_types(&self) -> Vec<String> {
        let mut v: Vec<String> = self.handlers.keys().cloned().collect();
        v.sort();
        v
    }

    /// Register the built-in example handlers (echo, sum, sleep, fail, flaky).
    pub fn with_examples(self) -> Self {
        use handlers::*;
        self.register("echo", Arc::new(EchoHandler))
            .register("sum", Arc::new(SumHandler))
            .register("sleep", Arc::new(SleepHandler))
            .register("fail", Arc::new(FailHandler))
            .register("flaky", Arc::new(FlakyHandler))
    }
}
