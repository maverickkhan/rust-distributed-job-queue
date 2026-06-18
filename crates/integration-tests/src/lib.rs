//! Shared helpers for the integration test suite.
//!
//! Tests connect to the database named by `TEST_DATABASE_URL` (falling back to
//! `DATABASE_URL`). When neither is set the helper returns `None` and each test
//! prints a skip notice and returns early, so `cargo test` stays green without
//! infrastructure while still running for real in CI / `make test`.
//!
//! Isolation: every test uses a unique, UUID-derived queue name so suites can
//! share one database without interfering.

use djq_core::NewJob;
use djq_queue::QueueService;
use djq_storage::PgStore;
use djq_telemetry::Metrics;
use std::sync::Arc;
use uuid::Uuid;

/// A connected service plus the concrete store (for direct lower-level calls).
pub struct TestCtx {
    pub service: QueueService,
    pub store: Arc<PgStore>,
}

/// Connect + migrate, or `None` when no database URL is configured.
pub async fn ctx() -> Option<TestCtx> {
    let url = std::env::var("TEST_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .ok()?;
    let store = PgStore::connect(&url, 16)
        .await
        .expect("connect to test database");
    store.migrate().await.expect("run migrations");
    let store = Arc::new(store);
    let service = QueueService::new(store.clone(), Metrics::new());
    Some(TestCtx { service, store })
}

/// A unique queue name for test isolation.
pub fn unique_queue() -> String {
    format!("test_{}", Uuid::new_v4().simple())
}

/// Build a minimal valid `NewJob` for `queue`.
pub fn new_job(queue: &str, job_type: &str, payload: serde_json::Value) -> NewJob {
    NewJob {
        queue: queue.to_string(),
        job_type: job_type.to_string(),
        payload,
        priority: 0,
        idempotency_key: None,
        max_attempts: None,
        delay_secs: None,
        run_at: None,
        timeout_secs: None,
        backoff_base_secs: None,
        backoff_max_secs: None,
        dead_letter: None,
        metadata: serde_json::json!({}),
        correlation_id: None,
    }
}
