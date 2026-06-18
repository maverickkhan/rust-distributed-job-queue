//! Environment-driven worker configuration.

use std::time::Duration;
use uuid::Uuid;

/// Runtime configuration for a single worker process.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Stable worker identity (generated per process by default).
    pub id: Uuid,
    /// Reported hostname.
    pub hostname: String,
    /// Queues this worker pulls from (ordered by priority of attempt).
    pub queues: Vec<String>,
    /// Maximum jobs executed concurrently.
    pub concurrency: usize,
    /// Lease duration requested per job; renewed to cover the job timeout.
    pub lease_secs: i64,
    /// Idle poll interval when no job is available.
    pub poll_interval: Duration,
    /// Heartbeat cadence.
    pub heartbeat_interval: Duration,
    /// How long to wait for in-flight jobs to drain on shutdown.
    pub shutdown_grace: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            id: Uuid::new_v4(),
            hostname: "unknown".to_string(),
            queues: vec!["default".to_string()],
            concurrency: 8,
            lease_secs: 30,
            poll_interval: Duration::from_millis(500),
            heartbeat_interval: Duration::from_secs(10),
            shutdown_grace: Duration::from_secs(30),
        }
    }
}

impl WorkerConfig {
    /// Load configuration from the environment, falling back to defaults.
    ///
    /// * `WORKER_QUEUES` — comma-separated queue names (default `default`).
    /// * `WORKER_CONCURRENCY` — max concurrent jobs (default `8`).
    /// * `WORKER_LEASE_SECS` — lease duration (default `30`).
    /// * `WORKER_POLL_MS` — idle poll interval (default `500`).
    /// * `WORKER_HEARTBEAT_SECS` — heartbeat cadence (default `10`).
    /// * `WORKER_SHUTDOWN_GRACE_SECS` — drain timeout (default `30`).
    /// * `HOSTNAME` — reported hostname.
    pub fn from_env() -> Self {
        let mut cfg = WorkerConfig::default();
        if let Ok(h) = std::env::var("HOSTNAME") {
            if !h.is_empty() {
                cfg.hostname = h;
            }
        }
        if let Ok(q) = std::env::var("WORKER_QUEUES") {
            let queues: Vec<String> = q
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if !queues.is_empty() {
                cfg.queues = queues;
            }
        }
        if let Some(n) = env_parse("WORKER_CONCURRENCY") {
            cfg.concurrency = (n as usize).max(1);
        }
        if let Some(n) = env_parse("WORKER_LEASE_SECS") {
            cfg.lease_secs = n.max(1);
        }
        if let Some(n) = env_parse("WORKER_POLL_MS") {
            cfg.poll_interval = Duration::from_millis(n.max(10) as u64);
        }
        if let Some(n) = env_parse("WORKER_HEARTBEAT_SECS") {
            cfg.heartbeat_interval = Duration::from_secs(n.max(1) as u64);
        }
        if let Some(n) = env_parse("WORKER_SHUTDOWN_GRACE_SECS") {
            cfg.shutdown_grace = Duration::from_secs(n.max(1) as u64);
        }
        cfg
    }
}

fn env_parse(key: &str) -> Option<i64> {
    std::env::var(key).ok().and_then(|v| v.trim().parse().ok())
}
