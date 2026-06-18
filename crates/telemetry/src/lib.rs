//! Centralized tracing and Prometheus metrics for every binary.
//!
//! [`init_tracing`] installs a structured subscriber driven by `RUST_LOG`
//! (defaulting to `info`) with optional JSON output. [`Metrics`] holds the
//! Prometheus collectors shared by the API and worker; it is cheap to clone
//! (everything behind it is `Arc` internally in the `prometheus` crate).

use once_cell::sync::OnceCell;
use prometheus::{
    Encoder, Gauge, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
    Opts, Registry, TextEncoder,
};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

static TRACING: OnceCell<()> = OnceCell::new();

/// Install the global tracing subscriber. Safe to call multiple times; only
/// the first call takes effect (subsequent calls are no-ops).
///
/// * `json` — emit machine-readable JSON lines instead of pretty text.
/// * `service` — value attached as the `service` field on every span/event.
pub fn init_tracing(service: &str, json: bool) {
    TRACING.get_or_init(|| {
        let filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("info,sqlx=warn,hyper=warn"));

        let registry = tracing_subscriber::registry().with(filter);
        if json {
            registry
                .with(fmt::layer().json().with_current_span(true))
                .init();
        } else {
            registry.with(fmt::layer().compact()).init();
        }
        tracing::info!(service, "tracing initialized");
    });
}

/// All Prometheus collectors for the queue, plus the registry that renders
/// them at `/metrics`.
#[derive(Clone)]
pub struct Metrics {
    pub registry: Registry,
    /// Jobs accepted at the API, labelled by queue.
    pub jobs_submitted: IntCounterVec,
    /// Jobs that completed successfully, labelled by queue.
    pub jobs_completed: IntCounterVec,
    /// Jobs that failed an attempt, labelled by queue.
    pub jobs_failed: IntCounterVec,
    /// Retries scheduled, labelled by queue.
    pub jobs_retried: IntCounterVec,
    /// Jobs moved to the dead-letter queue, labelled by queue.
    pub jobs_dead_lettered: IntCounterVec,
    /// Attempts that hit their execution timeout.
    pub jobs_timed_out: IntCounter,
    /// Current backlog depth (leaseable jobs) per queue.
    pub queue_depth: IntGaugeVec,
    /// Number of workers seen alive in the last heartbeat window.
    pub active_workers: IntGauge,
    /// End-to-end processing duration in seconds.
    pub processing_duration: Histogram,
    /// Time a job spent leaseable before being picked up (queue wait).
    pub lease_wait: Histogram,
    /// Build info as a constant gauge (value always 1).
    pub build_info: Gauge,
}

impl Metrics {
    /// Construct and register every collector against a fresh registry.
    pub fn new() -> Self {
        let registry = Registry::new();

        let jobs_submitted = IntCounterVec::new(
            Opts::new("djq_jobs_submitted_total", "Jobs accepted at the API"),
            &["queue"],
        )
        .expect("valid metric");
        let jobs_completed = IntCounterVec::new(
            Opts::new("djq_jobs_completed_total", "Jobs completed successfully"),
            &["queue"],
        )
        .expect("valid metric");
        let jobs_failed = IntCounterVec::new(
            Opts::new("djq_jobs_failed_total", "Job attempts that failed"),
            &["queue"],
        )
        .expect("valid metric");
        let jobs_retried = IntCounterVec::new(
            Opts::new("djq_jobs_retried_total", "Retries scheduled"),
            &["queue"],
        )
        .expect("valid metric");
        let jobs_dead_lettered = IntCounterVec::new(
            Opts::new("djq_jobs_dead_lettered_total", "Jobs sent to the DLQ"),
            &["queue"],
        )
        .expect("valid metric");
        let jobs_timed_out =
            IntCounter::with_opts(Opts::new("djq_jobs_timed_out_total", "Attempts timed out"))
                .expect("valid metric");
        let queue_depth = IntGaugeVec::new(
            Opts::new("djq_queue_depth", "Leaseable jobs currently waiting"),
            &["queue"],
        )
        .expect("valid metric");
        let active_workers =
            IntGauge::with_opts(Opts::new("djq_active_workers", "Workers alive in window"))
                .expect("valid metric");
        let processing_duration = Histogram::with_opts(
            HistogramOpts::new(
                "djq_processing_duration_seconds",
                "Job processing duration in seconds",
            )
            .buckets(vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
            ]),
        )
        .expect("valid metric");
        let lease_wait = Histogram::with_opts(
            HistogramOpts::new(
                "djq_lease_wait_seconds",
                "Time spent leaseable before pickup",
            )
            .buckets(vec![
                0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0,
            ]),
        )
        .expect("valid metric");
        let build_info = Gauge::with_opts(
            Opts::new("djq_build_info", "Build info")
                .const_label("version", env!("CARGO_PKG_VERSION")),
        )
        .expect("valid metric");
        build_info.set(1.0);

        for c in [
            &jobs_submitted,
            &jobs_completed,
            &jobs_failed,
            &jobs_retried,
            &jobs_dead_lettered,
        ] {
            registry.register(Box::new(c.clone())).expect("register");
        }
        registry
            .register(Box::new(jobs_timed_out.clone()))
            .expect("register");
        registry
            .register(Box::new(queue_depth.clone()))
            .expect("register");
        registry
            .register(Box::new(active_workers.clone()))
            .expect("register");
        registry
            .register(Box::new(processing_duration.clone()))
            .expect("register");
        registry
            .register(Box::new(lease_wait.clone()))
            .expect("register");
        registry
            .register(Box::new(build_info.clone()))
            .expect("register");

        Self {
            registry,
            jobs_submitted,
            jobs_completed,
            jobs_failed,
            jobs_retried,
            jobs_dead_lettered,
            jobs_timed_out,
            queue_depth,
            active_workers,
            processing_duration,
            lease_wait,
            build_info,
        }
    }

    /// Render the registry in the Prometheus text exposition format.
    pub fn render(&self) -> String {
        let mut buf = Vec::new();
        let encoder = TextEncoder::new();
        let families = self.registry.gather();
        // Encoding to an in-memory buffer cannot fail for well-formed metrics.
        if let Err(e) = encoder.encode(&families, &mut buf) {
            tracing::error!(error = %e, "failed to encode metrics");
            return String::new();
        }
        String::from_utf8(buf).unwrap_or_default()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_registered_metrics() {
        let m = Metrics::new();
        m.jobs_submitted.with_label_values(&["emails"]).inc();
        m.active_workers.set(3);
        let out = m.render();
        assert!(out.contains("djq_jobs_submitted_total"));
        assert!(out.contains("djq_active_workers 3"));
        assert!(out.contains("djq_build_info"));
    }
}
