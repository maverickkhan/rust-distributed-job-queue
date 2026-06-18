//! Background maintenance: lease recovery, worker pruning and gauge refresh.
//!
//! Run as a single Tokio task (typically from the API binary). It cancels
//! cleanly via a [`CancellationToken`] so the process can shut down gracefully.

use crate::QueueService;
use std::time::Duration;
use tokio::time::{interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

/// Tunables for the maintenance loop.
#[derive(Debug, Clone)]
pub struct MaintenanceConfig {
    /// How often to run a sweep.
    pub tick: Duration,
    /// Max jobs to recover per sweep (bounded to avoid long transactions).
    pub recover_batch: i64,
    /// Workers silent longer than this are considered dead and pruned.
    pub worker_ttl_secs: i64,
    /// Window used to count "active" workers for the gauge.
    pub active_window_secs: i64,
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            tick: Duration::from_secs(5),
            recover_batch: 100,
            worker_ttl_secs: 60,
            active_window_secs: 30,
        }
    }
}

/// Run sweeps until `shutdown` is triggered. Errors are logged, never fatal —
/// a transient DB blip must not kill the maintenance loop.
pub async fn run(service: QueueService, cfg: MaintenanceConfig, shutdown: CancellationToken) {
    let mut ticker = interval(cfg.tick);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    tracing::info!(?cfg.tick, "maintenance loop started");

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("maintenance loop shutting down");
                break;
            }
            _ = ticker.tick() => {
                sweep(&service, &cfg).await;
            }
        }
    }
}

async fn sweep(service: &QueueService, cfg: &MaintenanceConfig) {
    let store = service.store();

    match store.recover_expired_leases(cfg.recover_batch).await {
        Ok(ids) if !ids.is_empty() => {
            tracing::warn!(count = ids.len(), "recovered abandoned jobs");
        }
        Ok(_) => {}
        Err(e) => tracing::error!(error = %e, "lease recovery failed"),
    }

    match store.prune_dead_workers(cfg.worker_ttl_secs).await {
        Ok(n) if n > 0 => tracing::info!(pruned = n, "removed dead workers"),
        Ok(_) => {}
        Err(e) => tracing::error!(error = %e, "worker pruning failed"),
    }

    // Refresh gauges.
    match store.list_active_workers(cfg.active_window_secs).await {
        Ok(workers) => service.metrics().active_workers.set(workers.len() as i64),
        Err(e) => tracing::error!(error = %e, "active worker count failed"),
    }

    match service.list_queues().await {
        Ok(queues) => {
            for q in queues {
                if let Ok(stats) = service.queue_stats(&q.name).await {
                    let depth = stats.queued + stats.scheduled + stats.retrying;
                    service
                        .metrics()
                        .queue_depth
                        .with_label_values(&[q.name.as_str()])
                        .set(depth);
                }
            }
        }
        Err(e) => tracing::error!(error = %e, "queue depth refresh failed"),
    }
}
