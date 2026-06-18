//! The worker run loop: registration, heartbeats, bounded-concurrency leasing,
//! timed execution and graceful drain.

use crate::{HandlerRegistry, WorkerConfig};
use djq_core::{ExecutionOutcome, Job, Worker};
use djq_queue::QueueService;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::{interval, sleep, timeout, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Owns the dependencies needed to run a worker process.
pub struct WorkerRuntime {
    service: QueueService,
    registry: HandlerRegistry,
    config: WorkerConfig,
}

impl WorkerRuntime {
    pub fn new(service: QueueService, registry: HandlerRegistry, config: WorkerConfig) -> Self {
        Self {
            service,
            registry,
            config,
        }
    }

    /// Run until `shutdown` is cancelled, then drain in-flight jobs and
    /// deregister. Returns once shutdown is complete.
    pub async fn run(self, shutdown: CancellationToken) -> anyhow::Result<()> {
        let WorkerRuntime {
            service,
            registry,
            config,
        } = self;

        let now = chrono::Utc::now();
        let worker = Worker {
            id: config.id,
            hostname: config.hostname.clone(),
            queues: config.queues.clone(),
            concurrency: config.concurrency as i32,
            registered_at: now,
            last_heartbeat: now,
        };
        service.store().register_worker(&worker).await?;
        tracing::info!(
            worker_id = %config.id,
            queues = ?config.queues,
            concurrency = config.concurrency,
            handlers = ?registry.job_types(),
            "worker registered"
        );

        // Heartbeat task.
        let hb = tokio::spawn(heartbeat_loop(
            service.clone(),
            worker.clone(),
            config.heartbeat_interval,
            shutdown.clone(),
        ));

        let sem = Arc::new(Semaphore::new(config.concurrency));
        let mut tasks: JoinSet<()> = JoinSet::new();

        loop {
            // Acquire a concurrency slot (or stop on shutdown).
            let permit = tokio::select! {
                _ = shutdown.cancelled() => break,
                res = sem.clone().acquire_owned() => match res {
                    Ok(p) => p,
                    Err(_) => break, // semaphore closed
                },
            };

            match service
                .lease_next(config.id, &config.queues, config.lease_secs)
                .await
            {
                Ok(Some(job)) => {
                    let svc = service.clone();
                    let reg = registry.clone();
                    let worker_id = config.id;
                    tasks.spawn(async move {
                        execute_job(&svc, &reg, worker_id, job).await;
                        drop(permit); // release slot only after the job finishes
                    });
                }
                Ok(None) => {
                    drop(permit);
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        _ = sleep(config.poll_interval) => {}
                    }
                }
                Err(e) => {
                    drop(permit);
                    tracing::error!(error = %e, "lease attempt failed");
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        _ = sleep(config.poll_interval) => {}
                    }
                }
            }

            // Reap finished tasks without blocking.
            while tasks.try_join_next().is_some() {}
        }

        // Graceful drain of in-flight jobs.
        tracing::info!(inflight = tasks.len(), "draining in-flight jobs");
        let drain = timeout(config.shutdown_grace, async {
            while tasks.join_next().await.is_some() {}
        });
        if drain.await.is_err() {
            tracing::warn!("shutdown grace elapsed; abandoning in-flight jobs (will be recovered)");
            tasks.abort_all();
        }

        hb.abort();
        if let Err(e) = service.store().deregister_worker(config.id).await {
            tracing::warn!(error = %e, "failed to deregister worker");
        }
        tracing::info!(worker_id = %config.id, "worker stopped");
        Ok(())
    }
}

/// Execute a single leased job under its per-attempt timeout and report back.
async fn execute_job(
    service: &QueueService,
    registry: &HandlerRegistry,
    worker_id: Uuid,
    job: Job,
) {
    // Extend the lease so it comfortably covers the execution timeout.
    let cover = job.timeout_secs as i64 + 30;
    if let Err(e) = service.renew_lease(job.id, worker_id, cover).await {
        tracing::warn!(job_id = %job.id, error = %e, "could not renew lease; skipping (lease lost)");
        return;
    }

    let start = Instant::now();
    let outcome = match registry.get(&job.job_type) {
        None => ExecutionOutcome::Failure(format!("no handler for job_type '{}'", job.job_type)),
        Some(handler) => {
            let dur = Duration::from_secs(job.timeout_secs.max(1) as u64);
            match timeout(dur, handler.handle(&job)).await {
                Ok(Ok(result)) => ExecutionOutcome::Success(result),
                Ok(Err(msg)) => ExecutionOutcome::Failure(msg),
                Err(_) => ExecutionOutcome::Timeout,
            }
        }
    };
    let elapsed = start.elapsed().as_secs_f64();

    match service
        .report_outcome(&job, worker_id, outcome, elapsed)
        .await
    {
        Ok(updated) => tracing::info!(
            job_id = %job.id,
            job_type = %job.job_type,
            status = %updated.status,
            attempt = job.attempts,
            elapsed_s = elapsed,
            "job finished"
        ),
        Err(e) => tracing::error!(job_id = %job.id, error = %e, "failed to report outcome"),
    }
}

async fn heartbeat_loop(
    service: QueueService,
    worker: Worker,
    period: Duration,
    shutdown: CancellationToken,
) {
    let mut ticker = interval(period);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = ticker.tick() => {
                match service.store().heartbeat(worker.id).await {
                    Ok(true) => {}
                    Ok(false) => {
                        tracing::warn!(worker_id = %worker.id, "worker row missing; re-registering");
                        if let Err(e) = service.store().register_worker(&worker).await {
                            tracing::error!(error = %e, "re-registration failed");
                        }
                    }
                    Err(e) => tracing::error!(error = %e, "heartbeat failed"),
                }
            }
        }
    }
}
