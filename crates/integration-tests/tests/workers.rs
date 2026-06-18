//! Worker registry, heartbeats and an end-to-end runtime run with graceful
//! shutdown.

use djq_core::{Store, Worker};
use djq_integration_tests::{ctx, new_job, unique_queue};
use djq_worker::{HandlerRegistry, WorkerConfig, WorkerRuntime};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[tokio::test]
async fn register_heartbeat_and_missing_worker() {
    let Some(tc) = ctx().await else {
        eprintln!("SKIP register_heartbeat_and_missing_worker: no database");
        return;
    };
    let q = unique_queue();
    let id = Uuid::new_v4();
    let now = chrono::Utc::now();
    let worker = Worker {
        id,
        hostname: "test-host".into(),
        queues: vec![q.clone()],
        concurrency: 4,
        registered_at: now,
        last_heartbeat: now,
    };
    tc.store.register_worker(&worker).await.unwrap();

    assert!(tc.store.heartbeat(id).await.unwrap(), "known worker beats");
    assert!(
        !tc.store.heartbeat(Uuid::new_v4()).await.unwrap(),
        "unknown worker returns false"
    );

    let active = tc.store.list_active_workers(60).await.unwrap();
    assert!(active.iter().any(|w| w.id == id));

    // A long TTL prunes nothing.
    assert_eq!(tc.store.prune_dead_workers(3600).await.unwrap(), 0);

    tc.store.deregister_worker(id).await.unwrap();
    assert!(!tc.store.heartbeat(id).await.unwrap());
}

#[tokio::test]
async fn runtime_processes_jobs_and_shuts_down() {
    let Some(tc) = ctx().await else {
        eprintln!("SKIP runtime_processes_jobs_and_shuts_down: no database");
        return;
    };
    let q = unique_queue();
    const N: usize = 20;
    for i in 0..N {
        tc.service
            .submit(new_job(&q, "echo", serde_json::json!({"i": i})))
            .await
            .unwrap();
    }

    let config = WorkerConfig {
        id: Uuid::new_v4(),
        hostname: "itest".into(),
        queues: vec![q.clone()],
        concurrency: 4,
        lease_secs: 30,
        poll_interval: Duration::from_millis(50),
        heartbeat_interval: Duration::from_secs(5),
        shutdown_grace: Duration::from_secs(10),
    };
    let registry = HandlerRegistry::new().with_examples();
    let runtime = WorkerRuntime::new(tc.service.clone(), registry, config);

    let token = CancellationToken::new();
    let handle = tokio::spawn(runtime.run(token.clone()));

    // Wait until all jobs are completed (or time out).
    let mut completed = 0;
    for _ in 0..200 {
        let stats = tc.service.queue_stats(&q).await.unwrap();
        completed = stats.completed;
        if completed as usize >= N {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(completed as usize, N, "all jobs should complete");

    token.cancel();
    let result = tokio::time::timeout(Duration::from_secs(15), handle).await;
    assert!(
        result.is_ok(),
        "runtime should shut down within grace period"
    );

    // Worker deregistered itself on shutdown.
    let active = tc.store.list_active_workers(60).await.unwrap();
    assert!(active.iter().all(|w| w.hostname != "itest"));
}

#[tokio::test]
async fn flaky_job_eventually_succeeds_via_retries() {
    let Some(tc) = ctx().await else {
        eprintln!("SKIP flaky_job_eventually_succeeds_via_retries: no database");
        return;
    };
    let q = unique_queue();
    let mut job = new_job(&q, "flaky", serde_json::json!({"fail_until": 3}));
    job.max_attempts = Some(5);
    job.backoff_base_secs = Some(1); // 1s, 2s backoff — keep the test short
    job.backoff_max_secs = Some(2);
    let sub = tc.service.submit(job).await.unwrap();

    let config = WorkerConfig {
        id: Uuid::new_v4(),
        hostname: "itest-flaky".into(),
        queues: vec![q.clone()],
        concurrency: 2,
        lease_secs: 30,
        poll_interval: Duration::from_millis(50),
        heartbeat_interval: Duration::from_secs(5),
        shutdown_grace: Duration::from_secs(10),
    };
    let registry = HandlerRegistry::new().with_examples();
    let runtime = WorkerRuntime::new(tc.service.clone(), registry, config);
    let token = CancellationToken::new();
    let handle = tokio::spawn(runtime.run(token.clone()));

    let mut final_status = None;
    for _ in 0..120 {
        let job = tc.service.get_job(sub.job.id).await.unwrap();
        if job.status == djq_core::JobStatus::Completed {
            final_status = Some(job);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    token.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(15), handle).await;

    let job = final_status.expect("flaky job should eventually complete");
    assert_eq!(job.status, djq_core::JobStatus::Completed);
    assert!(job.attempts >= 3, "should have retried at least twice");
}
