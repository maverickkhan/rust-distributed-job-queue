//! Concurrent leasing must hand every job to exactly one worker — no job is
//! ever processed twice, none is lost. This exercises the `FOR UPDATE SKIP
//! LOCKED` leasing path under contention.

use djq_core::Store;
use djq_integration_tests::{ctx, new_job, unique_queue};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

#[tokio::test]
async fn concurrent_workers_do_not_double_process() {
    let Some(tc) = ctx().await else {
        eprintln!("SKIP concurrent_workers_do_not_double_process: no database");
        return;
    };
    let q = unique_queue();
    const JOBS: usize = 60;
    const WORKERS: usize = 8;

    let mut ids = Vec::with_capacity(JOBS);
    for i in 0..JOBS {
        let sub = tc
            .service
            .submit(new_job(&q, "echo", serde_json::json!({"i": i})))
            .await
            .unwrap();
        ids.push(sub.job.id);
    }

    // Tracks how many times each job id was leased.
    let lease_counts: Arc<Mutex<HashMap<Uuid, usize>>> = Arc::new(Mutex::new(HashMap::new()));

    let mut handles = Vec::new();
    for _ in 0..WORKERS {
        let store = tc.store.clone();
        let counts = lease_counts.clone();
        let queue = q.clone();
        handles.push(tokio::spawn(async move {
            let worker = Uuid::new_v4();
            while let Some(job) = store
                .lease_next(worker, std::slice::from_ref(&queue), 30)
                .await
                .unwrap()
            {
                counts
                    .lock()
                    .await
                    .entry(job.id)
                    .and_modify(|c| *c += 1)
                    .or_insert(1);
                // Simulate quick work then complete.
                store.complete_job(job.id, worker, None).await.unwrap();
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let counts = lease_counts.lock().await;
    assert_eq!(counts.len(), JOBS, "every job should have been leased once");
    for (id, &c) in counts.iter() {
        assert_eq!(c, 1, "job {id} was leased {c} times (expected exactly 1)");
    }

    // All jobs ended up completed.
    let stats = tc.service.queue_stats(&q).await.unwrap();
    assert_eq!(stats.completed as usize, JOBS);
}
