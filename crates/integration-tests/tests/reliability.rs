//! Lease/complete, retry + backoff, dead-lettering, terminal-failed and
//! expired-lease recovery.

use djq_core::{JobStatus, Store};
use djq_integration_tests::{ctx, new_job, unique_queue};
use uuid::Uuid;

#[tokio::test]
async fn lease_then_complete_transitions() {
    let Some(tc) = ctx().await else {
        eprintln!("SKIP lease_then_complete_transitions: no database");
        return;
    };
    let q = unique_queue();
    let sub = tc
        .service
        .submit(new_job(&q, "echo", serde_json::json!({})))
        .await
        .unwrap();

    let worker = Uuid::new_v4();
    let leased = tc
        .store
        .lease_next(worker, std::slice::from_ref(&q), 30)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(leased.status, JobStatus::Processing);
    assert_eq!(leased.attempts, 1);
    assert_eq!(leased.locked_by, Some(worker));

    let done = tc
        .store
        .complete_job(leased.id, worker, Some(serde_json::json!({"ok": true})))
        .await
        .unwrap();
    assert_eq!(done.status, JobStatus::Completed);
    assert_eq!(done.result.unwrap()["ok"], true);

    // A second lease of the same queue finds nothing.
    assert!(tc
        .store
        .lease_next(worker, std::slice::from_ref(&q), 30)
        .await
        .unwrap()
        .is_none());

    // Attempt history records one succeeded attempt.
    let attempts = tc.service.list_attempts(sub.job.id).await.unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].status, "succeeded");
}

#[tokio::test]
async fn failure_schedules_retry_with_backoff() {
    let Some(tc) = ctx().await else {
        eprintln!("SKIP failure_schedules_retry_with_backoff: no database");
        return;
    };
    let q = unique_queue();
    let mut job = new_job(&q, "fail", serde_json::json!({}));
    job.max_attempts = Some(3);
    job.backoff_base_secs = Some(60); // large so it stays in the future
    let sub = tc.service.submit(job).await.unwrap();

    let worker = Uuid::new_v4();
    let leased = tc
        .store
        .lease_next(worker, std::slice::from_ref(&q), 30)
        .await
        .unwrap()
        .unwrap();

    let failed = tc
        .store
        .fail_job(leased.id, worker, "boom", false)
        .await
        .unwrap();
    assert_eq!(failed.status, JobStatus::Retrying);
    assert!(
        failed.run_at > sub.job.run_at,
        "run_at should be pushed out"
    );

    // Not leaseable until the backoff elapses.
    assert!(tc
        .store
        .lease_next(worker, std::slice::from_ref(&q), 30)
        .await
        .unwrap()
        .is_none());

    let attempts = tc.service.list_attempts(sub.job.id).await.unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].status, "failed");
}

#[tokio::test]
async fn exhausted_attempts_dead_letter() {
    let Some(tc) = ctx().await else {
        eprintln!("SKIP exhausted_attempts_dead_letter: no database");
        return;
    };
    let q = unique_queue();
    let mut job = new_job(&q, "fail", serde_json::json!({}));
    job.max_attempts = Some(1);
    let sub = tc.service.submit(job).await.unwrap();

    let worker = Uuid::new_v4();
    let leased = tc
        .store
        .lease_next(worker, std::slice::from_ref(&q), 30)
        .await
        .unwrap()
        .unwrap();
    let dead = tc
        .store
        .fail_job(leased.id, worker, "fatal", false)
        .await
        .unwrap();
    assert_eq!(dead.status, JobStatus::DeadLettered);

    let dlq = tc.service.list_dead_letter(Some(&q), 50).await.unwrap();
    assert_eq!(dlq.len(), 1);
    assert_eq!(dlq[0].id, sub.job.id);

    // Manual retry resurrects it from the DLQ.
    let retried = tc.service.retry_job(sub.job.id).await.unwrap();
    assert_eq!(retried.status, JobStatus::Queued);
    assert_eq!(retried.attempts, 0);
    assert!(tc
        .service
        .list_dead_letter(Some(&q), 50)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn exhausted_without_dlq_becomes_failed() {
    let Some(tc) = ctx().await else {
        eprintln!("SKIP exhausted_without_dlq_becomes_failed: no database");
        return;
    };
    let q = unique_queue();
    let mut job = new_job(&q, "fail", serde_json::json!({}));
    job.max_attempts = Some(1);
    job.dead_letter = Some(false);
    let sub = tc.service.submit(job).await.unwrap();

    let worker = Uuid::new_v4();
    let leased = tc
        .store
        .lease_next(worker, std::slice::from_ref(&q), 30)
        .await
        .unwrap()
        .unwrap();
    let failed = tc
        .store
        .fail_job(leased.id, worker, "fatal", false)
        .await
        .unwrap();
    assert_eq!(failed.status, JobStatus::Failed);
    assert!(tc
        .service
        .list_dead_letter(Some(&q), 50)
        .await
        .unwrap()
        .is_empty());

    // Failed jobs are manually retryable.
    let retried = tc.service.retry_job(sub.job.id).await.unwrap();
    assert_eq!(retried.status, JobStatus::Queued);
}

#[tokio::test]
async fn expired_lease_is_recovered() {
    let Some(tc) = ctx().await else {
        eprintln!("SKIP expired_lease_is_recovered: no database");
        return;
    };
    let q = unique_queue();
    let mut job = new_job(&q, "echo", serde_json::json!({}));
    job.max_attempts = Some(5);
    let sub = tc.service.submit(job).await.unwrap();

    let worker = Uuid::new_v4();
    // Lease with a zero-second lease so it is already expired on the next tick.
    let leased = tc
        .store
        .lease_next(worker, std::slice::from_ref(&q), 0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(leased.status, JobStatus::Processing);

    // Give the clock a moment to pass the (now) lease expiry.
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    let recovered = tc.store.recover_expired_leases(100).await.unwrap();
    assert!(recovered.contains(&sub.job.id));

    let after = tc.service.get_job(sub.job.id).await.unwrap();
    assert_eq!(after.status, JobStatus::Retrying);
    assert!(after.locked_by.is_none());
}
