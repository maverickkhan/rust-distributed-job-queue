//! Submission, idempotency, listing, cancellation, scheduling, priorities and
//! queue pause/resume.

use djq_core::{JobStatus, Store};
use djq_integration_tests::{ctx, new_job, unique_queue};
use uuid::Uuid;

#[tokio::test]
async fn submit_get_and_list() {
    let Some(tc) = ctx().await else {
        eprintln!("SKIP submit_get_and_list: no database");
        return;
    };
    let q = unique_queue();
    let sub = tc
        .service
        .submit(new_job(&q, "echo", serde_json::json!({"hello": "world"})))
        .await
        .unwrap();
    assert!(sub.created);
    assert_eq!(sub.job.status, JobStatus::Queued);

    let fetched = tc.service.get_job(sub.job.id).await.unwrap();
    assert_eq!(fetched.id, sub.job.id);
    assert_eq!(fetched.payload["hello"], "world");

    let filter = djq_core::JobFilter {
        queue: Some(q.clone()),
        ..Default::default()
    };
    let jobs = tc.service.list_jobs(&filter).await.unwrap();
    assert_eq!(jobs.len(), 1);
}

#[tokio::test]
async fn idempotency_key_returns_same_job() {
    let Some(tc) = ctx().await else {
        eprintln!("SKIP idempotency_key_returns_same_job: no database");
        return;
    };
    let q = unique_queue();
    let mut job = new_job(&q, "echo", serde_json::json!({"n": 1}));
    job.idempotency_key = Some("order-42".to_string());

    let first = tc.service.submit(job.clone()).await.unwrap();
    assert!(first.created, "first submission should create");

    let second = tc.service.submit(job).await.unwrap();
    assert!(!second.created, "second submission should be idempotent");
    assert_eq!(first.job.id, second.job.id);

    // Only one job should exist for the queue.
    let filter = djq_core::JobFilter {
        queue: Some(q),
        ..Default::default()
    };
    assert_eq!(tc.service.list_jobs(&filter).await.unwrap().len(), 1);
}

#[tokio::test]
async fn cancel_queued_job_then_reject_recancel() {
    let Some(tc) = ctx().await else {
        eprintln!("SKIP cancel_queued_job_then_reject_recancel: no database");
        return;
    };
    let q = unique_queue();
    let sub = tc
        .service
        .submit(new_job(&q, "echo", serde_json::json!({})))
        .await
        .unwrap();

    let cancelled = tc.service.cancel_job(sub.job.id).await.unwrap();
    assert_eq!(cancelled.status, JobStatus::Cancelled);

    // Cancelling a cancelled (terminal) job is an invalid operation.
    let err = tc.service.cancel_job(sub.job.id).await.unwrap_err();
    matches!(err, djq_core::QueueError::InvalidOperation { .. });
}

#[tokio::test]
async fn delayed_job_is_scheduled_and_not_leaseable() {
    let Some(tc) = ctx().await else {
        eprintln!("SKIP delayed_job_is_scheduled_and_not_leaseable: no database");
        return;
    };
    let q = unique_queue();
    let mut job = new_job(&q, "echo", serde_json::json!({}));
    job.delay_secs = Some(3600);
    let sub = tc.service.submit(job).await.unwrap();
    assert_eq!(sub.job.status, JobStatus::Scheduled);

    let worker = Uuid::new_v4();
    let leased = tc
        .store
        .lease_next(worker, std::slice::from_ref(&q), 30)
        .await
        .unwrap();
    assert!(leased.is_none(), "scheduled job must not be leaseable yet");
}

#[tokio::test]
async fn higher_priority_is_leased_first() {
    let Some(tc) = ctx().await else {
        eprintln!("SKIP higher_priority_is_leased_first: no database");
        return;
    };
    let q = unique_queue();
    let mut low = new_job(&q, "echo", serde_json::json!({"p": "low"}));
    low.priority = 0;
    let mut high = new_job(&q, "echo", serde_json::json!({"p": "high"}));
    high.priority = 100;

    let low_id = tc.service.submit(low).await.unwrap().job.id;
    let _high_id = tc.service.submit(high).await.unwrap().job.id;

    let worker = Uuid::new_v4();
    let leased = tc
        .store
        .lease_next(worker, std::slice::from_ref(&q), 30)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(leased.payload["p"], "high");
    assert_ne!(leased.id, low_id);
}

#[tokio::test]
async fn paused_queue_yields_no_work() {
    let Some(tc) = ctx().await else {
        eprintln!("SKIP paused_queue_yields_no_work: no database");
        return;
    };
    let q = unique_queue();
    tc.service
        .submit(new_job(&q, "echo", serde_json::json!({})))
        .await
        .unwrap();
    tc.service.set_queue_paused(&q, true).await.unwrap();

    let worker = Uuid::new_v4();
    assert!(tc
        .store
        .lease_next(worker, std::slice::from_ref(&q), 30)
        .await
        .unwrap()
        .is_none());

    tc.service.set_queue_paused(&q, false).await.unwrap();
    assert!(tc
        .store
        .lease_next(worker, std::slice::from_ref(&q), 30)
        .await
        .unwrap()
        .is_some());
}
