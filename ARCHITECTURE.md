# Architecture

## Goals and non-goals

**Goals:** at-least-once delivery, no double-processing, crash recovery, retries with backoff, dead-lettering, observability, and a clean, testable separation between domain logic and infrastructure.

**Non-goals:** exactly-once delivery (impossible in general; we provide at-least-once + idempotency keys), authentication (front with a gateway), multi-database sharding.

## Layering and dependency direction

```
djq-core  ◄── djq-storage ◄── djq-queue ◄── djq-api
   ▲             ▲               ▲    ▲
   └─────────────┴───────────────┘    └──── djq-worker
        (everything depends on the pure core)
```

`djq-core` is pure (no Tokio runtime, no DB). It owns the domain: models, the `JobStatus` state machine, backoff math, error types, and the `Store` trait. Every higher layer depends inward. The only crate that knows SQL exists is `djq-storage`.

This direction is what makes the system testable: the bulk of the logic (validation, status transitions, backoff) is unit-tested without any I/O, and the database-coupled behaviour is exercised by a focused integration suite.

## The `Store` seam

```rust
#[async_trait]
pub trait Store: Send + Sync + 'static {
    async fn submit(&self, job: NormalizedJob) -> Result<Submission>;
    async fn lease_next(&self, worker: Uuid, queues: &[String], lease_secs: i64) -> Result<Option<Job>>;
    async fn complete_job(&self, id: Uuid, worker: Uuid, result: Option<Json>) -> Result<Job>;
    async fn fail_job(&self, id: Uuid, worker: Uuid, error: &str, timed_out: bool) -> Result<Job>;
    async fn recover_expired_leases(&self, limit: i64) -> Result<Vec<Uuid>>;
    // ... queues, workers, dead-letter, inspection
}
```

`QueueService` holds an `Arc<dyn Store>`. Swapping the backend means writing one impl; the API and worker are unaffected.

## Data model

| Table | Purpose | Notable indexes/constraints |
|-------|---------|-----------------------------|
| `queues` | Named queues; `paused` flag | PK on `name` |
| `jobs` | The job and its full lifecycle state | partial **unique** `(queue, idempotency_key)`; partial index on leaseable rows `(queue, priority DESC, run_at)`; partial index on `lease_expires_at` for the reaper; `CHECK` constraint enumerating valid statuses |
| `job_attempts` | One row per execution attempt | index `(job_id, attempt DESC)` |
| `workers` | Registry + heartbeat | index on `last_heartbeat` |
| `dead_letter_jobs` | Exhausted jobs for inspection | index `(queue, dead_lettered_at DESC)` |

Status is stored as `TEXT` with a `CHECK` constraint rather than a native PG enum, deliberately, so `djq-core` carries **no** sqlx dependency and stays pure.

## Job lifecycle

```
              submit
                │
        ┌───────┴────────┐
     run_at>now        run_at<=now
        │                 │
   [scheduled] ─────► [queued] ──lease──► [processing] ──ok──► [completed]
                                              │
                          ┌───────────────────┼───────────────────┐
                       fail (n<max)        timeout            lease expired
                          │                   │                   │
                      [retrying] ◄────────────┘            (reaper) [retrying]
                          │  (run_at = now + backoff)
                          └─lease─► [processing] ...
                          │
                   fail (n>=max)
                          │
              dead_letter? ──yes──► [dead_lettered] (+ dead_letter_jobs row)
                          └──no───► [failed]

  any non-terminal ──cancel──► [cancelled]
```

Legal transitions are encoded in `JobStatus::can_transition_to` and unit-tested. Storage methods only perform transitions the machine permits.

## Concurrency model

- **Leasing:** `UPDATE ... WHERE id = (SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1)`. The row lock is held for the duration of the single `UPDATE` statement; `SKIP LOCKED` means contending workers skip locked rows and grab the next available one. No two workers can lease the same job. Verified under contention by `concurrent_workers_do_not_double_process`.
- **Multi-statement transitions** (lease+attempt-insert, complete+attempt-close, the branchy fail logic) run inside a transaction so the job row and its attempt row move atomically.
- **Worker concurrency** is bounded by a Tokio `Semaphore`; each in-flight job holds one permit until it finishes, so a worker never exceeds `WORKER_CONCURRENCY`.
- **Bounded growth:** the reaper recovers at most `recover_batch` jobs per sweep to keep transactions short; pagination is clamped (max 200/page).
- **Timeouts:** every handler runs under `tokio::time::timeout`. Leases are renewed to cover the job timeout, and the reaper is the backstop if a worker dies mid-job.

## Background maintenance

A single Tokio task (`djq-queue::maintenance`) sweeps every few seconds to:
1. recover expired leases (→ `retrying`, or dead-letter/`failed` when exhausted),
2. prune workers whose heartbeat has gone stale,
3. refresh the `djq_active_workers` and `djq_queue_depth` gauges.

It logs errors but never dies on a transient DB blip, and cancels cleanly on shutdown via a `CancellationToken`.

## Graceful shutdown

On SIGINT/SIGTERM:
- **API:** `axum::serve(...).with_graceful_shutdown(...)` stops accepting new connections and drains in-flight requests; the maintenance task is cancelled.
- **Worker:** stops leasing, waits up to `WORKER_SHUTDOWN_GRACE_SECS` for in-flight jobs to finish (via `JoinSet`), aborts anything still running (it will be recovered by the reaper), then deregisters.

## Why at-least-once (not exactly-once)

If a worker completes a job and then crashes before the `complete_job` commit lands, the lease expires and the job is retried — so a handler may run more than once. This is the standard, honest guarantee for durable queues. Mitigations provided: idempotency keys at submit time, and the recommendation that handlers be idempotent. Exactly-once would require coordinating the handler's side effects with the queue transaction, which the queue cannot do in general.
