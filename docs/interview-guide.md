# Interview guide

Talking points and likely questions for this project.

## 30-second pitch

"A distributed background-job platform in Rust. Workers lease jobs from Postgres using `FOR UPDATE SKIP LOCKED`, so many workers pull disjoint work concurrently with no double-processing. It does at-least-once delivery with idempotency keys, exponential-backoff retries, dead-lettering, lease-based crash recovery, live SSE status, and Prometheus metrics. It's a Cargo workspace with a pure domain core behind a `Store` trait, fully tested against a real database."

## Concepts demonstrated

- **Rust:** workspace design, trait objects (`Arc<dyn Store>`), `async_trait`, typed errors with `thiserror`, ownership across Tokio tasks, `Semaphore`-bounded concurrency, `JoinSet`, `CancellationToken` graceful shutdown, `tokio::time::timeout`.
- **Distributed systems:** at-least-once vs exactly-once, lease/visibility-timeout, `SKIP LOCKED` work distribution, idempotency, dead-letter queues, heartbeats and failure detection, backoff.
- **Databases:** transactional state machines, partial indexes, optimistic vs pessimistic locking, idempotency via partial unique index.

## Five likely questions (with answers)

1. **How do you prevent two workers running the same job?**
   The lease is a single `UPDATE ... WHERE id = (SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1)`. The selected row is locked for that statement; other workers skip it. Exactly one worker wins. The `concurrent_workers_do_not_double_process` test asserts each of 60 jobs is leased exactly once under 8 racing workers.

2. **What happens when a worker crashes mid-job?**
   The job stays `processing` with a `lease_expires_at`. The maintenance reaper finds expired leases and returns them to `retrying` (with backoff) or dead-letters them if attempts are exhausted. So work is never lost — at the cost of possible re-execution (at-least-once).

3. **Exactly-once?**
   No — and I'd push back on anyone who claims it for a durable queue. If a handler's side effect commits but the queue's `complete` doesn't, the job re-runs. I provide idempotency keys at submit and recommend idempotent handlers.

4. **Why Postgres instead of Redis/Kafka?**
   Correctness with one dependency. Transactional transitions, durability, idempotency uniqueness, and rich queries are built in. `SKIP LOCKED` is a battle-tested queue primitive. Redis Streams is a roadmap transport for higher throughput, not a correctness requirement.

5. **How does backoff work and why is it a pure function?**
   `delay = min(base * 2^(attempt-1), max)`, optional jitter. It's a pure function in `core` with no clock or I/O, so it's exhaustively unit-tested (growth, ceiling, overflow guard, jitter bounds). The storage layer computes `run_at = now + delay` inside the failing transaction.

## Things I'd improve with more time

`LISTEN/NOTIFY` push for SSE, an optional Redis transport, recurring cron schedules, OpenTelemetry spans across submit→process, and property-based tests on the state machine.
