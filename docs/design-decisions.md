# Design decisions

Each entry: the decision, the alternatives, and why.

## 1. PostgreSQL as the queue backend (not Redis/NATS)

**Decision.** Use Postgres for both durable state *and* the work-distribution mechanism, via `FOR UPDATE SKIP LOCKED` leasing.

**Alternatives.** Redis Streams, NATS JetStream, RabbitMQ.

**Why.** A correct queue needs transactional state transitions (lease, complete, retry, dead-letter) that are atomic with the job's durable record. Doing this in Postgres gives durability, exactly-the-state-machine-we-want, rich filtering/stats, and idempotency-key uniqueness *for free*, with **one** dependency to run and test. `SKIP LOCKED` is a proven pattern (River, Oban, graphile-worker, Sidekiq-pg). Redis/NATS would add a moving part and push correctness (acks, visibility timeouts) into application code. Redis Streams as an optional high-throughput transport is on the roadmap; it is not required for the guarantees this project makes.

## 2. SQLx runtime API, not compile-time `query!` macros

**Decision.** Use `sqlx::query`/`query_as` (runtime) rather than `query!` (compile-time checked).

**Why.** The `query!` macros require a live database (or a cached `.sqlx` dir) **at compile time**. That complicates CI, Docker builds and first-clone experience. The runtime API builds anywhere with no `DATABASE_URL`, and correctness is instead guaranteed by the integration suite running against a real Postgres. Trade-off accepted: we lose compile-time SQL checking; we gain a workspace that always builds.

## 3. Status as `TEXT` + `CHECK`, not a native PG enum

**Decision.** `status TEXT CHECK (status IN (...))`.

**Why.** A native enum would force `djq-core::JobStatus` to derive `sqlx::Type`, coupling the pure domain crate to sqlx. Keeping the domain pure is worth the marginally weaker typing at the DB layer (the `CHECK` still enforces the domain).

## 4. Trait-based storage seam

**Decision.** `Store` trait in `core`; `PgStore` in `storage`; services depend on `Arc<dyn Store>`.

**Why.** Separates domain logic from persistence (the brief's explicit ask), enables unit testing of orchestration, and makes the backend swappable without touching the API/worker.

## 5. A `dead_letter` flag so both `failed` and `dead_lettered` are real states

**Decision.** Per-job boolean `dead_letter` (default `true`). On attempt exhaustion: `true` → `dead_lettered` (+ DLQ row), `false` → terminal `failed` (inspectable, manually retryable).

**Why.** The brief requires both statuses to exist and mean something. This gives operators a choice: auto-park in the DLQ, or keep failed jobs in place for manual handling.

## 6. SSE via polling, not `LISTEN/NOTIFY`

**Decision.** The SSE handler polls the job row every 500 ms and emits on status change, ending at a terminal state or a hard cap.

**Why.** Polling is backend-agnostic, simple, and correct, with bounded connection lifetime. `LISTEN/NOTIFY` push is a clear optimization but adds a dedicated listener connection and reconnect logic; it is on the roadmap. Honesty over premature complexity.

## 7. `thiserror` in libraries, `anyhow` only in binaries

**Decision.** Domain/library errors are typed `thiserror` enums with an `ErrorCategory`; binaries use `anyhow` for context.

**Why.** Library callers can match on variants and map them to HTTP codes; application entrypoints get ergonomic `?`-with-context. This is the idiomatic split.

## 8. Lease renewal covers the job timeout

**Decision.** After leasing, the worker renews the lease to `job.timeout_secs + 30`.

**Why.** The initial lease (`WORKER_LEASE_SECS`, default 30s) bounds how long a *crashed* worker can hold a job before recovery. But a legitimately long job would otherwise have its lease expire mid-run and be double-leased. Renewing to cover the timeout prevents that while keeping fast crash recovery for short jobs.
