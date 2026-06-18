# rust-distributed-job-queue

A distributed background-job processing platform built with **Rust, Tokio, Axum, PostgreSQL and SQLx**. It provides reliable, at-least-once job execution with priorities, delays, retries with exponential backoff, dead-lettering, distributed workers with lease-based concurrency control, live status streaming over SSE, and Prometheus metrics — in the spirit of BullMQ, Sidekiq and Celery, but implemented as a typed Rust workspace.

> **Status:** functional and tested. 19 unit tests + 15 integration tests pass against a real PostgreSQL instance (see [Testing](#testing)). Benchmarks ship as reproducible scripts — this README contains **no invented numbers** (see [Benchmarks](#benchmarks)).

---

## Table of contents

- [Why this exists](#why-this-exists)
- [Features](#features)
- [Architecture](#architecture)
- [Technology decisions](#technology-decisions)
- [Local setup](#local-setup)
- [Docker setup](#docker-setup)
- [Environment variables](#environment-variables)
- [API examples](#api-examples)
- [Testing](#testing)
- [Benchmarks](#benchmarks)
- [Observability](#observability)
- [Failure handling](#failure-handling)
- [Security considerations](#security-considerations)
- [Known limitations](#known-limitations)
- [Roadmap](#roadmap)

---

## Why this exists

**Business problem.** Most web applications need to run work *outside* the request/response cycle — sending email, generating reports, calling slow third-party APIs, processing uploads. Doing this inline makes requests slow and fragile. A job queue decouples "accept the work" from "do the work", absorbs spikes, and retries transient failures.

**Technical problem.** A *correct* distributed queue is deceptively hard. It must guarantee that a job is never silently lost and never run by two workers at once, survive worker crashes, apply backoff so a failing dependency isn't hammered, and expose enough observability to operate. This project implements those guarantees on top of PostgreSQL using transactional state transitions and `FOR UPDATE SKIP LOCKED` leasing.

## Features

**Submission & queues**
- REST submission with JSON payloads, multiple job types, unique UUID ids, **idempotency keys**, metadata and correlation ids, with input validation.
- Multiple named queues, integer **priorities**, **delayed** (`delay_secs`) and **scheduled** (`run_at`) execution, and queue **pause/resume**.

**Workers**
- Distributed worker processes with registration, **heartbeats**, **job leasing**, **lease expiry**, **abandoned-job recovery**, configurable concurrency and **graceful shutdown** (drains in-flight work, then deregisters).

**Reliability**
- **At-least-once** delivery, configurable **retries** with **exponential backoff**, per-attempt **timeouts**, a **dead-letter queue**, idempotency-key duplicate protection, and transaction-safe state transitions validated by an explicit state machine.

**Job statuses** — `queued`, `scheduled`, `processing`, `completed`, `failed`, `retrying`, `cancelled`, `dead_lettered`.

**Operations** — submit, get, filter + paginate, cancel, retry, attempt history, queue statistics, dead-letter inspection, and purge of finished-job history.

**Real-time** — per-job **Server-Sent Events** stream of status transitions.

**Observability** — queue-depth gauge, processing-duration histogram, completion/failure/retry/dead-letter counters, active-worker gauge, correlation ids and structured logs.

## Architecture

A Cargo workspace with a clean dependency direction: pure domain at the bottom, I/O at the edges. The `core` crate defines a `Store` **trait**; the `storage` crate is the only place that knows about Postgres.

```mermaid
flowchart TD
    client[Client] -->|REST + SSE| api[djq-api · Axum]
    api --> queue[djq-queue · QueueService]
    worker[djq-worker · runtime] --> queue
    queue --> coreTrait{{djq-core::Store trait}}
    coreTrait --> storage[djq-storage · PgStore]
    storage --> pg[(PostgreSQL)]
    api -.->|/metrics| prom[Prometheus]
    worker -.->|/metrics| prom

    subgraph workspace [Cargo workspace]
        api
        worker
        queue
        core[djq-core · models · status machine · backoff · errors]
        storage
        telemetry[djq-telemetry · tracing + metrics]
    end
    queue --> core
    storage --> core
    api --> telemetry
    worker --> telemetry
```

**Crates**

| Crate | Responsibility |
|-------|----------------|
| `djq-core` | Pure domain: typed models, the `JobStatus` state machine, exponential-backoff math, `thiserror` error types, and the `Store` trait. No I/O. |
| `djq-storage` | `PgStore` — the PostgreSQL implementation of `Store` (SQLx runtime API, `FOR UPDATE SKIP LOCKED` leasing, embedded migrations). |
| `djq-queue` | `QueueService` orchestration over `Store`, metric recording, and the background maintenance loop (lease recovery, worker pruning, gauge refresh). |
| `djq-telemetry` | Tracing subscriber + Prometheus collectors. |
| `djq-worker` | Worker runtime, `JobHandler` trait, handler registry, example handlers, and the `djq-worker` binary. |
| `djq-api` | Axum router, handlers, SSE, error mapping, and the `djq-api` binary. |
| `djq-integration-tests` | End-to-end tests against a real Postgres. |

**Database tables:** `queues`, `jobs`, `job_attempts`, `workers`, `dead_letter_jobs` — with partial unique indexes for idempotency and partial indexes tuned for the leasing hot path. See [`crates/storage/migrations`](crates/storage/migrations) and [ARCHITECTURE.md](ARCHITECTURE.md).

### The leasing query (heart of the system)

```sql
UPDATE jobs SET status='processing', locked_by=$1,
  lease_expires_at = now() + ($3 * interval '1 second'), attempts = attempts + 1
WHERE id = (
  SELECT j.id FROM jobs j JOIN queues q ON q.name = j.queue
  WHERE j.queue = ANY($2) AND q.paused = FALSE
    AND j.status IN ('queued','scheduled','retrying') AND j.run_at <= now()
  ORDER BY j.priority DESC, j.run_at ASC
  FOR UPDATE OF j SKIP LOCKED LIMIT 1)
RETURNING ...;
```

`SKIP LOCKED` lets many workers pull *disjoint* jobs concurrently without blocking each other and without ever handing the same job to two workers — proven by the `concurrent_workers_do_not_double_process` test.

## Technology decisions

- **PostgreSQL as the queue backend** (not Redis/NATS). Transactional state transitions, `SKIP LOCKED` leasing, durability and rich querying come for free, and a single dependency keeps the system easy to run and test. Redis Streams / NATS JetStream as an alternative transport is a documented roadmap item. Rationale in [docs/design-decisions.md](docs/design-decisions.md).
- **SQLx runtime API** (not the compile-time `query!` macros) so the workspace builds in CI and Docker **without** a live database or `DATABASE_URL`.
- **Axum 0.8 + Tower** for ergonomic, middleware-friendly HTTP.
- **`thiserror` in libraries, `anyhow` only at binary boundaries** — typed, matchable domain errors; convenient context at the edges.
- **Trait-based storage seam** (`Store`) so domain logic is testable and the backend is swappable.

## Local setup

Prerequisites: Rust stable (`rustup`), Docker (for Postgres).

```bash
# 1. Start a Postgres
make db                       # docker run postgres:16 on :5432
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/djq

# 2. Run the API (auto-runs migrations on startup)
make run-api                  # listens on :8080

# 3. In another shell, run a worker
WORKER_QUEUES=default,emails make run-worker

# 4. Quality gates
make fmt-check lint test
```

## Docker setup

```bash
docker compose up --build
```

Brings up Postgres, the API (`:8080`), two worker replicas, and Prometheus (`:9090`). The API runs migrations on boot; workers connect and start leasing.

## Environment variables

See [.env.example](.env.example). Highlights:

| Variable | Default | Used by | Purpose |
|----------|---------|---------|---------|
| `DATABASE_URL` | — (required) | api, worker | Postgres connection string |
| `DB_MAX_CONNECTIONS` | `10` | api, worker | Pool size |
| `API_BIND_ADDR` | `0.0.0.0:8080` | api | HTTP listen address |
| `API_MAX_BODY_BYTES` | `1048576` | api | Request body limit |
| `WORKER_QUEUES` | `default` | worker | Comma-separated queues to serve |
| `WORKER_CONCURRENCY` | `8` | worker | Max concurrent jobs |
| `WORKER_LEASE_SECS` | `30` | worker | Lease duration (renewed to cover job timeout) |
| `WORKER_METRICS_ADDR` | `0.0.0.0:9091` | worker | Worker metrics/health endpoint |
| `RUST_LOG` | `info,sqlx=warn` | both | Tracing filter |
| `LOG_JSON` | `false` | both | JSON structured logs |

## API examples

```bash
# Create a queue
curl -X POST localhost:8080/api/v1/queues -H 'content-type: application/json' \
  -d '{"name":"emails"}'

# Submit a job (idempotency key in body or Idempotency-Key header)
curl -X POST localhost:8080/api/v1/jobs -H 'content-type: application/json' \
  -d '{"queue":"emails","job_type":"echo","payload":{"to":"a@b.c"},
       "idempotency_key":"welcome-42","priority":10,"max_attempts":5}'

# A delayed job (runs in 60s) → status "scheduled"
curl -X POST localhost:8080/api/v1/jobs -H 'content-type: application/json' \
  -d '{"queue":"emails","job_type":"echo","payload":{},"delay_secs":60}'

# Inspect / operate
curl localhost:8080/api/v1/jobs/<id>
curl 'localhost:8080/api/v1/jobs?queue=emails&status=completed&limit=20'
curl localhost:8080/api/v1/jobs/<id>/attempts
curl -X POST localhost:8080/api/v1/jobs/<id>/cancel
curl -X POST localhost:8080/api/v1/jobs/<id>/retry
curl localhost:8080/api/v1/queues/emails/stats
curl 'localhost:8080/api/v1/dlq?queue=emails'

# Live status stream (Server-Sent Events)
curl -N localhost:8080/api/v1/jobs/<id>/events

# Health / readiness / metrics
curl localhost:8080/healthz
curl localhost:8080/readyz
curl localhost:8080/metrics
```

Built-in example job types served by the worker: `echo`, `sum` (`{"numbers":[...]}`), `sleep` (`{"ms":N}`), `fail`, `flaky` (`{"fail_until":N}`).

## Testing

```bash
make db
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/djq
cargo test --workspace --all-features
```

Unit tests (no DB) cover the status machine and backoff arithmetic. Integration tests (real Postgres) cover: submission, idempotency, state transitions, lease → complete, retry + backoff, dead-lettering, terminal-failed, **expired-lease recovery**, **concurrent workers (no double-processing)**, worker registration/heartbeat, the **full worker runtime with graceful shutdown**, and flaky-job-eventually-succeeds.

Without a database, integration tests print a skip notice and pass, so `cargo test` stays green locally; CI provides a Postgres service container so they run for real.

## Benchmarks

This project ships a reproducible load generator at [`scripts/load_test.sh`](scripts/load_test.sh):

```bash
API=http://localhost:8080 COUNT=2000 CONCURRENCY=32 ./scripts/load_test.sh
```

It reports submission throughput and end-to-end drain throughput **for your machine**. For latency percentiles (P50/P95/P99) use a dedicated HTTP load tool, e.g.:

```bash
# oha or wrk against the submit endpoint
oha -n 5000 -c 50 -m POST -H 'content-type: application/json' \
  -d '{"queue":"default","job_type":"echo","payload":{}}' \
  http://localhost:8080/api/v1/jobs
```

To compare **one worker vs. many**, run the load test with `WORKER_CONCURRENCY=1` and a single replica, then with several replicas, and compare the drain throughput the script prints.

> **No benchmark numbers are published here.** Hardware, Postgres tuning and handler cost dominate results; publishing canned figures would be misleading. The methodology above is fully reproducible.

## Observability

- **Metrics:** `GET /metrics` (API on `:8080`, each worker on `:9091`). Series include `djq_jobs_submitted_total`, `djq_jobs_completed_total`, `djq_jobs_failed_total`, `djq_jobs_retried_total`, `djq_jobs_dead_lettered_total`, `djq_queue_depth`, `djq_active_workers`, and the `djq_processing_duration_seconds` histogram.
- **Tracing:** structured logs via `tracing`; set `LOG_JSON=true` for JSON. Every HTTP request gets a correlation id (honoured from `X-Correlation-ID` or generated) echoed back on the response.
- **Health:** `GET /healthz` (liveness), `GET /readyz` (checks the database).

## Failure handling

- **Worker crash:** the lease expires; the maintenance reaper returns the job to `retrying` (with backoff) or dead-letters it if attempts are exhausted.
- **Handler failure:** retried with exponential backoff until `max_attempts`, then dead-lettered (or marked `failed` if `dead_letter=false`).
- **Handler timeout:** the per-attempt timeout cancels the future; recorded as a `timeout` attempt and retried.
- **Duplicate submit:** an idempotency key returns the existing job instead of creating a second one.
- **Graceful shutdown:** on SIGINT/SIGTERM the worker stops leasing, drains in-flight jobs within a grace window, then deregisters; the API drains in-flight HTTP connections.

## Security considerations

This is a backend service intended to run inside a trusted network behind an authenticated gateway. It deliberately does **not** ship auth (that is the job of [rust-api-gateway](https://github.com/maverickkhan/rust-api-gateway) in this portfolio). The API has a request body-size limit. Payloads are stored as JSONB — do not place secrets in payloads. No credentials are committed; configuration is environment-based. See [SECURITY.md](SECURITY.md).

## Known limitations

- Real-time updates use **polling** inside the SSE handler (500 ms). Postgres `LISTEN/NOTIFY` push is a roadmap optimization.
- No built-in authentication/authorization (by design — front it with a gateway).
- Single-database; no sharding. Throughput scales with Postgres and worker count, not horizontally across databases.
- Cron-style recurring schedules are not implemented (one-shot delay/`run_at` only).

## Roadmap

See [ROADMAP.md](ROADMAP.md). Highlights: `LISTEN/NOTIFY` push, optional Redis Streams transport, recurring schedules, OpenTelemetry traces, and a web dashboard.

## License

MIT — see [LICENSE](LICENSE).
