# Roadmap

Status of features and what's next. This list is honest about what exists today.

## Implemented

- [x] REST submission with idempotency keys, priorities, delays/scheduling, validation
- [x] Named queues with pause/resume
- [x] `FOR UPDATE SKIP LOCKED` leasing; worker registration, heartbeats, leases
- [x] At-least-once delivery, retries with exponential backoff, per-attempt timeouts
- [x] Dead-letter queue; terminal `failed` mode; manual retry
- [x] Expired-lease recovery (reaper) and dead-worker pruning
- [x] All 8 job statuses with an enforced transition machine
- [x] Job operations: get, filter/paginate, cancel, retry, attempt history, queue stats, purge
- [x] Server-Sent Events status stream
- [x] Prometheus metrics + structured logs + correlation ids + health/readiness
- [x] Graceful shutdown (API + worker drain)
- [x] Docker, Docker Compose, GitHub Actions CI, reproducible load test

## Near-term

- [ ] Postgres `LISTEN/NOTIFY` push for SSE (replace polling)
- [ ] OpenTelemetry trace export (submit → process spans)
- [ ] `cargo audit` / `cargo deny` wired as required CI gates (configs ship today)
- [ ] Batch submission endpoint

## Medium-term

- [ ] Optional Redis Streams transport for very high throughput
- [ ] Recurring / cron schedules
- [ ] Rate limiting per queue (token bucket)
- [ ] Web dashboard for queues, jobs and the DLQ

## Longer-term

- [ ] Job dependencies / workflows (DAGs)
- [ ] Multi-tenant isolation
- [ ] Horizontal sharding across databases
