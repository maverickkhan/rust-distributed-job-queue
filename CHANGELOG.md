# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.0] - 2026-06-18

### Added
- Cargo workspace: `djq-core`, `djq-storage`, `djq-queue`, `djq-telemetry`,
  `djq-worker`, `djq-api`, `djq-integration-tests`.
- Pure domain core: typed models, `JobStatus` transition machine, exponential
  backoff, `thiserror` error types, and the `Store` trait.
- PostgreSQL storage with `FOR UPDATE SKIP LOCKED` leasing, transactional state
  transitions, embedded migrations and tuned partial indexes.
- REST API (Axum): submission with idempotency keys, priorities,
  delay/scheduling; get/list/filter/cancel/retry; attempt history; queue
  create/pause/resume/stats; dead-letter listing; purge; SSE status stream;
  health/readiness; Prometheus metrics.
- Worker runtime: registration, heartbeats, bounded-concurrency leasing, timed
  execution, retries/backoff, dead-lettering, graceful drain; example handlers
  (`echo`, `sum`, `sleep`, `fail`, `flaky`).
- Background maintenance: expired-lease recovery, dead-worker pruning, gauge
  refresh.
- Observability: structured tracing, correlation ids, Prometheus collectors.
- Tooling: Dockerfile, Docker Compose, GitHub Actions CI (fmt, clippy, tests on
  a Postgres service container, release build, Docker build, `cargo audit`),
  Makefile, `deny.toml`, reproducible load-test script.
- 19 unit tests and 15 integration tests (all passing against Postgres 16).
