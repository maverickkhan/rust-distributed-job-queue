-- Initial schema for the distributed job queue.
-- Status is stored as TEXT with a CHECK constraint (rather than a native PG
-- enum) to keep the domain crate free of any sqlx coupling.

CREATE TABLE IF NOT EXISTS queues (
    name            TEXT PRIMARY KEY,
    paused          BOOLEAN NOT NULL DEFAULT FALSE,
    max_concurrency INTEGER,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS jobs (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue             TEXT NOT NULL REFERENCES queues(name) ON DELETE CASCADE,
    job_type          TEXT NOT NULL,
    payload           JSONB NOT NULL DEFAULT '{}'::jsonb,
    status            TEXT NOT NULL DEFAULT 'queued'
                          CHECK (status IN ('queued','scheduled','processing',
                                            'completed','failed','retrying',
                                            'cancelled','dead_lettered')),
    priority          INTEGER NOT NULL DEFAULT 0,
    idempotency_key   TEXT,
    max_attempts      INTEGER NOT NULL DEFAULT 5 CHECK (max_attempts >= 1),
    attempts          INTEGER NOT NULL DEFAULT 0,
    run_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    timeout_secs      INTEGER NOT NULL DEFAULT 300 CHECK (timeout_secs >= 1),
    backoff_base_secs INTEGER NOT NULL DEFAULT 2 CHECK (backoff_base_secs >= 1),
    backoff_max_secs  INTEGER NOT NULL DEFAULT 300 CHECK (backoff_max_secs >= 1),
    dead_letter       BOOLEAN NOT NULL DEFAULT TRUE,
    locked_by         UUID,
    locked_at         TIMESTAMPTZ,
    lease_expires_at  TIMESTAMPTZ,
    last_error        TEXT,
    result            JSONB,
    metadata          JSONB NOT NULL DEFAULT '{}'::jsonb,
    correlation_id    TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at      TIMESTAMPTZ
);

-- Enforce idempotency per (queue, key) when a key is supplied.
CREATE UNIQUE INDEX IF NOT EXISTS jobs_idem_uniq
    ON jobs (queue, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- Hot path: the leasing query orders by priority then run_at over leaseable rows.
CREATE INDEX IF NOT EXISTS jobs_lease_fetch_idx
    ON jobs (queue, priority DESC, run_at)
    WHERE status IN ('queued','scheduled','retrying');

-- Reaper path: find expired leases quickly.
CREATE INDEX IF NOT EXISTS jobs_expired_lease_idx
    ON jobs (lease_expires_at)
    WHERE status = 'processing';

-- General listing / stats.
CREATE INDEX IF NOT EXISTS jobs_status_queue_idx ON jobs (status, queue);
CREATE INDEX IF NOT EXISTS jobs_created_at_idx ON jobs (created_at);

CREATE TABLE IF NOT EXISTS job_attempts (
    id          BIGSERIAL PRIMARY KEY,
    job_id      UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt     INTEGER NOT NULL,
    worker_id   UUID,
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    status      TEXT NOT NULL DEFAULT 'running'
                    CHECK (status IN ('running','succeeded','failed','timeout')),
    error       TEXT
);
CREATE INDEX IF NOT EXISTS job_attempts_job_idx ON job_attempts (job_id, attempt DESC);

CREATE TABLE IF NOT EXISTS workers (
    id             UUID PRIMARY KEY,
    hostname       TEXT NOT NULL,
    queues         TEXT[] NOT NULL,
    concurrency    INTEGER NOT NULL,
    registered_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS workers_heartbeat_idx ON workers (last_heartbeat);

CREATE TABLE IF NOT EXISTS dead_letter_jobs (
    id                  UUID PRIMARY KEY,
    queue               TEXT NOT NULL,
    job_type            TEXT NOT NULL,
    payload             JSONB NOT NULL,
    attempts            INTEGER NOT NULL,
    last_error          TEXT,
    dead_lettered_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    original_created_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS dlq_queue_idx ON dead_letter_jobs (queue, dead_lettered_at DESC);
