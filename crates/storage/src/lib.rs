//! `djq-storage` — the PostgreSQL implementation of [`djq_core::Store`].
//!
//! All queries use the sqlx *runtime* API (not the compile-time `query!`
//! macros) so the workspace builds without a live database or `DATABASE_URL`.
//! Correctness is exercised by the `djq-integration-tests` crate against a real
//! Postgres instance.
//!
//! Concurrency model: job leasing uses `UPDATE ... WHERE id = (SELECT ... FOR
//! UPDATE SKIP LOCKED LIMIT 1)`, which lets many workers pull disjoint jobs
//! without blocking each other and without ever handing the same job to two
//! workers. Multi-statement transitions (lease, complete, fail) run inside a
//! transaction so the job row and its attempt row move together.

mod row;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use djq_core::{
    BackoffPolicy, DeadLetterJob, Job, JobAttempt, NormalizedJob, Queue, QueueError, QueueStats,
    Result, Store, Submission, Worker,
};
use row::{AttemptRow, DeadLetterRow, JobRow, QueueRow, WorkerRow, JOB_COLUMNS};
use serde_json::Value as Json;
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::time::Duration;
use uuid::Uuid;

/// Postgres-backed store. Cheap to clone — wraps an `Arc`'d connection pool.
#[derive(Clone)]
pub struct PgStore {
    pool: PgPool,
}

fn storage_err(e: sqlx::Error) -> QueueError {
    QueueError::Storage(e.to_string())
}

fn is_unique_violation(e: &sqlx::Error) -> bool {
    matches!(e, sqlx::Error::Database(db) if db.code().as_deref() == Some("23505"))
}

impl PgStore {
    /// Connect with a bounded pool and verify connectivity.
    pub async fn connect(database_url: &str, max_connections: u32) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections.max(1))
            .acquire_timeout(Duration::from_secs(10))
            .connect(database_url)
            .await
            .map_err(storage_err)?;
        Ok(Self { pool })
    }

    /// Build from an existing pool (used by tests).
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Borrow the underlying pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Run embedded migrations. Idempotent.
    pub async fn migrate(&self) -> Result<()> {
        sqlx::migrate!("./migrations")
            .run(&self.pool)
            .await
            .map_err(|e| QueueError::Storage(format!("migration failed: {e}")))
    }

    /// Lightweight readiness probe (`SELECT 1`).
    pub async fn ping(&self) -> Result<()> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(storage_err)
    }

    async fn fetch_job(&self, id: Uuid) -> Result<Option<Job>> {
        let sql = format!("SELECT {JOB_COLUMNS} FROM jobs WHERE id = $1");
        let row = sqlx::query_as::<_, JobRow>(&sql)
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_err)?;
        row.map(JobRow::into_job).transpose()
    }
}

#[async_trait]
impl Store for PgStore {
    // ---- queues -----------------------------------------------------------

    async fn ensure_queue(&self, name: &str) -> Result<Queue> {
        sqlx::query("INSERT INTO queues(name) VALUES ($1) ON CONFLICT (name) DO NOTHING")
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(storage_err)?;
        let row = sqlx::query_as::<_, QueueRow>(
            "SELECT name, paused, max_concurrency, created_at FROM queues WHERE name = $1",
        )
        .bind(name)
        .fetch_one(&self.pool)
        .await
        .map_err(storage_err)?;
        Ok(row.into())
    }

    async fn get_queue(&self, name: &str) -> Result<Option<Queue>> {
        let row = sqlx::query_as::<_, QueueRow>(
            "SELECT name, paused, max_concurrency, created_at FROM queues WHERE name = $1",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_err)?;
        Ok(row.map(Into::into))
    }

    async fn list_queues(&self) -> Result<Vec<Queue>> {
        let rows = sqlx::query_as::<_, QueueRow>(
            "SELECT name, paused, max_concurrency, created_at FROM queues ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(storage_err)?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn set_queue_paused(&self, name: &str, paused: bool) -> Result<Queue> {
        let row = sqlx::query_as::<_, QueueRow>(
            "UPDATE queues SET paused = $2 WHERE name = $1 \
             RETURNING name, paused, max_concurrency, created_at",
        )
        .bind(name)
        .bind(paused)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_err)?;
        row.map(Into::into)
            .ok_or_else(|| QueueError::QueueNotFound(name.to_string()))
    }

    async fn queue_stats(&self, name: &str) -> Result<QueueStats> {
        let queue = self
            .get_queue(name)
            .await?
            .ok_or_else(|| QueueError::QueueNotFound(name.to_string()))?;

        let rows = sqlx::query_as::<_, (String, i64)>(
            "SELECT status, COUNT(*) FROM jobs WHERE queue = $1 GROUP BY status",
        )
        .bind(name)
        .fetch_all(&self.pool)
        .await
        .map_err(storage_err)?;

        let mut stats = QueueStats {
            queue: name.to_string(),
            paused: queue.paused,
            ..Default::default()
        };
        for (status, count) in rows {
            match status.as_str() {
                "queued" => stats.queued = count,
                "scheduled" => stats.scheduled = count,
                "processing" => stats.processing = count,
                "completed" => stats.completed = count,
                "failed" => stats.failed = count,
                "retrying" => stats.retrying = count,
                "cancelled" => stats.cancelled = count,
                "dead_lettered" => stats.dead_lettered = count,
                _ => {}
            }
        }
        Ok(stats)
    }

    // ---- submission / inspection -----------------------------------------

    async fn submit(&self, job: NormalizedJob) -> Result<Submission> {
        self.ensure_queue(&job.queue).await?;

        // Fast path for idempotency: return the existing job if present.
        if let Some(key) = &job.idempotency_key {
            let sql =
                format!("SELECT {JOB_COLUMNS} FROM jobs WHERE queue = $1 AND idempotency_key = $2");
            if let Some(existing) = sqlx::query_as::<_, JobRow>(&sql)
                .bind(&job.queue)
                .bind(key)
                .fetch_optional(&self.pool)
                .await
                .map_err(storage_err)?
            {
                return Ok(Submission {
                    job: existing.into_job()?,
                    created: false,
                });
            }
        }

        let insert = format!(
            "INSERT INTO jobs \
             (queue, job_type, payload, status, priority, idempotency_key, max_attempts, \
              run_at, timeout_secs, backoff_base_secs, backoff_max_secs, dead_letter, \
              metadata, correlation_id) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) \
             RETURNING {JOB_COLUMNS}"
        );
        let result = sqlx::query_as::<_, JobRow>(&insert)
            .bind(&job.queue)
            .bind(&job.job_type)
            .bind(&job.payload)
            .bind(job.status.as_str())
            .bind(job.priority)
            .bind(&job.idempotency_key)
            .bind(job.max_attempts)
            .bind(job.run_at)
            .bind(job.timeout_secs)
            .bind(job.backoff_base_secs)
            .bind(job.backoff_max_secs)
            .bind(job.dead_letter)
            .bind(&job.metadata)
            .bind(&job.correlation_id)
            .fetch_one(&self.pool)
            .await;

        match result {
            Ok(row) => Ok(Submission {
                job: row.into_job()?,
                created: true,
            }),
            // Lost an idempotency race: fetch the row the other writer inserted.
            Err(e) if is_unique_violation(&e) => {
                let key = job
                    .idempotency_key
                    .as_ref()
                    .ok_or_else(|| QueueError::Storage(e.to_string()))?;
                let sql = format!(
                    "SELECT {JOB_COLUMNS} FROM jobs WHERE queue = $1 AND idempotency_key = $2"
                );
                let existing = sqlx::query_as::<_, JobRow>(&sql)
                    .bind(&job.queue)
                    .bind(key)
                    .fetch_one(&self.pool)
                    .await
                    .map_err(storage_err)?;
                Ok(Submission {
                    job: existing.into_job()?,
                    created: false,
                })
            }
            Err(e) => Err(storage_err(e)),
        }
    }

    async fn get_job(&self, id: Uuid) -> Result<Option<Job>> {
        self.fetch_job(id).await
    }

    async fn list_jobs(
        &self,
        queue: Option<&str>,
        status: Option<&str>,
        job_type: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<Job>> {
        // Dynamic but fully parameterized — no user value is concatenated.
        let sql = format!(
            "SELECT {JOB_COLUMNS} FROM jobs \
             WHERE ($1::text IS NULL OR queue = $1) \
               AND ($2::text IS NULL OR status = $2) \
               AND ($3::text IS NULL OR job_type = $3) \
             ORDER BY created_at DESC LIMIT $4 OFFSET $5"
        );
        let rows = sqlx::query_as::<_, JobRow>(&sql)
            .bind(queue)
            .bind(status)
            .bind(job_type)
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await
            .map_err(storage_err)?;
        rows.into_iter().map(JobRow::into_job).collect()
    }

    async fn list_attempts(&self, job_id: Uuid) -> Result<Vec<JobAttempt>> {
        let rows = sqlx::query_as::<_, AttemptRow>(
            "SELECT id, job_id, attempt, worker_id, started_at, finished_at, status, error \
             FROM job_attempts WHERE job_id = $1 ORDER BY attempt DESC, id DESC",
        )
        .bind(job_id)
        .fetch_all(&self.pool)
        .await
        .map_err(storage_err)?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn list_dead_letter(
        &self,
        queue: Option<&str>,
        limit: i64,
    ) -> Result<Vec<DeadLetterJob>> {
        let rows = sqlx::query_as::<_, DeadLetterRow>(
            "SELECT id, queue, job_type, payload, attempts, last_error, dead_lettered_at, \
                    original_created_at \
             FROM dead_letter_jobs \
             WHERE ($1::text IS NULL OR queue = $1) \
             ORDER BY dead_lettered_at DESC LIMIT $2",
        )
        .bind(queue)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(storage_err)?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    // ---- operator actions -------------------------------------------------

    async fn cancel_job(&self, id: Uuid) -> Result<Job> {
        // Only cancellable states are touched; the RETURNING tells us what we did.
        let sql = format!(
            "UPDATE jobs SET status = 'cancelled', locked_by = NULL, locked_at = NULL, \
                    lease_expires_at = NULL, updated_at = now() \
             WHERE id = $1 \
               AND status IN ('queued','scheduled','retrying','processing') \
             RETURNING {JOB_COLUMNS}"
        );
        let row = sqlx::query_as::<_, JobRow>(&sql)
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_err)?;
        match row {
            Some(r) => r.into_job(),
            None => {
                // Distinguish "not found" from "not cancellable".
                match self.fetch_job(id).await? {
                    Some(j) => Err(QueueError::InvalidOperation {
                        op: "cancel",
                        status: j.status,
                    }),
                    None => Err(QueueError::JobNotFound(id)),
                }
            }
        }
    }

    async fn retry_job(&self, id: Uuid) -> Result<Job> {
        let sql = format!(
            "UPDATE jobs SET status = 'queued', run_at = now(), attempts = 0, \
                    last_error = NULL, locked_by = NULL, locked_at = NULL, \
                    lease_expires_at = NULL, updated_at = now() \
             WHERE id = $1 AND status IN ('failed','dead_lettered') \
             RETURNING {JOB_COLUMNS}"
        );
        let row = sqlx::query_as::<_, JobRow>(&sql)
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_err)?;
        match row {
            Some(r) => {
                // Remove the matching DLQ entry if it existed.
                let _ = sqlx::query("DELETE FROM dead_letter_jobs WHERE id = $1")
                    .bind(id)
                    .execute(&self.pool)
                    .await
                    .map_err(storage_err)?;
                r.into_job()
            }
            None => match self.fetch_job(id).await? {
                Some(j) => Err(QueueError::InvalidOperation {
                    op: "retry",
                    status: j.status,
                }),
                None => Err(QueueError::JobNotFound(id)),
            },
        }
    }

    async fn purge_finished(&self, older_than: DateTime<Utc>) -> Result<u64> {
        let res = sqlx::query(
            "DELETE FROM jobs WHERE status IN ('completed','cancelled') AND updated_at < $1",
        )
        .bind(older_than)
        .execute(&self.pool)
        .await
        .map_err(storage_err)?;
        Ok(res.rows_affected())
    }

    // ---- leasing protocol -------------------------------------------------

    async fn lease_next(
        &self,
        worker_id: Uuid,
        queues: &[String],
        lease_secs: i64,
    ) -> Result<Option<Job>> {
        let mut tx = self.pool.begin().await.map_err(storage_err)?;

        let sql = format!(
            "UPDATE jobs SET \
                status = 'processing', \
                locked_by = $1, \
                locked_at = now(), \
                lease_expires_at = now() + ($3::bigint * interval '1 second'), \
                attempts = attempts + 1, \
                updated_at = now() \
             WHERE id = ( \
                SELECT j.id FROM jobs j \
                JOIN queues q ON q.name = j.queue \
                WHERE j.queue = ANY($2) \
                  AND q.paused = FALSE \
                  AND j.status IN ('queued','scheduled','retrying') \
                  AND j.run_at <= now() \
                ORDER BY j.priority DESC, j.run_at ASC, j.created_at ASC \
                FOR UPDATE OF j SKIP LOCKED \
                LIMIT 1 \
             ) \
             RETURNING {JOB_COLUMNS}"
        );
        let row = sqlx::query_as::<_, JobRow>(&sql)
            .bind(worker_id)
            .bind(queues.to_vec())
            .bind(lease_secs)
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage_err)?;

        let job = match row {
            Some(r) => r.into_job()?,
            None => {
                tx.rollback().await.map_err(storage_err)?;
                return Ok(None);
            }
        };

        sqlx::query(
            "INSERT INTO job_attempts (job_id, attempt, worker_id, started_at, status) \
             VALUES ($1, $2, $3, now(), 'running')",
        )
        .bind(job.id)
        .bind(job.attempts)
        .bind(worker_id)
        .execute(&mut *tx)
        .await
        .map_err(storage_err)?;

        tx.commit().await.map_err(storage_err)?;
        Ok(Some(job))
    }

    async fn complete_job(&self, id: Uuid, worker_id: Uuid, result: Option<Json>) -> Result<Job> {
        let mut tx = self.pool.begin().await.map_err(storage_err)?;

        let sql = format!(
            "UPDATE jobs SET status = 'completed', result = $3, completed_at = now(), \
                    locked_by = NULL, locked_at = NULL, lease_expires_at = NULL, \
                    last_error = NULL, updated_at = now() \
             WHERE id = $1 AND locked_by = $2 AND status = 'processing' \
             RETURNING {JOB_COLUMNS}"
        );
        let row = sqlx::query_as::<_, JobRow>(&sql)
            .bind(id)
            .bind(worker_id)
            .bind(&result)
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage_err)?;

        let job = match row {
            Some(r) => r.into_job()?,
            None => {
                tx.rollback().await.map_err(storage_err)?;
                return self.lease_lost_error(id, worker_id, "complete").await;
            }
        };

        sqlx::query(
            "UPDATE job_attempts SET status = 'succeeded', finished_at = now() \
             WHERE job_id = $1 AND status = 'running'",
        )
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(storage_err)?;

        tx.commit().await.map_err(storage_err)?;
        Ok(job)
    }

    async fn fail_job(
        &self,
        id: Uuid,
        worker_id: Uuid,
        error: &str,
        timed_out: bool,
    ) -> Result<Job> {
        let mut tx = self.pool.begin().await.map_err(storage_err)?;

        // Lock the row and read everything needed to decide the next state.
        let sql = format!("SELECT {JOB_COLUMNS} FROM jobs WHERE id = $1 FOR UPDATE");
        let current = sqlx::query_as::<_, JobRow>(&sql)
            .bind(id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage_err)?;
        let current = match current {
            Some(r) => r.into_job()?,
            None => {
                tx.rollback().await.map_err(storage_err)?;
                return Err(QueueError::JobNotFound(id));
            }
        };
        if current.status != djq_core::JobStatus::Processing || current.locked_by != Some(worker_id)
        {
            tx.rollback().await.map_err(storage_err)?;
            return Err(QueueError::InvalidOperation {
                op: "fail",
                status: current.status,
            });
        }

        let exhausted = current.attempts >= current.max_attempts;
        if exhausted {
            if current.dead_letter {
                sqlx::query(
                    "INSERT INTO dead_letter_jobs \
                        (id, queue, job_type, payload, attempts, last_error, original_created_at) \
                     VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (id) DO NOTHING",
                )
                .bind(current.id)
                .bind(&current.queue)
                .bind(&current.job_type)
                .bind(&current.payload)
                .bind(current.attempts)
                .bind(error)
                .bind(current.created_at)
                .execute(&mut *tx)
                .await
                .map_err(storage_err)?;

                sqlx::query(
                    "UPDATE jobs SET status = 'dead_lettered', last_error = $2, \
                            locked_by = NULL, locked_at = NULL, lease_expires_at = NULL, \
                            updated_at = now() WHERE id = $1",
                )
                .bind(id)
                .bind(error)
                .execute(&mut *tx)
                .await
                .map_err(storage_err)?;
            } else {
                sqlx::query(
                    "UPDATE jobs SET status = 'failed', last_error = $2, \
                            locked_by = NULL, locked_at = NULL, lease_expires_at = NULL, \
                            updated_at = now() WHERE id = $1",
                )
                .bind(id)
                .bind(error)
                .execute(&mut *tx)
                .await
                .map_err(storage_err)?;
            }
        } else {
            let policy = BackoffPolicy::exponential(
                current.backoff_base_secs.max(1) as u64,
                current.backoff_max_secs.max(1) as u64,
            );
            let delay = policy.delay_for_attempt(current.attempts as u32);
            let delay_secs = delay.as_secs_f64().ceil() as i64;
            sqlx::query(
                "UPDATE jobs SET status = 'retrying', last_error = $2, \
                        run_at = now() + ($3::bigint * interval '1 second'), \
                        locked_by = NULL, locked_at = NULL, lease_expires_at = NULL, \
                        updated_at = now() WHERE id = $1",
            )
            .bind(id)
            .bind(error)
            .bind(delay_secs)
            .execute(&mut *tx)
            .await
            .map_err(storage_err)?;
        }

        let attempt_status = if timed_out { "timeout" } else { "failed" };
        sqlx::query(
            "UPDATE job_attempts SET status = $2, finished_at = now(), error = $3 \
             WHERE job_id = $1 AND status = 'running'",
        )
        .bind(id)
        .bind(attempt_status)
        .bind(error)
        .execute(&mut *tx)
        .await
        .map_err(storage_err)?;

        tx.commit().await.map_err(storage_err)?;

        self.fetch_job(id).await?.ok_or(QueueError::JobNotFound(id))
    }

    async fn renew_lease(&self, id: Uuid, worker_id: Uuid, lease_secs: i64) -> Result<()> {
        let res = sqlx::query(
            "UPDATE jobs SET lease_expires_at = now() + ($3::bigint * interval '1 second'), \
                    updated_at = now() \
             WHERE id = $1 AND locked_by = $2 AND status = 'processing'",
        )
        .bind(id)
        .bind(worker_id)
        .bind(lease_secs)
        .execute(&self.pool)
        .await
        .map_err(storage_err)?;
        if res.rows_affected() == 0 {
            return Err(QueueError::InvalidOperation {
                op: "renew_lease",
                status: djq_core::JobStatus::Processing,
            });
        }
        Ok(())
    }

    async fn recover_expired_leases(&self, limit: i64) -> Result<Vec<Uuid>> {
        let mut recovered: Vec<Uuid> = Vec::new();

        // 1) Retryable expired leases → back to 'retrying' with computed backoff.
        let retry_ids: Vec<(Uuid,)> = sqlx::query_as(
            "UPDATE jobs SET status = 'retrying', \
                    last_error = COALESCE(last_error, 'lease expired'), \
                    run_at = now() + (LEAST(backoff_max_secs::float8, \
                        backoff_base_secs::float8 * power(2, GREATEST(attempts - 1, 0))) \
                        * interval '1 second'), \
                    locked_by = NULL, locked_at = NULL, lease_expires_at = NULL, \
                    updated_at = now() \
             WHERE id IN ( \
                SELECT id FROM jobs \
                WHERE status = 'processing' AND lease_expires_at < now() \
                  AND attempts < max_attempts \
                ORDER BY lease_expires_at LIMIT $1 \
                FOR UPDATE SKIP LOCKED) \
             RETURNING id",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(storage_err)?;
        recovered.extend(retry_ids.iter().map(|(id,)| *id));

        // 2) Exhausted + dead_letter → dead-letter table + status.
        let dlq_ids: Vec<(Uuid,)> = sqlx::query_as(
            "WITH expired AS ( \
                SELECT id, queue, job_type, payload, attempts, last_error, created_at \
                FROM jobs \
                WHERE status = 'processing' AND lease_expires_at < now() \
                  AND attempts >= max_attempts AND dead_letter = TRUE \
                ORDER BY lease_expires_at LIMIT $1 \
                FOR UPDATE SKIP LOCKED), \
             ins AS ( \
                INSERT INTO dead_letter_jobs \
                    (id, queue, job_type, payload, attempts, last_error, original_created_at) \
                SELECT id, queue, job_type, payload, attempts, \
                       COALESCE(last_error, 'lease expired'), created_at FROM expired \
                ON CONFLICT (id) DO NOTHING) \
             UPDATE jobs SET status = 'dead_lettered', \
                    last_error = COALESCE(last_error, 'lease expired'), \
                    locked_by = NULL, locked_at = NULL, lease_expires_at = NULL, \
                    updated_at = now() \
             WHERE id IN (SELECT id FROM expired) RETURNING id",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(storage_err)?;
        recovered.extend(dlq_ids.iter().map(|(id,)| *id));

        // 3) Exhausted + no dead-letter → terminal 'failed'.
        let failed_ids: Vec<(Uuid,)> = sqlx::query_as(
            "UPDATE jobs SET status = 'failed', \
                    last_error = COALESCE(last_error, 'lease expired'), \
                    locked_by = NULL, locked_at = NULL, lease_expires_at = NULL, \
                    updated_at = now() \
             WHERE id IN ( \
                SELECT id FROM jobs \
                WHERE status = 'processing' AND lease_expires_at < now() \
                  AND attempts >= max_attempts AND dead_letter = FALSE \
                ORDER BY lease_expires_at LIMIT $1 \
                FOR UPDATE SKIP LOCKED) \
             RETURNING id",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(storage_err)?;
        recovered.extend(failed_ids.iter().map(|(id,)| *id));

        // Close out any still-'running' attempt rows for recovered jobs.
        if !recovered.is_empty() {
            sqlx::query(
                "UPDATE job_attempts SET status = 'timeout', finished_at = now(), \
                        error = COALESCE(error, 'lease expired') \
                 WHERE status = 'running' AND job_id = ANY($1)",
            )
            .bind(&recovered)
            .execute(&self.pool)
            .await
            .map_err(storage_err)?;
        }

        Ok(recovered)
    }

    // ---- worker registry --------------------------------------------------

    async fn register_worker(&self, worker: &Worker) -> Result<()> {
        sqlx::query(
            "INSERT INTO workers (id, hostname, queues, concurrency, registered_at, last_heartbeat) \
             VALUES ($1, $2, $3, $4, now(), now()) \
             ON CONFLICT (id) DO UPDATE SET hostname = EXCLUDED.hostname, \
                queues = EXCLUDED.queues, concurrency = EXCLUDED.concurrency, \
                last_heartbeat = now()",
        )
        .bind(worker.id)
        .bind(&worker.hostname)
        .bind(&worker.queues)
        .bind(worker.concurrency)
        .execute(&self.pool)
        .await
        .map_err(storage_err)?;
        Ok(())
    }

    async fn heartbeat(&self, worker_id: Uuid) -> Result<bool> {
        let res = sqlx::query("UPDATE workers SET last_heartbeat = now() WHERE id = $1")
            .bind(worker_id)
            .execute(&self.pool)
            .await
            .map_err(storage_err)?;
        Ok(res.rows_affected() > 0)
    }

    async fn deregister_worker(&self, worker_id: Uuid) -> Result<()> {
        sqlx::query("DELETE FROM workers WHERE id = $1")
            .bind(worker_id)
            .execute(&self.pool)
            .await
            .map_err(storage_err)?;
        Ok(())
    }

    async fn list_active_workers(&self, within_secs: i64) -> Result<Vec<Worker>> {
        let rows = sqlx::query_as::<_, WorkerRow>(
            "SELECT id, hostname, queues, concurrency, registered_at, last_heartbeat \
             FROM workers WHERE last_heartbeat > now() - ($1::bigint * interval '1 second') \
             ORDER BY registered_at",
        )
        .bind(within_secs)
        .fetch_all(&self.pool)
        .await
        .map_err(storage_err)?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn prune_dead_workers(&self, older_secs: i64) -> Result<u64> {
        let res = sqlx::query(
            "DELETE FROM workers WHERE last_heartbeat < now() - ($1::bigint * interval '1 second')",
        )
        .bind(older_secs)
        .execute(&self.pool)
        .await
        .map_err(storage_err)?;
        Ok(res.rows_affected())
    }
}

impl PgStore {
    /// Build a precise error explaining why a lease-bound update matched no row.
    async fn lease_lost_error(&self, id: Uuid, _worker_id: Uuid, op: &'static str) -> Result<Job> {
        match self.fetch_job(id).await? {
            Some(j) => Err(QueueError::InvalidOperation {
                op,
                status: j.status,
            }),
            None => Err(QueueError::JobNotFound(id)),
        }
    }
}
