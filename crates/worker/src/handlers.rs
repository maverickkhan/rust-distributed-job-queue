//! Built-in example handlers. They double as the fixtures used by the
//! integration tests to exercise success, failure, retry and timeout paths.

use crate::{HandlerResult, JobHandler};
use async_trait::async_trait;
use djq_core::Job;
use serde_json::json;
use std::time::Duration;

/// Returns the job payload unchanged as the result.
pub struct EchoHandler;

#[async_trait]
impl JobHandler for EchoHandler {
    async fn handle(&self, job: &Job) -> HandlerResult {
        Ok(Some(job.payload.clone()))
    }
}

/// Sums `payload.numbers` (an array of numbers) and returns `{ "sum": n }`.
pub struct SumHandler;

#[async_trait]
impl JobHandler for SumHandler {
    async fn handle(&self, job: &Job) -> HandlerResult {
        let numbers = job
            .payload
            .get("numbers")
            .and_then(|v| v.as_array())
            .ok_or_else(|| "payload.numbers must be an array".to_string())?;
        let mut sum = 0f64;
        for n in numbers {
            sum += n
                .as_f64()
                .ok_or_else(|| format!("non-numeric element: {n}"))?;
        }
        Ok(Some(json!({ "sum": sum })))
    }
}

/// Sleeps for `payload.ms` milliseconds (default 100). Used to test timeouts
/// and concurrency. Async sleep keeps the worker thread free.
pub struct SleepHandler;

#[async_trait]
impl JobHandler for SleepHandler {
    async fn handle(&self, job: &Job) -> HandlerResult {
        let ms = job
            .payload
            .get("ms")
            .and_then(|v| v.as_u64())
            .unwrap_or(100);
        tokio::time::sleep(Duration::from_millis(ms)).await;
        Ok(Some(json!({ "slept_ms": ms })))
    }
}

/// Always fails with `payload.message` (default "intentional failure"). Used to
/// drive retry/backoff and dead-letter tests.
pub struct FailHandler;

#[async_trait]
impl JobHandler for FailHandler {
    async fn handle(&self, job: &Job) -> HandlerResult {
        let msg = job
            .payload
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("intentional failure");
        Err(msg.to_string())
    }
}

/// Fails until the job's attempt count reaches `payload.fail_until`, then
/// succeeds. Because `job.attempts` is incremented at lease time, the first
/// execution sees `attempts == 1`. Used to prove retries eventually succeed.
pub struct FlakyHandler;

#[async_trait]
impl JobHandler for FlakyHandler {
    async fn handle(&self, job: &Job) -> HandlerResult {
        let fail_until = job
            .payload
            .get("fail_until")
            .and_then(|v| v.as_i64())
            .unwrap_or(2);
        if (job.attempts as i64) < fail_until {
            Err(format!(
                "flaky failure on attempt {} (< {fail_until})",
                job.attempts
            ))
        } else {
            Ok(Some(json!({ "succeeded_on_attempt": job.attempts })))
        }
    }
}
