//! Pure, deterministic retry-backoff arithmetic.
//!
//! Kept free of I/O and time sources so it can be unit-tested exhaustively.
//! The worker and the lease reaper both call [`BackoffPolicy::delay_for_attempt`]
//! to compute when a failed job becomes eligible again.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Exponential backoff with an upper bound and optional deterministic jitter.
///
/// The delay for a given (1-based) attempt is:
///
/// ```text
/// delay = min(base * factor^(attempt - 1), max)   ± jitter
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct BackoffPolicy {
    /// Delay applied after the first failed attempt.
    pub base: Duration,
    /// Multiplier applied per subsequent attempt.
    pub factor: f64,
    /// Hard ceiling on any single delay.
    pub max: Duration,
    /// Fractional jitter in `[0.0, 1.0]`; `0.2` means ±20%.
    pub jitter: f64,
}

impl Default for BackoffPolicy {
    fn default() -> Self {
        Self {
            base: Duration::from_secs(2),
            factor: 2.0,
            max: Duration::from_secs(300),
            jitter: 0.0,
        }
    }
}

impl BackoffPolicy {
    /// Construct an exponential policy from whole seconds with no jitter.
    pub fn exponential(base_secs: u64, max_secs: u64) -> Self {
        Self {
            base: Duration::from_secs(base_secs.max(1)),
            factor: 2.0,
            max: Duration::from_secs(max_secs.max(base_secs.max(1))),
            jitter: 0.0,
        }
    }

    /// Base delay for `attempt` (1-based) **before** jitter is applied.
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let attempt = attempt.max(1);
        let exp = (attempt - 1).min(32); // guard against f64 overflow
        let raw = self.base.as_secs_f64() * self.factor.powi(exp as i32);
        let capped = raw.min(self.max.as_secs_f64());
        Duration::from_secs_f64(capped.max(0.0))
    }

    /// Delay including jitter. `rand_unit` must be in `[0.0, 1.0)`; callers
    /// pass a value derived from their own RNG so this stays pure/testable.
    pub fn delay_with_jitter(&self, attempt: u32, rand_unit: f64) -> Duration {
        let base = self.delay_for_attempt(attempt);
        if self.jitter <= 0.0 {
            return base;
        }
        let span = base.as_secs_f64() * self.jitter;
        // map rand_unit in [0,1) to [-span, +span]
        let offset = (rand_unit.clamp(0.0, 1.0) * 2.0 - 1.0) * span;
        let secs = (base.as_secs_f64() + offset)
            .max(0.0)
            .min(self.max.as_secs_f64());
        Duration::from_secs_f64(secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grows_exponentially() {
        let p = BackoffPolicy::exponential(2, 1000);
        assert_eq!(p.delay_for_attempt(1), Duration::from_secs(2));
        assert_eq!(p.delay_for_attempt(2), Duration::from_secs(4));
        assert_eq!(p.delay_for_attempt(3), Duration::from_secs(8));
        assert_eq!(p.delay_for_attempt(4), Duration::from_secs(16));
    }

    #[test]
    fn respects_the_ceiling() {
        let p = BackoffPolicy::exponential(2, 10);
        assert_eq!(p.delay_for_attempt(10), Duration::from_secs(10));
    }

    #[test]
    fn attempt_zero_is_treated_as_one() {
        let p = BackoffPolicy::exponential(3, 1000);
        assert_eq!(p.delay_for_attempt(0), Duration::from_secs(3));
    }

    #[test]
    fn jitter_stays_within_bounds() {
        let p = BackoffPolicy {
            jitter: 0.5,
            ..BackoffPolicy::exponential(10, 1000)
        };
        for r in [0.0, 0.25, 0.5, 0.75, 0.999] {
            let d = p.delay_with_jitter(1, r).as_secs_f64();
            assert!(
                (5.0..=15.0).contains(&d),
                "jittered delay {d} out of bounds"
            );
        }
    }

    #[test]
    fn zero_jitter_is_identity() {
        let p = BackoffPolicy::exponential(5, 1000);
        assert_eq!(p.delay_with_jitter(2, 0.9), p.delay_for_attempt(2));
    }

    #[test]
    fn does_not_overflow_for_large_attempts() {
        let p = BackoffPolicy::exponential(2, 600);
        assert_eq!(p.delay_for_attempt(1_000), Duration::from_secs(600));
    }
}
