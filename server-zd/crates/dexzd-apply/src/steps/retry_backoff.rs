// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_protocol::dex::RetryPolicy;

const DEFAULT_INITIAL_INTERVAL_SECONDS: i64 = 1;
const DEFAULT_BACKOFF_COEFFICIENT: f64 = 2.0;
const DEFAULT_MAXIMUM_INTERVAL_SECONDS: i64 = 100;
const DEFAULT_TOTAL_DURATION_SECONDS: i64 = 4 * 60 * 60;
const MICROS_PER_SECOND: i64 = 1_000_000;

/// What happens after a failed attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetryDecision {
    /// Retry: the next attempt becomes pending at this time.
    RetryAt { fires_at_micros: i64 },
    /// No retry is left; the method call failed.
    Exhausted,
}

/// Decides the retry of a failed attempt with the Go Server's defaults: initial interval 1 s,
/// coefficient 2, maximum interval 100 s, unlimited attempts, and 4 hours of total duration.
///
/// `failed_attempt` counts from 1. `retry_after_seconds > 0` from the Worker overrides the next
/// interval. The coefficient is applied by repeated multiplication, never `powf`, so every
/// platform computes the same interval.
pub(crate) fn decide_retry(
    policy: Option<&RetryPolicy>,
    failed_attempt: u32,
    first_attempt_at_micros: i64,
    now_micros: i64,
    retry_after_seconds: i32,
    is_non_retryable: bool,
) -> RetryDecision {
    if is_non_retryable {
        return RetryDecision::Exhausted;
    }
    let default_policy = RetryPolicy::default();
    let policy = policy.unwrap_or(&default_policy);
    let maximum_attempts = u32::try_from(policy.maximum_attempts).unwrap_or(0);
    if maximum_attempts > 0 && failed_attempt >= maximum_attempts {
        return RetryDecision::Exhausted;
    }
    let initial_seconds = positive_or(
        i64::from(policy.initial_interval_seconds),
        DEFAULT_INITIAL_INTERVAL_SECONDS,
    );
    let maximum_seconds = positive_or(
        i64::from(policy.maximum_interval_seconds),
        DEFAULT_MAXIMUM_INTERVAL_SECONDS,
    );
    let coefficient = if policy.backoff_coefficient > 0.0 {
        f64::from(policy.backoff_coefficient)
    } else {
        DEFAULT_BACKOFF_COEFFICIENT
    };
    let interval_micros = if retry_after_seconds > 0 {
        i64::from(retry_after_seconds) * MICROS_PER_SECOND
    } else {
        let mut interval_seconds = initial_seconds as f64;
        for _ in 1..failed_attempt.min(64) {
            interval_seconds *= coefficient;
            if interval_seconds >= maximum_seconds as f64 {
                break;
            }
        }
        // Round to whole milliseconds so comparisons never depend on float formatting.
        let interval_millis =
            (interval_seconds.min(maximum_seconds as f64) * 1000.0).round() as i64;
        interval_millis * 1000
    };
    let total_seconds = positive_or(
        i64::from(policy.total_duration_seconds),
        DEFAULT_TOTAL_DURATION_SECONDS,
    );
    let fires_at_micros = now_micros.saturating_add(interval_micros);
    let deadline_micros =
        first_attempt_at_micros.saturating_add(total_seconds.saturating_mul(MICROS_PER_SECOND));
    if fires_at_micros > deadline_micros {
        return RetryDecision::Exhausted;
    }
    RetryDecision::RetryAt { fires_at_micros }
}

fn positive_or(value: i64, default: i64) -> i64 {
    if value > 0 { value } else { default }
}

#[cfg(test)]
mod tests {
    use dexzd_protocol::dex::RetryPolicy;

    use super::{RetryDecision, decide_retry};

    const SECOND: i64 = 1_000_000;

    #[test]
    fn default_policy_doubles_up_to_one_hundred_seconds() {
        let intervals: Vec<i64> = (1..=9)
            .map(
                |attempt| match decide_retry(None, attempt, 0, 0, 0, false) {
                    RetryDecision::RetryAt { fires_at_micros } => fires_at_micros / SECOND,
                    RetryDecision::Exhausted => -1,
                },
            )
            .collect();
        assert_eq!(intervals, vec![1, 2, 4, 8, 16, 32, 64, 100, 100]);
    }

    #[test]
    fn maximum_attempts_and_total_duration_exhaust() {
        let policy = RetryPolicy {
            maximum_attempts: 3,
            ..RetryPolicy::default()
        };
        assert!(matches!(
            decide_retry(Some(&policy), 2, 0, 0, 0, false),
            RetryDecision::RetryAt { .. }
        ));
        assert_eq!(
            decide_retry(Some(&policy), 3, 0, 0, 0, false),
            RetryDecision::Exhausted
        );
        let short = RetryPolicy {
            total_duration_seconds: 5,
            ..RetryPolicy::default()
        };
        assert_eq!(
            decide_retry(Some(&short), 4, 0, 0, 0, false),
            RetryDecision::Exhausted
        );
        assert_eq!(
            decide_retry(None, 1, 0, 0, 0, true),
            RetryDecision::Exhausted
        );
    }

    #[test]
    fn retry_after_overrides_the_interval() {
        assert_eq!(
            decide_retry(None, 1, 0, 10 * SECOND, 30, false),
            RetryDecision::RetryAt {
                fires_at_micros: 40 * SECOND
            }
        );
    }
}
