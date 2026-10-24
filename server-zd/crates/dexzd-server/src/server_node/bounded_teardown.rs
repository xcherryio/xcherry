// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Shutdown steps that each run within their own budget.

use std::fmt::Display;
use std::future::Future;
use std::time::Duration;

use crate::ServerError;

/// Runs shutdown steps with separate budgets, so a stalled step never skips the engine's lease release.
#[derive(Default)]
pub(super) struct BoundedTeardown {
    incomplete_steps: Vec<String>,
}

impl BoundedTeardown {
    /// Runs `step` and abandons it after `budget`.
    pub(super) async fn run(
        &mut self,
        name: &'static str,
        budget: Duration,
        step: impl Future<Output = ()>,
    ) {
        if tokio::time::timeout(budget, step).await.is_err() {
            self.record_overrun(name, budget);
        }
    }

    /// Runs a fallible `step`, abandons it after `budget`, and records its failure.
    pub(super) async fn run_fallible<E: Display>(
        &mut self,
        name: &'static str,
        budget: Duration,
        step: impl Future<Output = Result<(), E>>,
    ) {
        match tokio::time::timeout(budget, step).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => self.record_failure(name, &error),
            Err(_) => self.record_overrun(name, budget),
        }
    }

    /// Records a step that its owner abandoned after `budget`.
    pub(super) fn record_overrun(&mut self, name: &'static str, budget: Duration) {
        tracing::error!(
            step = name,
            budget_milliseconds = budget.as_millis(),
            "a shutdown step did not finish within its budget; abandoning it"
        );
        self.incomplete_steps.push(format!(
            "{name} exceeded its {} ms budget",
            budget.as_millis()
        ));
    }

    fn record_failure(&mut self, name: &'static str, error: &dyn Display) {
        tracing::error!(step = name, %error, "a shutdown step failed");
        self.incomplete_steps
            .push(format!("{name} failed: {error}"));
    }

    /// Returns how serving ended.
    ///
    /// Returns `outcome` when it is an error or every step finished, and
    /// [`ServerError::ShutdownIncomplete`] when serving ended normally but a step was abandoned or
    /// failed.
    pub(super) fn finish(self, outcome: Result<(), ServerError>) -> Result<(), ServerError> {
        if outcome.is_err() || self.incomplete_steps.is_empty() {
            return outcome;
        }
        Err(ServerError::ShutdownIncomplete {
            steps: self.incomplete_steps.join(", "),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn a_stalled_step_is_abandoned_and_later_steps_still_run() {
        let mut teardown = BoundedTeardown::default();
        teardown
            .run(
                "effect tasks",
                Duration::from_secs(1),
                std::future::pending(),
            )
            .await;
        let mut is_engine_stopped = false;
        teardown
            .run("engine", Duration::from_secs(1), async {
                is_engine_stopped = true;
            })
            .await;
        assert!(
            is_engine_stopped,
            "the engine step runs after a stalled step"
        );
        assert!(matches!(
            teardown.finish(Ok(())),
            Err(ServerError::ShutdownIncomplete { steps })
                if steps == "effect tasks exceeded its 1000 ms budget"
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_step_makes_a_normal_shutdown_incomplete() {
        let mut teardown = BoundedTeardown::default();
        teardown
            .run_fallible("engine", Duration::from_secs(1), async {
                Err::<(), _>("the lease release was rejected")
            })
            .await;
        let mut is_stream_store_closed = false;
        teardown
            .run("Stream Store", Duration::from_secs(1), async {
                is_stream_store_closed = true;
            })
            .await;
        assert!(
            is_stream_store_closed,
            "later steps run after a failed step"
        );
        assert!(matches!(
            teardown.finish(Ok(())),
            Err(ServerError::ShutdownIncomplete { steps })
                if steps == "engine failed: the lease release was rejected"
        ));
        let mut stalled = BoundedTeardown::default();
        stalled
            .run_fallible(
                "engine",
                Duration::from_secs(1),
                std::future::pending::<Result<(), &str>>(),
            )
            .await;
        assert!(matches!(
            stalled.finish(Ok(())),
            Err(ServerError::ShutdownIncomplete { steps })
                if steps == "engine exceeded its 1000 ms budget"
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn finished_steps_keep_the_serving_outcome() {
        let mut teardown = BoundedTeardown::default();
        teardown
            .run("engine", Duration::from_secs(1), async {})
            .await;
        assert!(teardown.finish(Ok(())).is_ok());
        let mut stalled = BoundedTeardown::default();
        stalled.record_overrun("engine", Duration::from_secs(1));
        assert!(matches!(
            stalled.finish(Err(ServerError::ServingTaskFailed {
                detail: "lost".to_owned()
            })),
            Err(ServerError::ServingTaskFailed { .. })
        ));
    }
}
