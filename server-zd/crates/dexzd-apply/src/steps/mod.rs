// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Step executions and their method calls (plan sections 8.1 and 8.3).

mod attempt_snapshot;
mod invocations;
mod retry_backoff;
mod step_creation;
mod step_decisions;
mod step_results;
pub(crate) mod wait_conditions;

pub(crate) use invocations::{
    InvocationEnd, configured_method_timeout_seconds, end_running_attempt, make_invocation_pending,
    start_attempt,
};
pub(crate) use retry_backoff::{
    DEFAULT_BACKOFF_COEFFICIENT, DEFAULT_INITIAL_INTERVAL_SECONDS,
    DEFAULT_MAXIMUM_INTERVAL_SECONDS, DEFAULT_TOTAL_DURATION_SECONDS, RetryDecision, decide_retry,
};
pub(crate) use step_creation::{NewStepExecution, create_step_execution};
pub(crate) use step_decisions::{
    apply_step_decision, cancel_steps_by_type, complete_gracefully_if_ready, finish_step, is_active,
};
pub(crate) use step_results::{
    StepCommandOutcome, apply_attempt_failed, apply_attempt_heartbeat, apply_attempt_started,
    apply_method_completed, apply_retry_backoff_fired, arm_timer, disarm_timer,
};

/// Step execution ID of the Flow timeout handler.
pub(crate) const TIMEOUT_HANDLER_STEP_EXECUTION_ID: &str = "sys:timeout_handler-1";
/// Step type of the Flow timeout handler.
pub(crate) const TIMEOUT_HANDLER_STEP_TYPE: &str = "sys:timeout_handler";
pub(crate) const STARTING_STEP_FROM_STEP_EXECUTION_ID: &str = "__start__";
