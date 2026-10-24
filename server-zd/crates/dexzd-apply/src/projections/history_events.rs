// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! History events projected while applying commands (plan section 9.9).

use dexzd_protocol::dex::{
    FlowClosedHistoryEvent, FlowExecutionId, FlowInitialStart, FlowStartedOrContinuedHistoryEvent,
    Kv, PendingStepMethodPhase, RetryPolicy, StepCompletionOutput, StepExecuteCompletedEvent,
    StepExecuteCompletedOutput, StepExecuteFailedEvent, StepMethodEventContext,
    StepMethodEventInput, StepMethodFailedOutput, StepMethodFailure, StepMethodOptions,
    StepMethodPendingEvent, StepWaitForCompletedEvent, StepWaitForCompletedOutput,
    StepWaitForFailedEvent, flow_history_event, flow_started_or_continued_history_event,
};
use dexzd_protocol::dexzd::v1::{
    StartFlowReply, StepExecutionRecord, StepInvocationPhase, StepInvocationRecord, StepMethod,
    StepMethodCompletedCommand, step_method_completed_command,
};

use crate::ApplyError;
use crate::command_handlers::run_cursor::{RunCursor, timestamp};
use crate::state_access::FlowTransaction;
use crate::steps::wait_conditions::wire_condition_results;
use crate::steps::{
    DEFAULT_BACKOFF_COEFFICIENT, DEFAULT_INITIAL_INTERVAL_SECONDS,
    DEFAULT_MAXIMUM_INTERVAL_SECONDS, DEFAULT_TOTAL_DURATION_SECONDS,
};
use crate::values::wire_value;

const DEFAULT_METHOD_TIMEOUT_SECONDS: i32 = 2 * 60 * 60;
const DEFAULT_HEARTBEAT_TIMEOUT_SECONDS: i32 = 60;

pub(crate) fn record_flow_started(
    cursor: &mut RunCursor,
    request: &dexzd_protocol::dex::StartFlowRequest,
    flow_config: &dexzd_protocol::dex::FlowConfig,
) {
    let options = request.flow_start_options.clone().unwrap_or_default();
    let initial_attributes = options
        .attributes
        .iter()
        .map(|write| Kv {
            key: write.key.clone(),
            value: write.value.clone(),
        })
        .collect();
    let event = FlowStartedOrContinuedHistoryEvent {
        flow_execution_id: Some(FlowExecutionId {
            flow_id: request.flow_id.clone(),
            run_id: cursor.run_id().to_string(),
        }),
        flow_type: request.flow_type.clone(),
        flow_config: Some(flow_config.clone()),
        flow_timeout: (request.flow_timeout_seconds > 0).then(|| prost_types::Duration {
            seconds: i64::from(request.flow_timeout_seconds),
            nanos: 0,
        }),
        flow_timeout_policy: cursor.record.timeout_policy,
        timeout_handler_options: options.timeout_handler_options.clone(),
        start_or_continue: Some(
            flow_started_or_continued_history_event::StartOrContinue::InitialStart(
                FlowInitialStart {
                    start_step_type: request.start_step_type.clone(),
                    step_input: request.step_input.clone(),
                    step_options: request.step_options.clone(),
                    initial_attributes,
                },
            ),
        ),
    };
    cursor.add_event(flow_history_event::Payload::FlowStartedOrContinued(event));
}

pub(crate) fn record_flow_closed(cursor: &mut RunCursor, results: Vec<StepCompletionOutput>) {
    let close = cursor.record.close.clone().unwrap_or_default();
    let event = FlowClosedHistoryEvent {
        flow_status: cursor.record.status,
        results,
        error_type: close.error_type,
        error_message: close.error_message,
        continued_to_run_id: String::new(),
        sticky_app_worker_id: cursor.record.sticky_app_worker_id.clone(),
    };
    cursor.add_event(flow_history_event::Payload::FlowClosed(event));
}

pub(crate) fn record_method_pending(
    cursor: &mut RunCursor,
    step: &StepExecutionRecord,
    closed_at_micros: i64,
) {
    let Some(invocation) = step.invocation.as_ref() else {
        return;
    };
    let method = StepMethod::try_from(invocation.method).unwrap_or(StepMethod::Unspecified);
    let final_attempt = if invocation.phase == StepInvocationPhase::Running as i32 {
        invocation.attempt
    } else if invocation.last_failure.is_some() {
        invocation.attempt.saturating_sub(1)
    } else {
        invocation.attempt
    };
    let mut context = method_context(step, invocation, method, final_attempt, closed_at_micros);
    context.pull_routing = invocation.pull_routing.clone();
    let input = method_input(cursor, step);
    let pending = StepMethodPendingEvent {
        input: Some(input),
        context: Some(context),
        phase: if invocation.phase == StepInvocationPhase::Running as i32 {
            PendingStepMethodPhase::Started as i32
        } else {
            PendingStepMethodPhase::Scheduled as i32
        },
    };
    let payload = match method {
        StepMethod::WaitFor => flow_history_event::Payload::StepWaitForPending(pending),
        StepMethod::Execute | StepMethod::Unspecified => {
            flow_history_event::Payload::StepExecutePending(pending)
        }
    };
    cursor.add_event(payload);
}

fn method_context(
    step: &StepExecutionRecord,
    invocation: &StepInvocationRecord,
    method: StepMethod,
    attempt: u32,
    event_at_micros: i64,
) -> StepMethodEventContext {
    let started_at_micros = if invocation.attempt_started_at_micros > 0 {
        invocation.attempt_started_at_micros
    } else {
        invocation.first_attempt_at_micros
    };
    let duration_micros = event_at_micros.saturating_sub(started_at_micros);
    StepMethodEventContext {
        step_execution_id: step.step_execution_id.clone(),
        from_step_execution_id: step.from_step_execution_id.clone(),
        step_type: step.step_type.clone(),
        durability: invocation.effective_durability,
        final_attempt: i32::try_from(attempt).unwrap_or(i32::MAX),
        started_time: Some(timestamp(started_at_micros)),
        duration: Some(prost_types::Duration {
            seconds: duration_micros / 1_000_000,
            nanos: i32::try_from(duration_micros % 1_000_000)
                .expect("fractional microseconds fit i32")
                * 1000,
        }),
        method_options: Some(effective_method_options(step, method)),
        last_failure_info: invocation.last_failure.clone(),
        ..StepMethodEventContext::default()
    }
}

fn effective_method_options(step: &StepExecutionRecord, method: StepMethod) -> StepMethodOptions {
    let options = step.options.as_ref();
    let (timeout_seconds, retry_policy) = match method {
        StepMethod::WaitFor => (
            options.map_or(0, |options| options.wait_for_timeout_seconds),
            options.and_then(|options| options.wait_for_retry_policy.as_ref()),
        ),
        StepMethod::Execute | StepMethod::Unspecified => (
            options.map_or(0, |options| options.execute_timeout_seconds),
            options.and_then(|options| options.execute_retry_policy.as_ref()),
        ),
    };
    let heartbeat_timeout_seconds = options.map_or(0, |options| options.heartbeat_timeout_seconds);
    StepMethodOptions {
        timeout_seconds: if timeout_seconds > 0 {
            timeout_seconds
        } else {
            DEFAULT_METHOD_TIMEOUT_SECONDS
        },
        retry_policy: Some(effective_retry_policy(retry_policy)),
        heartbeat_timeout_seconds: if heartbeat_timeout_seconds > 0 {
            heartbeat_timeout_seconds
        } else {
            DEFAULT_HEARTBEAT_TIMEOUT_SECONDS
        },
    }
}

fn effective_retry_policy(policy: Option<&RetryPolicy>) -> RetryPolicy {
    let mut effective = policy.cloned().unwrap_or_default();
    if effective.initial_interval_seconds <= 0 {
        effective.initial_interval_seconds = DEFAULT_INITIAL_INTERVAL_SECONDS as i32;
    }
    if effective.backoff_coefficient <= 0.0 || effective.backoff_coefficient.is_nan() {
        effective.backoff_coefficient = DEFAULT_BACKOFF_COEFFICIENT as f32;
    }
    if effective.maximum_interval_seconds <= 0 {
        effective.maximum_interval_seconds = DEFAULT_MAXIMUM_INTERVAL_SECONDS as i32;
    }
    if effective.total_duration_seconds <= 0 {
        effective.total_duration_seconds = DEFAULT_TOTAL_DURATION_SECONDS as i32;
    }
    effective
}

fn method_input(cursor: &RunCursor, step: &StepExecutionRecord) -> StepMethodEventInput {
    let is_execute = step
        .invocation
        .as_ref()
        .is_some_and(|invocation| invocation.method == StepMethod::Execute as i32);
    StepMethodEventInput {
        unavailable: step.input.is_none(),
        step_input: step
            .input
            .as_ref()
            .map(|stored| wire_value(cursor.rows, stored)),
        condition_results: if is_execute {
            step.condition_results
                .as_ref()
                .map(|results| wire_condition_results(cursor.rows, results))
        } else {
            None
        },
        attributes: step
            .invocation
            .as_ref()
            .and_then(|invocation| invocation.snapshot.as_ref())
            .map(|snapshot| {
                snapshot
                    .attributes
                    .iter()
                    .map(|attribute| Kv {
                        key: attribute.key.clone(),
                        value: attribute
                            .value
                            .as_ref()
                            .map(|stored| wire_value(cursor.rows, stored)),
                    })
                    .collect()
            })
            .unwrap_or_default(),
        step_execution_locals: if is_execute {
            step.locals
                .iter()
                .map(|local| Kv {
                    key: local.key.clone(),
                    value: local
                        .value
                        .as_ref()
                        .map(|stored| wire_value(cursor.rows, stored)),
                })
                .collect()
        } else {
            Vec::new()
        },
    }
}

/// Records `step_wait_for_completed` or `step_execute_completed` before the attempt ends, so
/// the input is the attempt's snapshot.
pub(crate) fn record_method_completed(
    transaction: &FlowTransaction<'_>,
    cursor: &mut RunCursor,
    step: &StepExecutionRecord,
    command: &StepMethodCompletedCommand,
    completed_at_micros: i64,
) -> Result<(), ApplyError> {
    let _ = transaction;
    let invocation = step
        .invocation
        .as_ref()
        .ok_or_else(|| ApplyError::InvariantViolation {
            detail: "completed Step method has no invocation".to_owned(),
        })?;
    let method = StepMethod::try_from(invocation.method).unwrap_or(StepMethod::Unspecified);
    let mut context = method_context(
        step,
        invocation,
        method,
        command.attempt,
        completed_at_micros,
    );
    context.pull_routing = command.pull_routing.clone();
    let input = method_input(cursor, step);
    let payload = match &command.response {
        Some(step_method_completed_command::Response::WaitFor(response)) => {
            flow_history_event::Payload::StepWaitForCompleted(StepWaitForCompletedEvent {
                input: Some(input),
                output: Some(StepWaitForCompletedOutput {
                    wait_for_condition: response.waiting_condition.clone(),
                    upsert_attributes: response.upsert_attributes.clone(),
                    publish_to_channel: response.publish_to_channel.clone(),
                    record_events: response.record_events.clone(),
                    upsert_step_execution_locals: response.upsert_step_exe_locals.clone(),
                    delete_from_channel: response.delete_from_channel.clone(),
                }),
                context: Some(context),
            })
        }
        Some(step_method_completed_command::Response::Execute(response)) => {
            flow_history_event::Payload::StepExecuteCompleted(StepExecuteCompletedEvent {
                input: Some(input),
                output: Some(StepExecuteCompletedOutput {
                    step_decision: response.step_decision.clone(),
                    upsert_attributes: response.upsert_attributes.clone(),
                    publish_to_channel: response.publish_to_channel.clone(),
                    record_events: response.record_events.clone(),
                    upsert_step_execution_locals: response.upsert_step_exe_locals.clone(),
                    delete_from_channel: response.delete_from_channel.clone(),
                }),
                context: Some(context),
            })
        }
        None => return Ok(()),
    };
    cursor.add_event(payload);
    Ok(())
}

pub(crate) fn record_method_failed(
    transaction: &FlowTransaction<'_>,
    cursor: &mut RunCursor,
    step: &StepExecutionRecord,
    method: StepMethod,
    failure: &StepMethodFailure,
    failed_at_micros: i64,
) -> Result<(), ApplyError> {
    let _ = transaction;
    let invocation = step
        .invocation
        .as_ref()
        .ok_or_else(|| ApplyError::InvariantViolation {
            detail: "failed Step method has no invocation".to_owned(),
        })?;
    let attempt = u32::try_from(failure.attempt).unwrap_or(0);
    let mut context = method_context(step, invocation, method, attempt, failed_at_micros);
    context.pull_routing = failure.pull_routing.clone();
    let input = method_input(cursor, step);
    let output = Some(StepMethodFailedOutput {
        failure: Some(failure.clone()),
    });
    let payload = match method {
        StepMethod::WaitFor => {
            flow_history_event::Payload::StepWaitForFailed(StepWaitForFailedEvent {
                input: Some(input),
                output,
                context: Some(context),
            })
        }
        StepMethod::Execute | StepMethod::Unspecified => {
            flow_history_event::Payload::StepExecuteFailed(StepExecuteFailedEvent {
                input: Some(input),
                output,
                context: Some(context),
            })
        }
    };
    cursor.add_event(payload);
    Ok(())
}

/// Reply for a started run.
pub(crate) fn start_flow_reply(cursor: &RunCursor, is_existing_run: bool) -> StartFlowReply {
    StartFlowReply {
        run_id: cursor.run_id().as_bytes().to_vec(),
        is_existing_run,
    }
}
