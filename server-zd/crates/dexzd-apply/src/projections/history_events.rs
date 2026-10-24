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
    Kv, PendingStepMethodPhase, StepCompletionOutput, StepExecuteCompletedEvent,
    StepExecuteCompletedOutput, StepExecuteFailedEvent, StepMethodEventContext,
    StepMethodEventInput, StepMethodFailedOutput, StepMethodFailure, StepMethodOptions,
    StepMethodPendingEvent, StepWaitForCompletedEvent, StepWaitForCompletedOutput,
    StepWaitForFailedEvent, flow_history_event, flow_started_or_continued_history_event,
};
use dexzd_protocol::dexzd::v1::{
    StartFlowReply, StepExecutionRecord, StepInvocationPhase, StepMethod,
    StepMethodCompletedCommand, step_method_completed_command,
};

use crate::ApplyError;
use crate::command_handlers::run_cursor::{RunCursor, timestamp};
use crate::state_access::FlowTransaction;
use crate::steps::wait_conditions::wire_condition_results;
use crate::values::wire_value;

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
    let options = step.options.as_ref();
    let (timeout_seconds, retry_policy, durability) = match method {
        StepMethod::WaitFor => (
            options.map_or(0, |options| options.wait_for_timeout_seconds),
            options.and_then(|options| options.wait_for_retry_policy),
            options.map_or(0, |options| options.wait_for_durability_override),
        ),
        StepMethod::Execute | StepMethod::Unspecified => (
            options.map_or(0, |options| options.execute_timeout_seconds),
            options.and_then(|options| options.execute_retry_policy),
            options.map_or(0, |options| options.execute_durability_override),
        ),
    };
    let started_at = invocation.attempt_started_at_micros;
    let mut context = method_context(step, invocation.attempt, started_at);
    context.durability = durability;
    context.method_options = Some(StepMethodOptions {
        timeout_seconds,
        retry_policy,
        heartbeat_timeout_seconds: options.map_or(0, |options| options.heartbeat_timeout_seconds),
    });
    context.last_failure_info = invocation.last_failure.clone();
    context.pull_routing = invocation.pull_routing.clone();
    let duration_micros = closed_at_micros.saturating_sub(invocation.first_attempt_at_micros);
    if invocation.first_attempt_at_micros > 0 && duration_micros >= 0 {
        context.duration = Some(prost_types::Duration {
            seconds: duration_micros / 1_000_000,
            nanos: i32::try_from(duration_micros % 1_000_000).unwrap_or(0) * 1000,
        });
    }
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
    attempt: u32,
    started_at_micros: i64,
) -> StepMethodEventContext {
    StepMethodEventContext {
        step_execution_id: step.step_execution_id.clone(),
        from_step_execution_id: step.from_step_execution_id.clone(),
        step_type: step.step_type.clone(),
        final_attempt: i32::try_from(attempt).unwrap_or(i32::MAX),
        started_time: (started_at_micros > 0).then(|| timestamp(started_at_micros)),
        ..StepMethodEventContext::default()
    }
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
) -> Result<(), ApplyError> {
    let _ = transaction;
    let started_at = step
        .invocation
        .as_ref()
        .map_or(0, |invocation| invocation.attempt_started_at_micros);
    let mut context = method_context(step, command.attempt, started_at);
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
) -> Result<(), ApplyError> {
    let _ = transaction;
    let started_at = step
        .invocation
        .as_ref()
        .map_or(0, |invocation| invocation.attempt_started_at_micros);
    let attempt = u32::try_from(failure.attempt).unwrap_or(0);
    let mut context = method_context(step, attempt, started_at);
    context.last_failure_info = Some(failure.clone());
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
