// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Step decisions returned by Execute (plan section 8.1): cancellations, the close decision,
//! next Steps, then completion of the deciding Step. Matches the Go interpreter's
//! `checkClosingWorkflow` and graceful completion rules.

use dexzd_protocol::dex::{CloseDecisionType, FlowErrorType, StepCompletionOutput, StepDecision};
use dexzd_protocol::dexzd::v1::{
    StepExecutionPhase, StepExecutionRecord, StoredStepCompletionOutput,
};

use crate::ApplyError;
use crate::command_handlers::CommandScope;
use crate::command_handlers::flow_close::{FlowClosing, close_flow};
use crate::command_handlers::run_cursor::RunCursor;
use crate::state_access::FlowTransaction;
use crate::steps::{InvocationEnd, NewStepExecution, create_step_execution, end_running_attempt};
use crate::values::{ValueHolder, ValueRejection, release_stored_value, store_value};

/// Applies the decision of a finished Execute and completes the Step.
pub(crate) fn apply_step_decision(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    scope: &CommandScope<'_>,
    step: &mut StepExecutionRecord,
    decision: Option<&StepDecision>,
) -> Result<Result<(), ValueRejection>, ApplyError> {
    let decision = decision.cloned().unwrap_or_default();
    finish_step(transaction, cursor, step, StepExecutionPhase::Completed)?;
    if !decision.cancel_step_types.is_empty() || !decision.cancel_sibling_step_types.is_empty() {
        cancel_steps_by_type(
            transaction,
            cursor,
            &decision.cancel_step_types,
            &decision.cancel_sibling_step_types,
            &step.from_step_execution_id,
        )?;
    }
    let mut can_go_next = true;
    if let Some(close) = &decision.close_decision {
        let close_type = CloseDecisionType::try_from(close.close_decision_type)
            .unwrap_or(CloseDecisionType::Unspecified);
        let output = || StepCompletionOutput {
            completed_step_type: step.step_type.clone(),
            completed_step_execution_id: step.step_execution_id.clone(),
            completed_step_output: close.close_input.clone(),
        };
        match close_type {
            CloseDecisionType::ForceCompleteOnChannelsEmpty => {
                let mut is_empty = true;
                for channel_name in &close.conditional_channel_names {
                    let meta = cursor.rows.channel_meta(transaction, channel_name)?;
                    is_empty &= meta.pending_count == 0 && meta.claimed_count == 0;
                }
                if is_empty {
                    if let Err(rejection) = collect_output(transaction, cursor, &output())? {
                        return Ok(Err(rejection));
                    }
                    close_flow(transaction, cursor, scope, FlowClosing::completed())?;
                    return Ok(Ok(()));
                }
            }
            CloseDecisionType::GracefulComplete => {
                if let Err(rejection) = collect_output(transaction, cursor, &output())? {
                    return Ok(Err(rejection));
                }
                cursor.record.is_graceful_completion_requested = true;
                can_go_next = false;
            }
            CloseDecisionType::ForceComplete => {
                if let Err(rejection) = collect_output(transaction, cursor, &output())? {
                    return Ok(Err(rejection));
                }
                close_flow(transaction, cursor, scope, FlowClosing::completed())?;
                return Ok(Ok(()));
            }
            CloseDecisionType::ForceFail => {
                if let Err(rejection) = collect_output(transaction, cursor, &output())? {
                    return Ok(Err(rejection));
                }
                close_flow(
                    transaction,
                    cursor,
                    scope,
                    FlowClosing::failed(
                        FlowErrorType::StepDecisionFailingFlow,
                        "the Step decision failed the Flow",
                    ),
                )?;
                return Ok(Ok(()));
            }
            CloseDecisionType::DeadEnd => can_go_next = false,
            CloseDecisionType::Unspecified => {
                close_flow(
                    transaction,
                    cursor,
                    scope,
                    FlowClosing::failed(FlowErrorType::Internal, "invalid close decision type"),
                )?;
                return Ok(Ok(()));
            }
        }
    }
    if can_go_next {
        for movement in &decision.next_steps {
            let new_step = NewStepExecution {
                step_type: &movement.step_type,
                input: movement.step_input.as_ref(),
                options: movement.step_options.as_ref(),
                from_step_execution_id: &step.step_execution_id,
                is_timeout_handler: false,
            };
            if let Err(rejection) =
                create_step_execution(transaction, cursor, scope.now_micros, &new_step)?
            {
                return Ok(Err(rejection));
            }
        }
    }
    complete_gracefully_if_ready(transaction, cursor, scope)?;
    Ok(Ok(()))
}

/// Closes the Flow as completed when graceful completion was requested and no user Step is
/// active any more (the timeout handler does not count).
pub(crate) fn complete_gracefully_if_ready(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    scope: &CommandScope<'_>,
) -> Result<(), ApplyError> {
    if !cursor.record.is_graceful_completion_requested || is_closed(cursor) {
        return Ok(());
    }
    let has_active_user_step = cursor
        .rows
        .step_executions(transaction)?
        .iter()
        .any(|(_, step)| !step.is_timeout_handler && is_active(step));
    if !has_active_user_step {
        close_flow(transaction, cursor, scope, FlowClosing::completed())?;
    }
    Ok(())
}

fn is_closed(cursor: &RunCursor) -> bool {
    cursor.record.status != dexzd_protocol::dex::FlowStatus::Running as i32
}

pub(crate) fn is_active(step: &StepExecutionRecord) -> bool {
    matches!(
        StepExecutionPhase::try_from(step.phase),
        Ok(StepExecutionPhase::WaitFor | StepExecutionPhase::Waiting | StepExecutionPhase::Execute)
    )
}

fn collect_output(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    output: &StepCompletionOutput,
) -> Result<Result<(), ValueRejection>, ApplyError> {
    let Some(value) = &output.completed_step_output else {
        return Ok(Ok(()));
    };
    if crate::values::is_null_value(Some(value)) {
        return Ok(Ok(()));
    }
    let close = cursor.record.close.get_or_insert_with(Default::default);
    let index = u32::try_from(close.results.len()).unwrap_or(u32::MAX);
    let holder = ValueHolder::FlowResult { index };
    let limits = cursor.value_limits();
    let stored = match store_value(
        transaction,
        cursor.rows,
        limits,
        &mut cursor.values,
        value,
        &holder,
    )? {
        Ok(stored) => stored,
        Err(rejection) => return Ok(Err(rejection)),
    };
    let close = cursor.record.close.get_or_insert_with(Default::default);
    close.results.push(StoredStepCompletionOutput {
        completed_step_type: output.completed_step_type.clone(),
        completed_step_execution_id: output.completed_step_execution_id.clone(),
        completed_step_output: Some(stored),
    });
    Ok(Ok(()))
}

/// Cancels every active Step of `global_step_types`, and of `sibling_step_types` under
/// `parent_step_execution_id`.
pub(crate) fn cancel_steps_by_type(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    global_step_types: &[String],
    sibling_step_types: &[String],
    parent_step_execution_id: &str,
) -> Result<(), ApplyError> {
    for (_, mut other) in cursor.rows.step_executions(transaction)? {
        // Sibling types match only Steps of the same parent, as in the Go Server's selector.
        let is_target = global_step_types.contains(&other.step_type)
            || (sibling_step_types.contains(&other.step_type)
                && other.from_step_execution_id == parent_step_execution_id);
        if is_target && is_active(&other) {
            finish_step(
                transaction,
                cursor,
                &mut other,
                StepExecutionPhase::Canceled,
            )?;
        }
    }
    Ok(())
}

/// Ends the Step: cancels its running attempt, releases its input, locals, condition results,
/// and wait registration, and stores the final phase.
pub(crate) fn finish_step(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    step: &mut StepExecutionRecord,
    phase: StepExecutionPhase,
) -> Result<(), ApplyError> {
    end_running_attempt(transaction, cursor, step, InvocationEnd::Canceled)?;
    if let Some(invocation) = step.invocation.take()
        && let Some(checkpoint) = &invocation.heartbeat_checkpoint
    {
        let holder = ValueHolder::HeartbeatCheckpoint {
            step_execution_id: step.step_execution_id.clone(),
            method: invocation.method,
        };
        release_stored_value(transaction, cursor.rows, checkpoint, &holder, cursor.seq)?;
    }
    if let Some(input) = step.input.take() {
        let holder = ValueHolder::StepInput {
            step_execution_id: step.step_execution_id.clone(),
        };
        release_stored_value(transaction, cursor.rows, &input, &holder, cursor.seq)?;
    }
    for local in std::mem::take(&mut step.locals) {
        let holder = ValueHolder::StepLocal {
            step_execution_id: step.step_execution_id.clone(),
            local_key: local.key.clone(),
        };
        if let Some(stored) = &local.value {
            release_stored_value(transaction, cursor.rows, stored, &holder, cursor.seq)?;
        }
    }
    crate::steps::wait_conditions::release_condition_results(transaction, cursor, step)?;
    step.wait_for_pull_routing = None;
    step.phase = phase as i32;
    step.completed_seq = cursor.seq;
    cursor.rows.put_step_execution(transaction, step);
    let run_id = cursor.run_id();
    cursor.add_effect(crate::ApplyEffect::WakeWaiters {
        run_id,
        change: crate::WaitableChange::StepExecutionsFinished {
            step_execution_ids: vec![step.step_execution_id.clone()],
        },
    });
    Ok(())
}
