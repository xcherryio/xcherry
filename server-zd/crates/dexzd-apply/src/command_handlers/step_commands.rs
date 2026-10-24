// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Step attempt commands and fired timers.

use dexzd_protocol::dex::{FlowErrorType, FlowStatus, FlowTimeoutPolicy, PullRouting};
use dexzd_protocol::dexzd::v1::{
    StepAttemptFailedCommand, StepAttemptFailureKind, StepExecutionPhase, StepMethod,
    TimersFiredCommand, flow_command::Command, timer_id,
};

use crate::ApplyError;
use crate::command_handlers::command_replies::ok_reply;
use crate::command_handlers::flow_close::{FlowClosing, close_flow};
use crate::command_handlers::run_cursor::{HistoryKind, RunCursor};
use crate::command_handlers::{CommandScope, HandlerResult};
use crate::state_access::FlowTransaction;
use crate::steps::{
    NewStepExecution, StepCommandOutcome, TIMEOUT_HANDLER_STEP_EXECUTION_ID, apply_attempt_failed,
    apply_attempt_heartbeat, apply_attempt_started, apply_method_completed,
    apply_retry_backoff_fired, create_step_execution, disarm_timer, make_invocation_pending,
};
use crate::values::ValueRejection;

/// Result of a Step command that may have to be re-applied as a failure of the attempt.
pub(crate) enum StepHandlerResult {
    Done(HandlerResult),
    /// The Worker's result was invalid in a way only apply can detect; the caller re-applies
    /// the command as this non-retryable failure in a fresh transaction.
    ApplyAsFailure(StepAttemptFailedCommand),
}

pub(crate) fn apply_step_command(
    transaction: &mut FlowTransaction<'_>,
    mut cursor: RunCursor,
    scope: &CommandScope<'_>,
    command: &Command,
) -> Result<StepHandlerResult, ApplyError> {
    let (outcome, kind) = match command {
        Command::StepAttemptStarted(started) => (
            apply_attempt_started(transaction, &mut cursor, scope, started)?,
            HistoryKind::BOOKKEEPING,
        ),
        Command::StepAttemptHeartbeat(heartbeat) => (
            apply_attempt_heartbeat(transaction, &mut cursor, heartbeat)?,
            HistoryKind::BOOKKEEPING,
        ),
        Command::StepAttemptFailed(failed) => (
            apply_attempt_failed(transaction, &mut cursor, scope, failed)?,
            HistoryKind::SEMANTIC,
        ),
        Command::StepMethodCompleted(completed) => {
            let outcome = apply_method_completed(transaction, &mut cursor, scope, completed)?;
            if let Some(failure) = rejected_result_failure(
                completed.invocation.clone(),
                completed.attempt,
                completed.pull_routing.clone(),
                &outcome,
            ) {
                return Ok(StepHandlerResult::ApplyAsFailure(failure));
            }
            (outcome, HistoryKind::SEMANTIC)
        }
        _ => {
            return Err(ApplyError::InvariantViolation {
                detail: "apply_step_command received a non-Step command".to_owned(),
            });
        }
    };
    match outcome {
        StepCommandOutcome::Applied(reply) => Ok(StepHandlerResult::Done(HandlerResult::Applied(
            cursor.finish(transaction, scope, reply, kind)?,
        ))),
        StepCommandOutcome::Ignored(reply) => {
            Ok(StepHandlerResult::Done(HandlerResult::Rejected(reply)))
        }
        outcome @ (StepCommandOutcome::RejectedResult(_)
        | StepCommandOutcome::MismatchedResult(_)) => {
            let (invocation, attempt) = match command {
                Command::StepAttemptHeartbeat(heartbeat) => {
                    (heartbeat.invocation.clone(), heartbeat.attempt)
                }
                _ => (None, 0),
            };
            rejected_result_failure(invocation, attempt, None, &outcome)
                .map(StepHandlerResult::ApplyAsFailure)
                .ok_or_else(|| ApplyError::InvariantViolation {
                    detail: "a rejected Step result produced no attempt failure".to_owned(),
                })
        }
    }
}

/// Returns the non-retryable attempt failure that replaces a rejected Worker result, keeping
/// the result's `pull_routing`, or `None` when the outcome is not a rejection.
fn rejected_result_failure(
    invocation: Option<dexzd_protocol::dexzd::v1::StepInvocationKey>,
    attempt: u32,
    pull_routing: Option<PullRouting>,
    outcome: &StepCommandOutcome,
) -> Option<StepAttemptFailedCommand> {
    let (kind, server_detail) = match outcome {
        StepCommandOutcome::RejectedResult(rejection @ ValueRejection::TooLarge { .. }) => {
            (StepAttemptFailureKind::ValueTooLarge, rejection.message())
        }
        StepCommandOutcome::RejectedResult(rejection @ ValueRejection::UnknownReference { .. }) => {
            (StepAttemptFailureKind::InvalidResponse, rejection.message())
        }
        StepCommandOutcome::MismatchedResult(detail) => {
            (StepAttemptFailureKind::InvalidResponse, detail.clone())
        }
        StepCommandOutcome::Applied(_) | StepCommandOutcome::Ignored(_) => return None,
    };
    Some(StepAttemptFailedCommand {
        invocation,
        attempt,
        kind: kind as i32,
        worker_error: None,
        server_detail,
        is_non_retryable: true,
        pull_routing,
    })
}

/// Applies timers that fired. Timers of another run attempt, or already removed, are skipped.
pub(crate) fn apply_timers_fired(
    transaction: &mut FlowTransaction<'_>,
    mut cursor: RunCursor,
    scope: &CommandScope<'_>,
    command: &TimersFiredCommand,
) -> Result<HandlerResult, ApplyError> {
    let mut is_any_applied = false;
    let mut is_retention_fired = false;
    for timer in &command.timers {
        let is_retention = matches!(
            timer.kind,
            Some(timer_id::Kind::Retention(_) | timer_id::Kind::OutboxRetention(_))
        );
        if disarm_timer(transaction, &cursor, timer)?.is_none() {
            continue;
        }
        is_any_applied = true;
        if !is_retention && timer.run_attempt != cursor.attempt() {
            continue;
        }
        match &timer.kind {
            Some(timer_id::Kind::RetryBackoff(backoff)) => {
                apply_retry_backoff_fired(transaction, &mut cursor, backoff)?;
            }
            Some(timer_id::Kind::FlowTimeout(_)) => {
                apply_flow_timeout(transaction, &mut cursor, scope)?
            }
            Some(timer_id::Kind::Retention(_)) => is_retention_fired = true,
            Some(timer_id::Kind::WaitCondition(wait_timer)) => {
                crate::steps::wait_conditions::on_timer_fired(
                    transaction,
                    &mut cursor,
                    scope,
                    wait_timer,
                )?;
            }
            Some(timer_id::Kind::OutboxRetention(outbox_timer)) => {
                crate::cross_flow::on_outbox_retention_fired(
                    transaction,
                    &mut cursor,
                    outbox_timer,
                )?;
            }
            None => {}
        }
    }
    if !is_any_applied {
        return Ok(HandlerResult::Rejected(ok_reply()));
    }
    if is_retention_fired {
        // Decided after every timer, so the outbox check sees this command's final rows.
        crate::cross_flow::on_retention_fired(transaction, &mut cursor, scope)?;
    }
    Ok(HandlerResult::Applied(cursor.finish(
        transaction,
        scope,
        ok_reply(),
        HistoryKind::SEMANTIC,
    )?))
}

/// The Flow timeout fired (plan section 9.1). FAIL fails with FLOW_TIMEOUT, CANCEL cancels,
/// and HANDLER (graceful timeout) runs the timeout handler's Execute.
pub(crate) fn apply_flow_timeout(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    scope: &CommandScope<'_>,
) -> Result<(), ApplyError> {
    if cursor.record.status != FlowStatus::Running as i32 {
        return Ok(());
    }
    let policy = FlowTimeoutPolicy::try_from(cursor.record.timeout_policy)
        .unwrap_or(FlowTimeoutPolicy::Fail);
    match policy {
        FlowTimeoutPolicy::Handler => {
            if cursor
                .rows
                .step_execution(transaction, TIMEOUT_HANDLER_STEP_EXECUTION_ID)?
                .is_some()
            {
                return Ok(());
            }
            let handler_options = cursor
                .record
                .timeout_handler_options
                .clone()
                .unwrap_or_default();
            let options = dexzd_protocol::dex::StepOptions {
                execute_timeout_seconds: handler_options.method_timeout_seconds,
                heartbeat_timeout_seconds: handler_options.heartbeat_timeout_seconds,
                execute_retry_policy: handler_options.retry_policy,
                execute_durability_override: handler_options.durability_override,
                execute_lock_attribute_keys: handler_options.lock_attribute_keys.clone(),
                execute_load_attribute_map_instances: handler_options
                    .load_attribute_map_instances
                    .clone(),
                execute_load_channel_names: handler_options.load_channel_names.clone(),
                execute_load_channel_map_instances: handler_options
                    .load_channel_map_instances
                    .clone(),
                skip_wait_for: true,
                ..dexzd_protocol::dex::StepOptions::default()
            };
            let new_step = NewStepExecution {
                step_type: crate::steps::TIMEOUT_HANDLER_STEP_TYPE,
                input: None,
                options: Some(&options),
                from_step_execution_id: crate::steps::STARTING_STEP_FROM_STEP_EXECUTION_ID,
                is_timeout_handler: true,
            };
            if create_step_execution(transaction, cursor, scope.now_micros, &new_step)?.is_ok()
                && let Some(mut handler) = cursor
                    .rows
                    .step_execution(transaction, TIMEOUT_HANDLER_STEP_EXECUTION_ID)?
            {
                handler.phase = StepExecutionPhase::Execute as i32;
                make_invocation_pending(
                    transaction,
                    cursor,
                    &mut handler,
                    StepMethod::Execute,
                    1,
                    scope.now_micros,
                )?;
                cursor.rows.put_step_execution(transaction, &handler);
            }
        }
        FlowTimeoutPolicy::Cancel => {
            close_flow(transaction, cursor, scope, FlowClosing::canceled())?;
        }
        FlowTimeoutPolicy::Fail | FlowTimeoutPolicy::Unspecified => {
            close_flow(
                transaction,
                cursor,
                scope,
                FlowClosing::failed(
                    FlowErrorType::FlowTimeout,
                    format!(
                        "Flow timed out after {} seconds",
                        (cursor.record.timeout_deadline_micros - cursor.record.started_at_micros)
                            / 1_000_000
                    ),
                ),
            )?;
        }
    }
    Ok(())
}
