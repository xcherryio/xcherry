// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Attempt lifecycle commands: started, heartbeat, failed, completed, and retry backoff.

use dexzd_keyspace::ColumnFamily;
use dexzd_keyspace::row_keys::{timer_index_key, timer_key};
use dexzd_protocol::dex::{ErrorSubStatus, FlowErrorType, ServiceErrorResponse, StepMethodFailure};
use dexzd_protocol::dexzd::v1::{
    RetryBackoffTimerId, StepAttemptFailedCommand, StepAttemptHeartbeatCommand,
    StepAttemptStartedCommand, StepExecutionPhase, StepExecutionRecord, StepInvocationKey,
    StepInvocationPhase, StepMethod, StepMethodCompletedCommand, TimerId, TimerRecord,
    step_attempt_heartbeat_command, step_method_completed_command, timer_id,
};
use prost::Message;

use crate::command_handlers::CommandScope;
use crate::command_handlers::command_replies::{error_reply, grpc_code, ok_reply};
use crate::command_handlers::run_cursor::RunCursor;
use crate::projections::history_events;
use crate::state_access::FlowTransaction;
use crate::steps::attempt_snapshot::compute_attempt_snapshot;
use crate::steps::invocations::{can_take_locks, release_heartbeat_checkpoint};
use crate::steps::{
    InvocationEnd, RetryDecision, apply_step_decision, decide_retry, end_running_attempt,
    make_invocation_pending, start_attempt, wait_conditions,
};
use crate::values::{ValueHolder, ValueRejection, release_stored_value, store_value};
use crate::{ApplyEffect, ApplyError};
use dexzd_protocol::dexzd::v1::CommandReply;

/// Outcome of a Step command handler.
pub(crate) enum StepCommandOutcome {
    /// Applied; record it with this reply.
    Applied(CommandReply),
    /// Nothing changes (stale attempt, missing Step); reply and discard writes.
    Ignored(CommandReply),
    /// The Worker's result is invalid in a way only apply can see; apply it as a non-retryable
    /// failure of the attempt instead.
    RejectedResult(ValueRejection),
}

fn stale_reply(detail: &str) -> CommandReply {
    error_reply(
        grpc_code::FAILED_PRECONDITION,
        ErrorSubStatus::Uncategorized,
        detail.to_owned(),
    )
}

/// Loads the Step and checks that `key`/`attempt` names its current invocation in `phase`.
fn load_invocation(
    transaction: &FlowTransaction<'_>,
    cursor: &RunCursor,
    key: Option<&StepInvocationKey>,
    attempt: u32,
    phase: StepInvocationPhase,
) -> Result<Option<StepExecutionRecord>, ApplyError> {
    let Some(key) = key else { return Ok(None) };
    let Some(step) = cursor
        .rows
        .step_execution(transaction, &key.step_execution_id)?
    else {
        return Ok(None);
    };
    let is_current = step.invocation.as_ref().is_some_and(|invocation| {
        invocation.method == key.method
            && invocation.attempt == attempt
            && invocation.phase == phase as i32
    });
    Ok(is_current.then_some(step))
}

pub(crate) fn apply_attempt_started(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    scope: &CommandScope<'_>,
    command: &StepAttemptStartedCommand,
) -> Result<StepCommandOutcome, ApplyError> {
    let Some(mut step) = load_invocation(
        transaction,
        cursor,
        command.invocation.as_ref(),
        command.attempt,
        StepInvocationPhase::Pending,
    )?
    else {
        return Ok(StepCommandOutcome::Ignored(stale_reply(
            "the attempt is no longer pending",
        )));
    };
    let key = command.invocation.clone().unwrap_or_default();
    let lock_keys = step
        .invocation
        .as_ref()
        .map(|invocation| invocation.lock_keys.clone())
        .unwrap_or_default();
    if !can_take_locks(transaction, cursor, &key, &lock_keys)? {
        return Ok(StepCommandOutcome::Ignored(error_reply(
            grpc_code::ABORTED,
            ErrorSubStatus::Uncategorized,
            "the attempt's locks are not available yet",
        )));
    }
    let method = StepMethod::try_from(key.method).unwrap_or(StepMethod::Execute);
    let snapshot = compute_attempt_snapshot(transaction, cursor, &step, method, command.attempt)?;
    start_attempt(
        transaction,
        cursor,
        &mut step,
        &command.task_id,
        scope.now_micros,
    )?;
    if let Some(invocation) = step.invocation.as_mut() {
        invocation.snapshot = Some(snapshot);
    }
    cursor.rows.put_step_execution(transaction, &step);
    Ok(StepCommandOutcome::Applied(ok_reply()))
}

pub(crate) fn apply_attempt_heartbeat(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    command: &StepAttemptHeartbeatCommand,
) -> Result<StepCommandOutcome, ApplyError> {
    let Some(mut step) = load_invocation(
        transaction,
        cursor,
        command.invocation.as_ref(),
        command.attempt,
        StepInvocationPhase::Running,
    )?
    else {
        return Ok(StepCommandOutcome::Ignored(stale_reply(
            "the attempt is no longer running",
        )));
    };
    let Some(invocation) = step.invocation.as_mut() else {
        return Ok(StepCommandOutcome::Ignored(stale_reply(
            "the attempt is no longer running",
        )));
    };
    let holder = ValueHolder::HeartbeatCheckpoint {
        step_execution_id: step.step_execution_id.clone(),
        method: invocation.method,
    };
    if let Some(previous) = invocation.heartbeat_checkpoint.take() {
        release_stored_value(transaction, cursor.rows, &previous, &holder, cursor.seq)?;
    }
    if let Some(step_attempt_heartbeat_command::Checkpoint::Value(value)) = &command.checkpoint {
        let limits = cursor.value_limits();
        match store_value(
            transaction,
            cursor.rows,
            limits,
            &mut cursor.values,
            value,
            &holder,
        )? {
            Ok(stored) => invocation.heartbeat_checkpoint = Some(stored),
            Err(rejection) => return Ok(StepCommandOutcome::RejectedResult(rejection)),
        }
    }
    cursor.rows.put_step_execution(transaction, &step);
    Ok(StepCommandOutcome::Applied(ok_reply()))
}

pub(crate) fn apply_attempt_failed(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    scope: &CommandScope<'_>,
    command: &StepAttemptFailedCommand,
) -> Result<StepCommandOutcome, ApplyError> {
    let Some(mut step) = load_invocation(
        transaction,
        cursor,
        command.invocation.as_ref(),
        command.attempt,
        StepInvocationPhase::Running,
    )?
    else {
        return Ok(StepCommandOutcome::Ignored(ok_reply()));
    };
    end_running_attempt(transaction, cursor, &mut step, InvocationEnd::Finished)?;
    let worker_error = command.worker_error.clone().unwrap_or_default();
    let failure = StepMethodFailure {
        backend_error: if command.server_detail.is_empty() {
            worker_error.detail.clone()
        } else {
            command.server_detail.clone()
        },
        details: Some(ServiceErrorResponse {
            detail: command.server_detail.clone(),
            sub_status: ErrorSubStatus::WorkerApiError as i32,
            original_worker_error_detail: worker_error.detail.clone(),
            original_worker_error_type: worker_error.error_type.clone(),
            original_worker_error_status: worker_error.grpc_status,
            original_worker_error_stack_trace: worker_error.stack_trace.clone(),
        }),
        attempt: i32::try_from(command.attempt).unwrap_or(i32::MAX),
        pull_routing: command.pull_routing.clone(),
    };
    let key = command.invocation.clone().unwrap_or_default();
    let method = StepMethod::try_from(key.method).unwrap_or(StepMethod::Execute);
    let options = step.options.clone().unwrap_or_default();
    let policy = match method {
        StepMethod::WaitFor => options.wait_for_retry_policy.as_ref(),
        StepMethod::Execute | StepMethod::Unspecified => options.execute_retry_policy.as_ref(),
    };
    let first_attempt_at = step
        .invocation
        .as_ref()
        .map_or(scope.now_micros, |invocation| {
            invocation.first_attempt_at_micros
        });
    let decision = decide_retry(
        policy,
        command.attempt,
        first_attempt_at,
        scope.now_micros,
        worker_error.retry_after_seconds,
        command.is_non_retryable,
    );
    match decision {
        RetryDecision::RetryAt { fires_at_micros } => {
            let next_attempt = command.attempt + 1;
            if let Some(invocation) = step.invocation.as_mut() {
                invocation.phase = StepInvocationPhase::Backoff as i32;
                invocation.attempt = next_attempt;
                invocation.backoff_fires_at_micros = fires_at_micros;
                invocation.last_failure = Some(failure);
                invocation.snapshot = None;
            }
            arm_retry_timer(transaction, cursor, &key, next_attempt, fires_at_micros);
            cursor.rows.put_step_execution(transaction, &step);
        }
        RetryDecision::Exhausted => {
            if let Some(invocation) = step.invocation.as_mut() {
                invocation.last_failure = Some(failure.clone());
                release_heartbeat_checkpoint(
                    transaction,
                    cursor,
                    &step.step_execution_id,
                    invocation,
                )?;
            }
            history_events::record_method_failed(transaction, cursor, &step, method, &failure)?;
            step.phase = StepExecutionPhase::Failed as i32;
            step.completed_seq = cursor.seq;
            step.invocation = None;
            cursor.rows.put_step_execution(transaction, &step);
            let message = format!(
                "Step execution {} failed after {} attempts: {}",
                step.step_execution_id, command.attempt, failure.backend_error
            );
            crate::command_handlers::flow_close::close_flow(
                transaction,
                cursor,
                scope,
                crate::command_handlers::flow_close::FlowClosing::failed(
                    FlowErrorType::WorkerApiFail,
                    message,
                ),
            )?;
        }
    }
    Ok(StepCommandOutcome::Applied(ok_reply()))
}

fn arm_retry_timer(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    key: &StepInvocationKey,
    next_attempt: u32,
    fires_at_micros: i64,
) {
    let timer = TimerId {
        run_attempt: cursor.attempt(),
        kind: Some(timer_id::Kind::RetryBackoff(RetryBackoffTimerId {
            invocation: Some(key.clone()),
            next_attempt,
        })),
    };
    arm_timer(transaction, cursor, &timer, fires_at_micros);
}

/// Writes a timer row and its partition index entry.
pub(crate) fn arm_timer(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    timer: &TimerId,
    fires_at_micros: i64,
) {
    let encoded = timer.encode_to_vec();
    let flow_hash = cursor.rows.flow_hash;
    let run_id = cursor.run_id();
    let partition = transaction.context().partition;
    transaction.put_message(
        ColumnFamily::Flow,
        timer_key(flow_hash, run_id, &encoded),
        &TimerRecord {
            timer_id: Some(timer.clone()),
            fire_at_micros: fires_at_micros,
        },
    );
    transaction.put(
        ColumnFamily::PartitionIndex,
        timer_index_key(partition, fires_at_micros, flow_hash, run_id, &encoded),
        Vec::new(),
    );
    cursor.add_effect(ApplyEffect::TimerArmed {
        fire_at_micros: fires_at_micros,
    });
}

/// Removes a timer row and its index entry; returns the record when it existed.
pub(crate) fn disarm_timer(
    transaction: &mut FlowTransaction<'_>,
    cursor: &RunCursor,
    timer: &TimerId,
) -> Result<Option<TimerRecord>, ApplyError> {
    let encoded = timer.encode_to_vec();
    let flow_hash = cursor.rows.flow_hash;
    let run_id = cursor.run_id();
    let key = timer_key(flow_hash, run_id, &encoded);
    let record: Option<TimerRecord> = transaction.get_message(ColumnFamily::Flow, &key, "timer")?;
    if let Some(record) = &record {
        transaction.delete(ColumnFamily::Flow, key);
        let partition = transaction.context().partition;
        transaction.delete(
            ColumnFamily::PartitionIndex,
            timer_index_key(
                partition,
                record.fire_at_micros,
                flow_hash,
                run_id,
                &encoded,
            ),
        );
    }
    Ok(record)
}

/// A retry backoff timer fired: the next attempt becomes pending and queues for its locks.
pub(crate) fn apply_retry_backoff_fired(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    timer: &RetryBackoffTimerId,
) -> Result<(), ApplyError> {
    let Some(key) = &timer.invocation else {
        return Ok(());
    };
    let Some(mut step) = cursor
        .rows
        .step_execution(transaction, &key.step_execution_id)?
    else {
        return Ok(());
    };
    let is_waiting_for_this_timer = step.invocation.as_ref().is_some_and(|invocation| {
        invocation.method == key.method
            && invocation.attempt == timer.next_attempt
            && invocation.phase == StepInvocationPhase::Backoff as i32
    });
    if !is_waiting_for_this_timer {
        return Ok(());
    }
    let method = StepMethod::try_from(key.method).unwrap_or(StepMethod::Execute);
    let first_attempt_at = step
        .invocation
        .as_ref()
        .map_or(0, |invocation| invocation.first_attempt_at_micros);
    make_invocation_pending(
        transaction,
        cursor,
        &mut step,
        method,
        timer.next_attempt,
        first_attempt_at,
    )?;
    cursor.rows.put_step_execution(transaction, &step);
    Ok(())
}

pub(crate) fn apply_method_completed(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    scope: &CommandScope<'_>,
    command: &StepMethodCompletedCommand,
) -> Result<StepCommandOutcome, ApplyError> {
    let Some(mut step) = load_invocation(
        transaction,
        cursor,
        command.invocation.as_ref(),
        command.attempt,
        StepInvocationPhase::Running,
    )?
    else {
        return Ok(StepCommandOutcome::Ignored(ok_reply()));
    };
    if !command.app_worker_id.is_empty() {
        cursor.record.sticky_app_worker_id = command.app_worker_id.clone();
    }
    history_events::record_method_completed(transaction, cursor, &step, command)?;
    end_running_attempt(transaction, cursor, &mut step, InvocationEnd::Finished)?;
    if let Some(invocation) = step.invocation.as_mut()
        && let Some(checkpoint) = invocation.heartbeat_checkpoint.take()
    {
        let holder = ValueHolder::HeartbeatCheckpoint {
            step_execution_id: step.step_execution_id.clone(),
            method: invocation.method,
        };
        release_stored_value(transaction, cursor.rows, &checkpoint, &holder, cursor.seq)?;
    }
    step.invocation = None;
    let outcome = match &command.response {
        Some(step_method_completed_command::Response::WaitFor(response)) => {
            let effects = crate::command_handlers::flow_data::MethodEffects {
                writes: &response.upsert_attributes,
                deletions: &response.delete_from_channel,
                publications: &response.publish_to_channel,
                locals: &response.upsert_step_exe_locals,
                permission_mappings: response.action_permission_mappings.as_ref(),
                hard_delete_objects: &command.hard_delete_objects,
            };
            if let Err(rejection) = crate::command_handlers::flow_data::apply_method_effects(
                transaction,
                cursor,
                &mut step,
                &effects,
            )? {
                return Ok(StepCommandOutcome::RejectedResult(rejection));
            }
            wait_conditions::register_wait(
                transaction,
                cursor,
                scope,
                &mut step,
                response.waiting_condition.as_ref(),
            )?
        }
        Some(step_method_completed_command::Response::Execute(response)) => {
            let effects = crate::command_handlers::flow_data::MethodEffects {
                writes: &response.upsert_attributes,
                deletions: &response.delete_from_channel,
                publications: &response.publish_to_channel,
                locals: &response.upsert_step_exe_locals,
                permission_mappings: response.action_permission_mappings.as_ref(),
                hard_delete_objects: &command.hard_delete_objects,
            };
            if let Err(rejection) = crate::command_handlers::flow_data::apply_method_effects(
                transaction,
                cursor,
                &mut step,
                &effects,
            )? {
                return Ok(StepCommandOutcome::RejectedResult(rejection));
            }
            apply_step_decision(
                transaction,
                cursor,
                scope,
                &mut step,
                response.step_decision.as_ref(),
            )?
        }
        None => Ok(()),
    };
    if let Err(rejection) = outcome {
        return Ok(StepCommandOutcome::RejectedResult(rejection));
    }
    Ok(StepCommandOutcome::Applied(ok_reply()))
}
