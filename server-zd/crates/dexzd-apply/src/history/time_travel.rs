// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! TimeTravel (plan section 8.4): forks attempt n+1 from a point and reapplies later input.

use std::collections::BTreeSet;

use dexzd_keyspace::row_keys::{active_work_index_key, timer_index_key};
use dexzd_keyspace::{ColumnFamily, RunTable};
use dexzd_protocol::dex::{
    ErrorSubStatus, FlowStatus, TimeTravelForkHistoryEvent, flow_history_event,
};
use dexzd_protocol::dexzd::v1::{
    AttemptRecord, CommandReply, FlowCommand, RetentionTimerId, RunRecord, StepExecutionRecord,
    StepInvocationKey, StepInvocationPhase, TimeTravelCommand, TimeTravelReply, TimerId,
    TimerRecord, command_reply, flow_command::Command, timer_id,
};
use prost::Message;

use crate::command_handlers::command_replies::{error_reply, grpc_code};
use crate::command_handlers::flow_close::update_directory_status;
use crate::command_handlers::run_cursor::{HistoryKind, RunCursor};
use crate::command_handlers::{CommandScope, HandlerResult};
use crate::history::attempt_scope::{
    AttemptRow, decode_timer_id, hex, holder_rows, read_attempt_rows, run_status_of, set_run_status,
};
use crate::history::lineage::{Lineage, history_records};
use crate::history::replay_state::{TransactionReader, absorb_writes};
use crate::history::reset_target::resolve_target;
use crate::history::state_rebuild::rebuild_attempt_state;
use crate::state_access::{FlowTransaction, read_flow_directory};
use crate::steps::{arm_timer, disarm_timer};
use crate::values::ValueIdAllocator;
use crate::{ApplyEffect, ApplyError, BehaviorVersion, WaitableChange};

pub(crate) fn apply_time_travel(
    transaction: &mut FlowTransaction<'_>,
    mut cursor: RunCursor,
    scope: &CommandScope<'_>,
    command: &TimeTravelCommand,
) -> Result<HandlerResult, ApplyError> {
    let request = command.request.clone().unwrap_or_default();
    let previous_attempt = cursor.attempt();
    let selected_attempt = match u32::try_from(request.run_attempt) {
        Ok(0) => previous_attempt,
        Ok(attempt) if attempt <= previous_attempt => attempt,
        _ => {
            return Ok(HandlerResult::Rejected(error_reply(
                grpc_code::INVALID_ARGUMENT,
                ErrorSubStatus::Uncategorized,
                format!(
                    "run attempt {} does not exist; the run has attempts 1 to {previous_attempt}",
                    request.run_attempt
                ),
            )));
        }
    };
    let directory = read_flow_directory(transaction, scope.tenant_id, scope.flow_id())?;
    let is_latest_run = directory
        .runs
        .last()
        .is_some_and(|run| run.run_id == cursor.run_id().as_bytes());
    if !is_latest_run {
        // ID reuse decisions read only the latest run, so an older run must stay closed.
        return Ok(HandlerResult::Rejected(error_reply(
            grpc_code::FAILED_PRECONDITION,
            ErrorSubStatus::Uncategorized,
            format!(
                "only the latest run of Flow {} can time travel",
                scope.flow_id()
            ),
        )));
    }
    let lineage = Lineage::of_attempt(transaction, cursor.rows, selected_attempt)?;
    let target = match resolve_target(transaction, &cursor, scope.now_micros, &request, &lineage)? {
        Ok(target) => target,
        Err(reply) => return Ok(HandlerResult::Rejected(reply)),
    };
    let is_reapplying = !request.skip_writes_reapply;
    if is_reapplying && cursor.record.max_pruned_external_input_seq > target.point.seq {
        return Ok(HandlerResult::Rejected(error_reply(
            grpc_code::FAILED_PRECONDITION,
            ErrorSubStatus::Uncategorized,
            format!(
                "external input after the target point left the time travel window (run seq {}), \
                 so it cannot be reapplied; set skip_writes_reapply to travel without reapplying \
                 writes",
                cursor.record.max_pruned_external_input_seq
            ),
        )));
    }
    let reapplied_inputs = if is_reapplying {
        collect_external_inputs(transaction, &cursor, &lineage, target.point.seq)?
    } else {
        Vec::new()
    };
    let rebuilt = rebuild_attempt_state(transaction, cursor.rows, &cursor.record, target.point)?;
    let previous_rows = read_attempt_rows(transaction, cursor.rows)?;
    let previous_status = run_status_of(&cursor.record);
    let was_running = cursor.record.status == FlowStatus::Running as i32;
    cancel_running_invocations(&mut cursor, &previous_rows)?;
    let new_attempt = previous_attempt + 1;
    cursor.rows.put_attempt_record(
        transaction,
        &AttemptRecord {
            attempt: new_attempt,
            parent_attempt: target.point.attempt,
            parent_seq: target.point.seq,
            start_seq: cursor.seq,
            created_at_micros: scope.now_micros,
            reason: request.reason.clone(),
            checkpoint_seqs: Vec::new(),
        },
    );
    cursor.record.current_attempt = new_attempt;
    set_run_status(&mut cursor.record, &rebuilt.run_status);
    cursor.starts_attempt = true;
    replace_attempt_rows(
        transaction,
        &mut cursor,
        scope,
        &previous_rows,
        rebuilt.rows,
    )?;
    update_run_lifecycle(transaction, &mut cursor, scope, was_running)?;
    let restored_rows = read_attempt_rows(transaction, cursor.rows)?;
    crate::values::rewrite_holders_for_time_travel(
        transaction,
        &cursor,
        &holder_rows(&previous_rows, &previous_status),
        &holder_rows(&restored_rows, &run_status_of(&cursor.record)),
        cursor.seq,
    )?;
    apply_received_child_results(transaction, &mut cursor, scope)?;
    let reapplied_events =
        reapply_external_inputs(transaction, &mut cursor, scope, &reapplied_inputs)?;
    let final_rows = read_attempt_rows(transaction, cursor.rows)?;
    dispatch_restored_invocations(&mut cursor, &final_rows)?;
    cursor.add_event(flow_history_event::Payload::TimeTravelFork(
        TimeTravelForkHistoryEvent {
            previous_run_id: cursor.run_id().to_string(),
            previous_run_attempt: i32::try_from(previous_attempt).unwrap_or(i32::MAX),
            target_run_attempt: i32::try_from(target.point.attempt).unwrap_or(i32::MAX),
            target_event_id: target.last_event_id,
        },
    ));
    for event in reapplied_events {
        cursor.add_event(event);
    }
    cursor.wake(WaitableChange::RunAttemptChanged);
    let reply = CommandReply {
        reply: Some(command_reply::Reply::TimeTravel(TimeTravelReply {
            run_attempt: new_attempt,
        })),
    };
    let kind = if cursor.reapplied_inputs.is_empty() {
        HistoryKind::SEMANTIC
    } else {
        HistoryKind::EXTERNAL_INPUT
    };
    let output = cursor.finish(transaction, scope, reply, kind)?;
    Ok(HandlerResult::Applied(output))
}

/// External input of the lineage after `target_seq`, in seq order. StopFlow is never input.
///
/// A TimeTravel record contributes the input it reapplied.
fn collect_external_inputs(
    transaction: &FlowTransaction<'_>,
    cursor: &RunCursor,
    lineage: &Lineage,
    target_seq: u64,
) -> Result<Vec<FlowCommand>, ApplyError> {
    let mut inputs = Vec::new();
    for record in history_records(transaction, cursor.rows, target_seq + 1, u64::MAX)? {
        if !record.is_external_input || !lineage.contains(record.attempt, record.seq) {
            continue;
        }
        let Some(command) = record.command else {
            continue;
        };
        if matches!(command.command, Some(Command::TimeTravel(_))) {
            inputs.extend(record.reapplied_inputs);
        } else if is_reapplied_input(&command) {
            inputs.push(command);
        }
    }
    Ok(inputs)
}

fn is_reapplied_input(command: &FlowCommand) -> bool {
    matches!(
        command.command,
        Some(
            Command::AttributesSet(_)
                | Command::ChannelPublish(_)
                | Command::ChannelMessageDelete(_)
                | Command::RpcCommitted(_)
                | Command::TimerSkip(_)
        )
    )
}

/// Tells Workers to stop every attempt the previous attempt state was running.
fn cancel_running_invocations(
    cursor: &mut RunCursor,
    previous_rows: &[AttemptRow],
) -> Result<(), ApplyError> {
    for step in step_executions(previous_rows)? {
        if let Some(invocation) = &step.invocation
            && invocation.phase == StepInvocationPhase::Running as i32
        {
            let run_id = cursor.run_id();
            cursor.add_effect(ApplyEffect::InvocationCanceled {
                run_id,
                task_id: invocation.task_id.clone(),
            });
        }
    }
    Ok(())
}

/// Replaces attempt rows; hard-delete Attributes stay current.
///
/// Restored running attempts get new dispatch IDs, so old cancellations miss them.
fn replace_attempt_rows(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    scope: &CommandScope<'_>,
    previous_rows: &[AttemptRow],
    rebuilt_rows: Vec<AttemptRow>,
) -> Result<(), ApplyError> {
    let rows = cursor.rows;
    let partition = transaction.context().partition;
    let mut hard_delete_keys = BTreeSet::new();
    for row in previous_rows {
        if row.is_hard_delete_attribute()? {
            hard_delete_keys.insert(row.key_suffix.clone());
            continue;
        }
        if row.table == RunTable::Timer {
            let timer = decode_timer_record(row)?;
            transaction.delete(
                ColumnFamily::PartitionIndex,
                timer_index_key(
                    partition,
                    timer.fire_at_micros,
                    rows.flow_hash,
                    rows.run_id,
                    &row.key_suffix,
                ),
            );
        }
        transaction.delete(ColumnFamily::Flow, row.key(rows));
    }
    let mut restored_running_count = 0_u32;
    for row in rebuilt_rows {
        match row.table {
            RunTable::Attribute => {
                if row.is_hard_delete_attribute()? || hard_delete_keys.contains(&row.key_suffix) {
                    continue;
                }
                transaction.put(ColumnFamily::Flow, row.key(rows), row.value);
            }
            RunTable::Timer => {
                let timer = decode_timer_record(&row)?;
                let mut timer_id = match timer.timer_id {
                    Some(timer_id) => timer_id,
                    None => decode_timer_id(&row.key_suffix)?,
                };
                timer_id.run_attempt = cursor.attempt();
                arm_timer(transaction, cursor, &timer_id, timer.fire_at_micros);
            }
            RunTable::StepExecution => {
                let mut step = decode_step_execution(&row)?;
                if let Some(invocation) = step.invocation.as_mut()
                    && invocation.phase == StepInvocationPhase::Running as i32
                {
                    invocation.task_id =
                        restored_task_id(&scope.command.command_id, restored_running_count);
                    restored_running_count += 1;
                }
                cursor.rows.put_step_execution(transaction, &step);
            }
            _ => transaction.put(ColumnFamily::Flow, row.key(rows), row.value),
        }
    }
    Ok(())
}

/// A dispatch ID derived from the TimeTravel command ID and the restored attempt's index.
fn restored_task_id(command_id: &[u8], index: u32) -> Vec<u8> {
    let mut task_id = vec![0; 16];
    let prefix_length = command_id.len().min(12);
    task_id[..prefix_length].copy_from_slice(&command_id[..prefix_length]);
    task_id[12..].copy_from_slice(&index.to_be_bytes());
    task_id
}

/// Reopens a closed run or closes a running one when the restored status differs.
fn update_run_lifecycle(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    scope: &CommandScope<'_>,
    was_running: bool,
) -> Result<(), ApplyError> {
    let is_running = cursor.record.status == FlowStatus::Running as i32;
    let partition = transaction.context().partition;
    let active_work_key = active_work_index_key(partition, cursor.rows.flow_hash, cursor.run_id());
    let retention_timer = TimerId {
        run_attempt: 0,
        kind: Some(timer_id::Kind::Retention(RetentionTimerId {})),
    };
    match (was_running, is_running) {
        (true, true) => return Ok(()),
        (false, true) => {
            disarm_timer(transaction, cursor, &retention_timer)?;
            cursor.record.retention_deadline_micros = 0;
            transaction.put(ColumnFamily::PartitionIndex, active_work_key, Vec::new());
        }
        (true, false) => {
            let settings = cursor.record.settings.unwrap_or_default();
            let retention_deadline = scope
                .now_micros
                .saturating_add(settings.retention_after_close_micros);
            cursor.record.retention_deadline_micros = retention_deadline;
            arm_timer(transaction, cursor, &retention_timer, retention_deadline);
            transaction.delete(ColumnFamily::PartitionIndex, active_work_key);
            cursor.wake(WaitableChange::FlowClosed);
        }
        (false, false) => {}
    }
    update_directory_status(transaction, cursor, scope)
}

/// Completes restored SubFlow conditions from received child results (C15); tasks A3 and A6.
fn apply_received_child_results(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    scope: &CommandScope<'_>,
) -> Result<(), ApplyError> {
    let _ = (transaction, cursor, scope);
    Ok(())
}

/// Re-dispatches the restored calls: running attempts with their stored snapshots and attempt
/// numbers, and pending calls.
fn dispatch_restored_invocations(
    cursor: &mut RunCursor,
    restored_rows: &[AttemptRow],
) -> Result<(), ApplyError> {
    let run_id = cursor.run_id();
    let run_attempt = cursor.attempt();
    for step in step_executions(restored_rows)? {
        let Some(invocation) = &step.invocation else {
            continue;
        };
        let key = StepInvocationKey {
            step_execution_id: step.step_execution_id.clone(),
            method: invocation.method,
        };
        let phase = StepInvocationPhase::try_from(invocation.phase)
            .unwrap_or(StepInvocationPhase::Unspecified);
        match phase {
            StepInvocationPhase::Pending => cursor.add_effect(ApplyEffect::InvocationPending {
                run_id,
                run_attempt,
                invocation: key,
                attempt: invocation.attempt,
            }),
            StepInvocationPhase::Running => cursor.add_effect(ApplyEffect::InvocationStarted {
                run_id,
                run_attempt,
                invocation: key,
                attempt: invocation.attempt,
                task_id: invocation.task_id.clone(),
            }),
            StepInvocationPhase::Backoff | StepInvocationPhase::Unspecified => {}
        }
    }
    Ok(())
}

/// Reapplies input onto the restored state within the TimeTravel command, so its start
/// checkpoint includes it. Returns the reapplied input's history events.
fn reapply_external_inputs(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    scope: &CommandScope<'_>,
    inputs: &[FlowCommand],
) -> Result<Vec<flow_history_event::Payload>, ApplyError> {
    let mut events = Vec::new();
    if inputs.is_empty() {
        return Ok(events);
    }
    let behavior_version = BehaviorVersion::from_command(scope.command.behavior_version)?;
    let rows = cursor.rows;
    rows.put_run_record(transaction, &cursor.record);
    let mut values = std::mem::replace(&mut cursor.values, ValueIdAllocator::new(cursor.seq));
    for input in inputs {
        cursor.reapplied_inputs.push(input.clone());
        let record = required_run_record(transaction, cursor)?;
        // Client writes to a closed Flow are rejected, so a closed state skips the input.
        if record.status != FlowStatus::Running as i32 {
            continue;
        }
        let command = reapplied_command(input, scope);
        let Some(body) = &command.command else {
            return Err(ApplyError::InvariantViolation {
                detail: "a recorded external input has no command body".to_owned(),
            });
        };
        let mut input_scope = CommandScope::new(&command, behavior_version);
        input_scope.is_replay = true;
        let (writes, output) = {
            let reader = TransactionReader::new(transaction);
            let mut input_transaction = FlowTransaction::new(&reader, transaction.context());
            let input_cursor =
                RunCursor::for_reapplied_input(rows, record, cursor.seq, values.clone());
            match crate::command_handlers::apply_run_command(
                &mut input_transaction,
                input_cursor,
                &input_scope,
                body,
            )? {
                HandlerResult::Applied(output) => (input_transaction.into_writes(), output),
                HandlerResult::Rejected(reply) if is_skippable_rejection(body, &reply) => continue,
                HandlerResult::Rejected(reply) => {
                    return Err(ApplyError::InvariantViolation {
                        detail: format!("time travel could not reapply recorded input: {reply:?}"),
                    });
                }
            }
        };
        absorb_writes(transaction, writes);
        let Some(replayed) = output.replayed else {
            return Err(ApplyError::InvariantViolation {
                detail: "a reapplied input recorded its own history".to_owned(),
            });
        };
        values = replayed.values;
        events.extend(replayed.events);
        // Dispatch follows the final restored state instead.
        for effect in output.effects {
            match effect {
                ApplyEffect::InvocationPending {
                    invocation,
                    attempt: 1,
                    ..
                } => cursor.reapplied_pending_invocations.push(invocation),
                ApplyEffect::InvocationPending { .. } | ApplyEffect::InvocationStarted { .. } => {}
                other => cursor.effects.push(other),
            }
        }
    }
    cursor.record = required_run_record(transaction, cursor)?;
    cursor.values = values;
    Ok(events)
}

/// The best-effort skips of plan section 8.4: input the restored state no longer accepts.
fn is_skippable_rejection(body: &Command, reply: &CommandReply) -> bool {
    let Some(command_reply::Reply::Error(error)) = &reply.reply else {
        return false;
    };
    match body {
        Command::TimerSkip(_) => error.grpc_code == grpc_code::INVALID_ARGUMENT,
        Command::ChannelMessageDelete(_) => {
            error.grpc_code == grpc_code::NOT_FOUND
                && error.sub_status == ErrorSubStatus::ChannelMessageNotFound as i32
        }
        _ => false,
    }
}

fn required_run_record(
    transaction: &FlowTransaction<'_>,
    cursor: &RunCursor,
) -> Result<RunRecord, ApplyError> {
    cursor
        .rows
        .run_record(transaction)?
        .ok_or_else(|| ApplyError::InvariantViolation {
            detail: "a time traveling run has no run record".to_owned(),
        })
}

/// Clears request IDs, which the run-scoped deduplication table would otherwise answer.
fn reapplied_command(input: &FlowCommand, scope: &CommandScope<'_>) -> FlowCommand {
    let mut command = input.clone();
    command.proposed_at_micros = scope.now_micros;
    command.expected_run_attempt = 0;
    command.behavior_version = scope.command.behavior_version;
    match command.command.as_mut() {
        Some(Command::AttributesSet(set)) => {
            if let Some(request) = set.request.as_mut() {
                request.request_id.clear();
            }
        }
        Some(Command::ChannelMessageDelete(delete)) => {
            if let Some(request) = delete.request.as_mut() {
                request.request_id.clear();
            }
        }
        Some(Command::RpcCommitted(rpc)) => {
            if let Some(request) = rpc.request.as_mut() {
                request.request_id.clear();
            }
        }
        _ => {}
    }
    command
}

fn step_executions(attempt_rows: &[AttemptRow]) -> Result<Vec<StepExecutionRecord>, ApplyError> {
    attempt_rows
        .iter()
        .filter(|row| row.table == RunTable::StepExecution)
        .map(decode_step_execution)
        .collect()
}

fn decode_step_execution(row: &AttemptRow) -> Result<StepExecutionRecord, ApplyError> {
    StepExecutionRecord::decode(row.value.as_slice()).map_err(|error| ApplyError::CorruptState {
        table: "step execution",
        key: hex(&row.key_suffix),
        detail: error.to_string(),
    })
}

fn decode_timer_record(row: &AttemptRow) -> Result<TimerRecord, ApplyError> {
    TimerRecord::decode(row.value.as_slice()).map_err(|error| ApplyError::CorruptState {
        table: "timer",
        key: hex(&row.key_suffix),
        detail: error.to_string(),
    })
}
