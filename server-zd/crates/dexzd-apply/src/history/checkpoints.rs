// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Per-command history bookkeeping (plan section 8.4): which Step method calls became pending,
//! when to write an attempt checkpoint, and the window.

use dexzd_keyspace::row_keys::checkpoint_key;
use dexzd_keyspace::{ColumnFamily, parse_run_key};
use dexzd_protocol::dexzd::v1::{CheckpointRecord, HistoryStepMethodPending};

use crate::command_handlers::run_cursor::{HistoryKind, RunCursor};
use crate::history::attempt_scope::{ATTEMPT_SCOPED_TABLES, read_attempt_rows, run_status_of};
use crate::history::window::advance_window_if_full;
use crate::state_access::FlowTransaction;
use crate::{ApplyEffect, ApplyError};

/// Returns the method calls whose first attempt the command made pending, in effect order.
/// The fork command of a new attempt re-dispatches restored calls, which are not new.
pub(crate) fn pending_step_methods(
    transaction: &FlowTransaction<'_>,
    cursor: &RunCursor,
) -> Result<Vec<HistoryStepMethodPending>, ApplyError> {
    if cursor.starts_attempt {
        return Ok(Vec::new());
    }
    let mut pending: Vec<HistoryStepMethodPending> = Vec::new();
    for effect in &cursor.effects {
        let ApplyEffect::InvocationPending {
            invocation,
            attempt: 1,
            ..
        } = effect
        else {
            continue;
        };
        let is_recorded = pending.iter().any(|recorded| {
            recorded.step_execution_id == invocation.step_execution_id
                && recorded.method == invocation.method
        });
        if is_recorded {
            continue;
        }
        let Some(step) = cursor
            .rows
            .step_execution(transaction, &invocation.step_execution_id)?
        else {
            continue;
        };
        pending.push(HistoryStepMethodPending {
            step_execution_id: invocation.step_execution_id.clone(),
            step_type: step.step_type,
            method: invocation.method,
            is_step_created: step.created_seq == cursor.seq,
        });
    }
    Ok(pending)
}

/// Runs after the command's history record is buffered: writes a checkpoint when due and
/// prunes the window after semantic commands.
pub(crate) fn after_command(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    kind: HistoryKind,
) -> Result<(), ApplyError> {
    let written_bytes = attempt_write_bytes(transaction, cursor);
    let record = &mut cursor.record;
    record.written_bytes_since_checkpoint = record
        .written_bytes_since_checkpoint
        .saturating_add(written_bytes);
    let settings = record.settings.unwrap_or_default();
    let is_attempt_start = cursor.starts_attempt || record.last_checkpoint_seq == 0;
    let has_enough_commands = settings.checkpoint_max_commands > 0
        && record.commands_since_checkpoint >= settings.checkpoint_max_commands;
    let written_threshold = settings
        .checkpoint_min_written_bytes
        .max(record.last_checkpoint_state_bytes);
    let has_enough_bytes = record.written_bytes_since_checkpoint >= written_threshold;
    if is_attempt_start || has_enough_commands || has_enough_bytes {
        write_checkpoint(transaction, cursor)?;
    }
    if kind.is_semantic {
        advance_window_if_full(transaction, cursor)?;
    }
    Ok(())
}

/// Writes the checkpoint of the current attempt at the command's seq.
fn write_checkpoint(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
) -> Result<(), ApplyError> {
    let attempt = cursor.attempt();
    let attempt_rows = read_attempt_rows(transaction, cursor.rows)?;
    let state_bytes = attempt_rows
        .iter()
        .map(|row| row.encoded_bytes())
        .fold(0_u64, u64::saturating_add);
    let checkpoint = CheckpointRecord {
        attempt,
        seq: cursor.seq,
        rows: attempt_rows
            .iter()
            .map(|row| row.to_checkpoint_row())
            .collect(),
        run_status: Some(run_status_of(&cursor.record)),
    };
    let flow_hash = cursor.rows.flow_hash;
    let run_id = cursor.run_id();
    transaction.put_message(
        ColumnFamily::History,
        checkpoint_key(flow_hash, run_id, attempt, cursor.seq),
        &checkpoint,
    );
    let Some(mut attempt_record) = cursor.rows.attempt_record(transaction, attempt)? else {
        return Err(ApplyError::InvariantViolation {
            detail: format!("run attempt {attempt} has no attempt record"),
        });
    };
    attempt_record.checkpoint_seqs.push(cursor.seq);
    cursor.rows.put_attempt_record(transaction, &attempt_record);
    let record = &mut cursor.record;
    record.last_checkpoint_seq = cursor.seq;
    record.last_checkpoint_state_bytes = state_bytes;
    record.written_bytes_since_checkpoint = 0;
    record.commands_since_checkpoint = 0;
    Ok(())
}

/// Bytes of attempt-scoped rows this command wrote to the run.
fn attempt_write_bytes(transaction: &FlowTransaction<'_>, cursor: &RunCursor) -> u64 {
    transaction
        .buffered_puts(ColumnFamily::Flow)
        .filter_map(|(key, value)| {
            let parsed = parse_run_key(key).ok()?;
            let is_attempt_row =
                parsed.run_id == cursor.run_id() && ATTEMPT_SCOPED_TABLES.contains(&parsed.table);
            is_attempt_row.then(|| u64::try_from(key.len() + value.len()).unwrap_or(u64::MAX))
        })
        .fold(0, u64::saturating_add)
}
