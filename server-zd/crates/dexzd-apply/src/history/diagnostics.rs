// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Read-only state hashes for debug and test verification; apply never calls these helpers.

use dexzd_keyspace::{RunId, RunTable};
use dexzd_protocol::dexzd::v1::{
    FlowCommand, StepExecutionRecord, TimerRecord, flow_command::Command,
};
use prost::Message;

use super::attempt_scope::{AttemptRow, decode_timer_id, read_attempt_rows, run_status_of};
use super::lineage::{Lineage, genesis_seq};
use super::reset_target::resolve_target;
use super::state_rebuild::rebuild_attempt_state;
use crate::command_handlers::CommandScope;
use crate::command_handlers::run_cursor::RunCursor;
use crate::state_access::{FlowTransaction, RunRows};
use crate::{ApplyContext, ApplyError, BehaviorVersion, FlowStateReader};

const FNV_128_OFFSET_BASIS: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
const FNV_128_PRIME: u128 = 0x0000_0000_0100_0000_0000_0000_0000_013b;

/// Hash of the attempt state at one semantic history point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemanticPointHash {
    /// Run containing the point.
    pub run_id: RunId,
    /// Attempt containing the point.
    pub run_attempt: u32,
    /// Run sequence of the semantic point.
    pub run_seq: u64,
    /// Stable hash of the attempt's rows and status.
    pub state_hash: [u8; 16],
    /// Oldest retained semantic sequence for this run.
    pub window_start_seq: u64,
    /// Preserved start sequence of the run.
    pub genesis_seq: u64,
}

/// Hashes the command's staged semantic point; returns `None` when no matching point exists.
pub fn current_semantic_point_hash(
    context: &ApplyContext,
    state: &dyn FlowStateReader,
    run_id: RunId,
    command_id: &[u8],
) -> Result<Option<SemanticPointHash>, ApplyError> {
    let transaction = FlowTransaction::new(state, *context);
    let rows = RunRows::new(context.flow_hash, run_id);
    let Some(record) = rows.run_record(&transaction)? else {
        return Ok(None);
    };
    let Some(run_seq) = record.next_run_seq.checked_sub(1) else {
        return Ok(None);
    };
    let Some(history) = rows.history_record(&transaction, run_seq)? else {
        return Ok(None);
    };
    if !history.is_semantic
        || history
            .command
            .as_ref()
            .is_none_or(|command| command.command_id != command_id)
    {
        return Ok(None);
    }
    let attempt_rows = read_attempt_rows(&transaction, rows)?;
    let encoded_state = encode_attempt_state(&attempt_rows, &run_status_of(&record))?;
    Ok(Some(SemanticPointHash {
        run_id,
        run_attempt: record.current_attempt,
        run_seq,
        state_hash: hash_attempt_state(&encoded_state),
        window_start_seq: record.window_start_seq,
        genesis_seq: genesis_seq(&transaction, rows)?,
    }))
}

/// Hashes a TimeTravel target's reconstructed state; returns `None` when no valid target exists.
pub fn time_travel_target_hash(
    context: &ApplyContext,
    state: &dyn FlowStateReader,
    command: &FlowCommand,
) -> Result<Option<SemanticPointHash>, ApplyError> {
    let Some(Command::TimeTravel(time_travel)) = &command.command else {
        return Ok(None);
    };
    let transaction = FlowTransaction::new(state, *context);
    let scope = CommandScope::new(
        command,
        BehaviorVersion::from_command(command.behavior_version)?,
    );
    let Ok(cursor) = RunCursor::open(&transaction, &scope)? else {
        return Ok(None);
    };
    let request = time_travel.request.clone().unwrap_or_default();
    let selected_attempt = match u32::try_from(request.run_attempt) {
        Ok(0) => cursor.record.current_attempt,
        Ok(attempt) if attempt <= cursor.record.current_attempt => attempt,
        _ => return Ok(None),
    };
    let lineage = Lineage::of_attempt(&transaction, cursor.rows, selected_attempt)?;
    let Ok(target) = resolve_target(&transaction, &cursor, scope.now_micros, &request, &lineage)?
    else {
        return Ok(None);
    };
    let rebuilt = rebuild_attempt_state(&transaction, cursor.rows, &cursor.record, target.point)?;
    let encoded_state = encode_attempt_state(&rebuilt.rows, &rebuilt.run_status)?;
    Ok(Some(SemanticPointHash {
        run_id: cursor.rows.run_id,
        run_attempt: target.point.attempt,
        run_seq: target.point.seq,
        state_hash: hash_attempt_state(&encoded_state),
        window_start_seq: cursor.record.window_start_seq,
        genesis_seq: genesis_seq(&transaction, cursor.rows)?,
    }))
}

fn encode_attempt_state(
    rows: &[AttemptRow],
    status: &dexzd_protocol::dexzd::v1::CheckpointRunStatus,
) -> Result<Vec<u8>, ApplyError> {
    let mut encoded = Vec::new();
    append_bytes(&mut encoded, &status.encode_to_vec());
    let mut canonical_rows = Vec::with_capacity(rows.len());
    for row in rows {
        if row.is_hard_delete_attribute()? {
            continue;
        }
        let (key_suffix, value) = canonicalized_attempt_row(row)?;
        canonical_rows.push((row.table, key_suffix, value));
    }
    canonical_rows.sort_by(|first, second| (first.0, &first.1).cmp(&(second.0, &second.1)));
    for (table, key_suffix, value) in canonical_rows {
        encoded.push(table as u8);
        append_bytes(&mut encoded, &key_suffix);
        append_bytes(&mut encoded, &value);
    }
    Ok(encoded)
}

fn hash_attempt_state(encoded: &[u8]) -> [u8; 16] {
    let mut hash = FNV_128_OFFSET_BASIS;
    for byte in encoded {
        hash = (hash ^ u128::from(*byte)).wrapping_mul(FNV_128_PRIME);
    }
    hash.to_be_bytes()
}

fn canonicalized_attempt_row(row: &AttemptRow) -> Result<(Vec<u8>, Vec<u8>), ApplyError> {
    match row.table {
        RunTable::Timer => {
            let mut timer_id = decode_timer_id(&row.key_suffix)?;
            timer_id.run_attempt = 0;
            let mut timer = TimerRecord::decode(row.value.as_slice()).map_err(|error| {
                ApplyError::InvariantViolation {
                    detail: format!("diagnostic timer row does not decode: {error}"),
                }
            })?;
            if let Some(stored_id) = timer.timer_id.as_mut() {
                stored_id.run_attempt = 0;
            }
            Ok((timer_id.encode_to_vec(), timer.encode_to_vec()))
        }
        RunTable::StepExecution => {
            let mut step = StepExecutionRecord::decode(row.value.as_slice()).map_err(|error| {
                ApplyError::InvariantViolation {
                    detail: format!("diagnostic Step execution row does not decode: {error}"),
                }
            })?;
            if let Some(invocation) = step.invocation.as_mut() {
                invocation.task_id.clear();
            }
            Ok((row.key_suffix.clone(), step.encode_to_vec()))
        }
        _ => Ok((row.key_suffix.clone(), row.value.clone())),
    }
}

fn append_bytes(encoded: &mut Vec<u8>, bytes: &[u8]) {
    encoded.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
    encoded.extend_from_slice(bytes);
}
