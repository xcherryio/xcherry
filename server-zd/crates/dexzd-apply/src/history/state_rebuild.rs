// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Rebuilds S' (plan section 8.4 step 2): a checkpoint, then the attempt's commands replayed.

use dexzd_keyspace::ColumnFamily;
use dexzd_keyspace::row_keys::checkpoint_key;
use dexzd_protocol::dexzd::v1::{
    CheckpointRecord, CheckpointRunStatus, FlowCommand, HistoryRecord, RunRecord,
    TimersFiredCommand, flow_command::Command,
};

use crate::history::attempt_scope::{AttemptRow, read_attempt_rows, run_status_of, set_run_status};
use crate::history::lineage::{history_records, required_attempt_record};
use crate::history::replay_state::{ReplayState, TransactionReader};
use crate::state_access::{FlowTransaction, RunRows};
use crate::{ApplyContext, ApplyError, BehaviorVersion};

/// A point of a run: the state right after the command at `seq`, in attempt `attempt`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HistoryPoint {
    pub(crate) attempt: u32,
    pub(crate) seq: u64,
}

/// The attempt-scoped state at a point.
pub(crate) struct RebuiltAttempt {
    pub(crate) rows: Vec<AttemptRow>,
    pub(crate) run_status: CheckpointRunStatus,
}

/// Rebuilds the attempt state at `point` on top of what `transaction` reads.
pub(crate) fn rebuild_attempt_state(
    transaction: &FlowTransaction<'_>,
    rows: RunRows,
    record: &RunRecord,
    point: HistoryPoint,
) -> Result<RebuiltAttempt, ApplyError> {
    let checkpoint_seq = required_attempt_record(transaction, rows, point.attempt)?
        .checkpoint_seqs
        .iter()
        .copied()
        .filter(|seq| *seq <= point.seq)
        .max()
        .ok_or_else(|| ApplyError::InvariantViolation {
            detail: format!(
                "run attempt {} has no checkpoint at or before seq {}",
                point.attempt, point.seq
            ),
        })?;
    let checkpoint: CheckpointRecord = transaction
        .get_message(
            ColumnFamily::History,
            &checkpoint_key(rows.flow_hash, rows.run_id, point.attempt, checkpoint_seq),
            "checkpoint",
        )?
        .ok_or_else(|| ApplyError::InvariantViolation {
            detail: format!(
                "checkpoint ({}, {checkpoint_seq}) is listed but missing",
                point.attempt
            ),
        })?;
    let checkpoint_rows = checkpoint
        .rows
        .iter()
        .map(AttemptRow::from_checkpoint_row)
        .collect::<Result<Vec<_>, _>>()?;
    let mut replay_record = record.clone();
    set_run_status(
        &mut replay_record,
        &checkpoint.run_status.unwrap_or_default(),
    );
    replay_record.current_attempt = point.attempt;
    replay_record.is_purging = false;
    let base = TransactionReader::new(transaction);
    let mut replay_state = ReplayState::new(&base, rows, &checkpoint_rows, &replay_record);
    let context = transaction.context();
    for history_record in history_records(transaction, rows, checkpoint_seq + 1, point.seq)? {
        if history_record.attempt != point.attempt {
            continue;
        }
        replay_command(&mut replay_state, &context, &history_record)?;
    }
    let replay_transaction = FlowTransaction::new(&replay_state, context);
    let rebuilt_rows = read_attempt_rows(&replay_transaction, rows)?;
    let run_status = run_status_of(&replay_state.run_record()?);
    Ok(RebuiltAttempt {
        rows: rebuilt_rows,
        run_status,
    })
}

fn replay_command(
    replay_state: &mut ReplayState<'_>,
    context: &ApplyContext,
    history_record: &HistoryRecord,
) -> Result<(), ApplyError> {
    let Some(command) = replayed_command(history_record)? else {
        return Ok(());
    };
    replay_state.prepare_command(history_record.seq)?;
    let behavior_version = BehaviorVersion::from_command(command.behavior_version)?;
    let replayed = crate::command_handlers::apply_replayed_command(
        context,
        replay_state,
        &command,
        behavior_version,
    )?;
    let Some(writes) = replayed else {
        return Err(ApplyError::InvariantViolation {
            detail: format!(
                "replaying the command at seq {} was rejected",
                history_record.seq
            ),
        });
    };
    replay_state.commit(writes)
}

/// The command to replay for a history record, or `None` when it changes only run-scoped state.
fn replayed_command(history_record: &HistoryRecord) -> Result<Option<FlowCommand>, ApplyError> {
    let Some(command) = &history_record.command else {
        return Err(ApplyError::InvariantViolation {
            detail: format!("history record {} has no command", history_record.seq),
        });
    };
    let Some(body) = &command.command else {
        return Err(ApplyError::InvariantViolation {
            detail: format!("history record {} has no command body", history_record.seq),
        });
    };
    let replayed_body = match body {
        Command::TimersFired(fired) => {
            let timers: Vec<_> = fired
                .timers
                .iter()
                .filter(|timer| timer.run_attempt != 0)
                .cloned()
                .collect();
            if timers.is_empty() {
                return Ok(None);
            }
            Command::TimersFired(TimersFiredCommand { timers })
        }
        Command::FlowConfigUpdate(_)
        | Command::OutboxAck(_)
        | Command::HardDeleteObjectsDeleted(_)
        | Command::RunPurge(_) => return Ok(None),
        // Attempts start at a checkpoint after these, and only the latest run travels.
        Command::FlowStart(_) | Command::TimeTravel(_) => {
            return Err(ApplyError::InvariantViolation {
                detail: format!(
                    "history record {} starts a run or attempt inside the replayed range",
                    history_record.seq
                ),
            });
        }
        other => other.clone(),
    };
    Ok(Some(FlowCommand {
        command: Some(replayed_body),
        ..command.clone()
    }))
}
