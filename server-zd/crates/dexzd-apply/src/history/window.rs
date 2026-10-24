// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! The time travel window (plan section 8.4): the newest N semantic points, pruned M at a time.
//!
//! FlowStart and the genesis checkpoint are kept until the run is deleted (D12).

use std::collections::{BTreeMap, BTreeSet};

use dexzd_keyspace::row_keys::{
    checkpoint_key, history_record_key, history_row_metadata_key, prefix_range,
};
use dexzd_keyspace::{ColumnFamily, RunTable, run_table_prefix};
use dexzd_protocol::dexzd::v1::HistoryRowMetadata;

use crate::ApplyError;
use crate::command_handlers::run_cursor::RunCursor;
use crate::history::lineage::{genesis_seq, required_attempt_record};
use crate::state_access::FlowTransaction;

const HISTORY_ROWS_PER_COMMAND: usize = 128;

pub(crate) enum WindowAdvanceTarget {
    SemanticPoints(u64),
    GarbageSeq(u64),
}

/// Advances the window start W when the window holds more than N + M semantic points.
pub(crate) fn advance_window_if_full(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
) -> Result<(), ApplyError> {
    let settings = cursor.record.settings.unwrap_or_default();
    let window_events = u64::from(settings.history_window_semantic_events);
    let chunk_events = u64::from(settings.history_prune_chunk_semantic_events.max(1));
    if window_events == 0 {
        return Ok(());
    }
    let in_window = cursor
        .record
        .semantic_event_count
        .saturating_sub(cursor.record.pruned_semantic_event_count);
    if in_window <= window_events + chunk_events {
        return Ok(());
    }
    if crate::values::projected_superseded_value_bytes(transaction, cursor)?
        > settings.max_retained_superseded_bytes
    {
        return Ok(());
    }
    advance_window(
        transaction,
        cursor,
        WindowAdvanceTarget::SemanticPoints(chunk_events),
    )?;
    Ok(())
}

/// Moves W past up to the requested semantic points, keeping what rebuilds every later point.
///
/// Returns false without a later semantic point; the superseded value byte cap also calls this.
pub(crate) fn advance_window(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    target: WindowAdvanceTarget,
) -> Result<bool, ApplyError> {
    if cursor.history_window_page_used
        || matches!(target, WindowAdvanceTarget::SemanticPoints(0))
        || matches!(target, WindowAdvanceTarget::GarbageSeq(seq) if seq < cursor.record.window_start_seq)
    {
        return Ok(false);
    }
    let Some(pruned) = find_new_window_start(transaction, cursor, target)? else {
        return Ok(false);
    };
    let record = &mut cursor.record;
    record.window_start_seq = pruned.window_start_seq;
    record.window_scan_seq = 0;
    record.pruned_semantic_event_count += pruned.semantic_points;
    record.max_pruned_external_input_seq = record
        .max_pruned_external_input_seq
        .max(pruned.max_external_input_seq);
    crate::values::prune_garbage(transaction, cursor, pruned.window_start_seq)?;
    Ok(true)
}

struct PrunedChunk {
    window_start_seq: u64,
    semantic_points: u64,
    max_external_input_seq: u64,
}

/// Skips up to one page of history metadata; the next semantic record becomes the new W.
fn find_new_window_start(
    transaction: &FlowTransaction<'_>,
    cursor: &mut RunCursor,
    target: WindowAdvanceTarget,
) -> Result<Option<PrunedChunk>, ApplyError> {
    let rows = cursor.rows;
    let current_start = cursor.record.window_start_seq;
    let start_seq = cursor.record.window_scan_seq.max(current_start);
    let start = history_row_metadata_key(
        rows.flow_hash,
        rows.run_id,
        start_seq,
        RunTable::HistoryRecord,
        0,
    );
    let prefix = run_table_prefix(rows.flow_hash, rows.run_id, RunTable::HistoryRowMetadata);
    let (_, end) = prefix_range(&prefix);
    let page = transaction.scan_messages::<HistoryRowMetadata>(
        ColumnFamily::Flow,
        &start,
        &end,
        HISTORY_ROWS_PER_COMMAND,
        "history metadata",
    )?;
    cursor.history_window_page_used = true;
    let start_metadata_key = history_row_metadata_key(
        rows.flow_hash,
        rows.run_id,
        current_start,
        RunTable::HistoryRecord,
        0,
    );
    let start_metadata: HistoryRowMetadata = transaction
        .get_message(ColumnFamily::Flow, &start_metadata_key, "history metadata")?
        .ok_or_else(|| ApplyError::InvariantViolation {
            detail: format!("window start {current_start} has no history metadata"),
        })?;
    let mut previous_semantic_seq = current_start;
    let mut was_previous_external = start_metadata.is_external_input;
    let mut max_external_input_seq = 0;
    let mut semantic_points = 0;
    let mut candidate = None;
    let mut last_scanned_seq = None;
    for (key, metadata) in page {
        let (seq, history_table, _) = metadata_identity(&key, &prefix)?;
        last_scanned_seq = Some(seq);
        if history_table != RunTable::HistoryRecord
            || !metadata.is_semantic
            || seq <= current_start
            || seq > cursor.seq
        {
            continue;
        }
        if was_previous_external {
            max_external_input_seq = max_external_input_seq.max(previous_semantic_seq);
        }
        semantic_points += 1;
        candidate = Some(PrunedChunk {
            window_start_seq: seq,
            semantic_points,
            max_external_input_seq,
        });
        previous_semantic_seq = seq;
        was_previous_external = metadata.is_external_input;
        match target {
            WindowAdvanceTarget::SemanticPoints(limit) if semantic_points >= limit => break,
            WindowAdvanceTarget::GarbageSeq(garbage_seq) if seq > garbage_seq => break,
            _ => {}
        }
    }
    if candidate.is_none()
        && let Some(last_seq) = last_scanned_seq
    {
        cursor.record.window_scan_seq = last_seq.saturating_add(1);
    }
    Ok(candidate)
}

/// Deletes history records and checkpoints older than the checkpoint that rebuilds W.
pub(crate) fn prune_window_history(
    transaction: &mut FlowTransaction<'_>,
    cursor: &RunCursor,
) -> Result<usize, ApplyError> {
    let rows = cursor.rows;
    let window_start_seq = cursor.record.window_start_seq;
    let window_metadata_key = history_row_metadata_key(
        rows.flow_hash,
        rows.run_id,
        window_start_seq,
        RunTable::HistoryRecord,
        0,
    );
    let window_metadata: HistoryRowMetadata = transaction
        .get_message(ColumnFamily::Flow, &window_metadata_key, "history metadata")?
        .ok_or_else(|| ApplyError::InvariantViolation {
            detail: format!("window start {window_start_seq} has no history metadata"),
        })?;
    let keep_from = required_attempt_record(transaction, rows, window_metadata.attempt)?
        .checkpoint_seqs
        .iter()
        .copied()
        .filter(|seq| *seq <= window_start_seq)
        .max()
        .ok_or_else(|| ApplyError::InvariantViolation {
            detail: format!(
                "run attempt {} has no checkpoint at or before seq {window_start_seq}",
                window_metadata.attempt
            ),
        })?;
    let genesis = genesis_seq(transaction, rows)?;
    if keep_from <= genesis + 1 {
        return Ok(0);
    }
    let prefix = run_table_prefix(rows.flow_hash, rows.run_id, RunTable::HistoryRowMetadata);
    let start = history_row_metadata_key(
        rows.flow_hash,
        rows.run_id,
        genesis + 1,
        RunTable::HistoryRecord,
        0,
    );
    let end = history_row_metadata_key(
        rows.flow_hash,
        rows.run_id,
        keep_from,
        RunTable::HistoryRecord,
        0,
    );
    let page = transaction.scan_messages::<HistoryRowMetadata>(
        ColumnFamily::Flow,
        &start,
        &end,
        HISTORY_ROWS_PER_COMMAND,
        "history metadata",
    )?;
    let mut deleted_checkpoints = BTreeMap::<u32, BTreeSet<u64>>::new();
    let count = page.len();
    for (key, _) in page {
        let (seq, history_table, attempt) = metadata_identity(&key, &prefix)?;
        match history_table {
            RunTable::HistoryRecord => transaction.delete(
                ColumnFamily::History,
                history_record_key(rows.flow_hash, rows.run_id, seq),
            ),
            RunTable::Checkpoint => {
                transaction.delete(
                    ColumnFamily::History,
                    checkpoint_key(rows.flow_hash, rows.run_id, attempt, seq),
                );
                deleted_checkpoints.entry(attempt).or_default().insert(seq);
            }
            _ => {
                return Err(ApplyError::InvariantViolation {
                    detail: "history metadata names an unexpected table".to_owned(),
                });
            }
        }
    }
    for (attempt, deleted_seqs) in deleted_checkpoints {
        let mut attempt_record = required_attempt_record(transaction, rows, attempt)?;
        attempt_record
            .checkpoint_seqs
            .retain(|seq| !deleted_seqs.contains(seq));
        rows.put_attempt_record(transaction, &attempt_record);
    }
    Ok(count)
}

fn metadata_identity(key: &[u8], prefix: &[u8]) -> Result<(u64, RunTable, u32), ApplyError> {
    let suffix = key
        .strip_prefix(prefix)
        .ok_or_else(|| ApplyError::InvariantViolation {
            detail: "history metadata key is outside its run".to_owned(),
        })?;
    if suffix.len() != 13 {
        return Err(ApplyError::InvariantViolation {
            detail: "history metadata key must have a 13-byte suffix".to_owned(),
        });
    }
    let seq = u64::from_be_bytes(suffix[..8].try_into().expect("eight-byte seq"));
    let history_table =
        RunTable::from_byte(suffix[8]).map_err(|error| ApplyError::InvariantViolation {
            detail: format!("history metadata table is invalid: {error}"),
        })?;
    let attempt = u32::from_be_bytes(suffix[9..].try_into().expect("four-byte attempt"));
    Ok((seq, history_table, attempt))
}
