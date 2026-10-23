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

use dexzd_keyspace::ColumnFamily;
use dexzd_keyspace::row_keys::{checkpoint_key, history_record_key};

use crate::ApplyError;
use crate::command_handlers::run_cursor::RunCursor;
use crate::history::lineage::{genesis_seq, history_page, required_attempt_record};
use crate::state_access::FlowTransaction;

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
    let Some(pruned) = find_new_window_start(transaction, cursor, chunk_events)? else {
        return Err(ApplyError::InvariantViolation {
            detail: format!(
                "the window counts {in_window} semantic events but history has fewer from seq {}",
                cursor.record.window_start_seq
            ),
        });
    };
    let record = &mut cursor.record;
    record.window_start_seq = pruned.window_start_seq;
    record.pruned_semantic_event_count += chunk_events;
    record.max_pruned_external_input_seq = record
        .max_pruned_external_input_seq
        .max(pruned.max_external_input_seq);
    delete_unneeded_history(transaction, cursor, pruned.window_start_attempt)?;
    crate::values::prune_garbage(transaction, cursor, pruned.window_start_seq)?;
    Ok(())
}

struct PrunedChunk {
    window_start_seq: u64,
    window_start_attempt: u32,
    max_external_input_seq: u64,
}

/// Skips `chunk_events` semantic records from W; the next semantic record is the new W.
fn find_new_window_start(
    transaction: &FlowTransaction<'_>,
    cursor: &RunCursor,
    chunk_events: u64,
) -> Result<Option<PrunedChunk>, ApplyError> {
    let mut remaining = chunk_events;
    let mut max_external_input_seq = 0;
    let mut next_seq = cursor.record.window_start_seq;
    loop {
        let page = history_page(transaction, cursor.rows, next_seq)?;
        let Some(last) = page.last() else {
            return Ok(None);
        };
        next_seq = last.seq + 1;
        for record in page {
            if record.is_semantic {
                if remaining == 0 {
                    return Ok(Some(PrunedChunk {
                        window_start_seq: record.seq,
                        window_start_attempt: record.attempt,
                        max_external_input_seq,
                    }));
                }
                remaining -= 1;
            }
            if record.is_external_input {
                max_external_input_seq = max_external_input_seq.max(record.seq);
            }
        }
    }
}

/// Deletes history records and checkpoints older than the checkpoint that rebuilds W.
fn delete_unneeded_history(
    transaction: &mut FlowTransaction<'_>,
    cursor: &RunCursor,
    window_start_attempt: u32,
) -> Result<(), ApplyError> {
    let window_start_seq = cursor.record.window_start_seq;
    let rows = cursor.rows;
    let keep_from = required_attempt_record(transaction, rows, window_start_attempt)?
        .checkpoint_seqs
        .iter()
        .copied()
        .filter(|seq| *seq <= window_start_seq)
        .max()
        .ok_or_else(|| ApplyError::InvariantViolation {
            detail: format!(
                "run attempt {window_start_attempt} has no checkpoint at or before seq \
                 {window_start_seq}"
            ),
        })?;
    let genesis = genesis_seq(transaction, rows)?;
    if keep_from > genesis + 1 {
        transaction.delete_range(
            ColumnFamily::History,
            history_record_key(rows.flow_hash, rows.run_id, genesis + 1),
            history_record_key(rows.flow_hash, rows.run_id, keep_from),
        );
    }
    for attempt in 1..=cursor.record.current_attempt {
        let mut attempt_record = required_attempt_record(transaction, rows, attempt)?;
        let is_kept = |seq: &u64| *seq >= keep_from || (attempt == 1 && *seq == genesis);
        let (kept, deleted): (Vec<u64>, Vec<u64>) = attempt_record
            .checkpoint_seqs
            .iter()
            .copied()
            .partition(is_kept);
        if deleted.is_empty() {
            continue;
        }
        for seq in deleted {
            transaction.delete(
                ColumnFamily::History,
                checkpoint_key(rows.flow_hash, rows.run_id, attempt, seq),
            );
        }
        attempt_record.checkpoint_seqs = kept;
        rows.put_attempt_record(transaction, &attempt_record);
    }
    Ok(())
}
