// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_keyspace::row_keys::{prefix_range, value_row_key};
use dexzd_keyspace::{ColumnFamily, RunTable, ValueId, run_table_prefix};
use dexzd_protocol::dexzd::v1::{HistoryRecord, ValueGarbageRecord};

use crate::ApplyError;
use crate::command_handlers::run_cursor::RunCursor;
use crate::state_access::FlowTransaction;
use crate::values::reconcile_run_storage;

const GARBAGE_ROWS_PER_CALL: usize = 128;

/// Collects at most 128 values whose last holder disappeared before the window start.
pub(crate) fn prune_garbage(
    transaction: &mut FlowTransaction<'_>,
    cursor: &RunCursor,
    window_start_seq: u64,
) -> Result<usize, ApplyError> {
    let rows = cursor.rows;
    let prefix = run_table_prefix(rows.flow_hash, rows.run_id, RunTable::ValueGarbage);
    let (start, end) = prefix_range(&prefix);
    let garbage = transaction.scan_messages::<ValueGarbageRecord>(
        ColumnFamily::Flow,
        &start,
        &end,
        GARBAGE_ROWS_PER_CALL,
        "value garbage",
    )?;
    let mut collected = 0;
    for (garbage_key, _) in garbage {
        let suffix = &garbage_key[prefix.len()..];
        if suffix.len() != 20 {
            return Err(ApplyError::InvariantViolation {
                detail: "value garbage key must have a 20-byte suffix".to_owned(),
            });
        }
        let lost_last_holder_seq = u64::from_be_bytes(
            suffix[..8]
                .try_into()
                .expect("the garbage key suffix has at least eight bytes"),
        );
        if lost_last_holder_seq >= window_start_seq {
            break;
        }
        let value_id = ValueId {
            run_seq: u64::from_be_bytes(
                suffix[8..16]
                    .try_into()
                    .expect("the garbage key suffix has twelve value ID bytes"),
            ),
            index_in_command: u32::from_be_bytes(
                suffix[16..20]
                    .try_into()
                    .expect("the garbage key suffix has four index bytes"),
            ),
        };
        transaction.delete(
            ColumnFamily::Value,
            value_row_key(rows.flow_hash, rows.run_id, RunTable::Value, value_id),
        );
        transaction.delete(
            ColumnFamily::Flow,
            value_row_key(rows.flow_hash, rows.run_id, RunTable::ValueBirth, value_id),
        );
        transaction.delete(ColumnFamily::Flow, garbage_key);
        collected += 1;
    }
    Ok(collected)
}

/// Returns the superseded bytes beyond this run's configured window cap.
pub(crate) fn superseded_bytes_over_cap(cursor: &RunCursor) -> u64 {
    let usage = cursor.record.storage.unwrap_or_default();
    let settings = cursor.record.settings.unwrap_or_default();
    usage
        .superseded_value_bytes
        .saturating_sub(settings.max_retained_superseded_bytes)
}

/// Advances the semantic window until superseded values fit its byte cap.
pub(crate) fn enforce_superseded_byte_cap(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
) -> Result<(), ApplyError> {
    while prune_garbage(transaction, cursor, cursor.record.window_start_seq)? > 0 {}
    reconcile_run_storage(transaction, cursor)?;
    while superseded_bytes_over_cap(cursor) > 0 {
        let next_window_start = next_semantic_window_start(transaction, cursor)?;
        if next_window_start <= cursor.record.window_start_seq {
            return Err(ApplyError::InvariantViolation {
                detail: "superseded values exceed the cap without an advanceable window".to_owned(),
            });
        }
        cursor.record.window_start_seq = next_window_start;
        prune_history_records(transaction, cursor)?;
        while prune_garbage(transaction, cursor, next_window_start)? > 0 {}
        reconcile_run_storage(transaction, cursor)?;
    }
    Ok(())
}

fn next_semantic_window_start(
    transaction: &FlowTransaction<'_>,
    cursor: &RunCursor,
) -> Result<u64, ApplyError> {
    let prefix = run_table_prefix(
        cursor.rows.flow_hash,
        cursor.rows.run_id,
        RunTable::HistoryRecord,
    );
    let (start, end) = prefix_range(&prefix);
    let records = transaction.scan_messages::<HistoryRecord>(
        ColumnFamily::History,
        &start,
        &end,
        usize::MAX,
        "history record",
    )?;
    Ok(records
        .into_iter()
        .map(|(_, record)| record)
        .find(|record| record.is_semantic && record.seq > cursor.record.window_start_seq)
        .map_or_else(|| cursor.seq.saturating_add(1), |record| record.seq))
}

fn prune_history_records(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
) -> Result<(), ApplyError> {
    let prefix = run_table_prefix(
        cursor.rows.flow_hash,
        cursor.rows.run_id,
        RunTable::HistoryRecord,
    );
    let (start, end) = prefix_range(&prefix);
    let records = transaction.scan_messages::<HistoryRecord>(
        ColumnFamily::History,
        &start,
        &end,
        usize::MAX,
        "history record",
    )?;
    for (key, record) in records {
        if record.seq > 1 && record.seq < cursor.record.window_start_seq {
            if record.is_external_input {
                cursor.record.max_pruned_external_input_seq =
                    cursor.record.max_pruned_external_input_seq.max(record.seq);
            }
            transaction.delete(ColumnFamily::History, key);
        }
    }
    Ok(())
}
