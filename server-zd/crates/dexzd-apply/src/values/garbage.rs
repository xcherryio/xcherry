// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_keyspace::row_keys::{prefix_range, value_row_key};
use dexzd_keyspace::{ColumnFamily, RunTable, ValueId, run_table_prefix};
use dexzd_protocol::dexzd::v1::ValueGarbageRecord;

use crate::ApplyError;
use crate::command_handlers::run_cursor::RunCursor;
use crate::history::{WindowAdvanceTarget, advance_window, prune_window_history};
use crate::state_access::FlowTransaction;
use crate::values::{projected_superseded_value_bytes, reconcile_run_storage};

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
#[expect(dead_code, reason = "Available to window maintenance handlers.")]
pub(crate) fn superseded_bytes_over_cap(cursor: &RunCursor) -> u64 {
    let usage = cursor.record.storage.unwrap_or_default();
    let settings = cursor.record.settings.unwrap_or_default();
    usage
        .superseded_value_bytes
        .saturating_sub(settings.max_retained_superseded_bytes)
}

/// Advances the semantic window toward its byte cap while retaining the current point.
pub(crate) fn enforce_superseded_byte_cap(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
) -> Result<(), ApplyError> {
    prune_garbage(transaction, cursor, cursor.record.window_start_seq)?;
    let superseded_bytes = projected_superseded_value_bytes(transaction, cursor)?;
    let cap = cursor
        .record
        .settings
        .unwrap_or_default()
        .max_retained_superseded_bytes;
    let excess_bytes = superseded_bytes.saturating_sub(cap);
    if excess_bytes > 0
        && let Some(target_garbage_seq) = garbage_seq_to_fit_cap(transaction, cursor, excess_bytes)?
    {
        advance_window(
            transaction,
            cursor,
            WindowAdvanceTarget::GarbageSeq(target_garbage_seq),
        )?;
    }
    prune_window_history(transaction, cursor)?;
    reconcile_run_storage(transaction, cursor)?;
    Ok(())
}

fn garbage_seq_to_fit_cap(
    transaction: &FlowTransaction<'_>,
    cursor: &RunCursor,
    excess_bytes: u64,
) -> Result<Option<u64>, ApplyError> {
    let prefix = run_table_prefix(
        cursor.rows.flow_hash,
        cursor.rows.run_id,
        RunTable::ValueGarbage,
    );
    let (start, end) = prefix_range(&prefix);
    let garbage = transaction.scan_messages::<ValueGarbageRecord>(
        ColumnFamily::Flow,
        &start,
        &end,
        GARBAGE_ROWS_PER_CALL,
        "value garbage",
    )?;
    let mut collected_bytes = 0_u64;
    let mut target_seq = None;
    for (key, record) in garbage {
        let suffix = &key[prefix.len()..];
        if suffix.len() != 20 {
            return Err(ApplyError::InvariantViolation {
                detail: "value garbage key must have a 20-byte suffix".to_owned(),
            });
        }
        let garbage_seq = u64::from_be_bytes(
            suffix[..8]
                .try_into()
                .expect("garbage keys have an eight-byte sequence"),
        );
        if garbage_seq >= cursor.seq {
            break;
        }
        target_seq = Some(garbage_seq);
        collected_bytes = collected_bytes.saturating_add(record.size_bytes);
        if collected_bytes >= excess_bytes {
            break;
        }
    }
    Ok(target_seq)
}
