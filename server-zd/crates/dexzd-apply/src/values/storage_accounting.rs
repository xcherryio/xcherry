// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_keyspace::row_keys::run_singleton_key;
use dexzd_keyspace::{ColumnFamily, RunTable, parse_run_key, run_prefix, run_table_prefix};
use dexzd_protocol::dexzd::v1::{
    HistoryRowMetadata, RunRecord, RunStorageUsage, ValueBirthRecord, ValueGarbageRecord,
};
use prost::Message;

use crate::ApplyError;
use crate::command_handlers::run_cursor::RunCursor;
use crate::state_access::{FlowTransaction, StateRowChange};

// The RunRecord size can change when its byte totals cross protobuf varint boundaries.
const MAX_RUN_RECORD_SIZE_PASSES: usize = 12;

/// Reconciles storage totals with this command's row writes and deletes.
pub(crate) fn reconcile_run_storage(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
) -> Result<(), ApplyError> {
    cursor.rows.put_run_record(transaction, &cursor.record);
    let prefix = run_prefix(cursor.rows.flow_hash, cursor.rows.run_id);
    let mut changes = transaction.changed_run_rows(&prefix)?;
    let record_key = run_singleton_key(
        cursor.rows.flow_hash,
        cursor.rows.run_id,
        RunTable::RunRecord,
    );
    let record_change = changes
        .iter()
        .position(|change| change.column_family == ColumnFamily::Flow && change.key == record_key)
        .ok_or_else(|| ApplyError::InvariantViolation {
            detail: "run storage accounting has no buffered run record".to_owned(),
        })?;
    for _ in 0..MAX_RUN_RECORD_SIZE_PASSES {
        cursor.rows.put_run_record(transaction, &cursor.record);
        changes[record_change].current = Some(cursor.record.encode_to_vec());
        let usage = usage_after_changes(&changes, cursor)?;
        if cursor.record.storage.as_ref() == Some(&usage) {
            return Ok(());
        }
        cursor.record.storage = Some(usage);
    }
    Err(ApplyError::InvariantViolation {
        detail: "run storage accounting did not converge".to_owned(),
    })
}

fn usage_after_changes(
    changes: &[StateRowChange],
    cursor: &RunCursor,
) -> Result<RunStorageUsage, ApplyError> {
    let record_key = run_singleton_key(
        cursor.rows.flow_hash,
        cursor.rows.run_id,
        RunTable::RunRecord,
    );
    let committed_usage = changes
        .iter()
        .find(|change| change.column_family == ColumnFamily::Flow && change.key == record_key)
        .and_then(|change| change.previous.as_ref())
        .map(|bytes| decode_record::<RunRecord>(bytes, "run record"))
        .transpose()?
        .and_then(|record| record.storage)
        .unwrap_or_default();
    let mut state_delta = 0_i128;
    let mut history_delta = 0_i128;
    let mut birth_delta = 0_i128;
    let mut garbage_delta = 0_i128;
    for change in changes {
        let parsed =
            parse_run_key(&change.key).map_err(|error| ApplyError::InvariantViolation {
                detail: format!("buffered run key is invalid: {error}"),
            })?;
        match change.column_family {
            ColumnFamily::Flow => {
                let row_delta = row_size(change.current.as_ref(), &change.key)
                    - row_size(change.previous.as_ref(), &change.key);
                match parsed.table {
                    RunTable::ValueBirth => {
                        state_delta += row_delta;
                        birth_delta += record_size::<ValueBirthRecord>(change, "value birth")?;
                    }
                    RunTable::ValueGarbage => {
                        state_delta += row_delta;
                        garbage_delta +=
                            record_size::<ValueGarbageRecord>(change, "value garbage")?;
                    }
                    RunTable::HistoryRowMetadata => {
                        history_delta += row_delta
                            + record_size::<HistoryRowMetadata>(change, "history metadata")?;
                    }
                    _ => state_delta += row_delta,
                }
            }
            _ => {
                return Err(ApplyError::InvariantViolation {
                    detail: "a non-run column family entered storage accounting".to_owned(),
                });
            }
        }
    }
    Ok(RunStorageUsage {
        state_row_bytes: add_delta(committed_usage.state_row_bytes, state_delta)?,
        live_value_bytes: add_delta(
            committed_usage.live_value_bytes,
            birth_delta - garbage_delta,
        )?,
        superseded_value_bytes: add_delta(committed_usage.superseded_value_bytes, garbage_delta)?,
        history_bytes: add_delta(committed_usage.history_bytes, history_delta)?,
    })
}

/// Projects superseded bytes before final RunRecord storage reconciliation.
pub(crate) fn projected_superseded_value_bytes(
    transaction: &FlowTransaction<'_>,
    cursor: &RunCursor,
) -> Result<u64, ApplyError> {
    let prefix = run_table_prefix(
        cursor.rows.flow_hash,
        cursor.rows.run_id,
        RunTable::ValueGarbage,
    );
    let delta = transaction
        .buffered_row_changes_with_prefix(ColumnFamily::Flow, &prefix)?
        .iter()
        .map(|change| record_size::<ValueGarbageRecord>(change, "value garbage"))
        .try_fold(0_i128, |total, size| size.map(|size| total + size))?;
    let committed = cursor.record.storage.unwrap_or_default();
    add_delta(committed.superseded_value_bytes, delta)
}

fn row_size(value: Option<&Vec<u8>>, key: &[u8]) -> i128 {
    value.map_or(0, |value| (key.len() + value.len()) as i128)
}

trait SizedValueRecord: Message + Default {
    fn size_bytes(&self) -> u64;
}

impl SizedValueRecord for ValueBirthRecord {
    fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
}

impl SizedValueRecord for ValueGarbageRecord {
    fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
}

impl SizedValueRecord for HistoryRowMetadata {
    fn size_bytes(&self) -> u64 {
        self.row_size_bytes
    }
}

fn record_size<Record: SizedValueRecord>(
    change: &StateRowChange,
    table: &'static str,
) -> Result<i128, ApplyError> {
    let old_size = change
        .previous
        .as_ref()
        .map(|bytes| decode_record::<Record>(bytes, table))
        .transpose()?
        .map_or(0, |record| i128::from(record.size_bytes()));
    let new_size = change
        .current
        .as_ref()
        .map(|bytes| decode_record::<Record>(bytes, table))
        .transpose()?
        .map_or(0, |record| i128::from(record.size_bytes()));
    Ok(new_size - old_size)
}

fn decode_record<Record: Message + Default>(
    bytes: &[u8],
    table: &'static str,
) -> Result<Record, ApplyError> {
    Record::decode(bytes).map_err(|error| ApplyError::InvariantViolation {
        detail: format!("stored {table} row is corrupt: {error}"),
    })
}

fn add_delta(current: u64, delta: i128) -> Result<u64, ApplyError> {
    u64::try_from(i128::from(current) + delta).map_err(|_| ApplyError::InvariantViolation {
        detail: "run storage byte count overflowed or became negative".to_owned(),
    })
}

pub(crate) fn total_run_storage_bytes(usage: &RunStorageUsage) -> u64 {
    active_run_storage_bytes(usage).saturating_add(usage.superseded_value_bytes)
}

pub(crate) fn active_run_storage_bytes(usage: &RunStorageUsage) -> u64 {
    usage.state_row_bytes.saturating_add(usage.live_value_bytes)
}
