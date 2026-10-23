// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Which run state belongs to an attempt (plan section 7). Time travel replaces exactly these
//! rows; everything else in the run is run-scoped and never rolls back.

use dexzd_keyspace::row_keys::prefix_range;
use dexzd_keyspace::{ColumnFamily, RunTable, run_key, run_table_prefix};
use dexzd_protocol::dexzd::v1::{
    AttributeRecord, CheckpointRow, CheckpointRunStatus, RunRecord, TimerId,
};
use prost::Message;

use crate::ApplyError;
use crate::state_access::{FlowTransaction, RunRows};

/// Tables whose rows belong to the current attempt. Timer rows belong to it only when their
/// TimerId names a run attempt (run-scoped timers use run attempt 0).
pub(crate) const ATTEMPT_SCOPED_TABLES: [RunTable; 8] = [
    RunTable::Attribute,
    RunTable::ChannelMessage,
    RunTable::ChannelMeta,
    RunTable::StepExecution,
    RunTable::StepCounter,
    RunTable::LockTable,
    RunTable::Waiter,
    RunTable::Timer,
];

/// One attempt-scoped row of a run: its table, key suffix after the table byte, and value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AttemptRow {
    pub(crate) table: RunTable,
    pub(crate) key_suffix: Vec<u8>,
    pub(crate) value: Vec<u8>,
}

impl AttemptRow {
    pub(crate) fn key(&self, rows: RunRows) -> Vec<u8> {
        run_key(rows.flow_hash, rows.run_id, self.table, &self.key_suffix)
    }

    pub(crate) fn encoded_bytes(&self) -> u64 {
        u64::try_from(self.key_suffix.len() + self.value.len()).unwrap_or(u64::MAX)
    }

    pub(crate) fn to_checkpoint_row(&self) -> CheckpointRow {
        CheckpointRow {
            table: u32::from(self.table as u8),
            key_suffix: self.key_suffix.clone(),
            value: self.value.clone(),
        }
    }

    pub(crate) fn from_checkpoint_row(row: &CheckpointRow) -> Result<Self, ApplyError> {
        let table = u8::try_from(row.table)
            .ok()
            .and_then(|byte| RunTable::from_byte(byte).ok())
            .filter(|table| ATTEMPT_SCOPED_TABLES.contains(table))
            .ok_or_else(|| ApplyError::InvariantViolation {
                detail: format!("checkpoint row has table {} outside the attempt", row.table),
            })?;
        Ok(Self {
            table,
            key_suffix: row.key_suffix.clone(),
            value: row.value.clone(),
        })
    }

    /// Whether this Attribute row is a hard-delete Attribute, which is run-scoped.
    pub(crate) fn is_hard_delete_attribute(&self) -> Result<bool, ApplyError> {
        if self.table != RunTable::Attribute {
            return Ok(false);
        }
        let record = AttributeRecord::decode(self.value.as_slice()).map_err(|error| {
            ApplyError::CorruptState {
                table: "attribute",
                key: hex(&self.key_suffix),
                detail: error.to_string(),
            }
        })?;
        Ok(record.is_hard_delete_on_null)
    }
}

/// Reads every attempt-scoped row of the run in (table, key) order, including buffered writes.
pub(crate) fn read_attempt_rows(
    transaction: &FlowTransaction<'_>,
    rows: RunRows,
) -> Result<Vec<AttemptRow>, ApplyError> {
    let mut attempt_rows = Vec::new();
    for table in ATTEMPT_SCOPED_TABLES {
        let prefix = run_table_prefix(rows.flow_hash, rows.run_id, table);
        let (start, end) = prefix_range(&prefix);
        for (key, value) in transaction.scan(ColumnFamily::Flow, &start, &end, usize::MAX)? {
            let key_suffix = key[prefix.len()..].to_vec();
            if table == RunTable::Timer && decode_timer_id(&key_suffix)?.run_attempt == 0 {
                continue;
            }
            attempt_rows.push(AttemptRow {
                table,
                key_suffix,
                value,
            });
        }
    }
    Ok(attempt_rows)
}

/// Decodes the TimerId that a Timer row key suffix encodes.
pub(crate) fn decode_timer_id(key_suffix: &[u8]) -> Result<TimerId, ApplyError> {
    TimerId::decode(key_suffix).map_err(|error| ApplyError::CorruptState {
        table: "timer",
        key: hex(key_suffix),
        detail: error.to_string(),
    })
}

/// Returns the attempt-scoped fields of the run record.
pub(crate) fn run_status_of(record: &RunRecord) -> CheckpointRunStatus {
    CheckpointRunStatus {
        status: record.status,
        closed_at_micros: record.closed_at_micros,
        close: record.close.clone(),
        is_graceful_completion_requested: record.is_graceful_completion_requested,
    }
}

/// Replaces the attempt-scoped fields of the run record.
pub(crate) fn set_run_status(record: &mut RunRecord, status: &CheckpointRunStatus) {
    record.status = status.status;
    record.closed_at_micros = status.closed_at_micros;
    record.close.clone_from(&status.close);
    record.is_graceful_completion_requested = status.is_graceful_completion_requested;
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
