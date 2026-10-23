// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! State readers that time travel layers on the committed state: the temporary state that
//! replays an attempt, and a reader over a transaction for nested transactions.

use std::collections::BTreeMap;

use dexzd_keyspace::row_keys::run_singleton_key;
use dexzd_keyspace::{ColumnFamily, RunTable, run_table_prefix};
use dexzd_protocol::dexzd::v1::{RunRecord, SubFlowResultRecord, ValueBirthRecord, ValueRecord};
use prost::Message;

use crate::history::attempt_scope::{ATTEMPT_SCOPED_TABLES, AttemptRow};
use crate::state_access::{FlowTransaction, RunRows};
use crate::{ApplyError, FlowStateReader, StateReadError, StateRow, StateWrite};

/// Base rows fetched per page while merging a scan with the overlay.
const SCAN_PAGE_ROWS: usize = 256;

/// The state one attempt is replayed on: the committed state with the attempt-scoped tables
/// replaced by a checkpoint, plus the writes of the commands replayed so far.
///
/// Run-scoped tables stay visible, adjusted so replayed commands see what they saw when they
/// were first applied:
/// - Deduplication tables look empty, because every recorded command was not a duplicate.
/// - Child Flow results received at or after the replayed seq are hidden.
/// - Value rows that window pruning already deleted look live. The window only prunes values
///   that no point at or after W references, so they never reach the rebuilt state.
pub(crate) struct ReplayState<'base> {
    base: &'base dyn FlowStateReader,
    overlay: BTreeMap<(ColumnFamily, Vec<u8>), Option<Vec<u8>>>,
    masked_prefixes: Vec<Vec<u8>>,
    sub_flow_result_prefix: Vec<u8>,
    value_birth_prefix: Vec<u8>,
    value_prefix: Vec<u8>,
    run_record_key: Vec<u8>,
    as_of_seq: u64,
}

impl<'base> ReplayState<'base> {
    pub(crate) fn new(
        base: &'base dyn FlowStateReader,
        rows: RunRows,
        checkpoint_rows: &[AttemptRow],
        run_record: &RunRecord,
    ) -> Self {
        let table_prefix = |table| run_table_prefix(rows.flow_hash, rows.run_id, table);
        let mut masked_prefixes: Vec<Vec<u8>> =
            ATTEMPT_SCOPED_TABLES.iter().map(|table| table_prefix(*table)).collect();
        masked_prefixes.extend(
            [
                RunTable::RequestDedup,
                RunTable::RequestDedupOrder,
                RunTable::InboxDedup,
            ]
            .map(table_prefix),
        );
        let mut overlay = BTreeMap::new();
        for row in checkpoint_rows {
            overlay.insert((ColumnFamily::Flow, row.key(rows)), Some(row.value.clone()));
        }
        let run_record_key = run_singleton_key(rows.flow_hash, rows.run_id, RunTable::RunRecord);
        overlay.insert(
            (ColumnFamily::Flow, run_record_key.clone()),
            Some(run_record.encode_to_vec()),
        );
        Self {
            base,
            overlay,
            masked_prefixes,
            sub_flow_result_prefix: table_prefix(RunTable::SubFlowResult),
            value_birth_prefix: table_prefix(RunTable::ValueBirth),
            value_prefix: table_prefix(RunTable::Value),
            run_record_key,
            as_of_seq: 0,
        }
    }

    /// Prepares the next replayed command: its seq and the run record it opens.
    pub(crate) fn prepare_command(&mut self, seq: u64) -> Result<(), ApplyError> {
        let mut record = self.run_record()?;
        record.next_run_seq = seq;
        self.overlay.insert(
            (ColumnFamily::Flow, self.run_record_key.clone()),
            Some(record.encode_to_vec()),
        );
        self.as_of_seq = seq;
        Ok(())
    }

    pub(crate) fn run_record(&self) -> Result<RunRecord, ApplyError> {
        let bytes = self
            .overlay
            .get(&(ColumnFamily::Flow, self.run_record_key.clone()))
            .cloned()
            .flatten()
            .unwrap_or_default();
        RunRecord::decode(bytes.as_slice()).map_err(|error| ApplyError::CorruptState {
            table: "run record",
            key: String::new(),
            detail: error.to_string(),
        })
    }

    /// Applies a replayed command's writes to the temporary state.
    pub(crate) fn commit(&mut self, writes: Vec<StateWrite>) -> Result<(), ApplyError> {
        for write in writes {
            match write {
                StateWrite::Put {
                    column_family,
                    key,
                    value,
                } => {
                    self.overlay.insert((column_family, key), Some(value));
                }
                StateWrite::Delete { column_family, key } => {
                    self.overlay.insert((column_family, key), None);
                }
                StateWrite::DeleteRange { .. } => {
                    return Err(ApplyError::InvariantViolation {
                        detail: "a replayed command deleted a key range".to_owned(),
                    });
                }
            }
        }
        Ok(())
    }

    fn is_masked(&self, column_family: ColumnFamily, key: &[u8]) -> bool {
        column_family == ColumnFamily::Flow
            && self
                .masked_prefixes
                .iter()
                .any(|prefix| key.starts_with(prefix))
    }

    /// Whether a committed row is invisible to the command being replayed.
    fn is_hidden_base_row(
        &self,
        column_family: ColumnFamily,
        key: &[u8],
        value: &[u8],
    ) -> Result<bool, StateReadError> {
        if self.is_masked(column_family, key) {
            return Ok(true);
        }
        if column_family == ColumnFamily::Flow && key.starts_with(&self.sub_flow_result_prefix) {
            let record = SubFlowResultRecord::decode(value).map_err(|error| StateReadError {
                column_family,
                detail: format!("child Flow result is corrupt: {error}"),
            })?;
            return Ok(record.received_seq >= self.as_of_seq);
        }
        Ok(false)
    }

    fn pruned_value_stand_in(&self, column_family: ColumnFamily, key: &[u8]) -> Option<Vec<u8>> {
        match column_family {
            ColumnFamily::Flow if key.starts_with(&self.value_birth_prefix) => {
                Some(ValueBirthRecord::default().encode_to_vec())
            }
            ColumnFamily::Value if key.starts_with(&self.value_prefix) => {
                Some(ValueRecord::default().encode_to_vec())
            }
            _ => None,
        }
    }
}

impl FlowStateReader for ReplayState<'_> {
    fn get(
        &self,
        column_family: ColumnFamily,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StateReadError> {
        if let Some(buffered) = self.overlay.get(&(column_family, key.to_vec())) {
            return Ok(buffered.clone());
        }
        let committed = if self.is_masked(column_family, key) {
            None
        } else {
            match self.base.get(column_family, key)? {
                Some(value) if self.is_hidden_base_row(column_family, key, &value)? => None,
                other => other,
            }
        };
        Ok(committed.or_else(|| self.pruned_value_stand_in(column_family, key)))
    }

    fn scan(
        &self,
        column_family: ColumnFamily,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<StateRow>, StateReadError> {
        let mut merged: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        if !self.is_masked(column_family, start) {
            let mut page_start = start.to_vec();
            loop {
                let page = self
                    .base
                    .scan(column_family, &page_start, end, SCAN_PAGE_ROWS)?;
                let is_last_page = page.len() < SCAN_PAGE_ROWS;
                for (key, value) in page {
                    page_start = key.clone();
                    page_start.push(0);
                    let is_overwritten = self.overlay.contains_key(&(column_family, key.clone()));
                    if !is_overwritten && !self.is_hidden_base_row(column_family, &key, &value)? {
                        merged.insert(key, value);
                    }
                }
                if is_last_page || merged.len() >= limit {
                    break;
                }
            }
        }
        let buffered = self
            .overlay
            .range((column_family, start.to_vec())..(column_family, end.to_vec()))
            .filter_map(|((_, key), value)| {
                value.as_ref().map(|value| (key.clone(), value.clone()))
            });
        merged.extend(buffered);
        Ok(merged.into_iter().take(limit).collect())
    }
}

/// Reads a transaction, including its buffered writes, so a nested transaction can run on it.
pub(crate) struct TransactionReader<'transaction, 'reader> {
    transaction: &'transaction FlowTransaction<'reader>,
}

impl<'transaction, 'reader> TransactionReader<'transaction, 'reader> {
    pub(crate) fn new(transaction: &'transaction FlowTransaction<'reader>) -> Self {
        Self { transaction }
    }
}

impl FlowStateReader for TransactionReader<'_, '_> {
    fn get(
        &self,
        column_family: ColumnFamily,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StateReadError> {
        self.transaction
            .get(column_family, key)
            .map_err(|error| nested_read_error(column_family, &error))
    }

    fn scan(
        &self,
        column_family: ColumnFamily,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<StateRow>, StateReadError> {
        self.transaction
            .scan(column_family, start, end, limit)
            .map_err(|error| nested_read_error(column_family, &error))
    }
}

fn nested_read_error(column_family: ColumnFamily, error: &ApplyError) -> StateReadError {
    StateReadError {
        column_family,
        detail: error.to_string(),
    }
}

/// Buffers the writes of a committed nested transaction in its parent.
pub(crate) fn absorb_writes(transaction: &mut FlowTransaction<'_>, writes: Vec<StateWrite>) {
    for write in writes {
        match write {
            StateWrite::Put {
                column_family,
                key,
                value,
            } => transaction.put(column_family, key, value),
            StateWrite::Delete { column_family, key } => transaction.delete(column_family, key),
            StateWrite::DeleteRange {
                column_family,
                start,
                end,
            } => transaction.delete_range(column_family, start, end),
        }
    }
}
