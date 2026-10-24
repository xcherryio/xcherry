// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::{BTreeMap, BTreeSet};

use dexzd_keyspace::row_keys::{
    HARD_DELETE_CURRENT_OBJECT, TimerIndexEntry, decode_timer_index_key, partition_index_range,
};
use dexzd_keyspace::{
    ColumnFamily, FLOW_DIRECTORY_KEY_TAG, FlowHash, IndexKind, PartitionId, RunId, RunTable,
    ValueId, parse_run_key,
};
use dexzd_protocol::dexzd::v1::{
    AttemptRecord, AttributeRecord, ChannelMessageRecord, ChannelMetaRecord, FlowDirectoryRecord,
    HardDeleteObjectRecord, HistoryRecord, LockRecord, RequestDedupRecord, RunRecord,
    StepExecutionRecord, SubFlowResultRecord, TimerRecord, ValueBirthRecord, ValueGarbageRecord,
};
use prost::Message;

use crate::memory_state::MemoryFlowState;
use crate::row_decoding::hex;

/// The decoded state of one Flow: its directory, runs, and timer index entries.
#[derive(Debug, Clone, Default)]
pub struct FlowView {
    /// The Flow directory row, when it exists.
    pub directory: Option<FlowDirectoryRecord>,
    /// Every run with at least one row, by run ID.
    pub runs: BTreeMap<RunId, RunView>,
    /// Timer index entries of this Flow in its partition.
    pub timer_index: Vec<TimerIndexEntry>,
}

/// Which runs a [`FlowView`] decodes completely; other runs get only their run record.
#[derive(Debug, Clone, Copy)]
pub enum RunSelection<'selection> {
    /// Every run.
    All,
    /// Only these runs.
    Only(&'selection BTreeSet<RunId>),
}

impl RunSelection<'_> {
    fn contains(&self, run_id: &RunId) -> bool {
        match self {
            Self::All => true,
            Self::Only(run_ids) => run_ids.contains(run_id),
        }
    }
}

/// The decoded rows of one run.
#[derive(Debug, Clone, Default)]
pub struct RunView {
    /// True when every row of the run was decoded, not only its run record.
    pub is_complete: bool,
    /// The run record, when it exists.
    pub record: Option<RunRecord>,
    /// Attempt records by attempt.
    pub attempts: BTreeMap<u32, AttemptRecord>,
    /// Attributes by key.
    pub attributes: BTreeMap<String, AttributeRecord>,
    /// Pending Channel messages in key order.
    pub channel_messages: Vec<ChannelMessageRecord>,
    /// Channel counters by channel name.
    pub channel_metas: BTreeMap<String, ChannelMetaRecord>,
    /// Step executions by Step execution ID.
    pub step_executions: BTreeMap<String, StepExecutionRecord>,
    /// Lock rows by lock key.
    pub locks: BTreeMap<String, LockRecord>,
    /// Timer rows: encoded TimerId (the key suffix) and record.
    pub timers: Vec<(Vec<u8>, TimerRecord)>,
    /// Request deduplication rows: request kind byte, request ID, record.
    pub request_dedups: Vec<(u8, String, RequestDedupRecord)>,
    /// Child Flow results by child Flow ID.
    pub sub_flow_results: BTreeMap<String, SubFlowResultRecord>,
    /// Value birth rows (table B).
    pub value_births: BTreeMap<ValueId, ValueBirthRecord>,
    /// Holder encodings per value (table R).
    pub value_holders: BTreeMap<ValueId, BTreeSet<Vec<u8>>>,
    /// Garbage rows (table G): lost-last-holder seq, value, record.
    pub value_garbage: Vec<(u64, ValueId, ValueGarbageRecord)>,
    /// Values with a row in the value column family.
    pub value_rows: BTreeSet<ValueId>,
    /// History records: key seq and record.
    pub history: Vec<(u64, HistoryRecord)>,
    /// Current hard-delete objects by Attribute key.
    pub hard_delete_current: BTreeMap<String, HardDeleteObjectRecord>,
}

/// A row the view could not decode; redecoding normally rejects these first.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("cannot read {what} at key {key_hex}: {detail}")]
pub struct StateViewError {
    /// What was being read.
    pub what: &'static str,
    /// Hex-encoded key.
    pub key_hex: String,
    /// Decoder message.
    pub detail: String,
}

impl FlowView {
    /// Decodes the Flow's directory, every run record, and every row of the selected runs.
    ///
    /// Returns the first row that does not decode.
    pub fn read(
        state: &MemoryFlowState,
        flow_hash: FlowHash,
        partition: PartitionId,
        selection: RunSelection<'_>,
    ) -> Result<Self, StateViewError> {
        let mut view = Self::default();
        let flow_prefix = flow_hash.to_be_bytes();
        for (key, value) in state.rows_with_prefix(ColumnFamily::Flow, &flow_prefix) {
            if key.get(8) == Some(&FLOW_DIRECTORY_KEY_TAG) {
                view.directory = Some(decode(key, value, "Flow directory")?);
                continue;
            }
            let parsed = parse_key(key)?;
            if selection.contains(&parsed.run_id) {
                view.run_mut(parsed.run_id).is_complete = true;
                view.read_flow_row(key, value)?;
            } else if parsed.table == RunTable::RunRecord {
                view.run_mut(parsed.run_id).record = Some(decode(key, value, "run record")?);
            }
        }
        for (key, _) in state.rows_with_prefix(ColumnFamily::Value, &flow_prefix) {
            let parsed = parse_key(key)?;
            if !selection.contains(&parsed.run_id) {
                continue;
            }
            let value_id = value_id_from_suffix(key, parsed.suffix)?;
            view.run_mut(parsed.run_id).value_rows.insert(value_id);
        }
        for (key, value) in state.rows_with_prefix(ColumnFamily::History, &flow_prefix) {
            let parsed = parse_key(key)?;
            if parsed.table != RunTable::HistoryRecord || !selection.contains(&parsed.run_id) {
                continue;
            }
            let seq = u64_from_suffix(key, parsed.suffix)?;
            let record = decode(key, value, "history record")?;
            view.run_mut(parsed.run_id).history.push((seq, record));
        }
        let (start, end) = partition_index_range(partition, IndexKind::Timer);
        let timer_rows = dexzd_apply::FlowStateReader::scan(
            state,
            ColumnFamily::PartitionIndex,
            &start,
            &end,
            usize::MAX,
        )
        .map_err(|error| StateViewError {
            what: "timer index",
            key_hex: hex(&start),
            detail: error.to_string(),
        })?;
        view.timer_index = timer_rows
            .iter()
            .filter_map(|(key, _)| decode_timer_index_key(key))
            .filter(|entry| entry.flow_hash == flow_hash)
            .collect();
        Ok(view)
    }

    /// Returns the last run of the directory and its view.
    pub fn latest_run(&self) -> Option<(RunId, &RunView)> {
        let directory = self.directory.as_ref()?;
        let run = directory.runs.last()?;
        let run_id = RunId::from_slice(&run.run_id).ok()?;
        self.runs.get(&run_id).map(|view| (run_id, view))
    }

    fn run_mut(&mut self, run_id: RunId) -> &mut RunView {
        self.runs.entry(run_id).or_default()
    }

    fn read_flow_row(&mut self, key: &[u8], value: &[u8]) -> Result<(), StateViewError> {
        let parsed = parse_key(key)?;
        let suffix = parsed.suffix;
        let run = self.run_mut(parsed.run_id);
        match parsed.table {
            RunTable::RunRecord => run.record = Some(decode(key, value, "run record")?),
            RunTable::AttemptRecord => {
                let record: AttemptRecord = decode(key, value, "attempt record")?;
                run.attempts.insert(record.attempt, record);
            }
            RunTable::Attribute => {
                run.attributes
                    .insert(name(key, suffix)?, decode(key, value, "Attribute")?);
            }
            RunTable::ChannelMessage => {
                run.channel_messages
                    .push(decode(key, value, "Channel message")?);
            }
            RunTable::ChannelMeta => {
                run.channel_metas
                    .insert(name(key, suffix)?, decode(key, value, "Channel meta")?);
            }
            RunTable::StepExecution => {
                run.step_executions
                    .insert(name(key, suffix)?, decode(key, value, "Step execution")?);
            }
            RunTable::LockTable => {
                run.locks
                    .insert(name(key, suffix)?, decode(key, value, "lock")?);
            }
            RunTable::Timer => {
                run.timers
                    .push((suffix.to_vec(), decode(key, value, "timer")?));
            }
            RunTable::RequestDedup => {
                let Some((kind, request_id)) = suffix.split_first() else {
                    return Err(view_error(key, "request dedup", "empty key suffix"));
                };
                let request_id = String::from_utf8(request_id.to_vec())
                    .map_err(|error| view_error(key, "request dedup", &error.to_string()))?;
                run.request_dedups
                    .push((*kind, request_id, decode(key, value, "request dedup")?));
            }
            RunTable::SubFlowResult => {
                run.sub_flow_results
                    .insert(name(key, suffix)?, decode(key, value, "SubFlow result")?);
            }
            RunTable::ValueBirth => {
                run.value_births.insert(
                    value_id_from_suffix(key, suffix)?,
                    decode(key, value, "value birth")?,
                );
            }
            RunTable::ValueHolder => {
                if suffix.len() < 12 {
                    return Err(view_error(key, "value holder", "short key suffix"));
                }
                let value_id = value_id_from_suffix(key, &suffix[..12])?;
                run.value_holders
                    .entry(value_id)
                    .or_default()
                    .insert(suffix[12..].to_vec());
            }
            RunTable::ValueGarbage => {
                if suffix.len() != 20 {
                    return Err(view_error(
                        key,
                        "value garbage",
                        "key suffix is not 20 bytes",
                    ));
                }
                let seq = u64_from_suffix(key, &suffix[..8])?;
                let value_id = value_id_from_suffix(key, &suffix[8..])?;
                run.value_garbage
                    .push((seq, value_id, decode(key, value, "value garbage")?));
            }
            RunTable::HardDeleteObject => {
                if let Some((&HARD_DELETE_CURRENT_OBJECT, attribute_key)) = suffix.split_first() {
                    run.hard_delete_current.insert(
                        name(key, attribute_key)?,
                        decode(key, value, "hard-delete object")?,
                    );
                }
            }
            RunTable::StepCounter
            | RunTable::Waiter
            | RunTable::Outbox
            | RunTable::InboxDedup
            | RunTable::PermissionSet
            | RunTable::Value
            | RunTable::HistoryRecord
            | RunTable::HistoryRowMetadata
            | RunTable::Checkpoint
            | RunTable::SearchDocument
            | RunTable::RequestDedupOrder => {}
        }
        Ok(())
    }
}

fn parse_key(key: &[u8]) -> Result<dexzd_keyspace::ParsedRunKey<'_>, StateViewError> {
    parse_run_key(key).map_err(|error| view_error(key, "run key", &error.to_string()))
}

fn decode<RowMessage: Message + Default>(
    key: &[u8],
    value: &[u8],
    what: &'static str,
) -> Result<RowMessage, StateViewError> {
    RowMessage::decode(value).map_err(|error| view_error(key, what, &error.to_string()))
}

fn name(key: &[u8], suffix: &[u8]) -> Result<String, StateViewError> {
    String::from_utf8(suffix.to_vec())
        .map_err(|error| view_error(key, "row name", &error.to_string()))
}

fn u64_from_suffix(key: &[u8], suffix: &[u8]) -> Result<u64, StateViewError> {
    let bytes: [u8; 8] = suffix
        .try_into()
        .map_err(|_| view_error(key, "seq", "key suffix is not 8 bytes"))?;
    Ok(u64::from_be_bytes(bytes))
}

fn value_id_from_suffix(key: &[u8], suffix: &[u8]) -> Result<ValueId, StateViewError> {
    if suffix.len() != 12 {
        return Err(view_error(key, "value ID", "key suffix is not 12 bytes"));
    }
    let run_seq = u64_from_suffix(key, &suffix[..8])?;
    let index_bytes: [u8; 4] = suffix[8..]
        .try_into()
        .map_err(|_| view_error(key, "value ID", "index is not 4 bytes"))?;
    Ok(ValueId {
        run_seq,
        index_in_command: u32::from_be_bytes(index_bytes),
    })
}

fn view_error(key: &[u8], what: &'static str, detail: &str) -> StateViewError {
    StateViewError {
        what,
        key_hex: hex(key),
        detail: detail.to_owned(),
    }
}
