// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Decoding of committed state rows and partition index keys that the effect runner reads.

use dexzd_apply::FlowStateReader;
use dexzd_keyspace::{
    ColumnFamily, FlowHash, IndexKind, PartitionId, RunId, RunTable, partition_index_prefix,
    run_key, run_table_prefix,
};
use dexzd_protocol::dexzd::v1::{RunRecord, StepExecutionRecord, TimerId};
use prost::Message;

use crate::effects::EffectError;

/// Largest number of rows read from one index or table in one scan.
pub(crate) const SCAN_PAGE_ROWS: usize = 1024;

/// Hard-delete object table key prefix of the current object of an Attribute key.
pub(crate) const HARD_DELETE_CURRENT_OBJECT_TAG: u8 = 0x00;
/// Hard-delete object table key prefix of an object waiting for deletion.
pub(crate) const HARD_DELETE_PENDING_OBJECT_TAG: u8 = 0x01;

/// A run identified by the keys that prefix its state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct RunLocation {
    pub(crate) flow_hash: FlowHash,
    pub(crate) run_id: RunId,
}

/// Decodes one stored row, naming the table on failure.
pub(crate) fn decode_row<M: Message + Default>(
    table: &'static str,
    key: &[u8],
    bytes: &[u8],
) -> Result<M, EffectError> {
    M::decode(bytes).map_err(|error| EffectError::CorruptState {
        table,
        key: hex(key),
        detail: error.to_string(),
    })
}

/// Reads the run record, or `None` when the run no longer exists.
pub(crate) fn read_run_record(
    reader: &dyn FlowStateReader,
    run: RunLocation,
) -> Result<Option<RunRecord>, EffectError> {
    let key = run_key(run.flow_hash, run.run_id, RunTable::RunRecord, &[]);
    reader
        .get(ColumnFamily::Flow, &key)?
        .map(|bytes| decode_row("RunRecord", &key, &bytes))
        .transpose()
}

/// Reads one Step execution of the run's current attempt.
pub(crate) fn read_step_execution(
    reader: &dyn FlowStateReader,
    run: RunLocation,
    step_execution_id: &str,
) -> Result<Option<StepExecutionRecord>, EffectError> {
    let key = run_key(
        run.flow_hash,
        run.run_id,
        RunTable::StepExecution,
        step_execution_id.as_bytes(),
    );
    reader
        .get(ColumnFamily::Flow, &key)?
        .map(|bytes| decode_row("StepExecution", &key, &bytes))
        .transpose()
}

/// Reads every Step execution of the run's current attempt, in key order.
pub(crate) fn read_step_executions(
    reader: &dyn FlowStateReader,
    run: RunLocation,
) -> Result<Vec<StepExecutionRecord>, EffectError> {
    let prefix = run_table_prefix(run.flow_hash, run.run_id, RunTable::StepExecution);
    scan_all(reader, ColumnFamily::Flow, &prefix)?
        .into_iter()
        .map(|(key, value)| decode_row("StepExecution", &key, &value))
        .collect()
}

/// Returns every row whose key starts with `prefix`, reading in pages.
pub(crate) fn scan_all(
    reader: &dyn FlowStateReader,
    column_family: ColumnFamily,
    prefix: &[u8],
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, EffectError> {
    let end = prefix_successor(prefix);
    let mut rows = Vec::new();
    let mut start = prefix.to_vec();
    loop {
        let page = reader.scan(column_family, &start, &end, SCAN_PAGE_ROWS)?;
        let is_last_page = page.len() < SCAN_PAGE_ROWS;
        if let Some((last_key, _)) = page.last() {
            start = key_successor(last_key);
        }
        rows.extend(page);
        if is_last_page {
            return Ok(rows);
        }
    }
}

/// Returns the first key after every key that starts with `prefix`.
///
/// Every index and run prefix starts with a byte below 0xff, so the successor always exists.
pub(crate) fn prefix_successor(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last != u8::MAX {
            end.push(last + 1);
            return end;
        }
    }
    vec![u8::MAX; prefix.len() + 1]
}

/// Returns the smallest key greater than `key`.
pub(crate) fn key_successor(key: &[u8]) -> Vec<u8> {
    let mut next = key.to_vec();
    next.push(0);
    next
}

/// One row of the Timer index.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TimerIndexEntry {
    pub(crate) fire_at_micros: i64,
    pub(crate) run: RunLocation,
    pub(crate) timer_id: TimerId,
}

/// Parses `fire_at_micros(8) ‖ flow_hash(8) ‖ run_id(16) ‖ timer_id` after the index prefix.
pub(crate) fn parse_timer_index_key(
    partition: PartitionId,
    key: &[u8],
) -> Result<TimerIndexEntry, EffectError> {
    let rest = strip_index_prefix(partition, IndexKind::Timer, key)?;
    if rest.len() < 32 {
        return Err(corrupt_index("Timer", key, "key is too short"));
    }
    let fire_at_micros = i64::from_be_bytes(rest[..8].try_into().expect("8 bytes"));
    let run = parse_run_location(&rest[8..32]);
    let timer_id = decode_row("Timer index", key, &rest[32..])?;
    Ok(TimerIndexEntry {
        fire_at_micros,
        run,
        timer_id,
    })
}

/// Returns the Timer index key of the first timer that fires at or after `fire_at_micros`.
pub(crate) fn timer_index_start_key(partition: PartitionId, fire_at_micros: i64) -> Vec<u8> {
    let mut key = partition_index_prefix(partition, IndexKind::Timer);
    key.extend_from_slice(&fire_at_micros.max(0).to_be_bytes());
    key
}

/// Parses an index key whose suffix is `flow_hash(8) ‖ run_id(16) ‖ item`, returning the run
/// and the item bytes.
pub(crate) fn parse_run_item_index_key<'a>(
    partition: PartitionId,
    kind: IndexKind,
    key: &'a [u8],
) -> Result<(RunLocation, &'a [u8]), EffectError> {
    let rest = strip_index_prefix(partition, kind, key)?;
    if rest.len() < 24 {
        return Err(corrupt_index("run item", key, "key is too short"));
    }
    Ok((parse_run_location(&rest[..24]), &rest[24..]))
}

/// Parses a Retention index key `delete_at_micros(8) ‖ flow_hash(8) ‖ run_id(16)`.
pub(crate) fn parse_retention_index_key(
    partition: PartitionId,
    key: &[u8],
) -> Result<(i64, RunLocation), EffectError> {
    let rest = strip_index_prefix(partition, IndexKind::Retention, key)?;
    if rest.len() != 32 {
        return Err(corrupt_index("Retention", key, "key has the wrong length"));
    }
    let delete_at_micros = i64::from_be_bytes(rest[..8].try_into().expect("8 bytes"));
    Ok((delete_at_micros, parse_run_location(&rest[8..])))
}

fn strip_index_prefix<'a>(
    partition: PartitionId,
    kind: IndexKind,
    key: &'a [u8],
) -> Result<&'a [u8], EffectError> {
    let prefix = partition_index_prefix(partition, kind);
    key.strip_prefix(prefix.as_slice())
        .ok_or_else(|| corrupt_index("partition index", key, "key has another prefix"))
}

fn parse_run_location(bytes: &[u8]) -> RunLocation {
    RunLocation {
        flow_hash: FlowHash(u64::from_be_bytes(bytes[..8].try_into().expect("8 bytes"))),
        run_id: RunId(bytes[8..24].try_into().expect("16 bytes")),
    }
}

fn corrupt_index(table: &'static str, key: &[u8], detail: &str) -> EffectError {
    EffectError::CorruptState {
        table,
        key: hex(key),
        detail: detail.to_owned(),
    }
}

/// Returns `bytes` as lowercase hexadecimal.
pub(crate) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut text, byte| {
            write!(text, "{byte:02x}").expect("writing to a String cannot fail");
            text
        })
}
