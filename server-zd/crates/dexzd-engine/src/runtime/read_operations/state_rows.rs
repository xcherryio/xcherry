// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Decoding of Flow state rows for runtime reads (plan 5.5, `state.proto`).

use dexzd_apply::{ApplyContext, FlowStateReader};
use dexzd_keyspace::{
    ColumnFamily, FlowHash, RunId, RunTable, VALUE_REFERENCE_PREFIX, ValueId, ValueReference,
    flow_directory_key, run_key, run_table_prefix,
};
use dexzd_protocol::dex::{FlowStatus, Value, value};
use dexzd_protocol::dexzd::v1::{
    FlowDirectoryRecord, HardDeleteObjectReference, RunRecord, StoredValue, ValueRecord,
    stored_value,
};
use prost::Message;

use crate::runtime::{FlowAddress, RuntimeError};

const SCAN_PAGE_ROWS: usize = 256;

/// A run and its run record.
#[derive(Debug, Clone)]
pub(crate) struct ResolvedRun {
    pub(crate) run_id: RunId,
    pub(crate) record: RunRecord,
}

/// A stored value as a client or Worker receives it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum WireValue {
    /// The value itself, or a value reference for values above the inline threshold.
    Value(Value),
    /// A hard-delete Attribute object, read from object storage.
    HardDeleteObject(HardDeleteObjectReference),
}

pub(crate) fn decode_row<RowMessage: Message + Default>(
    state: &dyn FlowStateReader,
    column_family: ColumnFamily,
    key: &[u8],
    table: &str,
) -> Result<Option<RowMessage>, RuntimeError> {
    let Some(bytes) = state.get(column_family, key)? else {
        return Ok(None);
    };
    RowMessage::decode(bytes.as_slice())
        .map(Some)
        .map_err(|error| RuntimeError::corrupt_row(table, error))
}

/// Returns every row of `table` in the run whose key suffix starts with `suffix_prefix`.
pub(crate) fn scan_run_rows<RowMessage: Message + Default>(
    state: &dyn FlowStateReader,
    flow_hash: FlowHash,
    run_id: RunId,
    table: RunTable,
    suffix_prefix: &[u8],
) -> Result<Vec<(Vec<u8>, RowMessage)>, RuntimeError> {
    let table_prefix_length = run_table_prefix(flow_hash, run_id, table).len();
    let start = run_key(flow_hash, run_id, table, suffix_prefix);
    let end = prefix_successor(&start);
    let mut rows = Vec::new();
    let mut page_start = start;
    loop {
        let page = state.scan(table.column_family(), &page_start, &end, SCAN_PAGE_ROWS)?;
        let is_last_page = page.len() < SCAN_PAGE_ROWS;
        for (key, bytes) in page {
            let row = RowMessage::decode(bytes.as_slice())
                .map_err(|error| RuntimeError::corrupt_row(&format!("{table:?}"), error))?;
            page_start.clone_from(&key);
            page_start.push(0);
            rows.push((key[table_prefix_length..].to_vec(), row));
        }
        if is_last_page {
            return Ok(rows);
        }
    }
}

/// Parses a client-supplied run ID; empty means the current run.
pub(crate) fn parse_run_id_text(run_id: &str) -> Result<Option<RunId>, RuntimeError> {
    if run_id.is_empty() {
        return Ok(None);
    }
    run_id
        .parse()
        .map(Some)
        .map_err(|_| RuntimeError::invalid_argument(format!("run ID {run_id:?} is not valid")))
}

pub(crate) fn read_directory(
    state: &dyn FlowStateReader,
    context: ApplyContext,
    flow: &FlowAddress,
) -> Result<Option<FlowDirectoryRecord>, RuntimeError> {
    decode_row(
        state,
        ColumnFamily::Flow,
        &flow_directory_key(context.flow_hash, flow.tenant_id, &flow.flow_id),
        "Flow directory",
    )
}

/// Returns the run a command targets: the explicit run ID, or the newest run of the Flow.
///
/// Returns `NOT_FOUND` + `FLOW_NOT_EXISTS` when the Flow has no run.
pub(crate) fn resolve_command_run(
    state: &dyn FlowStateReader,
    context: ApplyContext,
    flow: &FlowAddress,
    run_id: &str,
) -> Result<RunId, RuntimeError> {
    if let Some(run_id) = parse_run_id_text(run_id)? {
        return Ok(run_id);
    }
    let directory = read_directory(state, context, flow)?;
    let newest = directory.and_then(|directory| directory.runs.last().cloned());
    let Some(newest) = newest else {
        return Err(RuntimeError::flow_not_found(&flow.flow_id));
    };
    RunId::from_slice(&newest.run_id)
        .map_err(|error| RuntimeError::corrupt_row("Flow directory", error))
}

/// Returns the run and its record that a read targets.
///
/// Returns `NOT_FOUND` + `FLOW_NOT_EXISTS` when the run does not exist or belongs to another
/// Flow.
pub(crate) fn read_run(
    state: &dyn FlowStateReader,
    context: ApplyContext,
    flow: &FlowAddress,
    run_id: &str,
) -> Result<ResolvedRun, RuntimeError> {
    let run_id = resolve_command_run(state, context, flow, run_id)?;
    let record: Option<RunRecord> = decode_row(
        state,
        ColumnFamily::Flow,
        &run_key(context.flow_hash, run_id, RunTable::RunRecord, &[]),
        "run",
    )?;
    match record {
        Some(record) if record.flow_id == flow.flow_id && record.tenant_id == flow.tenant_id.0 => {
            Ok(ResolvedRun { run_id, record })
        }
        _ => Err(RuntimeError::flow_not_found(&flow.flow_id)),
    }
}

/// Converts a stored value to its wire form for `run_id`.
pub(crate) fn wire_value(stored: &StoredValue, run_id: RunId) -> Option<WireValue> {
    match stored.kind.as_ref()? {
        stored_value::Kind::InlineValue(value) => Some(WireValue::Value(value.clone())),
        stored_value::Kind::TableValue(reference) => {
            let value_id = reference.value_id.as_ref()?;
            let text = ValueReference {
                run_id,
                value_id: ValueId {
                    run_seq: value_id.run_seq,
                    index_in_command: value_id.index_in_command,
                },
            }
            .to_string();
            let kind = if reference.is_string_value {
                value::Kind::InternalBlobIdForStringValue(text)
            } else {
                value::Kind::InternalBlobIdForObjValue(text)
            };
            Some(WireValue::Value(Value { kind: Some(kind) }))
        }
        stored_value::Kind::HardDeleteObject(object) => {
            Some(WireValue::HardDeleteObject(object.clone()))
        }
    }
}

/// Converts a stored value to its wire form, loading value-table rows when `is_hydrating`.
///
/// Returns `None` for an empty stored value, and `Internal` when a held value-table row is
/// missing.
pub(crate) fn response_value(
    state: &dyn FlowStateReader,
    flow_hash: FlowHash,
    run_id: RunId,
    stored: &StoredValue,
    is_hydrating: bool,
) -> Result<Option<WireValue>, RuntimeError> {
    if !is_hydrating || !matches!(stored.kind, Some(stored_value::Kind::TableValue(_))) {
        return Ok(wire_value(stored, run_id));
    }
    let value = load_complete_value(state, flow_hash, run_id, stored)?.ok_or_else(|| {
        RuntimeError::Internal {
            detail: format!("a held value of run {run_id} has no value row"),
        }
    })?;
    Ok(Some(WireValue::Value(value)))
}

/// Replaces a value reference of `run_id` with the referenced value; other values, including
/// hard-delete references, are left unchanged.
///
/// Returns false when `value` is a value reference that does not name a live value of the run.
pub(crate) fn hydrate_value_reference(
    state: &dyn FlowStateReader,
    flow_hash: FlowHash,
    run_id: RunId,
    value: &mut Value,
) -> Result<bool, RuntimeError> {
    let Some(
        value::Kind::InternalBlobIdForStringValue(text)
        | value::Kind::InternalBlobIdForObjValue(text),
    ) = value.kind.as_ref()
    else {
        return Ok(true);
    };
    if !text.starts_with(VALUE_REFERENCE_PREFIX) {
        return Ok(true);
    }
    let Ok(reference) = text.parse::<ValueReference>() else {
        return Ok(false);
    };
    if reference.run_id != run_id {
        return Ok(false);
    }
    let Some(loaded) = read_table_value(state, flow_hash, run_id, reference.value_id)? else {
        return Ok(false);
    };
    *value = loaded;
    Ok(true)
}

/// Returns the complete value of an inline or value-table stored value; `None` for a missing
/// table row or a hard-delete object.
pub(crate) fn load_complete_value(
    state: &dyn FlowStateReader,
    flow_hash: FlowHash,
    run_id: RunId,
    stored: &StoredValue,
) -> Result<Option<Value>, RuntimeError> {
    match stored.kind.as_ref() {
        Some(stored_value::Kind::InlineValue(value)) => Ok(Some(value.clone())),
        Some(stored_value::Kind::TableValue(reference)) => {
            let Some(value_id) = reference.value_id.as_ref() else {
                return Ok(None);
            };
            read_table_value(
                state,
                flow_hash,
                run_id,
                ValueId {
                    run_seq: value_id.run_seq,
                    index_in_command: value_id.index_in_command,
                },
            )
        }
        Some(stored_value::Kind::HardDeleteObject(_)) | None => Ok(None),
    }
}

pub(crate) fn read_table_value(
    state: &dyn FlowStateReader,
    flow_hash: FlowHash,
    run_id: RunId,
    value_id: ValueId,
) -> Result<Option<Value>, RuntimeError> {
    let record: Option<ValueRecord> = decode_row(
        state,
        ColumnFamily::Value,
        &run_key(
            flow_hash,
            run_id,
            RunTable::Value,
            &value_id.to_key_suffix(),
        ),
        "value",
    )?;
    Ok(record.and_then(|record| record.value))
}

pub(crate) fn is_closed_status(status: i32) -> bool {
    !matches!(
        FlowStatus::try_from(status),
        Ok(FlowStatus::Running | FlowStatus::Unspecified) | Err(_)
    )
}

fn prefix_successor(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last != u8::MAX {
            end.push(last + 1);
            return end;
        }
    }
    vec![u8::MAX; prefix.len() + 1]
}
