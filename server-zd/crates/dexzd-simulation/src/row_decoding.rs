// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_keyspace::{ColumnFamily, FLOW_DIRECTORY_KEY_TAG, RunTable, parse_run_key};
use dexzd_protocol::dexzd::v1::{
    AttemptRecord, AttributeRecord, ChannelMessageRecord, ChannelMetaRecord, ChannelWaiterRecord,
    CheckpointRecord, FlowDirectoryRecord, HardDeleteObjectRecord, HistoryRecord,
    HistoryRowMetadata, InboxDedupRecord, LockRecord, OutboxRecord, PermissionSetRecord,
    RequestDedupRecord, RunRecord, SearchDocumentRecord, StepCounterRecord, StepExecutionRecord,
    SubFlowResultRecord, TimerRecord, ValueBirthRecord, ValueGarbageRecord, ValueRecord,
};
use prost::Message;

/// A stored row that does not decode as the message its key names.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("row {column_family:?} {key_hex} does not decode as {table}: {detail}")]
pub struct RowDecodeError {
    /// Column family of the row.
    pub column_family: ColumnFamily,
    /// Hex-encoded key.
    pub key_hex: String,
    /// The table the key names.
    pub table: &'static str,
    /// Decoder message.
    pub detail: String,
}

/// Decodes a row as the message its key names and returns its encoding again.
///
/// Returns rows without a message, such as holder and index rows, unchanged.
pub fn reencode_row(
    column_family: ColumnFamily,
    key: &[u8],
    value: &[u8],
) -> Result<Vec<u8>, RowDecodeError> {
    let error = |table: &'static str, detail: String| RowDecodeError {
        column_family,
        key_hex: hex(key),
        table,
        detail,
    };
    if !column_family.is_flow_scoped() {
        return Ok(value.to_vec());
    }
    if column_family == ColumnFamily::Flow && key.get(8) == Some(&FLOW_DIRECTORY_KEY_TAG) {
        return reencode::<FlowDirectoryRecord>(value).map_err(|detail| error("directory", detail));
    }
    let parsed =
        parse_run_key(key).map_err(|parse_error| error("run key", parse_error.to_string()))?;
    let table_name = table_name(parsed.table);
    let result = match parsed.table {
        RunTable::RunRecord => reencode::<RunRecord>(value),
        RunTable::AttemptRecord => reencode::<AttemptRecord>(value),
        RunTable::Attribute => reencode::<AttributeRecord>(value),
        RunTable::ChannelMessage => reencode::<ChannelMessageRecord>(value),
        RunTable::ChannelMeta => reencode::<ChannelMetaRecord>(value),
        RunTable::StepExecution => reencode::<StepExecutionRecord>(value),
        RunTable::StepCounter => reencode::<StepCounterRecord>(value),
        RunTable::LockTable => reencode::<LockRecord>(value),
        RunTable::Waiter => reencode::<ChannelWaiterRecord>(value),
        RunTable::Timer => reencode::<TimerRecord>(value),
        RunTable::Outbox => reencode::<OutboxRecord>(value),
        RunTable::InboxDedup => reencode::<InboxDedupRecord>(value),
        RunTable::RequestDedup => reencode::<RequestDedupRecord>(value),
        RunTable::SubFlowResult => reencode::<SubFlowResultRecord>(value),
        RunTable::PermissionSet => reencode::<PermissionSetRecord>(value),
        RunTable::Value => reencode::<ValueRecord>(value),
        RunTable::ValueBirth => reencode::<ValueBirthRecord>(value),
        RunTable::ValueGarbage => reencode::<ValueGarbageRecord>(value),
        RunTable::HardDeleteObject => reencode::<HardDeleteObjectRecord>(value),
        RunTable::HistoryRecord => reencode::<HistoryRecord>(value),
        RunTable::HistoryRowMetadata => reencode::<HistoryRowMetadata>(value),
        RunTable::Checkpoint => reencode::<CheckpointRecord>(value),
        RunTable::SearchDocument => reencode::<SearchDocumentRecord>(value),
        RunTable::HistoryRowMetadata => reencode::<HistoryRowMetadata>(value),
        RunTable::ValueHolder | RunTable::RequestDedupOrder => Ok(value.to_vec()),
    };
    result.map_err(|detail| error(table_name, detail))
}

fn reencode<RowMessage: Message + Default>(value: &[u8]) -> Result<Vec<u8>, String> {
    RowMessage::decode(value)
        .map(|message| message.encode_to_vec())
        .map_err(|decode_error| decode_error.to_string())
}

fn table_name(table: RunTable) -> &'static str {
    match table {
        RunTable::RunRecord => "RunRecord",
        RunTable::AttemptRecord => "AttemptRecord",
        RunTable::Attribute => "Attribute",
        RunTable::ChannelMessage => "ChannelMessage",
        RunTable::ChannelMeta => "ChannelMeta",
        RunTable::StepExecution => "StepExecution",
        RunTable::StepCounter => "StepCounter",
        RunTable::LockTable => "LockTable",
        RunTable::Waiter => "Waiter",
        RunTable::Timer => "Timer",
        RunTable::Outbox => "Outbox",
        RunTable::InboxDedup => "InboxDedup",
        RunTable::RequestDedup => "RequestDedup",
        RunTable::SubFlowResult => "SubFlowResult",
        RunTable::PermissionSet => "PermissionSet",
        RunTable::Value => "Value",
        RunTable::ValueBirth => "ValueBirth",
        RunTable::ValueHolder => "ValueHolder",
        RunTable::ValueGarbage => "ValueGarbage",
        RunTable::HardDeleteObject => "HardDeleteObject",
        RunTable::HistoryRecord => "HistoryRecord",
        RunTable::HistoryRowMetadata => "HistoryRowMetadata",
        RunTable::Checkpoint => "Checkpoint",
        RunTable::SearchDocument => "SearchDocument",
        RunTable::RequestDedupOrder => "RequestDedupOrder",
        RunTable::HistoryRowMetadata => "HistoryRowMetadata",
    }
}

/// Lower-case hex encoding for diagnostics.
pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
