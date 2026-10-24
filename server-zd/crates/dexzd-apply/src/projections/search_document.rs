// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Search document and partition indexes derived from current run state.

use std::collections::{BTreeMap, BTreeSet};

use dexzd_keyspace::row_keys::{flow_list_index_key, tenant_run_index_key};
use dexzd_keyspace::{ColumnFamily, IndexKind, TenantId};
use dexzd_protocol::dex::{
    ActiveStepSearchMode, EncodedObject, FlowStatus, IndexConfig, IndexType, Value, value,
};
use dexzd_protocol::dexzd::v1::{
    SearchAttributeValue, SearchDocumentRecord, StepExecutionPhase, flow_command::Command,
};
use time::format_description::well_known::Rfc3339;
use time::{OffsetDateTime, UtcOffset};

use crate::ApplyError;
use crate::command_handlers::CommandScope;
use crate::command_handlers::run_cursor::RunCursor;
use crate::state_access::FlowTransaction;
use crate::values::load_stored_value;

const MAX_SEARCH_VALUE_BYTES: usize = 256 * 1024;

pub(crate) fn refresh_search_document(
    transaction: &mut FlowTransaction<'_>,
    cursor: &RunCursor,
    scope: &CommandScope<'_>,
) -> Result<(), ApplyError> {
    let old_document = cursor.rows.search_document(transaction)?;
    if old_document.is_some()
        && matches!(
            scope.command.command.as_ref(),
            Some(Command::StepAttemptStarted(_) | Command::StepAttemptHeartbeat(_))
        )
    {
        return Ok(());
    }
    let record = &cursor.record;
    let mode = record
        .flow_config
        .as_ref()
        .and_then(|config| config.active_step_search_mode)
        .and_then(|mode| ActiveStepSearchMode::try_from(mode).ok())
        .unwrap_or(ActiveStepSearchMode::Unspecified);
    let active_step_types: BTreeSet<String> = cursor
        .rows
        .step_executions(transaction)?
        .into_iter()
        .filter(|(_, step)| {
            if step.is_timeout_handler {
                return false;
            }
            let is_active = matches!(
                StepExecutionPhase::try_from(step.phase),
                Ok(StepExecutionPhase::WaitFor
                    | StepExecutionPhase::Waiting
                    | StepExecutionPhase::Execute)
            );
            let has_wait_for = !step
                .options
                .as_ref()
                .is_some_and(|options| options.skip_wait_for);
            is_active
                && match mode {
                    ActiveStepSearchMode::Disabled => false,
                    ActiveStepSearchMode::EnabledForAll => true,
                    ActiveStepSearchMode::Unspecified
                    | ActiveStepSearchMode::EnabledForStepsWithWaitFor => has_wait_for,
                }
        })
        .map(|(_, step)| step.step_type)
        .collect();
    let mut indexed: BTreeMap<String, SearchAttributeValue> = old_document
        .as_ref()
        .map(|document| {
            document
                .attributes
                .iter()
                .map(|attribute| (attribute.index_key.clone(), attribute.clone()))
                .collect()
        })
        .unwrap_or_default();
    if old_document.is_none() {
        let mut attributes = cursor.rows.attributes(transaction)?;
        attributes.sort_by(|(left_key, left), (right_key, right)| {
            (left.written_seq, left_key).cmp(&(right.written_seq, right_key))
        });
        for (attribute_key, attribute) in attributes {
            let Some(index_config) = attribute
                .index_config
                .as_ref()
                .filter(|config| config.enable)
            else {
                continue;
            };
            let Some(stored) = attribute.value.as_ref() else {
                continue;
            };
            let Some(value) = load_stored_value(transaction, cursor.rows, stored)? else {
                continue;
            };
            if let Some(indexed_value) =
                indexed_attribute_value(&attribute_key, &value, index_config)
            {
                indexed.insert(indexed_value.index_key.clone(), indexed_value);
            }
        }
    }
    for write in &cursor.indexed_attribute_writes {
        let Some(index_config) = write.index_config.as_ref() else {
            continue;
        };
        let index_key = index_key(&write.key, index_config);
        if is_index_null(write.value.as_ref()) {
            indexed.remove(&index_key);
            continue;
        }
        let Some(value) = write.value.as_ref() else {
            continue;
        };
        if let Some(indexed_value) = indexed_attribute_value(&write.key, value, index_config) {
            indexed.insert(index_key, indexed_value);
        }
    }
    let permissions = cursor.rows.permission_set(transaction)?.permissions;
    let parent_flow_id = record
        .parent
        .as_ref()
        .and_then(|parent| parent.parent.as_ref())
        .map_or_else(String::new, |parent| parent.flow_id.clone());
    let document = SearchDocumentRecord {
        version: cursor.seq,
        tenant_id: record.tenant_id,
        flow_id: record.flow_id.clone(),
        run_id: record.run_id.clone(),
        flow_type: record.flow_type.clone(),
        status: record.status,
        started_at_micros: record.started_at_micros,
        closed_at_micros: record.closed_at_micros,
        parent_flow_id,
        active_step_types: active_step_types.into_iter().collect(),
        work_queue_permissions: permissions,
        attributes: indexed.into_values().collect(),
    };
    if let Some(previous) = old_document.as_ref() {
        let mut comparable = document.clone();
        comparable.version = previous.version;
        if &comparable == previous {
            return Ok(());
        }
    }
    let partition = transaction.context().partition;
    let tenant_id = TenantId(record.tenant_id);
    let flow_hash = cursor.rows.flow_hash;
    let run_id = cursor.run_id();
    if let Some(previous) = old_document
        && (previous.status != document.status || previous.flow_type != document.flow_type)
    {
        transaction.delete(
            ColumnFamily::PartitionIndex,
            flow_list_index_key(
                partition,
                tenant_id,
                &previous.flow_type,
                previous.status as u8,
                flow_hash,
                run_id,
            ),
        );
    }
    // RunPurge must delete both SearchDocument and FlowList index keys.
    cursor.rows.put_search_document(transaction, &document);
    transaction.put(
        ColumnFamily::PartitionIndex,
        tenant_run_index_key(
            partition,
            IndexKind::SearchDocument,
            tenant_id,
            flow_hash,
            run_id,
        ),
        cursor.seq.to_be_bytes().to_vec(),
    );
    transaction.put(
        ColumnFamily::PartitionIndex,
        flow_list_index_key(
            partition,
            tenant_id,
            &record.flow_type,
            u8::try_from(record.status).unwrap_or(FlowStatus::Unspecified as u8),
            flow_hash,
            run_id,
        ),
        Vec::new(),
    );
    Ok(())
}

fn is_index_null(value: Option<&Value>) -> bool {
    match value {
        None => true,
        Some(value) => matches!(value.kind.as_ref(), Some(value::Kind::NullValue(_))),
    }
}

fn index_key(attribute_key: &str, index_config: &IndexConfig) -> String {
    if index_config.index_key.is_empty() {
        attribute_key.to_owned()
    } else {
        index_config.index_key.clone()
    }
}

fn indexed_attribute_value(
    attribute_key: &str,
    value: &Value,
    index_config: &IndexConfig,
) -> Option<SearchAttributeValue> {
    if value_payload_size(value) > MAX_SEARCH_VALUE_BYTES {
        return None;
    }
    let index_type = if index_config.r#type == IndexType::Unspecified as i32 {
        inferred_index_type(value)
    } else {
        IndexType::try_from(index_config.r#type).unwrap_or(IndexType::Unspecified)
    };
    Some(SearchAttributeValue {
        index_key: index_key(attribute_key, index_config),
        index_type: index_type as i32,
        value: Some(convert_index_value(value, index_type)?),
    })
}

fn value_payload_size(value: &Value) -> usize {
    match value.kind.as_ref() {
        Some(value::Kind::StringValue(text)) => text.len(),
        Some(value::Kind::ObjValue(object)) => object.payload.len(),
        _ => prost::Message::encoded_len(value),
    }
}

fn inferred_index_type(value: &Value) -> IndexType {
    match value.kind {
        Some(value::Kind::StringValue(_) | value::Kind::ObjValue(_)) => IndexType::Keyword,
        Some(value::Kind::IntValue(_)) => IndexType::Int,
        Some(value::Kind::DoubleValue(_)) => IndexType::Double,
        Some(value::Kind::BoolValue(_)) => IndexType::Bool,
        _ => IndexType::Unspecified,
    }
}

fn convert_index_value(value: &Value, index_type: IndexType) -> Option<Value> {
    use value::Kind;

    let kind = match (index_type, value.kind.as_ref()?) {
        (IndexType::Keyword | IndexType::Text, Kind::StringValue(text)) => {
            Kind::StringValue(text.clone())
        }
        (IndexType::Keyword | IndexType::Text, Kind::ObjValue(object)) => {
            Kind::StringValue(String::from_utf8(object.payload.clone()).ok()?)
        }
        (IndexType::Keyword | IndexType::Text, Kind::IntValue(number)) => {
            Kind::StringValue(number.to_string())
        }
        (IndexType::Keyword | IndexType::Text, Kind::DoubleValue(number)) => {
            Kind::StringValue(format_go_float(*number))
        }
        (IndexType::Keyword | IndexType::Text, Kind::BoolValue(boolean)) => {
            Kind::StringValue(boolean.to_string())
        }
        (IndexType::KeywordArray, Kind::StringValue(text)) => keyword_array_value(text.as_bytes())?,
        (IndexType::KeywordArray, Kind::ObjValue(object)) => keyword_array_value(&object.payload)?,
        (IndexType::Int, Kind::IntValue(number)) => Kind::IntValue(*number),
        (IndexType::Int, Kind::StringValue(text)) => Kind::IntValue(text.parse().ok()?),
        (IndexType::Double, Kind::DoubleValue(number)) => Kind::DoubleValue(*number),
        (IndexType::Double, Kind::StringValue(text)) => Kind::DoubleValue(text.parse().ok()?),
        (IndexType::Bool, Kind::BoolValue(boolean)) => Kind::BoolValue(*boolean),
        (IndexType::Bool, Kind::StringValue(text)) => Kind::BoolValue(parse_go_bool(text)?),
        (IndexType::Datetime, Kind::StringValue(text)) => {
            let timestamp = OffsetDateTime::parse(text, &Rfc3339).ok()?;
            Kind::StringValue(timestamp.to_offset(UtcOffset::UTC).format(&Rfc3339).ok()?)
        }
        _ => return None,
    };
    Some(Value { kind: Some(kind) })
}

fn parse_go_bool(text: &str) -> Option<bool> {
    match text {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Some(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Some(false),
        _ => None,
    }
}

fn format_go_float(number: f64) -> String {
    if number.is_nan() {
        return "NaN".to_owned();
    }
    if number == f64::INFINITY {
        return "+Inf".to_owned();
    }
    if number == f64::NEG_INFINITY {
        return "-Inf".to_owned();
    }
    let magnitude = number.abs();
    if magnitude >= 1_000_000.0 || (magnitude > 0.0 && magnitude < 0.0001) {
        let scientific = format!("{number:e}");
        let (mantissa, exponent) = scientific
            .split_once('e')
            .expect("Rust scientific formatting always includes an exponent");
        let exponent: i32 = exponent
            .parse()
            .expect("Rust scientific formatting always uses an integer exponent");
        return format!("{mantissa}e{exponent:+03}");
    }
    number.to_string()
}

fn keyword_array_value(bytes: &[u8]) -> Option<value::Kind> {
    let values: Vec<String> = serde_json::from_slice(bytes).ok()?;
    let payload = serde_json::to_vec(&values).ok()?;
    Some(value::Kind::ObjValue(EncodedObject {
        encoding: "json".to_owned(),
        payload,
    }))
}
