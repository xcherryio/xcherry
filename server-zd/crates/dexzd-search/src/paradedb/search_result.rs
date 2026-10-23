// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;

use dexzd_protocol::dex::{self, FlowStatus, IndexType};
use serde_json::Value as JsonValue;
use tokio_postgres::Row;

use super::wire_value::decode_wire_value;
use crate::SearchError;

pub(super) fn response_entry(row: &Row) -> Result<dex::SearchFlowsResponseEntry, SearchError> {
    let flow_type: String = read(row, "flow_type")?;
    let execution_status: String = read(row, "execution_status")?;
    let flow_status = match execution_status.as_str() {
        "Running" => FlowStatus::Running,
        "Completed" => FlowStatus::Completed,
        "Failed" => FlowStatus::Failed,
        "Canceled" => FlowStatus::Canceled,
        "Terminated" => FlowStatus::Terminated,
        "TimedOut" => FlowStatus::ServerSideTimeoutInternalOnly,
        "ContinuedAsNew" => FlowStatus::ContinuedAsNew,
        _ => {
            return Err(SearchError::Internal(format!(
                "ParadeDB run has unknown execution status {execution_status}"
            )));
        }
    };
    let start_time_ns: i64 = read(row, "start_time_ns")?;
    let close_time_ns: Option<i64> = read(row, "close_time_ns")?;
    let keyword = read_bucket(row, "sa_keyword")?;
    let text = read_bucket(row, "sa_text")?;
    let number = read_bucket(row, "sa_number")?;
    let number_types = read_bucket(row, "sa_number_types")?;
    let datetime = read_bucket(row, "sa_datetime")?;
    let boolean = read_bucket(row, "sa_bool")?;
    let wire = read_bucket(row, "sa_wire")?;
    let mut indexed_values = wire
        .into_iter()
        .map(|(field, value)| {
            let decoded = decode_wire_value(&field, &value)?;
            Ok((field, decoded))
        })
        .collect::<Result<BTreeMap<_, _>, SearchError>>()?;
    indexed_values.insert("FlowType".to_owned(), string_value(flow_type.clone()));
    if let Some(parent_flow_id) = read::<Option<String>>(row, "dex_parent_flow_id")? {
        indexed_values.insert("DexParentFlowID".to_owned(), string_value(parent_flow_id));
    }
    for (name, column) in [
        ("ActiveStepTypes", "active_step_types"),
        ("DexWorkQueuePermissions", "dex_work_queue_permissions"),
    ] {
        match read::<Option<Vec<String>>>(row, column)? {
            Some(values) if !values.is_empty() => {
                indexed_values.insert(name.to_owned(), string_array_value(&values)?);
            }
            _ => {
                indexed_values.remove(name);
            }
        }
    }
    for (field, value) in keyword {
        let index_type = match value {
            JsonValue::String(_) => IndexType::Keyword,
            JsonValue::Array(_) => IndexType::KeywordArray,
            _ => return Err(invalid_attribute(&field)),
        };
        indexed_values
            .entry(field.clone())
            .or_insert(attribute_value(&field, index_type, &value)?);
    }
    for (bucket, index_type) in [
        (text, IndexType::Text),
        (datetime, IndexType::Datetime),
        (boolean, IndexType::Bool),
    ] {
        for (field, value) in bucket {
            indexed_values
                .entry(field.clone())
                .or_insert(attribute_value(&field, index_type, &value)?);
        }
    }
    for (field, value) in number {
        let encoded_type = number_types
            .get(&field)
            .and_then(JsonValue::as_i64)
            .ok_or_else(|| invalid_attribute(&field))?;
        let encoded_type = i32::try_from(encoded_type).map_err(|_| invalid_attribute(&field))?;
        let index_type =
            IndexType::try_from(encoded_type).map_err(|_| invalid_attribute(&field))?;
        if !matches!(index_type, IndexType::Int | IndexType::Double) {
            return Err(invalid_attribute(&field));
        }
        indexed_values
            .entry(field.clone())
            .or_insert(attribute_value(&field, index_type, &value)?);
    }
    Ok(dex::SearchFlowsResponseEntry {
        flow_id: read(row, "workflow_id")?,
        run_id: read(row, "run_id")?,
        indexed_attributes: indexed_values
            .into_iter()
            .map(|(key, value)| dex::Kv {
                key,
                value: Some(value),
            })
            .collect(),
        flow_type,
        flow_status: flow_status.into(),
        start_time: Some(timestamp(start_time_ns)),
        close_time: close_time_ns.map(timestamp),
    })
}

fn read<'a, T>(row: &'a Row, column: &str) -> Result<T, SearchError>
where
    T: tokio_postgres::types::FromSql<'a>,
{
    row.try_get(column)
        .map_err(|error| SearchError::Internal(format!("read ParadeDB column {column}: {error}")))
}

fn read_bucket(row: &Row, column: &str) -> Result<BTreeMap<String, JsonValue>, SearchError> {
    let encoded: String = read(row, column)?;
    serde_json::from_str(&encoded)
        .map_err(|error| SearchError::Internal(format!("decode ParadeDB {column}: {error}")))
}

fn attribute_value(
    field: &str,
    index_type: IndexType,
    value: &JsonValue,
) -> Result<dex::Value, SearchError> {
    use dex::value::Kind;

    let kind = match (index_type, value) {
        (IndexType::Keyword | IndexType::Text, JsonValue::String(value)) => {
            Kind::StringValue(value.clone())
        }
        (IndexType::KeywordArray, JsonValue::Array(values)) => {
            let values = values
                .iter()
                .map(|value| value.as_str().ok_or_else(|| invalid_attribute(field)))
                .collect::<Result<Vec<_>, _>>()?;
            return string_array_value(&values);
        }
        (IndexType::Int, JsonValue::Number(value)) => {
            Kind::IntValue(value.as_i64().ok_or_else(|| invalid_attribute(field))?)
        }
        (IndexType::Double, JsonValue::Number(value)) => {
            Kind::DoubleValue(value.as_f64().ok_or_else(|| invalid_attribute(field))?)
        }
        (IndexType::Datetime, JsonValue::Number(value)) => {
            let nanos = value.as_i64().ok_or_else(|| invalid_attribute(field))?;
            Kind::IntValue(nanos)
        }
        (IndexType::Bool, JsonValue::Bool(value)) => Kind::BoolValue(*value),
        _ => return Err(invalid_attribute(field)),
    };
    Ok(dex::Value { kind: Some(kind) })
}

fn invalid_attribute(field: &str) -> SearchError {
    SearchError::Internal(format!(
        "ParadeDB field {field} has a mismatched indexed value"
    ))
}

fn string_value(value: String) -> dex::Value {
    dex::Value {
        kind: Some(dex::value::Kind::StringValue(value)),
    }
}

fn string_array_value(values: &[impl AsRef<str>]) -> Result<dex::Value, SearchError> {
    let strings: Vec<&str> = values.iter().map(AsRef::as_ref).collect();
    let payload = serde_json::to_vec(&strings)
        .map_err(|error| SearchError::Internal(format!("encode search array: {error}")))?;
    Ok(dex::Value {
        kind: Some(dex::value::Kind::ObjValue(dex::EncodedObject {
            encoding: "json".into(),
            payload,
        })),
    })
}

fn timestamp(nanoseconds: i64) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: nanoseconds.div_euclid(1_000_000_000),
        nanos: nanoseconds.rem_euclid(1_000_000_000) as i32,
    }
}
