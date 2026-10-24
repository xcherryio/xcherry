// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;

use dexzd_keyspace::{RunId, TenantId};
use dexzd_protocol::dex::{self, FlowStatus, IndexType};
use dexzd_protocol::dexzd::v1::SearchDocumentRecord;
use dexzd_visibility_query::{SearchDocument, SearchValue};
use prost::Message;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::SearchError;
use crate::page_token::PagePosition;

pub(crate) struct IndexedDocument {
    record: SearchDocumentRecord,
    run_id: RunId,
    status: FlowStatus,
    attributes: BTreeMap<String, SearchValue>,
    start_time_ns: i64,
    close_time_ns: Option<i64>,
}

impl IndexedDocument {
    pub(crate) fn decode(
        encoded: &[u8],
        tenant_id: TenantId,
        run_id: RunId,
    ) -> Result<Self, SearchError> {
        let record = SearchDocumentRecord::decode(encoded)
            .map_err(|error| SearchError::Internal(format!("decode search document: {error}")))?;
        if record.tenant_id != tenant_id.0 || record.run_id != run_id.as_bytes() {
            return Err(SearchError::Internal(
                "search document identity differs from index".into(),
            ));
        }
        let status = FlowStatus::try_from(record.status)
            .map_err(|_| SearchError::Internal("search document has unknown status".into()))?;
        if status == FlowStatus::Unspecified {
            return Err(SearchError::Internal(
                "search document has unspecified status".into(),
            ));
        }
        let start_time_ns = micros_to_nanos(record.started_at_micros)?;
        let close_time_ns = (record.closed_at_micros != 0)
            .then(|| micros_to_nanos(record.closed_at_micros))
            .transpose()?;
        let mut attributes = BTreeMap::new();
        for attribute in &record.attributes {
            let Some(value) = attribute.value.as_ref() else {
                return Err(SearchError::Internal(format!(
                    "search attribute {} has no value",
                    attribute.index_key
                )));
            };
            let index_type = IndexType::try_from(attribute.index_type).map_err(|_| {
                SearchError::Internal(format!(
                    "search attribute {} has unknown index type",
                    attribute.index_key
                ))
            })?;
            if let Some(decoded) = decode_attribute(index_type, value, &attribute.index_key)? {
                attributes.insert(attribute.index_key.clone(), decoded);
            }
        }
        Ok(Self {
            record,
            run_id,
            status,
            attributes,
            start_time_ns,
            close_time_ns,
        })
    }

    pub(super) fn page_position(&self) -> PagePosition {
        PagePosition {
            is_running: self.status == FlowStatus::Running,
            close_time_ns: self.close_time_ns,
            start_time_ns: self.start_time_ns,
            run_id: self.run_id,
        }
    }

    pub(super) fn response_entry(&self) -> dex::SearchFlowsResponseEntry {
        let mut indexed_values: BTreeMap<String, dex::Value> = self
            .record
            .attributes
            .iter()
            .filter_map(|attribute| {
                attribute
                    .value
                    .as_ref()
                    .map(|value| (attribute.index_key.clone(), value.clone()))
            })
            .collect();
        indexed_values.insert("FlowType".into(), string_value(&self.record.flow_type));
        if !self.record.parent_flow_id.is_empty() {
            indexed_values.insert(
                "DexParentFlowID".into(),
                string_value(&self.record.parent_flow_id),
            );
        }
        for (name, values) in [
            ("ActiveStepTypes", &self.record.active_step_types),
            (
                "DexWorkQueuePermissions",
                &self.record.work_queue_permissions,
            ),
        ] {
            if values.is_empty() {
                indexed_values.remove(name);
            } else {
                indexed_values.insert(name.into(), string_array_value(values));
            }
        }
        dex::SearchFlowsResponseEntry {
            flow_id: self.record.flow_id.clone(),
            run_id: self.run_id.to_string(),
            indexed_attributes: indexed_values
                .into_iter()
                .map(|(key, value)| dex::Kv {
                    key,
                    value: Some(value),
                })
                .collect(),
            flow_type: self.record.flow_type.clone(),
            flow_status: self.status.into(),
            start_time: Some(timestamp(self.record.started_at_micros)),
            close_time: (self.record.closed_at_micros != 0)
                .then(|| timestamp(self.record.closed_at_micros)),
        }
    }
}

impl SearchDocument for IndexedDocument {
    fn field_value(&self, field: &str) -> Option<SearchValue> {
        match field {
            "WorkflowId" => Some(SearchValue::String(self.record.flow_id.clone())),
            "RunId" => Some(SearchValue::String(self.run_id.to_string())),
            "WorkflowType" => Some(SearchValue::String("Engine".into())),
            "FlowType" => Some(SearchValue::String(self.record.flow_type.clone())),
            "ExecutionStatus" => Some(SearchValue::String(status_name(self.status).into())),
            "StartTime" => Some(SearchValue::Datetime(self.start_time_ns)),
            "CloseTime" => self.close_time_ns.map(SearchValue::Datetime),
            "DexParentFlowID" => (!self.record.parent_flow_id.is_empty())
                .then(|| SearchValue::String(self.record.parent_flow_id.clone())),
            "ActiveStepTypes" => (!self.record.active_step_types.is_empty())
                .then(|| SearchValue::StringArray(self.record.active_step_types.clone())),
            "DexWorkQueuePermissions" => (!self.record.work_queue_permissions.is_empty())
                .then(|| SearchValue::StringArray(self.record.work_queue_permissions.clone())),
            _ => self.attributes.get(field).cloned(),
        }
    }
}

fn decode_attribute(
    index_type: IndexType,
    value: &dex::Value,
    name: &str,
) -> Result<Option<SearchValue>, SearchError> {
    use dex::value::Kind;

    let Some(kind) = value.kind.as_ref() else {
        return Err(invalid_attribute(name));
    };
    let decoded = match (index_type, kind) {
        (_, Kind::NullValue(_)) => return Ok(None),
        (IndexType::Keyword | IndexType::Text, Kind::StringValue(value)) => {
            SearchValue::String(value.clone())
        }
        (IndexType::KeywordArray, Kind::ObjValue(object)) if object.encoding == "json" => {
            let values: Vec<String> =
                serde_json::from_slice(&object.payload).map_err(|_| invalid_attribute(name))?;
            SearchValue::StringArray(values)
        }
        (IndexType::Int, Kind::IntValue(value)) => SearchValue::Int(*value),
        (IndexType::Double, Kind::DoubleValue(value)) if value.is_finite() => {
            SearchValue::Double(*value)
        }
        (IndexType::Bool, Kind::BoolValue(value)) => SearchValue::Bool(*value),
        (IndexType::Datetime, Kind::StringValue(value)) => {
            let timestamp = OffsetDateTime::parse(value, &Rfc3339)
                .map_err(|_| invalid_attribute(name))?
                .unix_timestamp_nanos();
            SearchValue::Datetime(i64::try_from(timestamp).map_err(|_| invalid_attribute(name))?)
        }
        (IndexType::Datetime, Kind::IntValue(value)) => SearchValue::Datetime(*value),
        _ => return Err(invalid_attribute(name)),
    };
    Ok(Some(decoded))
}

fn invalid_attribute(name: &str) -> SearchError {
    SearchError::Internal(format!(
        "search attribute {name} has a mismatched indexed value"
    ))
}

fn string_value(value: &str) -> dex::Value {
    dex::Value {
        kind: Some(dex::value::Kind::StringValue(value.to_owned())),
    }
}

fn string_array_value(values: &[String]) -> dex::Value {
    dex::Value {
        kind: Some(dex::value::Kind::ObjValue(dex::EncodedObject {
            encoding: "json".into(),
            payload: serde_json::to_vec(values)
                .expect("serializing a string array to an in-memory JSON buffer cannot fail"),
        })),
    }
}

fn micros_to_nanos(micros: i64) -> Result<i64, SearchError> {
    micros.checked_mul(1_000).ok_or_else(|| {
        SearchError::Internal("search document timestamp exceeds nanosecond range".into())
    })
}

fn timestamp(micros: i64) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: micros.div_euclid(1_000_000),
        nanos: (micros.rem_euclid(1_000_000) * 1_000) as i32,
    }
}

fn status_name(status: FlowStatus) -> &'static str {
    match status {
        FlowStatus::Running => "Running",
        FlowStatus::Completed => "Completed",
        FlowStatus::Failed => "Failed",
        FlowStatus::Canceled => "Canceled",
        FlowStatus::Terminated => "Terminated",
        FlowStatus::ServerSideTimeoutInternalOnly => "TimedOut",
        FlowStatus::ContinuedAsNew => "ContinuedAsNew",
        FlowStatus::Unspecified => "Unspecified",
    }
}
