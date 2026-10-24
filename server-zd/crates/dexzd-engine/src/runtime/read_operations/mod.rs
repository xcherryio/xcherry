// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Client reads. They see only committed state, never the speculative overlay (plan 6.4).

mod permission_json;
mod read_model_adapter;
pub(crate) mod state_rows;

use std::collections::BTreeMap;

use dexzd_keyspace::row_keys::channel_name_prefix;
use dexzd_keyspace::{ColumnFamily, RunId, RunTable, TenantId, ValueReference, run_key};
use dexzd_protocol::dex::{
    ChannelMessage, EncodedObject, FlowExecutionId, GetAttributesRequest, GetAttributesResponse,
    GetChannelMessagesRequest, GetChannelMessagesResponse, GetFlowStateRequest,
    GetFlowStateResponse, GetFlowSummaryRequest, GetFlowSummaryResponse, GetHistoryEventsRequest,
    GetHistoryEventsResponse, Kv, LoadBlobsRequest, LoadBlobsResponse, Value, value,
};
use dexzd_protocol::dexzd::v1::{
    AttributeRecord, ChannelMessageRecord, HardDeleteObjectReference, PermissionSetRecord,
};

use crate::runtime::read_operations::state_rows::{
    WireValue, decode_row, read_directory, read_run, read_table_value, scan_run_rows, wire_value,
};
use crate::runtime::{FlowAddress, FlowRuntimeRegistry, RuntimeError};

/// Attribute key of the server-maintained work queue permission projection.
pub(crate) const WORK_QUEUE_PERMISSIONS_KEY: &str = "DexWorkQueuePermissions";

impl FlowRuntimeRegistry {
    /// Returns the requested Attributes of a run, sorted by key; missing keys are omitted.
    ///
    /// Values above the inline threshold are returned as value references; hard-delete values
    /// are always returned as content and omitted once deleted.
    pub async fn get_attributes(
        &self,
        tenant_id: TenantId,
        request: GetAttributesRequest,
    ) -> Result<GetAttributesResponse, RuntimeError> {
        if request.flow_id.is_empty() {
            return Err(RuntimeError::invalid_argument("flow ID is required"));
        }
        self.check_running()?;
        let flow = FlowAddress::new(tenant_id, request.flow_id.clone());
        let (run_id, records) = self.read_attribute_records(&flow, &request)?;
        let mut attributes = Vec::with_capacity(records.len());
        for (key, record) in records {
            let Some(value) = record
                .value
                .as_ref()
                .and_then(|stored| wire_value(stored, run_id))
            else {
                continue;
            };
            let value = match value {
                WireValue::Value(value) => Some(value),
                WireValue::HardDeleteObject(object) => {
                    self.read_hard_delete_attribute(&flow, run_id, &key, object)
                        .await?
                }
            };
            if let Some(value) = value {
                attributes.push(Kv {
                    key,
                    value: Some(value),
                });
            }
        }
        Ok(GetAttributesResponse { attributes })
    }

    /// Returns every pending message of one Channel of a run, oldest first.
    pub async fn get_channel_messages(
        &self,
        tenant_id: TenantId,
        request: GetChannelMessagesRequest,
    ) -> Result<GetChannelMessagesResponse, RuntimeError> {
        if request.flow_id.is_empty() || request.channel_name.is_empty() {
            return Err(RuntimeError::invalid_argument(
                "flow ID and channel name are required",
            ));
        }
        self.check_running()?;
        let flow = FlowAddress::new(tenant_id, request.flow_id.clone());
        let context = self.flow_context(&flow);
        let committed = self.inner.dependencies.committer.committed_view()?;
        let run = read_run(committed.as_ref(), context, &flow, &request.run_id)?;
        let suffix_prefix = channel_name_prefix(&request.channel_name);
        let rows: Vec<(Vec<u8>, ChannelMessageRecord)> = scan_run_rows(
            committed.as_ref(),
            context.flow_hash,
            run.run_id,
            RunTable::ChannelMessage,
            &suffix_prefix,
        )?;
        let messages = rows
            .into_iter()
            .map(|(_, record)| ChannelMessage {
                value: record.value.as_ref().and_then(|stored| {
                    match wire_value(stored, run.run_id)? {
                        WireValue::Value(value) => Some(value),
                        WireValue::HardDeleteObject(_) => None,
                    }
                }),
                channel_name: record.channel_name,
                message_id: record.message_id,
            })
            .collect();
        Ok(GetChannelMessagesResponse { messages })
    }

    /// Returns the summary of a run: IDs, request ID, Flow type, status, and times.
    pub async fn get_flow_summary(
        &self,
        tenant_id: TenantId,
        request: GetFlowSummaryRequest,
    ) -> Result<GetFlowSummaryResponse, RuntimeError> {
        if request.flow_id.is_empty() {
            return Err(RuntimeError::invalid_argument("flow ID is required"));
        }
        self.check_running()?;
        let flow = FlowAddress::new(tenant_id, request.flow_id.clone());
        let context = self.flow_context(&flow);
        let committed = self.inner.dependencies.committer.committed_view()?;
        let run = read_run(committed.as_ref(), context, &flow, &request.run_id)?;
        let run_id_text = run.run_id.to_string();
        let record = run.record;
        Ok(GetFlowSummaryResponse {
            flow_execution_id: Some(FlowExecutionId {
                flow_id: request.flow_id,
                run_id: run_id_text.clone(),
            }),
            // Time travel keeps the run ID, so the first run of the chain is the run itself.
            first_run_id: run_id_text,
            request_id: record.start_request_id,
            flow_type: record.flow_type,
            flow_status: record.status,
            run_attempt: i32::try_from(record.current_attempt)
                .map_err(|error| RuntimeError::corrupt_row("run", error))?,
            start_time: Some(timestamp_from_micros(record.started_at_micros)),
            close_time: (record.closed_at_micros > 0)
                .then(|| timestamp_from_micros(record.closed_at_micros)),
        })
    }

    /// Returns the values behind value references, keyed by reference text.
    ///
    /// Every reference must belong to a run of the entry's Flow. Values that no longer exist
    /// are omitted.
    pub async fn load_blobs(
        &self,
        tenant_id: TenantId,
        request: LoadBlobsRequest,
    ) -> Result<LoadBlobsResponse, RuntimeError> {
        self.check_running()?;
        let committed = self.inner.dependencies.committer.committed_view()?;
        let mut values = BTreeMap::new();
        for entry in request.entries {
            if entry.flow_id.is_empty() {
                return Err(RuntimeError::invalid_argument(
                    "Flow ID is required for every Blob",
                ));
            }
            let reference_text = match entry.blob_value.and_then(|blob_value| blob_value.kind) {
                Some(
                    value::Kind::InternalBlobIdForStringValue(text)
                    | value::Kind::InternalBlobIdForObjValue(text),
                ) => text,
                _ => {
                    return Err(RuntimeError::invalid_argument(
                        "blob value must be an internal blob ID",
                    ));
                }
            };
            let reference: ValueReference = reference_text.parse().map_err(|_| {
                RuntimeError::invalid_argument(format!(
                    "blob ID {reference_text:?} is not a value reference"
                ))
            })?;
            let flow = FlowAddress::new(tenant_id, entry.flow_id);
            let context = self.flow_context(&flow);
            let directory = read_directory(committed.as_ref(), context, &flow)?;
            let is_run_of_flow = directory.is_some_and(|directory| {
                directory
                    .runs
                    .iter()
                    .any(|run| run.run_id.as_slice() == reference.run_id.as_bytes())
            });
            if !is_run_of_flow {
                return Err(RuntimeError::invalid_argument(format!(
                    "blob ID {reference_text:?} does not belong to Flow {}",
                    flow.flow_id
                )));
            }
            if let Some(value) = read_table_value(
                committed.as_ref(),
                context.flow_hash,
                reference.run_id,
                reference.value_id,
            )? {
                values.insert(reference_text, value);
            }
        }
        Ok(LoadBlobsResponse { values })
    }

    /// Returns the state of a run through apply's read model.
    pub async fn get_flow_state(
        &self,
        tenant_id: TenantId,
        request: GetFlowStateRequest,
    ) -> Result<GetFlowStateResponse, RuntimeError> {
        self.check_running()?;
        let flow = FlowAddress::new(tenant_id, request.flow_id.clone());
        let committed = self.inner.dependencies.committer.committed_view()?;
        read_model_adapter::read_flow_state(
            committed.as_ref(),
            self.flow_context(&flow),
            &flow,
            &request,
        )
    }

    /// Returns a page of history events of a run through apply's read model.
    pub async fn get_history_events(
        &self,
        tenant_id: TenantId,
        request: GetHistoryEventsRequest,
    ) -> Result<GetHistoryEventsResponse, RuntimeError> {
        self.check_running()?;
        let flow = FlowAddress::new(tenant_id, request.flow_id.clone());
        let committed = self.inner.dependencies.committer.committed_view()?;
        read_model_adapter::read_history_events(
            committed.as_ref(),
            self.flow_context(&flow),
            &flow,
            &request,
        )
    }

    fn read_attribute_records(
        &self,
        flow: &FlowAddress,
        request: &GetAttributesRequest,
    ) -> Result<(RunId, BTreeMap<String, AttributeRecord>), RuntimeError> {
        let context = self.flow_context(flow);
        let committed = self.inner.dependencies.committer.committed_view()?;
        let state = committed.as_ref();
        let run = read_run(state, context, flow, &request.run_id)?;
        let mut records = BTreeMap::new();
        let mut is_permission_requested = request.all_keys;
        if request.all_keys {
            let rows: Vec<(Vec<u8>, AttributeRecord)> = scan_run_rows(
                state,
                context.flow_hash,
                run.run_id,
                RunTable::Attribute,
                &[],
            )?;
            for (suffix, record) in rows {
                let key = String::from_utf8(suffix)
                    .map_err(|error| RuntimeError::corrupt_row("Attribute key", error))?;
                records.insert(key, record);
            }
        } else {
            for key in &request.keys {
                if key == WORK_QUEUE_PERMISSIONS_KEY {
                    is_permission_requested = true;
                }
                let record: Option<AttributeRecord> = decode_row(
                    state,
                    ColumnFamily::Flow,
                    &run_key(
                        context.flow_hash,
                        run.run_id,
                        RunTable::Attribute,
                        key.as_bytes(),
                    ),
                    "Attribute",
                )?;
                if let Some(record) = record {
                    records.insert(key.clone(), record);
                }
            }
        }
        if is_permission_requested && !records.contains_key(WORK_QUEUE_PERMISSIONS_KEY) {
            let permissions: Option<PermissionSetRecord> = decode_row(
                state,
                ColumnFamily::Flow,
                &run_key(context.flow_hash, run.run_id, RunTable::PermissionSet, &[]),
                "permission set",
            )?;
            if let Some(permissions) = permissions.filter(|record| !record.permissions.is_empty()) {
                records.insert(
                    WORK_QUEUE_PERMISSIONS_KEY.to_owned(),
                    permission_attribute_record(&permissions.permissions),
                );
            }
        }
        Ok((run.run_id, records))
    }

    /// Reads a hard-delete value, retrying once with the current object after an overwrite (plan 10.3).
    async fn read_hard_delete_attribute(
        &self,
        flow: &FlowAddress,
        run_id: RunId,
        key: &str,
        object: HardDeleteObjectReference,
    ) -> Result<Option<Value>, RuntimeError> {
        let reader = &self.inner.dependencies.hard_delete_reader;
        let first = reader
            .read_hard_delete_value(flow, run_id, &object)
            .await
            .map_err(|error| RuntimeError::Unavailable {
                detail: error.to_string(),
            })?;
        if first.is_some() {
            return Ok(first);
        }
        let context = self.flow_context(flow);
        let current: Option<AttributeRecord> = {
            let committed = self.inner.dependencies.committer.committed_view()?;
            decode_row(
                committed.as_ref(),
                ColumnFamily::Flow,
                &run_key(
                    context.flow_hash,
                    run_id,
                    RunTable::Attribute,
                    key.as_bytes(),
                ),
                "Attribute",
            )?
        };
        let current_object = current.and_then(|record| record.value).and_then(|stored| {
            match wire_value(&stored, run_id)? {
                WireValue::HardDeleteObject(current) => Some(current),
                WireValue::Value(_) => None,
            }
        });
        let Some(current_object) =
            current_object.filter(|current| current.object_id != object.object_id)
        else {
            return Ok(None);
        };
        reader
            .read_hard_delete_value(flow, run_id, &current_object)
            .await
            .map_err(|error| RuntimeError::Unavailable {
                detail: error.to_string(),
            })
    }
}

fn permission_attribute_record(permissions: &[String]) -> AttributeRecord {
    AttributeRecord {
        value: Some(dexzd_protocol::dexzd::v1::StoredValue {
            kind: Some(dexzd_protocol::dexzd::v1::stored_value::Kind::InlineValue(
                Value {
                    kind: Some(value::Kind::ObjValue(EncodedObject {
                        encoding: "json".to_owned(),
                        payload: permission_json::encode_string_array(permissions),
                    })),
                },
            )),
        }),
        ..AttributeRecord::default()
    }
}

fn timestamp_from_micros(micros: i64) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: micros.div_euclid(1_000_000),
        nanos: i32::try_from(micros.rem_euclid(1_000_000) * 1_000)
            .expect("sub-second nanoseconds fit in i32"),
    }
}
