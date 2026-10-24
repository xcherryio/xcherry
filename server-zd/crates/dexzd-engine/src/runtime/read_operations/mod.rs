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
    RunRecord,
};

use crate::effects::HardDeleteObjectOwner;
use crate::runtime::read_operations::state_rows::{
    WireValue, decode_row, read_directory, read_run, read_table_value, response_value,
    scan_run_rows,
};
use crate::runtime::{FlowAddress, FlowRuntimeRegistry, RuntimeError};

/// Attribute key of the server-maintained work queue permission projection.
pub(crate) const WORK_QUEUE_PERMISSIONS_KEY: &str = "DexWorkQueuePermissions";

impl FlowRuntimeRegistry {
    /// Returns the requested Attributes of a run, sorted by key; missing keys are omitted.
    ///
    /// Values above the inline threshold are returned as value references unless hydration is
    /// configured; hard-delete values are always returned as content.
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
        let (run_id, values) = self.read_attribute_values(&flow, &request)?;
        let mut attributes = Vec::with_capacity(values.len());
        for (key, value) in values {
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
    ///
    /// Values above the inline threshold are value references unless hydration is configured.
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
        let is_hydrating = self.inner.config.hydrate_value_references;
        let mut messages = Vec::with_capacity(rows.len());
        for (_, record) in rows {
            let value = match record.value.as_ref() {
                Some(stored) => response_value(
                    committed.as_ref(),
                    context.flow_hash,
                    run.run_id,
                    stored,
                    is_hydrating,
                )?,
                None => None,
            };
            messages.push(ChannelMessage {
                value: match value {
                    Some(WireValue::Value(value)) => Some(value),
                    Some(WireValue::HardDeleteObject(_)) | None => None,
                },
                channel_name: record.channel_name,
                message_id: record.message_id,
            });
        }
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
    /// Every reference for a live Flow must belong to one of its runs. Values that no longer
    /// exist are omitted.
    ///
    /// Reads only committed state, so no client sees a value that may never commit. Dispatch
    /// inlines uncommitted values instead.
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
            let Some(directory) = directory else {
                continue;
            };
            let is_run_of_flow = directory
                .runs
                .iter()
                .any(|run| run.run_id.as_slice() == reference.run_id.as_bytes());
            if !is_run_of_flow {
                return Err(RuntimeError::invalid_argument(format!(
                    "blob ID {reference_text:?} does not belong to Flow {}",
                    flow.flow_id
                )));
            }
            let run_record: Option<RunRecord> = decode_row(
                committed.as_ref(),
                ColumnFamily::Flow,
                &run_key(
                    context.flow_hash,
                    reference.run_id,
                    RunTable::RunRecord,
                    &[],
                ),
                "run",
            )?;
            if run_record.is_some_and(|record| {
                record.flow_id == flow.flow_id
                    && record.tenant_id == flow.tenant_id.0
                    && record.is_purging
            }) {
                continue;
            }
            read_run(
                committed.as_ref(),
                context,
                &flow,
                &reference.run_id.to_string(),
            )?;
            if let Some(value) = read_table_value(
                committed.as_ref(),
                context.flow_hash,
                reference.run_id,
                reference.value_id,
            )? {
                values.insert(reference_text, value);
            } else {
                // A Worker that receives no value cannot run its Step.
                tracing::debug!(
                    flow_id = %flow.flow_id,
                    reference = %reference_text,
                    "LoadBlobs omitted a value reference that names no committed value"
                );
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
            self.inner.config.hydrate_value_references,
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

    /// Returns the run and the wire values of the requested Attributes, sorted by key, read from
    /// one committed view.
    fn read_attribute_values(
        &self,
        flow: &FlowAddress,
        request: &GetAttributesRequest,
    ) -> Result<(RunId, Vec<(String, WireValue)>), RuntimeError> {
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
        let is_hydrating = self.inner.config.hydrate_value_references;
        let mut values = Vec::with_capacity(records.len());
        for (key, record) in records {
            let Some(stored) = record.value.as_ref() else {
                continue;
            };
            if let Some(value) =
                response_value(state, context.flow_hash, run.run_id, stored, is_hydrating)?
            {
                values.push((key, value));
            }
        }
        Ok((run.run_id, values))
    }

    /// Reads a hard-delete value, retrying once with the current object after an overwrite (plan 10.3).
    pub(crate) async fn read_hard_delete_attribute(
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
        let current_value = {
            let committed = self.inner.dependencies.committer.committed_view()?;
            read_run(committed.as_ref(), context, flow, &run_id.to_string())?;
            let current: Option<AttributeRecord> = decode_row(
                committed.as_ref(),
                ColumnFamily::Flow,
                &run_key(
                    context.flow_hash,
                    run_id,
                    RunTable::Attribute,
                    key.as_bytes(),
                ),
                "Attribute",
            )?;
            current
                .and_then(|record| record.value)
                .as_ref()
                .map(|stored| {
                    response_value(
                        committed.as_ref(),
                        context.flow_hash,
                        run_id,
                        stored,
                        self.inner.config.hydrate_value_references,
                    )
                })
                .transpose()?
                .flatten()
        };
        let current_object = match current_value {
            Some(WireValue::Value(value)) => return Ok(Some(value)),
            Some(WireValue::HardDeleteObject(current_object)) => current_object,
            None => return Ok(None),
        };
        if current_object.object_id == object.object_id {
            return Err(missing_hard_delete_object(
                flow,
                context.flow_hash,
                run_id,
                &object,
            ));
        }
        let value = reader
            .read_hard_delete_value(flow, run_id, &current_object)
            .await
            .map_err(|error| RuntimeError::Unavailable {
                detail: error.to_string(),
            })?;
        if value.is_none() {
            let committed = self.inner.dependencies.committer.committed_view()?;
            read_run(committed.as_ref(), context, flow, &run_id.to_string())?;
            return Err(missing_hard_delete_object(
                flow,
                context.flow_hash,
                run_id,
                &current_object,
            ));
        }
        Ok(value)
    }
}

fn missing_hard_delete_object(
    flow: &FlowAddress,
    flow_hash: dexzd_keyspace::FlowHash,
    run_id: RunId,
    object: &HardDeleteObjectReference,
) -> RuntimeError {
    let object_key = HardDeleteObjectOwner {
        tenant_id: flow.tenant_id,
        flow_hash,
        run_id,
    }
    .object_key(&object.object_id);
    tracing::error!(%object_key, "a live hard-delete Attribute object is missing");
    RuntimeError::Internal {
        detail: format!("hard-delete Attribute object {object_key} is missing"),
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
