// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! InvokeRPC: read-only, non-transactional, and transactional paths (plan 9.4).

use std::collections::{BTreeMap, BTreeSet};

use dexzd_apply::{ApplyContext, FlowStateReader};
use dexzd_keyspace::row_keys::channel_name_prefix;
use dexzd_keyspace::{RunId, RunTable, TenantId};
use dexzd_protocol::dex::{
    ChannelInfo, ChannelMessage, ChannelValues, Context, ErrorSubStatus, InvokeRpcRequest,
    InvokeRpcResponse, InvokeWorkerRpcRequest, InvokeWorkerRpcResponse, Kv,
};
use dexzd_protocol::dexzd::v1::{
    AttributeRecord, ChannelMessageRecord, ChannelMetaRecord, RpcCommittedCommand, command_reply,
    flow_command,
};
use dexzd_request_validation::{
    ClientRequest, WorkerResponse, validate_client_request, validate_worker_response,
};
use prost::Message;

use crate::runtime::client_operations::hard_delete_uploads::hard_delete_references;
use crate::runtime::client_operations::request_fingerprint::request_fingerprint;
use crate::runtime::client_operations::unstamped_flow_command;
use crate::runtime::read_operations::state_rows::{
    ResolvedRun, WireValue, hydrate_value_reference, load_complete_value, read_run, response_value,
    scan_run_rows,
};
use crate::runtime::{
    FlowAddress, FlowRuntimeRegistry, RpcInvokeError, RuntimeError, WorkerRpcInvocation,
    WorkerRpcResult, grpc_code,
};

/// An RPC snapshot whose hard-delete Attribute values are still to be read.
struct RpcSnapshot {
    run: ResolvedRun,
    worker_request: InvokeWorkerRpcRequest,
    hard_delete_attributes: Vec<(usize, dexzd_protocol::dexzd::v1::HardDeleteObjectReference)>,
}

impl FlowRuntimeRegistry {
    /// Invokes an RPC handler on a Worker against a committed snapshot and commits its effects.
    ///
    /// A read-only RPC (no locks, not transactional, no effects) is never logged. Lock keys are
    /// taken for the whole call; any held key fails with `ABORTED`.
    pub async fn invoke_rpc(
        &self,
        tenant_id: TenantId,
        request: InvokeRpcRequest,
    ) -> Result<InvokeRpcResponse, RuntimeError> {
        validate_client_request(
            ClientRequest::InvokeRpc(&request),
            &self.inner.config.validation,
        )?;
        self.check_running()?;
        let flow = FlowAddress::new(tenant_id, request.flow_id.clone());
        let context = self.flow_context(&flow);
        let is_transactional = request.is_transactional || !request.lock_attribute_keys.is_empty();
        let lease = self.lease(&flow);
        let mut lock_keys = request.lock_attribute_keys.clone();
        lock_keys.sort();
        lock_keys.dedup();
        let run_id = {
            let committed = self.inner.dependencies.committer.committed_view()?;
            read_run(committed.as_ref(), context, &flow, &request.run_id)?.run_id
        };
        let lock_guard = if lock_keys.is_empty() {
            None
        } else {
            Some(self.acquire_rpc_locks(lease, run_id, &lock_keys)?)
        };
        if lock_guard.is_some() {
            // Writes proposed before the locks were taken must be in the snapshot (plan 8.3, L2).
            self.sync_barrier(&flow).await?;
        }
        let is_hydrating = self.inner.config.hydrate_value_references;
        let snapshot = {
            let committed = self.inner.dependencies.committer.committed_view()?;
            build_rpc_snapshot(
                committed.as_ref(),
                context,
                &flow,
                &run_id.to_string(),
                &request,
                is_hydrating,
            )?
        };
        let run = snapshot.run.clone();
        let worker_request = self.complete_rpc_snapshot(&flow, snapshot).await?;
        if worker_request.encoded_len() > self.inner.config.max_worker_rpc_request_bytes {
            return Err(RuntimeError::rejected(
                grpc_code::RESOURCE_EXHAUSTED,
                ErrorSubStatus::Uncategorized,
                format!(
                    "RPC Worker request exceeds the {}-byte gRPC message limit",
                    self.inner.config.max_worker_rpc_request_bytes
                ),
            ));
        }
        let timeout = if request.timeout_seconds > 0 {
            std::time::Duration::from_secs(u64::from(request.timeout_seconds.unsigned_abs()))
        } else {
            self.inner.config.rpc_default_timeout
        };
        let deadline = tokio::time::Instant::now() + timeout;
        let call = self
            .inner
            .dependencies
            .rpc_invoker
            .invoke_worker_rpc(WorkerRpcInvocation {
                flow: flow.clone(),
                run_id: run.run_id,
                flow_type: run.record.flow_type.clone(),
                sticky_app_worker_id: run.record.sticky_app_worker_id.clone(),
                request: worker_request,
                deadline,
            });
        // A stalled invoker must not hold the RPC locks past the deadline (plan 8.3).
        let result = tokio::time::timeout_at(deadline, call)
            .await
            .unwrap_or(Err(RpcInvokeError::DeadlineExceeded))
            .map_err(rpc_invoke_error)?;
        validate_worker_response(
            WorkerResponse::Rpc(Some(&result.response)),
            &self.inner.config.validation,
        )
        .map_err(|error| {
            RuntimeError::rejected(
                grpc_code::FAILED_PRECONDITION,
                ErrorSubStatus::WorkerApiError,
                format!("worker RPC response is invalid: {}", error.message),
            )
        })?;
        if !is_transactional && !has_rpc_effects(&result.response) {
            let mut output = result.response.output;
            if is_hydrating && let Some(output) = output.as_mut() {
                let committed = self.inner.dependencies.committer.committed_view()?;
                if !hydrate_value_reference(committed.as_ref(), context.flow_hash, run_id, output)?
                {
                    return Err(RuntimeError::rejected(
                        grpc_code::FAILED_PRECONDITION,
                        ErrorSubStatus::WorkerApiError,
                        "worker RPC output is a value reference that names no value of this run",
                    ));
                }
            }
            return Ok(InvokeRpcResponse { output });
        }
        let output = self
            .commit_rpc(&flow, &run, request, result, is_transactional)
            .await;
        drop(lock_guard);
        output
    }

    async fn complete_rpc_snapshot(
        &self,
        flow: &FlowAddress,
        snapshot: RpcSnapshot,
    ) -> Result<InvokeWorkerRpcRequest, RuntimeError> {
        let RpcSnapshot {
            run,
            mut worker_request,
            hard_delete_attributes,
        } = snapshot;
        for (index, object) in hard_delete_attributes {
            let value = self
                .inner
                .dependencies
                .hard_delete_reader
                .read_hard_delete_value(flow, run.run_id, &object)
                .await
                .map_err(|error| RuntimeError::Unavailable {
                    detail: error.to_string(),
                })?;
            worker_request.attributes[index].value = value;
        }
        worker_request
            .attributes
            .retain(|attribute| attribute.value.is_some());
        Ok(worker_request)
    }

    async fn commit_rpc(
        &self,
        flow: &FlowAddress,
        run: &ResolvedRun,
        request: InvokeRpcRequest,
        result: WorkerRpcResult,
        is_transactional: bool,
    ) -> Result<InvokeRpcResponse, RuntimeError> {
        let fingerprint = request_fingerprint(&request);
        let mut response = result.response;
        // Apply cannot mint IDs, so every publication gets a new one, as Go's AssignIDs does.
        for message in &mut response.publish_to_channel {
            message.message_id = self
                .inner
                .dependencies
                .id_source
                .channel_message_id()
                .to_string();
        }
        let uploads = self
            .upload_hard_delete_values(flow, run.run_id, &mut response.upsert_attributes)
            .await?;
        let mut command = unstamped_flow_command(
            flow,
            Some(run.run_id),
            flow_command::Command::RpcCommitted(RpcCommittedCommand {
                request: Some(request),
                response: Some(response),
                app_worker_id: result.app_worker_id,
                hard_delete_objects: hard_delete_references(uploads.as_ref()),
                request_fingerprint: fingerprint,
                pull_routing: result.pull_routing,
                is_transactional,
            }),
        );
        if is_transactional {
            // Apply rejects the commit when time travel replaced the attempt the snapshot read.
            command.expected_run_attempt = run.record.current_attempt;
        }
        let reply = self
            .propose_client_command_with_uploads(flow, uploads, |_| Ok(command.clone()))
            .await?;
        let command_reply::Reply::Rpc(rpc_reply) = reply else {
            return Err(RuntimeError::Internal {
                detail: format!("apply returned an unexpected reply for an RPC: {reply:?}"),
            });
        };
        let Some(stored_output) = rpc_reply.output else {
            return Ok(InvokeRpcResponse { output: None });
        };
        let context = self.flow_context(flow);
        let committed = self.inner.dependencies.committer.committed_view()?;
        let output = load_complete_value(
            committed.as_ref(),
            context.flow_hash,
            run.run_id,
            &stored_output,
        )?;
        Ok(InvokeRpcResponse { output })
    }
}

/// Reads the run's Attributes and Channels into the Worker request, with complete values when
/// `is_hydrating`.
///
/// Returns `INVALID_ARGUMENT` when hydrating and the RPC input is a value reference that names no
/// value of the run.
fn build_rpc_snapshot(
    state: &dyn FlowStateReader,
    context: ApplyContext,
    flow: &FlowAddress,
    run_id: &str,
    request: &InvokeRpcRequest,
    is_hydrating: bool,
) -> Result<RpcSnapshot, RuntimeError> {
    let run = read_run(state, context, flow, run_id)?;
    let run_id = run.run_id;
    let mut input = request.input.clone();
    if is_hydrating
        && let Some(input) = input.as_mut()
        && !hydrate_value_reference(state, context.flow_hash, run_id, input)?
    {
        return Err(RuntimeError::invalid_argument(
            "RPC input is a value reference that names no value of this run",
        ));
    }
    let attribute_selections = sorted_unique(&request.load_attribute_map_instances);
    let channel_names = sorted_unique(&request.load_channel_names);
    let channel_selections = sorted_unique(&request.load_channel_map_instances);

    let mut attributes = Vec::new();
    let mut hard_delete_attributes = Vec::new();
    let rows: Vec<(Vec<u8>, AttributeRecord)> =
        scan_run_rows(state, context.flow_hash, run_id, RunTable::Attribute, &[])?;
    for (suffix, record) in rows {
        let key = String::from_utf8(suffix)
            .map_err(|error| RuntimeError::corrupt_row("Attribute key", error))?;
        if !is_selected(&key, &attribute_selections) {
            continue;
        }
        let Some(stored) = record.value.as_ref() else {
            continue;
        };
        let Some(value) = response_value(state, context.flow_hash, run_id, stored, is_hydrating)?
        else {
            continue;
        };
        match value {
            WireValue::Value(value) => attributes.push(Kv {
                key,
                value: Some(value),
            }),
            WireValue::HardDeleteObject(object) => {
                hard_delete_attributes.push((attributes.len(), object));
                attributes.push(Kv { key, value: None });
            }
        }
    }

    let mut channel_infos = BTreeMap::new();
    let mut loaded_channel_messages = BTreeMap::new();
    let metas: Vec<(Vec<u8>, ChannelMetaRecord)> =
        scan_run_rows(state, context.flow_hash, run_id, RunTable::ChannelMeta, &[])?;
    let mut nonempty_channels = BTreeSet::new();
    for (suffix, meta) in metas {
        let channel_name = String::from_utf8(suffix)
            .map_err(|error| RuntimeError::corrupt_row("Channel name", error))?;
        if meta.pending_count == 0 {
            continue;
        }
        channel_infos.insert(
            channel_name.clone(),
            ChannelInfo {
                size: i32::try_from(meta.pending_count).unwrap_or(i32::MAX),
            },
        );
        nonempty_channels.insert(channel_name);
    }
    let mut loaded_channels: BTreeSet<String> = channel_names.iter().cloned().collect();
    for selection in &channel_selections {
        if selection.ends_with('/') {
            loaded_channels.extend(
                nonempty_channels
                    .iter()
                    .filter(|name| name.starts_with(selection.as_str()))
                    .cloned(),
            );
        } else {
            loaded_channels.insert(selection.clone());
        }
    }
    for channel_name in loaded_channels {
        let messages = read_channel_messages(state, context, run_id, &channel_name, is_hydrating)?;
        loaded_channel_messages.insert(channel_name, ChannelValues { messages });
    }

    let worker_request = InvokeWorkerRpcRequest {
        context: Some(Context {
            flow_id: flow.flow_id.clone(),
            run_id: run_id.to_string(),
            flow_started_timestamp: run.record.started_at_micros.div_euclid(1_000_000),
            ..Context::default()
        }),
        flow_type: run.record.flow_type.clone(),
        rpc_name: request.rpc_name.clone(),
        input,
        attributes,
        channel_infos,
        loaded_channel_messages,
        loaded_attribute_map_instances: attribute_selections,
        loaded_channel_names: channel_names,
        loaded_channel_map_instances: channel_selections,
    };
    Ok(RpcSnapshot {
        run,
        worker_request,
        hard_delete_attributes,
    })
}

fn read_channel_messages(
    state: &dyn FlowStateReader,
    context: ApplyContext,
    run_id: RunId,
    channel_name: &str,
    is_hydrating: bool,
) -> Result<Vec<ChannelMessage>, RuntimeError> {
    let suffix_prefix = channel_name_prefix(channel_name);
    let rows: Vec<(Vec<u8>, ChannelMessageRecord)> = scan_run_rows(
        state,
        context.flow_hash,
        run_id,
        RunTable::ChannelMessage,
        &suffix_prefix,
    )?;
    let mut messages = Vec::with_capacity(rows.len());
    for (_, record) in rows {
        let value = match record.value.as_ref() {
            Some(stored) => response_value(state, context.flow_hash, run_id, stored, is_hydrating)?,
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
    Ok(messages)
}

/// Ordinary Attributes are always loaded; AttributeMap instances only when selected exactly
/// or by a `definition/` prefix (plan 9.3).
fn is_selected(key: &str, selections: &[String]) -> bool {
    if !key.contains('/') {
        return true;
    }
    selections.iter().any(|selection| {
        if selection.ends_with('/') {
            key.starts_with(selection.as_str())
        } else {
            key == selection
        }
    })
}

fn sorted_unique(values: &[String]) -> Vec<String> {
    let set: BTreeSet<&String> = values.iter().collect();
    set.into_iter().cloned().collect()
}

fn has_rpc_effects(response: &InvokeWorkerRpcResponse) -> bool {
    let has_step_decision = response.step_decision.as_ref().is_some_and(|decision| {
        !decision.next_steps.is_empty() || !decision.cancel_step_types.is_empty()
    });
    has_step_decision
        || !response.upsert_attributes.is_empty()
        || !response.record_events.is_empty()
        || !response.delete_from_channel.is_empty()
        || !response.publish_to_channel.is_empty()
        || response
            .action_permission_mappings
            .as_ref()
            .is_some_and(|mappings| !mappings.mappings.is_empty())
}

fn rpc_invoke_error(error: RpcInvokeError) -> RuntimeError {
    match error {
        RpcInvokeError::WorkerError { response } => RuntimeError::WorkerRpcFailed {
            worker_error: response,
        },
        RpcInvokeError::NoWorkerAvailable { detail }
        | RpcInvokeError::WorkerDisconnected { detail } => RuntimeError::Unavailable { detail },
        RpcInvokeError::UnknownRpc { detail } => RuntimeError::rejected(
            grpc_code::FAILED_PRECONDITION,
            ErrorSubStatus::Uncategorized,
            detail,
        ),
        RpcInvokeError::DeadlineExceeded => RuntimeError::rejected(
            grpc_code::DEADLINE_EXCEEDED,
            ErrorSubStatus::Uncategorized,
            "worker RPC deadline exceeded",
        ),
    }
}
