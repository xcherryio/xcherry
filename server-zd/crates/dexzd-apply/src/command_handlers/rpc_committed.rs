// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Applies a Worker's committed RPC response to one run.

use std::collections::BTreeMap;

use dexzd_keyspace::row_keys::RequestKind;
use dexzd_protocol::dex::{
    ChannelMessageDeletion, ErrorSubStatus, ExecuteMethodFailurePolicy, FlowStatus,
    InvokeWorkerRpcResponse, RpcExecutionCompletedEvent, WaitForMethodFailurePolicy,
    flow_history_event,
};
use dexzd_protocol::dexzd::v1::{
    CommandReply, RequestDedupRecord, RpcCommittedCommand, RpcReply, command_reply,
};

use crate::ApplyError;
use crate::command_handlers::command_replies::{error_reply, grpc_code};
use crate::command_handlers::flow_data::{
    apply_attribute_writes, apply_channel_deletions, apply_channel_publications,
    apply_permission_mappings,
};
use crate::command_handlers::run_cursor::{HistoryKind, RunCursor, flow_not_found_reply};
use crate::command_handlers::{CommandScope, HandlerResult};
use crate::state_access::FlowTransaction;
use crate::steps::{
    NewStepExecution, cancel_steps_by_type, create_step_execution, wait_conditions,
};
use crate::values::{ValueHolder, ValueRejection, store_value, validate_value};

pub(crate) fn apply_rpc_committed(
    transaction: &mut FlowTransaction<'_>,
    mut cursor: RunCursor,
    scope: &CommandScope<'_>,
    command: &RpcCommittedCommand,
) -> Result<HandlerResult, ApplyError> {
    let Some(request) = command.request.as_ref() else {
        return Ok(reject_invalid("RPC request is missing"));
    };
    if request.request_id.is_empty() {
        return Ok(reject_invalid("RPC request ID is missing"));
    }
    if let Some(record) =
        cursor
            .rows
            .request_dedup(transaction, RequestKind::InvokeRpc, &request.request_id)?
    {
        if record.request_fingerprint != command.request_fingerprint {
            return Ok(reject_invalid(format!(
                "request ID {} was already used for a different request",
                request.request_id
            )));
        }
        let reply = record.reply.ok_or_else(|| ApplyError::InvariantViolation {
            detail: format!("RPC dedup entry {} has no reply", request.request_id),
        })?;
        return Ok(HandlerResult::Rejected(reply));
    }
    if cursor.record.status != FlowStatus::Running as i32 {
        return Ok(HandlerResult::Rejected(flow_not_found_reply(
            scope.flow_id(),
        )));
    }
    let Some(response) = command.response.as_ref() else {
        return Ok(reject_invalid("RPC response is missing"));
    };
    let is_transactional = command.is_transactional;
    if let Some(decision) = response.step_decision.as_ref()
        && (decision.close_decision.is_some() || !decision.cancel_sibling_step_types.is_empty())
    {
        return Ok(reject_invalid(
            "RPC Step decision has unsupported operations",
        ));
    }
    if response.step_decision.as_ref().is_some_and(|decision| {
        decision.next_steps.iter().any(|movement| {
            movement.step_options.as_ref().is_some_and(|options| {
                options.wait_for_failure_policy
                    == WaitForMethodFailurePolicy::ProceedOnFailure as i32
                    || options.execute_failure_policy
                        == ExecuteMethodFailurePolicy::ProceedToConfiguredStep as i32
                    || !options.execute_failure_proceed_step_type.is_empty()
                    || options.execute_failure_proceed_step_options.is_some()
            })
        })
    }) {
        return Ok(reject_invalid(
            "RPC Step decision uses an unsupported failure policy",
        ));
    }
    if is_transactional
        && let Some(deletion) =
            first_missing_deletion(transaction, &cursor, &response.delete_from_channel)?
    {
        return Ok(HandlerResult::Rejected(error_reply(
            grpc_code::NOT_FOUND,
            ErrorSubStatus::ChannelMessageNotFound,
            format!(
                "message {} is not pending in channel {}",
                deletion.message_id, deletion.channel_name
            ),
        )));
    }
    if let Err(rejection) = apply_attribute_writes(
        transaction,
        &mut cursor,
        &response.upsert_attributes,
        &command.hard_delete_objects,
        None,
    )? {
        return Ok(reject_value(rejection));
    }
    apply_channel_deletions(transaction, &mut cursor, &response.delete_from_channel)?;
    let changed_channels =
        match apply_channel_publications(transaction, &mut cursor, &response.publish_to_channel)? {
            Ok(channels) => channels,
            Err(rejection) => return Ok(reject_value(rejection)),
        };
    wait_conditions::on_channels_changed(
        transaction,
        &mut cursor,
        scope.now_micros,
        &changed_channels,
    )?;
    if let Some(mappings) = response.action_permission_mappings.as_ref() {
        apply_permission_mappings(
            transaction,
            &mut cursor,
            &response.upsert_attributes,
            mappings,
        )?;
    }
    if let Some(decision) = response.step_decision.as_ref() {
        if !decision.cancel_step_types.is_empty() {
            cancel_steps_by_type(transaction, &mut cursor, &decision.cancel_step_types, "")?;
        }
        let source = format!("__rpc/{}", request.rpc_name);
        for movement in &decision.next_steps {
            let new_step = NewStepExecution {
                step_type: &movement.step_type,
                input: movement.step_input.as_ref(),
                options: movement.step_options.as_ref(),
                from_step_execution_id: &source,
                is_timeout_handler: false,
            };
            if let Err(rejection) =
                create_step_execution(transaction, &mut cursor, scope.now_micros, &new_step)?
            {
                return Ok(reject_value(rejection));
            }
        }
    }
    if is_transactional && !command.app_worker_id.is_empty() {
        cursor.record.sticky_app_worker_id = command.app_worker_id.clone();
    }
    let output = if let Some(value) = response.output.as_ref() {
        let holder = ValueHolder::RpcReply {
            request_id: request.request_id.clone(),
        };
        let limits = cursor.value_limits();
        match store_value(
            transaction,
            cursor.rows,
            limits,
            &mut cursor.values,
            value,
            &holder,
        )? {
            Ok(stored) => Some(stored),
            Err(rejection) => return Ok(reject_value(rejection)),
        }
    } else {
        None
    };
    if let Err(rejection) = validate_ephemeral_response_values(transaction, &cursor, response)? {
        return Ok(reject_value(rejection));
    }
    cursor.add_event(flow_history_event::Payload::RpcExecutionCompleted(
        RpcExecutionCompletedEvent {
            rpc_name: request.rpc_name.clone(),
            input: if is_transactional {
                request.input.clone()
            } else {
                None
            },
            output: if is_transactional {
                response.output.clone()
            } else {
                None
            },
            step_decision: response.step_decision.clone(),
            upsert_attributes: response.upsert_attributes.clone(),
            record_events: response.record_events.clone(),
            publish_to_channel: response.publish_to_channel.clone(),
            delete_from_channel: response.delete_from_channel.clone(),
            served_by_app_worker_id: command.app_worker_id.clone(),
            ..RpcExecutionCompletedEvent::default()
        },
    ));
    let reply = CommandReply {
        reply: Some(command_reply::Reply::Rpc(RpcReply { output })),
    };
    remember_reply(
        transaction,
        &cursor,
        request.request_id.as_str(),
        command,
        &reply,
    )?;
    Ok(HandlerResult::Applied(cursor.finish(
        transaction,
        scope,
        reply,
        HistoryKind::EXTERNAL_INPUT,
    )?))
}

fn first_missing_deletion<'a>(
    transaction: &FlowTransaction<'_>,
    cursor: &RunCursor,
    deletions: &'a [ChannelMessageDeletion],
) -> Result<Option<&'a ChannelMessageDeletion>, ApplyError> {
    let mut remaining_by_channel = BTreeMap::<String, BTreeMap<String, usize>>::new();
    for deletion in deletions {
        if !remaining_by_channel.contains_key(&deletion.channel_name) {
            let mut counts = BTreeMap::<String, usize>::new();
            for message in
                cursor
                    .rows
                    .channel_messages(transaction, &deletion.channel_name, usize::MAX)?
            {
                *counts.entry(message.message_id).or_default() += 1;
            }
            remaining_by_channel.insert(deletion.channel_name.clone(), counts);
        }
        let Some(remaining) = remaining_by_channel
            .get_mut(&deletion.channel_name)
            .and_then(|counts| counts.get_mut(&deletion.message_id))
        else {
            return Ok(Some(deletion));
        };
        if *remaining == 0 {
            return Ok(Some(deletion));
        }
        *remaining -= 1;
    }
    Ok(None)
}

fn validate_ephemeral_response_values(
    transaction: &FlowTransaction<'_>,
    cursor: &RunCursor,
    response: &InvokeWorkerRpcResponse,
) -> Result<Result<(), ValueRejection>, ApplyError> {
    for event in &response.record_events {
        let Some(value) = event.value.as_ref() else {
            continue;
        };
        match validate_value(transaction, cursor.rows, cursor.value_limits(), value)? {
            Ok(()) => {}
            Err(rejection) => return Ok(Err(rejection)),
        }
    }
    if let Some(mappings) = response.action_permission_mappings.as_ref() {
        for mapping in &mappings.mappings {
            for value in &mapping.equal_values {
                match validate_value(transaction, cursor.rows, cursor.value_limits(), value)? {
                    Ok(()) => {}
                    Err(rejection) => return Ok(Err(rejection)),
                }
            }
        }
    }
    Ok(Ok(()))
}

fn remember_reply(
    transaction: &mut FlowTransaction<'_>,
    cursor: &RunCursor,
    request_id: &str,
    command: &RpcCommittedCommand,
    reply: &CommandReply,
) -> Result<(), ApplyError> {
    let capacity = cursor
        .record
        .settings
        .as_ref()
        .map_or(1000, |settings| settings.request_dedup_capacity)
        .max(1);
    cursor.rows.put_request_dedup(
        transaction,
        RequestKind::InvokeRpc,
        request_id,
        &RequestDedupRecord {
            request_fingerprint: command.request_fingerprint.clone(),
            reply: Some(reply.clone()),
            recorded_seq: cursor.seq,
        },
        capacity,
    )?;
    Ok(())
}

fn reject_invalid(message: impl Into<String>) -> HandlerResult {
    HandlerResult::Rejected(error_reply(
        grpc_code::INVALID_ARGUMENT,
        ErrorSubStatus::Uncategorized,
        message,
    ))
}

fn reject_value(rejection: ValueRejection) -> HandlerResult {
    reject_invalid(rejection.message())
}
