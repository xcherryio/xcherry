// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Client writes: SetAttributes, PublishToChannel, DeleteChannelMessage, StopFlow, and
//! UpdateFlowConfig.

use dexzd_keyspace::row_keys::RequestKind;
use dexzd_protocol::dex::{
    ChannelExternalDeleteEvent, ChannelExternalPublishEvent, ErrorSubStatus, FlowErrorType,
    RpcExecutionCompletedEvent, StopType, flow_history_event,
};
use dexzd_protocol::dexzd::v1::{
    AttributesSetCommand, ChannelMessageDeleteCommand, ChannelPublishCommand, CommandReply,
    FlowConfigUpdateCommand, FlowStopCommand, RequestDedupRecord,
};

use crate::ApplyError;
use crate::command_handlers::command_replies::{error_reply, grpc_code, ok_reply};
use crate::command_handlers::flow_close::{FlowClosing, close_flow};
use crate::command_handlers::flow_data::{
    apply_attribute_writes, apply_channel_deletions, apply_channel_publications,
    find_pending_message,
};
use crate::command_handlers::run_cursor::{HistoryKind, RunCursor};
use crate::command_handlers::{CommandScope, HandlerResult};
use crate::state_access::FlowTransaction;
use crate::steps::wait_conditions;

/// Returns the recorded reply for a repeated request ID, or a rejection for a reused ID with
/// different content (plan C7).
fn deduplicate(
    transaction: &FlowTransaction<'_>,
    cursor: &RunCursor,
    kind: RequestKind,
    request_id: &str,
    fingerprint: &[u8],
) -> Result<Option<CommandReply>, ApplyError> {
    if request_id.is_empty() {
        return Ok(None);
    }
    let Some(record) = cursor.rows.request_dedup(transaction, kind, request_id)? else {
        return Ok(None);
    };
    if record.request_fingerprint != fingerprint {
        return Ok(Some(error_reply(
            grpc_code::INVALID_ARGUMENT,
            ErrorSubStatus::Uncategorized,
            format!("request ID {request_id} was already used for a different request"),
        )));
    }
    Ok(Some(record.reply.unwrap_or_else(ok_reply)))
}

fn remember_request(
    transaction: &mut FlowTransaction<'_>,
    cursor: &RunCursor,
    kind: RequestKind,
    request_id: &str,
    fingerprint: &[u8],
    reply: &CommandReply,
) -> Result<(), ApplyError> {
    if request_id.is_empty() {
        return Ok(());
    }
    let capacity = cursor
        .record
        .settings
        .as_ref()
        .map_or(1000, |settings| settings.request_dedup_capacity);
    cursor.rows.put_request_dedup(
        transaction,
        kind,
        request_id,
        &RequestDedupRecord {
            request_fingerprint: fingerprint.to_vec(),
            reply: Some(reply.clone()),
            recorded_seq: cursor.seq,
        },
        capacity,
    )
}

fn closed_flow_reply(flow_id: &str) -> CommandReply {
    crate::command_handlers::run_cursor::flow_not_found_reply(flow_id)
}

fn is_running(cursor: &RunCursor) -> bool {
    cursor.record.status == dexzd_protocol::dex::FlowStatus::Running as i32
}

pub(crate) fn apply_attributes_set(
    transaction: &mut FlowTransaction<'_>,
    mut cursor: RunCursor,
    scope: &CommandScope<'_>,
    command: &AttributesSetCommand,
) -> Result<HandlerResult, ApplyError> {
    let request = command.request.clone().unwrap_or_default();
    if let Some(reply) = deduplicate(
        transaction,
        &cursor,
        RequestKind::SetAttributes,
        &request.request_id,
        &command.request_fingerprint,
    )? {
        return Ok(HandlerResult::Rejected(reply));
    }
    if !is_running(&cursor) {
        return Ok(HandlerResult::Rejected(closed_flow_reply(scope.flow_id())));
    }
    if let Err(rejection) = apply_attribute_writes(
        transaction,
        &mut cursor,
        &request.attributes,
        &command.hard_delete_objects,
        request.action_permission_mappings.as_ref(),
    )? {
        return Ok(HandlerResult::Rejected(error_reply(
            grpc_code::INVALID_ARGUMENT,
            ErrorSubStatus::Uncategorized,
            rejection.message(),
        )));
    }
    cursor.add_event(flow_history_event::Payload::RpcExecutionCompleted(
        RpcExecutionCompletedEvent {
            upsert_attributes: request.attributes.clone(),
            is_set_attribute_api: true,
            ..RpcExecutionCompletedEvent::default()
        },
    ));
    let reply = ok_reply();
    remember_request(
        transaction,
        &cursor,
        RequestKind::SetAttributes,
        &request.request_id,
        &command.request_fingerprint,
        &reply,
    )?;
    Ok(HandlerResult::Applied(cursor.finish(
        transaction,
        scope,
        reply,
        HistoryKind::EXTERNAL_INPUT,
    )?))
}

pub(crate) fn apply_channel_publish(
    transaction: &mut FlowTransaction<'_>,
    mut cursor: RunCursor,
    scope: &CommandScope<'_>,
    command: &ChannelPublishCommand,
) -> Result<HandlerResult, ApplyError> {
    let request = command.request.clone().unwrap_or_default();
    if !is_running(&cursor) {
        return Ok(HandlerResult::Rejected(closed_flow_reply(scope.flow_id())));
    }
    let channels = match apply_channel_publications(transaction, &mut cursor, &request.messages)? {
        Ok(channels) => channels,
        Err(rejection) => {
            return Ok(HandlerResult::Rejected(error_reply(
                grpc_code::INVALID_ARGUMENT,
                ErrorSubStatus::Uncategorized,
                rejection.message(),
            )));
        }
    };
    cursor.add_event(flow_history_event::Payload::ChannelExternalPublish(
        ChannelExternalPublishEvent {
            messages: request.messages.clone(),
        },
    ));
    wait_conditions::on_channels_changed(transaction, &mut cursor, scope.now_micros, &channels)?;
    Ok(HandlerResult::Applied(cursor.finish(
        transaction,
        scope,
        ok_reply(),
        HistoryKind::EXTERNAL_INPUT,
    )?))
}

pub(crate) fn apply_channel_message_delete(
    transaction: &mut FlowTransaction<'_>,
    mut cursor: RunCursor,
    scope: &CommandScope<'_>,
    command: &ChannelMessageDeleteCommand,
) -> Result<HandlerResult, ApplyError> {
    let request = command.request.clone().unwrap_or_default();
    if let Some(reply) = deduplicate(
        transaction,
        &cursor,
        RequestKind::DeleteChannelMessage,
        &request.request_id,
        &command.request_fingerprint,
    )? {
        return Ok(HandlerResult::Rejected(reply));
    }
    if !is_running(&cursor) {
        return Ok(HandlerResult::Rejected(closed_flow_reply(scope.flow_id())));
    }
    if find_pending_message(
        transaction,
        &cursor,
        &request.channel_name,
        &request.message_id,
    )?
    .is_none()
    {
        return Ok(HandlerResult::Rejected(error_reply(
            grpc_code::NOT_FOUND,
            ErrorSubStatus::ChannelMessageNotFound,
            format!(
                "message {} is not pending in channel {}",
                request.message_id, request.channel_name
            ),
        )));
    }
    let deletion = dexzd_protocol::dex::ChannelMessageDeletion {
        channel_name: request.channel_name.clone(),
        message_id: request.message_id.clone(),
    };
    apply_channel_deletions(transaction, &mut cursor, std::slice::from_ref(&deletion))?;
    cursor.add_event(flow_history_event::Payload::ChannelExternalDelete(
        ChannelExternalDeleteEvent {
            messages: vec![deletion],
        },
    ));
    let reply = ok_reply();
    remember_request(
        transaction,
        &cursor,
        RequestKind::DeleteChannelMessage,
        &request.request_id,
        &command.request_fingerprint,
        &reply,
    )?;
    Ok(HandlerResult::Applied(cursor.finish(
        transaction,
        scope,
        reply,
        HistoryKind::EXTERNAL_INPUT,
    )?))
}

pub(crate) fn apply_flow_stop(
    transaction: &mut FlowTransaction<'_>,
    mut cursor: RunCursor,
    scope: &CommandScope<'_>,
    command: &FlowStopCommand,
) -> Result<HandlerResult, ApplyError> {
    let request = command.request.clone().unwrap_or_default();
    if !is_running(&cursor) {
        // Stopping a closed run succeeds without changes, like the first Stop winning.
        return Ok(HandlerResult::Rejected(ok_reply()));
    }
    let stop_type = StopType::try_from(request.stop_type).unwrap_or(StopType::Unspecified);
    let closing = match stop_type {
        StopType::Unspecified | StopType::Cancel => FlowClosing::canceled(),
        StopType::Terminate => FlowClosing::terminated(request.reason.clone()),
        StopType::Fail => {
            let reason = if request.reason.is_empty() {
                "fail by client".to_owned()
            } else {
                request.reason.clone()
            };
            FlowClosing::failed(FlowErrorType::ClientApiFailingFlow, reason)
        }
    };
    close_flow(transaction, &mut cursor, scope, closing)?;
    Ok(HandlerResult::Applied(cursor.finish(
        transaction,
        scope,
        ok_reply(),
        HistoryKind::SEMANTIC,
    )?))
}

/// Merges FlowConfig field by field; the Worker target and Attribute Store names replace the
/// whole value (as in the Go Server).
pub(crate) fn apply_flow_config_update(
    transaction: &mut FlowTransaction<'_>,
    mut cursor: RunCursor,
    scope: &CommandScope<'_>,
    command: &FlowConfigUpdateCommand,
) -> Result<HandlerResult, ApplyError> {
    let update = command
        .request
        .as_ref()
        .and_then(|request| request.flow_config.clone())
        .unwrap_or_default();
    let config = cursor
        .record
        .flow_config
        .get_or_insert_with(Default::default);
    if update.active_step_search_mode.is_some() {
        config.active_step_search_mode = update.active_step_search_mode;
    }
    if update.continue_as_new_threshold.is_some() {
        config.continue_as_new_threshold = update.continue_as_new_threshold;
    }
    if update.continue_as_new_page_size_in_bytes.is_some() {
        config.continue_as_new_page_size_in_bytes = update.continue_as_new_page_size_in_bytes;
    }
    if update.step_durability.is_some() {
        config.step_durability = update.step_durability;
    }
    if update.worker_target.is_some() {
        config.worker_target = update.worker_target;
    }
    if update.attribute_store_names.is_some() {
        config.attribute_store_names = update.attribute_store_names;
    }
    Ok(HandlerResult::Applied(cursor.finish(
        transaction,
        scope,
        ok_reply(),
        HistoryKind::BOOKKEEPING,
    )?))
}
