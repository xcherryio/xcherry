// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! At-least-once delivery of committed outbox messages: the receiving Flow commits an
//! `InboxMessage` (deduplicated by source run and message ID), then the sender commits
//! `OutboxAck`.

use dexzd_keyspace::row_keys::outbox_key;
use dexzd_keyspace::{ColumnFamily, RunId, TenantId};
use dexzd_protocol::dexzd::v1::{
    CrossFlowEndpoint, FlowCommand, InboxMessageCommand, OutboxAckCommand, OutboxRecord,
    command_reply, flow_command, inbox_message_command, outbox_record,
};

use crate::effects::state_reads::decode_row;
use crate::effects::{EffectContext, EffectError};
use crate::runtime::{CommitUrgency, FlowAddress};

/// Delivers one outbox message unless it was already acknowledged.
///
/// Acknowledges only an OK reply; any other reply is retried with backoff until the node stops.
pub(crate) async fn deliver_outbox_message(
    context: &EffectContext,
    flow: &FlowAddress,
    run_id: RunId,
    message_id: &[u8],
) -> Result<(), EffectError> {
    let mut backoff = context.retry_backoff();
    loop {
        let delivery = match read_delivery(context, flow, run_id, message_id) {
            Ok(Some(delivery)) => delivery,
            Ok(None) => return Ok(()),
            Err(error) if error.is_retryable() => {
                tracing::warn!(%error, %run_id, "reading an outbox message failed; retrying");
                if !backoff.wait(&context.shutdown).await {
                    return Err(EffectError::Stopped);
                }
                continue;
            }
            Err(error) => return Err(error),
        };
        let reply = context
            .propose_until_committed(&delivery.target, delivery.command, CommitUrgency::Prompt)
            .await
            .ok_or(EffectError::Stopped)?;
        if matches!(reply.reply, Some(command_reply::Reply::Ok(_))) {
            break;
        }
        tracing::warn!(
            %run_id,
            target_flow_id = %delivery.target.flow_id,
            reply = ?reply.reply,
            "receiver did not accept an outbox message; retrying"
        );
        if !backoff.wait(&context.shutdown).await {
            return Err(EffectError::Stopped);
        }
    }
    let ack_command = FlowCommand {
        flow_id: flow.flow_id.clone(),
        run_id: run_id.as_bytes().to_vec(),
        tenant_id: flow.tenant_id.0,
        command: Some(flow_command::Command::OutboxAck(OutboxAckCommand {
            message_ids: vec![message_id.to_vec()],
        })),
        ..FlowCommand::default()
    };
    let ack_reply = context
        .propose_until_committed(flow, ack_command, CommitUrgency::Prompt)
        .await
        .ok_or(EffectError::Stopped)?;
    if let Some(command_reply::Reply::Error(error)) = ack_reply.reply {
        tracing::warn!(%run_id, message = %error.message, "OutboxAck was rejected");
    }
    Ok(())
}

/// An `InboxMessage` for the receiving Flow.
struct Delivery {
    target: FlowAddress,
    command: FlowCommand,
}

/// Returns the delivery of a committed, unacknowledged outbox message, or `None` when the
/// message is gone or acknowledged.
///
/// Each child start delivery gets a new run ID; apply checks inbox deduplication before using it.
fn read_delivery(
    context: &EffectContext,
    flow: &FlowAddress,
    run_id: RunId,
    message_id: &[u8],
) -> Result<Option<Delivery>, EffectError> {
    let run = context.run_location(flow, run_id);
    let record: OutboxRecord = {
        let reader = context.runtime.flow_state_reader(flow, false)?;
        let key = outbox_key(run.flow_hash, run.run_id, message_id);
        let Some(bytes) = reader.get(ColumnFamily::Flow, &key)? else {
            return Ok(None);
        };
        decode_row("Outbox", &key, &bytes)?
    };
    if record.acknowledged_at_micros != 0 {
        return Ok(None);
    }
    let corrupt = |detail: &str| EffectError::CorruptState {
        table: "Outbox",
        key: format!("{run_id}"),
        detail: detail.to_owned(),
    };
    let target = record
        .target
        .ok_or_else(|| corrupt("outbox message has no target"))?;
    let (source, payload, new_run_id, settings) = match record.payload {
        Some(outbox_record::Payload::ChildStart(start)) => {
            let source = start
                .parent
                .as_ref()
                .and_then(|parent| parent.parent.clone())
                .ok_or_else(|| corrupt("child start has no parent endpoint"))?;
            (
                source,
                inbox_message_command::Payload::ChildStart(start),
                context.id_source.run_id().as_bytes().to_vec(),
                Some(context.config.child_run_settings),
            )
        }
        Some(outbox_record::Payload::ChildCompleted(completed)) => {
            let source = CrossFlowEndpoint {
                tenant_id: flow.tenant_id.0,
                flow_id: completed.child_flow_id.clone(),
                run_id: completed.child_run_id.clone(),
                run_attempt: completed.child_run_attempt,
            };
            (
                source,
                inbox_message_command::Payload::ChildCompleted(completed),
                Vec::new(),
                None,
            )
        }
        None => return Err(corrupt("outbox message has no payload")),
    };
    let command = FlowCommand {
        flow_id: target.flow_id.clone(),
        run_id: target.run_id.clone(),
        tenant_id: target.tenant_id,
        command: Some(flow_command::Command::InboxMessage(InboxMessageCommand {
            message_id: message_id.to_vec(),
            source: Some(source),
            payload: Some(payload),
            new_run_id,
            settings,
        })),
        ..FlowCommand::default()
    };
    Ok(Some(Delivery {
        target: FlowAddress {
            tenant_id: TenantId(target.tenant_id),
            flow_id: target.flow_id,
        },
        command,
    }))
}
