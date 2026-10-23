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

use crate::effects::state_reads::{decode_row, read_run_record};
use crate::effects::{EffectContext, EffectError};
use crate::runtime::{CommitUrgency, FlowAddress};

/// Delivers one outbox message unless it was already acknowledged.
pub(crate) async fn deliver_outbox_message(
    context: &EffectContext,
    flow: &FlowAddress,
    run_id: RunId,
    message_id: &[u8],
) -> Result<(), EffectError> {
    let run = context.run_location(flow, run_id);
    let (record, source_run_attempt) = {
        let reader = context.runtime.flow_state_reader(flow, false)?;
        let key = outbox_key(run.flow_hash, run.run_id, message_id);
        let Some(bytes) = reader.get(ColumnFamily::Flow, &key)? else {
            return Ok(());
        };
        let record: OutboxRecord = decode_row("Outbox", &key, &bytes)?;
        let source_run_attempt = read_run_record(reader.as_ref(), run)?
            .map(|record| record.current_attempt)
            .unwrap_or_default();
        (record, source_run_attempt)
    };
    if record.acknowledged_at_micros != 0 {
        return Ok(());
    }
    let target = record
        .target
        .clone()
        .ok_or_else(|| EffectError::CorruptState {
            table: "Outbox",
            key: format!("{run_id}"),
            detail: "outbox message has no target".to_owned(),
        })?;
    let payload = match record.payload {
        Some(outbox_record::Payload::ChildStart(start)) => {
            inbox_message_command::Payload::ChildStart(start)
        }
        Some(outbox_record::Payload::ChildCompleted(completed)) => {
            inbox_message_command::Payload::ChildCompleted(completed)
        }
        None => {
            return Err(EffectError::CorruptState {
                table: "Outbox",
                key: format!("{run_id}"),
                detail: "outbox message has no payload".to_owned(),
            });
        }
    };
    let target_flow = FlowAddress {
        tenant_id: TenantId(target.tenant_id),
        flow_id: target.flow_id.clone(),
    };
    let inbox_command = FlowCommand {
        flow_id: target.flow_id.clone(),
        run_id: target.run_id.clone(),
        tenant_id: target.tenant_id,
        command: Some(flow_command::Command::InboxMessage(InboxMessageCommand {
            message_id: message_id.to_vec(),
            source: Some(CrossFlowEndpoint {
                tenant_id: flow.tenant_id.0,
                flow_id: flow.flow_id.clone(),
                run_id: run_id.as_bytes().to_vec(),
                run_attempt: source_run_attempt,
            }),
            payload: Some(payload),
            new_run_id: Vec::new(),
            settings: None,
        })),
        ..FlowCommand::default()
    };
    let inbox_reply = context
        .propose_until_committed(&target_flow, inbox_command, CommitUrgency::Prompt)
        .await
        .ok_or(EffectError::Stopped)?;
    if let Some(command_reply::Reply::Error(error)) = &inbox_reply.reply {
        // Apply rejects deterministically; redelivery would be rejected again.
        tracing::warn!(%run_id, message = %error.message, "receiver rejected an outbox message");
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
