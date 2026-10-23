// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_keyspace::row_keys::length_prefixed_name;

/// Something that keeps a value alive. Its encoding is part of the holder row key, so the
/// kind bytes are permanent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ValueHolder {
    /// The FlowStart command: its input and initial Attributes, held until the run is deleted
    /// (plan D12).
    Genesis,
    /// The current value of an Attribute.
    Attribute { attribute_key: String },
    /// A pending Channel message.
    ChannelMessage {
        channel_name: String,
        enqueued_seq: u64,
    },
    /// A Step execution's input.
    StepInput { step_execution_id: String },
    /// A Step execution's local.
    StepLocal {
        step_execution_id: String,
        local_key: String,
    },
    /// A message consumed into a Step execution's condition results.
    ConditionResult {
        step_execution_id: String,
        channel_name: String,
        enqueued_seq: u64,
    },
    /// A heartbeat checkpoint of a method call.
    HeartbeatCheckpoint {
        step_execution_id: String,
        method: i32,
    },
    /// The snapshot of a running attempt (a pin, plan R3').
    AttemptSnapshot {
        step_execution_id: String,
        method: i32,
        attempt: u32,
    },
    /// An output collected for the Flow result.
    FlowResult { index: u32 },
    /// A child Flow result kept for SubFlow conditions.
    #[expect(dead_code, reason = "Used by SubFlow results (task A6).")]
    SubFlowResult { child_flow_id: String },
    /// A recorded RPC reply for request ID deduplication.
    #[expect(dead_code, reason = "Used by RPC commits.")]
    RpcReply { request_id: String },
    /// A complete value pinned by an unexpired outbox message.
    OutboxMessage {
        message_id: Vec<u8>,
        value_index: u32,
    },
    /// One output of a completed child Flow.
    SubFlowResultOutput {
        child_flow_id: String,
        output_index: u32,
    },
}

impl ValueHolder {
    /// Returns the holder's key bytes.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::new();
        match self {
            Self::Genesis => encoded.push(0x01),
            Self::Attribute { attribute_key } => {
                encoded.push(0x02);
                encoded.extend_from_slice(&length_prefixed_name(attribute_key));
            }
            Self::ChannelMessage {
                channel_name,
                enqueued_seq,
            } => {
                encoded.push(0x03);
                encoded.extend_from_slice(&length_prefixed_name(channel_name));
                encoded.extend_from_slice(&enqueued_seq.to_be_bytes());
            }
            Self::StepInput { step_execution_id } => {
                encoded.push(0x04);
                encoded.extend_from_slice(&length_prefixed_name(step_execution_id));
            }
            Self::StepLocal {
                step_execution_id,
                local_key,
            } => {
                encoded.push(0x05);
                encoded.extend_from_slice(&length_prefixed_name(step_execution_id));
                encoded.extend_from_slice(&length_prefixed_name(local_key));
            }
            Self::ConditionResult {
                step_execution_id,
                channel_name,
                enqueued_seq,
            } => {
                encoded.push(0x06);
                encoded.extend_from_slice(&length_prefixed_name(step_execution_id));
                encoded.extend_from_slice(&length_prefixed_name(channel_name));
                encoded.extend_from_slice(&enqueued_seq.to_be_bytes());
            }
            Self::HeartbeatCheckpoint {
                step_execution_id,
                method,
            } => {
                encoded.push(0x07);
                encoded.extend_from_slice(&length_prefixed_name(step_execution_id));
                encoded.extend_from_slice(&method.to_be_bytes());
            }
            Self::AttemptSnapshot {
                step_execution_id,
                method,
                attempt,
            } => {
                encoded.push(0x08);
                encoded.extend_from_slice(&length_prefixed_name(step_execution_id));
                encoded.extend_from_slice(&method.to_be_bytes());
                encoded.extend_from_slice(&attempt.to_be_bytes());
            }
            Self::FlowResult { index } => {
                encoded.push(0x09);
                encoded.extend_from_slice(&index.to_be_bytes());
            }
            Self::SubFlowResult { child_flow_id } => {
                encoded.push(0x0a);
                encoded.extend_from_slice(&length_prefixed_name(child_flow_id));
            }
            Self::RpcReply { request_id } => {
                encoded.push(0x0b);
                encoded.extend_from_slice(&length_prefixed_name(request_id));
            }
            Self::OutboxMessage {
                message_id,
                value_index,
            } => {
                encoded.push(0x0c);
                encoded.extend_from_slice(message_id);
                encoded.extend_from_slice(&value_index.to_be_bytes());
            }
            Self::SubFlowResultOutput {
                child_flow_id,
                output_index,
            } => {
                encoded.push(0x0d);
                encoded.extend_from_slice(&length_prefixed_name(child_flow_id));
                encoded.extend_from_slice(&output_index.to_be_bytes());
            }
        }
        encoded
    }
}
