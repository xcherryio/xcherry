// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! FlowService requests of simulated clients. Each write carries a unique operation marker.

use dexzd_protocol::dex::{
    AttributeWrite, ChannelMessage, DeleteChannelMessageRequest, FlowAlreadyStartedOptions,
    FlowConfig, FlowStartOptions, FlowTimeoutHandlerOptions, FlowTimeoutPolicy, IdReusePolicy,
    InvokeRpcRequest, PublishToChannelRequest, SetAttributesRequest, StartFlowRequest,
    StopFlowRequest, Value, WorkerTarget, value,
};
use dexzd_protocol::dexzd::v1::{FlowCommand, flow_command::Command};
use rand::RngExt;
use rand_chacha::ChaCha8Rng;

use crate::engine_simulation::workload::worker_responses::engine_step_options;
use crate::model::{
    ATTRIBUTE_KEYS, CHANNEL_NAMES, STEP_TYPES, int_value, null_value, pick, random_value,
};

pub(crate) fn start_flow_request(
    rng: &mut ChaCha8Rng,
    flow_id: &str,
    marker: &str,
) -> StartFlowRequest {
    let flow_timeout_seconds = if rng.random_bool(0.2) {
        rng.random_range(20..120)
    } else {
        0
    };
    let policy = match rng.random_range(0..6) {
        _ if flow_timeout_seconds == 0 => FlowTimeoutPolicy::Unspecified,
        0 => FlowTimeoutPolicy::Fail,
        1 => FlowTimeoutPolicy::Cancel,
        2 => FlowTimeoutPolicy::Handler,
        _ => FlowTimeoutPolicy::Unspecified,
    };
    let id_reuse_policy = match rng.random_range(0..4) {
        0 => IdReusePolicy::AllowIfNoRunning,
        1 => IdReusePolicy::AllowTerminateIfRunning,
        _ => IdReusePolicy::Unspecified,
    };
    let mut attributes = vec![AttributeWrite {
        key: "counter".to_owned(),
        value: Some(int_value(0)),
        ..AttributeWrite::default()
    }];
    let key_count = rng.random_range(0..3);
    for key in distinct_keys(rng, key_count) {
        attributes.push(AttributeWrite {
            key,
            value: Some(random_value(rng)),
            ..AttributeWrite::default()
        });
    }
    let start_step_type = pick(rng, STEP_TYPES);
    StartFlowRequest {
        flow_id: flow_id.to_owned(),
        flow_type: "OrderFlow".to_owned(),
        flow_timeout_seconds,
        flow_timeout_policy: policy as i32,
        start_step_type: start_step_type.to_owned(),
        step_input: rng.random_bool(0.8).then(|| random_value(rng)),
        step_options: Some(engine_step_options(rng, start_step_type)),
        flow_start_options: Some(FlowStartOptions {
            id_reuse_policy: id_reuse_policy as i32,
            attributes,
            flow_already_started_options: Some(FlowAlreadyStartedOptions {
                ignore_already_started_error: rng.random_bool(0.3),
            }),
            flow_config_override: Some(FlowConfig {
                worker_target: Some(WorkerTarget {
                    is_pull_mode: true,
                    ..WorkerTarget::default()
                }),
                ..FlowConfig::default()
            }),
            timeout_handler_options: (policy == FlowTimeoutPolicy::Handler).then(|| {
                FlowTimeoutHandlerOptions {
                    lock_attribute_keys: vec!["status".to_owned()],
                    ..FlowTimeoutHandlerOptions::default()
                }
            }),
            ..FlowStartOptions::default()
        }),
        request_id: marker.to_owned(),
    }
}

pub(crate) fn set_attributes_request(
    rng: &mut ChaCha8Rng,
    flow_id: &str,
    marker: &str,
) -> SetAttributesRequest {
    let key_count = rng.random_range(1..4);
    let attributes = distinct_keys(rng, key_count)
        .into_iter()
        .map(|key| AttributeWrite {
            key,
            value: Some(if rng.random_bool(0.1) {
                null_value()
            } else {
                random_value(rng)
            }),
            ..AttributeWrite::default()
        })
        .collect();
    SetAttributesRequest {
        flow_id: flow_id.to_owned(),
        attributes,
        request_id: marker.to_owned(),
        ..SetAttributesRequest::default()
    }
}

/// The first message's value is the marker, since publishing has no request ID.
pub(crate) fn publish_request(
    rng: &mut ChaCha8Rng,
    flow_id: &str,
    marker: &str,
) -> PublishToChannelRequest {
    let mut messages = vec![ChannelMessage {
        channel_name: pick(rng, CHANNEL_NAMES).to_owned(),
        value: Some(marker_value(marker)),
        message_id: String::new(),
    }];
    for _ in 0..rng.random_range(0..3) {
        messages.push(ChannelMessage {
            channel_name: pick(rng, CHANNEL_NAMES).to_owned(),
            value: Some(random_value(rng)),
            message_id: String::new(),
        });
    }
    PublishToChannelRequest {
        flow_id: flow_id.to_owned(),
        messages,
        ..PublishToChannelRequest::default()
    }
}

pub(crate) fn delete_message_request(
    flow_id: &str,
    channel_name: &str,
    message_id: &str,
    marker: &str,
) -> DeleteChannelMessageRequest {
    DeleteChannelMessageRequest {
        flow_id: flow_id.to_owned(),
        channel_name: channel_name.to_owned(),
        message_id: message_id.to_owned(),
        request_id: marker.to_owned(),
        ..DeleteChannelMessageRequest::default()
    }
}

/// The reason carries the marker, since stopping has no request ID.
pub(crate) fn stop_request(rng: &mut ChaCha8Rng, flow_id: &str, marker: &str) -> StopFlowRequest {
    StopFlowRequest {
        flow_id: flow_id.to_owned(),
        reason: marker.to_owned(),
        stop_type: rng.random_range(0..3),
        ..StopFlowRequest::default()
    }
}

pub(crate) fn rpc_request(rng: &mut ChaCha8Rng, flow_id: &str, marker: &str) -> InvokeRpcRequest {
    InvokeRpcRequest {
        flow_id: flow_id.to_owned(),
        rpc_name: "getStatus".to_owned(),
        input: Some(random_value(rng)),
        request_id: marker.to_owned(),
        timeout_seconds: rng.random_range(1..10),
        lock_attribute_keys: if rng.random_bool(0.3) {
            vec!["status".to_owned()]
        } else {
            Vec::new()
        },
        ..InvokeRpcRequest::default()
    }
}

/// Up to `count` distinct Attribute keys, since a request may write each key once.
fn distinct_keys(rng: &mut ChaCha8Rng, count: usize) -> Vec<String> {
    let mut keys: Vec<String> = Vec::new();
    for _ in 0..count {
        let key = pick(rng, ATTRIBUTE_KEYS);
        if !keys.iter().any(|existing| existing == key) {
            keys.push(key.to_owned());
        }
    }
    keys
}

fn marker_value(marker: &str) -> Value {
    Value {
        kind: Some(value::Kind::StringValue(marker.to_owned())),
    }
}

/// Returns the operation marker of a client write command, if it has one.
pub(crate) fn operation_marker(command: &FlowCommand) -> Option<&str> {
    let marker = match command.command.as_ref()? {
        Command::FlowStart(start) => start.request.as_ref()?.request_id.as_str(),
        Command::AttributesSet(set) => set.request.as_ref()?.request_id.as_str(),
        Command::ChannelMessageDelete(delete) => delete.request.as_ref()?.request_id.as_str(),
        Command::FlowStop(stop) => stop.request.as_ref()?.reason.as_str(),
        Command::RpcCommitted(rpc) => rpc.request.as_ref()?.request_id.as_str(),
        Command::ChannelPublish(publish) => {
            let first = publish.request.as_ref()?.messages.first()?;
            match first.value.as_ref()?.kind.as_ref()? {
                value::Kind::StringValue(text) => text.as_str(),
                _ => return None,
            }
        }
        _ => return None,
    };
    marker
        .starts_with(OPERATION_MARKER_PREFIX)
        .then_some(marker)
}

/// Prefix of every operation marker.
pub(crate) const OPERATION_MARKER_PREFIX: &str = "client-op-";
