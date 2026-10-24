// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Responses of simulated pull Workers to the method requests and RPCs the engine dispatches.

use dexzd_protocol::dex::{
    AttributeWrite, ChannelCondition, ChannelMessage, ChannelMessageDeletion, ChannelValues,
    CloseDecision, CloseDecisionType, InvokeExecuteMethodResponse, InvokeWaitForMethodResponse,
    InvokeWorkerRpcResponse, Kv, RetryPolicy, StepDecision, StepDurability, StepMovement,
    StepOptions, TimerCondition, Value, WaitingCondition, WorkerErrorResponse, value,
};
use rand::RngExt;
use rand_chacha::ChaCha8Rng;
use std::collections::BTreeMap;

use crate::invariants::GUARDED_ATTRIBUTE_KEYS;
use crate::model::{
    ATTRIBUTE_KEYS, CHANNEL_NAMES, LOCAL_KEYS, STEP_TYPES, int_value, null_value, pick,
    random_value,
};

/// The Step type whose Execute method holds the guarded `counter` lock.
pub(crate) const COUNTER_STEP_TYPE: &str = "ChargeStep";

/// Random valid Step options, except locks fixed by Step type so a Worker knows its locks.
pub(crate) fn engine_step_options(rng: &mut ChaCha8Rng, step_type: &str) -> StepOptions {
    StepOptions {
        skip_wait_for: rng.random_bool(0.4),
        wait_for_lock_attribute_keys: match step_type {
            "ShipStep" => vec!["orders/1".to_owned()],
            _ => Vec::new(),
        },
        execute_lock_attribute_keys: match step_type {
            COUNTER_STEP_TYPE => vec![GUARDED_ATTRIBUTE_KEYS[0].to_owned()],
            "ShipStep" => vec!["status".to_owned()],
            _ => Vec::new(),
        },
        wait_for_retry_policy: retry_policy(rng),
        execute_retry_policy: retry_policy(rng),
        wait_for_load_attribute_map_instances: subset(rng, &["orders/", "users/alice"], 0.3),
        execute_load_attribute_map_instances: subset(rng, &["orders/", "orders/2"], 0.3),
        wait_for_load_channel_names: subset(rng, LOADABLE_CHANNEL_NAMES, 0.25),
        execute_load_channel_names: subset(rng, LOADABLE_CHANNEL_NAMES, 0.3),
        execute_load_channel_map_instances: subset(rng, &["queue/"], 0.3),
        wait_for_durability_override: random_durability(rng) as i32,
        execute_durability_override: random_durability(rng) as i32,
        wait_for_timeout_seconds: random_timeout_seconds(rng),
        execute_timeout_seconds: random_timeout_seconds(rng),
        heartbeat_timeout_seconds: if rng.random_bool(0.3) {
            rng.random_range(10..20)
        } else {
            0
        },
        ..StepOptions::default()
    }
}

/// Channels a Step may load by name; `queue/a` and `queue/b` belong to the `queue/` map.
const LOADABLE_CHANNEL_NAMES: &[&str] = &["inbox", "events"];

fn retry_policy(rng: &mut ChaCha8Rng) -> Option<RetryPolicy> {
    rng.random_bool(0.7).then(|| RetryPolicy {
        initial_interval_seconds: rng.random_range(1..3),
        backoff_coefficient: if rng.random_bool(0.5) { 2.0 } else { 0.0 },
        maximum_interval_seconds: if rng.random_bool(0.5) { 5 } else { 0 },
        maximum_attempts: rng.random_range(0..5),
        total_duration_seconds: 0,
    })
}

/// Each item independently with `probability`, in the given order.
fn subset(rng: &mut ChaCha8Rng, items: &[&str], probability: f64) -> Vec<String> {
    items
        .iter()
        .filter(|_| rng.random_bool(probability))
        .map(|item| (*item).to_owned())
        .collect()
}

fn random_durability(rng: &mut ChaCha8Rng) -> StepDurability {
    match rng.random_range(0..10) {
        0..=3 => StepDurability::Async,
        4..=6 => StepDurability::Sync,
        _ => StepDurability::Unspecified,
    }
}

fn random_timeout_seconds(rng: &mut ChaCha8Rng) -> i32 {
    if rng.random_bool(0.5) {
        rng.random_range(3..20)
    } else {
        0
    }
}

/// What a Worker sees of one method request.
pub(crate) struct MethodCallView<'request> {
    pub(crate) step_type: &'request str,
    pub(crate) is_execute: bool,
    pub(crate) attributes: &'request [Kv],
    pub(crate) loaded_channel_messages: &'request BTreeMap<String, ChannelValues>,
}

impl MethodCallView<'_> {
    /// Whether the attempt holds the guarded `counter` lock.
    fn holds_counter_lock(&self) -> bool {
        self.is_execute && self.step_type == COUNTER_STEP_TYPE
    }
}

/// A WaitFor result: Attribute writes, locals, Channel changes, and maybe a waiting condition.
pub(crate) fn wait_for_response(
    rng: &mut ChaCha8Rng,
    call: &MethodCallView<'_>,
) -> InvokeWaitForMethodResponse {
    InvokeWaitForMethodResponse {
        upsert_attributes: attribute_writes(rng, call),
        waiting_condition: waiting_condition(rng),
        upsert_step_exe_locals: locals(rng),
        publish_to_channel: publications(rng),
        delete_from_channel: loaded_message_deletions(rng, call),
        ..InvokeWaitForMethodResponse::default()
    }
}

/// An Execute result. New Steps draw from `steps_left`, so every run eventually closes.
pub(crate) fn execute_response(
    rng: &mut ChaCha8Rng,
    call: &MethodCallView<'_>,
    steps_left: &mut u32,
) -> InvokeExecuteMethodResponse {
    InvokeExecuteMethodResponse {
        step_decision: Some(step_decision(rng, steps_left)),
        upsert_attributes: attribute_writes(rng, call),
        upsert_step_exe_locals: locals(rng),
        publish_to_channel: publications(rng),
        delete_from_channel: loaded_message_deletions(rng, call),
        ..InvokeExecuteMethodResponse::default()
    }
}

/// An RPC result that writes at least one Attribute, so an acknowledged RPC is always committed.
pub(crate) fn rpc_response(rng: &mut ChaCha8Rng) -> InvokeWorkerRpcResponse {
    InvokeWorkerRpcResponse {
        output: rng.random_bool(0.7).then(|| random_value(rng)),
        upsert_attributes: (0..rng.random_range(1..3))
            .map(|_| AttributeWrite {
                key: pick(rng, ATTRIBUTE_KEYS).to_owned(),
                value: Some(random_value(rng)),
                ..AttributeWrite::default()
            })
            .collect(),
        publish_to_channel: publications(rng),
        ..InvokeWorkerRpcResponse::default()
    }
}

pub(crate) fn worker_error(rng: &mut ChaCha8Rng) -> WorkerErrorResponse {
    WorkerErrorResponse {
        detail: "simulated Worker failure".to_owned(),
        error_type: "SimulatedError".to_owned(),
        retry_after_seconds: if rng.random_bool(0.2) {
            rng.random_range(1..4)
        } else {
            0
        },
        ..WorkerErrorResponse::default()
    }
}

/// A heartbeat checkpoint: a value, or `None` to send a heartbeat without one.
pub(crate) fn heartbeat_value(rng: &mut ChaCha8Rng) -> Option<Value> {
    rng.random_bool(0.6).then(|| random_value(rng))
}

/// A lock holder increments `counter` from its snapshot (the L2 read-modify-write).
fn attribute_writes(rng: &mut ChaCha8Rng, call: &MethodCallView<'_>) -> Vec<AttributeWrite> {
    let mut writes = Vec::new();
    if call.holds_counter_lock() && rng.random_bool(0.9) {
        let current = call
            .attributes
            .iter()
            .find(|attribute| attribute.key == GUARDED_ATTRIBUTE_KEYS[0])
            .and_then(|attribute| attribute.value.as_ref()?.kind.as_ref())
            .and_then(|kind| match kind {
                value::Kind::IntValue(number) => Some(*number),
                _ => None,
            })
            .unwrap_or(0);
        writes.push(AttributeWrite {
            key: GUARDED_ATTRIBUTE_KEYS[0].to_owned(),
            value: Some(int_value(current + 1)),
            ..AttributeWrite::default()
        });
    }
    for _ in 0..rng.random_range(0..3) {
        let value = if rng.random_bool(0.1) {
            null_value()
        } else {
            random_value(rng)
        };
        writes.push(AttributeWrite {
            key: pick(rng, ATTRIBUTE_KEYS).to_owned(),
            value: Some(value),
            ..AttributeWrite::default()
        });
    }
    writes
}

fn locals(rng: &mut ChaCha8Rng) -> Vec<Kv> {
    (0..rng.random_range(0..2))
        .map(|_| Kv {
            key: pick(rng, LOCAL_KEYS).to_owned(),
            value: Some(random_value(rng)),
        })
        .collect()
}

fn publications(rng: &mut ChaCha8Rng) -> Vec<ChannelMessage> {
    (0..rng.random_range(0..3))
        .map(|_| ChannelMessage {
            channel_name: pick(rng, CHANNEL_NAMES).to_owned(),
            value: Some(random_value(rng)),
            message_id: String::new(),
        })
        .collect()
}

fn loaded_message_deletions(
    rng: &mut ChaCha8Rng,
    call: &MethodCallView<'_>,
) -> Vec<ChannelMessageDeletion> {
    call.loaded_channel_messages
        .values()
        .flat_map(|channel| &channel.messages)
        .filter(|_| rng.random_bool(0.3))
        .map(|message| ChannelMessageDeletion {
            channel_name: message.channel_name.clone(),
            message_id: message.message_id.clone(),
        })
        .collect()
}

fn waiting_condition(rng: &mut ChaCha8Rng) -> Option<WaitingCondition> {
    match rng.random_range(0..10) {
        0 | 1 => Some(WaitingCondition {
            channel_conditions: vec![ChannelCondition {
                condition_id: "message".to_owned(),
                channel_name: pick(rng, CHANNEL_NAMES).to_owned(),
                at_least: Some(1),
                at_most: Some(rng.random_range(1..3)),
            }],
            ..WaitingCondition::default()
        }),
        2 | 3 => Some(WaitingCondition {
            timer_conditions: vec![TimerCondition {
                condition_id: "timer".to_owned(),
                duration_seconds: rng.random_range(1..20),
                ..TimerCondition::default()
            }],
            ..WaitingCondition::default()
        }),
        _ => None,
    }
}

fn step_decision(rng: &mut ChaCha8Rng, steps_left: &mut u32) -> StepDecision {
    let mut next_steps = Vec::new();
    for _ in 0..rng.random_range(0..3) {
        if *steps_left == 0 {
            break;
        }
        *steps_left -= 1;
        let step_type = pick(rng, STEP_TYPES);
        next_steps.push(StepMovement {
            step_type: step_type.to_owned(),
            step_input: rng.random_bool(0.8).then(|| random_value(rng)),
            step_options: Some(engine_step_options(rng, step_type)),
            ..StepMovement::default()
        });
    }
    let close_weight = if *steps_left == 0 { 6 } else { 1 };
    let close_decision_type = match rng.random_range(0..12) {
        roll if roll < close_weight => Some(if rng.random_bool(0.7) {
            CloseDecisionType::GracefulComplete
        } else {
            CloseDecisionType::ForceComplete
        }),
        11 if rng.random_bool(0.3) => Some(CloseDecisionType::ForceFail),
        _ => None,
    };
    StepDecision {
        next_steps,
        close_decision: close_decision_type.map(|close_type| CloseDecision {
            close_decision_type: close_type as i32,
            close_input: rng.random_bool(0.5).then(|| random_value(rng)),
            ..CloseDecision::default()
        }),
        ..StepDecision::default()
    }
}
