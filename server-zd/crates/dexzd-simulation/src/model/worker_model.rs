// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Commands that the owner and a pull Worker produce for method calls and timers.

use dexzd_keyspace::RunId;
use dexzd_protocol::dex::{
    AttributeWrite, ChannelCondition, ChannelMessage, ChannelMessageDeletion, CloseDecision,
    CloseDecisionType, InvokeExecuteMethodResponse, InvokeWaitForMethodResponse, Kv, StepDecision,
    StepMovement, TimerCondition, Value, WaitingCondition, WorkerErrorResponse, value,
};
use dexzd_protocol::dexzd::v1::{
    StepAttemptFailedCommand, StepAttemptHeartbeatCommand, StepAttemptStartedCommand,
    StepExecutionPhase, StepExecutionRecord, StepInvocationKey, StepInvocationRecord, StepMethod,
    StepMethodCompletedCommand, TimerId, TimersFiredCommand, flow_command::Command,
    step_attempt_heartbeat_command, step_method_completed_command,
};
use rand::RngExt;
use rand_chacha::ChaCha8Rng;

use crate::invariants::GUARDED_ATTRIBUTE_KEYS;
use crate::model::value_generation::{
    ATTRIBUTE_KEYS, CHANNEL_NAMES, LOCAL_KEYS, STEP_TYPES, int_value, null_value, pick,
    random_bytes, random_identifier, step_options, wire_value, worker_value,
};
use crate::state_view::RunView;

/// Active Step executions above which Workers stop creating new Steps.
const MAX_ACTIVE_STEP_EXECUTIONS: usize = 6;
/// Probability that a Worker value is a forged reference or too large.
const INVALID_VALUE_PROBABILITY: f64 = 0.01;

/// A method call of a Step execution as the owner and Worker see it.
pub(crate) struct Call<'run> {
    pub(crate) run_id: RunId,
    pub(crate) run: &'run RunView,
    pub(crate) step: &'run StepExecutionRecord,
    pub(crate) invocation: &'run StepInvocationRecord,
}

impl Call<'_> {
    pub(crate) fn key(&self) -> StepInvocationKey {
        StepInvocationKey {
            step_execution_id: self.step.step_execution_id.clone(),
            method: self.invocation.method,
        }
    }

    /// Whether the owner may start the pending attempt: each lock is free and this call is
    /// its oldest waiter.
    pub(crate) fn can_take_locks(&self) -> bool {
        let key = self.key();
        self.invocation.lock_keys.iter().all(|lock_key| {
            self.run.locks.get(lock_key).is_some_and(|lock| {
                lock.holder.is_none()
                    && lock
                        .waiters
                        .first()
                        .is_some_and(|waiter| waiter.invocation.as_ref() == Some(&key))
            })
        })
    }

    fn is_wait_for(&self) -> bool {
        self.invocation.method == StepMethod::WaitFor as i32
    }

    /// Values the Worker received and may echo back.
    fn visible_values(&self) -> Vec<Value> {
        let mut stored = Vec::new();
        stored.extend(self.step.input.iter());
        stored.extend(
            self.step
                .locals
                .iter()
                .filter_map(|local| local.value.as_ref()),
        );
        if let Some(snapshot) = &self.invocation.snapshot {
            stored.extend(snapshot.attributes.iter().filter_map(|a| a.value.as_ref()));
            for channel in &snapshot.loaded_channel_messages {
                stored.extend(channel.messages.iter().filter_map(|m| m.value.as_ref()));
            }
        }
        stored
            .into_iter()
            .filter_map(|stored| wire_value(self.run_id, stored))
            .collect()
    }
}

pub(crate) fn start_attempt(rng: &mut ChaCha8Rng, call: &Call<'_>) -> Command {
    Command::StepAttemptStarted(StepAttemptStartedCommand {
        invocation: Some(call.key()),
        attempt: call.invocation.attempt,
        task_id: random_bytes(rng).to_vec(),
        pull_routing: None,
    })
}

pub(crate) fn complete(rng: &mut ChaCha8Rng, call: &Call<'_>) -> Command {
    let visible = call.visible_values();
    let mut writes = guarded_writes(rng, call);
    for _ in 0..rng.random_range(0..3) {
        let value = if rng.random_bool(0.15) {
            null_value()
        } else {
            worker_value(rng, call.run_id, &visible, INVALID_VALUE_PROBABILITY)
        };
        writes.push(AttributeWrite {
            key: pick(rng, ATTRIBUTE_KEYS).to_owned(),
            value: Some(value),
            ..AttributeWrite::default()
        });
    }
    let locals = (0..rng.random_range(0..3))
        .map(|_| Kv {
            key: pick(rng, LOCAL_KEYS).to_owned(),
            value: Some(worker_value(
                rng,
                call.run_id,
                &visible,
                INVALID_VALUE_PROBABILITY,
            )),
        })
        .collect();
    let publications = (0..rng.random_range(0..3))
        .map(|_| ChannelMessage {
            channel_name: pick(rng, CHANNEL_NAMES).to_owned(),
            value: Some(worker_value(
                rng,
                call.run_id,
                &visible,
                INVALID_VALUE_PROBABILITY,
            )),
            message_id: random_identifier(rng),
        })
        .collect();
    let deletions = loaded_message_deletions(rng, call);
    let response = if call.is_wait_for() {
        step_method_completed_command::Response::WaitFor(InvokeWaitForMethodResponse {
            upsert_attributes: writes,
            waiting_condition: waiting_condition(rng),
            upsert_step_exe_locals: locals,
            publish_to_channel: publications,
            delete_from_channel: deletions,
            ..InvokeWaitForMethodResponse::default()
        })
    } else {
        step_method_completed_command::Response::Execute(InvokeExecuteMethodResponse {
            step_decision: Some(step_decision(rng, call, &visible)),
            upsert_attributes: writes,
            upsert_step_exe_locals: locals,
            publish_to_channel: publications,
            delete_from_channel: deletions,
            ..InvokeExecuteMethodResponse::default()
        })
    };
    Command::StepMethodCompleted(StepMethodCompletedCommand {
        invocation: Some(call.key()),
        attempt: call.invocation.attempt,
        response: Some(response),
        app_worker_id: format!("worker-{}", rng.random_range(0..3)),
        ..StepMethodCompletedCommand::default()
    })
}

/// A lock holder increments each guarded key it read (the L2 read-modify-write).
fn guarded_writes(rng: &mut ChaCha8Rng, call: &Call<'_>) -> Vec<AttributeWrite> {
    let mut writes = Vec::new();
    let Some(snapshot) = &call.invocation.snapshot else {
        return writes;
    };
    for guarded_key in GUARDED_ATTRIBUTE_KEYS {
        let holds_lock = call
            .invocation
            .lock_keys
            .iter()
            .any(|key| key == guarded_key);
        if !holds_lock || rng.random_bool(0.2) {
            continue;
        }
        let read = snapshot
            .attributes
            .iter()
            .find(|attribute| attribute.key == *guarded_key)
            .and_then(|attribute| wire_value(call.run_id, attribute.value.as_ref()?));
        let current = match read.and_then(|value| value.kind) {
            Some(value::Kind::IntValue(number)) => number,
            _ => 0,
        };
        writes.push(AttributeWrite {
            key: (*guarded_key).to_owned(),
            value: Some(int_value(current + 1)),
            ..AttributeWrite::default()
        });
    }
    writes
}

fn loaded_message_deletions(rng: &mut ChaCha8Rng, call: &Call<'_>) -> Vec<ChannelMessageDeletion> {
    let Some(snapshot) = &call.invocation.snapshot else {
        return Vec::new();
    };
    snapshot
        .loaded_channel_messages
        .iter()
        .flat_map(|channel| &channel.messages)
        .filter(|_| rng.random_bool(0.3))
        .map(|message| ChannelMessageDeletion {
            channel_name: message.channel_name.clone(),
            message_id: message.message_id.clone(),
        })
        .collect()
}

fn waiting_condition(rng: &mut ChaCha8Rng) -> Option<WaitingCondition> {
    match rng.random_range(0..20) {
        0 => Some(WaitingCondition {
            channel_conditions: vec![ChannelCondition {
                condition_id: "message".to_owned(),
                channel_name: pick(rng, CHANNEL_NAMES).to_owned(),
                at_least: Some(1),
                at_most: Some(rng.random_range(1..3)),
            }],
            ..WaitingCondition::default()
        }),
        1 => Some(WaitingCondition {
            timer_conditions: vec![TimerCondition {
                condition_id: "timer".to_owned(),
                duration_seconds: rng.random_range(1..30),
                ..TimerCondition::default()
            }],
            ..WaitingCondition::default()
        }),
        _ => None,
    }
}

fn step_decision(rng: &mut ChaCha8Rng, call: &Call<'_>, visible: &[Value]) -> StepDecision {
    let active_count = call
        .run
        .step_executions
        .values()
        .filter(|step| {
            matches!(
                StepExecutionPhase::try_from(step.phase),
                Ok(StepExecutionPhase::WaitFor
                    | StepExecutionPhase::Waiting
                    | StepExecutionPhase::Execute)
            )
        })
        .count();
    let next_steps = if active_count < MAX_ACTIVE_STEP_EXECUTIONS {
        (0..rng.random_range(0..3))
            .map(|_| StepMovement {
                step_type: pick(rng, STEP_TYPES).to_owned(),
                step_input: rng
                    .random_bool(0.8)
                    .then(|| worker_value(rng, call.run_id, visible, INVALID_VALUE_PROBABILITY)),
                step_options: step_options(rng),
                ..StepMovement::default()
            })
            .collect()
    } else {
        Vec::new()
    };
    let close_decision_type = match rng.random_range(0..40) {
        0..=2 => Some(CloseDecisionType::ForceComplete),
        3..=5 => Some(CloseDecisionType::GracefulComplete),
        6 => Some(CloseDecisionType::ForceFail),
        7 => Some(CloseDecisionType::DeadEnd),
        8 => Some(CloseDecisionType::ForceCompleteOnChannelsEmpty),
        _ => None,
    };
    let close_decision = close_decision_type.map(|close_type| CloseDecision {
        close_decision_type: close_type as i32,
        close_input: rng
            .random_bool(0.7)
            .then(|| worker_value(rng, call.run_id, visible, INVALID_VALUE_PROBABILITY)),
        conditional_channel_names: vec![pick(rng, CHANNEL_NAMES).to_owned()],
    });
    StepDecision {
        next_steps,
        close_decision,
        cancel_step_types: if rng.random_bool(0.05) {
            vec![pick(rng, STEP_TYPES).to_owned()]
        } else {
            Vec::new()
        },
        ..StepDecision::default()
    }
}

pub(crate) fn fail(rng: &mut ChaCha8Rng, call: &Call<'_>) -> Command {
    Command::StepAttemptFailed(StepAttemptFailedCommand {
        invocation: Some(call.key()),
        attempt: call.invocation.attempt,
        kind: rng.random_range(1..9),
        worker_error: Some(WorkerErrorResponse {
            detail: "simulated failure".to_owned(),
            retry_after_seconds: if rng.random_bool(0.2) {
                rng.random_range(1..10)
            } else {
                0
            },
            ..WorkerErrorResponse::default()
        }),
        server_detail: String::new(),
        is_non_retryable: rng.random_bool(0.03),
        pull_routing: None,
    })
}

pub(crate) fn heartbeat(rng: &mut ChaCha8Rng, call: &Call<'_>) -> Command {
    let checkpoint = if rng.random_bool(0.25) {
        step_attempt_heartbeat_command::Checkpoint::IsCleared(true)
    } else {
        let visible = call.visible_values();
        step_attempt_heartbeat_command::Checkpoint::Value(worker_value(
            rng,
            call.run_id,
            &visible,
            INVALID_VALUE_PROBABILITY,
        ))
    };
    Command::StepAttemptHeartbeat(StepAttemptHeartbeatCommand {
        invocation: Some(call.key()),
        attempt: call.invocation.attempt,
        checkpoint: Some(checkpoint),
    })
}

/// A result, failure, or heartbeat for an attempt that is not the running one.
pub(crate) fn stale_attempt_command(rng: &mut ChaCha8Rng, call: &Call<'_>) -> Command {
    let attempt = if call.invocation.attempt > 1 && rng.random_bool(0.6) {
        call.invocation.attempt - 1
    } else {
        call.invocation.attempt + 1
    };
    let mut command = match rng.random_range(0..3) {
        0 => complete(rng, call),
        1 => fail(rng, call),
        _ => heartbeat(rng, call),
    };
    match &mut command {
        Command::StepMethodCompleted(completed) => completed.attempt = attempt,
        Command::StepAttemptFailed(failed) => failed.attempt = attempt,
        Command::StepAttemptHeartbeat(heartbeat) => heartbeat.attempt = attempt,
        _ => {}
    }
    command
}

pub(crate) fn fire_timers(timers: Vec<TimerId>) -> Command {
    Command::TimersFired(TimersFiredCommand { timers })
}
