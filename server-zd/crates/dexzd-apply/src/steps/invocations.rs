// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Method calls of Step executions and their Attribute locks (plan section 8.3).
//!
//! Locks are held only while an attempt runs. Waiting calls queue FIFO per key by the seq at
//! which they became pending; apply re-checks what the owner proposed.

use dexzd_protocol::dex::{StepDurability, StepOptions};
use dexzd_protocol::dexzd::v1::{
    LockWaiterRecord, StepExecutionRecord, StepInvocationKey, StepInvocationPhase,
    StepInvocationRecord, StepMethod,
};

use crate::command_handlers::run_cursor::RunCursor;
use crate::state_access::FlowTransaction;
use crate::values::{ValueHolder, release_stored_value};
use crate::{ApplyEffect, ApplyError};

/// How a running attempt ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InvocationEnd {
    /// The Worker returned a result or an error.
    Finished,
    /// The Step was canceled, the Flow closed, or time travel abandoned the attempt; the
    /// Worker should stop.
    Canceled,
}

pub(crate) fn invocation_key(step: &StepExecutionRecord, method: StepMethod) -> StepInvocationKey {
    StepInvocationKey {
        step_execution_id: step.step_execution_id.clone(),
        method: method as i32,
    }
}

/// Lock keys of a method: the Attribute keys declared in the Step options, sorted and unique.
fn method_lock_keys(step: &StepExecutionRecord, method: StepMethod) -> Vec<String> {
    let options = step.options.clone().unwrap_or_default();
    let mut keys = match method {
        StepMethod::WaitFor => options.wait_for_lock_attribute_keys,
        StepMethod::Execute | StepMethod::Unspecified => options.execute_lock_attribute_keys,
    };
    keys.sort();
    keys.dedup();
    keys
}

/// Makes `attempt` of the method pending and queues it on its lock keys.
pub(crate) fn make_invocation_pending(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    step: &mut StepExecutionRecord,
    method: StepMethod,
    attempt: u32,
    first_attempt_at_micros: i64,
) -> Result<(), ApplyError> {
    let lock_keys = method_lock_keys(step, method);
    let key = invocation_key(step, method);
    for lock_key in &lock_keys {
        let mut lock = cursor.rows.lock(transaction, lock_key)?;
        lock.waiters.push(LockWaiterRecord {
            invocation: Some(key.clone()),
            pending_since_seq: cursor.seq,
        });
        cursor.rows.put_lock(transaction, lock_key, &lock);
    }
    let mut previous = step.invocation.take();
    if let Some(other_method) = previous
        .as_mut()
        .filter(|invocation| invocation.method != method as i32)
    {
        release_heartbeat_checkpoint(transaction, cursor, &step.step_execution_id, other_method)?;
        previous = None;
    }
    let effective_durability = previous
        .as_ref()
        .map(|invocation| invocation.effective_durability)
        .filter(|durability| *durability != StepDurability::Unspecified as i32)
        .unwrap_or_else(|| effective_method_durability(cursor, step, method));
    step.invocation = Some(StepInvocationRecord {
        method: method as i32,
        phase: StepInvocationPhase::Pending as i32,
        attempt,
        first_attempt_at_micros,
        pending_since_seq: cursor.seq,
        lock_keys,
        effective_durability,
        last_failure: previous
            .as_ref()
            .and_then(|invocation| invocation.last_failure.clone()),
        heartbeat_checkpoint: previous.and_then(|invocation| invocation.heartbeat_checkpoint),
        ..StepInvocationRecord::default()
    });
    let run_id = cursor.run_id();
    let run_attempt = cursor.attempt();
    cursor.add_effect(ApplyEffect::InvocationPending {
        run_id,
        run_attempt,
        invocation: key,
        attempt,
    });
    Ok(())
}

fn effective_method_durability(
    cursor: &RunCursor,
    step: &StepExecutionRecord,
    method: StepMethod,
) -> i32 {
    let options = step.options.as_ref();
    let override_durability = match method {
        StepMethod::WaitFor => options.map_or(0, |options| options.wait_for_durability_override),
        StepMethod::Execute | StepMethod::Unspecified => {
            options.map_or(0, |options| options.execute_durability_override)
        }
    };
    if override_durability != StepDurability::Unspecified as i32 {
        return override_durability;
    }
    cursor
        .record
        .flow_config
        .as_ref()
        .and_then(|config| config.step_durability)
        .filter(|durability| *durability != StepDurability::Unspecified as i32)
        .unwrap_or(StepDurability::Sync as i32)
}

/// Releases the invocation's heartbeat checkpoint, if any, when the invocation ends for good.
pub(crate) fn release_heartbeat_checkpoint(
    transaction: &mut FlowTransaction<'_>,
    cursor: &RunCursor,
    step_execution_id: &str,
    invocation: &mut StepInvocationRecord,
) -> Result<(), ApplyError> {
    let Some(checkpoint) = invocation.heartbeat_checkpoint.take() else {
        return Ok(());
    };
    let holder = ValueHolder::HeartbeatCheckpoint {
        step_execution_id: step_execution_id.to_owned(),
        method: invocation.method,
    };
    release_stored_value(transaction, cursor.rows, &checkpoint, &holder, cursor.seq)
}

/// Whether the owner may start this pending attempt: every lock key is free and this call is
/// the oldest waiter on each key.
pub(crate) fn can_take_locks(
    transaction: &FlowTransaction<'_>,
    cursor: &RunCursor,
    key: &StepInvocationKey,
    lock_keys: &[String],
) -> Result<bool, ApplyError> {
    for lock_key in lock_keys {
        let lock = cursor.rows.lock(transaction, lock_key)?;
        let is_first_waiter = lock
            .waiters
            .first()
            .and_then(|waiter| waiter.invocation.as_ref())
            .is_some_and(|waiter| waiter == key);
        if lock.holder.is_some() || !is_first_waiter {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Takes the locks of a pending attempt and marks it running. The caller checked
/// [`can_take_locks`] and stores the snapshot.
pub(crate) fn start_attempt(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    step: &mut StepExecutionRecord,
    task_id: &[u8],
    now_micros: i64,
) -> Result<(), ApplyError> {
    let Some(invocation) = step.invocation.as_mut() else {
        return Err(ApplyError::InvariantViolation {
            detail: "start without an invocation".to_owned(),
        });
    };
    let key = StepInvocationKey {
        step_execution_id: step.step_execution_id.clone(),
        method: invocation.method,
    };
    for lock_key in &invocation.lock_keys {
        let mut lock = cursor.rows.lock(transaction, lock_key)?;
        lock.waiters
            .retain(|waiter| waiter.invocation.as_ref() != Some(&key));
        lock.holder = Some(key.clone());
        lock.holder_attempt = invocation.attempt;
        cursor.rows.put_lock(transaction, lock_key, &lock);
    }
    invocation.phase = StepInvocationPhase::Running as i32;
    invocation.task_id = task_id.to_vec();
    invocation.attempt_started_at_micros = now_micros;
    let run_id = cursor.run_id();
    let run_attempt = cursor.attempt();
    let attempt = invocation.attempt;
    let method_timeout_seconds =
        configured_method_timeout_seconds(step.options.as_ref(), invocation.method);
    cursor.add_effect(ApplyEffect::InvocationStarted {
        run_id,
        run_attempt,
        invocation: key,
        attempt,
        task_id: task_id.to_vec(),
        method_timeout_seconds,
    });
    Ok(())
}

/// Returns the configured timeout in seconds of `method`, or 0 when the Step options leave it
/// to the default; [`ApplyEffect::InvocationStarted`] carries this value.
pub(crate) fn configured_method_timeout_seconds(options: Option<&StepOptions>, method: i32) -> i32 {
    options.map_or(0, |options| {
        if method == StepMethod::WaitFor as i32 {
            options.wait_for_timeout_seconds
        } else {
            options.execute_timeout_seconds
        }
    })
}

/// Ends the running or pending attempt of the Step's current method: releases its locks and
/// lock queue entries, unpins its snapshot, and for running attempts that were canceled tells
/// the Worker to stop. Leaves `step.invocation` in place for the caller to update or clear.
pub(crate) fn end_running_attempt(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    step: &mut StepExecutionRecord,
    end: InvocationEnd,
) -> Result<(), ApplyError> {
    let Some(invocation) = step.invocation.as_mut() else {
        return Ok(());
    };
    let key = StepInvocationKey {
        step_execution_id: step.step_execution_id.clone(),
        method: invocation.method,
    };
    let mut released_keys = Vec::new();
    for lock_key in &invocation.lock_keys {
        let mut lock = cursor.rows.lock(transaction, lock_key)?;
        let was_holder = lock.holder.as_ref() == Some(&key);
        if was_holder {
            lock.holder = None;
            lock.holder_attempt = 0;
            released_keys.push(lock_key.clone());
        }
        lock.waiters
            .retain(|waiter| waiter.invocation.as_ref() != Some(&key));
        cursor.rows.put_lock(transaction, lock_key, &lock);
    }
    let was_running = invocation.phase == StepInvocationPhase::Running as i32;
    if let Some(snapshot) = invocation.snapshot.take() {
        let pin = ValueHolder::AttemptSnapshot {
            step_execution_id: step.step_execution_id.clone(),
            method: invocation.method,
            attempt: invocation.attempt,
        };
        for attribute in &snapshot.attributes {
            if let Some(stored) = &attribute.value {
                release_stored_value(transaction, cursor.rows, stored, &pin, cursor.seq)?;
            }
        }
        for channel in &snapshot.loaded_channel_messages {
            for message in &channel.messages {
                if let Some(stored) = &message.value {
                    release_stored_value(transaction, cursor.rows, stored, &pin, cursor.seq)?;
                }
            }
        }
    }
    let run_id = cursor.run_id();
    if was_running && end == InvocationEnd::Canceled {
        cursor.add_effect(ApplyEffect::InvocationCanceled {
            run_id,
            task_id: invocation.task_id.clone(),
        });
    }
    if !released_keys.is_empty() {
        cursor.add_effect(ApplyEffect::LocksReleased {
            run_id,
            lock_keys: released_keys,
        });
    }
    invocation.task_id.clear();
    Ok(())
}
