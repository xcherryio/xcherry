// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Method calls, locks, and snapshots (plan sections 8.1 and 8.3: L1, L2, L3, S5).

use dexzd_protocol::dexzd::v1::{
    StepExecutionPhase, StepExecutionRecord, StepInvocationKey, StepInvocationPhase,
    StepInvocationRecord, StepMethod, StoredValue, TimerId, timer_id,
};
use prost::Message;

use crate::invariants::{GUARDED_ATTRIBUTE_KEYS, InvariantViolation, RunContext};
use crate::state_view::RunView;

const INVOCATION: &str = "invocation-shape";
const HOLDER: &str = "L3-lock-holder";
const WAITER: &str = "L4-lock-waiter";
const LOST_UPDATE: &str = "L2-guarded-read-modify-write";
const FRESH_SNAPSHOT: &str = "S5-fresh-snapshot";

pub(crate) fn check_invocations(
    context: &RunContext<'_>,
    run: &RunView,
) -> Result<(), InvariantViolation> {
    let current_attempt = run
        .record
        .as_ref()
        .map_or(0, |record| record.current_attempt);
    for step in run.step_executions.values() {
        let id = &step.step_execution_id;
        let expected_method = match StepExecutionPhase::try_from(step.phase) {
            Ok(StepExecutionPhase::WaitFor) => Some(StepMethod::WaitFor),
            Ok(StepExecutionPhase::Execute) => Some(StepMethod::Execute),
            _ => None,
        };
        let actual_method = step.invocation.as_ref().map(|invocation| invocation.method);
        if expected_method.map(|method| method as i32) != actual_method {
            return Err(InvariantViolation::new(
                INVOCATION,
                format!(
                    "{context} Step execution {id} in phase {} has invocation method {actual_method:?}",
                    step.phase
                ),
            ));
        }
        let Some(invocation) = &step.invocation else {
            continue;
        };
        let is_running = invocation.phase == StepInvocationPhase::Running as i32;
        if is_running != invocation.snapshot.is_some() {
            return Err(InvariantViolation::new(
                INVOCATION,
                format!(
                    "{context} Step execution {id} invocation in phase {} has snapshot: {}",
                    invocation.phase,
                    invocation.snapshot.is_some()
                ),
            ));
        }
        if invocation.phase == StepInvocationPhase::Backoff as i32 {
            let timer = TimerId {
                run_attempt: current_attempt,
                kind: Some(timer_id::Kind::RetryBackoff(
                    dexzd_protocol::dexzd::v1::RetryBackoffTimerId {
                        invocation: Some(invocation_key(step, invocation)),
                        next_attempt: invocation.attempt,
                    },
                )),
            }
            .encode_to_vec();
            if !run.timers.iter().any(|(encoded, _)| *encoded == timer) {
                return Err(InvariantViolation::new(
                    INVOCATION,
                    format!(
                        "{context} Step execution {id} is in backoff for attempt {} without a \
                         retry timer",
                        invocation.attempt
                    ),
                ));
            }
        }
        for lock_key in &invocation.lock_keys {
            let lock = run.locks.get(lock_key);
            let key = invocation_key(step, invocation);
            let holds = lock.is_some_and(|lock| {
                lock.holder.as_ref() == Some(&key) && lock.holder_attempt == invocation.attempt
            });
            let waits = lock.is_some_and(|lock| {
                lock.waiters.iter().any(|waiter| {
                    waiter.invocation.as_ref() == Some(&key)
                        && waiter.pending_since_seq == invocation.pending_since_seq
                })
            });
            let phase = StepInvocationPhase::try_from(invocation.phase);
            let is_consistent = match phase {
                Ok(StepInvocationPhase::Running) => holds && !waits,
                Ok(StepInvocationPhase::Pending) => waits && !holds,
                _ => !holds && !waits,
            };
            if !is_consistent {
                return Err(InvariantViolation::new(
                    HOLDER,
                    format!(
                        "{context} Step execution {id} invocation in phase {:?} attempt {}: \
                         holds lock {lock_key}: {holds}, waits: {waits} (lock row {lock:?})",
                        phase, invocation.attempt
                    ),
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn check_locks(
    context: &RunContext<'_>,
    run: &RunView,
) -> Result<(), InvariantViolation> {
    for (lock_key, lock) in &run.locks {
        if lock.holder.is_none() && lock.waiters.is_empty() {
            return Err(InvariantViolation::new(
                HOLDER,
                format!("{context} lock {lock_key} has an unused row"),
            ));
        }
        if let Some(holder) = &lock.holder {
            let invocation = current_invocation(run, holder);
            let is_running_holder = invocation.is_some_and(|invocation| {
                invocation.phase == StepInvocationPhase::Running as i32
                    && invocation.attempt == lock.holder_attempt
                    && invocation.lock_keys.contains(lock_key)
            });
            if !is_running_holder {
                return Err(InvariantViolation::new(
                    HOLDER,
                    format!(
                        "{context} lock {lock_key} is held by {holder:?} attempt {}, which is not \
                         a running attempt that declared it (invocation {:?})",
                        lock.holder_attempt,
                        invocation.map(|invocation| (invocation.phase, invocation.attempt))
                    ),
                ));
            }
        }
        let mut previous_seq = 0;
        for (index, waiter) in lock.waiters.iter().enumerate() {
            let Some(key) = &waiter.invocation else {
                return Err(InvariantViolation::new(
                    WAITER,
                    format!("{context} lock {lock_key} has a waiter without an invocation"),
                ));
            };
            let is_duplicate = lock.waiters[..index]
                .iter()
                .any(|earlier| earlier.invocation.as_ref() == Some(key));
            let is_pending = current_invocation(run, key).is_some_and(|invocation| {
                invocation.phase == StepInvocationPhase::Pending as i32
                    && invocation.pending_since_seq == waiter.pending_since_seq
            });
            if is_duplicate || !is_pending || waiter.pending_since_seq < previous_seq {
                return Err(InvariantViolation::new(
                    WAITER,
                    format!(
                        "{context} lock {lock_key} waiter {key:?} since seq {} is duplicate: \
                         {is_duplicate}, pending: {is_pending}, out of FIFO order: {}",
                        waiter.pending_since_seq,
                        waiter.pending_since_seq < previous_seq
                    ),
                ));
            }
            previous_seq = waiter.pending_since_seq;
        }
    }
    Ok(())
}

/// L2: a running holder of a guarded key read its current value; only holders write it.
pub(crate) fn check_guarded_attributes(
    context: &RunContext<'_>,
    run: &RunView,
) -> Result<(), InvariantViolation> {
    for step in run.step_executions.values() {
        let Some(invocation) = &step.invocation else {
            continue;
        };
        let Some(snapshot) = &invocation.snapshot else {
            continue;
        };
        if invocation.phase != StepInvocationPhase::Running as i32 {
            continue;
        }
        for guarded_key in GUARDED_ATTRIBUTE_KEYS {
            if !invocation.lock_keys.iter().any(|key| key == guarded_key) {
                continue;
            }
            let read = snapshot
                .attributes
                .iter()
                .find(|attribute| attribute.key == *guarded_key)
                .and_then(|attribute| attribute.value.as_ref());
            let current = run
                .attributes
                .get(*guarded_key)
                .and_then(|record| record.value.as_ref());
            if read != current {
                return Err(InvariantViolation::new(
                    LOST_UPDATE,
                    format!(
                        "{context} Step execution {} attempt {} holds {guarded_key} but read \
                         {read:?} while the current value is {current:?}",
                        step.step_execution_id, invocation.attempt
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// S5: a just-started attempt's snapshot equals the committed Attributes and loaded Channel messages.
pub fn check_fresh_snapshot(
    run: &RunView,
    step_execution_id: &str,
) -> Result<(), InvariantViolation> {
    let step = run.step_executions.get(step_execution_id);
    let Some(snapshot) = step
        .and_then(|step| step.invocation.as_ref())
        .and_then(|invocation| invocation.snapshot.as_ref())
    else {
        return Err(InvariantViolation::new(
            FRESH_SNAPSHOT,
            format!("started Step execution {step_execution_id} has no snapshot"),
        ));
    };
    for attribute in &snapshot.attributes {
        let current = run
            .attributes
            .get(&attribute.key)
            .and_then(|record| record.value.as_ref());
        if attribute.value.as_ref() != current {
            return Err(stale(
                step_execution_id,
                &attribute.key,
                attribute.value.as_ref(),
                current,
            ));
        }
    }
    for (key, record) in &run.attributes {
        let is_plain = !key.contains('/');
        let is_read = snapshot
            .attributes
            .iter()
            .any(|attribute| attribute.key == *key);
        if is_plain && !is_read {
            return Err(stale(step_execution_id, key, None, record.value.as_ref()));
        }
    }
    for loaded in &snapshot.loaded_channel_messages {
        let current: Vec<_> = run
            .channel_messages
            .iter()
            .filter(|message| message.channel_name == loaded.channel_name)
            .collect();
        let is_same = current.len() == loaded.messages.len()
            && current
                .iter()
                .zip(&loaded.messages)
                .all(|(current, read)| *current == read);
        if !is_same {
            return Err(InvariantViolation::new(
                FRESH_SNAPSHOT,
                format!(
                    "started Step execution {step_execution_id} read {} messages of Channel {} \
                     but {} are pending",
                    loaded.messages.len(),
                    loaded.channel_name,
                    current.len()
                ),
            ));
        }
    }
    Ok(())
}

fn stale(
    step_execution_id: &str,
    key: &str,
    read: Option<&StoredValue>,
    current: Option<&StoredValue>,
) -> InvariantViolation {
    InvariantViolation::new(
        FRESH_SNAPSHOT,
        format!(
            "started Step execution {step_execution_id} read Attribute {key} as {read:?} but the \
             committed value is {current:?}"
        ),
    )
}

fn invocation_key(
    step: &StepExecutionRecord,
    invocation: &StepInvocationRecord,
) -> StepInvocationKey {
    StepInvocationKey {
        step_execution_id: step.step_execution_id.clone(),
        method: invocation.method,
    }
}

fn current_invocation<'run>(
    run: &'run RunView,
    key: &StepInvocationKey,
) -> Option<&'run StepInvocationRecord> {
    run.step_executions
        .get(&key.step_execution_id)
        .and_then(|step| step.invocation.as_ref())
        .filter(|invocation| invocation.method == key.method)
}
