// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Method calls, locks, and snapshots (plan sections 8.1 and 8.3: L1, L2, L3, S5).

use std::collections::{BTreeMap, BTreeSet};

use dexzd_protocol::dex::ChannelInfo;
use dexzd_protocol::dexzd::v1::{
    LoadedChannelMessagesRecord, MethodSnapshotRecord, StepExecutionPhase, StepExecutionRecord,
    StepInvocationKey, StepInvocationPhase, StepInvocationRecord, StepMethod, StoredKeyValue,
    TimerId, timer_id,
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

/// S5: a just-started attempt's snapshot equals the one its Step options select from committed rows.
pub fn check_fresh_snapshot(
    run: &RunView,
    step_execution_id: &str,
) -> Result<(), InvariantViolation> {
    let step = run.step_executions.get(step_execution_id);
    let Some((step, invocation, snapshot)) = step.and_then(|step| {
        let invocation = step.invocation.as_ref()?;
        Some((step, invocation, invocation.snapshot.as_ref()?))
    }) else {
        return Err(InvariantViolation::new(
            FRESH_SNAPSHOT,
            format!("started Step execution {step_execution_id} has no snapshot"),
        ));
    };
    let expected = expected_snapshot(run, step, invocation.method);
    let differences = [
        ("Attributes", snapshot.attributes != expected.attributes),
        (
            "Channel infos",
            snapshot.channel_infos != expected.channel_infos,
        ),
        (
            "loaded Channel messages",
            snapshot.loaded_channel_messages != expected.loaded_channel_messages,
        ),
        (
            "loaded AttributeMap instances",
            snapshot.loaded_attribute_map_instances != expected.loaded_attribute_map_instances,
        ),
        (
            "loaded Channel names",
            snapshot.loaded_channel_names != expected.loaded_channel_names,
        ),
        (
            "loaded ChannelMap instances",
            snapshot.loaded_channel_map_instances != expected.loaded_channel_map_instances,
        ),
        (
            "condition results",
            snapshot.condition_results != expected.condition_results,
        ),
    ];
    match differences.iter().find(|(_, is_different)| *is_different) {
        Some((part, _)) => Err(InvariantViolation::new(
            FRESH_SNAPSHOT,
            format!(
                "started Step execution {step_execution_id} attempt {} read stale {part}: \
                 snapshot {snapshot:?}, committed state selects {expected:?}",
                invocation.attempt
            ),
        )),
        None => Ok(()),
    }
}

/// The snapshot the Step options of `method` select from the committed rows (plan C17).
fn expected_snapshot(
    run: &RunView,
    step: &StepExecutionRecord,
    method: i32,
) -> MethodSnapshotRecord {
    let options = step.options.clone().unwrap_or_default();
    let is_wait_for = method == StepMethod::WaitFor as i32;
    let (attribute_map_instances, channel_names, channel_map_instances) = if is_wait_for {
        (
            options.wait_for_load_attribute_map_instances,
            options.wait_for_load_channel_names,
            options.wait_for_load_channel_map_instances,
        )
    } else {
        (
            options.execute_load_attribute_map_instances,
            options.execute_load_channel_names,
            options.execute_load_channel_map_instances,
        )
    };
    let attribute_map_instances = sorted_unique(attribute_map_instances);
    let channel_names = sorted_unique(channel_names);
    let channel_map_instances = sorted_unique(channel_map_instances);
    let attributes = run
        .attributes
        .iter()
        .filter(|(key, _)| !key.contains('/') || is_selected(key, &attribute_map_instances))
        .map(|(key, record)| StoredKeyValue {
            key: key.clone(),
            value: record.value.clone(),
        })
        .collect();
    let channel_infos: BTreeMap<String, ChannelInfo> = run
        .channel_metas
        .iter()
        .filter(|(_, meta)| meta.pending_count > 0)
        .map(|(name, meta)| {
            let size = i32::try_from(meta.pending_count).unwrap_or(i32::MAX);
            (name.clone(), ChannelInfo { size })
        })
        .collect();
    // Prefix instances load only Channels with pending messages; exact names load even when empty.
    let mut loaded_names: BTreeSet<String> = channel_names.iter().cloned().collect();
    for instance in &channel_map_instances {
        if instance.ends_with('/') {
            loaded_names.extend(
                channel_infos
                    .keys()
                    .filter(|name| name.starts_with(instance.as_str()))
                    .cloned(),
            );
        } else {
            loaded_names.insert(instance.clone());
        }
    }
    let loaded_channel_messages = loaded_names
        .into_iter()
        .map(|channel_name| LoadedChannelMessagesRecord {
            messages: run
                .channel_messages
                .iter()
                .filter(|message| message.channel_name == channel_name)
                .cloned()
                .collect(),
            channel_name,
        })
        .collect();
    MethodSnapshotRecord {
        attributes,
        channel_infos,
        loaded_channel_messages,
        loaded_attribute_map_instances: attribute_map_instances,
        loaded_channel_names: channel_names,
        loaded_channel_map_instances: channel_map_instances,
        condition_results: if is_wait_for {
            None
        } else {
            step.condition_results.clone()
        },
    }
}

fn sorted_unique(mut names: Vec<String>) -> Vec<String> {
    names.sort();
    names.dedup();
    names
}

/// A map key is selected exactly or by a `map/` prefix.
fn is_selected(key: &str, instances: &[String]) -> bool {
    instances.iter().any(|instance| {
        if instance.ends_with('/') {
            key.starts_with(instance.as_str())
        } else {
            key == instance
        }
    })
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

#[cfg(test)]
mod tests {
    use dexzd_protocol::dex::{StepOptions, Value, value};
    use dexzd_protocol::dexzd::v1::{
        AttributeRecord, ChannelMessageRecord, ChannelMetaRecord, LockRecord, StoredValue,
        stored_value,
    };

    use super::*;

    const STEP: &str = "step-2";

    fn inline(text: &str) -> Option<StoredValue> {
        Some(StoredValue {
            kind: Some(stored_value::Kind::InlineValue(Value {
                kind: Some(value::Kind::StringValue(text.to_owned())),
            })),
        })
    }

    fn message(enqueued_seq: u64) -> ChannelMessageRecord {
        ChannelMessageRecord {
            channel_name: "inbox".to_owned(),
            message_id: format!("message-{enqueued_seq}"),
            value: inline("hello"),
            enqueued_seq,
        }
    }

    /// `step-2` took lock `status` from a finished holder after two inbox publications.
    fn run_after_lock_handoff() -> RunView {
        let key = StepInvocationKey {
            step_execution_id: STEP.to_owned(),
            method: StepMethod::Execute as i32,
        };
        let snapshot = MethodSnapshotRecord {
            attributes: vec![StoredKeyValue {
                key: "status".to_owned(),
                value: inline("paid"),
            }],
            channel_infos: [("inbox".to_owned(), ChannelInfo { size: 2 })].into(),
            loaded_channel_messages: vec![LoadedChannelMessagesRecord {
                channel_name: "inbox".to_owned(),
                messages: vec![message(10), message(20)],
            }],
            loaded_channel_names: vec!["inbox".to_owned()],
            ..MethodSnapshotRecord::default()
        };
        let step = StepExecutionRecord {
            step_execution_id: STEP.to_owned(),
            options: Some(StepOptions {
                execute_lock_attribute_keys: vec!["status".to_owned()],
                execute_load_channel_names: vec!["inbox".to_owned()],
                ..StepOptions::default()
            }),
            phase: StepExecutionPhase::Execute as i32,
            invocation: Some(StepInvocationRecord {
                method: StepMethod::Execute as i32,
                phase: StepInvocationPhase::Running as i32,
                attempt: 1,
                lock_keys: vec!["status".to_owned()],
                snapshot: Some(snapshot),
                ..StepInvocationRecord::default()
            }),
            ..StepExecutionRecord::default()
        };
        RunView {
            is_complete: true,
            attributes: [(
                "status".to_owned(),
                AttributeRecord {
                    value: inline("paid"),
                    ..AttributeRecord::default()
                },
            )]
            .into(),
            channel_messages: vec![message(10), message(20)],
            channel_metas: [(
                "inbox".to_owned(),
                ChannelMetaRecord {
                    pending_count: 2,
                    claimed_count: 0,
                },
            )]
            .into(),
            step_executions: [(STEP.to_owned(), step)].into(),
            locks: [(
                "status".to_owned(),
                LockRecord {
                    holder: Some(key),
                    holder_attempt: 1,
                    waiters: Vec::new(),
                },
            )]
            .into(),
            ..RunView::default()
        }
    }

    fn snapshot_mut(run: &mut RunView) -> &mut MethodSnapshotRecord {
        run.step_executions
            .get_mut(STEP)
            .and_then(|step| step.invocation.as_mut())
            .and_then(|invocation| invocation.snapshot.as_mut())
            .expect("the fixture's Step is running with a snapshot")
    }

    fn assert_stale(run: &RunView) {
        let violation =
            check_fresh_snapshot(run, STEP).expect_err("the snapshot predates the committed rows");
        assert_eq!(violation.invariant, FRESH_SNAPSHOT, "{violation}");
    }

    #[test]
    fn a_fresh_snapshot_after_a_lock_handoff_passes() {
        check_fresh_snapshot(&run_after_lock_handoff(), STEP).expect("the snapshot is fresh");
    }

    #[test]
    fn a_stale_channel_count_is_rejected() {
        let mut run = run_after_lock_handoff();
        snapshot_mut(&mut run).channel_infos =
            [("inbox".to_owned(), ChannelInfo { size: 1 })].into();
        assert_stale(&run);
    }

    #[test]
    fn an_omitted_requested_channel_is_rejected() {
        let mut run = run_after_lock_handoff();
        snapshot_mut(&mut run).loaded_channel_messages.clear();
        assert_stale(&run);
    }

    #[test]
    fn an_unselected_attribute_map_instance_is_rejected() {
        let mut run = run_after_lock_handoff();
        run.attributes.insert(
            "orders/1".to_owned(),
            AttributeRecord {
                value: inline("open"),
                ..AttributeRecord::default()
            },
        );
        snapshot_mut(&mut run).attributes.push(StoredKeyValue {
            key: "orders/1".to_owned(),
            value: inline("open"),
        });
        assert_stale(&run);
    }
}
