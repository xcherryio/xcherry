// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! The canonical committed log from every process's applied frames, checked against a shadow.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use dexzd_apply::{ApplyOutcome, StateWrite};
use dexzd_engine::{
    CommittedCommandSink, CommittedFlowCommand, EngineLocalStore, FlowRuntimeRegistry,
};
use dexzd_keyspace::{PartitionId, RunId};
use dexzd_protocol::dex::{ChannelMessage, Kv};
use dexzd_protocol::dexzd::v1::{FlowCommand, LogPosition};
use dexzd_storage::LocalReadView;
use prost::Message;

use crate::apply_checker::ApplyChecker;
use crate::apply_simulation::command_kind;
use crate::engine_simulation::event_trace::EventTrace;
use crate::engine_simulation::simulated_clients::ENGINE_FLOW_IDS;
use crate::engine_simulation::simulated_object_store::StorageObservations;
use crate::engine_simulation::workload::operation_marker;
use crate::invariants::InvariantViolation;
use crate::memory_state::MemoryFlowState;
use crate::model::wire_value;
use crate::row_decoding::hex;
use crate::state_view::RunSelection;

/// Orders frames of one partition: node, incarnation, then seq.
type FrameKey = (u16, String, u32, u64);

struct CanonicalCommand {
    command: FlowCommand,
    outcome: ApplyOutcome,
}

struct CanonicalFrame {
    /// Count of new frames the shadow accepted up to and including this one.
    generation: u64,
    commands: Vec<CanonicalCommand>,
}

struct OpenFrame {
    position: LogPosition,
    commands: Vec<CommittedFlowCommand>,
}

/// What a client read of one Flow may return: its newest run's Attributes and Channel messages.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct FlowReadState {
    /// Encoded wire value of each Attribute; `None` while the Flow has no run.
    attributes: Option<BTreeMap<String, Vec<u8>>>,
    /// Encoded messages of each Channel, oldest first.
    channel_messages: BTreeMap<String, Vec<Vec<u8>>>,
}

/// The generations a read may observe: what its node had applied when the read began and ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReadWindow {
    pub(crate) floor: u64,
    pub(crate) ceiling: u64,
}

struct CommittedLogState {
    shadow: ApplyChecker,
    frames: BTreeMap<FrameKey, CanonicalFrame>,
    /// New frames accepted so far; each frame's generation.
    generation: u64,
    /// Read states of client Flows by the generation that produced them.
    read_states: BTreeMap<String, BTreeMap<u64, FlowReadState>>,
    newest_frames: BTreeMap<u16, FrameKey>,
    open_frames: BTreeMap<(String, u16), OpenFrame>,
    first_violation: Option<InvariantViolation>,
    committed_markers: BTreeSet<String>,
    applied_by_kind: BTreeMap<&'static str, u64>,
    /// Commands committed without writes, such as a second result of one attempt.
    rejected_by_kind: BTreeMap<&'static str, u64>,
    replayed_frames: u64,
}

/// Collects applied frames from every node process and checks them against the shadow.
pub(crate) struct CommittedLog {
    state: Mutex<CommittedLogState>,
    trace: Arc<EventTrace>,
    /// Counts frames of old-epoch processes once their successor holds the lease.
    storage_observations: Arc<Mutex<StorageObservations>>,
}

impl CommittedLog {
    /// The shadow places Flows with [`ApplyChecker::new`]'s partitioning, which the cluster copies.
    pub(crate) fn new(
        trace: Arc<EventTrace>,
        storage_observations: Arc<Mutex<StorageObservations>>,
    ) -> Self {
        Self {
            state: Mutex::new(CommittedLogState {
                shadow: ApplyChecker::new(),
                frames: BTreeMap::new(),
                generation: 0,
                read_states: BTreeMap::new(),
                newest_frames: BTreeMap::new(),
                open_frames: BTreeMap::new(),
                first_violation: None,
                committed_markers: BTreeSet::new(),
                applied_by_kind: BTreeMap::new(),
                rejected_by_kind: BTreeMap::new(),
                replayed_frames: 0,
            }),
            trace,
            storage_observations,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, CommittedLogState> {
        self.state
            .lock()
            .expect("committed log lock is not poisoned")
    }

    /// Returns the first violation any node's frames produced.
    pub(crate) fn first_violation(&self) -> Option<InvariantViolation> {
        self.lock().first_violation.clone()
    }

    pub(crate) fn has_committed_marker(&self, marker: &str) -> bool {
        self.lock().committed_markers.contains(marker)
    }

    /// Returns the generation of the newest canonical frame of `flow_id`'s partition in `store`.
    ///
    /// Returns zero before the store applied any frame of the partition, or the read error.
    pub(crate) fn applied_generation(
        &self,
        store: &EngineLocalStore,
        flow_id: &str,
    ) -> Result<u64, String> {
        let state = self.lock();
        let partition = state.shadow.context(flow_id).partition;
        let meta = store
            .partition_meta(partition)
            .map_err(|error| format!("reading partition {} metadata: {error}", partition.0))?;
        let Some(applied) = meta.and_then(|meta| meta.applied) else {
            return Ok(0);
        };
        let lowest: FrameKey = (partition.0, String::new(), 0, 0);
        let applied_key: FrameKey = (
            partition.0,
            applied.node_id,
            applied.incarnation,
            applied.seq,
        );
        Ok(state
            .frames
            .range(lowest..=applied_key)
            .next_back()
            .map_or(0, |(_, frame)| frame.generation))
    }

    /// Checks that a GetAttributes response equals the newest run's Attributes at a generation
    /// in `window`.
    ///
    /// Returns the response and the states it could have matched when it matches none.
    pub(crate) fn check_attribute_read(
        &self,
        flow_id: &str,
        window: ReadWindow,
        attributes: &[Kv],
    ) -> Result<(), String> {
        let observed: BTreeMap<String, Vec<u8>> = attributes
            .iter()
            .map(|attribute| {
                let value = attribute.value.clone().unwrap_or_default();
                (attribute.key.clone(), value.encode_to_vec())
            })
            .collect();
        self.check_read_against_history(flow_id, window, |state| {
            state.attributes.as_ref() == Some(&observed)
        })
    }

    /// Checks that a GetChannelMessages response equals the Channel's messages at a generation
    /// in `window`.
    ///
    /// Returns a description of the mismatch when the response matches no state in the window.
    pub(crate) fn check_channel_read(
        &self,
        flow_id: &str,
        channel_name: &str,
        window: ReadWindow,
        messages: &[ChannelMessage],
    ) -> Result<(), String> {
        let observed: Vec<Vec<u8>> = messages.iter().map(Message::encode_to_vec).collect();
        self.check_read_against_history(flow_id, window, |state| {
            state.attributes.is_some()
                && state
                    .channel_messages
                    .get(channel_name)
                    .map_or(observed.is_empty(), |expected| *expected == observed)
        })
    }

    fn check_read_against_history(
        &self,
        flow_id: &str,
        window: ReadWindow,
        matches: impl Fn(&FlowReadState) -> bool,
    ) -> Result<(), String> {
        let state = self.lock();
        let empty = BTreeMap::new();
        let history = state.read_states.get(flow_id).unwrap_or(&empty);
        if is_state_in_window(history, window, matches) {
            return Ok(());
        }
        Err(format!(
            "no committed state of generations {}..={} matches it; the newest generation is {}",
            window.floor, window.ceiling, state.generation
        ))
    }

    /// Returns a copy of the shadow state, which never crashed.
    pub(crate) fn shadow_state(&self) -> MemoryFlowState {
        self.lock().shadow.state().clone()
    }

    /// Returns the shadow checker for read-only decoding.
    pub(crate) fn with_shadow<T>(&self, read: impl FnOnce(&ApplyChecker) -> T) -> T {
        read(&self.lock().shadow)
    }

    pub(crate) fn applied_by_kind(&self) -> BTreeMap<&'static str, u64> {
        self.lock().applied_by_kind.clone()
    }

    pub(crate) fn replayed_frames(&self) -> u64 {
        self.lock().replayed_frames
    }

    pub(crate) fn rejected_by_kind(&self) -> BTreeMap<&'static str, u64> {
        self.lock().rejected_by_kind.clone()
    }

    /// Returns a sink that records `process_label`'s frames and then passes them to `registry`.
    pub(crate) fn sink_for(
        self: &Arc<Self>,
        process_label: &str,
        registry: FlowRuntimeRegistry,
    ) -> Arc<ObservingCommandSink> {
        Arc::new(ObservingCommandSink {
            log: Arc::clone(self),
            process_label: process_label.to_owned(),
            registry,
        })
    }

    fn observe(&self, process_label: &str, committed: &CommittedFlowCommand) {
        let mut state = self.lock();
        let slot = (process_label.to_owned(), committed.context.partition.0);
        let open = state.open_frames.entry(slot).or_insert_with(|| OpenFrame {
            position: committed.committed.position.clone(),
            commands: Vec::new(),
        });
        if open.position != committed.committed.position {
            let violation = InvariantViolation::new(
                "frame-boundary",
                format!(
                    "{process_label} applied a command at {:?} before finishing frame {:?}",
                    committed.committed.position, open.position
                ),
            );
            state.record_violation(violation, &self.trace);
            return;
        }
        open.commands.push(committed.clone());
    }

    fn finish_frame(&self, process_label: &str, partition: PartitionId, position: &LogPosition) {
        let mut state = self.lock();
        let Some(frame) = state
            .open_frames
            .remove(&(process_label.to_owned(), partition.0))
        else {
            return;
        };
        if frame.position != *position {
            let violation = InvariantViolation::new(
                "frame-boundary",
                format!(
                    "{process_label} finished frame {position:?} of partition {} while frame {:?} \
                     was open",
                    partition.0, frame.position
                ),
            );
            state.record_violation(violation, &self.trace);
            return;
        }
        let key = (
            partition.0,
            position.node_id.clone(),
            position.incarnation,
            position.seq,
        );
        self.storage_observations
            .lock()
            .expect("storage observation lock is not poisoned")
            .old_epoch
            .record_frame(process_label);
        self.trace.record(&format!(
            "{process_label} applied frame p{} {}:{} with {} commands",
            partition.0,
            position.incarnation,
            position.seq,
            frame.commands.len()
        ));
        let result = if state.frames.contains_key(&key) {
            state.replayed_frames += 1;
            state.compare_with_canonical(&key, &frame.commands)
        } else {
            state.accept_new_frame(key, frame.commands)
        };
        if let Err(violation) = result {
            state.record_violation(
                InvariantViolation::new(
                    violation.invariant,
                    format!("{process_label}: {}", violation.detail),
                ),
                &self.trace,
            );
        }
    }
}

impl CommittedLogState {
    fn record_violation(&mut self, violation: InvariantViolation, trace: &EventTrace) {
        trace.record(&format!("violation {violation}"));
        if self.first_violation.is_none() {
            self.first_violation = Some(violation);
        }
    }

    /// A replayed or concurrently applied frame must match the canonical one exactly.
    fn compare_with_canonical(
        &self,
        key: &FrameKey,
        commands: &[CommittedFlowCommand],
    ) -> Result<(), InvariantViolation> {
        let canonical = &self
            .frames
            .get(key)
            .expect("the caller checked that the frame exists")
            .commands;
        let same_commands = canonical.len() == commands.len()
            && canonical
                .iter()
                .zip(commands)
                .all(|(expected, actual)| expected.command == actual.command);
        if !same_commands {
            return Err(InvariantViolation::new(
                "log-position-conflict",
                format!(
                    "frame {key:?} holds {} commands now and {} when first applied",
                    commands.len(),
                    canonical.len()
                ),
            ));
        }
        for (expected, actual) in canonical.iter().zip(commands) {
            if expected.outcome != actual.committed.outcome {
                return Err(InvariantViolation::new(
                    "recovered-state-diverged",
                    format!(
                        "replaying {} of Flow {} at frame {key:?}: {}",
                        command_kind(&actual.command),
                        actual.command.flow_id,
                        describe_outcome_difference(&expected.outcome, &actual.committed.outcome)
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Applies a new frame to the shadow, which must reproduce each engine outcome exactly.
    fn accept_new_frame(
        &mut self,
        key: FrameKey,
        commands: Vec<CommittedFlowCommand>,
    ) -> Result<(), InvariantViolation> {
        if let Some(newest) = self.newest_frames.get(&key.0)
            && *newest > key
        {
            return Err(InvariantViolation::new(
                "log-order",
                format!("frame {key:?} was applied for the first time after frame {newest:?}"),
            ));
        }
        let mut canonical = Vec::with_capacity(commands.len());
        for committed in commands {
            let expected_context = self.shadow.context(&committed.command.flow_id);
            if expected_context != committed.context {
                return Err(InvariantViolation::new(
                    "apply-context",
                    format!(
                        "Flow {} applied with {:?}, expected {expected_context:?}",
                        committed.command.flow_id, committed.context
                    ),
                ));
            }
            let shadow_outcome = self.shadow.apply_and_check(&committed.command)?;
            if shadow_outcome != committed.committed.outcome {
                return Err(InvariantViolation::new(
                    "engine-outcome-differs-from-shadow",
                    format!(
                        "{} of Flow {} at frame {key:?}: {}",
                        command_kind(&committed.command),
                        committed.command.flow_id,
                        describe_outcome_difference(&shadow_outcome, &committed.committed.outcome)
                    ),
                ));
            }
            self.remember_committed(&committed.command, &shadow_outcome);
            canonical.push(CanonicalCommand {
                command: committed.command,
                outcome: shadow_outcome,
            });
        }
        self.generation += 1;
        let touched_flows: BTreeSet<String> = canonical
            .iter()
            .map(|command| command.command.flow_id.clone())
            .filter(|flow_id| ENGINE_FLOW_IDS.contains(&flow_id.as_str()))
            .collect();
        for flow_id in touched_flows {
            let read_state = read_state_of(&self.shadow, &flow_id)?;
            let history = self.read_states.entry(flow_id).or_default();
            if history.values().next_back() != Some(&read_state) {
                history.insert(self.generation, read_state);
            }
        }
        self.newest_frames.insert(key.0, key.clone());
        self.frames.insert(
            key,
            CanonicalFrame {
                generation: self.generation,
                commands: canonical,
            },
        );
        Ok(())
    }

    fn remember_committed(&mut self, command: &FlowCommand, outcome: &ApplyOutcome) {
        let counts = if outcome.writes.is_empty() {
            &mut self.rejected_by_kind
        } else {
            &mut self.applied_by_kind
        };
        *counts.entry(command_kind(command)).or_default() += 1;
        if let Some(marker) = operation_marker(command) {
            self.committed_markers.insert(marker.to_owned());
        }
    }
}

/// Whether some state in effect during `window` satisfies `matches`.
fn is_state_in_window<T>(
    history: &BTreeMap<u64, T>,
    window: ReadWindow,
    matches: impl Fn(&T) -> bool,
) -> bool {
    let at_floor = history.range(..=window.floor).next_back();
    let during = (window.ceiling > window.floor)
        .then(|| history.range(window.floor + 1..=window.ceiling))
        .into_iter()
        .flatten();
    at_floor
        .into_iter()
        .chain(during)
        .any(|(_, state)| matches(state))
}

/// Returns what GetAttributes and GetChannelMessages of `flow_id` return in the shadow state.
fn read_state_of(
    shadow: &ApplyChecker,
    flow_id: &str,
) -> Result<FlowReadState, InvariantViolation> {
    let directory = shadow.view(flow_id, RunSelection::Only(&BTreeSet::new()))?;
    let newest = directory
        .directory
        .as_ref()
        .and_then(|directory| directory.runs.last())
        .and_then(|run| RunId::from_slice(&run.run_id).ok());
    let Some(run_id) = newest else {
        return Ok(FlowReadState::default());
    };
    let view = shadow.view(flow_id, RunSelection::Only(&BTreeSet::from([run_id])))?;
    let Some(run) = view.runs.get(&run_id).filter(|run| run.record.is_some()) else {
        return Ok(FlowReadState::default());
    };
    let attributes = run
        .attributes
        .iter()
        .filter_map(|(key, record)| {
            let value = wire_value(run_id, record.value.as_ref()?)?;
            Some((key.clone(), value.encode_to_vec()))
        })
        .collect();
    let mut channel_messages: BTreeMap<String, Vec<Vec<u8>>> = BTreeMap::new();
    for record in &run.channel_messages {
        let message = ChannelMessage {
            channel_name: record.channel_name.clone(),
            value: record
                .value
                .as_ref()
                .and_then(|stored| wire_value(run_id, stored)),
            message_id: record.message_id.clone(),
        };
        channel_messages
            .entry(record.channel_name.clone())
            .or_default()
            .push(message.encode_to_vec());
    }
    Ok(FlowReadState {
        attributes: Some(attributes),
        channel_messages,
    })
}

/// Names the first difference between two outcomes of one command.
fn describe_outcome_difference(expected: &ApplyOutcome, actual: &ApplyOutcome) -> String {
    let first_write_difference = expected
        .writes
        .iter()
        .zip(&actual.writes)
        .position(|(left, right)| left != right);
    match first_write_difference {
        Some(index) => format!(
            "write {index} differs: {} vs {}",
            describe_write(&expected.writes[index]),
            describe_write(&actual.writes[index])
        ),
        None if expected.writes.len() != actual.writes.len() => format!(
            "{} writes vs {} writes",
            expected.writes.len(),
            actual.writes.len()
        ),
        None => format!(
            "writes match but effects or replies differ: {:?} / {:?} vs {:?} / {:?}",
            expected.effects, expected.reply, actual.effects, actual.reply
        ),
    }
}

fn describe_write(write: &StateWrite) -> String {
    match write {
        StateWrite::Put {
            column_family,
            key,
            value,
        } => format!(
            "put {column_family:?} {} ({} bytes, xxh3 {:016x})",
            hex(key),
            value.len(),
            xxhash_rust::xxh3::xxh3_64(value)
        ),
        StateWrite::Delete { column_family, key } => {
            format!("delete {column_family:?} {}", hex(key))
        }
        StateWrite::DeleteRange {
            column_family,
            start,
            end,
        } => format!(
            "delete range {column_family:?} {}..{}",
            hex(start),
            hex(end)
        ),
    }
}

/// Records each committed command for the canonical log, then hands it to the Flow runtime.
pub(crate) struct ObservingCommandSink {
    log: Arc<CommittedLog>,
    process_label: String,
    registry: FlowRuntimeRegistry,
}

impl CommittedCommandSink for ObservingCommandSink {
    fn apply_committed(&self, committed: CommittedFlowCommand) {
        self.log.observe(&self.process_label, &committed);
        self.registry.apply_committed(committed);
    }

    fn finish_frame(&self, partition: PartitionId, position: &LogPosition) {
        self.log
            .finish_frame(&self.process_label, partition, position);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{ReadWindow, is_state_in_window};

    fn window(floor: u64, ceiling: u64) -> ReadWindow {
        ReadWindow { floor, ceiling }
    }

    #[test]
    fn a_read_after_an_overwrite_must_not_return_the_overwritten_value() {
        let history = BTreeMap::from([(1, "A"), (2, "B")]);
        let reads = |observed: &str, read_window| {
            is_state_in_window(&history, read_window, |state| *state == observed)
        };
        assert!(reads("A", window(1, 1)));
        assert!(reads("A", window(1, 2)));
        assert!(reads("B", window(1, 2)));
        assert!(reads("B", window(5, 7)));
        assert!(!reads("A", window(2, 2)));
        assert!(!reads("A", window(2, 9)));
        assert!(!reads("B", window(0, 1)));
    }

    #[test]
    fn a_read_before_the_first_commit_matches_no_state() {
        let history = BTreeMap::from([(3, "A")]);
        assert!(!is_state_in_window(&history, window(0, 2), |state| *state == "A"));
        assert!(is_state_in_window(&history, window(0, 3), |state| *state == "A"));
    }
}
