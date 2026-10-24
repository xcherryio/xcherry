// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! The canonical committed log, assembled from every node's applied frames, and its shadow state.
//!
//! The first frame applied at a log position is canonical. Every later copy must match it.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use dexzd_apply::{ApplyOutcome, StateWrite};
use dexzd_engine::{CommittedCommandSink, CommittedFlowCommand, FlowRuntimeRegistry};
use dexzd_keyspace::{ColumnFamily, PartitionId, RunTable, parse_run_key};
use dexzd_protocol::dexzd::v1::{
    AttributeRecord, ChannelMessageRecord, FlowCommand, LogPosition, stored_value,
};
use prost::Message;

use crate::apply_checker::ApplyChecker;
use crate::apply_simulation::command_kind;
use crate::engine_simulation::event_trace::EventTrace;
use crate::engine_simulation::workload::operation_marker;
use crate::invariants::InvariantViolation;
use crate::memory_state::MemoryFlowState;
use crate::model::wire_value;

/// Orders frames of one partition: node, incarnation, then seq.
type FrameKey = (u16, String, u32, u64);

struct CanonicalCommand {
    command: FlowCommand,
    outcome: ApplyOutcome,
}

struct OpenFrame {
    position: LogPosition,
    commands: Vec<CommittedFlowCommand>,
}

struct CommittedLogState {
    shadow: ApplyChecker,
    frames: BTreeMap<FrameKey, Vec<CanonicalCommand>>,
    newest_frames: BTreeMap<u16, FrameKey>,
    open_frames: BTreeMap<(String, u16), OpenFrame>,
    first_violation: Option<InvariantViolation>,
    committed_markers: BTreeSet<String>,
    /// Encoded wire values each Attribute of a Flow ever held in committed state.
    attribute_values: BTreeMap<(String, String), BTreeSet<Vec<u8>>>,
    /// Message IDs ever committed to a Flow's Channels.
    channel_message_ids: BTreeSet<(String, String)>,
    applied_by_kind: BTreeMap<&'static str, u64>,
    replayed_frames: u64,
}

/// Collects applied frames from every node process and checks them against the shadow.
pub(crate) struct CommittedLog {
    state: Mutex<CommittedLogState>,
    trace: Arc<EventTrace>,
}

impl CommittedLog {
    /// The shadow places Flows with [`ApplyChecker::new`]'s partitioning, which the cluster copies.
    pub(crate) fn new(trace: Arc<EventTrace>) -> Self {
        Self {
            state: Mutex::new(CommittedLogState {
                shadow: ApplyChecker::new(),
                frames: BTreeMap::new(),
                newest_frames: BTreeMap::new(),
                open_frames: BTreeMap::new(),
                first_violation: None,
                committed_markers: BTreeSet::new(),
                attribute_values: BTreeMap::new(),
                channel_message_ids: BTreeSet::new(),
                applied_by_kind: BTreeMap::new(),
                replayed_frames: 0,
            }),
            trace,
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

    /// Whether `value` (encoded) was ever committed for this Attribute of this Flow.
    pub(crate) fn was_attribute_committed(&self, flow_id: &str, key: &str, value: &[u8]) -> bool {
        self.lock()
            .attribute_values
            .get(&(flow_id.to_owned(), key.to_owned()))
            .is_some_and(|values| values.contains(value))
    }

    pub(crate) fn was_message_committed(&self, flow_id: &str, message_id: &str) -> bool {
        self.lock()
            .channel_message_ids
            .contains(&(flow_id.to_owned(), message_id.to_owned()))
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
        let canonical = self
            .frames
            .get(key)
            .expect("the caller checked that the frame exists");
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
        self.newest_frames.insert(key.0, key.clone());
        self.frames.insert(key, canonical);
        Ok(())
    }

    fn remember_committed(&mut self, command: &FlowCommand, outcome: &ApplyOutcome) {
        if !outcome.writes.is_empty() {
            *self
                .applied_by_kind
                .entry(command_kind(command))
                .or_default() += 1;
        }
        if let Some(marker) = operation_marker(command) {
            self.committed_markers.insert(marker.to_owned());
        }
        for write in &outcome.writes {
            let StateWrite::Put {
                column_family: ColumnFamily::Flow,
                key,
                value,
            } = write
            else {
                continue;
            };
            let Ok(parsed) = parse_run_key(key) else {
                continue;
            };
            match parsed.table {
                RunTable::Attribute => {
                    let (Ok(name), Ok(record)) = (
                        String::from_utf8(parsed.suffix.to_vec()),
                        AttributeRecord::decode(value.as_slice()),
                    ) else {
                        continue;
                    };
                    let wire = record
                        .value
                        .as_ref()
                        .filter(|stored| {
                            !matches!(stored.kind, Some(stored_value::Kind::HardDeleteObject(_)))
                        })
                        .and_then(|stored| wire_value(parsed.run_id, stored));
                    if let Some(wire) = wire {
                        self.attribute_values
                            .entry((command.flow_id.clone(), name))
                            .or_default()
                            .insert(wire.encode_to_vec());
                    }
                }
                RunTable::ChannelMessage => {
                    if let Ok(record) = ChannelMessageRecord::decode(value.as_slice()) {
                        self.channel_message_ids
                            .insert((command.flow_id.clone(), record.message_id));
                    }
                }
                _ => {}
            }
        }
    }
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
            "write {index} differs: {:?} vs {:?}",
            expected.writes[index], actual.writes[index]
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
