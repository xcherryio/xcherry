// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Engine-level deterministic simulation: real engine code on simulated storage, Workers, and
//! crashes.

mod committed_log;
mod engine_invariants;
mod event_trace;
mod simulated_clients;
mod simulated_clock;
mod simulated_node;
mod simulated_object_store;
mod simulated_workers;
mod simulation_driver;
mod workload;

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use dexzd_engine::EngineLocalStore;
use dexzd_protocol::dexzd::v1::NodeLease;
use prost::Message;

use crate::invariants::InvariantViolation;

/// How long one seed runs, in simulated time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineSimulationConfig {
    /// Seconds with clients, faults, crashes, and clock jumps.
    pub active_seconds: u64,
    /// Seconds of calm after the active phase: no clients, faults, or crashes.
    pub calm_tail_seconds: u64,
}

impl Default for EngineSimulationConfig {
    fn default() -> Self {
        Self {
            active_seconds: 120,
            calm_tail_seconds: 60,
        }
    }
}

/// What one seed exercised, and the hash of its event trace.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EngineSimulationSummary {
    /// Hash of the raw event trace; equal for two runs of one seed.
    pub trace_hash: u64,
    /// Events in the trace.
    pub trace_events: u64,
    /// Node process restarts, by what happened to the local store.
    pub restarts_by_local_store: BTreeMap<String, u64>,
    /// Successor lease writes that took effect while the old-epoch process still ran.
    pub old_epoch_overlaps: u64,
    /// Processes that crashed during recovery, before they served.
    pub crashes_during_start: u64,
    /// Frames old-epoch processes applied after their successor's lease write took effect.
    pub old_epoch_frames: u64,
    /// Writes old-epoch processes started after their successor's lease write took effect, by
    /// key class such as `log` or `lease`.
    pub old_epoch_write_attempts: BTreeMap<String, u64>,
    /// Conditional writes object storage rejected, by key class such as `log` or `lease`.
    pub precondition_failed_puts: BTreeMap<String, u64>,
    /// Objects deleted, by key class; `log` objects come from log cleanup.
    pub deleted_objects: BTreeMap<String, u64>,
    /// Snapshot manifest reads during starts, by how the start found its local store.
    pub snapshot_reads_during_start: BTreeMap<String, u64>,
    /// Commands committed without writes, by kind, such as a second result of one attempt.
    pub rejected_by_kind: BTreeMap<String, u64>,
    /// Running attempts the quiet-point crash compared with the successor's dispatches.
    pub attempts_compared_after_quiet_crash: u64,
    /// Processes that stopped by themselves, such as after losing the lease.
    pub self_stopped_processes: u64,
    /// Failed process starts, by error kind.
    pub start_failures: BTreeMap<String, u64>,
    /// Wall-clock jumps of a process.
    pub clock_jumps: u64,
    /// Object storage faults injected, by kind and what they did, such as
    /// `ResponseLost/WrittenOutcomeUnknown`.
    pub injected_faults: BTreeMap<String, u64>,
    /// Worker dispatches and RPCs, by behavior.
    pub worker_behaviors: BTreeMap<String, u64>,
    /// Commands that changed committed state, by command kind.
    pub applied_by_kind: BTreeMap<String, u64>,
    /// Client calls by operation and outcome.
    pub client_calls: BTreeMap<String, u64>,
    /// Client writes acknowledged and then found in the committed log.
    pub acknowledged_writes: u64,
    /// Client reads whose whole response matched the committed state at a generation their node
    /// applied during the call.
    pub checked_reads: u64,
    /// Values those reads returned.
    pub checked_read_values: u64,
    /// Frames applied again after a recovery and compared with their first application.
    pub replayed_frames: u64,
    /// Comparisons of a node's whole store with the shadow state.
    pub state_comparisons: u64,
    /// Comparisons of in-flight dispatches with committed running attempts.
    pub dispatch_comparisons: u64,
    /// Runs in the final state.
    pub final_runs: usize,
    /// The last trace events of the seed, oldest first; [`Self::merge`] keeps the latest seed's.
    pub recent_events: Vec<String>,
}

impl EngineSimulationSummary {
    /// Adds another seed's counts to this summary; the trace hash combines both.
    pub fn merge(&mut self, other: &Self) {
        self.trace_hash = self.trace_hash.rotate_left(5) ^ other.trace_hash;
        self.trace_events += other.trace_events;
        merge_counts(
            &mut self.restarts_by_local_store,
            &other.restarts_by_local_store,
        );
        self.old_epoch_overlaps += other.old_epoch_overlaps;
        self.crashes_during_start += other.crashes_during_start;
        self.old_epoch_frames += other.old_epoch_frames;
        merge_counts(
            &mut self.old_epoch_write_attempts,
            &other.old_epoch_write_attempts,
        );
        merge_counts(
            &mut self.precondition_failed_puts,
            &other.precondition_failed_puts,
        );
        merge_counts(&mut self.deleted_objects, &other.deleted_objects);
        merge_counts(
            &mut self.snapshot_reads_during_start,
            &other.snapshot_reads_during_start,
        );
        merge_counts(&mut self.rejected_by_kind, &other.rejected_by_kind);
        self.attempts_compared_after_quiet_crash += other.attempts_compared_after_quiet_crash;
        self.self_stopped_processes += other.self_stopped_processes;
        merge_counts(&mut self.start_failures, &other.start_failures);
        self.clock_jumps += other.clock_jumps;
        merge_counts(&mut self.injected_faults, &other.injected_faults);
        merge_counts(&mut self.worker_behaviors, &other.worker_behaviors);
        merge_counts(&mut self.applied_by_kind, &other.applied_by_kind);
        merge_counts(&mut self.client_calls, &other.client_calls);
        self.acknowledged_writes += other.acknowledged_writes;
        self.checked_reads += other.checked_reads;
        self.checked_read_values += other.checked_read_values;
        self.replayed_frames += other.replayed_frames;
        self.state_comparisons += other.state_comparisons;
        self.dispatch_comparisons += other.dispatch_comparisons;
        self.final_runs += other.final_runs;
        self.recent_events.clone_from(&other.recent_events);
    }
}

fn merge_counts(total: &mut BTreeMap<String, u64>, other: &BTreeMap<String, u64>) {
    for (name, count) in other {
        *total.entry(name.clone()).or_default() += count;
    }
}

/// A seed that broke an invariant.
#[derive(Debug, Clone)]
pub struct EngineSimulationFailure {
    /// The failing seed.
    pub seed: u64,
    /// The configuration it ran with.
    pub config: EngineSimulationConfig,
    /// Simulated time of the failure in microseconds since the start.
    pub simulated_micros: i64,
    /// The broken invariant.
    pub violation: InvariantViolation,
    /// The last trace events before the failure, oldest first.
    pub recent_events: Vec<String>,
}

impl fmt::Display for EngineSimulationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            formatter,
            "engine simulation seed {} broke an invariant at simulated time {} µs: {}",
            self.seed, self.simulated_micros, self.violation
        )?;
        writeln!(
            formatter,
            "reproduce with: DEXZD_SIMULATION_SEED={} DEXZD_SIMULATION_SECONDS={} cargo test -p \
             dexzd-simulation --test engine_simulation -- --nocapture",
            self.seed, self.config.active_seconds
        )?;
        writeln!(formatter, "last {} events:", self.recent_events.len())?;
        for event in &self.recent_events {
            writeln!(formatter, "  {event}")?;
        }
        Ok(())
    }
}

/// Everything one seed left behind, so two runs of the seed can be compared exactly.
pub struct EngineSimulationFinalState {
    /// Every trace event, oldest first, with engine IDs as the engine minted them.
    pub trace_events: Vec<String>,
    /// A copy of the serving process's local store at the end, if a process served.
    pub local_store: Option<Arc<EngineLocalStore>>,
    /// Every object in the shared object store at the end, by key.
    pub objects: BTreeMap<String, Bytes>,
}

impl EngineSimulationFinalState {
    /// Describes the first difference from `other`: a trace event, a local store row, or an
    /// object.
    ///
    /// Returns `None` when the traces, both local stores, and both object stores are identical.
    pub fn first_difference(&self, other: &Self) -> Option<String> {
        let event_count = self.trace_events.len().max(other.trace_events.len());
        if let Some(index) = (0..event_count)
            .find(|index| self.trace_events.get(*index) != other.trace_events.get(*index))
        {
            return Some(format!(
                "trace event {index} differs: {:?} vs {:?}",
                self.trace_events.get(index),
                other.trace_events.get(index)
            ));
        }
        match (&self.local_store, &other.local_store) {
            (Some(first), Some(second)) => {
                if let Err(difference) = engine_invariants::compare_local_stores(first, second) {
                    return Some(format!("final local stores differ: {difference}"));
                }
            }
            (None, None) => {}
            _ => return Some("only one run ended with a serving process".to_owned()),
        }
        let keys: std::collections::BTreeSet<&String> =
            self.objects.keys().chain(other.objects.keys()).collect();
        keys.into_iter()
            .find(|key| {
                comparable_object(key, self.objects.get(*key))
                    != comparable_object(key, other.objects.get(*key))
            })
            .map(|key| {
                format!(
                    "object {key} differs: {:?} vs {:?} bytes",
                    self.objects.get(key).map(Bytes::len),
                    other.objects.get(key).map(Bytes::len)
                )
            })
    }
}

/// Returns the object as compared between runs; lease holder IDs are random outside the engine.
fn comparable_object(key: &str, object: Option<&Bytes>) -> Option<Bytes> {
    let object = object?;
    if !key.starts_with("lease/") {
        return Some(object.clone());
    }
    let mut lease = NodeLease::decode(object.clone()).expect("stored leases decode");
    lease.holder_process_id.clear();
    Some(Bytes::from(lease.encode_to_vec()))
}

/// Runs one seed on a new current-thread runtime with paused time.
///
/// Returns what the seed exercised, or the first broken invariant with the events before it.
pub fn run_engine_seed(
    seed: u64,
    config: &EngineSimulationConfig,
) -> Result<EngineSimulationSummary, Box<EngineSimulationFailure>> {
    run_engine_seed_with_final_state(seed, config).map(|(summary, _)| summary)
}

/// Runs one seed like [`run_engine_seed`] and also returns its final state.
///
/// Returns what the seed exercised and its final state, or the first broken invariant with the
/// events before it.
pub fn run_engine_seed_with_final_state(
    seed: u64,
    config: &EngineSimulationConfig,
) -> Result<(EngineSimulationSummary, EngineSimulationFinalState), Box<EngineSimulationFailure>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("a current-thread runtime with paused time builds");
    runtime.block_on(simulation_driver::run_simulation(seed, *config))
}
