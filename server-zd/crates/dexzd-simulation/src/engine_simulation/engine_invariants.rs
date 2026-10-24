// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Invariants checked on a recovered node and at quiescent points.

use std::collections::{BTreeMap, BTreeSet};

use dexzd_apply::StateWrite;
use dexzd_engine::{EngineLocalStore, HardDeleteObjectOwner};
use dexzd_keyspace::row_keys::HARD_DELETE_PENDING_OBJECT;
use dexzd_keyspace::{ColumnFamily, RunTable, TenantId, parse_run_key};
use dexzd_protocol::dexzd::v1::{OutboxRecord, RunRecord, StepInvocationPhase, StepMethod};
use dexzd_storage::{
    CanonicalizedStoreField, InMemoryLocalStore, LocalReadView, LocalStore, ScanDirection,
    StateComparisonError, compare_store_views,
};
use prost::Message;

use crate::apply_checker::ApplyChecker;
use crate::engine_simulation::committed_log::CommittedLog;
use crate::engine_simulation::simulated_clients::ClientRecords;
use crate::engine_simulation::simulated_workers::AttemptIdentity;
use crate::invariants::{CheckerMemory, InvariantViolation, check_flow};
use crate::memory_state::MemoryFlowState;
use crate::row_decoding::hex;
use crate::state_view::{FlowView, RunSelection};

/// Holder encoding of the values a run pinned when it started (plan D12).
const GENESIS_HOLDER: [u8; 1] = [0x01];
/// Column families that apply writes; partition and node metadata differ between processes.
const APPLIED_COLUMN_FAMILIES: [ColumnFamily; 4] = [
    ColumnFamily::Flow,
    ColumnFamily::History,
    ColumnFamily::Value,
    ColumnFamily::PartitionIndex,
];

/// Invariant 1: every acknowledged write is in the canonical committed log.
pub(crate) fn check_acknowledged_writes(
    records: &ClientRecords,
    committed_log: &CommittedLog,
) -> Result<(), InvariantViolation> {
    for write in &records.acknowledged_writes {
        if committed_log.has_committed_marker(&write.marker) {
            continue;
        }
        // StartFlow may answer with the run that already exists, without a command.
        let is_existing_run = write.started_run_id.as_deref().is_some_and(|run_id| {
            committed_log.with_shadow(|shadow| run_exists(shadow, &write.flow_id, run_id))
        });
        if is_existing_run {
            continue;
        }
        return Err(InvariantViolation::new(
            "acknowledged-write-lost",
            format!(
                "{} {} of Flow {} was acknowledged, but no committed command carries it",
                write.operation, write.marker, write.flow_id
            ),
        ));
    }
    Ok(())
}

fn run_exists(shadow: &ApplyChecker, flow_id: &str, run_id: &str) -> bool {
    let Ok(view) = shadow.view(flow_id, RunSelection::Only(&BTreeSet::new())) else {
        return false;
    };
    view.runs
        .iter()
        .any(|(id, run)| id.to_string() == run_id && run.record.is_some())
}

/// Fields a valid recovery changes; every other byte must equal the never-crashed twin.
const RECOVERY_CANONICALIZED_FIELDS: [CanonicalizedStoreField; 4] = [
    // Restoring a snapshot starts a new local store lineage.
    CanonicalizedStoreField::NodeLineageId,
    // The restored store starts a new snapshot sequence.
    CanonicalizedStoreField::NodeLastSnapshotNumber,
    // Recovery records progress under its new writer epoch.
    CanonicalizedStoreField::PartitionApplied,
    // Recovery acquires a new writer epoch.
    CanonicalizedStoreField::PartitionAppliedEpoch,
];

/// Returns whether the node's hard-delete upload ledger is empty.
///
/// The ledger is node-local effect state that the committed log does not determine, so the twin
/// holds an empty one.
pub(crate) fn has_empty_upload_ledger(
    store: &EngineLocalStore,
) -> Result<bool, InvariantViolation> {
    let node_meta = store.node_meta().map_err(|error| {
        InvariantViolation::new(
            "local-store-read",
            format!("reading node metadata failed: {error}"),
        )
    })?;
    Ok(node_meta.is_none_or(|meta| meta.pending_hard_delete_uploads.is_empty()))
}

/// Invariant 5: the node's store equals a never-crashed twin, except the canonicalized fields.
pub(crate) fn check_store_matches_twin(
    store: &EngineLocalStore,
    twin: &InMemoryLocalStore,
) -> Result<(), InvariantViolation> {
    let view = store.consistent_view().map_err(|error| {
        InvariantViolation::new(
            "local-store-read",
            format!("opening a view failed: {error}"),
        )
    })?;
    compare_store_views(twin, view.as_ref(), &RECOVERY_CANONICALIZED_FIELDS).map_err(|error| {
        match error {
            StateComparisonError::Storage(error) => {
                InvariantViolation::new("local-store-read", format!("scan failed: {error}"))
            }
            StateComparisonError::Difference(difference) => InvariantViolation::new(
                "recovered-state-differs",
                format!("the never-crashed twin and the node differ: {difference}"),
            ),
        }
    })
}

/// Invariant 4: the stage-1 Flow invariants hold on the recovered store itself.
///
/// Returns whether some run was checked with window points older than the shadow's current ones,
/// or the first violation.
pub(crate) fn check_recovered_flows(
    store: &EngineLocalStore,
    committed_log: &CommittedLog,
) -> Result<bool, InvariantViolation> {
    let state = memory_state_of(store)?;
    let mut has_older_window_points = false;
    for flow_id in &committed_log.with_shadow(all_flow_ids) {
        // An old-epoch process may have applied frames this store has not.
        let applied_commands = committed_log
            .applied_shadow_commands(store, flow_id)
            .map_err(|detail| InvariantViolation::new("local-store-read", detail))?;
        committed_log.with_shadow(|shadow| {
            let context = shadow.context(flow_id);
            let view = FlowView::read(
                &state,
                context.flow_hash,
                context.partition,
                RunSelection::All,
            )
            .map_err(|error| InvariantViolation::new("row-decoding", error.to_string()))?;
            let memory = recovered_memory(&view, shadow, applied_commands);
            has_older_window_points |= memory.window_points.iter().any(|(run_id, points)| {
                *points != shadow.window_points_after(run_id, shadow.applied_commands())
            });
            check_flow(flow_id, &view, &memory)
        })?;
    }
    Ok(has_older_window_points)
}

/// Genesis holders as the store records them; the shadow already checked them command by command.
/// Window points come from the shadow, which recorded each point while it was current.
/// They are the points retained once the shadow had applied `applied_commands` commands.
fn recovered_memory(
    view: &FlowView,
    shadow: &ApplyChecker,
    applied_commands: u64,
) -> CheckerMemory {
    let genesis_values = view
        .runs
        .iter()
        .map(|(run_id, run)| {
            let pinned = run
                .value_holders
                .iter()
                .filter(|(_, holders)| holders.contains(GENESIS_HOLDER.as_slice()))
                .map(|(value_id, _)| *value_id)
                .collect();
            (*run_id, pinned)
        })
        .collect();
    let window_points = view
        .runs
        .keys()
        .map(|run_id| {
            (
                *run_id,
                shadow.window_points_after(run_id, applied_commands),
            )
        })
        .collect();
    CheckerMemory {
        genesis_values,
        window_points,
    }
}

fn memory_state_of(store: &EngineLocalStore) -> Result<MemoryFlowState, InvariantViolation> {
    let view = store.consistent_view().map_err(|error| {
        InvariantViolation::new(
            "local-store-read",
            format!("opening a view failed: {error}"),
        )
    })?;
    let mut state = MemoryFlowState::default();
    for column_family in APPLIED_COLUMN_FAMILIES {
        let rows = view
            .scan(
                column_family,
                None,
                None,
                ScanDirection::Forward,
                usize::MAX,
            )
            .map_err(|error| {
                InvariantViolation::new("local-store-read", format!("scan failed: {error}"))
            })?;
        let writes: Vec<StateWrite> = rows
            .into_iter()
            .map(|(key, value)| StateWrite::Put {
                column_family,
                key,
                value,
            })
            .collect();
        state.commit(&writes);
    }
    Ok(state)
}

/// Committed method attempts that are running now, as Workers identify them.
pub(crate) fn running_attempts(
    shadow: &ApplyChecker,
) -> Result<BTreeSet<AttemptIdentity>, InvariantViolation> {
    let mut running = BTreeSet::new();
    for flow_id in &all_flow_ids(shadow) {
        let view = shadow.view(flow_id, RunSelection::All)?;
        for (run_id, run) in &view.runs {
            let run_attempt = run
                .record
                .as_ref()
                .map_or(0, |record| record.current_attempt);
            for step in run.step_executions.values() {
                let Some(invocation) = &step.invocation else {
                    continue;
                };
                if invocation.phase != StepInvocationPhase::Running as i32 {
                    continue;
                }
                running.insert(AttemptIdentity {
                    flow_id: flow_id.clone(),
                    run_id: run_id.to_string(),
                    run_attempt: i32::try_from(run_attempt).unwrap_or(i32::MAX),
                    step_execution_id: step.step_execution_id.clone(),
                    is_execute: invocation.method == StepMethod::Execute as i32,
                    attempt: i32::try_from(invocation.attempt).unwrap_or(i32::MAX),
                });
            }
        }
    }
    Ok(running)
}

/// Invariant 5: after a crash, the successor redispatches every attempt that is still running.
pub(crate) fn check_running_attempts_redispatched(
    process_label: &str,
    dispatched: &[AttemptIdentity],
    running_at_recovery: &BTreeSet<AttemptIdentity>,
    running_now: &BTreeSet<AttemptIdentity>,
) -> Result<(), InvariantViolation> {
    let dispatched: BTreeSet<&AttemptIdentity> = dispatched.iter().collect();
    let forgotten = running_at_recovery
        .iter()
        .find(|attempt| !dispatched.contains(attempt) && running_now.contains(attempt));
    match forgotten {
        Some(attempt) => Err(InvariantViolation::new(
            "running-attempt-not-redispatched",
            format!("{process_label} never dispatched committed running attempt {attempt:?}"),
        )),
        None => Ok(()),
    }
}

/// Liveness after the calm tail: no overdue timer, no unfinished method call, no undelivered
/// outbox message, and no hard-delete object waiting for deletion.
pub(crate) fn check_drained(
    shadow: &ApplyChecker,
    now_unix_micros: i64,
    overdue_after_micros: i64,
) -> Result<(), InvariantViolation> {
    let work = outstanding_work(shadow)?;
    if let Some(row_key) = work.undelivered_outbox.iter().next() {
        return Err(InvariantViolation::new(
            "outbox-not-delivered",
            format!(
                "outbox row {} is unacknowledged after the calm tail",
                hex(row_key)
            ),
        ));
    }
    if let Some(object_key) = work.pending_hard_deletes.values().next() {
        return Err(InvariantViolation::new(
            "hard-delete-not-finished",
            format!("hard-delete object {object_key} still waits for deletion after the calm tail"),
        ));
    }
    for flow_id in &all_flow_ids(shadow) {
        let view = shadow.view(flow_id, RunSelection::All)?;
        if let Some(overdue) = view
            .timer_index
            .iter()
            .find(|entry| entry.fire_at_micros + overdue_after_micros < now_unix_micros)
        {
            return Err(InvariantViolation::new(
                "timer-not-fired",
                format!(
                    "Flow {flow_id} run {} has a timer due at {} that never fired",
                    overdue.run_id, overdue.fire_at_micros
                ),
            ));
        }
        for (run_id, run) in &view.runs {
            for step in run.step_executions.values() {
                let Some(invocation) = &step.invocation else {
                    continue;
                };
                let phase = StepInvocationPhase::try_from(invocation.phase);
                if matches!(
                    phase,
                    Ok(StepInvocationPhase::Pending | StepInvocationPhase::Running)
                ) {
                    return Err(InvariantViolation::new(
                        "method-call-not-finished",
                        format!(
                            "Flow {flow_id} run {run_id} Step execution {} attempt {} is still \
                             {phase:?} after the calm tail",
                            step.step_execution_id, invocation.attempt
                        ),
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Returns the number of runs the shadow holds.
pub(crate) fn run_count(shadow: &ApplyChecker) -> usize {
    all_flow_ids(shadow)
        .iter()
        .filter_map(|flow_id| {
            shadow
                .view(flow_id, RunSelection::Only(&BTreeSet::new()))
                .ok()
        })
        .map(|view| view.runs.len())
        .sum()
}

/// Returns every Flow with a run record, clients' Flows and their child Flows alike.
pub(crate) fn all_flow_ids(shadow: &ApplyChecker) -> BTreeSet<String> {
    shadow
        .state()
        .rows_with_prefix(ColumnFamily::Flow, &[])
        .filter(|(key, _)| {
            parse_run_key(key).is_ok_and(|parsed| parsed.table == RunTable::RunRecord)
        })
        .filter_map(|(_, value)| RunRecord::decode(value).ok())
        .map(|record| record.flow_id)
        .collect()
}

/// Committed work that a node must finish, recover, or redo after a crash.
#[derive(Debug, Default)]
pub(crate) struct OutstandingWork {
    /// Timer rows, which only firing or cancellation removes.
    pub(crate) timers: usize,
    /// Method attempts that are running.
    pub(crate) running_attempts: usize,
    /// Keys of outbox rows the receiver has not acknowledged.
    pub(crate) undelivered_outbox: BTreeSet<Vec<u8>>,
    /// Object keys of hard-delete objects waiting for deletion, by row key.
    pub(crate) pending_hard_deletes: BTreeMap<Vec<u8>, String>,
}

/// Returns the work outstanding in the shadow's committed state.
///
/// Returns the work, or the violation of a run that the shadow cannot decode.
pub(crate) fn outstanding_work(
    shadow: &ApplyChecker,
) -> Result<OutstandingWork, InvariantViolation> {
    let mut work = OutstandingWork {
        running_attempts: running_attempts(shadow)?.len(),
        ..OutstandingWork::default()
    };
    for (key, value) in shadow.state().rows_with_prefix(ColumnFamily::Flow, &[]) {
        let Ok(parsed) = parse_run_key(key) else {
            continue;
        };
        match parsed.table {
            RunTable::Timer => work.timers += 1,
            RunTable::Outbox => {
                let record = OutboxRecord::decode(value).map_err(|error| {
                    InvariantViolation::new("row-decoding", format!("outbox row: {error}"))
                })?;
                if record.acknowledged_at_micros == 0 {
                    work.undelivered_outbox.insert(key.to_vec());
                }
            }
            RunTable::HardDeleteObject => {
                if let Some((&HARD_DELETE_PENDING_OBJECT, object_id)) = parsed.suffix.split_first()
                {
                    let owner = HardDeleteObjectOwner {
                        tenant_id: TenantId::DEFAULT,
                        flow_hash: parsed.flow_hash,
                        run_id: parsed.run_id,
                    };
                    let object_key = owner.object_key(&String::from_utf8_lossy(object_id));
                    work.pending_hard_deletes.insert(key.to_vec(), object_key);
                }
            }
            _ => {}
        }
    }
    Ok(work)
}
