// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Invariants checked on a recovered node and at quiescent points.

use std::collections::BTreeSet;

use dexzd_apply::StateWrite;
use dexzd_engine::EngineLocalStore;
use dexzd_keyspace::ColumnFamily;
use dexzd_protocol::dexzd::v1::{StepInvocationPhase, StepMethod};
use dexzd_storage::{LocalReadView, LocalStore, ScanDirection};

use crate::apply_checker::ApplyChecker;
use crate::engine_simulation::committed_log::CommittedLog;
use crate::engine_simulation::simulated_clients::{ClientRecords, ENGINE_FLOW_IDS};
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

/// Invariant 5: the recovered store holds exactly the rows of the never-crashed shadow.
pub(crate) fn check_store_matches_shadow(
    store: &EngineLocalStore,
    shadow: &MemoryFlowState,
) -> Result<(), InvariantViolation> {
    let view = store.consistent_view().map_err(|error| {
        InvariantViolation::new(
            "local-store-read",
            format!("opening a view failed: {error}"),
        )
    })?;
    for column_family in APPLIED_COLUMN_FAMILIES {
        let engine_rows = view
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
        let mut shadow_rows = shadow.rows_with_prefix(column_family, &[]);
        for (engine_key, engine_value) in &engine_rows {
            match shadow_rows.next() {
                Some((shadow_key, shadow_value))
                    if shadow_key == engine_key.as_slice()
                        && shadow_value == engine_value.as_slice() => {}
                Some((shadow_key, _)) => {
                    return Err(state_difference(
                        column_family,
                        format!(
                            "the node has {} where the shadow has {}",
                            hex(engine_key),
                            hex(shadow_key)
                        ),
                    ));
                }
                None => {
                    return Err(state_difference(
                        column_family,
                        format!("the node has extra row {}", hex(engine_key)),
                    ));
                }
            }
        }
        if let Some((shadow_key, _)) = shadow_rows.next() {
            return Err(state_difference(
                column_family,
                format!("the node lacks row {}", hex(shadow_key)),
            ));
        }
    }
    Ok(())
}

/// Compares every column family of two stores row by row, like `compare_store_views` with no
/// canonicalized fields.
///
/// Returns the first differing key, or the error that stopped a read.
pub(crate) fn compare_local_stores(
    expected: &EngineLocalStore,
    actual: &EngineLocalStore,
) -> Result<(), String> {
    let expected_view = expected
        .consistent_view()
        .map_err(|error| error.to_string())?;
    let actual_view = actual
        .consistent_view()
        .map_err(|error| error.to_string())?;
    for column_family in ColumnFamily::ALL {
        let scan = |view: &dyn LocalReadView| {
            view.scan(
                column_family,
                None,
                None,
                ScanDirection::Forward,
                usize::MAX,
            )
            .map_err(|error| error.to_string())
        };
        let expected_rows = scan(expected_view.as_ref())?;
        let actual_rows = scan(actual_view.as_ref())?;
        let row_count = expected_rows.len().max(actual_rows.len());
        if let Some(index) =
            (0..row_count).find(|index| expected_rows.get(*index) != actual_rows.get(*index))
        {
            let key_of = |rows: &[(Vec<u8>, Vec<u8>)]| rows.get(index).map(|(key, _)| hex(key));
            return Err(format!(
                "column family {column_family:?} row {index}: {:?} vs {:?}",
                key_of(&expected_rows),
                key_of(&actual_rows)
            ));
        }
    }
    Ok(())
}

fn state_difference(column_family: ColumnFamily, detail: String) -> InvariantViolation {
    InvariantViolation::new(
        "recovered-state-differs",
        format!("column family {column_family:?}: {detail}"),
    )
}

/// Invariant 4: the stage-1 Flow invariants hold on the recovered store itself.
pub(crate) fn check_recovered_flows(
    store: &EngineLocalStore,
    shadow: &ApplyChecker,
) -> Result<(), InvariantViolation> {
    let state = memory_state_of(store)?;
    for flow_id in ENGINE_FLOW_IDS {
        let context = shadow.context(flow_id);
        let view = FlowView::read(
            &state,
            context.flow_hash,
            context.partition,
            RunSelection::All,
        )
        .map_err(|error| InvariantViolation::new("row-decoding", error.to_string()))?;
        check_flow(flow_id, &view, &genesis_memory(&view))?;
    }
    Ok(())
}

/// Genesis holders as the store records them; the shadow already checked them command by command.
fn genesis_memory(view: &FlowView) -> CheckerMemory {
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
    CheckerMemory { genesis_values }
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
    for flow_id in ENGINE_FLOW_IDS {
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
                    flow_id: (*flow_id).to_owned(),
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

/// Liveness after the calm tail: no overdue timer and no unfinished method call.
pub(crate) fn check_drained(
    shadow: &ApplyChecker,
    now_unix_micros: i64,
    overdue_after_micros: i64,
) -> Result<(), InvariantViolation> {
    for flow_id in ENGINE_FLOW_IDS {
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
    ENGINE_FLOW_IDS
        .iter()
        .filter_map(|flow_id| {
            shadow
                .view(flow_id, RunSelection::Only(&BTreeSet::new()))
                .ok()
        })
        .map(|view| view.runs.len())
        .sum()
}
