// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeSet;

use dexzd_apply::{ApplyContext, ApplyOutcome, StateWrite, apply_flow_command};
use dexzd_keyspace::row_keys::run_singleton_key;
use dexzd_keyspace::{ColumnFamily, PartitionHasher, RunId, RunTable, TenantId, parse_run_key};
use dexzd_protocol::dexzd::v1::{
    FlowCommand, RunRecord, StepInvocationKey, command_reply, flow_command::Command, stored_value,
};
use prost::Message;

use crate::invariants::{
    CheckerMemory, DefectTolerance, InvariantViolation, check_flow, check_fresh_snapshot,
};
use crate::memory_state::MemoryFlowState;
use crate::state_view::{FlowView, RunSelection};

/// Key of the keyed partition hash; any fixed value keeps runs reproducible.
const PARTITION_HASH_KEY: [u8; 16] = [7; 16];
const PARTITION_COUNT: u32 = 64;

/// Applies commands to an in-memory state and checks determinism and every invariant after
/// each one.
pub struct ApplyChecker {
    state: MemoryFlowState,
    /// The same state with every row decoded and encoded again when it was written.
    redecoded_state: MemoryFlowState,
    hasher: PartitionHasher,
    memory: CheckerMemory,
    tolerance: DefectTolerance,
    applied_results: BTreeSet<AppliedResultIdentity>,
}

/// One result of one attempt (S2): at most one may ever be applied.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct AppliedResultIdentity {
    run_id: RunId,
    run_attempt: u32,
    step_execution_id: String,
    method: i32,
    attempt: u32,
}

impl ApplyChecker {
    /// Creates a checker over an empty state.
    pub fn new(tolerance: DefectTolerance) -> Self {
        Self {
            state: MemoryFlowState::default(),
            redecoded_state: MemoryFlowState::default(),
            hasher: PartitionHasher::new(&PARTITION_HASH_KEY, PARTITION_COUNT)
                .expect("the fixed partition hash key and count are valid"),
            memory: CheckerMemory::default(),
            tolerance,
            applied_results: BTreeSet::new(),
        }
    }

    /// The committed state.
    pub fn state(&self) -> &MemoryFlowState {
        &self.state
    }

    /// Returns the apply context of `flow_id` in the default tenant.
    pub fn context(&self, flow_id: &str) -> ApplyContext {
        let flow_hash = self.hasher.flow_hash(TenantId::DEFAULT, flow_id);
        ApplyContext {
            partition: self.hasher.partition_of(flow_hash),
            flow_hash,
        }
    }

    /// Decodes `flow_id`: every run record, and every row of the runs `selection` names.
    ///
    /// Returns a violation when a row does not decode.
    pub fn view(
        &self,
        flow_id: &str,
        selection: RunSelection<'_>,
    ) -> Result<FlowView, InvariantViolation> {
        let context = self.context(flow_id);
        FlowView::read(&self.state, context.flow_hash, context.partition, selection)
            .map_err(|error| InvariantViolation::new("row-decoding", error.to_string()))
    }

    /// Applies `command` three times (A2), commits its writes, and checks the Flow's invariants.
    ///
    /// Returns the outcome, or the first violated invariant.
    pub fn apply_and_check(
        &mut self,
        command: &FlowCommand,
    ) -> Result<ApplyOutcome, InvariantViolation> {
        let context = self.context(&command.flow_id);
        let apply = |state: &MemoryFlowState| {
            apply_flow_command(&context, state, command)
                .map_err(|error| InvariantViolation::new("apply-error", error.to_string()))
        };
        // A2: a repeated apply and an apply on the re-decoded twin must match exactly.
        let outcome = apply(&self.state)?;
        let repeated = apply(&self.state)?;
        compare_outcomes("A2-apply-twice", &outcome, &repeated)?;
        let redecoded = apply(&self.redecoded_state)?;
        compare_outcomes("A2-apply-redecoded", &outcome, &redecoded)?;
        let is_applied = !outcome.writes.is_empty();
        if is_applied {
            self.record_applied_result(command)?;
        }
        self.state.commit(&outcome.writes);
        self.redecoded_state
            .commit_redecoded(&outcome.writes)
            .map_err(|error| InvariantViolation::new("row-decoding", error.to_string()))?;
        let touched_runs = touched_runs(&outcome);
        let view = self.view(&command.flow_id, RunSelection::Only(&touched_runs))?;
        if is_applied {
            self.check_command_effects(command, &outcome, &view)?;
        }
        check_flow(&command.flow_id, &view, &self.memory, self.tolerance)?;
        Ok(outcome)
    }

    fn record_applied_result(&mut self, command: &FlowCommand) -> Result<(), InvariantViolation> {
        let (invocation, attempt) = match &command.command {
            Some(Command::StepMethodCompleted(completed)) => {
                (completed.invocation.as_ref(), completed.attempt)
            }
            Some(Command::StepAttemptFailed(failed)) => {
                (failed.invocation.as_ref(), failed.attempt)
            }
            _ => return Ok(()),
        };
        let invocation = invocation
            .cloned()
            .unwrap_or_else(StepInvocationKey::default);
        let run_id = RunId::from_slice(&command.run_id)
            .map_err(|error| InvariantViolation::new("apply-error", error.to_string()))?;
        let run_attempt = self.run_record(&command.flow_id, run_id)?.current_attempt;
        let identity = AppliedResultIdentity {
            run_id,
            run_attempt,
            step_execution_id: invocation.step_execution_id,
            method: invocation.method,
            attempt,
        };
        if self.applied_results.contains(&identity) {
            return Err(InvariantViolation::new(
                "S2-result-once",
                format!("a second result was applied for {identity:?}"),
            ));
        }
        self.applied_results.insert(identity);
        Ok(())
    }

    fn check_command_effects(
        &mut self,
        command: &FlowCommand,
        outcome: &ApplyOutcome,
        view: &FlowView,
    ) -> Result<(), InvariantViolation> {
        match &command.command {
            Some(Command::FlowStart(_)) => {
                if let Some(command_reply::Reply::StartFlow(reply)) = &outcome.reply.reply
                    && !reply.is_existing_run
                {
                    self.remember_genesis_values(&reply.run_id, view)?;
                }
            }
            Some(Command::StepAttemptStarted(started)) => {
                let run_id = RunId::from_slice(&command.run_id)
                    .map_err(|error| InvariantViolation::new("apply-error", error.to_string()))?;
                let run = view.runs.get(&run_id).ok_or_else(|| {
                    InvariantViolation::new("S5-fresh-snapshot", "the started run has no rows")
                })?;
                let step_execution_id = started
                    .invocation
                    .as_ref()
                    .map(|invocation| invocation.step_execution_id.as_str())
                    .unwrap_or_default();
                check_fresh_snapshot(run, step_execution_id)?;
            }
            _ => {}
        }
        Ok(())
    }

    /// The new run's start Step input and initial Attributes hold the genesis pin (plan D12).
    fn remember_genesis_values(
        &mut self,
        run_id: &[u8],
        view: &FlowView,
    ) -> Result<(), InvariantViolation> {
        let run_id = RunId::from_slice(run_id)
            .map_err(|error| InvariantViolation::new("apply-error", error.to_string()))?;
        let Some(run) = view.runs.get(&run_id) else {
            return Err(InvariantViolation::new(
                "flow-directory",
                format!("StartFlow replied with run {run_id}, which has no rows"),
            ));
        };
        let stored_values = run
            .step_executions
            .values()
            .filter_map(|step| step.input.as_ref())
            .chain(
                run.attributes
                    .values()
                    .filter_map(|record| record.value.as_ref()),
            );
        let genesis = stored_values
            .filter_map(|stored| match &stored.kind {
                Some(stored_value::Kind::TableValue(reference)) => {
                    reference.value_id.map(|value_id| dexzd_keyspace::ValueId {
                        run_seq: value_id.run_seq,
                        index_in_command: value_id.index_in_command,
                    })
                }
                _ => None,
            })
            .collect();
        self.memory.genesis_values.insert(run_id, genesis);
        Ok(())
    }

    fn run_record(&self, flow_id: &str, run_id: RunId) -> Result<RunRecord, InvariantViolation> {
        let key = run_singleton_key(self.context(flow_id).flow_hash, run_id, RunTable::RunRecord);
        let bytes = self.state.row(ColumnFamily::Flow, &key).ok_or_else(|| {
            InvariantViolation::new("run-record", format!("run {run_id} has no run record"))
        })?;
        RunRecord::decode(bytes)
            .map_err(|error| InvariantViolation::new("row-decoding", error.to_string()))
    }
}

/// Runs whose rows the outcome wrote; only they can have changed.
fn touched_runs(outcome: &ApplyOutcome) -> BTreeSet<RunId> {
    let mut run_ids: BTreeSet<RunId> = outcome.run_id.into_iter().collect();
    for write in &outcome.writes {
        let (column_family, key) = match write {
            StateWrite::Put {
                column_family, key, ..
            }
            | StateWrite::Delete { column_family, key } => (column_family, key),
            StateWrite::DeleteRange {
                column_family,
                start,
                ..
            } => (column_family, start),
        };
        if column_family.is_flow_scoped()
            && let Ok(parsed) = parse_run_key(key)
        {
            run_ids.insert(parsed.run_id);
        }
    }
    run_ids
}

fn compare_outcomes(
    invariant: &'static str,
    expected: &ApplyOutcome,
    actual: &ApplyOutcome,
) -> Result<(), InvariantViolation> {
    if expected == actual {
        return Ok(());
    }
    let first_write_difference = expected
        .writes
        .iter()
        .zip(&actual.writes)
        .position(|(left, right)| left != right);
    let detail = match first_write_difference {
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
            "writes match but effects or reply differ: {:?} / {:?} vs {:?} / {:?}",
            expected.effects, expected.reply, actual.effects, actual.reply
        ),
    };
    Err(InvariantViolation::new(invariant, detail))
}

fn describe_write(write: &StateWrite) -> String {
    use crate::row_decoding::hex;
    match write {
        StateWrite::Put {
            column_family,
            key,
            value,
        } => format!("put {column_family:?} {} ({} bytes)", hex(key), value.len()),
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
