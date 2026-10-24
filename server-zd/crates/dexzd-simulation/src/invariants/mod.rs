// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! State invariants checked after every applied command (plan sections 8 and 10).

mod directory_invariants;
mod lock_invariants;
mod value_invariants;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use dexzd_keyspace::{FlowHash, RunId, ValueId};

use crate::memory_state::MemoryFlowState;
use crate::state_view::FlowView;

pub use lock_invariants::check_fresh_snapshot;
pub(crate) use value_invariants::remember_window_points;

pub(crate) fn check_window_value_content(
    flow_id: &str,
    view: &FlowView,
    memory: &CheckerMemory,
    state: &MemoryFlowState,
    flow_hash: FlowHash,
) -> Result<(), InvariantViolation> {
    for (run_id, run) in view.runs.iter().filter(|(_, run)| run.is_complete) {
        let context = RunContext { flow_id, run_id };
        value_invariants::check_window_value_content(
            &context,
            run,
            memory.window_points.get(run_id),
            state,
            flow_hash,
        )?;
    }
    Ok(())
}

/// A broken invariant: a stable name for grouping and a description of the evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvariantViolation {
    /// Stable name of the invariant, such as `L3-lock-holder`.
    pub invariant: &'static str,
    /// What was observed.
    pub detail: String,
}

impl InvariantViolation {
    /// Creates a violation of `invariant`.
    pub fn new(invariant: &'static str, detail: impl Into<String>) -> Self {
        Self {
            invariant,
            detail: detail.into(),
        }
    }
}

impl fmt::Display for InvariantViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.invariant, self.detail)
    }
}

/// Facts the checker learned from earlier commands that state alone does not show.
#[derive(Debug, Clone, Default)]
pub struct CheckerMemory {
    /// Values each run pinned with the genesis holder when it started (plan D12).
    pub genesis_values: BTreeMap<RunId, BTreeSet<ValueId>>,
    /// Value references observed when each semantic point was current, retained through its window.
    pub(crate) window_points: BTreeMap<RunId, BTreeMap<u64, WindowPointState>>,
}

#[derive(Debug, Clone)]
pub(crate) struct WindowPointState {
    pub(crate) references: BTreeSet<(ValueId, u64)>,
    pub(crate) attribute_values: BTreeSet<ValueId>,
}

/// Attribute keys that only lock-holding attempts write, for the lost-update check (L2).
pub const GUARDED_ATTRIBUTE_KEYS: &[&str] = &["counter"];

/// Checks the directory against every run record, and every invariant of completely decoded runs.
///
/// Returns the first violation found.
pub fn check_flow(
    flow_id: &str,
    view: &FlowView,
    memory: &CheckerMemory,
) -> Result<(), InvariantViolation> {
    directory_invariants::check_directory(flow_id, view)?;
    for (run_id, run) in view.runs.iter().filter(|(_, run)| run.is_complete) {
        let context = RunContext { flow_id, run_id };
        directory_invariants::check_run_record(&context, run)?;
        directory_invariants::check_history(&context, run)?;
        directory_invariants::check_timers(&context, run, view)?;
        lock_invariants::check_invocations(&context, run)?;
        lock_invariants::check_locks(&context, run)?;
        lock_invariants::check_guarded_attributes(&context, run)?;
        let genesis = memory.genesis_values.get(run_id);
        value_invariants::check_values(&context, run, genesis, memory.window_points.get(run_id))?;
    }
    Ok(())
}

/// Identifies the run being checked in violation messages.
pub(crate) struct RunContext<'check> {
    pub(crate) flow_id: &'check str,
    pub(crate) run_id: &'check RunId,
}

impl fmt::Display for RunContext<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Flow {} run {}", self.flow_id, self.run_id)
    }
}
