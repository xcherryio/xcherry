// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Reported apply defects that the checker tolerates, so other regressions stay visible.
//!
//! Delete an entry once apply fixes it; `DEXZD_SIMULATION_STRICT=1` verifies the fix.

use std::collections::BTreeSet;

use dexzd_keyspace::ValueId;
use dexzd_protocol::dexzd::v1::StepExecutionPhase;

use crate::state_view::RunView;

/// Whether the checker tolerates the reported apply defects listed in this module.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DefectTolerance {
    /// Every invariant is checked as specified.
    Strict,
    /// Differences that a listed defect explains are ignored.
    ///
    /// Defects 1 and 1b (hard-delete and plain writes replacing each other) are avoided instead.
    TolerateKnownApplyDefects,
}

const ATTRIBUTE_HOLDER: u8 = 0x02;
const HEARTBEAT_CHECKPOINT_HOLDER: u8 = 0x07;

/// Removes holder differences of one value that known apply defects explain.
pub(crate) fn remove_explained_holder_differences(
    run: &RunView,
    value_id: ValueId,
    expected: &mut BTreeSet<Vec<u8>>,
    actual: &mut BTreeSet<Vec<u8>>,
) {
    let stale: Vec<Vec<u8>> = actual.difference(expected).cloned().collect();
    for holder in stale {
        if is_leaked_checkpoint_holder(run, &holder) {
            actual.remove(&holder);
        }
    }
    let missing: Vec<Vec<u8>> = expected.difference(actual).cloned().collect();
    for holder in missing {
        if is_dropped_echo_holder(run, value_id, &holder) {
            expected.remove(&holder);
        }
    }
}

/// Defect 2: retry exhaustion drops the heartbeat checkpoint without releasing its holder.
fn is_leaked_checkpoint_holder(run: &RunView, holder: &[u8]) -> bool {
    if holder.first() != Some(&HEARTBEAT_CHECKPOINT_HOLDER) {
        return false;
    }
    first_name(holder)
        .and_then(|step_execution_id| run.step_executions.get(&step_execution_id))
        .is_some_and(|step| step.phase == StepExecutionPhase::Failed as i32)
}

/// Defect 3: rewriting an Attribute with a reference to its own, older value drops its holder.
fn is_dropped_echo_holder(run: &RunView, value_id: ValueId, holder: &[u8]) -> bool {
    if holder.first() != Some(&ATTRIBUTE_HOLDER) {
        return false;
    }
    first_name(holder)
        .and_then(|key| run.attributes.get(&key))
        .is_some_and(|record| value_id.run_seq < record.written_seq)
}

/// Defect 4: releasing an absent holder of a value in G writes another G row; the older row
/// stays.
pub(crate) fn is_superseded_garbage_row(run: &RunView, seq: u64, value_id: ValueId) -> bool {
    let Some(birth) = run.value_births.get(&value_id) else {
        return false;
    };
    birth.garbage_seq > seq
        && run.value_garbage.iter().any(|(other_seq, other_id, _)| {
            *other_id == value_id && *other_seq == birth.garbage_seq
        })
}

/// Decodes the first length-prefixed name after a holder's kind byte.
fn first_name(holder: &[u8]) -> Option<String> {
    let length_bytes: [u8; 2] = holder.get(1..3)?.try_into().ok()?;
    let length = usize::from(u16::from_be_bytes(length_bytes));
    let name = holder.get(3..3 + length)?;
    String::from_utf8(name.to_vec()).ok()
}
