// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::fmt;

use dexzd_apply::ApplyOutcome;

use crate::commit::CommittedCommand;

/// Two applications of the same command to the same state produced different outcomes.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub struct ReplayMismatch {
    /// Which comparison failed.
    pub check: ReplayCheck,
    /// First outcome.
    pub expected: Box<ApplyOutcome>,
    /// Second outcome.
    pub actual: Box<ApplyOutcome>,
}

/// The replay comparison that detected a mismatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayCheck {
    /// The applier applied the command twice to committed state.
    RepeatedApply,
    /// The runtime's speculative outcome differs from the committed outcome.
    SpeculativeVersusCommitted,
}

impl fmt::Display for ReplayMismatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let check = match self.check {
            ReplayCheck::RepeatedApply => "repeated apply",
            ReplayCheck::SpeculativeVersusCommitted => "speculative versus committed apply",
        };
        write!(
            formatter,
            "{check} produced different outcomes: {} versus {} writes, {} versus {} effects",
            self.expected.writes.len(),
            self.actual.writes.len(),
            self.expected.effects.len(),
            self.actual.effects.len()
        )
    }
}

pub(crate) fn compare_repeated_apply(
    first: &ApplyOutcome,
    second: &ApplyOutcome,
) -> Result<(), ReplayMismatch> {
    compare_outcomes(ReplayCheck::RepeatedApply, first, second)
}

pub(crate) fn compare_speculative_outcome(
    speculative: &ApplyOutcome,
    committed: &CommittedCommand,
) -> Result<(), ReplayMismatch> {
    compare_outcomes(
        ReplayCheck::SpeculativeVersusCommitted,
        speculative,
        &committed.outcome,
    )
}

fn compare_outcomes(
    check: ReplayCheck,
    expected: &ApplyOutcome,
    actual: &ApplyOutcome,
) -> Result<(), ReplayMismatch> {
    if expected == actual {
        return Ok(());
    }
    Err(ReplayMismatch {
        check,
        expected: Box::new(expected.clone()),
        actual: Box::new(actual.clone()),
    })
}
