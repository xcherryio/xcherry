// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;
use std::fmt;
use std::time::{Duration, Instant};

use dexzd_protocol::dexzd::v1::{FlowCommand, command_reply, flow_command::Command};

use crate::apply_checker::ApplyChecker;
use crate::invariants::InvariantViolation;
use crate::model::CommandGenerator;

/// Replays allowed while shrinking one failure.
const SHRINK_REPLAY_BUDGET: usize = 200;
const SHRINK_TIME_BUDGET: Duration = Duration::from_secs(30);

/// What one seed exercised.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SimulationSummary {
    /// Commands proposed.
    pub commands: usize,
    /// Commands that changed state, by command kind.
    pub applied_by_kind: BTreeMap<&'static str, usize>,
    /// Commands rejected without writes, by command kind.
    pub rejected_by_kind: BTreeMap<&'static str, usize>,
    /// Invalid client inputs rejected for their value size, by invalid input class.
    pub size_rejections: BTreeMap<&'static str, usize>,
    /// Rows in the final state.
    pub final_row_count: usize,
}

impl SimulationSummary {
    /// Applied commands of `kind`.
    pub fn applied(&self, kind: &str) -> usize {
        self.applied_by_kind.get(kind).copied().unwrap_or(0)
    }

    /// Adds another seed's counts to this summary.
    pub fn merge(&mut self, other: &Self) {
        self.commands += other.commands;
        self.final_row_count += other.final_row_count;
        for (kind, count) in &other.applied_by_kind {
            *self.applied_by_kind.entry(kind).or_default() += count;
        }
        for (kind, count) in &other.rejected_by_kind {
            *self.rejected_by_kind.entry(kind).or_default() += count;
        }
        for (class, count) in &other.size_rejections {
            *self.size_rejections.entry(class).or_default() += count;
        }
    }
}

/// A seed that broke an invariant, with a shrunk command sequence that still breaks it.
#[derive(Debug, Clone)]
pub struct SimulationFailure {
    /// The failing seed.
    pub seed: u64,
    /// Zero-based index of the command after which the invariant broke.
    pub command_index: usize,
    /// The broken invariant.
    pub violation: InvariantViolation,
    /// The shortest command sequence found that breaks the same invariant; its last command
    /// is the one that broke it.
    pub minimal_reproduction: Vec<FlowCommand>,
}

impl fmt::Display for SimulationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            formatter,
            "seed {} broke an invariant at command index {}: {}",
            self.seed, self.command_index, self.violation
        )?;
        writeln!(
            formatter,
            "reproduce with: DEXZD_SIMULATION_SEED={} DEXZD_SIMULATION_COMMANDS={} cargo test -p \
             dexzd-simulation",
            self.seed,
            self.command_index + 1,
        )?;
        writeln!(
            formatter,
            "reproduction ({} commands):",
            self.minimal_reproduction.len()
        )?;
        for (index, command) in self.minimal_reproduction.iter().enumerate() {
            writeln!(formatter, "[{index}] {command:?}")?;
        }
        Ok(())
    }
}

/// Runs `command_count` generated commands for `seed` and checks every invariant after each.
///
/// Reports a failure before shrinking it, then returns the failure with a shrunk reproduction.
/// The report callback receives the original sequence and may persist it before shrinking starts.
///
/// Returns what the seed exercised, or the first failure with a shrunk reproduction.
pub fn run_seed(
    seed: u64,
    command_count: usize,
    mut report_failure: impl FnMut(&SimulationFailure),
) -> Result<SimulationSummary, Box<SimulationFailure>> {
    let mut checker = ApplyChecker::new();
    let mut generator = CommandGenerator::new(seed);
    let mut summary = SimulationSummary::default();
    let mut history = Vec::with_capacity(command_count);
    for command_index in 0..command_count {
        let command = generator.next_command(&checker).map_err(|violation| {
            failure(
                seed,
                command_index,
                violation,
                history.clone(),
                &mut report_failure,
            )
        })?;
        history.push(command.clone());
        let outcome = checker.apply_and_check(&command).map_err(|violation| {
            failure(
                seed,
                command_index,
                violation,
                history.clone(),
                &mut report_failure,
            )
        })?;
        let kind = command_kind(&command);
        let is_error_reply = matches!(outcome.reply.reply, Some(command_reply::Reply::Error(_)));
        let counts = if outcome.writes.is_empty() || is_error_reply {
            &mut summary.rejected_by_kind
        } else {
            &mut summary.applied_by_kind
        };
        *counts.entry(kind).or_default() += 1;
        summary.commands += 1;
    }
    summary.size_rejections = checker.size_rejections().clone();
    summary.final_row_count = checker.state().row_count();
    Ok(summary)
}

/// Applies `commands` in order to an empty state with every check.
///
/// Returns the index and violation of the first failing command, or `None`.
pub fn replay_commands(commands: &[FlowCommand]) -> Option<(usize, InvariantViolation)> {
    let mut checker = ApplyChecker::new();
    for (index, command) in commands.iter().enumerate() {
        if let Err(violation) = checker.apply_and_check(command) {
            return Some((index, violation));
        }
    }
    None
}

fn failure(
    seed: u64,
    command_index: usize,
    violation: InvariantViolation,
    history: Vec<FlowCommand>,
    report_failure: &mut impl FnMut(&SimulationFailure),
) -> Box<SimulationFailure> {
    let mut failure = SimulationFailure {
        seed,
        command_index,
        violation,
        minimal_reproduction: history,
    };
    report_failure(&failure);
    failure.minimal_reproduction = shrink(
        std::mem::take(&mut failure.minimal_reproduction),
        failure.violation.invariant,
    );
    Box::new(failure)
}

/// Removes chunks of commands, halving the chunk size, while the same invariant still breaks.
fn shrink(mut commands: Vec<FlowCommand>, invariant: &str) -> Vec<FlowCommand> {
    let started_at = Instant::now();
    let mut replays_left = SHRINK_REPLAY_BUDGET;
    let mut chunk_size = commands.len().div_ceil(2);
    while chunk_size > 0 && replays_left > 0 && started_at.elapsed() < SHRINK_TIME_BUDGET {
        let mut start = 0;
        while start < commands.len()
            && replays_left > 0
            && started_at.elapsed() < SHRINK_TIME_BUDGET
        {
            let end = (start + chunk_size).min(commands.len());
            let mut candidate = commands[..start].to_vec();
            candidate.extend_from_slice(&commands[end..]);
            replays_left -= 1;
            match replay_commands(&candidate) {
                Some((index, violation)) if violation.invariant == invariant => {
                    candidate.truncate(index + 1);
                    commands = candidate;
                }
                _ => start = end,
            }
        }
        chunk_size /= 2;
    }
    commands
}

/// Short name of the command kind for summaries.
pub fn command_kind(command: &FlowCommand) -> &'static str {
    match &command.command {
        None => "none",
        Some(Command::FlowStart(_)) => "FlowStart",
        Some(Command::FlowStop(_)) => "FlowStop",
        Some(Command::FlowConfigUpdate(_)) => "FlowConfigUpdate",
        Some(Command::AttributesSet(_)) => "AttributesSet",
        Some(Command::ChannelPublish(_)) => "ChannelPublish",
        Some(Command::ChannelMessageDelete(_)) => "ChannelMessageDelete",
        Some(Command::StepAttemptStarted(_)) => "StepAttemptStarted",
        Some(Command::StepAttemptHeartbeat(_)) => "StepAttemptHeartbeat",
        Some(Command::StepAttemptFailed(_)) => "StepAttemptFailed",
        Some(Command::StepMethodCompleted(_)) => "StepMethodCompleted",
        Some(Command::RpcCommitted(_)) => "RpcCommitted",
        Some(Command::TimersFired(_)) => "TimersFired",
        Some(Command::TimerSkip(_)) => "TimerSkip",
        Some(Command::TimeTravel(_)) => "TimeTravel",
        Some(Command::InboxMessage(_)) => "InboxMessage",
        Some(Command::OutboxAck(_)) => "OutboxAck",
        Some(Command::HardDeleteObjectsDeleted(_)) => "HardDeleteObjectsDeleted",
        Some(Command::RunPurge(_)) => "RunPurge",
    }
}

#[cfg(test)]
mod tests {
    use super::{FlowCommand, InvariantViolation, failure};

    #[test]
    fn reports_seed_failure_before_shrinking() {
        let mut initial_report = None;
        let failure = failure(
            42,
            7,
            InvariantViolation::new("test-invariant", "broken"),
            vec![FlowCommand::default()],
            &mut |failure| initial_report = Some(failure.to_string()),
        );
        let initial_report = initial_report.expect("failure callback must run");
        assert!(initial_report.contains("seed 42 broke an invariant at command index 7"));
        assert!(initial_report.contains("reproduce with: DEXZD_SIMULATION_SEED=42"));
        assert!(initial_report.contains("reproduction (1 commands)"));
        assert_eq!(failure.seed, 42);
    }
}
