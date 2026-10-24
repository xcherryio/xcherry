// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::fmt;

use dexzd_apply::diagnostics::SemanticPointHash;
use dexzd_apply::{ApplyOutcome, StateWrite};
use dexzd_keyspace::PartitionId;
use dexzd_protocol::dexzd::v1::{FlowCommand, LogPosition, flow_command::Command};
use prost::Message;

use crate::runtime::CommittedCommand;

/// Two applications of the same command to the same state produced different outcomes.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub struct ReplayMismatch {
    /// Which comparison failed.
    pub check: ReplayCheck,
    /// First outcome.
    pub expected: Box<ApplyOutcome>,
    /// Second outcome.
    pub actual: Box<ApplyOutcome>,
    /// The stamped command that produced the mismatch.
    pub command: Box<FlowCommand>,
    /// Partition that applied the command.
    pub partition: PartitionId,
    /// Committed log position of the command.
    pub position: LogPosition,
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
            "{check} mismatch for partition {} seq {} {} Flow {} command {}: {}",
            self.partition.0,
            self.position.seq,
            command_kind(&self.command),
            self.command.flow_id,
            hex(&self.command.command_id),
            first_difference(&self.expected, &self.actual)
        )
    }
}

pub(crate) fn compare_repeated_apply(
    command: &FlowCommand,
    partition: PartitionId,
    position: &LogPosition,
    first: &ApplyOutcome,
    second: &ApplyOutcome,
) -> Result<(), ReplayMismatch> {
    compare_outcomes(
        ReplayCheck::RepeatedApply,
        command,
        partition,
        position,
        first,
        second,
    )
}

pub(crate) fn compare_speculative_outcome(
    speculative: &ApplyOutcome,
    command: &FlowCommand,
    partition: PartitionId,
    committed: &CommittedCommand,
) -> Result<(), ReplayMismatch> {
    compare_outcomes(
        ReplayCheck::SpeculativeVersusCommitted,
        command,
        partition,
        &committed.position,
        speculative,
        &committed.outcome,
    )
}

fn compare_outcomes(
    check: ReplayCheck,
    command: &FlowCommand,
    partition: PartitionId,
    position: &LogPosition,
    expected: &ApplyOutcome,
    actual: &ApplyOutcome,
) -> Result<(), ReplayMismatch> {
    if expected.writes == actual.writes
        && expected.effects == actual.effects
        && expected.run_id == actual.run_id
        && expected.reply.encode_to_vec() == actual.reply.encode_to_vec()
    {
        return Ok(());
    }
    Err(ReplayMismatch {
        check,
        expected: Box::new(expected.clone()),
        actual: Box::new(actual.clone()),
        command: Box::new(command.clone()),
        partition,
        position: position.clone(),
    })
}

fn first_difference(expected: &ApplyOutcome, actual: &ApplyOutcome) -> String {
    for (index, (first, second)) in expected.writes.iter().zip(&actual.writes).enumerate() {
        if first != second {
            let first_key = write_key(first);
            let second_key = write_key(second);
            if first_key != second_key {
                return format!("write {index} key {first_key} versus {second_key}");
            }
            if let (
                StateWrite::Put {
                    value: first_value, ..
                },
                StateWrite::Put {
                    value: second_value,
                    ..
                },
            ) = (first, second)
            {
                return format!(
                    "write {index} at {first_key} differs: {}",
                    value_difference(first_value, second_value)
                );
            }
            return format!("write {index} at {first_key} changes operation type");
        }
    }
    if expected.writes.len() != actual.writes.len() {
        let extra = expected
            .writes
            .get(actual.writes.len())
            .or_else(|| actual.writes.get(expected.writes.len()));
        return format!(
            "write count {} versus {} at {}",
            expected.writes.len(),
            actual.writes.len(),
            extra.map_or_else(|| "unknown key".to_owned(), write_key)
        );
    }
    if expected.effects != actual.effects {
        return format!(
            "effects differ: {:?} versus {:?}",
            expected.effects, actual.effects
        );
    }
    if expected.run_id != actual.run_id {
        return format!(
            "run ID differs: {:?} versus {:?}",
            expected.run_id, actual.run_id
        );
    }
    "reply bytes differ".to_owned()
}

fn value_difference(first: &[u8], second: &[u8]) -> String {
    if let Some(offset) = first
        .iter()
        .zip(second)
        .position(|(first_byte, second_byte)| first_byte != second_byte)
    {
        return format!(
            "byte {offset} is {:02x} versus {:02x}; lengths {} versus {}",
            first[offset],
            second[offset],
            first.len(),
            second.len()
        );
    }
    format!("lengths {} versus {}", first.len(), second.len())
}

fn write_key(write: &StateWrite) -> String {
    match write {
        StateWrite::Put {
            column_family, key, ..
        }
        | StateWrite::Delete { column_family, key } => format!("{column_family:?}/{}", hex(key)),
        StateWrite::DeleteRange {
            column_family,
            start,
            end,
        } => format!("{column_family:?}/{}..{}", hex(start), hex(end)),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn command_kind(command: &FlowCommand) -> &'static str {
    match &command.command {
        None => "None",
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

pub(crate) fn compare_semantic_point_hash(
    expected: &[u8; 16],
    reconstructed: &SemanticPointHash,
    command: &FlowCommand,
    position: &LogPosition,
) -> Result<(), String> {
    if expected == &reconstructed.state_hash {
        return Ok(());
    }
    Err(format!(
        "time-travel reconstruction differs for Flow {} command {} at log seq {} semantic key ({}, {}, {}): expected hash {}, actual hash {}",
        command.flow_id,
        hex(&command.command_id),
        position.seq,
        reconstructed.run_id,
        reconstructed.run_attempt,
        reconstructed.run_seq,
        hex(expected),
        hex(&reconstructed.state_hash)
    ))
}

#[cfg(test)]
mod tests {
    use dexzd_apply::diagnostics::SemanticPointHash;
    use dexzd_keyspace::RunId;
    use dexzd_protocol::dexzd::v1::{FlowCommand, LogPosition};

    use super::compare_semantic_point_hash;

    #[test]
    fn corrupted_reconstruction_names_the_command_and_semantic_key() {
        let reconstructed = SemanticPointHash {
            run_id: RunId([9; 16]),
            run_attempt: 2,
            run_seq: 7,
            state_hash: [2; 16],
            window_start_seq: 1,
            genesis_seq: 1,
        };
        let command = FlowCommand {
            flow_id: "order-flow".to_owned(),
            command_id: vec![3; 16],
            ..FlowCommand::default()
        };
        let error = compare_semantic_point_hash(
            &[1; 16],
            &reconstructed,
            &command,
            &LogPosition {
                seq: 11,
                ..LogPosition::default()
            },
        )
        .expect_err("corrupted reconstruction differs");
        assert!(error.contains("order-flow command 0303"));
        assert!(error.contains("semantic key ("));
        assert!(error.contains(", 2, 7)"));
    }
}
