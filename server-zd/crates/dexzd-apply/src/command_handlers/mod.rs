// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Routes each command to its handler.
//!
//! Handlers receive the transaction and a [`CommandScope`], write state through the
//! transaction, and return a [`HandlerOutput`]. Shared bookkeeping (run seq, history records,
//! checkpoints, window pruning) happens around the handler, not inside it.

mod command_replies;
mod command_scope;

pub(crate) use command_replies::unimplemented_reply;
pub(crate) use command_scope::{CommandScope, HandlerOutput};

use dexzd_protocol::dexzd::v1::FlowCommand;
use dexzd_protocol::dexzd::v1::flow_command::Command;

use crate::state_access::FlowTransaction;
use crate::{ApplyContext, ApplyError, ApplyOutcome, BehaviorVersion, FlowStateReader};

pub(crate) fn apply_command(
    context: &ApplyContext,
    state: &dyn FlowStateReader,
    command: &FlowCommand,
    behavior_version: BehaviorVersion,
) -> Result<ApplyOutcome, ApplyError> {
    let Some(body) = &command.command else {
        return Err(ApplyError::InvariantViolation {
            detail: "a committed Flow command has no command body".to_owned(),
        });
    };
    let transaction = FlowTransaction::new(state, *context);
    let scope = CommandScope::new(command, behavior_version);
    let output = match body {
        Command::FlowStart(_)
        | Command::FlowStop(_)
        | Command::FlowConfigUpdate(_)
        | Command::AttributesSet(_)
        | Command::ChannelPublish(_)
        | Command::ChannelMessageDelete(_)
        | Command::StepAttemptStarted(_)
        | Command::StepAttemptHeartbeat(_)
        | Command::StepAttemptFailed(_)
        | Command::StepMethodCompleted(_)
        | Command::RpcCommitted(_)
        | Command::TimersFired(_)
        | Command::TimerSkip(_)
        | Command::TimeTravel(_)
        | Command::InboxMessage(_)
        | Command::OutboxAck(_)
        | Command::HardDeleteObjectsDeleted(_)
        | Command::RunPurge(_) => HandlerOutput::rejected(unimplemented_reply(&scope)),
    };
    Ok(output.into_outcome(transaction.into_writes()))
}
