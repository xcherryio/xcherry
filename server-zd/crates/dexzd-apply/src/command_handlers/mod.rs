// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Routes each command to its handler.
//!
//! Handlers write state through the transaction. A rejected command discards every write.

mod client_writes;
pub(crate) mod command_replies;
mod command_scope;
pub(crate) mod flow_close;
pub(crate) mod flow_data;
mod flow_start;
mod rpc_committed;
pub(crate) mod run_cursor;
mod step_commands;

pub(crate) use command_replies::unimplemented_reply;
pub(crate) use command_scope::{CommandScope, HandlerOutput};

use dexzd_protocol::dexzd::v1::flow_command::Command;
use dexzd_protocol::dexzd::v1::{CommandReply, FlowCommand};

use crate::command_handlers::run_cursor::RunCursor;
use crate::command_handlers::step_commands::{StepHandlerResult, apply_step_command};
use crate::state_access::FlowTransaction;
use crate::{ApplyContext, ApplyError, ApplyOutcome, BehaviorVersion, FlowStateReader};

/// Result of a command handler.
pub(crate) enum HandlerResult {
    /// The command changed state; commit the transaction's writes.
    Applied(HandlerOutput),
    /// The command changed nothing; discard the transaction.
    Rejected(CommandReply),
}

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
    let scope = CommandScope::new(command, behavior_version);
    let mut transaction = FlowTransaction::new(state, *context);
    let result = match body {
        Command::FlowStart(start) => flow_start::apply_flow_start(&mut transaction, &scope, start)?,
        Command::StepAttemptStarted(_)
        | Command::StepAttemptHeartbeat(_)
        | Command::StepAttemptFailed(_)
        | Command::StepMethodCompleted(_) => {
            match with_run(&transaction, &scope)? {
                Err(reply) => HandlerResult::Rejected(reply),
                Ok(cursor) => match apply_step_command(&mut transaction, cursor, &scope, body)? {
                    StepHandlerResult::Done(result) => result,
                    StepHandlerResult::ApplyAsFailure(failure) => {
                        // Discard the result's partial writes and record the failure instead.
                        transaction = FlowTransaction::new(state, *context);
                        let failure_command = Command::StepAttemptFailed(failure);
                        let cursor = match with_run(&transaction, &scope)? {
                            Ok(cursor) => cursor,
                            Err(reply) => return Ok(ApplyOutcome::rejected(reply)),
                        };
                        match apply_step_command(
                            &mut transaction,
                            cursor,
                            &scope,
                            &failure_command,
                        )? {
                            StepHandlerResult::Done(result) => result,
                            StepHandlerResult::ApplyAsFailure(_) => {
                                return Err(ApplyError::InvariantViolation {
                                    detail: "a synthesized attempt failure was rejected".to_owned(),
                                });
                            }
                        }
                    }
                },
            }
        }
        _ => match with_run(&transaction, &scope)? {
            Err(reply) => HandlerResult::Rejected(reply),
            Ok(cursor) => apply_run_command(&mut transaction, cursor, &scope, body)?,
        },
    };
    Ok(match result {
        HandlerResult::Applied(output) => output.into_outcome(transaction.into_writes()),
        HandlerResult::Rejected(reply) => ApplyOutcome::rejected(reply),
    })
}

fn with_run(
    transaction: &FlowTransaction<'_>,
    scope: &CommandScope<'_>,
) -> Result<Result<RunCursor, CommandReply>, ApplyError> {
    RunCursor::open(transaction, scope)
}

fn apply_run_command(
    transaction: &mut FlowTransaction<'_>,
    cursor: RunCursor,
    scope: &CommandScope<'_>,
    body: &Command,
) -> Result<HandlerResult, ApplyError> {
    match body {
        Command::FlowStop(stop) => client_writes::apply_flow_stop(transaction, cursor, scope, stop),
        Command::FlowConfigUpdate(update) => {
            client_writes::apply_flow_config_update(transaction, cursor, scope, update)
        }
        Command::AttributesSet(set) => {
            client_writes::apply_attributes_set(transaction, cursor, scope, set)
        }
        Command::ChannelPublish(publish) => {
            client_writes::apply_channel_publish(transaction, cursor, scope, publish)
        }
        Command::ChannelMessageDelete(delete) => {
            client_writes::apply_channel_message_delete(transaction, cursor, scope, delete)
        }
        Command::TimersFired(fired) => {
            step_commands::apply_timers_fired(transaction, cursor, scope, fired)
        }
        Command::RpcCommitted(command) => {
            rpc_committed::apply_rpc_committed(transaction, cursor, scope, command)
        }
        Command::TimerSkip(_)
        | Command::TimeTravel(_)
        | Command::InboxMessage(_)
        | Command::OutboxAck(_)
        | Command::HardDeleteObjectsDeleted(_)
        | Command::RunPurge(_) => Ok(HandlerResult::Rejected(unimplemented_reply(scope))),
        Command::FlowStart(_)
        | Command::StepAttemptStarted(_)
        | Command::StepAttemptHeartbeat(_)
        | Command::StepAttemptFailed(_)
        | Command::StepMethodCompleted(_) => Err(ApplyError::InvariantViolation {
            detail: "command routed to the wrong handler".to_owned(),
        }),
    }
}
