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
pub(crate) mod run_cursor;
pub(crate) mod step_commands;

pub(crate) use command_replies::unimplemented_reply;
pub(crate) use command_scope::{CommandScope, HandlerOutput, ReplayedCommandOutput};

use dexzd_protocol::dex::ErrorSubStatus;
use dexzd_protocol::dexzd::v1::flow_command::Command;
use dexzd_protocol::dexzd::v1::{
    CommandReply, FlowCommand, StepAttemptFailedCommand, StepAttemptFailureKind,
};

use crate::command_handlers::command_replies::{error_reply, grpc_code};
use crate::command_handlers::run_cursor::RunCursor;
use crate::command_handlers::step_commands::{StepHandlerResult, apply_step_command};
use crate::state_access::{FlowTransaction, RunRows};
use crate::values::{active_run_storage_bytes, total_run_storage_bytes};
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
    let scope = CommandScope::new(command, behavior_version);
    Ok(apply_in_scope(context, state, &scope)?.0)
}

/// Replays a recorded command without recording history. Returns its writes, or `None` when rejected.
pub(crate) fn apply_replayed_command(
    context: &ApplyContext,
    state: &dyn FlowStateReader,
    command: &FlowCommand,
    behavior_version: BehaviorVersion,
) -> Result<Option<Vec<crate::StateWrite>>, ApplyError> {
    let mut scope = CommandScope::new(command, behavior_version);
    scope.is_replay = true;
    let (outcome, is_applied) = apply_in_scope(context, state, &scope)?;
    Ok(is_applied.then_some(outcome.writes))
}

/// Applies the scope's command; the flag tells whether it was applied or rejected.
fn apply_in_scope(
    context: &ApplyContext,
    state: &dyn FlowStateReader,
    scope: &CommandScope<'_>,
) -> Result<(ApplyOutcome, bool), ApplyError> {
    let command = scope.command;
    let behavior_version = scope.behavior_version;
    let Some(body) = &command.command else {
        return Err(ApplyError::InvariantViolation {
            detail: "a committed Flow command has no command body".to_owned(),
        });
    };
    let mut transaction = FlowTransaction::new(state, *context);
    let mut is_failure_reapply = false;
    let result = match body {
        Command::FlowStart(start) => flow_start::apply_flow_start(&mut transaction, scope, start)?,
        Command::StepAttemptStarted(_)
        | Command::StepAttemptHeartbeat(_)
        | Command::StepAttemptFailed(_)
        | Command::StepMethodCompleted(_) => {
            match with_run(&transaction, scope)? {
                Err(reply) => HandlerResult::Rejected(reply),
                Ok(cursor) => match apply_step_command(&mut transaction, cursor, scope, body)? {
                    StepHandlerResult::Done(result) => result,
                    StepHandlerResult::ApplyAsFailure(failure) => {
                        // Discard the result's partial writes and record the failure instead.
                        is_failure_reapply = true;
                        transaction = FlowTransaction::new(state, *context);
                        let failure_command = Command::StepAttemptFailed(failure);
                        // History records the failure, so replay applies what was applied.
                        let recorded_failure = FlowCommand {
                            command: Some(failure_command.clone()),
                            ..command.clone()
                        };
                        let mut failure_scope =
                            CommandScope::new(&recorded_failure, behavior_version);
                        failure_scope.is_replay = scope.is_replay;
                        let cursor = match with_run(&transaction, &failure_scope)? {
                            Ok(cursor) => cursor,
                            Err(reply) => return Ok((ApplyOutcome::rejected(reply), false)),
                        };
                        match apply_step_command(
                            &mut transaction,
                            cursor,
                            &failure_scope,
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
        _ => match with_run(&transaction, scope)? {
            Err(reply) => HandlerResult::Rejected(reply),
            Ok(cursor) => apply_run_command(&mut transaction, cursor, scope, body)?,
        },
    };
    Ok(match result {
        HandlerResult::Applied(output) => {
            if should_enforce_run_state_limit(body)
                && !is_failure_reapply
                && !scope.is_replay
                && let Some(run_id) = output.run_id
            {
                let rows = RunRows::new(context.flow_hash, run_id);
                let record = rows.run_record(&transaction)?.ok_or_else(|| {
                    ApplyError::InvariantViolation {
                        detail: "applied command did not write its run record".to_owned(),
                    }
                })?;
                let usage =
                    record
                        .storage
                        .as_ref()
                        .ok_or_else(|| ApplyError::InvariantViolation {
                            detail: "applied command did not account for run storage".to_owned(),
                        })?;
                let total_bytes = total_run_storage_bytes(usage);
                let max_bytes = record.settings.unwrap_or_default().max_run_state_bytes;
                let previous_transaction = FlowTransaction::new(state, *context);
                let previous_active = rows
                    .run_record(&previous_transaction)?
                    .and_then(|previous| previous.storage)
                    .as_ref()
                    .map_or(0, active_run_storage_bytes);
                if total_bytes > max_bytes && active_run_storage_bytes(usage) > previous_active {
                    let detail = format!(
                        "run state of {total_bytes} bytes exceeds the maximum of {max_bytes} bytes"
                    );
                    if let Some(failure) = storage_limit_attempt_failure(body, &detail) {
                        let mut failure_command = command.clone();
                        failure_command.command = Some(Command::StepAttemptFailed(failure));
                        let mut failure_scope =
                            CommandScope::new(&failure_command, behavior_version);
                        failure_scope.is_replay = scope.is_replay;
                        return apply_in_scope(context, state, &failure_scope);
                    }
                    return Ok((
                        ApplyOutcome::rejected(error_reply(
                            grpc_code::RESOURCE_EXHAUSTED,
                            ErrorSubStatus::Uncategorized,
                            detail,
                        )),
                        false,
                    ));
                }
            }
            (output.into_outcome(transaction.into_writes()), true)
        }
        HandlerResult::Rejected(reply) => (ApplyOutcome::rejected(reply), false),
    })
}

fn should_enforce_run_state_limit(body: &Command) -> bool {
    match body {
        Command::AttributesSet(command) => command.request.as_ref().is_some_and(|request| {
            request
                .attributes
                .iter()
                .any(|attribute| !crate::values::is_null_value(attribute.value.as_ref()))
        }),
        Command::FlowStart(_)
        | Command::ChannelPublish(_)
        | Command::StepAttemptHeartbeat(_)
        | Command::StepMethodCompleted(_) => true,
        _ => false,
    }
}

fn storage_limit_attempt_failure(body: &Command, detail: &str) -> Option<StepAttemptFailedCommand> {
    let (invocation, attempt) = match body {
        Command::StepAttemptHeartbeat(command) => (command.invocation.clone(), command.attempt),
        Command::StepMethodCompleted(command) => (command.invocation.clone(), command.attempt),
        _ => return None,
    };
    Some(StepAttemptFailedCommand {
        invocation,
        attempt,
        kind: StepAttemptFailureKind::ValueTooLarge as i32,
        server_detail: detail.to_owned(),
        is_non_retryable: true,
        ..StepAttemptFailedCommand::default()
    })
}

fn with_run(
    transaction: &FlowTransaction<'_>,
    scope: &CommandScope<'_>,
) -> Result<Result<RunCursor, CommandReply>, ApplyError> {
    RunCursor::open(transaction, scope)
}

pub(crate) fn apply_run_command(
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
        Command::TimeTravel(travel) => {
            crate::history::apply_time_travel(transaction, cursor, scope, travel)
        }
        Command::TimerSkip(skip) => {
            crate::steps::wait_conditions::apply_timer_skip(transaction, cursor, scope, skip)
        }
        Command::RpcCommitted(_)
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
