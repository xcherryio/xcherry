// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! StartFlow (plan section 9.1): ID reuse, run creation, initial Attributes, the start Step,
//! and the Flow timeout.

use dexzd_keyspace::row_keys::{active_work_index_key, tenant_run_index_key};
use dexzd_keyspace::{ColumnFamily, IndexKind, RunId};
use dexzd_protocol::dex::{
    ErrorSubStatus, FlowConfig, FlowStatus, FlowTimeoutPolicy, IdReusePolicy, StartFlowRequest,
    StopFlowRequest, StopType,
};
use dexzd_protocol::dexzd::v1::{
    AttemptRecord, CommandReply, FlowCommand, FlowDirectoryRun, FlowStartCommand, FlowStopCommand,
    FlowTimeoutTimerId, HardDeleteObjectReference, RunRecord, RunSettings, TimerId, command_reply,
    flow_command::Command, timer_id,
};

use prost::Message;

use crate::ApplyError;
use crate::command_handlers::command_replies::{error_reply, grpc_code};
use crate::command_handlers::flow_close::{FlowClosing, close_flow};
use crate::command_handlers::flow_data::apply_attribute_writes;
use crate::command_handlers::run_cursor::{HistoryKind, RunCursor};
use crate::command_handlers::{CommandScope, HandlerResult};
use crate::projections::history_events;
use crate::state_access::{FlowTransaction, RunRows, read_flow_directory, write_flow_directory};
use crate::steps::{
    NewStepExecution, STARTING_STEP_FROM_STEP_EXECUTION_ID, arm_timer, create_step_execution,
};
use crate::values::{ValueHolder, ValueRejection, hold_stored_value};

/// Everything needed to start a run, from a client StartFlow or a child Flow start.
pub(crate) struct RunStart<'start> {
    pub(crate) new_run_id: &'start [u8],
    pub(crate) request: &'start StartFlowRequest,
    pub(crate) settings: &'start RunSettings,
    pub(crate) resolved_timeout_policy: FlowTimeoutPolicy,
    pub(crate) flow_config: FlowConfig,
    pub(crate) hard_delete_objects: &'start [HardDeleteObjectReference],
    pub(crate) parent: Option<dexzd_protocol::dexzd::v1::ParentFlowReference>,
}

pub(crate) fn apply_flow_start(
    transaction: &mut FlowTransaction<'_>,
    scope: &CommandScope<'_>,
    command: &FlowStartCommand,
) -> Result<HandlerResult, ApplyError> {
    let request = command.request.clone().unwrap_or_default();
    let mut flow_config = request
        .flow_start_options
        .as_ref()
        .and_then(|options| options.flow_config_override.clone())
        .unwrap_or_default();
    flow_config.worker_target = command.worker_target.clone();
    let start = RunStart {
        new_run_id: &command.new_run_id,
        request: &request,
        settings: &command.settings.unwrap_or_default(),
        resolved_timeout_policy: FlowTimeoutPolicy::try_from(command.resolved_timeout_policy)
            .unwrap_or(FlowTimeoutPolicy::Unspecified),
        flow_config,
        hard_delete_objects: &command.hard_delete_objects,
        parent: None,
    };
    start_run(transaction, scope, &start)
}

/// Applies ID reuse against the Flow directory and, when allowed, creates and starts the run.
pub(crate) fn start_run(
    transaction: &mut FlowTransaction<'_>,
    scope: &CommandScope<'_>,
    start: &RunStart<'_>,
) -> Result<HandlerResult, ApplyError> {
    let request = start.request;
    let Ok(run_id) = RunId::from_slice(start.new_run_id) else {
        return Err(ApplyError::InvariantViolation {
            detail: "FlowStart has no 16-byte run ID".to_owned(),
        });
    };
    let directory = read_flow_directory(transaction, scope.tenant_id, scope.flow_id())?;
    let options = request.flow_start_options.clone().unwrap_or_default();
    let policy =
        IdReusePolicy::try_from(options.id_reuse_policy).unwrap_or(IdReusePolicy::Unspecified);
    let latest = directory.runs.last();
    let latest_status =
        latest.map(|run| FlowStatus::try_from(run.status).unwrap_or(FlowStatus::Unspecified));
    let is_latest_running = latest_status == Some(FlowStatus::Running);
    let is_conflict = match policy {
        IdReusePolicy::Unspecified | IdReusePolicy::DisallowReuse => latest.is_some(),
        IdReusePolicy::AllowIfNoRunning => is_latest_running,
        IdReusePolicy::AllowIfPreviousExistsAbnormally => {
            matches!(
                latest_status,
                Some(FlowStatus::Running | FlowStatus::Completed)
            )
        }
        IdReusePolicy::AllowTerminateIfRunning => false,
    };
    if let (true, Some(latest)) = (is_conflict, latest) {
        let is_same_request = options
            .flow_already_started_options
            .as_ref()
            .is_some_and(|already| already.ignore_already_started_error)
            && latest.start_request_id == request.request_id;
        if is_same_request {
            return Ok(HandlerResult::Rejected(CommandReply {
                reply: Some(command_reply::Reply::StartFlow(
                    dexzd_protocol::dexzd::v1::StartFlowReply {
                        run_id: latest.run_id.clone(),
                        is_existing_run: true,
                    },
                )),
            }));
        }
        return Ok(HandlerResult::Rejected(error_reply(
            grpc_code::ALREADY_EXISTS,
            ErrorSubStatus::FlowAlreadyStarted,
            format!("Flow {} is already started", scope.flow_id()),
        )));
    }
    let mut terminated_run_effects = Vec::new();
    if policy == IdReusePolicy::AllowTerminateIfRunning
        && is_latest_running
        && let Some(latest) = latest
    {
        terminated_run_effects = terminate_previous_run(transaction, scope, &latest.run_id)?;
    }
    let flow_hash = transaction.context().flow_hash;
    let rows = RunRows::new(flow_hash, run_id);
    let timeout_seconds = request.flow_timeout_seconds;
    let record = RunRecord {
        tenant_id: scope.tenant_id.0,
        flow_id: scope.flow_id().to_owned(),
        run_id: run_id.as_bytes().to_vec(),
        flow_type: request.flow_type.clone(),
        start_request_id: request.request_id.clone(),
        started_at_micros: scope.now_micros,
        settings: Some(*start.settings),
        status: FlowStatus::Running as i32,
        current_attempt: 1,
        next_run_seq: 1,
        next_event_id: 1,
        flow_config: Some(start.flow_config.clone()),
        timeout_deadline_micros: if timeout_seconds > 0 {
            scope
                .now_micros
                .saturating_add(i64::from(timeout_seconds) * 1_000_000)
        } else {
            0
        },
        timeout_policy: start.resolved_timeout_policy as i32,
        timeout_handler_options: options.timeout_handler_options.clone(),
        parent: start.parent.clone(),
        window_start_seq: 1,
        ..RunRecord::default()
    };
    let mut cursor = RunCursor::for_new_run(rows, record);
    rows.put_attempt_record(
        transaction,
        &AttemptRecord {
            attempt: 1,
            parent_attempt: 0,
            parent_seq: 0,
            start_seq: cursor.seq,
            created_at_micros: scope.now_micros,
            reason: String::new(),
            checkpoint_seqs: Vec::new(),
        },
    );
    if let Err(rejection) = apply_attribute_writes(
        transaction,
        &mut cursor,
        &options.attributes,
        start.hard_delete_objects,
        None,
    )? {
        return Ok(HandlerResult::Rejected(error_reply(
            grpc_code::INVALID_ARGUMENT,
            ErrorSubStatus::Uncategorized,
            rejection.message(),
        )));
    }
    pin_genesis_attributes(transaction, &cursor, &options.attributes)?;
    // Without a start Step type the Flow waits for RPCs or a close, as in the Go Server.
    if request.start_step_type.is_empty() {
        // The unused input still reaches history, so it must respect the value limit.
        if let Some(input) = &request.step_input {
            let size_bytes = u64::try_from(input.encoded_len()).unwrap_or(u64::MAX);
            let max_value_bytes = cursor.value_limits().max_value_bytes;
            if size_bytes > max_value_bytes {
                let rejection = ValueRejection::TooLarge {
                    size_bytes,
                    max_value_bytes,
                };
                return Ok(HandlerResult::Rejected(error_reply(
                    grpc_code::INVALID_ARGUMENT,
                    ErrorSubStatus::Uncategorized,
                    rejection.message(),
                )));
            }
        }
    } else {
        let start_step = NewStepExecution {
            step_type: &request.start_step_type,
            input: request.step_input.as_ref(),
            options: request.step_options.as_ref(),
            from_step_execution_id: STARTING_STEP_FROM_STEP_EXECUTION_ID,
            is_timeout_handler: false,
        };
        let start_step_id =
            match create_step_execution(transaction, &mut cursor, scope.now_micros, &start_step)? {
                Ok(step_execution_id) => step_execution_id,
                Err(rejection) => {
                    return Ok(HandlerResult::Rejected(error_reply(
                        grpc_code::INVALID_ARGUMENT,
                        ErrorSubStatus::Uncategorized,
                        rejection.message(),
                    )));
                }
            };
        if let Some(step) = cursor.rows.step_execution(transaction, &start_step_id)?
            && let Some(input) = &step.input
        {
            hold_stored_value(transaction, cursor.rows, input, &ValueHolder::Genesis)?;
        }
    }
    if cursor.record.timeout_deadline_micros > 0 {
        let deadline = cursor.record.timeout_deadline_micros;
        arm_timer(
            transaction,
            &mut cursor,
            &TimerId {
                run_attempt: 1,
                kind: Some(timer_id::Kind::FlowTimeout(FlowTimeoutTimerId {})),
            },
            deadline,
        );
    }
    // Re-read: terminating the previous run updated its directory entry.
    let mut directory = read_flow_directory(transaction, scope.tenant_id, scope.flow_id())?;
    directory.runs.push(FlowDirectoryRun {
        run_id: run_id.as_bytes().to_vec(),
        status: FlowStatus::Running as i32,
        started_at_micros: scope.now_micros,
        closed_at_micros: 0,
        start_request_id: request.request_id.clone(),
    });
    write_flow_directory(transaction, scope.tenant_id, scope.flow_id(), &directory);
    let partition = transaction.context().partition;
    transaction.put(
        ColumnFamily::PartitionIndex,
        active_work_index_key(partition, flow_hash, run_id),
        Vec::new(),
    );
    transaction.put(
        ColumnFamily::PartitionIndex,
        tenant_run_index_key(
            partition,
            IndexKind::TenantRuns,
            scope.tenant_id,
            flow_hash,
            run_id,
        ),
        Vec::new(),
    );
    history_events::record_flow_started(&mut cursor, request, &start.flow_config);
    let reply = CommandReply {
        reply: Some(command_reply::Reply::StartFlow(
            history_events::start_flow_reply(&cursor, false),
        )),
    };
    let mut output = cursor.finish(transaction, scope, reply, HistoryKind::SEMANTIC)?;
    terminated_run_effects.extend(output.effects);
    output.effects = terminated_run_effects;
    Ok(HandlerResult::Applied(output))
}

/// Initial Attributes stay readable for the whole run so "back to the beginning" always works
/// (plan D12).
fn pin_genesis_attributes(
    transaction: &mut FlowTransaction<'_>,
    cursor: &RunCursor,
    writes: &[dexzd_protocol::dex::AttributeWrite],
) -> Result<(), ApplyError> {
    for write in writes {
        if let Some(record) = cursor.rows.attribute(transaction, &write.key)?
            && let Some(stored) = &record.value
        {
            hold_stored_value(transaction, cursor.rows, stored, &ValueHolder::Genesis)?;
        }
    }
    Ok(())
}

/// Close reason of a run that StartFlow terminated for ID reuse.
const ID_REUSE_TERMINATION_REASON: &str = "terminated by StartFlow with ALLOW_TERMINATE_IF_RUNNING";

/// ID reuse with ALLOW_TERMINATE_IF_RUNNING: terminates the running run in the same command.
fn terminate_previous_run(
    transaction: &mut FlowTransaction<'_>,
    scope: &CommandScope<'_>,
    run_id_bytes: &[u8],
) -> Result<Vec<crate::ApplyEffect>, ApplyError> {
    let Ok(run_id) = RunId::from_slice(run_id_bytes) else {
        return Err(ApplyError::InvariantViolation {
            detail: "Flow directory has a malformed run ID".to_owned(),
        });
    };
    let rows = RunRows::new(transaction.context().flow_hash, run_id);
    let Some(record) = rows.run_record(transaction)? else {
        return Ok(Vec::new());
    };
    // The old run's history must replay as its own termination, not as the successor's start.
    let termination = id_reuse_termination_command(scope, run_id_bytes);
    let previous_run_scope = CommandScope {
        command: &termination,
        ..*scope
    };
    let mut cursor = RunCursor::for_new_run(rows, record);
    close_flow(
        transaction,
        &mut cursor,
        &previous_run_scope,
        FlowClosing::terminated(ID_REUSE_TERMINATION_REASON),
    )?;
    let reply = crate::command_handlers::command_replies::ok_reply();
    // The run's own history records the termination; its effects join this command's outcome.
    let output = cursor.finish(
        transaction,
        &previous_run_scope,
        reply,
        HistoryKind::SEMANTIC,
    )?;
    Ok(output.effects)
}

/// A TERMINATE stop of the previous run with the start's stamp; applying it alone produces
/// the same old-run state as the ID-reuse termination.
fn id_reuse_termination_command(scope: &CommandScope<'_>, run_id_bytes: &[u8]) -> FlowCommand {
    FlowCommand {
        run_id: run_id_bytes.to_vec(),
        expected_run_attempt: 0,
        command: Some(Command::FlowStop(FlowStopCommand {
            request: Some(StopFlowRequest {
                flow_id: scope.flow_id().to_owned(),
                stop_type: StopType::Terminate as i32,
                reason: ID_REUSE_TERMINATION_REASON.to_owned(),
                ..StopFlowRequest::default()
            }),
        })),
        ..scope.command.clone()
    }
}
