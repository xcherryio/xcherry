// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_keyspace::row_keys::{inbox_dedup_key, run_named_key};
use dexzd_keyspace::{ColumnFamily, RunId, RunTable};
use dexzd_protocol::dex::{
    ErrorSubStatus, FlowStatus, FlowTimeoutPolicy, IdReusePolicy, StartFlowRequest,
    SubFlowReusePolicy, WorkerTarget,
};
use dexzd_protocol::dexzd::v1::{
    ChildFlowCompletedMessage, ChildFlowStartMessage, CrossFlowEndpoint, InboxDedupRecord,
    InboxMessageCommand, StoredFlowResult, StoredStepCompletionOutput, SubFlowResultRecord,
    inbox_message_command,
};

use crate::ApplyError;
use crate::command_handlers::command_replies::{error_reply, grpc_code, ok_reply};
use crate::command_handlers::flow_start::{RunStart, start_run};
use crate::command_handlers::run_cursor::{HistoryKind, RunCursor};
use crate::command_handlers::{CommandScope, HandlerResult};
use crate::cross_flow::outbox::report_terminal_to_parent;
use crate::state_access::{FlowTransaction, RunRows, read_flow_directory};
use crate::values::{ValueHolder, release_stored_value, store_value};

/// Applies a delivered cross-Flow message. A successful reply acknowledges delivery.
pub(crate) fn apply_inbox_message(
    transaction: &mut FlowTransaction<'_>,
    scope: &CommandScope<'_>,
    command: &InboxMessageCommand,
) -> Result<HandlerResult, ApplyError> {
    let Some(source) = command.source.as_ref() else {
        return Ok(reject_child_start("cross-Flow message has no source"));
    };
    if source.run_id.len() != 16 || command.message_id.len() != 16 {
        return Ok(reject_child_start(
            "cross-Flow message has an invalid identity",
        ));
    }
    match command.payload.as_ref() {
        Some(inbox_message_command::Payload::ChildStart(start)) => {
            apply_child_start(transaction, scope, command, source, start)
        }
        Some(inbox_message_command::Payload::ChildCompleted(completed)) => {
            apply_child_completed(transaction, scope, command, source, completed)
        }
        None => Ok(reject_child_start("cross-Flow message has no payload")),
    }
}

fn apply_child_start(
    transaction: &mut FlowTransaction<'_>,
    scope: &CommandScope<'_>,
    command: &InboxMessageCommand,
    source: &CrossFlowEndpoint,
    start: &ChildFlowStartMessage,
) -> Result<HandlerResult, ApplyError> {
    if start.child_flow_id != scope.flow_id() || source.tenant_id != scope.tenant_id.0 {
        return Ok(reject_child_start(
            "child start target or tenant does not match the command",
        ));
    }
    let Some(parent) = start.parent.as_ref() else {
        return Ok(reject_child_start("child start has no parent reference"));
    };
    if parent.parent.as_ref() != Some(source) {
        return Ok(reject_child_start(
            "child start source does not match its parent",
        ));
    }
    let condition = start.condition.clone().unwrap_or_default();
    let options = condition.options.clone().unwrap_or_default();
    let config = start.flow_config.clone().unwrap_or_default();
    if let Some(detail) = invalid_child_parameters(&condition, &config) {
        return Ok(reject_child_start(detail));
    }
    let directory = read_flow_directory(transaction, scope.tenant_id, scope.flow_id())?;
    for entry in &directory.runs {
        let run_id = run_id_from_directory(entry.run_id.as_slice())?;
        if inbox_is_duplicate(
            transaction,
            RunRows::new(transaction.context().flow_hash, run_id),
            source,
            &command.message_id,
        )? {
            return Ok(HandlerResult::Rejected(ok_reply()));
        }
    }
    let latest = directory.runs.last();
    let policy = SubFlowReusePolicy::try_from(options.reuse_policy)
        .unwrap_or(SubFlowReusePolicy::Unspecified);
    let is_same_request = latest.is_some_and(|entry| entry.start_request_id == start.request_id);
    let should_attach = is_same_request
        || match (latest, policy) {
            (Some(_), SubFlowReusePolicy::Attach) => true,
            (Some(_), SubFlowReusePolicy::AlwaysRestart) => false,
            (Some(entry), _) => matches!(
                FlowStatus::try_from(entry.status).unwrap_or(FlowStatus::Unspecified),
                FlowStatus::Running | FlowStatus::Completed
            ),
            (None, _) => false,
        };
    if should_attach {
        let latest = latest.expect("attach requires an existing child run");
        let run_id = run_id_from_directory(&latest.run_id)?;
        return attach_existing(transaction, scope, command, source, start, run_id);
    }
    let reuse_policy = if latest.is_some() {
        match policy {
            SubFlowReusePolicy::AlwaysRestart => IdReusePolicy::AllowTerminateIfRunning,
            _ => IdReusePolicy::AllowIfPreviousExistsAbnormally,
        }
    } else {
        IdReusePolicy::DisallowReuse
    };
    let request = StartFlowRequest {
        flow_id: start.child_flow_id.clone(),
        flow_type: condition.sub_flow_type,
        flow_timeout_seconds: options.flow_timeout_seconds,
        flow_timeout_policy: options.flow_timeout_policy,
        start_step_type: condition.start_step_type,
        step_input: condition.step_input,
        step_options: condition.step_options,
        flow_start_options: Some(dexzd_protocol::dex::FlowStartOptions {
            id_reuse_policy: reuse_policy as i32,
            attributes: options.attributes,
            timeout_handler_options: options.timeout_handler_options,
            ..dexzd_protocol::dex::FlowStartOptions::default()
        }),
        request_id: start.request_id.clone(),
    };
    let timeout_policy = FlowTimeoutPolicy::try_from(request.flow_timeout_policy)
        .unwrap_or(FlowTimeoutPolicy::Unspecified);
    let resolved_timeout_policy =
        if timeout_policy == FlowTimeoutPolicy::Unspecified && request.flow_timeout_seconds > 0 {
            FlowTimeoutPolicy::Fail
        } else {
            timeout_policy
        };
    let run_start = RunStart {
        new_run_id: &command.new_run_id,
        request: &request,
        settings: &command.settings.unwrap_or_default(),
        resolved_timeout_policy,
        flow_config: config,
        hard_delete_objects: &[],
        parent: Some(parent.clone()),
    };
    let result = start_run(transaction, scope, &run_start)?;
    if let HandlerResult::Applied(output) = &result {
        let run_id = output
            .run_id
            .ok_or_else(|| ApplyError::InvariantViolation {
                detail: "child start applied without a run ID".to_owned(),
            })?;
        put_inbox_dedup(
            transaction,
            RunRows::new(transaction.context().flow_hash, run_id),
            source,
            &command.message_id,
            1,
        );
    }
    Ok(result)
}

fn attach_existing(
    transaction: &mut FlowTransaction<'_>,
    scope: &CommandScope<'_>,
    command: &InboxMessageCommand,
    source: &CrossFlowEndpoint,
    start: &ChildFlowStartMessage,
    run_id: RunId,
) -> Result<HandlerResult, ApplyError> {
    let rows = RunRows::new(transaction.context().flow_hash, run_id);
    let Some(record) = rows.run_record(transaction)? else {
        return Ok(reject_child_start("existing child run disappeared"));
    };
    let mut cursor = RunCursor::for_new_run(rows, record);
    let parent = start.parent.clone().unwrap_or_default();
    if cursor.record.parent.as_ref() != Some(&parent)
        && !cursor.record.attached_parents.contains(&parent)
    {
        cursor.record.attached_parents.push(parent.clone());
    }
    put_inbox_dedup(transaction, rows, source, &command.message_id, cursor.seq);
    if cursor.record.status != FlowStatus::Running as i32 {
        report_terminal_to_parent(transaction, &mut cursor, parent, 0)?;
    }
    Ok(HandlerResult::Applied(cursor.finish(
        transaction,
        scope,
        ok_reply(),
        HistoryKind::BOOKKEEPING,
    )?))
}

fn apply_child_completed(
    transaction: &mut FlowTransaction<'_>,
    scope: &CommandScope<'_>,
    command: &InboxMessageCommand,
    source: &CrossFlowEndpoint,
    completed: &ChildFlowCompletedMessage,
) -> Result<HandlerResult, ApplyError> {
    let Some(parent) = completed
        .parent
        .as_ref()
        .and_then(|parent| parent.parent.as_ref())
    else {
        return Ok(reject_child_start(
            "child completion has no parent endpoint",
        ));
    };
    if parent.tenant_id != scope.tenant_id.0
        || parent.flow_id != scope.flow_id()
        || parent.run_id != scope.command.run_id
        || source.flow_id != completed.child_flow_id
        || source.run_id != completed.child_run_id
    {
        return Ok(reject_child_start(
            "child completion source or target does not match",
        ));
    }
    let Ok(run_id) = RunId::from_slice(&scope.command.run_id) else {
        return Ok(HandlerResult::Rejected(ok_reply()));
    };
    let rows = RunRows::new(transaction.context().flow_hash, run_id);
    let Some(record) = rows.run_record(transaction)? else {
        return Ok(HandlerResult::Rejected(ok_reply()));
    };
    if record.is_purging || record.status != FlowStatus::Running as i32 {
        return Ok(HandlerResult::Rejected(ok_reply()));
    }
    if inbox_is_duplicate(transaction, rows, source, &command.message_id)? {
        return Ok(HandlerResult::Rejected(ok_reply()));
    }
    let mut cursor = RunCursor::for_new_run(rows, record);
    let key = run_named_key(
        rows.flow_hash,
        run_id,
        RunTable::SubFlowResult,
        &completed.child_flow_id,
    );
    let previous: Option<SubFlowResultRecord> =
        transaction.get_message(ColumnFamily::Flow, &key, "SubFlow result")?;
    if let Some(previous) = previous.and_then(|previous| previous.result) {
        for (index, output) in previous.results.into_iter().enumerate() {
            if let Some(stored) = output.completed_step_output {
                let holder = ValueHolder::SubFlowResultOutput {
                    child_flow_id: completed.child_flow_id.clone(),
                    output_index: u32::try_from(index).unwrap_or(u32::MAX),
                };
                release_stored_value(transaction, rows, &stored, &holder, cursor.seq)?;
            }
        }
    }
    let result = completed.result.clone().unwrap_or_default();
    let mut outputs = Vec::with_capacity(result.results.len());
    for (index, output) in result.results.into_iter().enumerate() {
        let stored = if let Some(value) = output.completed_step_output.as_ref() {
            let holder = ValueHolder::SubFlowResultOutput {
                child_flow_id: completed.child_flow_id.clone(),
                output_index: u32::try_from(index).unwrap_or(u32::MAX),
            };
            match store_value(
                transaction,
                rows,
                cursor.value_limits(),
                &mut cursor.values,
                value,
                &holder,
            )? {
                Ok(stored) => Some(stored),
                Err(rejection) => return Ok(reject_child_start(rejection.message())),
            }
        } else {
            None
        };
        outputs.push(StoredStepCompletionOutput {
            completed_step_type: output.completed_step_type,
            completed_step_execution_id: output.completed_step_execution_id,
            completed_step_output: stored,
        });
    }
    transaction.put_message(
        ColumnFamily::Flow,
        key,
        &SubFlowResultRecord {
            child_run_id: completed.child_run_id.clone(),
            child_run_attempt: completed.child_run_attempt,
            result: Some(StoredFlowResult {
                flow_status: result.flow_status,
                results: outputs,
                error_type: result.error_type,
                error_message: result.error_message,
            }),
            received_seq: cursor.seq,
        },
    );
    put_inbox_dedup(transaction, rows, source, &command.message_id, cursor.seq);
    Ok(HandlerResult::Applied(cursor.finish(
        transaction,
        scope,
        ok_reply(),
        HistoryKind::BOOKKEEPING,
    )?))
}

fn invalid_child_parameters(
    condition: &dexzd_protocol::dex::SubFlowCondition,
    config: &dexzd_protocol::dex::FlowConfig,
) -> Option<&'static str> {
    let options = condition.options.clone().unwrap_or_default();
    if options.flow_start_delay_seconds != 0 {
        return Some("SubFlow start delay is unsupported");
    }
    if options.retry_policy.is_some() {
        return Some("SubFlow Flow retry policy is unsupported");
    }
    if !config
        .worker_target
        .as_ref()
        .is_some_and(|target: &WorkerTarget| target.is_pull_mode && target.address.is_empty())
    {
        return Some("SubFlow requires a pull Worker target");
    }
    if let Some(step_options) = condition.step_options.as_ref()
        && (step_options.wait_for_failure_policy == 2 || step_options.execute_failure_policy == 2)
    {
        return Some("SubFlow Step failure proceed policy is unsupported");
    }
    if options
        .timeout_handler_options
        .as_ref()
        .is_some_and(|handler| handler.failure_policy == 2)
    {
        return Some("SubFlow timeout handler failure proceed policy is unsupported");
    }
    None
}

fn inbox_is_duplicate(
    transaction: &FlowTransaction<'_>,
    rows: RunRows,
    source: &CrossFlowEndpoint,
    message_id: &[u8],
) -> Result<bool, ApplyError> {
    let key = inbox_dedup_key(rows.flow_hash, rows.run_id, &source.run_id, message_id);
    Ok(transaction.get(ColumnFamily::Flow, &key)?.is_some())
}

fn put_inbox_dedup(
    transaction: &mut FlowTransaction<'_>,
    rows: RunRows,
    source: &CrossFlowEndpoint,
    message_id: &[u8],
    seq: u64,
) {
    transaction.put_message(
        ColumnFamily::Flow,
        inbox_dedup_key(rows.flow_hash, rows.run_id, &source.run_id, message_id),
        &InboxDedupRecord { received_seq: seq },
    );
}

fn run_id_from_directory(bytes: &[u8]) -> Result<RunId, ApplyError> {
    RunId::from_slice(bytes).map_err(|_| ApplyError::InvariantViolation {
        detail: "Flow directory has a malformed run ID".to_owned(),
    })
}

fn reject_child_start(detail: impl Into<String>) -> HandlerResult {
    // The delivery owner converts a rejected start reply to an INVALID_USER_FLOW_CODE completion.
    HandlerResult::Rejected(error_reply(
        grpc_code::INVALID_ARGUMENT,
        ErrorSubStatus::Uncategorized,
        detail,
    ))
}
