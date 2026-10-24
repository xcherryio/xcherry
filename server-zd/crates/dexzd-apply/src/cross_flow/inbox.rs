// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_keyspace::row_keys::inbox_dedup_key;
use dexzd_keyspace::{ColumnFamily, RunId};
use dexzd_protocol::dex::{
    ErrorSubStatus, FlowStatus, FlowTimeoutPolicy, IdReusePolicy, StartFlowRequest,
    SubFlowReusePolicy,
};
use dexzd_protocol::dexzd::v1::{
    ChildFlowCompletedMessage, ChildFlowStartMessage, CrossFlowEndpoint, InboxDedupRecord,
    InboxMessageCommand, ParentFlowReference, StoredFlowResult, StoredStepCompletionOutput,
    SubFlowResultRecord, inbox_message_command,
};

use crate::ApplyError;
use crate::command_handlers::command_replies::{error_reply, ok_reply};
use crate::command_handlers::flow_start::{RunStart, start_run};
use crate::command_handlers::run_cursor::{HistoryKind, RunCursor};
use crate::command_handlers::{CommandScope, HandlerResult};
use crate::cross_flow::outbox::{report_terminal_to_parent, validate_child_start_parameters};
use crate::cross_flow::sub_flow_result_key;
use crate::state_access::{FlowTransaction, RunRows, read_flow_directory};
use crate::values::{ValueHolder, release_stored_value, store_value};

const GRPC_UNAVAILABLE: i32 = 14;

/// Applies a delivered cross-Flow message. A successful reply acknowledges delivery.
pub(crate) fn apply_inbox_message(
    transaction: &mut FlowTransaction<'_>,
    scope: &CommandScope<'_>,
    command: &InboxMessageCommand,
) -> Result<HandlerResult, ApplyError> {
    let Some(source) = command.source.as_ref() else {
        return Err(invalid_cross_flow_message(
            "cross-Flow message has no source",
        ));
    };
    if source.run_id.len() != 16 || command.message_id.len() != 16 {
        return Err(invalid_cross_flow_message(
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
        None => Err(invalid_cross_flow_message(
            "cross-Flow message has no payload",
        )),
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
        return Err(invalid_cross_flow_message(
            "child start target or tenant does not match the command",
        ));
    }
    let Some(parent) = start.parent.as_ref() else {
        return Err(invalid_cross_flow_message(
            "child start has no parent reference",
        ));
    };
    if parent.parent.as_ref() != Some(source) {
        return Err(invalid_cross_flow_message(
            "child start source does not match its parent",
        ));
    }
    if start.child_flow_id != expected_child_flow_id(parent, &source.flow_id) {
        return Err(invalid_cross_flow_message(
            "child Flow ID does not match its parent Step",
        ));
    }
    let condition = start.condition.clone().unwrap_or_default();
    let options = condition.options.clone().unwrap_or_default();
    let config = start.flow_config.clone().unwrap_or_default();
    if let Err(detail) = validate_child_start_parameters(&condition, &config, parent.sub_flow_index)
    {
        return Err(invalid_cross_flow_message(detail));
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
    if let Some(latest) = latest {
        let latest_run_id = run_id_from_directory(&latest.run_id)?;
        let latest_rows = RunRows::new(transaction.context().flow_hash, latest_run_id);
        let latest_record = latest_rows.run_record(transaction)?.ok_or_else(|| {
            invalid_cross_flow_message("latest child run disappeared from its directory")
        })?;
        if latest_record.is_purging {
            return Ok(HandlerResult::Rejected(error_reply(
                GRPC_UNAVAILABLE,
                ErrorSubStatus::Uncategorized,
                "latest child run is being purged",
            )));
        }
    }
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
    let mut result = start_run(transaction, scope, &run_start)?;
    if let HandlerResult::Applied(output) = &mut result {
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
        output.reply = ok_reply();
    } else if let HandlerResult::Rejected(reply) = &result {
        return Err(invalid_cross_flow_message(format!(
            "validated child start was rejected: {reply:?}"
        )));
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
        return Err(invalid_cross_flow_message("existing child run disappeared"));
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
    let Some(parent_reference) = completed.parent.as_ref() else {
        return Err(invalid_cross_flow_message(
            "child completion has no parent reference",
        ));
    };
    let Some(parent) = parent_reference.parent.as_ref() else {
        return Err(invalid_cross_flow_message(
            "child completion has no parent endpoint",
        ));
    };
    if parent.tenant_id != scope.tenant_id.0
        || parent.flow_id != scope.flow_id()
        || parent.run_id != scope.command.run_id
        || source.tenant_id != scope.tenant_id.0
        || source.flow_id != completed.child_flow_id
        || source.run_id != completed.child_run_id
        || source.run_attempt != completed.child_run_attempt
        || completed.child_flow_id != expected_child_flow_id(parent_reference, scope.flow_id())
    {
        return Err(invalid_cross_flow_message(
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
    let registration_attempt = parent.run_attempt;
    let key = sub_flow_result_key(rows, &completed.child_flow_id, registration_attempt);
    let previous: Option<SubFlowResultRecord> =
        transaction.get_message(ColumnFamily::Flow, &key, "SubFlow result")?;
    if let Some(previous) = previous.and_then(|previous| previous.result) {
        for (index, output) in previous.results.into_iter().enumerate() {
            if let Some(stored) = output.completed_step_output {
                let holder = ValueHolder::SubFlowResultAttemptOutput {
                    child_flow_id: completed.child_flow_id.clone(),
                    registration_attempt,
                    output_index: u32::try_from(index)
                        .expect("stored output count fits a u32 index"),
                };
                release_stored_value(transaction, rows, &stored, &holder, cursor.seq)?;
            }
        }
    }
    let result = completed
        .result
        .clone()
        .ok_or_else(|| invalid_cross_flow_message("child completion has no Flow result"))?;
    let mut outputs = Vec::with_capacity(result.results.len());
    for (index, output) in result.results.into_iter().enumerate() {
        let stored = if let Some(value) = output.completed_step_output.as_ref() {
            let holder = ValueHolder::SubFlowResultAttemptOutput {
                child_flow_id: completed.child_flow_id.clone(),
                registration_attempt,
                output_index: u32::try_from(index).expect("child output count fits a u32 index"),
            };
            let mut limits = cursor.value_limits();
            limits.max_value_bytes = u64::MAX;
            match store_value(
                transaction,
                rows,
                limits,
                &mut cursor.values,
                value,
                &holder,
            )? {
                Ok(stored) => Some(stored),
                Err(rejection) => {
                    return Err(invalid_cross_flow_message(format!(
                        "admitted child output could not be stored: {}",
                        rejection.message()
                    )));
                }
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
            parent_registration_attempt: registration_attempt,
        },
    );
    put_inbox_dedup(transaction, rows, source, &command.message_id, cursor.seq);
    crate::steps::wait_conditions::on_child_result(
        transaction,
        &mut cursor,
        scope,
        &completed.child_flow_id,
        registration_attempt,
    )?;
    Ok(HandlerResult::Applied(cursor.finish(
        transaction,
        scope,
        ok_reply(),
        HistoryKind::SEMANTIC,
    )?))
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

fn expected_child_flow_id(parent: &ParentFlowReference, parent_flow_id: &str) -> String {
    format!(
        "SubFlow:{parent_flow_id}-{}-{}",
        parent.step_execution_id, parent.sub_flow_index
    )
}

fn invalid_cross_flow_message(detail: impl Into<String>) -> ApplyError {
    ApplyError::InvariantViolation {
        detail: detail.into(),
    }
}
