// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Closing the current attempt of a run (plan section 9.1): every path (Step decisions, Stop,
//! timeouts, exhausted retries, ID reuse termination) goes through [`close_flow`].

use dexzd_keyspace::ColumnFamily;
use dexzd_keyspace::row_keys::active_work_index_key;
use dexzd_protocol::dex::{FlowErrorType, FlowStatus, StepCompletionOutput};
use dexzd_protocol::dexzd::v1::{
    FlowTimeoutTimerId, RetentionTimerId, StepExecutionPhase, TimerId, timer_id,
};

use crate::command_handlers::CommandScope;
use crate::command_handlers::run_cursor::RunCursor;
use crate::projections::history_events;
use crate::state_access::{FlowTransaction, read_flow_directory, write_flow_directory};
use crate::steps::{arm_timer, disarm_timer, finish_step, is_active};
use crate::values::wire_value;
use crate::{ApplyError, WaitableChange};

/// How the attempt closes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FlowClosing {
    pub(crate) status: FlowStatus,
    pub(crate) error_type: FlowErrorType,
    pub(crate) error_message: String,
}

impl FlowClosing {
    pub(crate) fn completed() -> Self {
        Self {
            status: FlowStatus::Completed,
            error_type: FlowErrorType::Unspecified,
            error_message: String::new(),
        }
    }

    pub(crate) fn failed(error_type: FlowErrorType, message: impl Into<String>) -> Self {
        Self {
            status: FlowStatus::Failed,
            error_type,
            error_message: message.into(),
        }
    }

    pub(crate) fn canceled(reason: impl Into<String>) -> Self {
        Self {
            status: FlowStatus::Canceled,
            error_type: FlowErrorType::Unspecified,
            error_message: reason.into(),
        }
    }

    pub(crate) fn terminated(reason: impl Into<String>) -> Self {
        Self {
            status: FlowStatus::Terminated,
            error_type: FlowErrorType::Unspecified,
            error_message: reason.into(),
        }
    }
}

/// Closes the current attempt: cancels every active Step (results discarded, consumed
/// messages discarded, child Flows keep running), records the close, arms retention, reports
/// to the parent, and wakes waiters. Closing an already closed attempt does nothing.
pub(crate) fn close_flow(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    scope: &CommandScope<'_>,
    closing: FlowClosing,
) -> Result<(), ApplyError> {
    if cursor.record.status != FlowStatus::Running as i32 {
        return Ok(());
    }
    for (_, mut step) in cursor.rows.step_executions(transaction)? {
        if is_active(&step) {
            finish_step(transaction, cursor, &mut step, StepExecutionPhase::Canceled)?;
        }
    }
    let run_attempt = cursor.attempt();
    disarm_timer(
        transaction,
        cursor,
        &TimerId {
            run_attempt,
            kind: Some(timer_id::Kind::FlowTimeout(FlowTimeoutTimerId {})),
        },
    )?;
    cursor.record.status = closing.status as i32;
    cursor.record.closed_at_micros = scope.now_micros;
    cursor.record.is_graceful_completion_requested = false;
    let close = cursor.record.close.get_or_insert_with(Default::default);
    close.error_type = closing.error_type as i32;
    close.error_message = closing.error_message;
    let results: Vec<StepCompletionOutput> = close
        .results
        .iter()
        .map(|output| StepCompletionOutput {
            completed_step_type: output.completed_step_type.clone(),
            completed_step_execution_id: output.completed_step_execution_id.clone(),
            completed_step_output: output
                .completed_step_output
                .as_ref()
                .map(|stored| wire_value(cursor.rows, stored)),
        })
        .collect();
    update_directory_status(transaction, cursor, scope)?;
    let settings = cursor.record.settings.unwrap_or_default();
    let retention_deadline = scope
        .now_micros
        .saturating_add(settings.retention_after_close_micros);
    cursor.record.retention_deadline_micros = retention_deadline;
    arm_timer(
        transaction,
        cursor,
        &TimerId {
            run_attempt: 0,
            kind: Some(timer_id::Kind::Retention(RetentionTimerId {})),
        },
        retention_deadline,
    );
    let partition = transaction.context().partition;
    transaction.delete(
        ColumnFamily::PartitionIndex,
        active_work_index_key(partition, cursor.rows.flow_hash, cursor.run_id()),
    );
    history_events::record_flow_closed(cursor, results);
    crate::cross_flow::report_child_closed(transaction, cursor, scope)?;
    cursor.wake(WaitableChange::FlowClosed);
    Ok(())
}

/// Mirrors the run's status into the Flow directory, used by ID reuse decisions.
pub(crate) fn update_directory_status(
    transaction: &mut FlowTransaction<'_>,
    cursor: &RunCursor,
    scope: &CommandScope<'_>,
) -> Result<(), ApplyError> {
    let mut directory = read_flow_directory(transaction, scope.tenant_id, &cursor.record.flow_id)?;
    let run_id = cursor.run_id().as_bytes().to_vec();
    if let Some(entry) = directory
        .runs
        .iter_mut()
        .find(|entry| entry.run_id == run_id)
    {
        entry.status = cursor.record.status;
        entry.closed_at_micros = cursor.record.closed_at_micros;
    }
    write_flow_directory(
        transaction,
        scope.tenant_id,
        &cursor.record.flow_id,
        &directory,
    );
    Ok(())
}
