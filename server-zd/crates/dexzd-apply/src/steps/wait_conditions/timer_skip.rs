// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! SkipTimer (plan section 9.2): completes a pending timer condition early. Checking and
//! skipping happen in one command, so no stale-skip queue is needed.

use dexzd_protocol::dex::{ErrorSubStatus, FlowStatus, SkipTimerRequest};
use dexzd_protocol::dexzd::v1::{
    FlowTimeoutTimerId, StepExecutionPhase, StepExecutionRecord, TimerId, TimerSkipCommand,
    timer_id,
};

use crate::ApplyError;
use crate::command_handlers::command_replies::{error_reply, grpc_code, ok_reply};
use crate::command_handlers::run_cursor::{HistoryKind, RunCursor};
use crate::command_handlers::step_commands::apply_flow_timeout;
use crate::command_handlers::{CommandScope, HandlerResult};
use crate::state_access::FlowTransaction;
use crate::steps::wait_conditions::{
    evaluate_waiting_step, is_status_completed, mark_timer_completed, wait_timer_id,
};
use crate::steps::{TIMEOUT_HANDLER_STEP_EXECUTION_ID, complete_gracefully_if_ready, disarm_timer};

/// Skips the timer the request selects. Rejects with INVALID_ARGUMENT, writing nothing, when
/// the timer does not exist or is not pending.
pub(crate) fn apply_timer_skip(
    transaction: &mut FlowTransaction<'_>,
    mut cursor: RunCursor,
    scope: &CommandScope<'_>,
    command: &TimerSkipCommand,
) -> Result<HandlerResult, ApplyError> {
    let request = command.request.clone().unwrap_or_default();
    let is_skipped = if request.step_execution_id == TIMEOUT_HANDLER_STEP_EXECUTION_ID {
        skip_flow_timeout(transaction, &mut cursor, scope, &request)?
    } else {
        skip_wait_timer(transaction, &mut cursor, scope, &request)?
    };
    if !is_skipped {
        return Ok(HandlerResult::Rejected(error_reply(
            grpc_code::INVALID_ARGUMENT,
            ErrorSubStatus::Uncategorized,
            "requested timer condition does not exist or is not pending",
        )));
    }
    complete_gracefully_if_ready(transaction, &mut cursor, scope)?;
    Ok(HandlerResult::Applied(cursor.finish(
        transaction,
        scope,
        ok_reply(),
        HistoryKind::EXTERNAL_INPUT,
    )?))
}

/// The Flow timeout is the timeout handler's only timer: index 0, no condition ID. Skipping it
/// fires the timeout now.
fn skip_flow_timeout(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    scope: &CommandScope<'_>,
    request: &SkipTimerRequest,
) -> Result<bool, ApplyError> {
    let is_first_timer =
        request.timer_condition_id.is_empty() && request.timer_condition_index == Some(0);
    if !is_first_timer || cursor.record.status != FlowStatus::Running as i32 {
        return Ok(false);
    }
    let timer = TimerId {
        run_attempt: cursor.attempt(),
        kind: Some(timer_id::Kind::FlowTimeout(FlowTimeoutTimerId {})),
    };
    if disarm_timer(transaction, cursor, &timer)?.is_none() {
        return Ok(false);
    }
    apply_flow_timeout(transaction, cursor, scope)?;
    Ok(true)
}

fn skip_wait_timer(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    scope: &CommandScope<'_>,
    request: &SkipTimerRequest,
) -> Result<bool, ApplyError> {
    let Some(mut step) = cursor
        .rows
        .step_execution(transaction, &request.step_execution_id)?
    else {
        return Ok(false);
    };
    let Some(timer_index) = select_pending_timer(&step, request) else {
        return Ok(false);
    };
    if !mark_timer_completed(&mut step, timer_index) {
        return Ok(false);
    }
    let timer = wait_timer_id(cursor.attempt(), &step.step_execution_id, timer_index);
    disarm_timer(transaction, cursor, &timer)?;
    evaluate_waiting_step(transaction, cursor, scope.now_micros, &mut step)?;
    cursor.rows.put_step_execution(transaction, &step);
    Ok(true)
}

/// Selects by condition ID when one is given, otherwise by index, as the Go Server does. Only
/// a pending timer of a waiting Step qualifies.
fn select_pending_timer(step: &StepExecutionRecord, request: &SkipTimerRequest) -> Option<usize> {
    if step.phase != StepExecutionPhase::Waiting as i32 {
        return None;
    }
    let wait = step.wait.as_ref()?;
    let timers = wait.condition.as_ref()?.timer_conditions.as_slice();
    let is_pending = |index: usize| !is_status_completed(wait.timer_statuses.get(index).copied());
    if !request.timer_condition_id.is_empty() {
        return (0..timers.len()).find(|&index| {
            timers[index].condition_id == request.timer_condition_id && is_pending(index)
        });
    }
    let index = usize::try_from(request.timer_condition_index?).ok()?;
    (index < timers.len() && is_pending(index)).then_some(index)
}
