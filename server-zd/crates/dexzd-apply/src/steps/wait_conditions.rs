// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Wait conditions (plan section 9.2): registration after WaitFor, evaluation when channels,
//! timers, or child Flows change, and consumption of channel messages into condition results.
//!
//! The backbone handles a WaitFor without a condition. Timer, channel, and SubFlow conditions
//! are implemented by the wait condition engine (task A3); until then such Steps stay waiting.

use dexzd_protocol::dex::WaitingCondition;
use dexzd_protocol::dexzd::v1::{
    StepExecutionPhase, StepExecutionRecord, StepMethod, WaitStateRecord,
};

use crate::ApplyError;
use crate::command_handlers::CommandScope;
use crate::command_handlers::run_cursor::RunCursor;
use crate::state_access::FlowTransaction;
use crate::steps::make_invocation_pending;
use crate::values::ValueRejection;

/// Registers the condition a WaitFor returned. A missing or empty condition completes at once
/// and makes Execute pending.
pub(crate) fn register_wait(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    scope: &CommandScope<'_>,
    step: &mut StepExecutionRecord,
    condition: Option<&WaitingCondition>,
) -> Result<Result<(), ValueRejection>, ApplyError> {
    let is_empty = condition.is_none_or(|condition| {
        condition.timer_conditions.is_empty()
            && condition.channel_conditions.is_empty()
            && condition.sub_flow_conditions.is_empty()
    });
    if is_empty {
        step.phase = StepExecutionPhase::Execute as i32;
        make_invocation_pending(
            transaction,
            cursor,
            step,
            StepMethod::Execute,
            1,
            scope.now_micros,
        )?;
        cursor.rows.put_step_execution(transaction, step);
        return Ok(Ok(()));
    }
    step.phase = StepExecutionPhase::Waiting as i32;
    step.wait = Some(WaitStateRecord {
        condition: condition.cloned(),
        started_waiting_seq: cursor.seq,
        ..WaitStateRecord::default()
    });
    cursor.rows.put_step_execution(transaction, step);
    Ok(Ok(()))
}

/// Re-evaluates the Steps that wait on these channels.
pub(crate) fn on_channels_changed(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    channel_names: &[String],
) -> Result<(), ApplyError> {
    let _ = (transaction, cursor, channel_names);
    Ok(())
}

/// Releases the messages a Step consumed into its condition results (when its Execute ends,
/// is canceled, or the Flow closes) and its channel waiter registrations.
pub(crate) fn release_condition_results(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    step: &mut StepExecutionRecord,
) -> Result<(), ApplyError> {
    use crate::values::{ValueHolder, release_stored_value};
    let Some(results) = step.condition_results.as_ref() else {
        step.wait = None;
        return Ok(());
    };
    for channel in &results.channel_results {
        for message in &channel.messages {
            if let Some(stored) = &message.value {
                let holder = ValueHolder::ConditionResult {
                    step_execution_id: step.step_execution_id.clone(),
                    channel_name: message.channel_name.clone(),
                    enqueued_seq: message.enqueued_seq,
                };
                release_stored_value(transaction, cursor.rows, stored, &holder, cursor.seq)?;
            }
            let mut meta = cursor
                .rows
                .channel_meta(transaction, &message.channel_name)?;
            meta.claimed_count = meta.claimed_count.saturating_sub(1);
            cursor
                .rows
                .put_channel_meta(transaction, &message.channel_name, &meta);
        }
    }
    step.wait = None;
    Ok(())
}

/// A timer condition fired.
pub(crate) fn on_timer_fired(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    scope: &CommandScope<'_>,
    timer: &dexzd_protocol::dexzd::v1::WaitConditionTimerId,
) -> Result<(), ApplyError> {
    let _ = (transaction, cursor, scope, timer);
    Ok(())
}
