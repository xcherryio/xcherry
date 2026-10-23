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
//! A waiting Step owns these rows: one Waiter row per distinct channel of its condition and one
//! WaitCondition timer per pending timer condition. Both are removed when the Step leaves
//! waiting, is canceled, or the Flow closes. Messages are consumed only at the moment the
//! condition is satisfied; until its Execute ends they are claimed, held by
//! [`ValueHolder::ConditionResult`], and counted in the channel's `claimed_count`.

mod condition_plan;
mod timer_skip;

pub(crate) use timer_skip::apply_timer_skip;

use std::collections::BTreeMap;

use dexzd_keyspace::row_keys::{channel_name_prefix, channel_waiter_key, prefix_range};
use dexzd_keyspace::{ColumnFamily, RunTable, run_table_prefix};
use dexzd_protocol::dex::{
    ChannelResult, ConditionResults, ConditionStatus, FlowResult, FlowStatus, StepCompletionOutput,
    TimerResult, WaitingCondition,
};
use dexzd_protocol::dexzd::v1::{
    ChannelMessageRecord, ChannelResultRecord, ChannelWaiterRecord, ConditionResultsRecord,
    StepExecutionPhase, StepExecutionRecord, StepMethod, StoredFlowResult, SubFlowResultRecord,
    TimerId, WaitConditionTimerId, WaitStateRecord, timer_id,
};

use crate::ApplyError;
use crate::command_handlers::CommandScope;
use crate::command_handlers::run_cursor::RunCursor;
use crate::state_access::{FlowTransaction, RunRows};
use crate::steps::wait_conditions::condition_plan::{
    ChannelConsumption, ConditionProgress, plan_condition,
};
use crate::steps::{
    arm_timer, complete_gracefully_if_ready, disarm_timer, make_invocation_pending,
};
use crate::values::{
    ValueHolder, ValueRejection, hold_stored_value, release_stored_value, wire_value,
};

const MICROS_PER_SECOND: i64 = 1_000_000;

/// Registers the condition a WaitFor returned and evaluates it at once. A missing or empty
/// condition completes immediately and makes Execute pending.
///
/// Returns `Ok(Err(..))` when a SubFlow start carries a value the caller must reject.
pub(crate) fn register_wait(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    scope: &CommandScope<'_>,
    step: &mut StepExecutionRecord,
    condition: Option<&WaitingCondition>,
) -> Result<Result<(), ValueRejection>, ApplyError> {
    let Some(condition) = condition.filter(|condition| !is_empty_condition(condition)) else {
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
    };
    let step_execution_id = step.step_execution_id.clone();
    let sub_flow_child_ids: Vec<String> = (0..condition.sub_flow_conditions.len())
        .map(|index| sub_flow_child_flow_id(scope.flow_id(), &step_execution_id, index))
        .collect();
    if let Err(rejection) = crate::cross_flow::start_child_flows(
        transaction,
        cursor,
        scope,
        &step_execution_id,
        &condition.sub_flow_conditions,
        &sub_flow_child_ids,
    )? {
        return Ok(Err(rejection));
    }
    let mut stored_condition = condition.clone();
    let mut timer_fire_at_micros = Vec::with_capacity(stored_condition.timer_conditions.len());
    for timer in &mut stored_condition.timer_conditions {
        let duration_micros = timer
            .duration_seconds
            .max(0)
            .saturating_mul(MICROS_PER_SECOND);
        let fire_at_micros = scope.now_micros.saturating_add(duration_micros);
        timer.firing_unix_timestamp_seconds = ceil_seconds(fire_at_micros);
        timer_fire_at_micros.push(fire_at_micros);
    }
    // Child Flows own their start values; the wait keeps only identities.
    for sub_flow in &mut stored_condition.sub_flow_conditions {
        sub_flow.step_input = None;
        sub_flow.step_options = None;
        sub_flow.options = None;
    }
    for (index, fire_at_micros) in timer_fire_at_micros.iter().enumerate() {
        let timer = wait_timer_id(cursor.attempt(), &step_execution_id, index);
        arm_timer(transaction, cursor, &timer, *fire_at_micros);
    }
    for channel_name in distinct_channel_names(&stored_condition) {
        transaction.put_message(
            ColumnFamily::Flow,
            channel_waiter_key(
                cursor.rows.flow_hash,
                cursor.run_id(),
                channel_name,
                &step_execution_id,
            ),
            &ChannelWaiterRecord {
                started_waiting_seq: cursor.seq,
            },
        );
    }
    let timer_count = stored_condition.timer_conditions.len();
    step.phase = StepExecutionPhase::Waiting as i32;
    step.wait = Some(WaitStateRecord {
        condition: Some(stored_condition),
        timer_fire_at_micros,
        timer_statuses: vec![ConditionStatus::Waiting as i32; timer_count],
        sub_flow_child_ids,
        started_waiting_seq: cursor.seq,
    });
    evaluate_waiting_step(transaction, cursor, scope.now_micros, step)?;
    cursor.rows.put_step_execution(transaction, step);
    Ok(Ok(()))
}

/// Re-evaluates the Steps that wait on these channels, one after another in the order they
/// started waiting, so a consumption by one waiter is visible to the next.
pub(crate) fn on_channels_changed(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    now_micros: i64,
    channel_names: &[String],
) -> Result<(), ApplyError> {
    let mut waiters: Vec<(u64, String)> = Vec::new();
    for channel_name in channel_names {
        waiters.extend(channel_waiters(transaction, cursor.rows, channel_name)?);
    }
    waiters.sort();
    waiters.dedup();
    for (_, step_execution_id) in waiters {
        let Some(mut step) = cursor
            .rows
            .step_execution(transaction, &step_execution_id)?
        else {
            continue;
        };
        if evaluate_waiting_step(transaction, cursor, now_micros, &mut step)? {
            cursor.rows.put_step_execution(transaction, &step);
        }
    }
    Ok(())
}

/// A timer condition fired. Timers of a Step that no longer waits on them are ignored.
pub(crate) fn on_timer_fired(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    scope: &CommandScope<'_>,
    timer: &WaitConditionTimerId,
) -> Result<(), ApplyError> {
    let Some(mut step) = cursor
        .rows
        .step_execution(transaction, &timer.step_execution_id)?
    else {
        return Ok(());
    };
    let Ok(timer_index) = usize::try_from(timer.timer_index) else {
        return Ok(());
    };
    if !mark_timer_completed(&mut step, timer_index) {
        return Ok(());
    }
    evaluate_waiting_step(transaction, cursor, scope.now_micros, &mut step)?;
    cursor.rows.put_step_execution(transaction, &step);
    complete_gracefully_if_ready(transaction, cursor, scope)
}

/// A child Flow's result arrived. The caller (task A6) has already stored its
/// SubFlowResultRecord; this completes the SubFlow conditions waiting on `child_flow_id`.
pub(crate) fn on_child_result(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    scope: &CommandScope<'_>,
    child_flow_id: &str,
) -> Result<(), ApplyError> {
    let mut waiters: Vec<(u64, StepExecutionRecord)> = cursor
        .rows
        .step_executions(transaction)?
        .into_iter()
        .filter_map(|(_, step)| {
            let started_waiting_seq = step.wait.as_ref().and_then(|wait| {
                wait.sub_flow_child_ids
                    .iter()
                    .any(|id| id == child_flow_id)
                    .then_some(wait.started_waiting_seq)
            })?;
            Some((started_waiting_seq, step))
        })
        .collect();
    waiters.sort_by_key(|(started_waiting_seq, _)| *started_waiting_seq);
    for (_, mut step) in waiters {
        if evaluate_waiting_step(transaction, cursor, scope.now_micros, &mut step)? {
            cursor.rows.put_step_execution(transaction, &step);
        }
    }
    complete_gracefully_if_ready(transaction, cursor, scope)
}

/// Releases a Step's wait registration (Waiter rows and pending timers) and the messages it
/// consumed into its condition results. Called when its Execute ends, it is canceled, or the
/// Flow closes; a second call does nothing.
pub(crate) fn release_condition_results(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    step: &mut StepExecutionRecord,
) -> Result<(), ApplyError> {
    if let Some(wait) = step.wait.take() {
        remove_wait_registration(transaction, cursor, &step.step_execution_id, &wait)?;
    }
    let Some(results) = step.condition_results.take() else {
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
    Ok(())
}

/// Converts stored condition results to what a Worker's Execute receives: values only (no
/// message IDs), large values as references of this run.
pub(crate) fn wire_condition_results(
    rows: RunRows,
    results: &ConditionResultsRecord,
) -> ConditionResults {
    let wire_flow_result = |result: &StoredFlowResult| FlowResult {
        flow_status: result.flow_status,
        results: result
            .results
            .iter()
            .map(|output| StepCompletionOutput {
                completed_step_type: output.completed_step_type.clone(),
                completed_step_execution_id: output.completed_step_execution_id.clone(),
                completed_step_output: output
                    .completed_step_output
                    .as_ref()
                    .map(|stored| wire_value(rows, stored)),
            })
            .collect(),
        error_type: result.error_type,
        error_message: result.error_message.clone(),
    };
    ConditionResults {
        channel_results: results
            .channel_results
            .iter()
            .map(|channel| ChannelResult {
                condition_id: channel.condition_id.clone(),
                condition_status: channel.condition_status,
                channel_name: channel.channel_name.clone(),
                values: channel
                    .messages
                    .iter()
                    .filter_map(|message| message.value.as_ref())
                    .map(|stored| wire_value(rows, stored))
                    .collect(),
            })
            .collect(),
        timer_results: results.timer_results.clone(),
        wait_for_failed: results.wait_for_failed,
        sub_flow_results: results
            .sub_flow_results
            .iter()
            .map(wire_flow_result)
            .collect(),
    }
}

/// Evaluates a waiting Step. When its condition is satisfied, consumes the winning channel
/// messages, removes its wait registration, and makes Execute pending; the caller stores the
/// Step. Returns whether the Step left waiting.
fn evaluate_waiting_step(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    now_micros: i64,
    step: &mut StepExecutionRecord,
) -> Result<bool, ApplyError> {
    if step.phase != StepExecutionPhase::Waiting as i32 {
        return Ok(false);
    }
    let Some(wait) = step.wait.as_ref() else {
        return Ok(false);
    };
    let condition = wait.condition.clone().unwrap_or_default();
    let is_timer_completed: Vec<bool> = (0..condition.timer_conditions.len())
        .map(|index| is_status_completed(wait.timer_statuses.get(index).copied()))
        .collect();
    let mut sub_flow_results = Vec::with_capacity(wait.sub_flow_child_ids.len());
    for child_flow_id in &wait.sub_flow_child_ids {
        sub_flow_results.push(sub_flow_result(transaction, cursor.rows, child_flow_id)?);
    }
    let is_sub_flow_completed: Vec<bool> = sub_flow_results.iter().map(Option::is_some).collect();
    let mut pending_counts: BTreeMap<String, u64> = BTreeMap::new();
    for channel_name in distinct_channel_names(&condition) {
        let meta = cursor.rows.channel_meta(transaction, channel_name)?;
        pending_counts.insert(channel_name.to_owned(), meta.pending_count);
    }
    let progress = ConditionProgress {
        pending_counts: &pending_counts,
        is_timer_completed: &is_timer_completed,
        is_sub_flow_completed: &is_sub_flow_completed,
    };
    let Some(consumptions) = plan_condition(&condition, &progress) else {
        return Ok(false);
    };
    let Some(wait) = step.wait.take() else {
        return Ok(false);
    };
    remove_wait_registration(transaction, cursor, &step.step_execution_id, &wait)?;
    let consumed = consume_messages(
        transaction,
        cursor,
        &step.step_execution_id,
        &condition,
        &consumptions,
    )?;
    step.condition_results = Some(build_condition_results(
        &condition,
        &wait,
        consumed,
        sub_flow_results,
    ));
    step.phase = StepExecutionPhase::Execute as i32;
    make_invocation_pending(
        transaction,
        cursor,
        step,
        StepMethod::Execute,
        1,
        now_micros,
    )?;
    Ok(true)
}

/// Moves the planned messages from their channels into the Step's claim, FIFO. Each value
/// gains its condition-result holder before losing its channel holder, so it never becomes
/// garbage in between.
fn consume_messages(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    step_execution_id: &str,
    condition: &WaitingCondition,
    consumptions: &[ChannelConsumption],
) -> Result<BTreeMap<usize, Vec<ChannelMessageRecord>>, ApplyError> {
    let mut consumed = BTreeMap::new();
    for consumption in consumptions {
        let Some(channel) = condition
            .channel_conditions
            .get(consumption.channel_condition_index)
        else {
            continue;
        };
        let limit = usize::try_from(consumption.count).unwrap_or(usize::MAX);
        let messages = if limit == 0 {
            Vec::new()
        } else {
            cursor
                .rows
                .channel_messages(transaction, &channel.channel_name, limit)?
        };
        for message in &messages {
            if let Some(stored) = &message.value {
                let claim = ValueHolder::ConditionResult {
                    step_execution_id: step_execution_id.to_owned(),
                    channel_name: message.channel_name.clone(),
                    enqueued_seq: message.enqueued_seq,
                };
                hold_stored_value(transaction, cursor.rows, stored, &claim)?;
                let pending = ValueHolder::ChannelMessage {
                    channel_name: message.channel_name.clone(),
                    enqueued_seq: message.enqueued_seq,
                };
                release_stored_value(transaction, cursor.rows, stored, &pending, cursor.seq)?;
            }
            cursor.rows.delete_channel_message(transaction, message);
        }
        if !messages.is_empty() {
            let count = u64::try_from(messages.len()).unwrap_or(u64::MAX);
            let mut meta = cursor
                .rows
                .channel_meta(transaction, &channel.channel_name)?;
            meta.pending_count = meta.pending_count.saturating_sub(count);
            meta.claimed_count = meta.claimed_count.saturating_add(count);
            cursor
                .rows
                .put_channel_meta(transaction, &channel.channel_name, &meta);
        }
        consumed.insert(consumption.channel_condition_index, messages);
    }
    Ok(consumed)
}

/// Condition results as the Go Server reports them: one entry per timer, per channel
/// condition (messages only for the winning candidate), and per SubFlow.
///
/// SubFlow entries share stored values with the run's SubFlowResultRecord rows, which keep
/// their own holders until the run is deleted.
fn build_condition_results(
    condition: &WaitingCondition,
    wait: &WaitStateRecord,
    mut consumed: BTreeMap<usize, Vec<ChannelMessageRecord>>,
    sub_flow_results: Vec<Option<StoredFlowResult>>,
) -> ConditionResultsRecord {
    let timer_results = condition
        .timer_conditions
        .iter()
        .enumerate()
        .map(|(index, timer)| TimerResult {
            condition_id: timer.condition_id.clone(),
            condition_status: if is_status_completed(wait.timer_statuses.get(index).copied()) {
                ConditionStatus::Completed as i32
            } else {
                ConditionStatus::Waiting as i32
            },
        })
        .collect();
    let channel_results = condition
        .channel_conditions
        .iter()
        .enumerate()
        .map(|(index, channel)| {
            let messages = consumed.remove(&index);
            ChannelResultRecord {
                condition_id: channel.condition_id.clone(),
                condition_status: if messages.is_some() {
                    ConditionStatus::Completed as i32
                } else {
                    ConditionStatus::Waiting as i32
                },
                channel_name: channel.channel_name.clone(),
                messages: messages.unwrap_or_default(),
            }
        })
        .collect();
    let sub_flow_results = sub_flow_results
        .into_iter()
        .map(|result| {
            result.unwrap_or_else(|| StoredFlowResult {
                flow_status: FlowStatus::Running as i32,
                ..StoredFlowResult::default()
            })
        })
        .collect();
    ConditionResultsRecord {
        channel_results,
        timer_results,
        wait_for_failed: false,
        sub_flow_results,
    }
}

/// Deletes the Step's Waiter rows and disarms its timers that have not fired or been skipped.
fn remove_wait_registration(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    step_execution_id: &str,
    wait: &WaitStateRecord,
) -> Result<(), ApplyError> {
    let condition = wait.condition.clone().unwrap_or_default();
    for channel_name in distinct_channel_names(&condition) {
        transaction.delete(
            ColumnFamily::Flow,
            channel_waiter_key(
                cursor.rows.flow_hash,
                cursor.run_id(),
                channel_name,
                step_execution_id,
            ),
        );
    }
    for index in 0..condition.timer_conditions.len() {
        if !is_status_completed(wait.timer_statuses.get(index).copied()) {
            let timer = wait_timer_id(cursor.attempt(), step_execution_id, index);
            disarm_timer(transaction, cursor, &timer)?;
        }
    }
    Ok(())
}

/// Marks a pending timer condition of a waiting Step as completed (fired or skipped).
/// Returns false when the Step does not wait on that timer any more.
fn mark_timer_completed(step: &mut StepExecutionRecord, timer_index: usize) -> bool {
    if step.phase != StepExecutionPhase::Waiting as i32 {
        return false;
    }
    let Some(status) = step
        .wait
        .as_mut()
        .and_then(|wait| wait.timer_statuses.get_mut(timer_index))
    else {
        return false;
    };
    if is_status_completed(Some(*status)) {
        return false;
    }
    *status = ConditionStatus::Completed as i32;
    true
}

/// Waiters of one channel as (started waiting seq, Step execution ID).
fn channel_waiters(
    transaction: &FlowTransaction<'_>,
    rows: RunRows,
    channel_name: &str,
) -> Result<Vec<(u64, String)>, ApplyError> {
    let mut prefix = run_table_prefix(rows.flow_hash, rows.run_id, RunTable::Waiter);
    prefix.extend_from_slice(&channel_name_prefix(channel_name));
    let (start, end) = prefix_range(&prefix);
    transaction
        .scan_messages::<ChannelWaiterRecord>(
            ColumnFamily::Flow,
            &start,
            &end,
            usize::MAX,
            "channel waiter",
        )?
        .into_iter()
        .map(|(key, record)| {
            let step_execution_id =
                String::from_utf8(key[prefix.len()..].to_vec()).map_err(|error| {
                    ApplyError::CorruptState {
                        table: "channel waiter",
                        key: key.iter().map(|byte| format!("{byte:02x}")).collect(),
                        detail: error.to_string(),
                    }
                })?;
            Ok((record.started_waiting_seq, step_execution_id))
        })
        .collect()
}

fn sub_flow_result(
    transaction: &FlowTransaction<'_>,
    rows: RunRows,
    child_flow_id: &str,
) -> Result<Option<StoredFlowResult>, ApplyError> {
    let key = dexzd_keyspace::row_keys::run_named_key(
        rows.flow_hash,
        rows.run_id,
        RunTable::SubFlowResult,
        child_flow_id,
    );
    let record: Option<SubFlowResultRecord> =
        transaction.get_message(ColumnFamily::Flow, &key, "sub flow result")?;
    Ok(record.map(|record| record.result.unwrap_or_default()))
}

fn is_empty_condition(condition: &WaitingCondition) -> bool {
    condition.timer_conditions.is_empty()
        && condition.channel_conditions.is_empty()
        && condition.sub_flow_conditions.is_empty()
}

fn is_status_completed(status: Option<i32>) -> bool {
    status == Some(ConditionStatus::Completed as i32)
}

/// Distinct channel names of the condition's channel conditions, in declaration order.
fn distinct_channel_names(condition: &WaitingCondition) -> Vec<&str> {
    let mut names: Vec<&str> = Vec::new();
    for channel in &condition.channel_conditions {
        if !names.contains(&channel.channel_name.as_str()) {
            names.push(&channel.channel_name);
        }
    }
    names
}

fn wait_timer_id(run_attempt: u32, step_execution_id: &str, timer_index: usize) -> TimerId {
    TimerId {
        run_attempt,
        kind: Some(timer_id::Kind::WaitCondition(WaitConditionTimerId {
            step_execution_id: step_execution_id.to_owned(),
            timer_index: u32::try_from(timer_index).unwrap_or(u32::MAX),
        })),
    }
}

/// Child Flow ID of a SubFlow condition; visible to applications, so it matches the Go Server.
fn sub_flow_child_flow_id(parent_flow_id: &str, step_execution_id: &str, index: usize) -> String {
    format!("SubFlow:{parent_flow_id}-{step_execution_id}-{index}")
}

/// Whole seconds at or after `micros`, as reported in `firing_unix_timestamp_seconds` (C13).
fn ceil_seconds(micros: i64) -> i64 {
    let seconds = micros.div_euclid(MICROS_PER_SECOND);
    if micros.rem_euclid(MICROS_PER_SECOND) == 0 {
        seconds
    } else {
        seconds + 1
    }
}
