// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;

use dexzd_keyspace::{ColumnFamily, FlowHash, RunId, TenantId, flow_directory_key};
use dexzd_protocol::dex::{
    ActiveStepExecutionState, ActiveStepPhase, ChannelMessage, ChannelValues, ConditionStatus,
    FlowExecutionId, FlowResult, GetChannelMessagesResponse, GetFlowStateResponse,
    GetFlowSummaryResponse, InternalTimerStatus, Kv, StepCompletionOutput,
    StepExecutionCompletedConditions, StepMovement, SubFlowConditionState, TimerInfo,
    WaitingConditionState,
};
use dexzd_protocol::dexzd::v1::{
    FlowDirectoryRecord, StepExecutionPhase, StepExecutionRecord, StepMethod,
};
use prost::Message;

use crate::read_model::attributes::all_attribute_kvs;
use crate::read_model::{ReadModelError, RunView};
use crate::steps::is_active;
use crate::values::wire_value;
use crate::{ApplyError, FlowStateReader};

/// Resolves an optional run ID to the latest retained run of a Flow.
pub fn resolve_run_id(
    reader: &dyn FlowStateReader,
    flow_hash: FlowHash,
    tenant_id: TenantId,
    flow_id: &str,
    requested_run_id: &str,
) -> Result<Option<RunId>, ReadModelError> {
    if !requested_run_id.is_empty() {
        return requested_run_id
            .parse::<RunId>()
            .map(Some)
            .map_err(|error| ReadModelError::InvalidRequest {
                detail: format!("invalid run ID: {error}"),
            });
    }
    let key = flow_directory_key(flow_hash, tenant_id, flow_id);
    let Some(bytes) = reader
        .get(ColumnFamily::Flow, &key)
        .map_err(ApplyError::from)?
    else {
        return Ok(None);
    };
    let directory = FlowDirectoryRecord::decode(bytes.as_slice()).map_err(|error| {
        ApplyError::CorruptState {
            table: "flow directory",
            key: format!("{key:x?}"),
            detail: error.to_string(),
        }
    })?;
    directory
        .runs
        .last()
        .map(|run| {
            RunId::from_slice(&run.run_id).map_err(|error| {
                ReadModelError::State(ApplyError::InvariantViolation {
                    detail: format!("flow directory has an invalid run ID: {error}"),
                })
            })
        })
        .transpose()
}

/// Reads the summary of one retained run.
pub fn get_flow_summary(
    reader: &dyn FlowStateReader,
    flow_hash: FlowHash,
    run_id: RunId,
) -> Result<GetFlowSummaryResponse, ReadModelError> {
    let view = RunView::open(reader, flow_hash, run_id)?;
    let record = &view.record;
    Ok(GetFlowSummaryResponse {
        flow_execution_id: Some(FlowExecutionId {
            flow_id: record.flow_id.clone(),
            run_id: run_id.to_string(),
        }),
        first_run_id: run_id.to_string(),
        request_id: record.start_request_id.clone(),
        flow_type: record.flow_type.clone(),
        flow_status: record.status,
        start_time: Some(crate::command_handlers::run_cursor::timestamp(
            record.started_at_micros,
        )),
        close_time: (record.closed_at_micros != 0)
            .then(|| crate::command_handlers::run_cursor::timestamp(record.closed_at_micros)),
        run_attempt: i32::try_from(record.current_attempt).unwrap_or(i32::MAX),
    })
}

/// Reads all pending messages of one Channel in FIFO order.
pub fn get_channel_messages(
    reader: &dyn FlowStateReader,
    flow_hash: FlowHash,
    run_id: RunId,
    channel_name: &str,
) -> Result<GetChannelMessagesResponse, ReadModelError> {
    let view = RunView::open(reader, flow_hash, run_id)?;
    let messages = view
        .rows
        .channel_messages(&view.transaction, channel_name, usize::MAX)?
        .into_iter()
        .map(|record| ChannelMessage {
            channel_name: record.channel_name,
            message_id: record.message_id,
            value: record.value.map(|stored| wire_value(view.rows, &stored)),
        })
        .collect();
    Ok(GetChannelMessagesResponse { messages })
}

/// Reads the current Flow state without hydrating large values.
pub fn get_flow_state(
    reader: &dyn FlowStateReader,
    flow_hash: FlowHash,
    run_id: RunId,
) -> Result<GetFlowStateResponse, ReadModelError> {
    let view = RunView::open(reader, flow_hash, run_id)?;
    let mut steps = view.rows.step_executions(&view.transaction)?;
    steps.sort_by(|(_, left), (_, right)| {
        (left.created_seq, &left.step_execution_id)
            .cmp(&(right.created_seq, &right.step_execution_id))
    });
    let active_step_executions = steps
        .iter()
        .filter(|(_, step)| is_active(step))
        .map(|(_, step)| active_step_state(&view, step))
        .collect();
    let mut pending_channel_messages = BTreeMap::new();
    for (channel_name, meta) in view.rows.channel_metas(&view.transaction)? {
        if meta.pending_count == 0 {
            continue;
        }
        let messages = get_channel_messages(reader, flow_hash, run_id, &channel_name)?.messages;
        pending_channel_messages.insert(channel_name, ChannelValues { messages });
    }
    let completed_steps = view
        .record
        .close
        .as_ref()
        .map(|close| {
            close
                .results
                .iter()
                .map(|output| StepCompletionOutput {
                    completed_step_type: output.completed_step_type.clone(),
                    completed_step_execution_id: output.completed_step_execution_id.clone(),
                    completed_step_output: output
                        .completed_step_output
                        .as_ref()
                        .map(|stored| wire_value(view.rows, stored)),
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(GetFlowStateResponse {
        flow_config: view.record.flow_config.clone(),
        attributes: all_attribute_kvs(&view)?,
        active_step_executions,
        queued_steps: Vec::new(),
        pending_channel_messages,
        completed_steps,
        sticky_app_worker_id: view.record.sticky_app_worker_id,
    })
}

fn active_step_state(view: &RunView<'_>, step: &StepExecutionRecord) -> ActiveStepExecutionState {
    let movement = StepMovement {
        step_type: step.step_type.clone(),
        step_input: step
            .input
            .as_ref()
            .map(|stored| wire_value(view.rows, stored)),
        step_options: step.options.clone(),
        from_step_execution_id_internal_only: step.from_step_execution_id.clone(),
        ..StepMovement::default()
    };
    let phase = if step.phase == StepExecutionPhase::Waiting as i32 {
        ActiveStepPhase::Waiting
    } else {
        ActiveStepPhase::Active
    };
    let waiting_condition = step.wait.as_ref().and_then(|wait| {
        wait.condition
            .as_ref()
            .map(|condition| WaitingConditionState {
                waiting_condition_type: condition.waiting_condition_type,
                timer_conditions: condition.timer_conditions.clone(),
                channel_conditions: condition.channel_conditions.clone(),
                condition_combinations: condition.condition_combinations.clone(),
                sub_flow_conditions: condition
                    .sub_flow_conditions
                    .iter()
                    .map(|sub_flow| SubFlowConditionState {
                        condition_id: sub_flow.condition_id.clone(),
                    })
                    .collect(),
            })
    });
    let timers = step
        .wait
        .as_ref()
        .map(|wait| {
            wait.timer_fire_at_micros
                .iter()
                .enumerate()
                .map(|(index, fire_at)| TimerInfo {
                    condition_id: wait
                        .condition
                        .as_ref()
                        .and_then(|condition| condition.timer_conditions.get(index))
                        .map_or_else(String::new, |timer| timer.condition_id.clone()),
                    firing_unix_timestamp_seconds: fire_at.div_euclid(1_000_000)
                        + i64::from(fire_at.rem_euclid(1_000_000) > 0),
                    status: if wait.timer_statuses.get(index).copied()
                        == Some(ConditionStatus::Completed as i32)
                    {
                        InternalTimerStatus::Fired as i32
                    } else {
                        InternalTimerStatus::Pending as i32
                    },
                })
                .collect()
        })
        .unwrap_or_default();
    let step_execution_locals: Vec<Kv> = step
        .locals
        .iter()
        .map(|local| Kv {
            key: local.key.clone(),
            value: local
                .value
                .as_ref()
                .map(|stored| wire_value(view.rows, stored)),
        })
        .collect();
    let invocation = step.invocation.as_ref();
    let (wait_for_pull_routing, execute_pull_routing) = match invocation {
        Some(invocation) if invocation.method == StepMethod::WaitFor as i32 => {
            (invocation.pull_routing.clone(), None)
        }
        Some(invocation) if invocation.method == StepMethod::Execute as i32 => {
            (None, invocation.pull_routing.clone())
        }
        _ => (None, None),
    };
    ActiveStepExecutionState {
        step_execution_id: step.step_execution_id.clone(),
        from_step_execution_id: step.from_step_execution_id.clone(),
        step_type: step.step_type.clone(),
        phase: phase as i32,
        movement: Some(movement),
        waiting_condition,
        completed_conditions: completed_conditions(view, step),
        step_execution_locals,
        timers,
        last_failure_info: invocation.and_then(|invocation| invocation.last_failure.clone()),
        wait_for_pull_routing,
        execute_pull_routing,
    }
}

fn completed_conditions(
    view: &RunView<'_>,
    step: &StepExecutionRecord,
) -> Option<StepExecutionCompletedConditions> {
    let results = step.condition_results.as_ref()?;
    let completed_timer_conditions = results
        .timer_results
        .iter()
        .enumerate()
        .map(|(index, result)| {
            (
                i32::try_from(index).unwrap_or(i32::MAX),
                if result.condition_status == ConditionStatus::Completed as i32 {
                    InternalTimerStatus::Fired as i32
                } else {
                    InternalTimerStatus::Pending as i32
                },
            )
        })
        .collect();
    let completed_sub_flow_results = results
        .sub_flow_results
        .iter()
        .enumerate()
        .map(|(index, result)| {
            (
                i32::try_from(index).unwrap_or(i32::MAX),
                FlowResult {
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
                                .map(|stored| wire_value(view.rows, stored)),
                        })
                        .collect(),
                    error_type: result.error_type,
                    error_message: result.error_message.clone(),
                },
            )
        })
        .collect();
    Some(StepExecutionCompletedConditions {
        completed_timer_conditions,
        completed_sub_flow_results,
    })
}
