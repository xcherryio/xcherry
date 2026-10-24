// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Adapters to apply's read model for GetFlowState and GetHistoryEvents.

use dexzd_apply::read_model::{ReadModelError, get_flow_state, get_history_events};
use dexzd_apply::{ApplyContext, FlowStateReader};
use dexzd_protocol::dex::{
    GetFlowStateRequest, GetFlowStateResponse, GetHistoryEventsRequest, GetHistoryEventsResponse,
    Kv, StepCompletionOutput, Value,
};

use crate::runtime::read_operations::state_rows::{hydrate_value_reference, read_run};
use crate::runtime::{FlowAddress, RuntimeError};

/// Returns the Flow state projected by apply's read model, with value references replaced by
/// their values when `is_hydrating`.
///
/// Returns `NOT_FOUND` + `FLOW_NOT_EXISTS` when the run does not exist, and `Internal` when a
/// value reference of the projection has no value row.
pub(crate) fn read_flow_state(
    state: &dyn FlowStateReader,
    context: ApplyContext,
    flow: &FlowAddress,
    request: &GetFlowStateRequest,
    is_hydrating: bool,
) -> Result<GetFlowStateResponse, RuntimeError> {
    let run = read_run(state, context, flow, &request.run_id)?;
    let mut response = get_flow_state(state, context.flow_hash, run.run_id)
        .map_err(|error| read_model_error(flow, error))?;
    if is_hydrating {
        for value in flow_state_values(&mut response) {
            if !hydrate_value_reference(state, context.flow_hash, run.run_id, value)? {
                return Err(RuntimeError::Internal {
                    detail: format!(
                        "a GetFlowState value of run {} has no value row",
                        run.run_id
                    ),
                });
            }
        }
    }
    Ok(response)
}

/// Returns history events projected by apply's read model.
///
/// Returns `NOT_FOUND` + `FLOW_NOT_EXISTS` when the run does not exist, and `INVALID_ARGUMENT`
/// for a negative position or a malformed page token.
pub(crate) fn read_history_events(
    state: &dyn FlowStateReader,
    context: ApplyContext,
    flow: &FlowAddress,
    request: &GetHistoryEventsRequest,
) -> Result<GetHistoryEventsResponse, RuntimeError> {
    let run = read_run(state, context, flow, &request.run_id)?;
    get_history_events(
        state,
        context.flow_hash,
        run.run_id,
        request.start_internal_event_id,
        request.estimate_page_size,
        &request.next_page_token,
        request.run_attempt,
    )
    .map_err(|error| read_model_error(flow, error))
}

/// Returns every value of a GetFlowState response.
fn flow_state_values(response: &mut GetFlowStateResponse) -> Vec<&mut Value> {
    let mut values: Vec<&mut Value> = Vec::new();
    values.extend(kv_values(&mut response.attributes));
    for step in &mut response.active_step_executions {
        values.extend(
            step.movement
                .as_mut()
                .and_then(|movement| movement.step_input.as_mut()),
        );
        if let Some(conditions) = step.completed_conditions.as_mut() {
            for result in conditions.completed_sub_flow_results.values_mut() {
                values.extend(step_output_values(&mut result.results));
            }
        }
        values.extend(kv_values(&mut step.step_execution_locals));
    }
    for movement in &mut response.queued_steps {
        values.extend(movement.step_input.as_mut());
    }
    for channel in response.pending_channel_messages.values_mut() {
        values.extend(
            channel
                .messages
                .iter_mut()
                .filter_map(|message| message.value.as_mut()),
        );
    }
    values.extend(step_output_values(&mut response.completed_steps));
    values
}

fn kv_values(kvs: &mut [Kv]) -> impl Iterator<Item = &mut Value> {
    kvs.iter_mut().filter_map(|kv| kv.value.as_mut())
}

fn step_output_values(outputs: &mut [StepCompletionOutput]) -> impl Iterator<Item = &mut Value> {
    outputs
        .iter_mut()
        .filter_map(|output| output.completed_step_output.as_mut())
}

fn read_model_error(flow: &FlowAddress, error: ReadModelError) -> RuntimeError {
    match error {
        ReadModelError::RunNotFound { .. } => RuntimeError::flow_not_found(&flow.flow_id),
        ReadModelError::InvalidRequest { detail } => RuntimeError::invalid_argument(detail),
        ReadModelError::State(error) => error.into(),
        error @ ReadModelError::UnsupportedAttributeValue { .. } => RuntimeError::Internal {
            detail: error.to_string(),
        },
    }
}
