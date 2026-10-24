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
};

use crate::runtime::read_operations::state_rows::read_run;
use crate::runtime::{FlowAddress, RuntimeError};

/// Returns the Flow state projected by apply's read model.
///
/// Returns `NOT_FOUND` + `FLOW_NOT_EXISTS` when the run does not exist.
pub(crate) fn read_flow_state(
    state: &dyn FlowStateReader,
    context: ApplyContext,
    flow: &FlowAddress,
    request: &GetFlowStateRequest,
) -> Result<GetFlowStateResponse, RuntimeError> {
    let run = read_run(state, context, flow, &request.run_id)?;
    get_flow_state(state, context.flow_hash, run.run_id)
        .map_err(|error| read_model_error(flow, error))
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
