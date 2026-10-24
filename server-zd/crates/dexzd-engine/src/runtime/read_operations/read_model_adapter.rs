// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Adapters to apply's read model for GetFlowState and GetHistoryEvents; it has not landed yet.

use dexzd_apply::{ApplyContext, FlowStateReader};
use dexzd_protocol::dex::{
    ErrorSubStatus, GetFlowStateRequest, GetFlowStateResponse, GetHistoryEventsRequest,
    GetHistoryEventsResponse,
};

use crate::runtime::{RuntimeError, grpc_code};

/// Returns the Flow state projected by apply's read model.
///
/// Returns `UNIMPLEMENTED` until the read model lands.
pub(crate) fn read_flow_state(
    _state: &dyn FlowStateReader,
    _context: ApplyContext,
    request: &GetFlowStateRequest,
) -> Result<GetFlowStateResponse, RuntimeError> {
    Err(read_model_unavailable("GetFlowState", &request.flow_id))
}

/// Returns history events projected by apply's read model.
///
/// Returns `UNIMPLEMENTED` until the read model lands.
pub(crate) fn read_history_events(
    _state: &dyn FlowStateReader,
    _context: ApplyContext,
    request: &GetHistoryEventsRequest,
) -> Result<GetHistoryEventsResponse, RuntimeError> {
    Err(read_model_unavailable("GetHistoryEvents", &request.flow_id))
}

fn read_model_unavailable(operation: &str, flow_id: &str) -> RuntimeError {
    RuntimeError::rejected(
        grpc_code::UNIMPLEMENTED,
        ErrorSubStatus::Uncategorized,
        format!("server-zd cannot serve {operation} yet (Flow {flow_id})"),
    )
}
