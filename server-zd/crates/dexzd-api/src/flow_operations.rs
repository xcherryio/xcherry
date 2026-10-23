// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use async_trait::async_trait;
use dexzd_keyspace::TenantId;
use dexzd_protocol::dex::{
    DeleteChannelMessageRequest, FlowResult, GetAttributesRequest, GetAttributesResponse,
    GetChannelMessagesRequest, GetChannelMessagesResponse, GetFlowStateRequest,
    GetFlowStateResponse, GetFlowSummaryRequest, GetFlowSummaryResponse, GetHistoryEventsRequest,
    GetHistoryEventsResponse, InvokeRpcRequest, InvokeRpcResponse, LoadBlobsRequest,
    LoadBlobsResponse, PublishToChannelRequest, ResetFlowRequest, ResetFlowResponse,
    SetAttributesRequest, SkipTimerRequest, StartFlowRequest, StartFlowResponse, StopFlowRequest,
    UpdateFlowConfigRequest, WaitForAttributeRequest, WaitForAttributeResponse, WaitForFlowRequest,
    WaitForHistoryEventRequest, WaitForHistoryEventResponse, WaitForStepCompletionRequest,
    WaitForStepCompletionResponse,
};

use crate::FlowOperationError;

/// Flow writes, reads, and long polls behind FlowService.
///
/// Implementations validate requests; dropping a future stops its wait.
#[async_trait]
pub trait FlowOperations: Send + Sync + 'static {
    /// Starts a Flow run.
    async fn start_flow(
        &self,
        tenant_id: TenantId,
        request: StartFlowRequest,
    ) -> Result<StartFlowResponse, FlowOperationError>;

    /// Stops a Flow run.
    async fn stop_flow(
        &self,
        tenant_id: TenantId,
        request: StopFlowRequest,
    ) -> Result<(), FlowOperationError>;

    /// Writes Attributes.
    async fn set_attributes(
        &self,
        tenant_id: TenantId,
        request: SetAttributesRequest,
    ) -> Result<(), FlowOperationError>;

    /// Reads Attributes.
    async fn get_attributes(
        &self,
        tenant_id: TenantId,
        request: GetAttributesRequest,
    ) -> Result<GetAttributesResponse, FlowOperationError>;

    /// Publishes Channel messages.
    async fn publish_to_channel(
        &self,
        tenant_id: TenantId,
        request: PublishToChannelRequest,
    ) -> Result<(), FlowOperationError>;

    /// Reads queued Channel messages.
    async fn get_channel_messages(
        &self,
        tenant_id: TenantId,
        request: GetChannelMessagesRequest,
    ) -> Result<GetChannelMessagesResponse, FlowOperationError>;

    /// Deletes a queued Channel message.
    async fn delete_channel_message(
        &self,
        tenant_id: TenantId,
        request: DeleteChannelMessageRequest,
    ) -> Result<(), FlowOperationError>;

    /// Reads values that responses returned as references.
    async fn load_blobs(
        &self,
        tenant_id: TenantId,
        request: LoadBlobsRequest,
    ) -> Result<LoadBlobsResponse, FlowOperationError>;

    /// Waits until the run closes or the wait time ends.
    async fn wait_for_flow(
        &self,
        tenant_id: TenantId,
        request: WaitForFlowRequest,
    ) -> Result<FlowResult, FlowOperationError>;

    /// Reads a run summary.
    async fn get_flow_summary(
        &self,
        tenant_id: TenantId,
        request: GetFlowSummaryRequest,
    ) -> Result<GetFlowSummaryResponse, FlowOperationError>;

    /// Reads history events.
    async fn get_history_events(
        &self,
        tenant_id: TenantId,
        request: GetHistoryEventsRequest,
    ) -> Result<GetHistoryEventsResponse, FlowOperationError>;

    /// Waits for a history event.
    async fn wait_for_history_event(
        &self,
        tenant_id: TenantId,
        request: WaitForHistoryEventRequest,
    ) -> Result<WaitForHistoryEventResponse, FlowOperationError>;

    /// Reads the current Flow state.
    async fn get_flow_state(
        &self,
        tenant_id: TenantId,
        request: GetFlowStateRequest,
    ) -> Result<GetFlowStateResponse, FlowOperationError>;

    /// Time travels a run to an earlier point.
    async fn reset_flow(
        &self,
        tenant_id: TenantId,
        request: ResetFlowRequest,
    ) -> Result<ResetFlowResponse, FlowOperationError>;

    /// Invokes a Flow RPC on a Worker.
    async fn invoke_rpc(
        &self,
        tenant_id: TenantId,
        request: InvokeRpcRequest,
    ) -> Result<InvokeRpcResponse, FlowOperationError>;

    /// Fires a pending timer early.
    async fn skip_timer(
        &self,
        tenant_id: TenantId,
        request: SkipTimerRequest,
    ) -> Result<(), FlowOperationError>;

    /// Changes the Flow configuration.
    async fn update_flow_config(
        &self,
        tenant_id: TenantId,
        request: UpdateFlowConfigRequest,
    ) -> Result<(), FlowOperationError>;

    /// Waits until a Step execution completes.
    async fn wait_for_step_completion(
        &self,
        tenant_id: TenantId,
        request: WaitForStepCompletionRequest,
    ) -> Result<WaitForStepCompletionResponse, FlowOperationError>;

    /// Waits until an Attribute matches.
    async fn wait_for_attribute(
        &self,
        tenant_id: TenantId,
        request: WaitForAttributeRequest,
    ) -> Result<WaitForAttributeResponse, FlowOperationError>;
}
