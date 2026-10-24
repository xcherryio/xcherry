// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;

use dexzd_engine::{
    ATTRIBUTE_REQUEST_TIMEOUT_MESSAGE, STEP_COMPLETION_REQUEST_TIMEOUT_MESSAGE,
    WAIT_CALL_DEADLINE_MESSAGE, WAIT_FOR_FLOW_TIMEOUT_MESSAGE,
    WAIT_FOR_HISTORY_EVENT_TIMEOUT_MESSAGE,
};
use dexzd_keyspace::TenantId;
use dexzd_protocol::dex::flow_service_server::{self, FlowServiceServer};
use dexzd_protocol::dex::{
    DeleteChannelMessageRequest, FlowResult, GetAttributesRequest, GetAttributesResponse,
    GetChannelMessagesRequest, GetChannelMessagesResponse, GetFlowStateRequest,
    GetFlowStateResponse, GetFlowSummaryRequest, GetFlowSummaryResponse, GetHistoryEventsRequest,
    GetHistoryEventsResponse, HealthInfo, InvokeRpcRequest, InvokeRpcResponse,
    ListStreamMessagesRequest, ListStreamMessagesResponse, LoadBlobsRequest, LoadBlobsResponse,
    PublishToChannelRequest, ReadStreamRequest, ReadStreamResponse, ResetFlowRequest,
    ResetFlowResponse, SearchFlowsRequest, SearchFlowsResponse, ServerEngine, ServerInfo,
    SetAttributesRequest, SkipTimerRequest, StartFlowRequest, StartFlowResponse, StopFlowRequest,
    SyncAttributeIndexRequest, SyncAttributeIndexResponse, TriggerContinueAsNewRequest,
    UpdateFlowConfigRequest, WaitForAttributeRequest, WaitForAttributeResponse, WaitForFlowRequest,
    WaitForHistoryEventRequest, WaitForHistoryEventResponse, WaitForStepCompletionRequest,
    WaitForStepCompletionResponse, WriteStreamRequest,
};
use dexzd_search::{SearchBackend, SearchError, SearchRequest};
use tonic::{Request, Response, Status};

use crate::client_deadline::WaitCallDeadline;
use crate::stream_operations::STREAM_READ_TIMEOUT_MESSAGE;
use crate::{
    AttributeIndexes, FlowOperationError, FlowOperations, FlowServiceConfig, StreamOperations,
};

/// Oldest client protocol version this server accepts; equal to the Go Server's.
pub const MINIMUM_SUPPORTED_PROTOCOL_VERSION: u32 = 1;
/// Newest client protocol version this server implements; equal to the Go Server's.
pub const CURRENT_PROTOCOL_VERSION: u32 = 3;

/// Components the FlowService endpoint routes requests to.
pub struct FlowServiceDependencies {
    /// Flow writes, reads, and long polls.
    pub flow_operations: Arc<dyn FlowOperations>,
    /// Stream RPCs.
    pub stream_operations: StreamOperations,
    /// Attribute index registration and the search field registry.
    pub attribute_indexes: Arc<AttributeIndexes>,
    /// SearchFlows backend; `None` makes SearchFlows return `Unimplemented`.
    pub search_backend: Option<Arc<dyn SearchBackend>>,
}

/// The public `dex.FlowService` of server-zd.
///
/// Every request uses the default tenant until authentication assigns tenants.
pub struct DexFlowService {
    config: FlowServiceConfig,
    dependencies: FlowServiceDependencies,
}

impl DexFlowService {
    /// Creates the endpoint.
    pub fn new(config: &FlowServiceConfig, dependencies: FlowServiceDependencies) -> Self {
        Self {
            config: config.clone(),
            dependencies,
        }
    }

    /// Wraps the endpoint in a tonic server that accepts messages up to `max_message_bytes`.
    pub fn into_server(self, max_message_bytes: usize) -> FlowServiceServer<Self> {
        FlowServiceServer::new(self)
            .max_decoding_message_size(max_message_bytes)
            .max_encoding_message_size(max_message_bytes)
    }

    fn operations(&self) -> &dyn FlowOperations {
        self.dependencies.flow_operations.as_ref()
    }

    async fn search_flows(
        &self,
        request: SearchFlowsRequest,
    ) -> Result<SearchFlowsResponse, FlowOperationError> {
        if request.page_size < 0 {
            return Err(FlowOperationError::invalid_argument(
                "page size must be non-negative",
            ));
        }
        let Some(search_backend) = &self.dependencies.search_backend else {
            return Err(FlowOperationError::unimplemented(
                "SearchFlows requires a configured search backend",
            ));
        };
        let page_size = match usize::try_from(request.page_size) {
            Ok(0) | Err(_) => self.config.default_search_page_size,
            Ok(page_size) => page_size,
        };
        let field_registry = self
            .dependencies
            .attribute_indexes
            .field_registry(TenantId::DEFAULT)
            .await?;
        let page = search_backend
            .search(SearchRequest {
                tenant_id: TenantId::DEFAULT,
                query: &request.query,
                page_size,
                page_token: Some(request.next_page_token.as_str())
                    .filter(|token| !token.is_empty()),
                field_registry: &field_registry,
            })
            .await
            .map_err(search_error)?;
        Ok(SearchFlowsResponse {
            flow_runs: page.entries,
            next_page_token: page.next_page_token.unwrap_or_default(),
        })
    }
}

#[tonic::async_trait]
impl flow_service_server::FlowService for DexFlowService {
    async fn get_server_info(&self, _request: Request<()>) -> Result<Response<ServerInfo>, Status> {
        Ok(Response::new(ServerInfo {
            server_version: self.config.server_version.clone(),
            minimum_supported_protocol_version: MINIMUM_SUPPORTED_PROTOCOL_VERSION,
            current_protocol_version: CURRENT_PROTOCOL_VERSION,
            server_engine: ServerEngine::Zd.into(),
        }))
    }

    async fn start_flow(
        &self,
        request: Request<StartFlowRequest>,
    ) -> Result<Response<StartFlowResponse>, Status> {
        respond(
            self.operations()
                .start_flow(TenantId::DEFAULT, request.into_inner())
                .await,
        )
    }

    async fn publish_to_channel(
        &self,
        request: Request<PublishToChannelRequest>,
    ) -> Result<Response<()>, Status> {
        respond(
            self.operations()
                .publish_to_channel(TenantId::DEFAULT, request.into_inner())
                .await,
        )
    }

    async fn get_channel_messages(
        &self,
        request: Request<GetChannelMessagesRequest>,
    ) -> Result<Response<GetChannelMessagesResponse>, Status> {
        respond(
            self.operations()
                .get_channel_messages(TenantId::DEFAULT, request.into_inner())
                .await,
        )
    }

    async fn delete_channel_message(
        &self,
        request: Request<DeleteChannelMessageRequest>,
    ) -> Result<Response<()>, Status> {
        respond(
            self.operations()
                .delete_channel_message(TenantId::DEFAULT, request.into_inner())
                .await,
        )
    }

    async fn write_stream(
        &self,
        request: Request<WriteStreamRequest>,
    ) -> Result<Response<()>, Status> {
        respond(
            self.dependencies
                .stream_operations
                .write_stream(TenantId::DEFAULT, request.into_inner())
                .await,
        )
    }

    async fn read_stream(
        &self,
        request: Request<ReadStreamRequest>,
    ) -> Result<Response<ReadStreamResponse>, Status> {
        let deadline = WaitCallDeadline::new(
            &request,
            0,
            STREAM_READ_TIMEOUT_MESSAGE,
            STREAM_READ_TIMEOUT_MESSAGE,
        );
        respond(
            deadline
                .bound(
                    self.dependencies
                        .stream_operations
                        .read_stream(TenantId::DEFAULT, request.into_inner()),
                )
                .await,
        )
    }

    async fn list_stream_messages(
        &self,
        request: Request<ListStreamMessagesRequest>,
    ) -> Result<Response<ListStreamMessagesResponse>, Status> {
        respond(
            self.dependencies
                .stream_operations
                .list_stream_messages(TenantId::DEFAULT, request.into_inner())
                .await,
        )
    }

    async fn stop_flow(&self, request: Request<StopFlowRequest>) -> Result<Response<()>, Status> {
        respond(
            self.operations()
                .stop_flow(TenantId::DEFAULT, request.into_inner())
                .await,
        )
    }

    async fn get_attributes(
        &self,
        request: Request<GetAttributesRequest>,
    ) -> Result<Response<GetAttributesResponse>, Status> {
        respond(
            self.operations()
                .get_attributes(TenantId::DEFAULT, request.into_inner())
                .await,
        )
    }

    async fn set_attributes(
        &self,
        request: Request<SetAttributesRequest>,
    ) -> Result<Response<()>, Status> {
        respond(
            self.operations()
                .set_attributes(TenantId::DEFAULT, request.into_inner())
                .await,
        )
    }

    async fn load_blobs(
        &self,
        request: Request<LoadBlobsRequest>,
    ) -> Result<Response<LoadBlobsResponse>, Status> {
        respond(
            self.operations()
                .load_blobs(TenantId::DEFAULT, request.into_inner())
                .await,
        )
    }

    async fn wait_for_flow(
        &self,
        request: Request<WaitForFlowRequest>,
    ) -> Result<Response<FlowResult>, Status> {
        let deadline = WaitCallDeadline::new(
            &request,
            0,
            WAIT_FOR_FLOW_TIMEOUT_MESSAGE,
            WAIT_FOR_FLOW_TIMEOUT_MESSAGE,
        );
        respond(
            deadline
                .bound(
                    self.operations()
                        .wait_for_flow(TenantId::DEFAULT, request.into_inner()),
                )
                .await,
        )
    }

    async fn search_flows(
        &self,
        request: Request<SearchFlowsRequest>,
    ) -> Result<Response<SearchFlowsResponse>, Status> {
        respond(DexFlowService::search_flows(self, request.into_inner()).await)
    }

    async fn sync_attribute_indexes(
        &self,
        request: Request<SyncAttributeIndexRequest>,
    ) -> Result<Response<SyncAttributeIndexResponse>, Status> {
        respond(
            self.dependencies
                .attribute_indexes
                .sync_attribute_indexes(TenantId::DEFAULT, request.get_ref())
                .await
                .map(|()| SyncAttributeIndexResponse {}),
        )
    }

    async fn get_flow_summary(
        &self,
        request: Request<GetFlowSummaryRequest>,
    ) -> Result<Response<GetFlowSummaryResponse>, Status> {
        respond(
            self.operations()
                .get_flow_summary(TenantId::DEFAULT, request.into_inner())
                .await,
        )
    }

    async fn get_history_events(
        &self,
        request: Request<GetHistoryEventsRequest>,
    ) -> Result<Response<GetHistoryEventsResponse>, Status> {
        respond(
            self.operations()
                .get_history_events(TenantId::DEFAULT, request.into_inner())
                .await,
        )
    }

    async fn wait_for_history_event(
        &self,
        request: Request<WaitForHistoryEventRequest>,
    ) -> Result<Response<WaitForHistoryEventResponse>, Status> {
        let deadline = WaitCallDeadline::new(
            &request,
            0,
            WAIT_FOR_HISTORY_EVENT_TIMEOUT_MESSAGE,
            WAIT_FOR_HISTORY_EVENT_TIMEOUT_MESSAGE,
        );
        respond(
            deadline
                .bound(
                    self.operations()
                        .wait_for_history_event(TenantId::DEFAULT, request.into_inner()),
                )
                .await,
        )
    }

    async fn get_flow_state(
        &self,
        request: Request<GetFlowStateRequest>,
    ) -> Result<Response<GetFlowStateResponse>, Status> {
        respond(
            self.operations()
                .get_flow_state(TenantId::DEFAULT, request.into_inner())
                .await,
        )
    }

    async fn reset_flow(
        &self,
        request: Request<ResetFlowRequest>,
    ) -> Result<Response<ResetFlowResponse>, Status> {
        respond(
            self.operations()
                .reset_flow(TenantId::DEFAULT, request.into_inner())
                .await,
        )
    }

    async fn invoke_rpc(
        &self,
        request: Request<InvokeRpcRequest>,
    ) -> Result<Response<InvokeRpcResponse>, Status> {
        respond(
            self.operations()
                .invoke_rpc(TenantId::DEFAULT, request.into_inner())
                .await,
        )
    }

    async fn skip_timer(&self, request: Request<SkipTimerRequest>) -> Result<Response<()>, Status> {
        respond(
            self.operations()
                .skip_timer(TenantId::DEFAULT, request.into_inner())
                .await,
        )
    }

    async fn update_flow_config(
        &self,
        request: Request<UpdateFlowConfigRequest>,
    ) -> Result<Response<()>, Status> {
        respond(
            self.operations()
                .update_flow_config(TenantId::DEFAULT, request.into_inner())
                .await,
        )
    }

    async fn wait_for_step_completion(
        &self,
        request: Request<WaitForStepCompletionRequest>,
    ) -> Result<Response<WaitForStepCompletionResponse>, Status> {
        let deadline = WaitCallDeadline::new(
            &request,
            request.get_ref().request_timeout_seconds,
            STEP_COMPLETION_REQUEST_TIMEOUT_MESSAGE,
            WAIT_CALL_DEADLINE_MESSAGE,
        );
        respond(
            deadline
                .bound(
                    self.operations()
                        .wait_for_step_completion(TenantId::DEFAULT, request.into_inner()),
                )
                .await,
        )
    }

    async fn wait_for_attribute(
        &self,
        request: Request<WaitForAttributeRequest>,
    ) -> Result<Response<WaitForAttributeResponse>, Status> {
        let deadline = WaitCallDeadline::new(
            &request,
            request.get_ref().request_timeout_seconds,
            ATTRIBUTE_REQUEST_TIMEOUT_MESSAGE,
            WAIT_CALL_DEADLINE_MESSAGE,
        );
        respond(
            deadline
                .bound(
                    self.operations()
                        .wait_for_attribute(TenantId::DEFAULT, request.into_inner()),
                )
                .await,
        )
    }

    async fn trigger_continue_as_new(
        &self,
        _request: Request<TriggerContinueAsNewRequest>,
    ) -> Result<Response<()>, Status> {
        Err(FlowOperationError::unimplemented("server-zd does not support Continue-As-New").into())
    }

    async fn health_check(&self, _request: Request<()>) -> Result<Response<HealthInfo>, Status> {
        Ok(Response::new(HealthInfo {
            condition: "OK".to_owned(),
            hostname: self.config.node_name.clone(),
            duration: 0,
        }))
    }
}

fn respond<T>(result: Result<T, FlowOperationError>) -> Result<Response<T>, Status> {
    result
        .map(Response::new)
        .map_err(FlowOperationError::into_status)
}

fn search_error(error: SearchError) -> FlowOperationError {
    match error {
        SearchError::InvalidArgument(message) => FlowOperationError::invalid_argument(message),
        SearchError::ResourceExhausted(message) => FlowOperationError::resource_exhausted(message),
        SearchError::Unavailable(detail) => FlowOperationError::Unavailable { detail },
        other => FlowOperationError::Internal {
            detail: other.to_string(),
        },
    }
}
