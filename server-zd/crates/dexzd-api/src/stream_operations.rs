// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::time::{Duration, Instant};

use dexzd_keyspace::TenantId;
use dexzd_protocol::dex::{
    ListStreamMessagesRequest, ListStreamMessagesResponse, ReadStreamRequest, ReadStreamResponse,
    StreamMessage as ProtoStreamMessage, WriteStreamRequest, value,
};
use dexzd_stream_store::{StreamMessage, StreamStore, StreamStoreError, WriteStreamInput};

use crate::FlowOperationError;

/// Serves the Stream RPCs from the node's Stream Store.
///
/// Streams do not depend on Flows, so these calls bypass the runtime.
pub struct StreamOperations {
    stream_store: std::sync::Arc<StreamStore>,
    max_wait: Duration,
}

impl StreamOperations {
    /// Creates Stream operations; ReadStream waits at most `max_wait`, also when it asks for 0.
    pub fn new(stream_store: std::sync::Arc<StreamStore>, max_wait: Duration) -> Self {
        Self {
            stream_store,
            max_wait,
        }
    }

    /// Appends a message after the Go Server's request checks.
    pub async fn write_stream(
        &self,
        tenant_id: TenantId,
        request: WriteStreamRequest,
    ) -> Result<(), FlowOperationError> {
        if request.flow_id.is_empty()
            || request.flow_type.is_empty()
            || request.stream_name.is_empty()
        {
            return Err(FlowOperationError::invalid_argument(
                "flow ID, Flow type, and Stream name are required",
            ));
        }
        if request.stream_capacity_bytes <= 0 {
            return Err(FlowOperationError::invalid_argument(
                "max estimated bytes must be positive",
            ));
        }
        let Some(stream_value) = request.value else {
            return Err(FlowOperationError::invalid_argument(
                "Stream Value is required",
            ));
        };
        if request.source.is_empty() {
            return Err(FlowOperationError::invalid_argument(
                "Stream source is required",
            ));
        }
        if matches!(
            stream_value.kind,
            Some(
                value::Kind::InternalBlobIdForStringValue(_)
                    | value::Kind::InternalBlobIdForObjValue(_)
            )
        ) {
            return Err(FlowOperationError::invalid_argument(
                "worker response must not contain internal_blob_id arms",
            ));
        }
        self.stream_store
            .write(WriteStreamInput {
                tenant_id: tenant_id.0,
                flow_id: request.flow_id,
                flow_type: request.flow_type,
                stream_name: request.stream_name,
                stream_capacity_bytes: Some(request.stream_capacity_bytes),
                value: stream_value,
                source: request.source,
            })
            .await
            .map_err(stream_store_error)
    }

    /// Returns the first message after the resume token, waiting up to the request's wait time.
    ///
    /// Returns `DeadlineExceeded` + `LONG_POLL_TIME_OUT` when no message arrives in time.
    pub async fn read_stream(
        &self,
        tenant_id: TenantId,
        request: ReadStreamRequest,
    ) -> Result<ReadStreamResponse, FlowOperationError> {
        if request.flow_id.is_empty()
            || request.flow_type.is_empty()
            || request.stream_name.is_empty()
            || request.wait_time_seconds < 0
        {
            return Err(FlowOperationError::invalid_argument(
                "flow ID, Flow type, Stream name, and non-negative wait time are required",
            ));
        }
        let requested_wait = u64::try_from(request.wait_time_seconds)
            .ok()
            .filter(|seconds| *seconds > 0)
            .map_or(self.max_wait, Duration::from_secs);
        let deadline = Instant::now() + requested_wait.min(self.max_wait);
        let message = self
            .stream_store
            .read(
                tenant_id.0,
                &request.flow_type,
                &request.flow_id,
                &request.stream_name,
                &request.resume_token,
                deadline,
            )
            .await
            .map_err(stream_store_error)?
            .ok_or_else(|| FlowOperationError::long_poll_timeout("Stream read timed out"))?;
        Ok(ReadStreamResponse {
            message: Some(proto_stream_message(message)),
        })
    }

    /// Lists retained messages newest first.
    pub async fn list_stream_messages(
        &self,
        tenant_id: TenantId,
        request: ListStreamMessagesRequest,
    ) -> Result<ListStreamMessagesResponse, FlowOperationError> {
        if request.flow_id.is_empty()
            || request.flow_type.is_empty()
            || request.stream_name.is_empty()
        {
            return Err(FlowOperationError::invalid_argument(
                "flow ID, Flow type, and Stream name are required",
            ));
        }
        let page_size = usize::try_from(request.page_size).unwrap_or(0);
        let page = self
            .stream_store
            .list(
                tenant_id.0,
                &request.flow_type,
                &request.flow_id,
                &request.stream_name,
                page_size,
                &request.before_page_token,
            )
            .await
            .map_err(stream_store_error)?;
        Ok(ListStreamMessagesResponse {
            messages: page
                .messages
                .into_iter()
                .map(proto_stream_message)
                .collect(),
            next_page_token: page.next_page_token,
        })
    }
}

fn proto_stream_message(message: StreamMessage) -> ProtoStreamMessage {
    ProtoStreamMessage {
        value: Some(message.value),
        resume_token: message.resume_token,
        created_time: Some(prost_types::Timestamp::from(message.created_time)),
        source: message.source,
    }
}

fn stream_store_error(error: StreamStoreError) -> FlowOperationError {
    match error {
        StreamStoreError::Disabled => FlowOperationError::failed_precondition(error.to_string()),
        StreamStoreError::InvalidArgument(_) | StreamStoreError::InvalidResumeToken => {
            FlowOperationError::invalid_argument(error.to_string())
        }
        StreamStoreError::MessageTooLarge | StreamStoreError::CapacityExceeded => {
            FlowOperationError::resource_exhausted(error.to_string())
        }
        StreamStoreError::Closed | StreamStoreError::TrimWorkerFailed => {
            FlowOperationError::Unavailable {
                detail: error.to_string(),
            }
        }
        StreamStoreError::InvalidConfig(_) => FlowOperationError::Internal {
            detail: error.to_string(),
        },
    }
}
