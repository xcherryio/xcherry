// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! FlowService gRPC handlers for server-zd: request validation, routing to the engine, long polling, and error mapping.

mod attribute_indexes;
mod client_deadline;
mod flow_operation_error;
mod flow_operations;
mod flow_service;
mod flow_service_config;
mod health_status;
mod runtime_flow_operations;
mod stream_operations;

pub use attribute_indexes::AttributeIndexes;
pub use client_deadline::{CLIENT_DEADLINE_ANSWER_MARGIN, ClientDeadline};
pub use flow_operation_error::FlowOperationError;
pub use flow_operations::FlowOperations;
pub use flow_service::{
    CURRENT_PROTOCOL_VERSION, DexFlowService, FlowServiceDependencies,
    MINIMUM_SUPPORTED_PROTOCOL_VERSION,
};
pub use flow_service_config::FlowServiceConfig;
mod request_metrics;
pub use health_status::{HEALTH_SERVICE_NAMES, HealthStatus};
pub use request_metrics::RequestMetricsLayer;
pub use stream_operations::StreamOperations;
