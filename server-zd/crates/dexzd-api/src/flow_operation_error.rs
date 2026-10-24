// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_protocol::dex::{ErrorSubStatus, ServiceErrorResponse, WorkerErrorResponse};
use prost::Message;
use tonic::{Code, Status};

/// A FlowService failure, mapped to the gRPC status and `ServiceErrorResponse` the Go Server returns.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum FlowOperationError {
    /// No run of the Flow exists (`NotFound` + `FLOW_NOT_EXISTS`).
    #[error("Flow run is not found for flow ID {flow_id}")]
    FlowNotFound {
        /// The requested Flow ID.
        flow_id: String,
    },
    /// The request or the Flow's state rejected the operation with a specific code and sub-status.
    #[error("{message}")]
    Rejected {
        /// gRPC status code.
        code: Code,
        /// Dex error sub-status.
        sub_status: ErrorSubStatus,
        /// Message for the client.
        message: String,
    },
    /// A Worker RPC handler failed (`FailedPrecondition` + `WORKER_API_ERROR`).
    #[error("worker RPC failed: {}", worker_error.detail)]
    WorkerApiFailed {
        /// The Worker's original error.
        worker_error: WorkerErrorResponse,
    },
    /// Object storage, backpressure, or a stopping node prevented the operation; clients retry.
    #[error("unavailable: {detail}")]
    Unavailable {
        /// What is unavailable.
        detail: String,
    },
    /// The server broke an invariant.
    #[error("internal error: {detail}")]
    Internal {
        /// What broke.
        detail: String,
    },
}

impl FlowOperationError {
    /// Returns `InvalidArgument` + `UNCATEGORIZED`.
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::rejected(
            Code::InvalidArgument,
            ErrorSubStatus::Uncategorized,
            message,
        )
    }

    /// Returns `FailedPrecondition` + `UNCATEGORIZED`.
    pub fn failed_precondition(message: impl Into<String>) -> Self {
        Self::rejected(
            Code::FailedPrecondition,
            ErrorSubStatus::Uncategorized,
            message,
        )
    }

    /// Returns `ResourceExhausted` + `UNCATEGORIZED`.
    pub fn resource_exhausted(message: impl Into<String>) -> Self {
        Self::rejected(
            Code::ResourceExhausted,
            ErrorSubStatus::Uncategorized,
            message,
        )
    }

    /// Returns `DeadlineExceeded` + `LONG_POLL_TIME_OUT`; the client polls again.
    pub fn long_poll_timeout(message: impl Into<String>) -> Self {
        Self::rejected(
            Code::DeadlineExceeded,
            ErrorSubStatus::LongPollTimeOut,
            message,
        )
    }

    /// Returns `Unimplemented` + `UNCATEGORIZED`.
    pub fn unimplemented(message: impl Into<String>) -> Self {
        Self::rejected(Code::Unimplemented, ErrorSubStatus::Uncategorized, message)
    }

    /// Returns a rejection with an explicit code and sub-status.
    pub fn rejected(code: Code, sub_status: ErrorSubStatus, message: impl Into<String>) -> Self {
        Self::Rejected {
            code,
            sub_status,
            message: message.into(),
        }
    }

    /// Converts the error into a gRPC status whose details carry a `ServiceErrorResponse`.
    pub fn into_status(self) -> Status {
        let (code, response) = match self {
            Self::FlowNotFound { flow_id } => (
                Code::NotFound,
                ServiceErrorResponse {
                    detail: format!("Flow run is not found for flow ID {flow_id}"),
                    sub_status: ErrorSubStatus::FlowNotExists.into(),
                    ..ServiceErrorResponse::default()
                },
            ),
            Self::Rejected {
                code,
                sub_status,
                message,
            } => (
                code,
                ServiceErrorResponse {
                    detail: message,
                    sub_status: sub_status.into(),
                    ..ServiceErrorResponse::default()
                },
            ),
            Self::WorkerApiFailed { worker_error } => (
                Code::FailedPrecondition,
                ServiceErrorResponse {
                    detail: String::new(),
                    sub_status: ErrorSubStatus::WorkerApiError.into(),
                    original_worker_error_detail: worker_error.detail,
                    original_worker_error_type: worker_error.error_type,
                    original_worker_error_status: worker_error.grpc_status,
                    original_worker_error_stack_trace: worker_error.stack_trace,
                },
            ),
            Self::Unavailable { detail } => (
                Code::Unavailable,
                ServiceErrorResponse {
                    detail,
                    sub_status: ErrorSubStatus::Uncategorized.into(),
                    ..ServiceErrorResponse::default()
                },
            ),
            Self::Internal { detail } => (
                Code::Internal,
                ServiceErrorResponse {
                    detail,
                    sub_status: ErrorSubStatus::Uncategorized.into(),
                    ..ServiceErrorResponse::default()
                },
            ),
        };
        service_error_status(code, response)
    }
}

impl From<FlowOperationError> for Status {
    fn from(error: FlowOperationError) -> Self {
        error.into_status()
    }
}

/// The `google.rpc.Status` message that gRPC carries in `grpc-status-details-bin`.
#[derive(Clone, PartialEq, Message)]
pub(crate) struct GoogleRpcStatus {
    #[prost(int32, tag = "1")]
    pub(crate) code: i32,
    #[prost(string, tag = "2")]
    pub(crate) message: String,
    #[prost(message, repeated, tag = "3")]
    pub(crate) details: Vec<prost_types::Any>,
}

const SERVICE_ERROR_RESPONSE_TYPE_URL: &str = "type.googleapis.com/dex.ServiceErrorResponse";

/// Builds the status like the Go Server's `ErrorAndStatus.ToGRPCError`: the Worker's detail wins
/// over the server detail in the message.
fn service_error_status(code: Code, response: ServiceErrorResponse) -> Status {
    let message = if response.original_worker_error_detail.is_empty() {
        response.detail.clone()
    } else {
        response.original_worker_error_detail.clone()
    };
    let details = GoogleRpcStatus {
        code: code as i32,
        message: message.clone(),
        details: vec![prost_types::Any {
            type_url: SERVICE_ERROR_RESPONSE_TYPE_URL.to_owned(),
            value: response.encode_to_vec(),
        }],
    };
    Status::with_details(code, message, details.encode_to_vec().into())
}

#[cfg(test)]
mod tests {
    use dexzd_protocol::dex::{ErrorSubStatus, ServiceErrorResponse, WorkerErrorResponse};
    use prost::Message;
    use tonic::Code;

    use super::{FlowOperationError, GoogleRpcStatus, SERVICE_ERROR_RESPONSE_TYPE_URL};

    fn decode(error: FlowOperationError) -> (Code, String, ServiceErrorResponse) {
        let status = error.into_status();
        let details = GoogleRpcStatus::decode(status.details()).expect("status details decode");
        assert_eq!(details.code, status.code() as i32);
        assert_eq!(details.message, status.message());
        assert_eq!(details.details.len(), 1);
        assert_eq!(details.details[0].type_url, SERVICE_ERROR_RESPONSE_TYPE_URL);
        let response = ServiceErrorResponse::decode(details.details[0].value.as_slice())
            .expect("ServiceErrorResponse decodes");
        (status.code(), status.message().to_owned(), response)
    }

    #[test]
    fn every_error_carries_the_go_server_code_and_sub_status() {
        let cases = [
            (
                FlowOperationError::FlowNotFound {
                    flow_id: "order-1".to_owned(),
                },
                Code::NotFound,
                ErrorSubStatus::FlowNotExists,
                "Flow run is not found for flow ID order-1",
            ),
            (
                FlowOperationError::rejected(
                    Code::AlreadyExists,
                    ErrorSubStatus::FlowAlreadyStarted,
                    "already started",
                ),
                Code::AlreadyExists,
                ErrorSubStatus::FlowAlreadyStarted,
                "already started",
            ),
            (
                FlowOperationError::invalid_argument("bad"),
                Code::InvalidArgument,
                ErrorSubStatus::Uncategorized,
                "bad",
            ),
            (
                FlowOperationError::long_poll_timeout("still running"),
                Code::DeadlineExceeded,
                ErrorSubStatus::LongPollTimeOut,
                "still running",
            ),
            (
                FlowOperationError::rejected(
                    Code::DeadlineExceeded,
                    ErrorSubStatus::RequestTimeout,
                    "request timed out",
                ),
                Code::DeadlineExceeded,
                ErrorSubStatus::RequestTimeout,
                "request timed out",
            ),
            (
                FlowOperationError::Unavailable {
                    detail: "object storage".to_owned(),
                },
                Code::Unavailable,
                ErrorSubStatus::Uncategorized,
                "object storage",
            ),
            (
                FlowOperationError::unimplemented("no Continue-As-New"),
                Code::Unimplemented,
                ErrorSubStatus::Uncategorized,
                "no Continue-As-New",
            ),
            (
                FlowOperationError::Internal {
                    detail: "broken".to_owned(),
                },
                Code::Internal,
                ErrorSubStatus::Uncategorized,
                "broken",
            ),
        ];
        for (error, code, sub_status, message) in cases {
            let (actual_code, actual_message, response) = decode(error);
            assert_eq!(actual_code, code);
            assert_eq!(actual_message, message);
            assert_eq!(response.sub_status, sub_status as i32);
            assert_eq!(response.detail, message);
        }
    }

    #[test]
    fn worker_failures_report_the_worker_error_fields() {
        let (code, message, response) = decode(FlowOperationError::WorkerApiFailed {
            worker_error: WorkerErrorResponse {
                detail: "card declined".to_owned(),
                error_type: "PaymentError".to_owned(),
                stack_trace: "at charge".to_owned(),
                retry_after_seconds: 0,
                grpc_status: Code::FailedPrecondition as i32,
            },
        });
        assert_eq!(code, Code::FailedPrecondition);
        assert_eq!(message, "card declined");
        assert_eq!(response.sub_status, ErrorSubStatus::WorkerApiError as i32);
        assert!(response.detail.is_empty());
        assert_eq!(response.original_worker_error_detail, "card declined");
        assert_eq!(response.original_worker_error_type, "PaymentError");
        assert_eq!(response.original_worker_error_stack_trace, "at charge");
        assert_eq!(
            response.original_worker_error_status,
            Code::FailedPrecondition as i32
        );
    }
}
