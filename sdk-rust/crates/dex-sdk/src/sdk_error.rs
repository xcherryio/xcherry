// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::error::Error;
use std::fmt::{Display, Formatter};

use dex_protocol::dex::{ErrorSubStatus as ProtoErrorSubStatus, ServiceErrorResponse};
use prost::Message;
use prost_types::Any;
use tonic::Status;

use crate::GrpcCode;

/// Result returned by fallible Dex SDK operations.
pub type SdkResult<T> = Result<T, SdkError>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Provides Dex-specific detail beyond a gRPC status code.
pub enum ErrorSubStatus {
    /// Dex returned no specific status.
    Uncategorized,
    /// A start request targeted an already-existing Flow ID.
    FlowAlreadyStarted,
    /// The requested Flow does not exist or is no longer active.
    FlowNotExists,
    /// A Worker Step or RPC invocation failed.
    WorkerApiError,
    /// A long-poll deadline elapsed while the Flow remained active.
    LongPollTimeout,
    /// A pending Channel message ID no longer exists.
    ChannelMessageNotFound,
    /// A caller-defined request budget expired.
    RequestTimeout,
}

#[derive(Clone, Debug)]
/// Preserves one failed Dex service operation and its transport source.
pub struct ServiceError {
    code: GrpcCode,
    sub_status: ErrorSubStatus,
    detail: String,
    operation: &'static str,
    flow_id: Option<String>,
    source: Status,
    pull_registration_error: Option<Box<crate::PullRegistrationError>>,
}

impl ServiceError {
    /// Returns the outer gRPC status code.
    pub fn code(&self) -> GrpcCode {
        self.code
    }

    /// Returns the decoded Dex-specific status.
    pub fn sub_status(&self) -> ErrorSubStatus {
        self.sub_status
    }

    /// Returns the most specific server or transport detail available.
    pub fn detail(&self) -> &str {
        &self.detail
    }

    /// Returns the SDK operation name that failed.
    pub fn operation(&self) -> &str {
        self.operation
    }

    /// Returns the targeted Flow ID when the operation had one.
    pub fn flow_id(&self) -> Option<&str> {
        self.flow_id.as_deref()
    }

    pub(crate) fn local(operation: &'static str, detail: impl Into<String>) -> Self {
        let detail = detail.into();
        Self {
            code: GrpcCode::Unknown,
            sub_status: ErrorSubStatus::Uncategorized,
            detail: detail.clone(),
            operation,
            flow_id: None,
            source: Status::unknown(detail),
            pull_registration_error: None,
        }
    }
}

impl Display for ServiceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl Error for ServiceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.pull_registration_error {
            Some(rejection) => Some(rejection.as_ref()),
            None => Some(&self.source),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Preserves the original Worker-side error carried by a service failure.
pub struct WorkerError {
    code: Option<GrpcCode>,
    error_type: String,
    detail: String,
}

impl WorkerError {
    /// Returns the Worker gRPC status when available.
    pub fn code(&self) -> Option<GrpcCode> {
        self.code
    }

    /// Returns the Worker exception or error type name.
    pub fn error_type(&self) -> &str {
        &self.error_type
    }

    /// Returns the Worker-provided failure detail.
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

#[derive(Clone, Debug)]
/// Reports service, definition, value, handler, and Flow-completion failures.
///
/// Match specific variants when recovery differs. [`Self::service_error`] extracts common gRPC
/// metadata from every service-backed variant.
pub enum SdkError {
    /// A general Dex service or transport call failed.
    Service {
        /// Structured service failure.
        service: ServiceError,
    },
    /// Starting a Flow conflicted with an existing Flow ID.
    FlowAlreadyStarted {
        /// Structured start-operation failure.
        service: ServiceError,
    },
    /// An operation requiring an existing Flow targeted an unknown ID.
    FlowNotFound {
        /// Structured lookup failure.
        service: ServiceError,
    },
    /// A Flow was not found or the operation could not use its closed execution.
    ///
    /// RPC calls also use this variant for missing query-only targets. A query-only RPC without
    /// locks, transactionality, durable effects, or Server-forced Update routing can read a retained
    /// terminal execution. At that confirmed read boundary, treat this variant as not found without
    /// a lifecycle probe. Preserve other service and Worker failures.
    ///
    /// For mutations and active-only RPCs, this variant does not distinguish missing from closed
    /// targets or prove that the requested action succeeded. Match it as follows:
    ///
    /// ```no_run
    /// # use dex_sdk::{SdkError, SdkResult};
    /// fn report_unavailable(result: SdkResult<()>) -> SdkResult<()> {
    ///     match result {
    ///         Err(SdkError::FlowNotActiveOrNotFound { service }) => {
    ///             eprintln!("{} has no usable target", service.operation());
    ///             Err(SdkError::FlowNotActiveOrNotFound { service })
    ///         }
    ///         other => other,
    ///     }
    /// }
    /// ```
    FlowNotActiveOrNotFound {
        /// Structured target failure, including the original transport cause.
        service: ServiceError,
    },
    /// A Worker Step or RPC handler failed.
    WorkerInvocation {
        /// Outer Dex service failure.
        service: ServiceError,
        /// Original Worker-side failure metadata.
        worker: Box<WorkerError>,
    },
    /// A locking RPC could not acquire all requested Attribute locks.
    RpcLockConflict {
        /// Structured aborted-operation failure.
        service: ServiceError,
    },
    /// A long-poll timeout elapsed without indicating a Flow failure.
    LongPollTimeout {
        /// Structured timeout failure.
        service: ServiceError,
    },
    /// A requested Channel message is no longer pending.
    ChannelMessageNotFound {
        /// Structured deletion failure.
        service: ServiceError,
    },
    /// A caller-defined request budget expired.
    RequestTimeout {
        /// Structured timeout failure.
        service: ServiceError,
    },
    /// A registered Flow, Step, RPC, or persistence definition is invalid.
    FlowDefinition {
        /// Developer-actionable contract violation.
        message: String,
    },
    /// An SDK method received an invalid argument.
    InvalidArgument {
        /// Developer-actionable argument violation.
        message: String,
    },
    /// An application value could not be serialized or decoded.
    ValueMapping {
        /// Developer-actionable mapping failure.
        message: String,
    },
    /// A Step returned a value that violates its handler contract.
    InvalidStepResult {
        /// Flow type containing the Step.
        flow_type: String,
        /// Step type that returned the invalid value.
        step_type: String,
        /// Developer-actionable result violation.
        detail: String,
    },
}

impl Display for SdkError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Service { service }
            | Self::FlowAlreadyStarted { service }
            | Self::FlowNotFound { service }
            | Self::FlowNotActiveOrNotFound { service }
            | Self::RpcLockConflict { service }
            | Self::LongPollTimeout { service }
            | Self::ChannelMessageNotFound { service } => Display::fmt(service, formatter),
            Self::RequestTimeout { service } => Display::fmt(service, formatter),
            Self::WorkerInvocation { service, .. } => Display::fmt(service, formatter),
            Self::InvalidStepResult { detail, .. } => formatter.write_str(detail),
            Self::FlowDefinition { message }
            | Self::InvalidArgument { message }
            | Self::ValueMapping { message } => formatter.write_str(message),
        }
    }
}

impl Error for SdkError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.service_error()
            .map(|error| error as &(dyn Error + 'static))
    }
}

impl SdkError {
    /// Returns the typed pull queue rejection when a permanent error leaves no registered stream.
    ///
    /// Registration waiters and opt-in Worker shutdown return the existing [`Self::Service`]
    /// variant. This accessor preserves its queue name and original gRPC status. Other errors
    /// return `None`. See [`crate::PullRegistrationError`] for a complete inspection example.
    pub fn pull_registration_error(&self) -> Option<&crate::PullRegistrationError> {
        self.service_error()
            .and_then(|service| service.pull_registration_error.as_deref())
    }

    pub(crate) fn from_pull_registration(rejection: crate::PullRegistrationError) -> Self {
        Self::Service {
            service: ServiceError {
                code: rejection.status.code(),
                sub_status: ErrorSubStatus::Uncategorized,
                detail: rejection.to_string(),
                operation: "pull_worker_registration",
                flow_id: None,
                source: rejection.status.clone(),
                pull_registration_error: Some(Box::new(rejection)),
            },
        }
    }

    /// Returns shared service metadata for service-backed variants.
    ///
    /// Local definition, argument, mapping, and invalid-result errors return `None`.
    pub fn service_error(&self) -> Option<&ServiceError> {
        match self {
            Self::Service { service }
            | Self::FlowAlreadyStarted { service }
            | Self::FlowNotFound { service }
            | Self::FlowNotActiveOrNotFound { service }
            | Self::RpcLockConflict { service }
            | Self::LongPollTimeout { service }
            | Self::ChannelMessageNotFound { service } => Some(service),
            Self::RequestTimeout { service } => Some(service),
            Self::WorkerInvocation { service, .. } => Some(service),
            _ => None,
        }
    }

    pub(crate) fn from_status(
        status: Status,
        operation: &'static str,
        flow_id: Option<&str>,
        requirement: FlowTargetRequirement,
    ) -> Self {
        let code = status.code();
        let decoded = decode_details(status.details());
        let details = decoded.as_ref().ok().and_then(|details| details.as_ref());
        let detail = details
            .filter(|details| !details.detail.is_empty())
            .map_or_else(
                || {
                    if status.message().is_empty() {
                        format!("{code:?}")
                    } else {
                        status.message().to_string()
                    }
                },
                |details| details.detail.clone(),
            );
        let sub_status = details
            .map(|details| map_sub_status(details.sub_status))
            .unwrap_or(ErrorSubStatus::Uncategorized);
        let service = ServiceError {
            code,
            sub_status,
            detail,
            operation,
            flow_id: flow_id.map(str::to_string),
            source: status,
            pull_registration_error: None,
        };
        if decoded.is_err() {
            return Self::Service { service };
        }
        match sub_status {
            ErrorSubStatus::FlowAlreadyStarted => Self::FlowAlreadyStarted { service },
            ErrorSubStatus::FlowNotExists => match requirement {
                FlowTargetRequirement::Active => Self::FlowNotActiveOrNotFound { service },
                FlowTargetRequirement::Existing => Self::FlowNotFound { service },
                FlowTargetRequirement::None => Self::Service { service },
            },
            ErrorSubStatus::WorkerApiError if code == GrpcCode::Aborted => {
                Self::RpcLockConflict { service }
            }
            ErrorSubStatus::WorkerApiError => {
                let details = details.expect("worker details were decoded");
                Self::WorkerInvocation {
                    service,
                    worker: Box::new(WorkerError {
                        code: (details.original_worker_error_status != 0)
                            .then(|| GrpcCode::from_i32(details.original_worker_error_status)),
                        error_type: details.original_worker_error_type.clone(),
                        detail: details.original_worker_error_detail.clone(),
                    }),
                }
            }
            ErrorSubStatus::LongPollTimeout => Self::LongPollTimeout { service },
            ErrorSubStatus::ChannelMessageNotFound => Self::ChannelMessageNotFound { service },
            ErrorSubStatus::RequestTimeout => Self::RequestTimeout { service },
            ErrorSubStatus::Uncategorized => Self::Service { service },
        }
    }

    pub(crate) fn request_timeout(operation: &'static str, flow_id: &str) -> Self {
        let detail = "request timed out".to_string();
        Self::RequestTimeout {
            service: ServiceError {
                code: GrpcCode::DeadlineExceeded,
                sub_status: ErrorSubStatus::RequestTimeout,
                detail: detail.clone(),
                operation,
                flow_id: Some(flow_id.to_string()),
                source: Status::deadline_exceeded(detail),
                pull_registration_error: None,
            },
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum FlowTargetRequirement {
    None,
    Existing,
    Active,
}

#[derive(Clone, PartialEq, Message)]
struct GoogleRpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<Any>,
}

fn decode_details(bytes: &[u8]) -> Result<Option<ServiceErrorResponse>, prost::DecodeError> {
    if bytes.is_empty() {
        return Ok(None);
    }
    match GoogleRpcStatus::decode(bytes) {
        Ok(status) => {
            for detail in status.details {
                if detail.type_url.ends_with("/dex.ServiceErrorResponse") {
                    return ServiceErrorResponse::decode(detail.value.as_slice()).map(Some);
                }
            }
            Ok(None)
        }
        Err(status_error) => ServiceErrorResponse::decode(bytes)
            .map(Some)
            .or(Err(status_error)),
    }
}

fn map_sub_status(value: i32) -> ErrorSubStatus {
    match ProtoErrorSubStatus::try_from(value).ok() {
        Some(ProtoErrorSubStatus::FlowAlreadyStarted) => ErrorSubStatus::FlowAlreadyStarted,
        Some(ProtoErrorSubStatus::FlowNotExists) => ErrorSubStatus::FlowNotExists,
        Some(ProtoErrorSubStatus::WorkerApiError) => ErrorSubStatus::WorkerApiError,
        Some(ProtoErrorSubStatus::LongPollTimeOut) => ErrorSubStatus::LongPollTimeout,
        Some(ProtoErrorSubStatus::ChannelMessageNotFound) => ErrorSubStatus::ChannelMessageNotFound,
        Some(ProtoErrorSubStatus::RequestTimeout) => ErrorSubStatus::RequestTimeout,
        _ => ErrorSubStatus::Uncategorized,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_flow_uses_endpoint_lifecycle_requirement() {
        let found = SdkError::from_status(
            rich_status(GrpcCode::NotFound, ProtoErrorSubStatus::FlowNotExists),
            "describe_flow",
            Some("flow-id"),
            FlowTargetRequirement::Existing,
        );
        assert!(matches!(found, SdkError::FlowNotFound { .. }));

        let active = SdkError::from_status(
            rich_status(GrpcCode::NotFound, ProtoErrorSubStatus::FlowNotExists),
            "publish",
            Some("flow-id"),
            FlowTargetRequirement::Active,
        );
        assert!(matches!(active, SdkError::FlowNotActiveOrNotFound { .. }));
    }

    #[test]
    fn worker_details_and_lock_conflicts_are_distinct() {
        let invocation = SdkError::from_status(
            worker_status(GrpcCode::FailedPrecondition),
            "invoke_rpc",
            Some("flow-id"),
            FlowTargetRequirement::Active,
        );
        match invocation {
            SdkError::WorkerInvocation { service, worker } => {
                assert_eq!(GrpcCode::FailedPrecondition, service.code());
                assert_eq!(Some(GrpcCode::InvalidArgument), worker.code());
                assert_eq!("ApplicationError", worker.error_type());
                assert_eq!("invalid order", worker.detail());
            }
            error => panic!("expected WorkerInvocation, got {error:?}"),
        }

        let conflict = SdkError::from_status(
            worker_status(GrpcCode::Aborted),
            "invoke_rpc",
            Some("flow-id"),
            FlowTargetRequirement::Active,
        );
        assert!(matches!(conflict, SdkError::RpcLockConflict { .. }));
    }

    #[test]
    fn missing_and_malformed_details_use_generic_fallback() {
        let missing = SdkError::from_status(
            Status::internal("missing"),
            "search_flows",
            None,
            FlowTargetRequirement::None,
        );
        assert!(matches!(missing, SdkError::Service { .. }));

        let malformed = SdkError::from_status(
            Status::with_details(GrpcCode::Internal, "malformed", vec![255].into()),
            "search_flows",
            None,
            FlowTargetRequirement::None,
        );
        assert!(matches!(malformed, SdkError::Service { .. }));
    }

    fn rich_status(code: GrpcCode, sub_status: ProtoErrorSubStatus) -> Status {
        status_with_response(
            code,
            ServiceErrorResponse {
                detail: "service detail".to_string(),
                sub_status: sub_status as i32,
                ..ServiceErrorResponse::default()
            },
        )
    }

    fn worker_status(code: GrpcCode) -> Status {
        status_with_response(
            code,
            ServiceErrorResponse {
                detail: "worker failure".to_string(),
                sub_status: ProtoErrorSubStatus::WorkerApiError as i32,
                original_worker_error_detail: "invalid order".to_string(),
                original_worker_error_type: "ApplicationError".to_string(),
                original_worker_error_status: GrpcCode::InvalidArgument as i32,
                original_worker_error_stack_trace: String::new(),
            },
        )
    }

    fn status_with_response(code: GrpcCode, response: ServiceErrorResponse) -> Status {
        let status = GoogleRpcStatus {
            code: code as i32,
            message: "gRPC detail".to_string(),
            details: vec![Any {
                type_url: "type.googleapis.com/dex.ServiceErrorResponse".to_string(),
                value: response.encode_to_vec(),
            }],
        };
        Status::with_details(code, "gRPC detail", status.encode_to_vec().into())
    }
}
