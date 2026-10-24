// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_apply::{ApplyError, StateReadError};
use dexzd_protocol::dex::{ErrorSubStatus, WorkerErrorResponse};
use dexzd_protocol::dexzd::v1::CommandErrorReply;
use dexzd_request_validation::ValidationError;

use crate::runtime::CommitError;

/// gRPC status codes the runtime reports.
pub mod grpc_code {
    /// `INVALID_ARGUMENT`.
    pub const INVALID_ARGUMENT: i32 = 3;
    /// `DEADLINE_EXCEEDED`.
    pub const DEADLINE_EXCEEDED: i32 = 4;
    /// `NOT_FOUND`.
    pub const NOT_FOUND: i32 = 5;
    /// `RESOURCE_EXHAUSTED`.
    pub const RESOURCE_EXHAUSTED: i32 = 8;
    /// `FAILED_PRECONDITION`.
    pub const FAILED_PRECONDITION: i32 = 9;
    /// `ABORTED`.
    pub const ABORTED: i32 = 10;
    /// `UNIMPLEMENTED`.
    pub const UNIMPLEMENTED: i32 = 12;
    /// `INTERNAL`.
    pub const INTERNAL: i32 = 13;
    /// `UNAVAILABLE`.
    pub const UNAVAILABLE: i32 = 14;
}

/// Why a runtime operation failed. The API layer maps it to a gRPC status with
/// [`RuntimeError::grpc_code`] and [`RuntimeError::sub_status`].
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum RuntimeError {
    /// The request or the Flow's state rejected the operation.
    #[error("{message}")]
    Rejected {
        /// gRPC status code.
        grpc_code: i32,
        /// Dex error sub-status.
        sub_status: ErrorSubStatus,
        /// Message for the client.
        message: String,
    },
    /// A Worker RPC handler returned an error (`FAILED_PRECONDITION` + `WORKER_API_ERROR`).
    #[error("worker RPC failed: {}", worker_error.detail)]
    WorkerRpcFailed {
        /// The Worker's original error.
        worker_error: WorkerErrorResponse,
    },
    /// The node cannot serve the operation now; the client may retry.
    #[error("unavailable: {detail}")]
    Unavailable {
        /// What is unavailable.
        detail: String,
    },
    /// The node is stopping; the client retries against the next owner.
    #[error("node is stopping: {detail}")]
    NodeStopping {
        /// Why the node stops.
        detail: String,
    },
    /// Stored state broke a trusted invariant.
    #[error("internal error: {detail}")]
    Internal {
        /// What broke.
        detail: String,
    },
}

impl RuntimeError {
    /// Returns a rejection with `grpc_code` and `sub_status`.
    pub fn rejected(
        grpc_code: i32,
        sub_status: ErrorSubStatus,
        message: impl Into<String>,
    ) -> Self {
        Self::Rejected {
            grpc_code,
            sub_status,
            message: message.into(),
        }
    }

    /// Returns an `INVALID_ARGUMENT` rejection.
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::rejected(
            grpc_code::INVALID_ARGUMENT,
            ErrorSubStatus::Uncategorized,
            message,
        )
    }

    /// Returns `NOT_FOUND` + `FLOW_NOT_EXISTS`.
    pub fn flow_not_found(flow_id: &str) -> Self {
        Self::rejected(
            grpc_code::NOT_FOUND,
            ErrorSubStatus::FlowNotExists,
            format!("flow {flow_id} does not exist"),
        )
    }

    /// Returns the gRPC status code of the error.
    pub fn grpc_code(&self) -> i32 {
        match self {
            Self::Rejected { grpc_code, .. } => *grpc_code,
            Self::WorkerRpcFailed { .. } => grpc_code::FAILED_PRECONDITION,
            Self::Unavailable { .. } | Self::NodeStopping { .. } => grpc_code::UNAVAILABLE,
            Self::Internal { .. } => grpc_code::INTERNAL,
        }
    }

    /// Returns the Dex error sub-status of the error.
    pub fn sub_status(&self) -> ErrorSubStatus {
        match self {
            Self::Rejected { sub_status, .. } => *sub_status,
            Self::WorkerRpcFailed { .. } => ErrorSubStatus::WorkerApiError,
            Self::Unavailable { .. } | Self::NodeStopping { .. } | Self::Internal { .. } => {
                ErrorSubStatus::Uncategorized
            }
        }
    }

    pub(crate) fn from_reply(error: &CommandErrorReply) -> Self {
        Self::Rejected {
            grpc_code: error.grpc_code,
            sub_status: ErrorSubStatus::try_from(error.sub_status)
                .unwrap_or(ErrorSubStatus::Uncategorized),
            message: error.message.clone(),
        }
    }

    pub(crate) fn corrupt_row(table: &str, detail: impl std::fmt::Display) -> Self {
        Self::Internal {
            detail: format!("stored {table} row is corrupt: {detail}"),
        }
    }
}

impl From<CommitError> for RuntimeError {
    fn from(error: CommitError) -> Self {
        match error {
            CommitError::Unavailable { detail } => Self::Unavailable { detail },
            CommitError::NodeStopping { detail } => Self::NodeStopping { detail },
        }
    }
}

impl From<StateReadError> for RuntimeError {
    fn from(error: StateReadError) -> Self {
        Self::Unavailable {
            detail: error.to_string(),
        }
    }
}

impl From<ApplyError> for RuntimeError {
    fn from(error: ApplyError) -> Self {
        Self::NodeStopping {
            detail: format!("apply failed: {error}"),
        }
    }
}

impl From<ValidationError> for RuntimeError {
    fn from(error: ValidationError) -> Self {
        Self::invalid_argument(error.message)
    }
}
