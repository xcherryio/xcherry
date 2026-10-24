// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_protocol::dex::WorkerErrorResponse;
use dexzd_protocol::dexzd::v1::StepAttemptFailureKind;

use crate::effects::method_request::RequestValueSlot;
use crate::runtime::grpc_code;

pub(super) struct AttemptFailure {
    pub(super) kind: StepAttemptFailureKind,
    pub(super) worker_error: Option<WorkerErrorResponse>,
    pub(super) server_detail: String,
    pub(super) is_non_retryable: bool,
    pub(super) served_by_app_worker_id: String,
    /// Slots of the values a Worker reported LoadBlobs omitted; `None` for other failures.
    pub(super) omitted_value_slots: Option<Vec<RequestValueSlot>>,
}

impl AttemptFailure {
    pub(super) fn server(kind: StepAttemptFailureKind, detail: impl Into<String>) -> Self {
        Self {
            kind,
            worker_error: None,
            server_detail: detail.into(),
            is_non_retryable: false,
            served_by_app_worker_id: String::new(),
            omitted_value_slots: None,
        }
    }

    /// Returns true for an error from the Step's code: SDKs report those with gRPC `UNKNOWN`.
    pub(super) fn is_step_error(&self) -> bool {
        matches!(&self.worker_error, Some(error) if error.grpc_status == grpc_code::UNKNOWN)
            && self.omitted_value_slots.is_none()
    }
}
