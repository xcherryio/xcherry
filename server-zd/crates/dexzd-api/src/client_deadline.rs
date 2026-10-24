// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::future::Future;
use std::time::Duration;

use dexzd_protocol::dex::ErrorSubStatus;
use tokio::time::Instant;
use tonic::{Code, Request};

use crate::FlowOperationError;

/// How early wait RPCs answer before the client deadline; exceeds the transport fallback margin.
// Covers request transit and scheduling stalls on loaded hosts, which exceeded 40 ms in CI.
pub const CLIENT_DEADLINE_ANSWER_MARGIN: Duration = Duration::from_millis(200);

/// SDKs send the remaining request budget rounded up to whole seconds.
const REQUEST_TIMEOUT_ROUNDING: Duration = Duration::from_secs(1);

/// When the client abandons a call, from `grpc-timeout`; stored in the request extensions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientDeadline(pub Instant);

/// Ends a wait RPC before the client's deadline with the status the Go Server returns then.
pub(crate) struct WaitCallDeadline {
    client_deadline: Option<Instant>,
    request_deadline: Option<Instant>,
    request_timeout_message: &'static str,
    wait_timeout_message: &'static str,
}

impl WaitCallDeadline {
    /// Reads the client's deadline from `request`; `request_timeout_seconds` is 0 for waits without one.
    pub(crate) fn new<T>(
        request: &Request<T>,
        request_timeout_seconds: i32,
        request_timeout_message: &'static str,
        wait_timeout_message: &'static str,
    ) -> Self {
        let now = Instant::now();
        let request_deadline = u64::try_from(request_timeout_seconds)
            .ok()
            .filter(|seconds| *seconds > 0)
            .and_then(|seconds| now.checked_add(Duration::from_secs(seconds)));
        Self {
            client_deadline: request
                .extensions()
                .get::<ClientDeadline>()
                .map(|deadline| deadline.0),
            request_deadline,
            request_timeout_message,
            wait_timeout_message,
        }
    }

    /// Runs `wait` until it finishes or the client's deadline is about to pass.
    ///
    /// Returns the wait's own result, or `DEADLINE_EXCEEDED` with `REQUEST_TIMEOUT` when the
    /// request timeout ends with the client deadline and `LONG_POLL_TIME_OUT` otherwise.
    pub(crate) async fn bound<T>(
        self,
        wait: impl Future<Output = Result<T, FlowOperationError>>,
    ) -> Result<T, FlowOperationError> {
        let Some(client_deadline) = self.client_deadline else {
            return wait.await;
        };
        let answer_by = client_deadline
            .checked_sub(CLIENT_DEADLINE_ANSWER_MARGIN)
            .unwrap_or(client_deadline);
        tokio::select! {
            biased;
            result = wait => result,
            () = tokio::time::sleep_until(answer_by) => Err(self.client_deadline_error(client_deadline)),
        }
    }

    fn client_deadline_error(&self, client_deadline: Instant) -> FlowOperationError {
        let is_request_timeout = self.request_deadline.is_some_and(|request_deadline| {
            client_deadline
                .checked_add(REQUEST_TIMEOUT_ROUNDING)
                .is_none_or(|latest_request_deadline| request_deadline <= latest_request_deadline)
        });
        if is_request_timeout {
            FlowOperationError::rejected(
                Code::DeadlineExceeded,
                ErrorSubStatus::RequestTimeout,
                self.request_timeout_message,
            )
        } else {
            FlowOperationError::long_poll_timeout(self.wait_timeout_message)
        }
    }
}
