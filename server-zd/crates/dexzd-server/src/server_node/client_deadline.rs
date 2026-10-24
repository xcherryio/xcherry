// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Ends calls with `DEADLINE_EXCEEDED` when the client's `grpc-timeout` elapses.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use dexzd_api::CLIENT_DEADLINE_ANSWER_MARGIN;
use tokio::time::Instant;
use tonic::codegen::{Service, http};

/// How long before the client's deadline the server answers, so it wins tonic's own timer.
const DEADLINE_MARGIN: Duration = Duration::from_millis(100);
// Wait RPCs answer with their own timeout status before this fallback fires.
const _: () = assert!(DEADLINE_MARGIN.as_nanos() < CLIENT_DEADLINE_ANSWER_MARGIN.as_nanos());

/// Wraps every gRPC service of the server.
pub(super) fn client_deadline_layer<S>(inner: S) -> ClientDeadline<S> {
    ClientDeadline { inner }
}

/// Answers `DEADLINE_EXCEEDED` before tonic's own `grpc-timeout` handling answers `CANCELLED`.
#[derive(Clone)]
pub(super) struct ClientDeadline<S> {
    inner: S,
}

impl<S, RequestBody, ResponseBody> Service<http::Request<RequestBody>> for ClientDeadline<S>
where
    S: Service<http::Request<RequestBody>, Response = http::Response<ResponseBody>>,
    S::Future: Send + 'static,
    ResponseBody: Default + Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, mut request: http::Request<RequestBody>) -> Self::Future {
        let client_deadline = request
            .headers()
            .get("grpc-timeout")
            .and_then(|value| value.to_str().ok())
            .and_then(parse_grpc_timeout)
            .and_then(|timeout| Instant::now().checked_add(timeout));
        if let Some(client_deadline) = client_deadline {
            request
                .extensions_mut()
                .insert(dexzd_api::ClientDeadline(client_deadline));
        }
        let response = self.inner.call(request);
        Box::pin(async move {
            let Some(client_deadline) = client_deadline else {
                return response.await;
            };
            let answer_by = client_deadline
                .checked_sub(DEADLINE_MARGIN)
                .unwrap_or(client_deadline);
            match tokio::time::timeout_at(answer_by, response).await {
                Ok(result) => result,
                Err(_elapsed) => Ok(tonic::Status::deadline_exceeded(
                    "the client deadline elapsed before the call completed",
                )
                .into_http()),
            }
        })
    }
}

/// Parses a `grpc-timeout` value: at most 8 digits followed by one unit (H, M, S, m, u, n).
///
/// Returns `None` for a malformed value, which leaves the call without a server-side deadline.
fn parse_grpc_timeout(value: &str) -> Option<Duration> {
    let digit_count = value.len().checked_sub(1)?;
    if digit_count == 0 || digit_count > 8 {
        return None;
    }
    let (digits, unit) = value.split_at(digit_count);
    let amount: u64 = digits.parse().ok()?;
    match unit {
        "H" => Some(Duration::from_secs(amount.checked_mul(3600)?)),
        "M" => Some(Duration::from_secs(amount.checked_mul(60)?)),
        "S" => Some(Duration::from_secs(amount)),
        "m" => Some(Duration::from_millis(amount)),
        "u" => Some(Duration::from_micros(amount)),
        "n" => Some(Duration::from_nanos(amount)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grpc_timeout_values_parse_with_every_unit() {
        assert_eq!(parse_grpc_timeout("2H"), Some(Duration::from_secs(7200)));
        assert_eq!(parse_grpc_timeout("3M"), Some(Duration::from_secs(180)));
        assert_eq!(parse_grpc_timeout("1S"), Some(Duration::from_secs(1)));
        assert_eq!(parse_grpc_timeout("999m"), Some(Duration::from_millis(999)));
        assert_eq!(parse_grpc_timeout("5u"), Some(Duration::from_micros(5)));
        assert_eq!(parse_grpc_timeout("7n"), Some(Duration::from_nanos(7)));
        assert_eq!(parse_grpc_timeout("S"), None);
        assert_eq!(parse_grpc_timeout("123456789S"), None);
        assert_eq!(parse_grpc_timeout("1x"), None);
    }
}
