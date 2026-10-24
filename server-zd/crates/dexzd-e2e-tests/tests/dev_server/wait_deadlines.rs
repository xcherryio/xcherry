// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Wait RPCs answer like the Go Server when the request timeout or the client's deadline ends.

use std::time::Duration;

use dexzd_protocol::dex::flow_service_client::FlowServiceClient;
use dexzd_protocol::dex::{
    AttributeMatch, AttributeMatchOperator, ErrorSubStatus, FlowConfig, FlowStartOptions,
    ServiceErrorResponse, StartFlowRequest, WaitForAttributeRequest, WaitForFlowRequest,
    WaitForHistoryEventRequest, WaitForStepCompletionRequest, WorkerTarget,
};
use prost::Message;
use tokio::time::Instant;
use tonic::transport::Channel;

use crate::harness::DevServer;
use crate::string_value;

/// No Worker serves this Flow type, so its Step never runs and every wait stays pending.
const IDLE_FLOW_TYPE: &str = "IdleFlow";
const IDLE_FLOW_ID: &str = "idle-flow-1";
const ONE_SECOND: Duration = Duration::from_secs(1);

/// The `google.rpc.Status` that carries the `ServiceErrorResponse` in `grpc-status-details-bin`.
#[derive(Clone, PartialEq, prost::Message)]
struct GoogleRpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<prost_types::Any>,
}

/// How one wait call ended, as an SDK sees it.
#[derive(Debug)]
struct WaitTimeout {
    code: tonic::Code,
    sub_status: ErrorSubStatus,
    message: String,
    elapsed: Duration,
}

impl WaitTimeout {
    fn from_status(status: &tonic::Status, elapsed: Duration) -> Self {
        let details = GoogleRpcStatus::decode(status.details())
            .unwrap_or_else(|error| panic!("{status:?} has no google.rpc.Status details: {error}"));
        let response = details
            .details
            .iter()
            .find(|detail| detail.type_url == "type.googleapis.com/dex.ServiceErrorResponse")
            .map(|detail| ServiceErrorResponse::decode(detail.value.as_slice()))
            .unwrap_or_else(|| panic!("{status:?} has no ServiceErrorResponse"))
            .expect("the ServiceErrorResponse decodes");
        assert_eq!(response.detail, status.message());
        Self {
            code: status.code(),
            sub_status: ErrorSubStatus::try_from(response.sub_status)
                .expect("the sub-status is a known value"),
            message: status.message().to_owned(),
            elapsed,
        }
    }

    /// Asserts the Go Server's answer, delivered no earlier than `earliest` and before `transport_timeout`.
    fn assert_answer(
        &self,
        sub_status: ErrorSubStatus,
        message: &str,
        earliest: Duration,
        transport_timeout: Duration,
    ) {
        assert_eq!(self.code, tonic::Code::DeadlineExceeded, "{self:?}");
        assert_eq!(self.sub_status, sub_status, "{self:?}");
        assert_eq!(self.message, message, "{self:?}");
        assert!(
            self.elapsed >= earliest && self.elapsed < transport_timeout,
            "{self:?} must end between {earliest:?} and {transport_timeout:?}"
        );
    }
}

/// Sends one wait call with `transport_timeout` as its `grpc-timeout` and returns how it timed out.
async fn time_out<Reply, Call, CallFuture>(
    channel: &Channel,
    transport_timeout: Duration,
    call: Call,
) -> WaitTimeout
where
    Reply: std::fmt::Debug,
    Call: FnOnce(FlowServiceClient<Channel>) -> CallFuture,
    CallFuture: Future<Output = Result<tonic::Response<Reply>, tonic::Status>>,
{
    let started = Instant::now();
    let result = tokio::time::timeout(
        transport_timeout * 3,
        call(FlowServiceClient::new(channel.clone())),
    )
    .await
    .expect("the server answers the wait call");
    let status = result.expect_err("the wait times out");
    WaitTimeout::from_status(&status, started.elapsed())
}

fn with_timeout<T>(message: T, transport_timeout: Duration) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    request.set_timeout(transport_timeout);
    request
}

fn step_completion(request_timeout_seconds: i32) -> WaitForStepCompletionRequest {
    WaitForStepCompletionRequest {
        flow_id: IDLE_FLOW_ID.to_owned(),
        step_type: "IdleStep".to_owned(),
        step_execution_number: "1".to_owned(),
        request_timeout_seconds,
        ..WaitForStepCompletionRequest::default()
    }
}

fn attribute_match(request_timeout_seconds: i32) -> WaitForAttributeRequest {
    WaitForAttributeRequest {
        flow_id: IDLE_FLOW_ID.to_owned(),
        r#match: Some(AttributeMatch {
            key: "Status".to_owned(),
            operator: AttributeMatchOperator::Equal.into(),
            operand: Some(string_value("never")),
        }),
        request_timeout_seconds,
        ..WaitForAttributeRequest::default()
    }
}

/// Each wait at its timeout boundary, with the SDKs' default of a transport deadline equal to the request timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_rpcs_answer_go_timeouts_before_the_client_deadline() {
    let server = DevServer::start().await;
    let channel = server.channel();
    let started = FlowServiceClient::new(channel.clone())
        .start_flow(StartFlowRequest {
            flow_id: IDLE_FLOW_ID.to_owned(),
            flow_type: IDLE_FLOW_TYPE.to_owned(),
            start_step_type: "IdleStep".to_owned(),
            flow_start_options: Some(FlowStartOptions {
                flow_config_override: Some(FlowConfig {
                    worker_target: Some(WorkerTarget {
                        is_pull_mode: true,
                        ..WorkerTarget::default()
                    }),
                    ..FlowConfig::default()
                }),
                ..FlowStartOptions::default()
            }),
            request_id: "idle-request-1".to_owned(),
            ..StartFlowRequest::default()
        })
        .await
        .expect("StartFlow succeeds")
        .into_inner();
    let two_seconds = 2 * ONE_SECOND;
    let (
        step_at_shared_deadline,
        step_at_request_deadline,
        step_at_transport_deadline,
        attribute_at_shared_deadline,
        attribute_at_request_deadline,
        attribute_at_transport_deadline,
        flow_at_transport_deadline,
        history_at_transport_deadline,
    ) = tokio::join!(
        time_out(&channel, ONE_SECOND, |mut client| async move {
            client
                .wait_for_step_completion(with_timeout(step_completion(1), ONE_SECOND))
                .await
        }),
        time_out(&channel, two_seconds, |mut client| async move {
            client
                .wait_for_step_completion(with_timeout(step_completion(1), two_seconds))
                .await
        }),
        time_out(&channel, ONE_SECOND, |mut client| async move {
            client
                .wait_for_step_completion(with_timeout(step_completion(5), ONE_SECOND))
                .await
        }),
        time_out(&channel, ONE_SECOND, |mut client| async move {
            client
                .wait_for_attribute(with_timeout(attribute_match(1), ONE_SECOND))
                .await
        }),
        time_out(&channel, two_seconds, |mut client| async move {
            client
                .wait_for_attribute(with_timeout(attribute_match(1), two_seconds))
                .await
        }),
        time_out(&channel, ONE_SECOND, |mut client| async move {
            client
                .wait_for_attribute(with_timeout(attribute_match(5), ONE_SECOND))
                .await
        }),
        time_out(&channel, ONE_SECOND, |mut client| async move {
            client
                .wait_for_flow(with_timeout(
                    WaitForFlowRequest {
                        flow_id: IDLE_FLOW_ID.to_owned(),
                        wait_time_seconds: 30,
                        ..WaitForFlowRequest::default()
                    },
                    ONE_SECOND,
                ))
                .await
        }),
        time_out(&channel, ONE_SECOND, |mut client| {
            let run_id = started.run_id.clone();
            async move {
                client
                    .wait_for_history_event(with_timeout(
                        WaitForHistoryEventRequest {
                            flow_id: IDLE_FLOW_ID.to_owned(),
                            run_id,
                            next_internal_event_id: 1_000,
                        },
                        ONE_SECOND,
                    ))
                    .await
            }
        }),
    );
    let near_deadline = ONE_SECOND / 2;
    step_at_shared_deadline.assert_answer(
        ErrorSubStatus::RequestTimeout,
        "step completion request timed out",
        near_deadline,
        ONE_SECOND,
    );
    step_at_request_deadline.assert_answer(
        ErrorSubStatus::RequestTimeout,
        "step completion request timed out",
        ONE_SECOND,
        two_seconds,
    );
    step_at_transport_deadline.assert_answer(
        ErrorSubStatus::LongPollTimeOut,
        "context deadline exceeded",
        near_deadline,
        ONE_SECOND,
    );
    attribute_at_shared_deadline.assert_answer(
        ErrorSubStatus::RequestTimeout,
        "attribute request timed out",
        near_deadline,
        ONE_SECOND,
    );
    attribute_at_request_deadline.assert_answer(
        ErrorSubStatus::RequestTimeout,
        "attribute request timed out",
        ONE_SECOND,
        two_seconds,
    );
    attribute_at_transport_deadline.assert_answer(
        ErrorSubStatus::LongPollTimeOut,
        "context deadline exceeded",
        near_deadline,
        ONE_SECOND,
    );
    flow_at_transport_deadline.assert_answer(
        ErrorSubStatus::LongPollTimeOut,
        "flow is still running and waiting exceeded the timeout",
        near_deadline,
        ONE_SECOND,
    );
    history_at_transport_deadline.assert_answer(
        ErrorSubStatus::LongPollTimeOut,
        "history long poll exceeded the deadline",
        near_deadline,
        ONE_SECOND,
    );
    server.shutdown().await;
}
