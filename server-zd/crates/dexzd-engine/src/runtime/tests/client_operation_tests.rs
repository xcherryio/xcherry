// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_apply::ApplyOutcome;
use dexzd_protocol::dex::{
    AttributeWrite, ChannelMessage, ErrorSubStatus, InvokeRpcRequest, InvokeWorkerRpcResponse,
    PublishToChannelRequest, ResetFlowRequest, StopFlowRequest, WorkerErrorResponse, WorkerTarget,
};
use dexzd_protocol::dexzd::v1::flow_command;

use crate::runtime::tests::fixtures::{
    TENANT, set_attribute_request, start_flow, start_request, string_value,
};
use crate::runtime::tests::model_applier::not_found_reply;
use crate::runtime::tests::test_support::RuntimeHarness;
use crate::runtime::{RpcInvokeError, RuntimeError, WorkerRpcResult, grpc_code};

#[tokio::test(start_paused = true)]
async fn start_flow_proposes_a_complete_start_command_and_returns_the_run_id() {
    let harness = RuntimeHarness::new();
    let mut request = start_request("order-1");
    request.flow_timeout_seconds = 30;

    let response = harness
        .drive(harness.registry.start_flow(TENANT, request.clone()))
        .await
        .unwrap();

    let submitted = harness.submitted();
    assert_eq!(submitted.len(), 1);
    let Some(flow_command::Command::FlowStart(start)) = submitted[0].command.clone() else {
        panic!("expected FlowStart");
    };
    assert_eq!(
        dexzd_keyspace::RunId::from_slice(&start.new_run_id)
            .unwrap()
            .to_string(),
        response.run_id
    );
    assert!(submitted[0].run_id.is_empty());
    assert_eq!(start.request_fingerprint.len(), 16);
    assert_eq!(
        start.resolved_timeout_policy, 1,
        "positive timeout defaults to FAIL"
    );
    assert_eq!(
        start.worker_target,
        Some(WorkerTarget {
            is_pull_mode: true,
            ..WorkerTarget::default()
        })
    );
    assert_eq!(
        start.settings.unwrap().history_window_semantic_events,
        10_000
    );
    assert_eq!(start.request.unwrap(), request);
}

#[tokio::test(start_paused = true)]
async fn invalid_requests_are_rejected_before_proposing() {
    let harness = RuntimeHarness::new();
    let mut request = start_request("order-1");
    request.request_id.clear();
    let error = harness
        .registry
        .start_flow(TENANT, request)
        .await
        .unwrap_err();
    assert_eq!(error.grpc_code(), grpc_code::INVALID_ARGUMENT);

    let mut push_target = start_request("order-2");
    push_target
        .flow_start_options
        .as_mut()
        .unwrap()
        .flow_config_override
        .as_mut()
        .unwrap()
        .worker_target = Some(WorkerTarget {
        address: "localhost:9000".to_owned(),
        ..WorkerTarget::default()
    });
    let error = harness
        .registry
        .start_flow(TENANT, push_target)
        .await
        .unwrap_err();
    assert_eq!(error.grpc_code(), grpc_code::INVALID_ARGUMENT);
    assert!(harness.submitted().is_empty());
}

#[tokio::test(start_paused = true)]
async fn writes_target_the_newest_run_and_a_missing_flow_is_not_found() {
    let harness = RuntimeHarness::new();
    let run_id = start_flow(&harness, "order-1").await.unwrap();

    harness
        .drive(harness.registry.set_attributes(
            TENANT,
            set_attribute_request("order-1", "status", string_value("paid")),
        ))
        .await
        .unwrap();
    let submitted = harness.submitted();
    assert_eq!(submitted[1].run_id, run_id.as_bytes().to_vec());
    let Some(flow_command::Command::AttributesSet(set)) = submitted[1].command.clone() else {
        panic!("expected AttributesSet");
    };
    assert_eq!(set.request_fingerprint.len(), 16);

    let error = harness
        .drive(harness.registry.set_attributes(
            TENANT,
            set_attribute_request("missing", "status", string_value("paid")),
        ))
        .await
        .unwrap_err();
    assert_eq!(error.grpc_code(), grpc_code::NOT_FOUND);
    assert_eq!(error.sub_status(), ErrorSubStatus::FlowNotExists);

    let error = harness
        .registry
        .stop_flow(
            TENANT,
            StopFlowRequest {
                flow_id: "order-1".to_owned(),
                run_id: "not-a-uuid".to_owned(),
                ..StopFlowRequest::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.grpc_code(), grpc_code::INVALID_ARGUMENT);
}

#[tokio::test(start_paused = true)]
async fn error_replies_from_apply_become_runtime_errors() {
    let harness = RuntimeHarness::new();
    start_flow(&harness, "order-1").await.unwrap();
    for _ in 0..2 {
        harness
            .applier
            .scripted
            .lock()
            .unwrap()
            .push_back(Ok(ApplyOutcome::rejected(not_found_reply("order-1"))));
    }
    let error = harness
        .drive(harness.registry.stop_flow(
            TENANT,
            StopFlowRequest {
                flow_id: "order-1".to_owned(),
                ..StopFlowRequest::default()
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(
        error,
        RuntimeError::rejected(
            grpc_code::NOT_FOUND,
            ErrorSubStatus::FlowNotExists,
            "flow order-1 does not exist"
        )
    );
}

#[tokio::test(start_paused = true)]
async fn publish_replaces_message_ids_and_skips_empty_lists() {
    let harness = RuntimeHarness::new();
    start_flow(&harness, "order-1").await.unwrap();
    harness
        .registry
        .publish_to_channel(
            TENANT,
            PublishToChannelRequest {
                flow_id: "never-started".to_owned(),
                ..PublishToChannelRequest::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        harness.submitted().len(),
        1,
        "an empty publish proposes nothing"
    );

    harness
        .drive(harness.registry.publish_to_channel(
            TENANT,
            PublishToChannelRequest {
                flow_id: "order-1".to_owned(),
                messages: vec![ChannelMessage {
                    channel_name: "orders".to_owned(),
                    value: Some(string_value("hello")),
                    message_id: "client-chosen".to_owned(),
                }],
                ..PublishToChannelRequest::default()
            },
        ))
        .await
        .unwrap();
    let Some(flow_command::Command::ChannelPublish(publish)) =
        harness.submitted()[1].command.clone()
    else {
        panic!("expected ChannelPublish");
    };
    let message_id = &publish.request.unwrap().messages[0].message_id;
    assert_ne!(message_id, "client-chosen");
    assert_eq!(
        uuid::Uuid::parse_str(message_id).unwrap().get_version_num(),
        7
    );
}

#[tokio::test(start_paused = true)]
async fn reset_returns_the_unchanged_run_id() {
    let harness = RuntimeHarness::new();
    let run_id = start_flow(&harness, "order-1").await.unwrap();
    let response = harness
        .drive(harness.registry.reset_flow(
            TENANT,
            ResetFlowRequest {
                flow_id: "order-1".to_owned(),
                reset_type: 1,
                ..ResetFlowRequest::default()
            },
        ))
        .await
        .unwrap();
    assert_eq!(response.run_id, run_id.to_string());
}

fn rpc_request(flow_id: &str) -> InvokeRpcRequest {
    InvokeRpcRequest {
        flow_id: flow_id.to_owned(),
        rpc_name: "getOrder".to_owned(),
        request_id: "rpc-1".to_owned(),
        ..InvokeRpcRequest::default()
    }
}

#[tokio::test(start_paused = true)]
async fn read_only_rpc_is_not_logged_and_sees_committed_attributes() {
    let harness = RuntimeHarness::new();
    let run_id = start_flow(&harness, "order-1").await.unwrap();
    harness
        .drive(harness.registry.set_attributes(
            TENANT,
            set_attribute_request("order-1", "status", string_value("paid")),
        ))
        .await
        .unwrap();
    harness
        .rpc_invoker
        .results
        .lock()
        .unwrap()
        .push_back(Ok(WorkerRpcResult {
            response: InvokeWorkerRpcResponse {
                output: Some(string_value("paid order")),
                ..InvokeWorkerRpcResponse::default()
            },
            app_worker_id: "worker-1".to_owned(),
            pull_routing: None,
        }));
    let submitted_before = harness.submitted().len();

    let response = harness
        .registry
        .invoke_rpc(TENANT, rpc_request("order-1"))
        .await
        .unwrap();
    assert_eq!(response.output, Some(string_value("paid order")));
    assert_eq!(harness.submitted().len(), submitted_before);

    let invocation = harness.rpc_invoker.invocations.lock().unwrap()[0].clone();
    assert_eq!(invocation.run_id, run_id);
    assert_eq!(invocation.flow_type, "OrderFlow");
    let request = invocation.request;
    assert_eq!(request.rpc_name, "getOrder");
    assert_eq!(request.context.unwrap().run_id, run_id.to_string());
    assert_eq!(request.attributes.len(), 1);
    assert_eq!(request.attributes[0].key, "status");
}

#[tokio::test(start_paused = true)]
async fn rpc_with_effects_commits_and_returns_the_committed_output() {
    let harness = RuntimeHarness::new();
    let run_id = start_flow(&harness, "order-1").await.unwrap();
    harness
        .rpc_invoker
        .results
        .lock()
        .unwrap()
        .push_back(Ok(WorkerRpcResult {
            response: InvokeWorkerRpcResponse {
                output: Some(string_value("approved")),
                upsert_attributes: vec![AttributeWrite {
                    key: "approved".to_owned(),
                    value: Some(string_value("yes")),
                    ..AttributeWrite::default()
                }],
                ..InvokeWorkerRpcResponse::default()
            },
            app_worker_id: "worker-1".to_owned(),
            pull_routing: None,
        }));
    let mut request = rpc_request("order-1");
    request.is_transactional = true;
    let response = harness
        .drive(harness.registry.invoke_rpc(TENANT, request))
        .await
        .unwrap();
    assert_eq!(response.output, Some(string_value("approved")));
    let submitted = harness.submitted();
    let last = submitted.last().unwrap();
    assert_eq!(last.run_id, run_id.as_bytes().to_vec());
    assert_eq!(
        last.expected_run_attempt, 1,
        "transactional RPCs pin the attempt"
    );
    let Some(flow_command::Command::RpcCommitted(rpc)) = last.command.clone() else {
        panic!("expected RpcCommitted");
    };
    assert_eq!(rpc.app_worker_id, "worker-1");
    assert_eq!(rpc.request_fingerprint.len(), 16);
}

#[tokio::test(start_paused = true)]
async fn worker_rpc_errors_map_to_client_errors() {
    let harness = RuntimeHarness::new();
    start_flow(&harness, "order-1").await.unwrap();
    let worker_error = WorkerErrorResponse {
        detail: "order is locked".to_owned(),
        grpc_status: 9,
        ..WorkerErrorResponse::default()
    };
    harness.rpc_invoker.results.lock().unwrap().extend([
        Err(RpcInvokeError::WorkerError {
            response: worker_error.clone(),
        }),
        Err(RpcInvokeError::NoWorkerAvailable {
            detail: "no worker".to_owned(),
        }),
        Err(RpcInvokeError::DeadlineExceeded),
    ]);

    let error = harness
        .registry
        .invoke_rpc(TENANT, rpc_request("order-1"))
        .await
        .unwrap_err();
    assert_eq!(error, RuntimeError::WorkerRpcFailed { worker_error });
    assert_eq!(error.grpc_code(), grpc_code::FAILED_PRECONDITION);
    assert_eq!(error.sub_status(), ErrorSubStatus::WorkerApiError);

    let error = harness
        .registry
        .invoke_rpc(TENANT, rpc_request("order-1"))
        .await
        .unwrap_err();
    assert_eq!(error.grpc_code(), grpc_code::UNAVAILABLE);

    let error = harness
        .registry
        .invoke_rpc(TENANT, rpc_request("order-1"))
        .await
        .unwrap_err();
    assert_eq!(error.grpc_code(), grpc_code::DEADLINE_EXCEEDED);

    let error = harness
        .registry
        .invoke_rpc(TENANT, rpc_request("missing"))
        .await
        .unwrap_err();
    assert_eq!(error.sub_status(), ErrorSubStatus::FlowNotExists);
}
