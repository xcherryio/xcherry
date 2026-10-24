// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! End-to-end tests of `dex-server-zd dev` through the public gRPC protocol.

mod harness;
mod wait_deadlines;

use std::collections::BTreeMap;
use std::time::Duration;

use dexzd_protocol::dex::{
    FlowConfig, FlowStartOptions, FlowStatus, GetFlowSummaryRequest, IndexType,
    InvokeExecuteMethodResponse, ServerEngine, StartFlowRequest, StepOptions,
    SyncAttributeIndexRequest, Value, WaitForFlowRequest, WorkerTarget, value,
};

use harness::{DevServer, PullWorker, force_complete};

const FLOW_TYPE: &str = "SmokeFlow";

fn string_value(text: &str) -> Value {
    Value {
        kind: Some(value::Kind::StringValue(text.to_owned())),
    }
}

/// Every SDK's minimal path: handshake, pull Worker, one Step, result.
///
/// Shutdown must finish while the Worker stream is open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pull_worker_completes_a_single_step_flow() {
    let server = DevServer::start().await;
    let mut flow_client = server.flow_client();

    let server_info = flow_client
        .get_server_info(())
        .await
        .expect("GetServerInfo succeeds")
        .into_inner();
    assert_eq!(server_info.server_engine, ServerEngine::Zd as i32);
    assert_eq!(server_info.minimum_supported_protocol_version, 1);
    assert_eq!(server_info.current_protocol_version, 3);
    flow_client
        .sync_attribute_indexes(SyncAttributeIndexRequest {
            attribute_indexes: BTreeMap::from([(
                "CustomerId".to_owned(),
                IndexType::Keyword as i32,
            )]),
        })
        .await
        .expect("SyncAttributeIndexes succeeds");

    let worker = PullWorker::start(
        server.worker_client(),
        FLOW_TYPE,
        Box::new(|request| {
            assert_eq!(request.step_type, "GreetStep");
            InvokeExecuteMethodResponse {
                step_decision: Some(force_complete(string_value("hello from the Worker"))),
                ..InvokeExecuteMethodResponse::default()
            }
        }),
    )
    .await;

    let started = flow_client
        .start_flow(StartFlowRequest {
            flow_id: "smoke-flow-1".to_owned(),
            flow_type: FLOW_TYPE.to_owned(),
            start_step_type: "GreetStep".to_owned(),
            step_input: Some(string_value("hello")),
            step_options: Some(StepOptions {
                skip_wait_for: true,
                ..StepOptions::default()
            }),
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
            request_id: "smoke-request-1".to_owned(),
            ..StartFlowRequest::default()
        })
        .await
        .expect("StartFlow succeeds")
        .into_inner();
    assert!(!started.run_id.is_empty());

    let result = flow_client
        .wait_for_flow(WaitForFlowRequest {
            flow_id: "smoke-flow-1".to_owned(),
            needs_results: true,
            wait_time_seconds: 30,
            ..WaitForFlowRequest::default()
        })
        .await
        .expect("WaitForFlow succeeds")
        .into_inner();
    assert_eq!(result.flow_status, FlowStatus::Completed as i32);
    assert_eq!(result.results.len(), 1);
    assert_eq!(
        result.results[0].completed_step_output,
        Some(string_value("hello from the Worker"))
    );

    let summary = flow_client
        .get_flow_summary(GetFlowSummaryRequest {
            flow_id: "smoke-flow-1".to_owned(),
            run_id: String::new(),
        })
        .await
        .expect("GetFlowSummary succeeds")
        .into_inner();
    assert_eq!(summary.flow_status, FlowStatus::Completed as i32);
    let run_id = summary
        .flow_execution_id
        .as_ref()
        .map(|execution| execution.run_id.clone());
    assert_eq!(run_id.as_deref(), Some(started.run_id.as_str()));
    assert_eq!(summary.first_run_id, started.run_id);
    assert_eq!(summary.request_id, "smoke-request-1");
    assert_eq!(summary.flow_type, FLOW_TYPE);
    assert_eq!(summary.run_attempt, 1);

    let shutdown_time = server.shutdown().await;
    assert!(
        shutdown_time < Duration::from_secs(10),
        "shutdown took {shutdown_time:?} with an open Worker stream"
    );
    worker.stop().await;
}

/// Unknown Flows report the Go Server's NotFound text, and Continue-As-New is unavailable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn errors_match_the_go_server() {
    let server = DevServer::start().await;
    let mut flow_client = server.flow_client();
    let missing = flow_client
        .get_flow_summary(GetFlowSummaryRequest {
            flow_id: "missing-flow".to_owned(),
            run_id: String::new(),
        })
        .await
        .expect_err("a missing Flow is an error");
    assert_eq!(missing.code(), tonic::Code::NotFound);
    assert_eq!(
        missing.message(),
        "Flow run is not found for flow ID missing-flow"
    );
    let continue_as_new = flow_client
        .trigger_continue_as_new(dexzd_protocol::dex::TriggerContinueAsNewRequest::default())
        .await
        .expect_err("Continue-As-New is not supported");
    assert_eq!(continue_as_new.code(), tonic::Code::Unimplemented);
    let shutdown_time = server.shutdown().await;
    assert!(shutdown_time < Duration::from_secs(10));
}
