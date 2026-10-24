// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Process-level tests of `dex-server-zd serve` against s3mock emulators in the test process.

#![cfg(unix)]

mod harness;

use std::time::Duration;

use bytes::Bytes;
use dexzd_object_store::{
    ObjectStore, S3Credentials, S3ObjectStore, S3ObjectStoreConfig, WriteCondition,
};
use dexzd_protocol::dex::flow_service_client::FlowServiceClient;
use dexzd_protocol::dex::worker_task_service_client::WorkerTaskServiceClient;
use dexzd_protocol::dex::{
    CloseDecision, CloseDecisionType, FlowConfig, FlowStartOptions, FlowStatus,
    GetFlowSummaryRequest, InvokeExecuteMethodResponse, StartFlowRequest, StepDecision,
    StepOptions, Value, WaitForFlowRequest, WorkerTarget, value,
};
use s3mock_server::SignatureVerification;
use tempfile::TempDir;

use harness::{PullWorker, S3Emulator, ServeConfig, ServeProcess, free_loopback_address};

const NODE_ID: &str = "serve-node";
const PRIMARY_BUCKET: &str = "dex-serve";
const PRIMARY_PREFIX: &str = "cluster-a";
const HARD_DELETE_BUCKET: &str = "dex-serve-hard-delete";
const FLOW_TYPE: &str = "ServeSmokeFlow";
const FLOW_ID: &str = "serve-flow-1";
/// Longer than every test budget, so only a released lease lets the next process start.
const HELD_LEASE_DURATION: &str = "60s";
const SHUTDOWN_BUDGET: Duration = Duration::from_secs(15);

fn string_value(text: &str) -> Value {
    Value {
        kind: Some(value::Kind::StringValue(text.to_owned())),
    }
}

/// SIGTERM stops serving, releases the lease, and exits 0; the next process takes over at once.
///
/// The primary store verifies environment credentials with a session token; hard-delete values use anonymous access.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sigterm_releases_the_lease_and_a_second_process_takes_over() {
    let primary = S3Emulator::start(PRIMARY_BUCKET, SignatureVerification::Strict).await;
    let hard_delete = S3Emulator::start(HARD_DELETE_BUCKET, SignatureVerification::Off).await;
    let primary_endpoint = primary.endpoint();
    let hard_delete_endpoint = hard_delete.endpoint();
    let directory = TempDir::new().expect("a temporary test directory can be created");

    let first_address = free_loopback_address();
    let first_config = directory.path().join("first.yaml");
    ServeConfig {
        primary_endpoint: &primary_endpoint,
        hard_delete_endpoint: Some(&hard_delete_endpoint),
        listen: first_address,
        local_directory: &directory.path().join("first-store"),
        lease_duration: HELD_LEASE_DURATION,
        fail_if_held: true,
    }
    .write(&first_config);
    let mut first = ServeProcess::spawn(
        &first_config,
        first_address,
        directory.path().join("first.log"),
    );
    let channel = first.wait_until_serving().await;
    let worker = PullWorker::start(
        WorkerTaskServiceClient::new(channel.clone()),
        FLOW_TYPE,
        Box::new(|request| {
            assert_eq!(request.step_type, "GreetStep");
            InvokeExecuteMethodResponse {
                step_decision: Some(StepDecision {
                    close_decision: Some(CloseDecision {
                        close_decision_type: CloseDecisionType::ForceComplete.into(),
                        close_input: Some(string_value("hello from serve")),
                        ..CloseDecision::default()
                    }),
                    ..StepDecision::default()
                }),
                ..InvokeExecuteMethodResponse::default()
            }
        }),
    )
    .await;
    let mut flow_client = FlowServiceClient::new(channel);
    let started = flow_client
        .start_flow(StartFlowRequest {
            flow_id: FLOW_ID.to_owned(),
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
            request_id: "serve-request-1".to_owned(),
            ..StartFlowRequest::default()
        })
        .await
        .expect("StartFlow succeeds")
        .into_inner();
    let result = flow_client
        .wait_for_flow(WaitForFlowRequest {
            flow_id: FLOW_ID.to_owned(),
            needs_results: true,
            wait_time_seconds: 30,
            ..WaitForFlowRequest::default()
        })
        .await
        .expect("WaitForFlow succeeds")
        .into_inner();
    assert_eq!(result.flow_status, FlowStatus::Completed as i32);
    assert_eq!(
        result.results[0].completed_step_output,
        Some(string_value("hello from serve"))
    );

    let first_status = first.terminate(SHUTDOWN_BUDGET).await;
    let first_log = first.log();
    assert_eq!(first_status.code(), Some(0), "{first_log}");
    assert!(
        first_log.contains("dex-server-zd shutting down"),
        "{first_log}"
    );
    assert!(first_log.contains("engine node stopped"), "{first_log}");
    assert!(!first_log.contains("lease release failed"), "{first_log}");
    worker.stop().await;

    // A fresh local directory proves the takeover recovers from object storage alone.
    let second_address = free_loopback_address();
    let second_config = directory.path().join("second.yaml");
    ServeConfig {
        primary_endpoint: &primary_endpoint,
        hard_delete_endpoint: Some(&hard_delete_endpoint),
        listen: second_address,
        local_directory: &directory.path().join("second-store"),
        lease_duration: HELD_LEASE_DURATION,
        fail_if_held: true,
    }
    .write(&second_config);
    let mut second = ServeProcess::spawn(
        &second_config,
        second_address,
        directory.path().join("second.log"),
    );
    let channel = second.wait_until_serving().await;
    let summary = FlowServiceClient::new(channel)
        .get_flow_summary(GetFlowSummaryRequest {
            flow_id: FLOW_ID.to_owned(),
            run_id: String::new(),
        })
        .await
        .expect("GetFlowSummary succeeds on the second process")
        .into_inner();
    assert_eq!(summary.flow_status, FlowStatus::Completed as i32);
    assert_eq!(summary.first_run_id, started.run_id);

    assert_eq!(
        primary.top_level_prefixes(PRIMARY_BUCKET),
        [format!("{PRIMARY_PREFIX}/")]
    );
    let hard_delete_requests = hard_delete.request_log().await;
    assert!(
        hard_delete_requests.contains(&format!("\"bucket\":\"{HARD_DELETE_BUCKET}\""))
            && hard_delete_requests.contains("\"key\":\"hard-delete/"),
        "{hard_delete_requests}"
    );

    let second_status = second.terminate(SHUTDOWN_BUDGET).await;
    assert_eq!(second_status.code(), Some(0), "{}", second.log());
    primary.shutdown().await;
    hard_delete.shutdown().await;
}

/// A node whose lease is taken stops serving and exits non-zero.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn losing_the_lease_exits_non_zero() {
    let primary = S3Emulator::start(PRIMARY_BUCKET, SignatureVerification::Strict).await;
    let primary_endpoint = primary.endpoint();
    let directory = TempDir::new().expect("a temporary test directory can be created");
    let address = free_loopback_address();
    let config = directory.path().join("serve.yaml");
    ServeConfig {
        primary_endpoint: &primary_endpoint,
        hard_delete_endpoint: None,
        listen: address,
        local_directory: &directory.path().join("store"),
        lease_duration: "3s",
        fail_if_held: false,
    }
    .write(&config);
    let mut process = ServeProcess::spawn(&config, address, directory.path().join("serve.log"));
    process.wait_until_serving().await;

    let mut store_config = S3ObjectStoreConfig::new("us-east-1", PRIMARY_BUCKET);
    store_config.endpoint = Some(primary_endpoint.clone());
    store_config.key_prefix = PRIMARY_PREFIX.to_owned();
    store_config.force_path_style = true;
    store_config.credentials = S3Credentials::Static {
        access_key_id: harness::ACCESS_KEY_ID.to_owned(),
        secret_access_key: harness::SECRET_ACCESS_KEY.to_owned(),
        session_token: None,
    };
    S3ObjectStore::new(store_config)
        .await
        .expect("the test store is configured")
        .put(
            &format!("lease/{NODE_ID}"),
            Bytes::from_static(b"taken over by another process"),
            WriteCondition::Unconditional,
        )
        .await
        .expect("the lease object is overwritten");

    let status = process.wait_for_exit(SHUTDOWN_BUDGET).await;
    let log = process.log();
    assert_eq!(status.code(), Some(1), "{log}");
    assert!(log.contains("dex-server-zd: the node stopped"), "{log}");
    primary.shutdown().await;
}
