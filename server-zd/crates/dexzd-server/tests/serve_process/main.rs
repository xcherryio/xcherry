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
mod lease_release;
mod metrics;
mod purging_reads;
mod robustness;
mod standby_health;
mod standby_takeover;

use std::time::Duration;

use bytes::Bytes;
use dexzd_object_store::{
    ObjectStore, S3Credentials, S3ObjectStore, S3ObjectStoreConfig, WriteCondition,
};
use dexzd_protocol::dex::flow_service_client::FlowServiceClient;
use dexzd_protocol::dex::worker_task_service_client::WorkerTaskServiceClient;
use dexzd_protocol::dex::{
    AttributeWrite, CloseDecision, CloseDecisionType, FlowConfig, FlowStartOptions, FlowStatus,
    GetFlowSummaryRequest, IndexConfig, IndexType, InvokeExecuteMethodResponse, SearchFlowsRequest,
    SearchFlowsResponseEntry, SetAttributesRequest, StartFlowRequest, StepDecision, StepOptions,
    Value, WaitForFlowRequest, WorkerTarget, value,
};
use s3mock_server::SignatureVerification;
use tempfile::TempDir;
use tonic::transport::Channel;

use harness::{
    ParadeDbSchema, ParadeDbSettings, ParadeDbTcpProxy, PullWorker, S3Emulator, ServeConfig,
    ServeProcess, free_loopback_address,
};

const NODE_ID: &str = "serve-node";
const PRIMARY_BUCKET: &str = "dex-serve";
const PRIMARY_PREFIX: &str = "cluster-a";
const HARD_DELETE_BUCKET: &str = "dex-serve-hard-delete";
const FLOW_TYPE: &str = "ServeSmokeFlow";
const FLOW_ID: &str = "serve-flow-1";
/// Longer than every test budget, so only a released lease lets the next process start.
const HELD_LEASE_DURATION: &str = "60s";
const SHUTDOWN_BUDGET: Duration = Duration::from_secs(15);
/// Covers the projection's startup catch-up and rebuild.
const SEARCH_BUDGET: Duration = Duration::from_secs(30);

fn string_value(text: &str) -> Value {
    Value {
        kind: Some(value::Kind::StringValue(text.to_owned())),
    }
}

/// SIGTERM stops serving, releases the lease, and exits 0; the next process takes over at once.
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
        parade_db: None,
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
        parade_db: None,
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
        parade_db: None,
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

/// SIGTERM after the node took its lease finishes recovery, exits 0, and releases the lease.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sigterm_during_recovery_exits_zero_and_releases_the_lease() {
    let primary = S3Emulator::start(PRIMARY_BUCKET, SignatureVerification::Strict).await;
    let primary_endpoint = primary.endpoint();
    let directory = TempDir::new().expect("a temporary test directory can be created");
    // Only recovery reads the log, after the lease is taken, so slow reads hold startup there.
    primary
        .set_fault_rules(&format!(
            r#"[{{"operation":"GetObject","key_prefix":"{PRIMARY_PREFIX}/log/","action":{{"type":"delay","milliseconds":5000}}}}]"#
        ))
        .await;
    let first_address = free_loopback_address();
    let first_config = directory.path().join("first.yaml");
    ServeConfig {
        primary_endpoint: &primary_endpoint,
        hard_delete_endpoint: None,
        listen: first_address,
        local_directory: &directory.path().join("first-store"),
        lease_duration: HELD_LEASE_DURATION,
        fail_if_held: true,
        parade_db: None,
    }
    .write(&first_config);
    let mut first = ServeProcess::spawn(
        &first_config,
        first_address,
        directory.path().join("first.log"),
    );
    let lease_prefix = format!("{PRIMARY_PREFIX}/lease/");
    tokio::time::timeout(SEARCH_BUDGET, async {
        while !primary.has_object_under(PRIMARY_BUCKET, &lease_prefix) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the node takes its lease");
    let first_status = first.terminate(SHUTDOWN_BUDGET).await;
    let first_log = first.log();
    assert_eq!(first_status.code(), Some(0), "{first_log}");
    assert!(
        first_log.contains("termination signal during startup"),
        "{first_log}"
    );
    assert!(first_log.contains("engine node stopped"), "{first_log}");
    primary.set_fault_rules("[]").await;

    let second_address = free_loopback_address();
    let second_config = directory.path().join("second.yaml");
    ServeConfig {
        primary_endpoint: &primary_endpoint,
        hard_delete_endpoint: None,
        listen: second_address,
        local_directory: &directory.path().join("second-store"),
        lease_duration: HELD_LEASE_DURATION,
        fail_if_held: true,
        parade_db: None,
    }
    .write(&second_config);
    let mut second = ServeProcess::spawn(
        &second_config,
        second_address,
        directory.path().join("second.log"),
    );
    second.wait_until_serving().await;
    let second_status = second.terminate(SHUTDOWN_BUDGET).await;
    assert_eq!(second_status.code(), Some(0), "{}", second.log());
    primary.shutdown().await;
}

/// ParadeDB SearchFlows reads committed Flows, and a new process rebuilds a cleared projection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paradedb_search_projects_commits_and_rebuilds_on_startup() {
    let Ok(database_url) = std::env::var("DEX_ZD_PARADEDB_URL") else {
        return;
    };
    let schema = ParadeDbSchema::create(&database_url).await;
    let primary = S3Emulator::start(PRIMARY_BUCKET, SignatureVerification::Strict).await;
    let primary_endpoint = primary.endpoint();
    let directory = TempDir::new().expect("a temporary test directory can be created");
    let serve_config = |name: &str, listen| {
        let path = directory.path().join(format!("{name}.yaml"));
        ServeConfig {
            primary_endpoint: &primary_endpoint,
            hard_delete_endpoint: None,
            listen,
            local_directory: &directory.path().join(format!("{name}-store")),
            lease_duration: HELD_LEASE_DURATION,
            fail_if_held: true,
            parade_db: Some(ParadeDbSettings {
                connection_string: &database_url,
                schema: schema.name(),
            }),
        }
        .write(&path);
        path
    };

    let first_address = free_loopback_address();
    let mut first = ServeProcess::spawn(
        &serve_config("first", first_address),
        first_address,
        directory.path().join("first.log"),
    );
    let channel = first.wait_until_serving().await;
    let worker = start_greeting_worker(channel.clone()).await;
    let mut flow_client = FlowServiceClient::new(channel);
    let first_run_id = run_greeting_flow(&mut flow_client, "search-flow-1").await;
    let found = search_until_completed(&mut flow_client, "search-flow-1").await;
    assert_eq!(found.run_id, first_run_id);
    assert_eq!(found.flow_type, FLOW_TYPE);
    worker.stop().await;
    let first_status = first.terminate(SHUTDOWN_BUDGET).await;
    let first_log = first.log();
    assert_eq!(first_status.code(), Some(0), "{first_log}");
    assert!(
        first_log.contains("the ParadeDB search projection caught up"),
        "{first_log}"
    );

    schema.clear_projection().await;
    let second_address = free_loopback_address();
    let mut second = ServeProcess::spawn(
        &serve_config("second", second_address),
        second_address,
        directory.path().join("second.log"),
    );
    let channel = second.wait_until_serving().await;
    let mut flow_client = FlowServiceClient::new(channel.clone());
    let rebuilt = search_until_completed(&mut flow_client, "search-flow-1").await;
    assert_eq!(rebuilt.run_id, first_run_id);

    let worker = start_greeting_worker(channel).await;
    let second_run_id = run_greeting_flow(&mut flow_client, "search-flow-2").await;
    // Read-your-writes: the Flow's committed completion is searchable without polling.
    let entries = search_greeting_flows(&mut flow_client)
        .await
        .expect("SearchFlows succeeds once the projection caught up");
    let projected = entries
        .iter()
        .find(|entry| entry.flow_id == "search-flow-2")
        .unwrap_or_else(|| panic!("search-flow-2 is searchable: {entries:?}"));
    assert_eq!(projected.run_id, second_run_id);
    assert_eq!(projected.flow_status, FlowStatus::Completed as i32);
    worker.stop().await;

    let second_status = second.terminate(SHUTDOWN_BUDGET).await;
    assert_eq!(second_status.code(), Some(0), "{}", second.log());
    primary.shutdown().await;
    schema.drop_schema().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paradedb_outage_overflows_the_queue_and_rebuilds_every_committed_write() {
    let Ok(database_url) = std::env::var("DEX_ZD_PARADEDB_URL") else {
        return;
    };
    let schema = ParadeDbSchema::create(&database_url).await;
    let mut proxy = ParadeDbTcpProxy::start(&database_url).await;
    let proxied_database_url = proxy.connection_string(&database_url);
    let primary = S3Emulator::start(PRIMARY_BUCKET, SignatureVerification::Strict).await;
    let primary_endpoint = primary.endpoint();
    let directory = TempDir::new().expect("a temporary test directory can be created");
    let address = free_loopback_address();
    let config_path = directory.path().join("outage.yaml");
    ServeConfig {
        primary_endpoint: &primary_endpoint,
        hard_delete_endpoint: None,
        listen: address,
        local_directory: &directory.path().join("store"),
        lease_duration: HELD_LEASE_DURATION,
        fail_if_held: true,
        parade_db: Some(ParadeDbSettings {
            connection_string: &proxied_database_url,
            schema: schema.name(),
        }),
    }
    .write(&config_path);
    let mut server =
        ServeProcess::spawn(&config_path, address, directory.path().join("outage.log"));
    let channel = server.wait_until_serving().await;
    let mut flow_client = FlowServiceClient::new(channel);
    let flow_id = "outage-flow";
    flow_client
        .start_flow(StartFlowRequest {
            flow_id: flow_id.to_owned(),
            flow_type: FLOW_TYPE.to_owned(),
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
            request_id: "outage-start".to_owned(),
            ..StartFlowRequest::default()
        })
        .await
        .expect("the initial Flow start commits");
    search_greeting_flows(&mut flow_client)
        .await
        .expect("the initial Flow is searchable");

    proxy.cut().await;
    const WRITE_COUNT: usize = 600;
    let value_text = "x".repeat(1024);
    for write_number in 0..WRITE_COUNT {
        let attribute_name = format!("outage-{write_number:04}");
        let value = string_value(&format!("{write_number:04}-{value_text}"));
        tokio::time::timeout(
            Duration::from_secs(10),
            flow_client.set_attributes(SetAttributesRequest {
                flow_id: flow_id.to_owned(),
                attributes: vec![AttributeWrite {
                    key: attribute_name,
                    value: Some(value),
                    index_config: Some(IndexConfig {
                        enable: true,
                        r#type: IndexType::Keyword as i32,
                        ..IndexConfig::default()
                    }),
                    ..AttributeWrite::default()
                }],
                request_id: format!("outage-write-{write_number:04}"),
                ..SetAttributesRequest::default()
            }),
        )
        .await
        .expect("commits continue during the ParadeDB outage")
        .expect("an Attribute write commits during the ParadeDB outage");
    }
    assert!(
        server
            .log()
            .contains("a committed frame could not be queued for the search projection"),
        "the bounded projection queue overflows during the outage: {}",
        server.log()
    );
    proxy.restore();

    let mut retry = tokio::time::interval(Duration::from_millis(200));
    let mut last_search_result = String::new();
    let projected = tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            retry.tick().await;
            let search_result = search_greeting_flows(&mut flow_client).await;
            last_search_result = format!("{search_result:?}");
            match search_result {
                Ok(entries) => {
                    if let Some(entry) = entries.into_iter().find(|entry| entry.flow_id == flow_id)
                        && entry.indexed_attributes.len() >= WRITE_COUNT
                    {
                        return entry;
                    }
                }
                Err(status) if status.code() == tonic::Code::Unavailable => {}
                Err(status) => panic!("SearchFlows failed after ParadeDB recovery: {status}"),
            }
        }
    })
    .await
    .unwrap_or_else(|error| {
        panic!(
            "the partition rebuild makes every committed write searchable: {error}; last SearchFlows result: {last_search_result}; server log: {}",
            server.log()
        )
    });
    for write_number in 0..WRITE_COUNT {
        let attribute_name = format!("outage-{write_number:04}");
        let expected_value = string_value(&format!("{write_number:04}-{value_text}"));
        let actual_value = projected
            .indexed_attributes
            .iter()
            .find(|attribute| attribute.key == attribute_name)
            .and_then(|attribute| attribute.value.as_ref());
        assert_eq!(actual_value, Some(&expected_value), "{attribute_name}");
    }
    // Overflowed frames reach ParadeDB only through the partition rebuild.
    let exit_status = server.terminate(SHUTDOWN_BUDGET).await;
    assert_eq!(exit_status.code(), Some(0), "{}", server.log());
    proxy.shutdown().await;
    primary.shutdown().await;
    schema.drop_schema().await;
}

/// Starts a pull Worker whose GreetStep completes the Flow with a greeting.
async fn start_greeting_worker(channel: Channel) -> PullWorker {
    PullWorker::start(
        WorkerTaskServiceClient::new(channel),
        FLOW_TYPE,
        Box::new(|_| InvokeExecuteMethodResponse {
            step_decision: Some(StepDecision {
                close_decision: Some(CloseDecision {
                    close_decision_type: CloseDecisionType::ForceComplete.into(),
                    close_input: Some(string_value("hello from serve")),
                    ..CloseDecision::default()
                }),
                ..StepDecision::default()
            }),
            ..InvokeExecuteMethodResponse::default()
        }),
    )
    .await
}

/// Starts `flow_id` on the pull Worker, waits until it completes, and returns its run ID.
async fn run_greeting_flow(flow_client: &mut FlowServiceClient<Channel>, flow_id: &str) -> String {
    let started = flow_client
        .start_flow(StartFlowRequest {
            flow_id: flow_id.to_owned(),
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
            request_id: format!("{flow_id}-request"),
            ..StartFlowRequest::default()
        })
        .await
        .expect("StartFlow succeeds")
        .into_inner();
    let result = flow_client
        .wait_for_flow(WaitForFlowRequest {
            flow_id: flow_id.to_owned(),
            wait_time_seconds: 30,
            ..WaitForFlowRequest::default()
        })
        .await
        .expect("WaitForFlow succeeds")
        .into_inner();
    assert_eq!(result.flow_status, FlowStatus::Completed as i32);
    started.run_id
}

/// Searches until `flow_id` appears as Completed; Unavailable means the projection is catching up.
async fn search_until_completed(
    flow_client: &mut FlowServiceClient<Channel>,
    flow_id: &str,
) -> SearchFlowsResponseEntry {
    let deadline = tokio::time::Instant::now() + SEARCH_BUDGET;
    loop {
        let last_result = match search_greeting_flows(flow_client).await {
            Ok(entries) => {
                if let Some(entry) = entries.iter().find(|entry| {
                    entry.flow_id == flow_id && entry.flow_status == FlowStatus::Completed as i32
                }) {
                    return entry.clone();
                }
                format!("{entries:?}")
            }
            Err(status) if status.code() == tonic::Code::Unavailable => status.to_string(),
            Err(status) => panic!("SearchFlows failed: {status}"),
        };
        assert!(
            tokio::time::Instant::now() < deadline,
            "{flow_id} was not searchable as Completed within {SEARCH_BUDGET:?}: {last_result}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn search_greeting_flows(
    flow_client: &mut FlowServiceClient<Channel>,
) -> Result<Vec<SearchFlowsResponseEntry>, tonic::Status> {
    flow_client
        .search_flows(SearchFlowsRequest {
            query: format!("FlowType = '{FLOW_TYPE}'"),
            page_size: 10,
            ..SearchFlowsRequest::default()
        })
        .await
        .map(|response| response.into_inner().flow_runs)
}
