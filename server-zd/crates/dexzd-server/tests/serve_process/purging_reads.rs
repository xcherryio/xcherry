// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Client reads of a run whose retention purge is committed but whose RunPurge is not.

use std::time::Duration;

use dexzd_protocol::dex::flow_service_client::FlowServiceClient;
use dexzd_protocol::dex::{
    AttributeMatch, AttributeMatchOperator, AttributeWrite, ChannelMessage, ErrorSubStatus,
    FlowConfig, FlowStartOptions, FlowStatus, GetAttributesRequest, GetChannelMessagesRequest,
    GetFlowStateRequest, GetFlowSummaryRequest, GetHistoryEventsRequest, InvokeRpcRequest,
    LoadBlobRequestEntry, LoadBlobsRequest, PublishToChannelRequest, ServiceErrorResponse,
    StartFlowRequest, StepOptions, Value, WaitForAttributeRequest, WaitForFlowRequest,
    WaitForHistoryEventRequest, WaitForStepCompletionRequest, WorkerTarget, value,
};
use prost::Message;
use s3mock_server::SignatureVerification;
use tempfile::TempDir;
use tonic::transport::Channel;

use crate::harness::{
    ParadeDbSchema, ParadeDbSettings, S3Emulator, ServeConfig, ServeProcess, free_loopback_address,
};
use crate::{
    FLOW_TYPE, HARD_DELETE_BUCKET, HELD_LEASE_DURATION, PRIMARY_BUCKET, SEARCH_BUDGET,
    SHUTDOWN_BUDGET, search_greeting_flows, start_greeting_worker, string_value,
};

const PURGING_FLOW_ID: &str = "purging-flow-1";
const CLOSED_FLOW_RETENTION: &str = "2s";
/// Covers the retention period and the timer that commits the purge decision.
const PURGE_BUDGET: Duration = Duration::from_secs(30);
const SECRET_ATTRIBUTE: &str = "secret";
const REPORT_ATTRIBUTE: &str = "report";
const ORDERS_CHANNEL: &str = "orders";
const SERVICE_ERROR_RESPONSE_TYPE_URL: &str = "type.googleapis.com/dex.ServiceErrorResponse";

/// The `google.rpc.Status` that carries the `ServiceErrorResponse` in `grpc-status-details-bin`.
#[derive(Clone, PartialEq, prost::Message)]
struct GoogleRpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<StatusDetail>,
}

/// One `google.protobuf.Any` detail of a [`GoogleRpcStatus`].
#[derive(Clone, PartialEq, prost::Message)]
struct StatusDetail {
    #[prost(string, tag = "1")]
    type_url: String,
    #[prost(bytes = "vec", tag = "2")]
    value: Vec<u8>,
}

/// A serve process whose only run is purging, with its object listing failing before RunPurge.
struct FrozenPurge {
    primary: S3Emulator,
    hard_delete: S3Emulator,
    server: ServeProcess,
    flow_client: FlowServiceClient<Channel>,
    run_id: String,
    /// The run's large Attribute as GetAttributes returned it: an internal blob ID.
    report_reference: Value,
    _directory: TempDir,
}

impl FrozenPurge {
    /// Completes a Flow holding hard-delete, large, and Channel values, then waits for its held purge.
    async fn start(parade_db: Option<ParadeDbSettings<'_>>) -> Self {
        let primary = S3Emulator::start(PRIMARY_BUCKET, SignatureVerification::Strict).await;
        let hard_delete = S3Emulator::start(HARD_DELETE_BUCKET, SignatureVerification::Off).await;
        let directory = TempDir::new().expect("a temporary test directory can be created");
        let address = free_loopback_address();
        let config_path = directory.path().join("purging.yaml");
        ServeConfig {
            primary_endpoint: &primary.endpoint(),
            hard_delete_endpoint: Some(&hard_delete.endpoint()),
            listen: address,
            local_directory: &directory.path().join("store"),
            lease_duration: HELD_LEASE_DURATION,
            fail_if_held: true,
            parade_db,
        }
        .write_with_closed_flow_retention(&config_path, CLOSED_FLOW_RETENTION);
        let mut server =
            ServeProcess::spawn(&config_path, address, directory.path().join("purging.log"));
        let channel = server.wait_until_serving().await;
        let mut flow_client = FlowServiceClient::new(channel.clone());
        let run_id = start_flow_with_retained_values(&mut flow_client).await;
        let report_reference = read_report_reference(&mut flow_client).await;
        search_until_purging_flow_is_listed(&mut flow_client, true).await;
        assert!(
            !hard_delete.keys_under(HARD_DELETE_BUCKET, "").is_empty(),
            "the hard-delete Attribute is an object"
        );
        // Fails every listing, so the purge retries before it deletes the run's objects.
        hard_delete
            .set_fault_rules(&format!(
                r#"[{{"operation":"ListObjectsV2","bucket":"{HARD_DELETE_BUCKET}","action":{{"type":"error","status":503}}}}]"#
            ))
            .await;

        let worker = start_greeting_worker(channel).await;
        let result = flow_client
            .wait_for_flow(WaitForFlowRequest {
                flow_id: PURGING_FLOW_ID.to_owned(),
                wait_time_seconds: 30,
                ..WaitForFlowRequest::default()
            })
            .await
            .expect("WaitForFlow succeeds")
            .into_inner();
        assert_eq!(result.flow_status, FlowStatus::Completed as i32);
        worker.stop().await;
        wait_until_the_purge_lists_objects(&hard_delete, &server).await;
        Self {
            primary,
            hard_delete,
            server,
            flow_client,
            run_id,
            report_reference,
            _directory: directory,
        }
    }

    /// Lets the purge finish, then stops the process and emulators.
    async fn finish(mut self) {
        self.hard_delete.clear_fault_rules().await;
        let exit_status = self.server.terminate(SHUTDOWN_BUDGET).await;
        assert_eq!(exit_status.code(), Some(0), "{}", self.server.log());
        self.hard_delete.shutdown().await;
        self.primary.shutdown().await;
    }
}

/// Starts the Flow without a Worker, so it keeps running, and returns its run ID.
async fn start_flow_with_retained_values(flow_client: &mut FlowServiceClient<Channel>) -> String {
    let started = flow_client
        .start_flow(StartFlowRequest {
            flow_id: PURGING_FLOW_ID.to_owned(),
            flow_type: FLOW_TYPE.to_owned(),
            start_step_type: "GreetStep".to_owned(),
            step_input: Some(string_value("hello")),
            step_options: Some(StepOptions {
                skip_wait_for: true,
                ..StepOptions::default()
            }),
            flow_start_options: Some(FlowStartOptions {
                attributes: vec![
                    AttributeWrite {
                        key: SECRET_ATTRIBUTE.to_owned(),
                        value: Some(string_value("classified")),
                        is_hard_delete_on_null: true,
                        ..AttributeWrite::default()
                    },
                    AttributeWrite {
                        key: REPORT_ATTRIBUTE.to_owned(),
                        value: Some(string_value(&"r".repeat(200))),
                        ..AttributeWrite::default()
                    },
                ],
                flow_config_override: Some(FlowConfig {
                    worker_target: Some(WorkerTarget {
                        is_pull_mode: true,
                        ..WorkerTarget::default()
                    }),
                    ..FlowConfig::default()
                }),
                ..FlowStartOptions::default()
            }),
            request_id: format!("{PURGING_FLOW_ID}-request"),
            ..StartFlowRequest::default()
        })
        .await
        .expect("StartFlow succeeds")
        .into_inner();
    flow_client
        .publish_to_channel(PublishToChannelRequest {
            flow_id: PURGING_FLOW_ID.to_owned(),
            messages: vec![ChannelMessage {
                channel_name: ORDERS_CHANNEL.to_owned(),
                value: Some(string_value("order placed")),
                ..ChannelMessage::default()
            }],
            ..PublishToChannelRequest::default()
        })
        .await
        .expect("PublishToChannel succeeds");
    started.run_id
}

/// Returns the large Attribute's internal blob ID after checking every value is readable.
async fn read_report_reference(flow_client: &mut FlowServiceClient<Channel>) -> Value {
    let attributes = flow_client
        .get_attributes(GetAttributesRequest {
            flow_id: PURGING_FLOW_ID.to_owned(),
            all_keys: true,
            ..GetAttributesRequest::default()
        })
        .await
        .expect("GetAttributes succeeds")
        .into_inner()
        .attributes;
    let attribute_value = |key: &str| {
        attributes
            .iter()
            .find(|attribute| attribute.key == key)
            .and_then(|attribute| attribute.value.clone())
            .unwrap_or_else(|| panic!("{key} is readable: {attributes:?}"))
    };
    assert_eq!(
        attribute_value(SECRET_ATTRIBUTE),
        string_value("classified")
    );
    let report_reference = attribute_value(REPORT_ATTRIBUTE);
    let Some(value::Kind::InternalBlobIdForStringValue(blob_id)) = &report_reference.kind else {
        panic!("a large Attribute is returned as a blob ID: {report_reference:?}");
    };
    let loaded = flow_client
        .load_blobs(load_report_request(&report_reference))
        .await
        .expect("LoadBlobs succeeds before retention")
        .into_inner();
    assert_eq!(
        loaded.values.get(blob_id),
        Some(&string_value(&"r".repeat(200)))
    );
    report_reference
}

/// Waits for the purge effect's first failed listing, which follows the committed purge decision.
async fn wait_until_the_purge_lists_objects(hard_delete: &S3Emulator, server: &ServeProcess) {
    let deadline = tokio::time::Instant::now() + PURGE_BUDGET;
    // Only the fault rule makes the hard-delete emulator answer 503.
    while !hard_delete.request_log().await.contains("\"status\":503") {
        assert!(
            tokio::time::Instant::now() < deadline,
            "retention did not start the purge within {PURGE_BUDGET:?}:\n{}",
            server.log()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Searches until SearchFlows lists the Flow as `is_listed` says; Unavailable means catching up.
async fn search_until_purging_flow_is_listed(
    flow_client: &mut FlowServiceClient<Channel>,
    is_listed: bool,
) {
    let deadline = tokio::time::Instant::now() + SEARCH_BUDGET;
    loop {
        let last_result = match search_greeting_flows(flow_client).await {
            Ok(entries) => {
                if entries.iter().any(|entry| entry.flow_id == PURGING_FLOW_ID) == is_listed {
                    return;
                }
                format!("{entries:?}")
            }
            Err(status) if status.code() == tonic::Code::Unavailable => status.to_string(),
            Err(status) => panic!("SearchFlows failed: {status}"),
        };
        assert!(
            tokio::time::Instant::now() < deadline,
            "SearchFlows did not reach is_listed={is_listed} within {SEARCH_BUDGET:?}: {last_result}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn load_report_request(report_reference: &Value) -> LoadBlobsRequest {
    LoadBlobsRequest {
        entries: vec![LoadBlobRequestEntry {
            flow_id: PURGING_FLOW_ID.to_owned(),
            blob_value: Some(report_reference.clone()),
        }],
    }
}

/// Asserts the Go Server's answer for a missing Flow: NotFound, FLOW_NOT_EXISTS, and its detail.
fn assert_flow_not_found(api: &str, status: &tonic::Status) {
    assert_eq!(status.code(), tonic::Code::NotFound, "{api}: {status:?}");
    assert_eq!(
        status.message(),
        format!("Flow run is not found for flow ID {PURGING_FLOW_ID}"),
        "{api}"
    );
    let details = GoogleRpcStatus::decode(status.details())
        .unwrap_or_else(|error| panic!("{api}: {status:?} has no google.rpc.Status: {error}"));
    let response = details
        .details
        .iter()
        .find(|detail| detail.type_url == SERVICE_ERROR_RESPONSE_TYPE_URL)
        .map(|detail| ServiceErrorResponse::decode(detail.value.as_slice()))
        .unwrap_or_else(|| panic!("{api}: {status:?} has no ServiceErrorResponse"))
        .expect("the ServiceErrorResponse decodes");
    assert_eq!(
        response.sub_status,
        ErrorSubStatus::FlowNotExists as i32,
        "{api}"
    );
    assert_eq!(response.detail, status.message(), "{api}");
}

/// LoadBlobs of a purging run omits its values.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn load_blobs_of_a_purging_run_omits_values() {
    let mut purge = FrozenPurge::start(None).await;
    let loaded = purge
        .flow_client
        .load_blobs(load_report_request(&purge.report_reference))
        .await
        .expect("LoadBlobs succeeds during purge")
        .into_inner();
    assert!(loaded.values.is_empty());
    purge.finish().await;
}

/// Checks Flow reads return NotFound while LoadBlobs omits the purging run's values.
async fn assert_purging_flow_reads_not_found_and_blobs_omitted(purge: &mut FrozenPurge) {
    let client = &mut purge.flow_client;
    let flow_id = PURGING_FLOW_ID.to_owned();
    let run_id = purge.run_id.clone();
    for (api, status) in [
        (
            "GetAttributes",
            client
                .get_attributes(GetAttributesRequest {
                    flow_id: flow_id.clone(),
                    all_keys: true,
                    ..GetAttributesRequest::default()
                })
                .await
                .map(drop),
        ),
        (
            "GetAttributes of the hard-delete Attribute",
            client
                .get_attributes(GetAttributesRequest {
                    flow_id: flow_id.clone(),
                    run_id: run_id.clone(),
                    keys: vec![SECRET_ATTRIBUTE.to_owned()],
                    ..GetAttributesRequest::default()
                })
                .await
                .map(drop),
        ),
        (
            "GetFlowState",
            client
                .get_flow_state(GetFlowStateRequest {
                    flow_id: flow_id.clone(),
                    ..GetFlowStateRequest::default()
                })
                .await
                .map(drop),
        ),
        (
            "GetFlowSummary",
            client
                .get_flow_summary(GetFlowSummaryRequest {
                    flow_id: flow_id.clone(),
                    run_id: run_id.clone(),
                })
                .await
                .map(drop),
        ),
        (
            "GetChannelMessages",
            client
                .get_channel_messages(GetChannelMessagesRequest {
                    flow_id: flow_id.clone(),
                    channel_name: ORDERS_CHANNEL.to_owned(),
                    ..GetChannelMessagesRequest::default()
                })
                .await
                .map(drop),
        ),
        (
            "InvokeRPC",
            client
                .invoke_rpc(InvokeRpcRequest {
                    flow_id: flow_id.clone(),
                    rpc_name: "getOrder".to_owned(),
                    request_id: "purging-rpc".to_owned(),
                    timeout_seconds: 5,
                    ..InvokeRpcRequest::default()
                })
                .await
                .map(drop),
        ),
        (
            "GetHistoryEvents",
            client
                .get_history_events(GetHistoryEventsRequest {
                    flow_id: flow_id.clone(),
                    run_id: run_id.clone(),
                    ..GetHistoryEventsRequest::default()
                })
                .await
                .map(drop),
        ),
        (
            "WaitForHistoryEvent",
            client
                .wait_for_history_event(WaitForHistoryEventRequest {
                    flow_id: flow_id.clone(),
                    run_id: run_id.clone(),
                    next_internal_event_id: 1,
                })
                .await
                .map(drop),
        ),
        (
            "WaitForFlow of the latest run",
            client
                .wait_for_flow(WaitForFlowRequest {
                    flow_id: flow_id.clone(),
                    wait_time_seconds: 30,
                    ..WaitForFlowRequest::default()
                })
                .await
                .map(drop),
        ),
        (
            "WaitForFlow of the run",
            client
                .wait_for_flow(WaitForFlowRequest {
                    flow_id: flow_id.clone(),
                    run_id: run_id.clone(),
                    wait_time_seconds: 30,
                    ..WaitForFlowRequest::default()
                })
                .await
                .map(drop),
        ),
        (
            "WaitForStepCompletion",
            client
                .wait_for_step_completion(WaitForStepCompletionRequest {
                    flow_id: flow_id.clone(),
                    step_type: "GreetStep".to_owned(),
                    step_execution_number: "1".to_owned(),
                    request_timeout_seconds: 30,
                    ..WaitForStepCompletionRequest::default()
                })
                .await
                .map(drop),
        ),
        (
            "WaitForAttribute",
            client
                .wait_for_attribute(WaitForAttributeRequest {
                    flow_id: flow_id.clone(),
                    r#match: Some(AttributeMatch {
                        key: "status".to_owned(),
                        operator: AttributeMatchOperator::Equal as i32,
                        operand: Some(string_value("shipped")),
                    }),
                    request_timeout_seconds: 30,
                    ..WaitForAttributeRequest::default()
                })
                .await
                .map(drop),
        ),
    ] {
        let status = status.expect_err(api);
        assert_flow_not_found(api, &status);
    }
    let loaded = client
        .load_blobs(load_report_request(&purge.report_reference))
        .await
        .expect("LoadBlobs succeeds during purge")
        .into_inner();
    assert!(loaded.values.is_empty());
}

/// Checks reads before RunPurge commits, with and without hard-delete objects.
async fn assert_frozen_purge_reads(parade_db: Option<ParadeDbSettings<'_>>) {
    let mut purge = FrozenPurge::start(parade_db).await;
    assert!(
        !purge
            .hard_delete
            .keys_under(HARD_DELETE_BUCKET, "")
            .is_empty(),
        "the held purge has not deleted any object"
    );
    assert_purging_flow_reads_not_found_and_blobs_omitted(&mut purge).await;
    search_until_purging_flow_is_listed(&mut purge.flow_client, false).await;

    // The state after object deletion and before RunPurge commits.
    purge.hard_delete.delete_keys_under(HARD_DELETE_BUCKET, "");
    assert_purging_flow_reads_not_found_and_blobs_omitted(&mut purge).await;
    search_until_purging_flow_is_listed(&mut purge.flow_client, false).await;
    purge.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn frozen_purge_reads_are_not_found_except_load_blobs_with_builtin_search() {
    assert_frozen_purge_reads(None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn frozen_purge_reads_are_not_found_except_load_blobs_with_paradedb_search() {
    let Ok(database_url) = std::env::var("DEX_ZD_PARADEDB_URL") else {
        return;
    };
    let schema = ParadeDbSchema::create(&database_url).await;
    assert_frozen_purge_reads(Some(ParadeDbSettings {
        connection_string: &database_url,
        schema: schema.name(),
    }))
    .await;
    schema.drop_schema().await;
}
