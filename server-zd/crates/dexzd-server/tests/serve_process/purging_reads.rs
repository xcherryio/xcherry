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
    AttributeWrite, ChannelMessage, ErrorSubStatus, FlowConfig, FlowStartOptions, FlowStatus,
    GetAttributesRequest, GetFlowSummaryRequest, LoadBlobRequestEntry, LoadBlobsRequest,
    PublishToChannelRequest, ServiceErrorResponse, StartFlowRequest, StepOptions, Value,
    WaitForFlowRequest, WorkerTarget, value,
};
use prost::Message;
use s3mock_server::SignatureVerification;
use tempfile::TempDir;
use tonic::transport::Channel;

use crate::harness::{
    ParadeDbSettings, S3Emulator, ServeConfig, ServeProcess, free_loopback_address,
};
use crate::{
    FLOW_TYPE, HARD_DELETE_BUCKET, HELD_LEASE_DURATION, PRIMARY_BUCKET, SHUTDOWN_BUDGET,
    start_greeting_worker, string_value,
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

/// A serve process whose only run is purging: retention committed the purge, and the hard-delete
/// emulator holds the purge's object listing, so RunPurge cannot commit.
struct FrozenPurge {
    primary: S3Emulator,
    hard_delete: S3Emulator,
    server: ServeProcess,
    flow_client: FlowServiceClient<Channel>,
    /// The run's large Attribute as GetAttributes returned it: an internal blob ID.
    report_reference: Value,
    _directory: TempDir,
}

impl FrozenPurge {
    /// Runs a Flow with hard-delete, large, and Channel values to completion, then waits until
    /// its retention purge is committed and held before object deletion.
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
        start_flow_with_retained_values(&mut flow_client).await;
        let report_reference = read_report_reference(&mut flow_client).await;
        assert!(
            !hard_delete.keys_under(HARD_DELETE_BUCKET, "").is_empty(),
            "the hard-delete Attribute is an object"
        );
        // Holds every listing, so the purge stops before it deletes the run's objects.
        hard_delete
            .set_fault_rules(&format!(
                r#"[{{"operation":"ListObjectsV2","bucket":"{HARD_DELETE_BUCKET}","action":{{"type":"delay","milliseconds":600000}}}}]"#
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
        wait_until_purge_is_committed(&mut flow_client, &server).await;
        Self {
            primary,
            hard_delete,
            server,
            flow_client,
            report_reference,
            _directory: directory,
        }
    }

    /// Releases the held listing so the purge finishes, then stops the process and emulators.
    async fn finish(mut self) {
        self.hard_delete.clear_fault_rules().await;
        let exit_status = self.server.terminate(SHUTDOWN_BUDGET).await;
        assert_eq!(exit_status.code(), Some(0), "{}", self.server.log());
        self.hard_delete.shutdown().await;
        self.primary.shutdown().await;
    }
}

async fn start_flow_with_retained_values(flow_client: &mut FlowServiceClient<Channel>) {
    flow_client
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
        .expect("StartFlow succeeds");
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

/// Polls GetFlowSummary until the committed purge decision hides the run.
async fn wait_until_purge_is_committed(
    flow_client: &mut FlowServiceClient<Channel>,
    server: &ServeProcess,
) {
    let deadline = tokio::time::Instant::now() + PURGE_BUDGET;
    loop {
        match flow_client
            .get_flow_summary(GetFlowSummaryRequest {
                flow_id: PURGING_FLOW_ID.to_owned(),
                ..GetFlowSummaryRequest::default()
            })
            .await
        {
            Ok(_) => {}
            Err(status) if status.code() == tonic::Code::NotFound => return,
            Err(status) => panic!("GetFlowSummary failed: {status:?}"),
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "retention did not commit the purge within {PURGE_BUDGET:?}:\n{}",
            server.log()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
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

/// LoadBlobs of a purging run names the entry's Flow in the Go Server's NotFound detail.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn load_blobs_of_a_purging_run_names_the_missing_flow() {
    let mut purge = FrozenPurge::start(None).await;
    let status = purge
        .flow_client
        .load_blobs(load_report_request(&purge.report_reference))
        .await
        .expect_err("a purging run's values are not loadable");
    assert_flow_not_found("LoadBlobs", &status);
    purge.finish().await;
}
