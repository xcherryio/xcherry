// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use dexzd_protocol::dex::flow_service_client::FlowServiceClient;
use dexzd_protocol::dex::worker_task_service_client::WorkerTaskServiceClient;
use dexzd_protocol::dex::{GetFlowSummaryRequest, PollWorkerTaskRequest};
use dexzd_protocol::dexzd::v1::NodeLease;
use dexzd_server::{DevCommandOptions, ServerError, start_dev_server};
use prost::Message;
use s3mock::{
    ChecksumAlgorithm, Durability, EtagMode, PutObjectRequest, Store, StoreConfig,
    UploadChecksumExpectations,
};
use s3mock_server::SignatureVerification;
use tempfile::TempDir;
use tonic::Code;
use tonic::transport::Channel;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

use super::harness::{S3Emulator, ServeConfig, ServeProcess, free_loopback_address};
use super::{PRIMARY_BUCKET, SHUTDOWN_BUDGET};

const LEASE_DURATION: &str = "2s";
const RESPONSE_BUDGET: Duration = Duration::from_secs(1);
const HEALTH_SERVICE_NAMES: [&str; 3] = ["", "dex.FlowService", "dex.WorkerTaskService"];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sigterm_stops_a_standby_waiting_for_a_live_holder() {
    let primary = S3Emulator::start(PRIMARY_BUCKET, SignatureVerification::Strict).await;
    let primary_endpoint = primary.endpoint();
    let directory = TempDir::new().expect("a temporary test directory can be created");
    let first_address = free_loopback_address();
    let first_config = directory.path().join("first.yaml");
    ServeConfig {
        primary_endpoint: &primary_endpoint,
        hard_delete_endpoint: None,
        listen: first_address,
        local_directory: &directory.path().join("first-store"),
        lease_duration: "10s",
        fail_if_held: true,
        parade_db: None,
    }
    .write(&first_config);
    let mut first = ServeProcess::spawn(
        &first_config,
        first_address,
        directory.path().join("first.log"),
    );
    first.wait_until_serving().await;

    let standby_address = free_loopback_address();
    let standby_config = directory.path().join("standby.yaml");
    ServeConfig {
        primary_endpoint: &primary_endpoint,
        hard_delete_endpoint: None,
        listen: standby_address,
        local_directory: &directory.path().join("standby-store"),
        lease_duration: "10s",
        fail_if_held: false,
        parade_db: None,
    }
    .write(&standby_config);
    let mut standby = ServeProcess::spawn(
        &standby_config,
        standby_address,
        directory.path().join("standby.log"),
    );
    standby
        .wait_for_log_line(
            "another process holds the node lease; waiting",
            Duration::from_secs(10),
        )
        .await;
    let status = standby.terminate(Duration::from_secs(3)).await;
    assert_eq!(status.code(), Some(0), "{}", standby.log());
    assert!(
        first.is_serving_within(RESPONSE_BUDGET).await,
        "lease holder stopped serving:\n{}",
        first.log()
    );
    assert_eq!(
        first.terminate(SHUTDOWN_BUDGET).await.code(),
        Some(0),
        "{}",
        first.log()
    );
    primary.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_dev_start_releases_the_grpc_listener() {
    let directory = TempDir::new().expect("a temporary test directory can be created");
    let store = Store::open(&StoreConfig {
        root_directory: directory.path().join("s3"),
        durability: Durability::Relaxed,
        etag_mode: EtagMode::Md5,
    })
    .expect("the embedded object store opens");
    store
        .create_bucket("dex-dev", false)
        .expect("the dev bucket is created");
    let now_micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after the Unix epoch")
        .as_micros();
    let expires_at_unix_micros = i64::try_from(now_micros).expect("time fits i64") + 60_000_000;
    let lease = NodeLease {
        node_id: "dev".to_owned(),
        incarnation: 1,
        epoch: 1,
        holder_process_id: "held-by-another-process".to_owned(),
        expires_at_unix_micros,
        duration_micros: 60_000_000,
        version: 1,
    };
    let mut writer = store
        .begin_put("dex-dev", "lease/dev", ChecksumAlgorithm::Crc32)
        .expect("the lease upload starts");
    writer
        .write_all(&lease.encode_to_vec())
        .expect("the lease is written");
    let staged = writer
        .finish(&UploadChecksumExpectations::default())
        .expect("the lease upload finishes");
    store
        .commit_put("dex-dev", "lease/dev", staged, PutObjectRequest::default())
        .expect("the lease is stored");
    drop(store);

    let waiting_options = DevCommandOptions {
        data_dir: directory.path().to_path_buf(),
        listen: free_loopback_address(),
        log_dir: None,
        config: None,
    };
    let waiting_address = waiting_options.listen;
    let waiting = tokio::spawn(async move { start_dev_server(&waiting_options).await });
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(Ok(stream)) = tokio::time::timeout(
                RESPONSE_BUDGET,
                tokio::net::TcpStream::connect(waiting_address),
            )
            .await
            {
                drop(stream);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the waiting dev server opens its early listener");
    assert!(
        !waiting.is_finished(),
        "the second dev server must wait for the lease"
    );
    waiting.abort();
    let cancelled = match waiting.await {
        Ok(_) => panic!("the waiting start completed"),
        Err(error) => error,
    };
    assert!(cancelled.is_cancelled());
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match Store::open(&StoreConfig {
                root_directory: directory.path().join("s3"),
                durability: Durability::Relaxed,
                etag_mode: EtagMode::Md5,
            }) {
                Ok(store) => {
                    drop(store);
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await
    .expect("the cancelled dev server releases its embedded object store");

    let retry_options = DevCommandOptions {
        data_dir: directory.path().to_path_buf(),
        listen: waiting_address,
        log_dir: None,
        config: None,
    };
    let mut retry = tokio::spawn(async move { start_dev_server(&retry_options).await });
    tokio::select! {
        result = &mut retry => {
            match result.expect("the retry task completes normally") {
                Err(ServerError::Listen { source, .. }) => panic!("retry could not bind: {source}"),
                Err(error) => panic!("retry failed during startup: {error}"),
                Ok(server) => panic!("retry started despite held lease on {}", server.local_address()),
            }
        }
        () = tokio::time::sleep(Duration::from_millis(500)) => {}
    }
    retry.abort();
    let cancelled = match retry.await {
        Ok(_) => panic!("the retry start completed"),
        Err(error) => error,
    };
    assert!(cancelled.is_cancelled());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn waiting_standby_reports_not_serving_then_serves_after_takeover() {
    let primary = S3Emulator::start(PRIMARY_BUCKET, SignatureVerification::Strict).await;
    let primary_endpoint = primary.endpoint();
    let directory = TempDir::new().expect("a temporary test directory can be created");
    let first_address = free_loopback_address();
    let first_config = directory.path().join("first.yaml");
    ServeConfig {
        primary_endpoint: &primary_endpoint,
        hard_delete_endpoint: None,
        listen: first_address,
        local_directory: &directory.path().join("first-store"),
        lease_duration: LEASE_DURATION,
        fail_if_held: true,
        parade_db: None,
    }
    .write(&first_config);
    let mut first = ServeProcess::spawn(
        &first_config,
        first_address,
        directory.path().join("first.log"),
    );
    first.wait_until_serving().await;

    let second_address = free_loopback_address();
    let second_config = directory.path().join("second.yaml");
    ServeConfig {
        primary_endpoint: &primary_endpoint,
        hard_delete_endpoint: None,
        listen: second_address,
        local_directory: &directory.path().join("second-store"),
        lease_duration: LEASE_DURATION,
        fail_if_held: false,
        parade_db: None,
    }
    .write(&second_config);
    let mut second = ServeProcess::spawn(
        &second_config,
        second_address,
        directory.path().join("second.log"),
    );
    let channel = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(channel) = Channel::from_shared(format!("http://{second_address}"))
                .expect("the standby address is a valid URI")
                .connect()
                .await
            {
                break channel;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("the standby did not accept connections:\n{}", second.log()));

    let mut watches = Vec::new();
    for service in HEALTH_SERVICE_NAMES {
        let request = HealthCheckRequest {
            service: service.to_owned(),
        };
        let mut health = HealthClient::new(channel.clone());
        let check = tokio::time::timeout(RESPONSE_BUDGET, health.check(request.clone()))
            .await
            .expect("the standby health Check responds within one second")
            .expect("the standby health Check succeeds");
        assert_eq!(check.get_ref().status, ServingStatus::NotServing as i32);
        let mut watch = tokio::time::timeout(RESPONSE_BUDGET, health.watch(request))
            .await
            .expect("the standby health Watch responds within one second")
            .expect("the standby health Watch succeeds")
            .into_inner();
        let initial = tokio::time::timeout(RESPONSE_BUDGET, watch.message())
            .await
            .expect("the standby health Watch sends within one second")
            .expect("the standby health Watch remains open")
            .expect("the standby health Watch sends a status");
        assert_eq!(initial.status, ServingStatus::NotServing as i32);
        watches.push(watch);
    }
    let flow_error = tokio::time::timeout(
        RESPONSE_BUDGET,
        FlowServiceClient::new(channel.clone()).get_flow_summary(GetFlowSummaryRequest {
            flow_id: "standby-check".to_owned(),
            run_id: String::new(),
        }),
    )
    .await
    .expect("the standby FlowService responds within one second")
    .expect_err("the standby FlowService rejects requests");
    assert_eq!(flow_error.code(), Code::Unavailable);
    assert!(flow_error.message().contains("node is starting"));
    let worker_error = tokio::time::timeout(
        RESPONSE_BUDGET,
        WorkerTaskServiceClient::new(channel.clone())
            .poll_worker_task(tokio_stream::empty::<PollWorkerTaskRequest>()),
    )
    .await
    .expect("the standby WorkerTaskService responds within one second")
    .expect_err("the standby WorkerTaskService rejects requests");
    assert_eq!(worker_error.code(), Code::Unavailable);
    assert!(worker_error.message().contains("node is starting"));

    let first_status = first.terminate(SHUTDOWN_BUDGET).await;
    assert_eq!(first_status.code(), Some(0), "{}", first.log());
    let serving_channel = second.wait_until_serving().await;
    for mut watch in watches {
        let serving = tokio::time::timeout(RESPONSE_BUDGET, watch.message())
            .await
            .expect("the health Watch updates within one second")
            .expect("the health Watch remains open")
            .expect("the health Watch sends a status");
        assert_eq!(serving.status, ServingStatus::Serving as i32);
    }
    FlowServiceClient::new(serving_channel)
        .search_flows(dexzd_protocol::dex::SearchFlowsRequest {
            query: "FlowType = 'StandbyHealthFlow'".to_owned(),
            page_size: 10,
            ..Default::default()
        })
        .await
        .expect("the active FlowService serves requests");
    let second_status = second.terminate(SHUTDOWN_BUDGET).await;
    assert_eq!(second_status.code(), Some(0), "{}", second.log());
    primary.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_startup_closes_the_health_listener() {
    let primary = S3Emulator::start(PRIMARY_BUCKET, SignatureVerification::Strict).await;
    let primary_endpoint = primary.endpoint();
    let directory = TempDir::new().expect("a temporary test directory can be created");
    let first_address = free_loopback_address();
    let first_config = directory.path().join("first.yaml");
    ServeConfig {
        primary_endpoint: &primary_endpoint,
        hard_delete_endpoint: None,
        listen: first_address,
        local_directory: &directory.path().join("first-store"),
        lease_duration: "10s",
        fail_if_held: true,
        parade_db: None,
    }
    .write(&first_config);
    let mut first = ServeProcess::spawn(
        &first_config,
        first_address,
        directory.path().join("first.log"),
    );
    first.wait_until_serving().await;

    let second_address = free_loopback_address();
    let second_config = directory.path().join("second.yaml");
    ServeConfig {
        primary_endpoint: &primary_endpoint,
        hard_delete_endpoint: None,
        listen: second_address,
        local_directory: &directory.path().join("second-store"),
        lease_duration: "10s",
        fail_if_held: true,
        parade_db: None,
    }
    .write(&second_config);
    let mut second = ServeProcess::spawn(
        &second_config,
        second_address,
        directory.path().join("second.log"),
    );
    let status = second.wait_for_exit(Duration::from_secs(10)).await;
    assert_eq!(status.code(), Some(1), "{}", second.log());
    assert!(second.log().contains("lease is held"), "{}", second.log());
    assert!(
        tokio::net::TcpStream::connect(second_address)
            .await
            .is_err(),
        "failed startup left the health listener open"
    );
    assert_eq!(
        first.terminate(SHUTDOWN_BUDGET).await.code(),
        Some(0),
        "{}",
        first.log()
    );
    primary.shutdown().await;
}
