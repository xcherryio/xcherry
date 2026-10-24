// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! A standby process with the same node ID takes over after the serving process cleaned its log.

use std::time::Duration;

use dexzd_protocol::dex::flow_service_client::FlowServiceClient;
use dexzd_protocol::dex::{FlowStatus, GetFlowSummaryRequest};
use s3mock_server::SignatureVerification;
use tempfile::TempDir;
use tonic::transport::Channel;

use super::harness::{S3Emulator, ServeConfig, ServeProcess, free_loopback_address};
use super::{
    HELD_LEASE_DURATION, NODE_ID, PRIMARY_BUCKET, PRIMARY_PREFIX, SHUTDOWN_BUDGET,
    run_greeting_flow, start_greeting_worker,
};

/// Snapshots and log deletion run every second, so cleanup soon passes any restored snapshot.
const SNAPSHOT_INTERVAL: &str = "1s";
const LOG_DELETION_DELAY: &str = "1s";
const CLEANUP_BUDGET: Duration = Duration::from_secs(60);
/// Far below the 60 s lease, which a standby that missed the release would wait out.
const TAKEOVER_BUDGET: Duration = Duration::from_secs(15);
const HEALTH_CHECK_BUDGET: Duration = Duration::from_secs(2);
const STANDBY_WAIT_BUDGET: Duration = Duration::from_secs(30);
/// Logged once a lease read finds the lease held by another live process.
const LEASE_WAIT_LOG_LINE: &str = "another process holds the node lease";

/// A waiting standby takes over promptly after SIGTERM and serves every acknowledged Flow.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn waiting_standby_takes_over_without_losing_writes() {
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
            fail_if_held: false,
            parade_db: None,
        }
        .write_with_snapshot_schedule(&path, SNAPSHOT_INTERVAL, LOG_DELETION_DELAY);
        path
    };
    let log_prefix = format!("{PRIMARY_PREFIX}/log/{NODE_ID}/");

    let serving_address = free_loopback_address();
    let mut serving = ServeProcess::spawn(
        &serve_config("serving", serving_address),
        serving_address,
        directory.path().join("serving.log"),
    );
    let channel = serving.wait_until_serving().await;
    let worker = start_greeting_worker(channel.clone()).await;
    let mut writer = AcknowledgedWriter {
        flow_client: FlowServiceClient::new(channel),
        acknowledged: Vec::new(),
    };
    // Log cleanup implies a published snapshot, which a standby could restore while it waits.
    writer
        .write_until(|| first_log_seq(&primary, &log_prefix) > 1)
        .await;

    let standby_address = free_loopback_address();
    let mut standby = ServeProcess::spawn(
        &serve_config("standby", standby_address),
        standby_address,
        directory.path().join("standby.log"),
    );
    standby
        .wait_for_log_line(LEASE_WAIT_LOG_LINE, STANDBY_WAIT_BUDGET)
        .await;
    // Any state the standby read so far is at most this seq; cleanup must pass it.
    let last_seq_before_cleanup = last_log_seq(&primary, &log_prefix);
    assert!(
        !standby.is_serving_within(HEALTH_CHECK_BUDGET).await,
        "the standby serves while another process holds the lease:\n{}",
        standby.log()
    );
    writer
        .write_until(|| first_log_seq(&primary, &log_prefix) > last_seq_before_cleanup + 1)
        .await;
    assert!(
        !standby.is_serving_within(HEALTH_CHECK_BUDGET).await,
        "the standby serves while another process holds the lease:\n{}",
        standby.log()
    );

    let serving_status = serving.terminate(SHUTDOWN_BUDGET).await;
    assert_eq!(serving_status.code(), Some(0), "{}", serving.log());
    let released_at = tokio::time::Instant::now();
    let channel = tokio::time::timeout(TAKEOVER_BUDGET, standby.wait_until_serving())
        .await
        .unwrap_or_else(|_| {
            panic!(
                "the standby did not serve within {TAKEOVER_BUDGET:?} of the release:\n{}",
                standby.log()
            )
        });
    let takeover = released_at.elapsed();
    worker.stop().await;

    let mut flow_client = FlowServiceClient::new(channel);
    for (flow_id, run_id) in &writer.acknowledged {
        let summary = flow_client
            .get_flow_summary(GetFlowSummaryRequest {
                flow_id: flow_id.clone(),
                run_id: String::new(),
            })
            .await
            .unwrap_or_else(|status| {
                panic!(
                    "acknowledged Flow {flow_id} is missing after the takeover: {status}\n{}",
                    standby.log()
                )
            })
            .into_inner();
        assert_eq!(
            summary.flow_status,
            FlowStatus::Completed as i32,
            "{flow_id}"
        );
        assert_eq!(&summary.first_run_id, run_id, "{flow_id}");
    }
    let standby_status = standby.terminate(SHUTDOWN_BUDGET).await;
    assert_eq!(standby_status.code(), Some(0), "{}", standby.log());
    eprintln!(
        "the standby served {takeover:?} after the release with {} acknowledged Flows",
        writer.acknowledged.len()
    );
    primary.shutdown().await;
}

/// Runs greeting Flows one at a time and records each acknowledged Flow ID and run ID.
struct AcknowledgedWriter {
    flow_client: FlowServiceClient<Channel>,
    acknowledged: Vec<(String, String)>,
}

impl AcknowledgedWriter {
    /// Writes until `is_done` holds, at least once, failing after [`CLEANUP_BUDGET`].
    async fn write_until(&mut self, is_done: impl Fn() -> bool) {
        let deadline = tokio::time::Instant::now() + CLEANUP_BUDGET;
        loop {
            let flow_id = format!("standby-flow-{}", self.acknowledged.len());
            let run_id = run_greeting_flow(&mut self.flow_client, &flow_id).await;
            self.acknowledged.push((flow_id, run_id));
            if is_done() {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "log cleanup did not progress within {CLEANUP_BUDGET:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

fn first_log_seq(primary: &S3Emulator, log_prefix: &str) -> u64 {
    log_seqs(primary, log_prefix).first().copied().unwrap_or(0)
}

fn last_log_seq(primary: &S3Emulator, log_prefix: &str) -> u64 {
    log_seqs(primary, log_prefix).last().copied().unwrap_or(0)
}

/// Returns the seqs of the node's incarnation-1 log batches in ascending order.
fn log_seqs(primary: &S3Emulator, log_prefix: &str) -> Vec<u64> {
    primary
        .keys_under(PRIMARY_BUCKET, &format!("{log_prefix}0000000001/"))
        .iter()
        .map(|key| {
            key.rsplit('/')
                .next()
                .and_then(|seq| seq.parse().ok())
                .unwrap_or_else(|| panic!("log key {key} ends with a seq"))
        })
        .collect()
}
