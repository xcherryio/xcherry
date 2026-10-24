// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Crash, local disk loss, and frozen-owner tests that check every acknowledged write survives.

mod inspection;
mod processes;
mod workload;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tempfile::TempDir;
use tokio::time::Instant;

use crate::harness::free_loopback_address;
use inspection::{ObjectStoreInspector, first_log_batch_key};
use processes::{
    DurableEmulator, PROCESS_SHUTDOWN_BUDGET, RobustnessProcess, RobustnessServeConfig, ServeFleet,
    SupervisedNode, replay_field,
};
use workload::Workload;

/// A restart waits up to this plus a 1 s grace for the killed process's lease.
const LEASE_DURATION: &str = "3s";
/// Survives multi-second host stalls; takeover timing depends only on the owner's lease.
const STANDBY_LEASE_DURATION: &str = "15s";
const CALL_TIMEOUT: Duration = Duration::from_secs(3);
const CHAIN_STARTERS: usize = 4;
const COUNTER_COUNT: usize = 3;
/// New acknowledged calls between failures, so each failure lands mid-load.
const ACKNOWLEDGEMENTS_PER_PHASE: u64 = 60;
const KILL_COUNT: usize = 3;
const SNAPSHOT_BUDGET: Duration = Duration::from_secs(30);
const OBJECT_POLL_INTERVAL: Duration = Duration::from_millis(100);
/// Longer than the freeze, so a call in flight at the freeze sees the resumed owner's reply.
const FROZEN_OWNER_CALL_TIMEOUT: Duration = Duration::from_secs(20);
/// Adds time for the resumed owner to detect the takeover before it shuts down.
const FENCED_OWNER_EXIT_BUDGET: Duration =
    PROCESS_SHUTDOWN_BUDGET.saturating_add(Duration::from_secs(5));

/// SIGKILL under load, restarting on the same directory, loses no acknowledged write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill9_under_load_loses_no_acknowledged_write() {
    let emulator = DurableEmulator::start().await;
    let primary_endpoint = emulator.endpoint();
    let directory = TempDir::new().expect("a temporary test directory can be created");
    let workload = Workload::new();
    let mut fleet = ServeFleet::new(directory.path(), workload.summary_source());
    let address = free_loopback_address();
    let config = directory.path().join("serve.yaml");
    RobustnessServeConfig {
        primary_endpoint: &primary_endpoint,
        listen: address,
        local_directory: &directory.path().join("store"),
        lease_duration: LEASE_DURATION,
        snapshot_interval: "10m",
        log_deletion_delay: "24h",
    }
    .write(&config);

    let mut node =
        SupervisedNode::start(&mut fleet, &workload, &config, address, "serve", false).await;
    workload.start_counters(node.channel(), COUNTER_COUNT).await;
    let load = workload.start_load(address, "kill", CHAIN_STARTERS, CALL_TIMEOUT);
    for _ in 0..KILL_COUNT {
        node.wait_for_new_acknowledgements(&mut fleet, &workload, ACKNOWLEDGEMENTS_PER_PHASE)
            .await;
        node.kill_and_restart(&mut fleet, &workload).await;
    }
    node.wait_for_new_acknowledgements(&mut fleet, &workload, ACKNOWLEDGEMENTS_PER_PHASE)
        .await;
    load.stop().await;

    let report = node.verify(&workload).await;
    eprintln!(
        "kill9_under_load_loses_no_acknowledged_write: {} stale-batch restarts; {report}",
        node.stale_batch_restart_count()
    );
    node.terminate().await;
    emulator.shutdown().await;
}

/// A new process on an empty directory restores the newest snapshot and replays the log tail.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_local_directory_restores_from_snapshot_and_log_tail() {
    let emulator = DurableEmulator::start().await;
    let primary_endpoint = emulator.endpoint();
    let inspector = ObjectStoreInspector::connect(&primary_endpoint).await;
    let directory = TempDir::new().expect("a temporary test directory can be created");
    let workload = Workload::new();
    let mut fleet = ServeFleet::new(directory.path(), workload.summary_source());
    let address = free_loopback_address();
    let local_directory = directory.path().join("store");
    let config = |name: &str, snapshot_interval: &str| {
        let path = directory.path().join(format!("{name}.yaml"));
        RobustnessServeConfig {
            primary_endpoint: &primary_endpoint,
            listen: address,
            local_directory: &local_directory,
            lease_duration: LEASE_DURATION,
            snapshot_interval,
            log_deletion_delay: "1s",
        }
        .write(&path);
        path
    };
    let snapshotting_config = config("snapshotting", "1s");
    // No snapshot covers this process's writes, so only the log tail holds them.
    let log_tail_config = config("log-tail", "1h");

    let mut process = fleet.spawn(&snapshotting_config, address, "snapshotting");
    let channel = process.wait_until_serving().await;
    let worker = workload.start_worker(channel.clone()).await;
    workload.start_counters(channel, COUNTER_COUNT).await;
    let load = workload.start_load(address, "snapshotting", CHAIN_STARTERS, CALL_TIMEOUT);
    wait_for_snapshots_and_log_cleanup(&inspector, &mut process).await;
    load.stop().await;
    let status = process.terminate(PROCESS_SHUTDOWN_BUDGET).await;
    assert_eq!(status.code(), Some(0), "{}", process.log());
    worker.stop().await;
    let manifest_keys = inspector.snapshot_manifest_keys().await;

    let mut process = fleet.spawn(&log_tail_config, address, "log-tail");
    let channel = process.wait_until_serving().await;
    let worker = workload.start_worker(channel).await;
    let load = workload.start_load(address, "log-tail", CHAIN_STARTERS, CALL_TIMEOUT);
    workload
        .wait_for_new_acknowledgements(ACKNOWLEDGEMENTS_PER_PHASE, &mut process)
        .await;
    process.kill().await;
    worker.stop().await;
    load.stop().await;
    assert_eq!(
        inspector.snapshot_manifest_keys().await,
        manifest_keys,
        "the log-tail process must not snapshot:\n{}",
        process.log()
    );

    std::fs::remove_dir_all(&local_directory).expect("the local directory is deleted");
    let mut node = SupervisedNode::start(
        &mut fleet,
        &workload,
        &log_tail_config,
        address,
        "restored",
        true,
    )
    .await;
    assert!(
        !inspector.has_object(&first_log_batch_key()).await,
        "the first log batch must stay deleted, so only a snapshot can restore its state"
    );
    let restore_log = std::fs::read_to_string(directory.path().join(node.incarnation_log_name(1)))
        .expect("the restoring process log is readable");
    let last_applied_seq = replay_field(&restore_log, "last_applied_seq");
    assert!(
        last_applied_seq > 0,
        "the restore must start from a snapshot:\n{restore_log}"
    );
    // Repeated counter values would expose a write the restore lost.
    let load = workload.start_load(address, "restored", CHAIN_STARTERS, CALL_TIMEOUT);
    node.wait_for_new_acknowledgements(&mut fleet, &workload, ACKNOWLEDGEMENTS_PER_PHASE)
        .await;
    load.stop().await;

    let report = node.verify(&workload).await;
    eprintln!(
        "lost_local_directory_restores_from_snapshot_and_log_tail: restored at seq {last_applied_seq}; {} stale-batch restarts; {report}",
        node.stale_batch_restart_count()
    );
    node.terminate().await;
    emulator.shutdown().await;
}

/// An owner frozen past its lease stops on resume without writing to the new owner's log.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn frozen_owner_is_fenced() {
    let emulator = DurableEmulator::start().await;
    let primary_endpoint = emulator.endpoint();
    let inspector = ObjectStoreInspector::connect(&primary_endpoint).await;
    let directory = TempDir::new().expect("a temporary test directory can be created");
    let workload = Workload::new();
    let mut fleet = ServeFleet::new(directory.path(), workload.summary_source());
    // A short log deletion delay would let the standby lose writes, a separate known defect.
    let config = |name: &str, address, lease_duration| {
        let path = directory.path().join(format!("{name}.yaml"));
        RobustnessServeConfig {
            primary_endpoint: &primary_endpoint,
            listen: address,
            local_directory: &directory.path().join(format!("{name}-store")),
            lease_duration,
            snapshot_interval: "1h",
            log_deletion_delay: "24h",
        }
        .write(&path);
        path
    };
    let owner_address = free_loopback_address();
    let standby_address = free_loopback_address();
    let owner_config = config("owner", owner_address, LEASE_DURATION);
    let standby_config = config("standby", standby_address, STANDBY_LEASE_DURATION);

    let mut owner = fleet.spawn(&owner_config, owner_address, "owner");
    let owner_channel = owner.wait_until_serving().await;
    let owner_worker = workload.start_worker(owner_channel.clone()).await;
    workload.start_counters(owner_channel, COUNTER_COUNT).await;
    let load = workload.start_load(
        owner_address,
        "owner",
        CHAIN_STARTERS,
        FROZEN_OWNER_CALL_TIMEOUT,
    );
    workload
        .wait_for_new_acknowledgements(ACKNOWLEDGEMENTS_PER_PHASE, &mut owner)
        .await;
    let owner_epoch = inspector.node_lease().await.epoch;
    let mut standby = fleet.spawn(&standby_config, standby_address, "standby");
    workload
        .wait_for_new_acknowledgements(ACKNOWLEDGEMENTS_PER_PHASE, &mut owner)
        .await;
    assert!(
        standby.exit_status().is_none(),
        "the standby waits for the lease:\n{}",
        standby.log()
    );

    owner.send_signal("STOP").await;
    // The kill command has exited, so the owner seals nothing after this instant until SIGCONT.
    let frozen_at_unix_micros = unix_micros_now();
    let standby_channel = standby.wait_until_serving().await;
    // Steps re-dispatched after the takeover fail and back off while no Worker is connected.
    let standby_worker = workload.start_worker(standby_channel.clone()).await;
    let standby_lease = inspector.node_lease().await;
    assert!(
        standby_lease.epoch > owner_epoch,
        "{standby_lease:?} after owner epoch {owner_epoch}"
    );
    let headers = inspector.log_batch_headers().await;
    let epoch_start_seq = headers
        .iter()
        .find(|header| {
            header
                .epoch_start
                .as_ref()
                .is_some_and(|epoch_start| epoch_start.epoch == standby_lease.epoch)
        })
        .unwrap_or_else(|| panic!("the standby wrote no EpochStart: {headers:?}"))
        .seq;
    let acknowledged_before_resume = workload.acknowledged_count();

    owner.send_signal("CONT").await;
    let owner_status = owner.wait_for_exit(FENCED_OWNER_EXIT_BUDGET).await;
    assert_eq!(owner_status.code(), Some(1), "{}", owner.log());
    load.stop().await;
    owner_worker.stop().await;
    if let Some(status) = standby.exit_status() {
        panic!(
            "the standby exited with {status} while the owner resumed:\n{}",
            standby.log()
        );
    }
    let lease = inspector.node_lease().await;
    assert_eq!(
        (lease.holder_process_id.as_str(), lease.epoch),
        (
            standby_lease.holder_process_id.as_str(),
            standby_lease.epoch
        ),
        "the resumed owner must not take or release the standby's lease"
    );
    assert_ne!(
        lease.expires_at_unix_micros,
        i64::MIN,
        "the standby's lease must not be released"
    );
    let mut late_owner_batch_count = 0;
    for header in inspector.log_batch_headers().await {
        if header.epoch == owner_epoch {
            assert!(
                header.sealed_at_unix_micros <= frozen_at_unix_micros,
                "the resumed owner sealed seq {} at {} after the freeze at {frozen_at_unix_micros}",
                header.seq,
                header.sealed_at_unix_micros
            );
        }
        if header.seq < epoch_start_seq {
            continue;
        }
        // A PUT sent before the freeze may land late; replay voids it or the standby stops.
        if header.epoch == owner_epoch {
            late_owner_batch_count += 1;
        } else {
            assert_eq!(
                header.epoch, standby_lease.epoch,
                "seq {} was written by epoch {} after the standby's EpochStart at {epoch_start_seq}",
                header.seq, header.epoch
            );
        }
    }

    let report = workload.verify(standby_channel, &mut standby).await;
    eprintln!(
        "frozen_owner_is_fenced: EpochStart at seq {epoch_start_seq}; {late_owner_batch_count} owner batches sealed before the freeze landed after it; {} calls acknowledged after the resume; {report}",
        workload.acknowledged_count() - acknowledged_before_resume
    );
    let standby_status = standby.terminate(PROCESS_SHUTDOWN_BUDGET).await;
    assert_eq!(standby_status.code(), Some(0), "{}", standby.log());
    standby_worker.stop().await;
    emulator.shutdown().await;
}

/// Returns the wall clock in the unit and epoch of `LogBatchHeader.sealed_at_unix_micros`.
fn unix_micros_now() -> i64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the wall clock is after the Unix epoch");
    i64::try_from(elapsed.as_micros()).expect("the wall clock fits in i64 microseconds")
}

/// Polls until two snapshots exist and log cleanup has deleted the first log batch.
async fn wait_for_snapshots_and_log_cleanup(
    inspector: &ObjectStoreInspector,
    process: &mut RobustnessProcess,
) {
    let deadline = Instant::now() + SNAPSHOT_BUDGET;
    loop {
        let manifest_keys = inspector.snapshot_manifest_keys().await;
        let has_first_batch = inspector.has_object(&first_log_batch_key()).await;
        if manifest_keys.len() >= 2 && !has_first_batch {
            return;
        }
        if let Some(status) = process.exit_status() {
            panic!("dex-server-zd exited with {status}:\n{}", process.log());
        }
        assert!(
            Instant::now() < deadline,
            "within {SNAPSHOT_BUDGET:?} expected 2 snapshot manifests and no first log batch, got {manifest_keys:?} and first batch present {has_first_batch}:\n{}",
            process.log()
        );
        tokio::time::sleep(OBJECT_POLL_INTERVAL).await;
    }
}
