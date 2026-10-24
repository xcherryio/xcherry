// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use dexzd_engine::{
    EngineNode, EngineNodeConfig, EngineNodeDependencies, EngineRuntimeHooks, EngineServices,
    EngineStartError, LocalStoreConfig,
};
use dexzd_log::{DecodedBatch, decode_batch};
use dexzd_object_store::{ObjectStore, ObjectStoreOperation};
use dexzd_storage::{RocksDbLocalStore, RocksDbStoreOptions};

use crate::harness::{CountingApplier, RecordingRuntime, TestCluster, TestNode};

const TAKEOVER_FLOW: &str = "takeover-flow";
const LEASE_KEY: &str = "lease/node-a";
const LOG_PREFIX: &str = "log/node-a/";
/// Longest wait for a standby to read the held lease twice.
const STANDBY_LEASE_READ_DEADLINE: Duration = Duration::from_secs(30);

/// How the serving node stops before the standby takes over.
#[derive(Debug, Clone, Copy)]
enum ServingNodeExit {
    /// Shuts down and releases the lease.
    Shutdown,
    /// Stops without releasing the lease, which then expires.
    Crash,
}

#[tokio::test(start_paused = true)]
async fn a_standby_keeps_every_acknowledged_write_after_a_graceful_shutdown() {
    let mut cluster = TestCluster::new();
    let config = cluster.config(LocalStoreConfig::InMemory);
    take_over_after_log_cleanup(
        &mut cluster,
        config.clone(),
        config,
        ServingNodeExit::Shutdown,
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn a_standby_keeps_every_acknowledged_write_after_a_crash() {
    let mut cluster = TestCluster::new();
    let config = cluster.config(LocalStoreConfig::InMemory);
    take_over_after_log_cleanup(&mut cluster, config.clone(), config, ServingNodeExit::Crash).await;
}

#[tokio::test(start_paused = true)]
async fn a_standby_with_its_own_rocksdb_store_keeps_every_acknowledged_write() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let mut cluster = TestCluster::new();
    let serving_config = cluster.config(LocalStoreConfig::RocksDb {
        directory: directory.path().join("serving"),
        options: RocksDbStoreOptions::default(),
    });
    let standby_config = cluster.config(LocalStoreConfig::RocksDb {
        directory: directory.path().join("standby"),
        options: RocksDbStoreOptions::default(),
    });
    take_over_after_log_cleanup(
        &mut cluster,
        serving_config,
        standby_config,
        ServingNodeExit::Shutdown,
    )
    .await;
}

/// The standby must recover from the newest snapshot after the serving node cleaned its log.
async fn take_over_after_log_cleanup(
    cluster: &mut TestCluster,
    mut serving_config: EngineNodeConfig,
    mut standby_config: EngineNodeConfig,
    exit: ServingNodeExit,
) {
    serving_config.snapshots.deletion_delay = Duration::ZERO;
    standby_config.snapshots.deletion_delay = Duration::ZERO;
    let serving = cluster
        .start_with(&serving_config)
        .await
        .expect("serving node starts");
    let serving_epoch = recorded_epoch(cluster).await;
    serving.commit_prompt(TAKEOVER_FLOW, 3).await;
    serving
        .node
        .take_snapshot()
        .await
        .expect("first snapshot succeeds");
    let first_unsnapshotted_seq = last_seq_of_epoch(cluster, serving_epoch).await + 1;

    let standby = tokio::spawn(start_node(cluster, standby_config));
    wait_until_the_standby_reads_the_held_lease(cluster, &standby, 0).await;
    assert!(!standby.is_finished(), "the standby waits for the lease");

    serving.commit_prompt(TAKEOVER_FLOW, 4).await;
    serving
        .node
        .take_snapshot()
        .await
        .expect("second snapshot succeeds");
    assert!(
        log_batches(cluster).await[0].header.seq > first_unsnapshotted_seq,
        "log cleanup deleted the batches after the first snapshot"
    );
    serving.commit_prompt(TAKEOVER_FLOW, 2).await;
    assert_eq!(serving.committed_count(TAKEOVER_FLOW).await, 9);
    let serving_last_seq = last_seq_of_epoch(cluster, serving_epoch).await;
    match exit {
        ServingNodeExit::Shutdown => serving.node.shutdown().await.expect("lease released"),
        ServingNodeExit::Crash => drop(serving),
    }

    let standby = standby
        .await
        .expect("standby task completes")
        .expect("standby takes over");
    assert_eq!(standby.committed_count(TAKEOVER_FLOW).await, 9, "{exit:?}");
    let standby_epoch = recorded_epoch(cluster).await;
    assert!(standby_epoch > serving_epoch);
    let epoch_start_seq = log_batches(cluster)
        .await
        .iter()
        .find(|batch| batch.header.epoch == standby_epoch && batch.header.epoch_start.is_some())
        .expect("the standby wrote EpochStart")
        .header
        .seq;
    assert!(
        epoch_start_seq > serving_last_seq,
        "EpochStart at {epoch_start_seq} is not after the serving node's last batch \
         {serving_last_seq}"
    );
    let rewritten: Vec<(String, usize)> = cluster
        .faults
        .count_operations_per_key(ObjectStoreOperation::Put, LOG_PREFIX)
        .into_iter()
        .filter(|(_, writes)| *writes > 1)
        .collect();
    assert!(rewritten.is_empty(), "{rewritten:?}");
    standby.commit_prompt(TAKEOVER_FLOW, 1).await;
    assert_eq!(standby.committed_count(TAKEOVER_FLOW).await, 10);
    standby.node.shutdown().await.expect("lease released");
}

#[tokio::test(start_paused = true)]
async fn a_standby_store_inside_a_reclaimed_incarnation_keeps_every_acknowledged_write() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let mut cluster = TestCluster::new();
    let standby_store = rocksdb_store(&directory.path().join("standby"));
    let config = cluster.config(standby_store.clone());
    let earlier = cluster.start_with(&config).await.expect("node starts");
    earlier.commit_prompt(TAKEOVER_FLOW, 3).await;
    earlier.node.shutdown().await.expect("lease released");
    take_over_after_reclaiming_incarnation(&mut cluster, directory.path(), standby_store, 3).await;
}

#[tokio::test(start_paused = true)]
async fn a_standby_store_without_applied_batches_keeps_every_acknowledged_write() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let mut cluster = TestCluster::new();
    let standby_directory = directory.path().join("standby");
    drop(
        RocksDbLocalStore::open(&standby_directory, RocksDbStoreOptions::default())
            .expect("empty store opens"),
    );
    take_over_after_reclaiming_incarnation(
        &mut cluster,
        directory.path(),
        rocksdb_store(&standby_directory),
        0,
    )
    .await;
}

/// The standby's store predates incarnation 1's last writes, which a snapshot of incarnation 2
/// reclaims while the standby waits.
async fn take_over_after_reclaiming_incarnation(
    cluster: &mut TestCluster,
    directory: &Path,
    standby_store: LocalStoreConfig,
    standby_store_count: u64,
) {
    let serving_store = rocksdb_store(&directory.join("serving"));
    let config = cluster.config(serving_store.clone());
    let first_incarnation = cluster.start_with(&config).await.expect("node starts");
    first_incarnation.commit_prompt(TAKEOVER_FLOW, 2).await;
    first_incarnation
        .node
        .shutdown()
        .await
        .expect("lease released");

    let mut serving_config = cluster.config(serving_store);
    serving_config.node.incarnation = 2;
    serving_config.snapshots.deletion_delay = Duration::ZERO;
    let serving = cluster
        .start_with(&serving_config)
        .await
        .expect("serving node starts incarnation 2");
    let mut standby_config = cluster.config(standby_store);
    standby_config.node.incarnation = 2;
    let lease_reads_before_standby = count_lease_reads(cluster);
    let standby = tokio::spawn(start_node(cluster, standby_config));
    wait_until_the_standby_reads_the_held_lease(cluster, &standby, lease_reads_before_standby)
        .await;

    serving
        .node
        .take_snapshot()
        .await
        .expect("snapshot succeeds");
    assert!(
        log_batches(cluster)
            .await
            .iter()
            .all(|batch| batch.header.incarnation == 2),
        "the snapshot reclaimed incarnation 1"
    );
    let segments: Vec<(u32, u64)> = cluster
        .cluster_manifest()
        .await
        .partitions
        .iter()
        .flat_map(|partition| &partition.segments)
        .map(|segment| (segment.incarnation, segment.start_seq))
        .collect();
    assert!(
        segments.iter().all(|segment| *segment == (2, 1)),
        "{segments:?}"
    );
    serving.commit_prompt(TAKEOVER_FLOW, 1).await;
    let acknowledged = standby_store_count + 3;
    assert_eq!(serving.committed_count(TAKEOVER_FLOW).await, acknowledged);
    serving.node.shutdown().await.expect("lease released");

    let standby = standby
        .await
        .expect("standby task completes")
        .expect("standby takes over");
    assert_eq!(standby.committed_count(TAKEOVER_FLOW).await, acknowledged);
    standby.node.shutdown().await.expect("lease released");
}

/// Returns a node start that owns its inputs, so a task can run it while the test acts.
pub(crate) fn start_node(
    cluster: &TestCluster,
    config: EngineNodeConfig,
) -> impl Future<Output = Result<TestNode, EngineStartError>> + Send + 'static {
    let object_store = cluster.object_store.clone();
    let clock = cluster.clock.clone();
    let hasher = cluster.hasher.clone();
    async move {
        let applier = Arc::new(CountingApplier::default());
        let runtime = Arc::new(RecordingRuntime::default());
        let factory_runtime = runtime.clone();
        let node = EngineNode::start(
            &config,
            EngineNodeDependencies {
                object_store,
                clock,
                applier: applier.clone(),
                id_source: Arc::new(dexzd_engine::SystemEngineIdSource),
            },
            move |_services: EngineServices| EngineRuntimeHooks {
                committed_commands: factory_runtime.clone(),
                rebuilder: factory_runtime,
            },
        )
        .await?;
        Ok(TestNode {
            node,
            applier,
            runtime,
            hasher,
        })
    }
}

fn rocksdb_store(directory: &Path) -> LocalStoreConfig {
    LocalStoreConfig::RocksDb {
        directory: directory.to_owned(),
        options: RocksDbStoreOptions::default(),
    }
}

fn count_lease_reads(cluster: &TestCluster) -> usize {
    cluster
        .faults
        .count_operations(ObjectStoreOperation::Get, |key| key == LEASE_KEY)
}

/// Waits for two lease reads after `reads_before`; fails if the standby finishes or the deadline
/// passes.
async fn wait_until_the_standby_reads_the_held_lease<T>(
    cluster: &TestCluster,
    standby: &tokio::task::JoinHandle<T>,
    reads_before: usize,
) {
    let deadline = tokio::time::Instant::now() + STANDBY_LEASE_READ_DEADLINE;
    while count_lease_reads(cluster) < reads_before + 2 {
        assert!(!standby.is_finished(), "the standby waits for the lease");
        assert!(
            tokio::time::Instant::now() < deadline,
            "the standby did not read the held lease twice within {STANDBY_LEASE_READ_DEADLINE:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn recorded_epoch(cluster: &TestCluster) -> u64 {
    cluster.cluster_manifest().await.nodes["node-a"].epoch
}

/// Returns every retained batch of the node in seq order.
async fn log_batches(cluster: &TestCluster) -> Vec<DecodedBatch> {
    let listing = cluster
        .object_store
        .list(LOG_PREFIX, None, 1000)
        .await
        .expect("list log batches");
    assert!(listing.next_start_after.is_none());
    let mut batches = Vec::new();
    for object in listing.objects {
        let content = cluster
            .object_store
            .get(&object.key)
            .await
            .expect("read log batch")
            .expect("listed batch exists");
        batches.push(decode_batch(&content.bytes).expect("decode log batch"));
    }
    batches
}

async fn last_seq_of_epoch(cluster: &TestCluster, epoch: u64) -> u64 {
    log_batches(cluster)
        .await
        .iter()
        .filter(|batch| batch.header.epoch == epoch)
        .map(|batch| batch.header.seq)
        .max()
        .expect("the epoch wrote a batch")
}
