// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;
use std::time::Duration;

use dexzd_engine::{
    EngineNode, EngineNodeConfig, EngineNodeDependencies, EngineRuntimeHooks, EngineServices,
    EngineStartError, LocalStoreConfig,
};
use dexzd_log::{DecodedBatch, decode_batch};
use dexzd_object_store::{ObjectStore, ObjectStoreOperation};
use dexzd_storage::RocksDbStoreOptions;
use tokio::task::JoinHandle;

use crate::harness::{CountingApplier, RecordingRuntime, TestCluster, TestNode};

const TAKEOVER_FLOW: &str = "takeover-flow";
const LEASE_KEY: &str = "lease/node-a";
const LOG_PREFIX: &str = "log/node-a/";

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

/// A standby waits on the lease while the serving node snapshots, deletes the covered log, and
/// stops. The standby must recover from the newest snapshot, not from what it read before.
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

    let standby = spawn_node(cluster, standby_config);
    while cluster
        .faults
        .count_operations(ObjectStoreOperation::Get, |key| key == LEASE_KEY)
        < 2
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
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
        ServingNodeExit::Shutdown => serving.node.shutdown().await,
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
    standby.node.shutdown().await;
}

/// Starts a node in a task, so the test can act while it waits for the lease.
fn spawn_node(
    cluster: &TestCluster,
    config: EngineNodeConfig,
) -> JoinHandle<Result<TestNode, EngineStartError>> {
    let object_store = cluster.object_store.clone();
    let clock = cluster.clock.clone();
    let hasher = cluster.hasher.clone();
    tokio::spawn(async move {
        let applier = Arc::new(CountingApplier::default());
        let runtime = Arc::new(RecordingRuntime::default());
        let factory_runtime = runtime.clone();
        let node = EngineNode::start(
            &config,
            EngineNodeDependencies {
                object_store,
                clock,
                applier: applier.clone(),
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
    })
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
