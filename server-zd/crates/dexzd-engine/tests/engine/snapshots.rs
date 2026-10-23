// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;
use std::time::Duration;

use dexzd_engine::LocalStoreConfig;
use dexzd_object_store::ObjectStore;
use dexzd_protocol::dexzd::v1::SnapshotManifest;
use prost::Message;

use crate::harness::{CountingApplier, PARTITION_COUNT, RecordingRuntime, TestCluster};

async fn snapshot_manifest_keys(cluster: &TestCluster) -> Vec<String> {
    cluster
        .object_store
        .list("snapshot/", None, 1000)
        .await
        .expect("list snapshots")
        .objects
        .into_iter()
        .map(|object| object.key)
        .filter(|key| key.contains("/manifest/"))
        .collect()
}

async fn log_batch_keys(cluster: &TestCluster) -> Vec<String> {
    cluster
        .object_store
        .list("log/", None, 1000)
        .await
        .expect("list log")
        .objects
        .into_iter()
        .map(|object| object.key)
        .collect()
}

#[tokio::test(start_paused = true)]
async fn snapshot_covers_every_partition_and_is_recorded_in_the_manifest() {
    let mut cluster = TestCluster::new();
    let test = cluster.start_in_memory().await;
    test.commit_prompt("snapshot-flow", 2).await;
    let reference = test.node.take_snapshot().await.expect("snapshot succeeds");
    let content = cluster
        .object_store
        .get(&reference.manifest_key)
        .await
        .expect("read manifest")
        .expect("manifest exists");
    let manifest = SnapshotManifest::decode(content.bytes).expect("decode manifest");
    assert_eq!(manifest.partition_positions.len(), PARTITION_COUNT as usize);
    let cluster_manifest = dexzd_cluster_metadata::read_manifest(cluster.object_store.as_ref())
        .await
        .expect("read cluster manifest")
        .expect("cluster manifest exists");
    assert!(
        cluster_manifest
            .partitions
            .iter()
            .all(|partition| partition.snapshot.as_ref() == Some(&reference))
    );
    test.node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn snapshots_start_after_the_log_threshold_or_the_interval() {
    let mut cluster = TestCluster::new();
    let mut config = cluster.config(LocalStoreConfig::InMemory);
    config.snapshots.log_threshold_bytes = 1;
    let test = cluster
        .start(
            &config,
            Arc::new(CountingApplier::default()),
            Arc::new(RecordingRuntime::default()),
        )
        .await
        .expect("node starts");
    assert!(snapshot_manifest_keys(&cluster).await.is_empty());
    test.commit_prompt("threshold-flow", 1).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(snapshot_manifest_keys(&cluster).await.len(), 1);
    test.node.shutdown().await;

    let mut cluster = TestCluster::new();
    let test = cluster.start_in_memory().await;
    tokio::time::sleep(Duration::from_secs(9 * 60)).await;
    assert!(snapshot_manifest_keys(&cluster).await.is_empty());
    tokio::time::sleep(Duration::from_secs(61)).await;
    assert_eq!(snapshot_manifest_keys(&cluster).await.len(), 1);
    test.node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn restart_restores_the_snapshot_and_replays_only_later_batches() {
    let mut cluster = TestCluster::new();
    let first = cluster.start_in_memory().await;
    first.commit_prompt("restore-flow", 3).await;
    first.node.take_snapshot().await.expect("snapshot succeeds");
    first.commit_prompt("restore-flow", 2).await;
    drop(first);

    let second = cluster.start_in_memory().await;
    assert_eq!(second.applier.applied_commands().len(), 2);
    assert_eq!(second.committed_count("restore-flow").await, 5);
    second.node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn log_batches_are_deleted_only_after_a_snapshot_is_old_enough() {
    let mut cluster = TestCluster::new();
    let test = cluster.start_in_memory().await;
    test.commit_prompt("cleanup-flow", 3).await;
    let batches_before = log_batch_keys(&cluster).await;
    test.node.take_snapshot().await.expect("first snapshot");
    assert_eq!(log_batch_keys(&cluster).await, batches_before);

    cluster.clock.shift(25 * 60 * 60 * 1_000_000);
    test.commit_prompt("cleanup-flow", 1).await;
    test.node.take_snapshot().await.expect("second snapshot");
    let batches_after = log_batch_keys(&cluster).await;
    let first_retained = batches_before.last().expect("batches exist");
    assert_eq!(batches_after.first(), Some(first_retained));
    let manifest = dexzd_cluster_metadata::read_manifest(cluster.object_store.as_ref())
        .await
        .expect("read manifest")
        .expect("manifest exists");
    let first_retained_seq: u64 = first_retained
        .rsplit('/')
        .next()
        .expect("seq segment")
        .parse()
        .expect("numeric seq");
    assert!(
        manifest
            .partitions
            .iter()
            .flat_map(|partition| &partition.segments)
            .all(|segment| segment.start_seq == first_retained_seq)
    );
    test.commit_prompt("cleanup-flow", 1).await;
    drop(test);

    let restarted = cluster.start_in_memory().await;
    assert_eq!(restarted.committed_count("cleanup-flow").await, 5);
    assert_eq!(restarted.applier.applied_commands().len(), 1);
    restarted.node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_failed_snapshot_keeps_serving_and_succeeds_later() {
    let mut cluster = TestCluster::new();
    let test = cluster.start_in_memory().await;
    test.commit_prompt("retry-flow", 1).await;
    cluster.faults.fail_puts_under("snapshot/");
    test.node
        .take_snapshot()
        .await
        .expect_err("snapshot uploads fail");
    test.commit_prompt("retry-flow", 1).await;
    assert_eq!(test.node.stop_reason(), None);
    cluster.faults.fail_puts_under("never-matches/");
    test.node.take_snapshot().await.expect("snapshot succeeds");
    assert_eq!(snapshot_manifest_keys(&cluster).await.len(), 1);
    test.node.shutdown().await;
}
