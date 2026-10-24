// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use dexzd_engine::LocalStoreConfig;
use dexzd_object_store::{ObjectStore, ObjectStoreOperation, WriteCondition};
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

    // Nothing committed since that snapshot, so later intervals write nothing.
    let snapshot_writes = || {
        cluster
            .faults
            .count_operations(ObjectStoreOperation::Put, |key| {
                key.starts_with("snapshot/") || key == "manifest"
            })
    };
    let writes_after_first_snapshot = snapshot_writes();
    tokio::time::sleep(Duration::from_secs(35 * 60)).await;
    assert_eq!(snapshot_writes(), writes_after_first_snapshot);
    assert_eq!(snapshot_manifest_keys(&cluster).await.len(), 1);
    test.commit_prompt("idle-flow", 1).await;
    tokio::time::sleep(Duration::from_secs(10 * 60 + 2)).await;
    assert_eq!(snapshot_manifest_keys(&cluster).await.len(), 2);
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

async fn snapshot_object_keys(cluster: &TestCluster) -> Vec<String> {
    cluster
        .object_store
        .list("snapshot/", None, 1000)
        .await
        .expect("list snapshots")
        .objects
        .into_iter()
        .map(|object| object.key)
        .collect()
}

fn lineage_of(snapshot_key: &str) -> String {
    snapshot_key
        .split('/')
        .nth(2)
        .expect("snapshot keys name a lineage")
        .to_owned()
}

#[tokio::test(start_paused = true)]
async fn retention_reclaims_the_lineage_that_a_restore_replaced() {
    let mut cluster = TestCluster::new();
    let mut config = cluster.config(LocalStoreConfig::InMemory);
    config.snapshots.deletion_delay = Duration::ZERO;
    let retained_count = config.snapshots.retained_count;
    let first = cluster.start_with(&config).await.expect("node starts");
    for _ in 0..2 {
        first.commit_prompt("lineage-flow", 1).await;
        first.node.take_snapshot().await.expect("snapshot succeeds");
    }
    let replaced_lineage = lineage_of(&snapshot_manifest_keys(&cluster).await[0]);
    drop(first);

    let second = cluster
        .start_with(&config)
        .await
        .expect("node restores the snapshot");
    for _ in 0..=retained_count {
        second.commit_prompt("lineage-flow", 1).await;
        second
            .node
            .take_snapshot()
            .await
            .expect("snapshot succeeds");
    }
    let manifest_keys = snapshot_manifest_keys(&cluster).await;
    assert_eq!(manifest_keys.len(), retained_count, "{manifest_keys:?}");
    let objects = snapshot_object_keys(&cluster).await;
    assert!(
        objects
            .iter()
            .all(|key| lineage_of(key) != replaced_lineage),
        "{objects:?}"
    );
    for key in &manifest_keys {
        let content = cluster
            .object_store
            .get(key)
            .await
            .expect("read manifest")
            .expect("manifest exists");
        let manifest = SnapshotManifest::decode(content.bytes).expect("decode manifest");
        assert!(
            manifest
                .files
                .iter()
                .all(|file| objects.contains(&file.object_key))
        );
    }
    drop(second);

    let restarted = cluster
        .start_with(&config)
        .await
        .expect("node restores the newest snapshot");
    assert_eq!(
        restarted.committed_count("lineage-flow").await,
        3 + retained_count as u64
    );
    restarted.node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_snapshot_reads_each_stored_snapshot_manifest_at_most_once() {
    let mut cluster = TestCluster::new();
    let test = cluster.start_in_memory().await;
    for _ in 0..4 {
        test.commit_prompt("catalog-flow", 1).await;
        test.node.take_snapshot().await.expect("snapshot succeeds");
    }
    let manifest_reads = || {
        cluster
            .faults
            .count_operations(ObjectStoreOperation::Get, |key| key.contains("/manifest/"))
    };
    let reads_before = manifest_reads();
    test.commit_prompt("catalog-flow", 1).await;
    test.node.take_snapshot().await.expect("snapshot succeeds");
    assert_eq!(manifest_reads(), reads_before);
    assert_eq!(snapshot_manifest_keys(&cluster).await.len(), 5);
    test.node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn retention_deletes_only_old_snapshot_files_without_a_manifest() {
    let mut cluster = TestCluster::new();
    let mut config = cluster.config(LocalStoreConfig::InMemory);
    config.snapshots.deletion_delay = Duration::from_secs(60 * 60);
    let test = cluster.start_with(&config).await.expect("node starts");
    test.commit_prompt("orphan-flow", 1).await;
    let first = test.node.take_snapshot().await.expect("first snapshot");
    let first_content = cluster
        .object_store
        .get(&first.manifest_key)
        .await
        .expect("read first manifest")
        .expect("first manifest exists");
    let first_manifest = SnapshotManifest::decode(first_content.bytes).expect("decode manifest");
    assert!(!first_manifest.files.is_empty());
    let lineage = lineage_of(&first.manifest_key);
    let old_orphan = format!("snapshot/node-a/{lineage}/files/old-orphan");
    let young_orphan = format!("snapshot/node-a/{lineage}/files/young-orphan");
    cluster
        .object_store
        .put(
            &old_orphan,
            Bytes::from_static(b"old"),
            WriteCondition::CreateOnly,
        )
        .await
        .expect("upload old orphan");
    cluster.clock.shift(2 * 60 * 60 * 1_000_000);
    cluster
        .object_store
        .put(
            &young_orphan,
            Bytes::from_static(b"young"),
            WriteCondition::CreateOnly,
        )
        .await
        .expect("upload young orphan");
    test.commit_prompt("orphan-flow", 1).await;
    test.node.take_snapshot().await.expect("second snapshot");

    assert!(
        cluster
            .object_store
            .get(&old_orphan)
            .await
            .expect("read old orphan")
            .is_none()
    );
    assert!(
        cluster
            .object_store
            .get(&young_orphan)
            .await
            .expect("read young orphan")
            .is_some()
    );
    for file in &first_manifest.files {
        assert!(
            cluster
                .object_store
                .get(&file.object_key)
                .await
                .expect("read retained file")
                .is_some(),
            "{} remains referenced by the first manifest",
            file.object_key
        );
    }
    test.node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn retention_limits_snapshot_listing_pages_per_run() {
    let mut cluster = TestCluster::new();
    let mut config = cluster.config(LocalStoreConfig::InMemory);
    config.snapshots.deletion_delay = Duration::ZERO;
    let test = cluster.start_with(&config).await.expect("node starts");
    for index in 0..10_001 {
        let key = format!("snapshot/node-a/zzzz/files/orphan-{index:05}");
        cluster
            .object_store
            .put(&key, Bytes::new(), WriteCondition::CreateOnly)
            .await
            .expect("upload orphan");
    }
    let lists_before = cluster
        .object_store
        .call_count(ObjectStoreOperation::List, "snapshot/node-a/");
    test.commit_prompt("bounded-list-flow", 1).await;
    test.node.take_snapshot().await.expect("snapshot succeeds");
    let lists_after = cluster
        .object_store
        .call_count(ObjectStoreOperation::List, "snapshot/node-a/");
    assert_eq!(lists_after - lists_before, 10);
    let first_orphan = "snapshot/node-a/zzzz/files/orphan-00000";
    assert!(
        cluster
            .object_store
            .get(first_orphan)
            .await
            .expect("read orphan before scan completes")
            .is_some()
    );
    test.commit_prompt("bounded-list-flow", 1).await;
    test.node
        .take_snapshot()
        .await
        .expect("second snapshot succeeds");
    assert!(
        cluster
            .object_store
            .get(first_orphan)
            .await
            .expect("read orphan after scan completes")
            .is_none()
    );
    test.node.shutdown().await;
}
