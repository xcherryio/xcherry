// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use dexzd_engine::{CommitUrgency, LocalStoreConfig, NodeStopReason};
use dexzd_object_store::{
    FaultAction, FaultInjector, InMemoryObjectStore, ObjectStore, ObjectStoreOperation,
    WriteCondition,
};
use dexzd_protocol::dexzd::v1::SnapshotManifest;
use dexzd_storage::RocksDbStoreOptions;
use prost::Message;
use tokio::sync::Notify;

use crate::harness::{CountingApplier, PARTITION_COUNT, RecordingRuntime, TestCluster};

#[derive(Default)]
struct DelayedSnapshotDelete {
    is_armed: AtomicBool,
    should_fail_manifest_put: AtomicBool,
    entered: Notify,
}

impl FaultInjector for DelayedSnapshotDelete {
    fn action(&self, operation: ObjectStoreOperation, key: &str) -> FaultAction {
        if operation == ObjectStoreOperation::Put
            && key.contains("/manifest/")
            && self.should_fail_manifest_put.swap(false, Ordering::SeqCst)
        {
            return FaultAction::FailBeforeEffect;
        }
        if operation == ObjectStoreOperation::Delete
            && key.is_empty()
            && self.is_armed.swap(false, Ordering::SeqCst)
        {
            self.entered.notify_one();
            return FaultAction::Delay(Duration::from_secs(30));
        }
        FaultAction::Proceed
    }
}

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
    test.node.shutdown().await.expect("lease released");
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
    test.node.shutdown().await.expect("lease released");

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
    test.node.shutdown().await.expect("lease released");
}

#[tokio::test(start_paused = true)]
async fn restart_restores_the_snapshot_and_replays_only_later_batches() {
    let mut cluster = TestCluster::new();
    let first = cluster.start_in_memory().await;
    first.commit_prompt("restore-flow", 3).await;
    first.node.take_snapshot().await.expect("snapshot succeeds");
    let mut last_committed_command = None;
    for _ in 0..2 {
        let (command, ticket) = first
            .submit("restore-flow", CommitUrgency::Prompt)
            .expect("submit tail command");
        ticket.wait().await.expect("tail command commits");
        last_committed_command = Some(command);
    }
    let last_committed_command = last_committed_command.expect("tail has a command");
    let continuous = first
        .node
        .committer()
        .committed_view()
        .expect("continuous state view");
    drop(first);

    let second = cluster.start_in_memory().await;
    assert_eq!(
        second.applier.applied_commands().len(),
        2 * (1 + usize::from(cfg!(debug_assertions)))
    );
    assert_eq!(second.committed_count("restore-flow").await, 5);
    let recovered = second
        .node
        .committer()
        .committed_view()
        .expect("recovered state view");
    dexzd_storage::compare_store_views(
        &continuous,
        &recovered,
        &[
            // Restoring a snapshot starts a new local store lineage.
            dexzd_storage::CanonicalizedStoreField::NodeLineageId,
            // The restored store starts a new snapshot sequence.
            dexzd_storage::CanonicalizedStoreField::NodeLastSnapshotNumber,
            // Recovery records progress under its new writer epoch.
            dexzd_storage::CanonicalizedStoreField::PartitionApplied,
            // Recovery acquires a new writer epoch.
            dexzd_storage::CanonicalizedStoreField::PartitionAppliedEpoch,
        ],
    )
    .unwrap_or_else(|error| {
        panic!(
            "snapshot plus log differs after Flow {} command {:?}: {error}",
            last_committed_command.flow_id, last_committed_command.command_id
        )
    });
    second.node.shutdown().await.expect("lease released");
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
    assert_eq!(
        restarted.applier.applied_commands().len(),
        1 + usize::from(cfg!(debug_assertions))
    );
    restarted.node.shutdown().await.expect("lease released");
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
    test.node.shutdown().await.expect("lease released");
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
    for _ in 0..2 {
        second
            .node
            .take_snapshot()
            .await
            .expect("complete orphan sweep");
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
    restarted.node.shutdown().await.expect("lease released");
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
    test.node.shutdown().await.expect("lease released");
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
    test.node.take_snapshot().await.expect("mark old orphan");
    cluster.clock.shift(2 * 60 * 60 * 1_000_000);
    tokio::time::sleep(Duration::from_secs(4)).await;
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
    test.node.take_snapshot().await.expect("sweep old orphan");

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
    test.node.shutdown().await.expect("lease released");
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
        .expect("second snapshot marks orphans");
    assert!(
        cluster
            .object_store
            .get(first_orphan)
            .await
            .expect("read marked orphan")
            .is_some()
    );
    test.node.take_snapshot().await.expect("start fresh scan");
    test.node
        .take_snapshot()
        .await
        .expect("complete fresh sweep");
    assert!(
        cluster
            .object_store
            .get(first_orphan)
            .await
            .expect("read swept orphan")
            .is_none()
    );
    test.node.shutdown().await.expect("lease released");
}

#[tokio::test(start_paused = true)]
async fn retention_resumes_a_backlog_with_a_bounded_candidate_budget() {
    let mut cluster = TestCluster::new();
    let mut config = cluster.config(LocalStoreConfig::InMemory);
    config.snapshots.deletion_delay = Duration::ZERO;
    let test = cluster.start_with(&config).await.expect("node starts");
    test.commit_prompt("bounded-candidates-flow", 1).await;
    let retained_snapshot = test.node.take_snapshot().await.expect("first snapshot");
    let retained_manifest = cluster
        .object_store
        .get(&retained_snapshot.manifest_key)
        .await
        .expect("read retained manifest")
        .expect("retained manifest exists");
    let retained_files = SnapshotManifest::decode(retained_manifest.bytes)
        .expect("decode retained manifest")
        .files;
    assert!(!retained_files.is_empty());

    for index in 0..65 {
        let key = format!("snapshot/node-a/backlog/files/orphan-{index:03}");
        cluster
            .object_store
            .put(&key, Bytes::new(), WriteCondition::CreateOnly)
            .await
            .expect("upload orphan");
    }
    test.node.take_snapshot().await.expect("mark backlog");
    let head_count_before = cluster
        .object_store
        .call_count(ObjectStoreOperation::Head, "snapshot/node-a/backlog/files/");
    assert_eq!(head_count_before, 0);

    test.node.take_snapshot().await.expect("sweep first page");
    let head_count_after_first_sweep = cluster
        .object_store
        .call_count(ObjectStoreOperation::Head, "snapshot/node-a/backlog/files/");
    assert_eq!(head_count_after_first_sweep - head_count_before, 64);
    assert!(
        cluster
            .object_store
            .get("snapshot/node-a/backlog/files/orphan-000")
            .await
            .expect("read first orphan")
            .is_none()
    );
    assert!(
        cluster
            .object_store
            .get("snapshot/node-a/backlog/files/orphan-064")
            .await
            .expect("read pending orphan")
            .is_some()
    );
    for file in &retained_files {
        assert!(
            cluster
                .object_store
                .get(&file.object_key)
                .await
                .expect("read referenced file")
                .is_some(),
            "{} remains referenced by the retained manifest",
            file.object_key
        );
    }

    test.node
        .take_snapshot()
        .await
        .expect("resume backlog sweep");
    let head_count_after_second_sweep = cluster
        .object_store
        .call_count(ObjectStoreOperation::Head, "snapshot/node-a/backlog/files/");
    assert_eq!(
        head_count_after_second_sweep - head_count_after_first_sweep,
        1
    );
    assert!(
        cluster
            .object_store
            .get("snapshot/node-a/backlog/files/orphan-064")
            .await
            .expect("read final orphan")
            .is_none()
    );
    test.node.shutdown().await.expect("lease released");
}

#[tokio::test(start_paused = true)]
async fn idle_orphan_cleanup_reconsiders_a_file_after_the_delay() {
    let mut cluster = TestCluster::new();
    let mut config = cluster.config(LocalStoreConfig::InMemory);
    config.snapshots.deletion_delay = Duration::from_secs(3);
    let test = cluster.start_with(&config).await.expect("node starts");
    test.commit_prompt("idle-orphan-flow", 1).await;
    test.node.take_snapshot().await.expect("snapshot succeeds");
    let orphan_key = "snapshot/node-a/idle/files/orphan";
    cluster
        .object_store
        .put(orphan_key, Bytes::new(), WriteCondition::CreateOnly)
        .await
        .expect("upload orphan");
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(
        cluster
            .object_store
            .get(orphan_key)
            .await
            .expect("read marked orphan")
            .is_some()
    );
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(
        cluster
            .object_store
            .get(orphan_key)
            .await
            .expect("read swept orphan")
            .is_none()
    );
    assert_eq!(snapshot_manifest_keys(&cluster).await.len(), 1);
    test.node.shutdown().await.expect("lease released");
}

#[tokio::test(start_paused = true)]
async fn idle_orphan_cleanup_finishes_paginated_scans() {
    let mut cluster = TestCluster::new();
    let mut config = cluster.config(LocalStoreConfig::InMemory);
    config.snapshots.deletion_delay = Duration::ZERO;
    let test = cluster.start_with(&config).await.expect("node starts");
    for index in 0..10_001 {
        let key = format!("snapshot/node-a/zzzz/files/idle-{index:05}");
        cluster
            .object_store
            .put(&key, Bytes::new(), WriteCondition::CreateOnly)
            .await
            .expect("upload orphan");
    }
    test.commit_prompt("idle-pagination-flow", 1).await;
    test.node.take_snapshot().await.expect("snapshot succeeds");
    let first_orphan = "snapshot/node-a/zzzz/files/idle-00000";
    assert!(
        cluster
            .object_store
            .get(first_orphan)
            .await
            .expect("read orphan")
            .is_some()
    );
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(
        cluster
            .object_store
            .get(first_orphan)
            .await
            .expect("read swept orphan")
            .is_none()
    );
    assert_eq!(snapshot_manifest_keys(&cluster).await.len(), 1);
    test.node.shutdown().await.expect("lease released");
}

#[tokio::test(start_paused = true)]
async fn idle_orphan_cleanup_stops_after_losing_the_lease() {
    let mut cluster = TestCluster::new();
    let mut config = cluster.config(LocalStoreConfig::InMemory);
    config.snapshots.deletion_delay = Duration::from_secs(3);
    let test = cluster.start_with(&config).await.expect("node starts");
    test.commit_prompt("stopped-orphan-flow", 1).await;
    test.node.take_snapshot().await.expect("snapshot succeeds");
    let orphan_key = "snapshot/node-a/stopped/files/orphan";
    cluster
        .object_store
        .put(orphan_key, Bytes::new(), WriteCondition::CreateOnly)
        .await
        .expect("upload orphan");
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(
        cluster
            .object_store
            .get(orphan_key)
            .await
            .expect("read marked orphan")
            .is_some()
    );
    cluster.expire_lease_of("node-a").await;
    assert!(matches!(
        test.node.wait_for_stop().await,
        NodeStopReason::LeaseLost { .. }
    ));
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(
        cluster
            .object_store
            .get(orphan_key)
            .await
            .expect("read stopped orphan")
            .is_some()
    );
    test.node.shutdown().await.expect("stopped node shuts down");

    let successor = cluster.start_with(&config).await.expect("successor starts");
    tokio::time::sleep(Duration::from_secs(8)).await;
    assert!(
        cluster
            .object_store
            .get(orphan_key)
            .await
            .expect("read successor sweep")
            .is_none()
    );
    successor.node.shutdown().await.expect("lease released");
}

#[tokio::test(start_paused = true)]
async fn successor_snapshot_survives_old_owners_in_flight_delete() {
    let mut cluster = TestCluster::new();
    let delayed_delete = Arc::new(DelayedSnapshotDelete::default());
    cluster.object_store = Arc::new(InMemoryObjectStore::with_injector_and_clock(
        delayed_delete.clone(),
        cluster.clock.clone(),
    ));
    let mut config = cluster.config(LocalStoreConfig::InMemory);
    config.snapshots.deletion_delay = Duration::ZERO;
    let first = cluster
        .start_with(&config)
        .await
        .expect("first owner starts");
    first.commit_prompt("in-flight-delete-flow", 1).await;
    let first_reference = first.node.take_snapshot().await.expect("first snapshot");
    let first_lineage = lineage_of(&first_reference.manifest_key);
    let orphan_key = format!("snapshot/node-a/{first_lineage}/files/orphan");
    cluster
        .object_store
        .put(&orphan_key, Bytes::new(), WriteCondition::CreateOnly)
        .await
        .expect("upload orphan");
    first.node.take_snapshot().await.expect("mark orphan");

    delayed_delete.is_armed.store(true, Ordering::SeqCst);
    tokio::time::advance(Duration::from_secs(2)).await;
    delayed_delete.entered.notified().await;
    tokio::task::yield_now().await;
    assert!(
        cluster
            .object_store
            .get(&orphan_key)
            .await
            .expect("read in-flight orphan")
            .is_some()
    );
    cluster.expire_lease_of("node-a").await;
    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(matches!(
        first.node.wait_for_stop().await,
        NodeStopReason::LeaseLost { .. }
    ));

    let mut successor_config = cluster.config(LocalStoreConfig::InMemory);
    successor_config.snapshots.deletion_delay = Duration::from_secs(24 * 60 * 60);
    let successor = cluster
        .start_with(&successor_config)
        .await
        .expect("successor starts while delete is in flight");
    successor.commit_prompt("in-flight-delete-flow", 1).await;
    let successor_reference = successor
        .node
        .take_snapshot()
        .await
        .expect("successor snapshot");
    let successor_lineage = lineage_of(&successor_reference.manifest_key);
    assert_ne!(successor_lineage, first_lineage);
    let content = cluster
        .object_store
        .get(&successor_reference.manifest_key)
        .await
        .expect("read successor manifest")
        .expect("successor manifest exists");
    let successor_manifest = SnapshotManifest::decode(content.bytes).expect("decode manifest");
    assert!(!successor_manifest.files.is_empty());
    assert!(
        successor_manifest
            .files
            .iter()
            .all(|file| lineage_of(&file.object_key) == successor_lineage)
    );
    assert!(
        cluster
            .object_store
            .get(&orphan_key)
            .await
            .expect("read old in-flight file after successor snapshot")
            .is_some()
    );

    tokio::time::advance(Duration::from_secs(31)).await;
    tokio::task::yield_now().await;
    assert!(
        cluster
            .object_store
            .get(&orphan_key)
            .await
            .expect("read deleted old orphan")
            .is_none()
    );
    for file in &successor_manifest.files {
        assert!(
            cluster
                .object_store
                .get(&file.object_key)
                .await
                .expect("read successor snapshot file")
                .is_some(),
            "{} survives the old owner's delete",
            file.object_key
        );
    }
    first
        .node
        .shutdown()
        .await
        .expect("stopped node shuts down");
    successor.node.shutdown().await.expect("lease released");
}

#[tokio::test(start_paused = true)]
async fn reused_local_store_starts_a_new_snapshot_lineage() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let mut cluster = TestCluster::new();
    let config = cluster.config(LocalStoreConfig::RocksDb {
        directory: directory.path().join("store"),
        options: RocksDbStoreOptions::default(),
    });
    let first = cluster
        .start_with(&config)
        .await
        .expect("first owner starts");
    first.commit_prompt("reused-store-flow", 1).await;
    let first_reference = first.node.take_snapshot().await.expect("first snapshot");
    first.node.shutdown().await.expect("first lease released");

    let successor = cluster
        .start_with(&config)
        .await
        .expect("successor reuses the local store");
    successor.commit_prompt("reused-store-flow", 1).await;
    let successor_reference = successor
        .node
        .take_snapshot()
        .await
        .expect("successor snapshot");
    let first_lineage = lineage_of(&first_reference.manifest_key);
    let successor_lineage = lineage_of(&successor_reference.manifest_key);
    assert_ne!(successor_lineage, first_lineage);
    let content = cluster
        .object_store
        .get(&successor_reference.manifest_key)
        .await
        .expect("read successor manifest")
        .expect("successor manifest exists");
    let manifest = SnapshotManifest::decode(content.bytes).expect("decode successor manifest");
    assert!(!manifest.files.is_empty());
    assert!(
        manifest
            .files
            .iter()
            .all(|file| lineage_of(&file.object_key) == successor_lineage)
    );
    successor.node.shutdown().await.expect("lease released");
}

#[tokio::test(start_paused = true)]
async fn reused_rocksdb_store_snapshot_survives_old_owners_in_flight_orphan_delete() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let original_store_directory = directory.path().join("original-store");
    let reused_store_directory = directory.path().join("reused-store");
    let mut cluster = TestCluster::new();
    let delayed_delete = Arc::new(DelayedSnapshotDelete::default());
    cluster.object_store = Arc::new(InMemoryObjectStore::with_injector_and_clock(
        delayed_delete.clone(),
        cluster.clock.clone(),
    ));
    let mut original_config = cluster.config(LocalStoreConfig::RocksDb {
        directory: original_store_directory.clone(),
        options: RocksDbStoreOptions::default(),
    });
    original_config.snapshots.deletion_delay = Duration::ZERO;

    let initial_owner = cluster
        .start_with(&original_config)
        .await
        .expect("initial owner starts");
    initial_owner.commit_prompt("reused-orphan-flow", 1).await;
    delayed_delete
        .should_fail_manifest_put
        .store(true, Ordering::SeqCst);
    initial_owner
        .node
        .take_snapshot()
        .await
        .expect_err("manifest upload fails after file uploads");
    assert!(snapshot_manifest_keys(&cluster).await.is_empty());
    let orphan_keys = snapshot_object_keys(&cluster).await;
    let first_orphan_key = orphan_keys
        .first()
        .expect("failed upload left orphan files");
    let previous_lineage = lineage_of(first_orphan_key);
    initial_owner
        .node
        .shutdown()
        .await
        .expect("initial lease released");

    // A closed-store copy lets both owners hold the same RocksDB state during the overlapping DELETE.
    std::fs::create_dir(&reused_store_directory).expect("create reused store directory");
    for entry in
        std::fs::read_dir(&original_store_directory).expect("read original store directory")
    {
        let entry = entry.expect("read original store entry");
        let destination = reused_store_directory.join(entry.file_name());
        std::fs::copy(entry.path(), destination).expect("copy closed RocksDB file");
    }

    let former_owner = cluster
        .start_with(&original_config)
        .await
        .expect("former owner reopens the original store");
    delayed_delete.is_armed.store(true, Ordering::SeqCst);
    tokio::time::advance(Duration::from_secs(3)).await;
    delayed_delete.entered.notified().await;
    assert!(
        cluster
            .object_store
            .get(first_orphan_key)
            .await
            .expect("read in-flight orphan")
            .is_some()
    );
    cluster.expire_lease_of("node-a").await;
    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(matches!(
        former_owner.node.wait_for_stop().await,
        NodeStopReason::LeaseLost { .. }
    ));

    let object_keys_before_successor = snapshot_object_keys(&cluster).await;
    let mut successor_config = cluster.config(LocalStoreConfig::RocksDb {
        directory: reused_store_directory,
        options: RocksDbStoreOptions::default(),
    });
    successor_config.snapshots.deletion_delay = Duration::from_secs(24 * 60 * 60);
    let successor = cluster
        .start_with(&successor_config)
        .await
        .expect("successor reopens the existing RocksDB store");
    assert_eq!(successor.committed_count("reused-orphan-flow").await, 1);
    let successor_reference = successor
        .node
        .take_snapshot()
        .await
        .expect("successor snapshot");
    let successor_lineage = lineage_of(&successor_reference.manifest_key);
    assert_ne!(successor_lineage, previous_lineage);
    let newly_written_keys: Vec<String> = snapshot_object_keys(&cluster)
        .await
        .into_iter()
        .filter(|key| !object_keys_before_successor.contains(key))
        .collect();
    assert!(!newly_written_keys.is_empty());
    assert!(
        newly_written_keys
            .iter()
            .all(|key| lineage_of(key) == successor_lineage)
    );
    let content = cluster
        .object_store
        .get(&successor_reference.manifest_key)
        .await
        .expect("read successor manifest")
        .expect("successor manifest exists");
    let successor_manifest = SnapshotManifest::decode(content.bytes).expect("decode manifest");
    assert!(!successor_manifest.files.is_empty());
    assert!(
        successor_manifest
            .files
            .iter()
            .all(|file| lineage_of(&file.object_key) == successor_lineage)
    );

    tokio::time::advance(Duration::from_secs(31)).await;
    tokio::task::yield_now().await;
    for key in &orphan_keys {
        assert!(
            cluster
                .object_store
                .get(key)
                .await
                .expect("read deleted orphan")
                .is_none(),
            "{key} was deleted"
        );
    }
    for file in &successor_manifest.files {
        assert!(
            cluster
                .object_store
                .get(&file.object_key)
                .await
                .expect("read successor file")
                .is_some(),
            "{} survives the old owner's delete",
            file.object_key
        );
    }
    former_owner
        .node
        .shutdown()
        .await
        .expect("stopped node shuts down");
    successor.node.shutdown().await.expect("lease released");

    let restore_config = cluster.config(LocalStoreConfig::RocksDb {
        directory: directory.path().join("restored-store"),
        options: RocksDbStoreOptions::default(),
    });
    let restored = cluster
        .start_with(&restore_config)
        .await
        .expect("snapshot restores into a fresh RocksDB store");
    assert_eq!(restored.committed_count("reused-orphan-flow").await, 1);
    restored.node.shutdown().await.expect("lease released");
}
