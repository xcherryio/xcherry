// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_engine::{EngineStartError, LocalStoreConfig};
use dexzd_object_store::ObjectStore;
use dexzd_storage::RocksDbStoreOptions;

use crate::harness::TestCluster;

/// Returns each partition's segments as (incarnation, is open, start seq).
async fn segments_by_partition(cluster: &TestCluster) -> Vec<Vec<(u32, bool, u64)>> {
    cluster
        .cluster_manifest()
        .await
        .partitions
        .iter()
        .map(|partition| {
            partition
                .segments
                .iter()
                .map(|segment| (segment.incarnation, segment.is_open, segment.start_seq))
                .collect()
        })
        .collect()
}

async fn has_object(cluster: &TestCluster, key: &str) -> bool {
    cluster
        .object_store
        .head(key)
        .await
        .expect("head succeeds")
        .is_some()
}

#[tokio::test(start_paused = true)]
async fn a_new_incarnation_keeps_post_snapshot_writes_after_local_disk_loss() {
    let mut cluster = TestCluster::new();
    let first = cluster.start_in_memory().await;
    first.commit_prompt("incarnation-flow", 3).await;
    first.node.take_snapshot().await.expect("snapshot succeeds");
    first.commit_prompt("incarnation-flow", 2).await;
    drop(first);

    let mut config = cluster.config(LocalStoreConfig::InMemory);
    config.node.incarnation = 2;
    let second = cluster
        .start_with(&config)
        .await
        .expect("node starts with a new incarnation");
    assert_eq!(second.committed_count("incarnation-flow").await, 5);
    second.commit_prompt("incarnation-flow", 2).await;
    assert_eq!(second.committed_count("incarnation-flow").await, 7);
    assert!(
        segments_by_partition(&cluster)
            .await
            .iter()
            .all(|segments| segments == &[(1, false, 1), (2, true, 1)])
    );
    assert!(has_object(&cluster, "log/node-a/0000000002/00000000000000000001").await);
    drop(second);

    // The snapshot predates the end of incarnation 1, so recovery reads its closed segment.
    let third = cluster.start_with(&config).await.expect("node restarts");
    assert_eq!(third.committed_count("incarnation-flow").await, 7);
    third.commit_prompt("incarnation-flow", 1).await;
    assert_eq!(third.committed_count("incarnation-flow").await, 8);
    third.node.shutdown().await.expect("lease released");

    config.node.incarnation = 1;
    let result = cluster.start_with(&config).await;
    assert!(
        matches!(
            &result,
            Err(EngineStartError::IncarnationRegressed {
                configured: 1,
                newest_recorded: 2,
                ..
            })
        ),
        "{:?}",
        result.err()
    );
}

#[tokio::test(start_paused = true)]
async fn a_snapshot_of_the_new_incarnation_reclaims_the_closed_log_stream() {
    let mut cluster = TestCluster::new();
    let first = cluster.start_in_memory().await;
    first.commit_prompt("reclaimed-flow", 3).await;
    first.node.take_snapshot().await.expect("snapshot succeeds");
    first.commit_prompt("reclaimed-flow", 2).await;
    drop(first);

    let mut config = cluster.config(LocalStoreConfig::InMemory);
    config.node.incarnation = 2;
    config.snapshots.deletion_delay = std::time::Duration::ZERO;
    let second = cluster
        .start_with(&config)
        .await
        .expect("node starts with a new incarnation");
    second.commit_prompt("reclaimed-flow", 1).await;
    second
        .node
        .take_snapshot()
        .await
        .expect("snapshot succeeds");
    let log_keys: Vec<String> = cluster
        .object_store
        .list("log/node-a/", None, 1000)
        .await
        .expect("list log")
        .objects
        .into_iter()
        .map(|object| object.key)
        .collect();
    assert!(
        log_keys
            .iter()
            .all(|key| key.starts_with("log/node-a/0000000002/")),
        "{log_keys:?}"
    );
    assert!(
        segments_by_partition(&cluster)
            .await
            .iter()
            .all(|segments| segments.len() == 1 && segments[0].0 == 2 && segments[0].1)
    );
    drop(second);

    let restarted = cluster.start_with(&config).await.expect("node restarts");
    assert_eq!(restarted.committed_count("reclaimed-flow").await, 6);
    restarted.node.shutdown().await.expect("lease released");
}

#[tokio::test(start_paused = true)]
async fn a_new_incarnation_applies_new_batches_to_the_reused_rocksdb_store() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let mut cluster = TestCluster::new();
    let mut config = cluster.config(LocalStoreConfig::RocksDb {
        directory: directory.path().join("store"),
        options: RocksDbStoreOptions::default(),
    });
    let first = cluster.start_with(&config).await.expect("node starts");
    first.commit_prompt("reused-flow", 4).await;
    first.node.shutdown().await.expect("lease released");

    config.node.incarnation = 2;
    let second = cluster
        .start_with(&config)
        .await
        .expect("node starts with a new incarnation");
    assert!(second.applier.applied_commands().is_empty());
    assert_eq!(second.committed_count("reused-flow").await, 4);
    second.commit_prompt("reused-flow", 3).await;
    assert_eq!(second.committed_count("reused-flow").await, 7);
    second.node.shutdown().await.expect("lease released");

    let third = cluster.start_with(&config).await.expect("node restarts");
    assert!(third.applier.applied_commands().is_empty());
    assert_eq!(third.committed_count("reused-flow").await, 7);
    third.node.shutdown().await.expect("lease released");
}

#[tokio::test(start_paused = true)]
async fn an_interrupted_incarnation_change_completes_on_the_next_start() {
    let mut cluster = TestCluster::new();
    let first = cluster.start_in_memory().await;
    first.commit_prompt("interrupted-flow", 2).await;
    drop(first);
    let mut config = cluster.config(LocalStoreConfig::InMemory);
    config.node.incarnation = 2;

    // The epoch update succeeds; closing incarnation 1 fails after it was sealed.
    cluster.faults.fail_puts_of_after("manifest", 1);
    let result = cluster.start_with(&config).await;
    assert!(
        matches!(result, Err(EngineStartError::Metadata(_))),
        "{:?}",
        result.err()
    );
    cluster.faults.clear();
    assert!(
        segments_by_partition(&cluster)
            .await
            .iter()
            .all(|segments| segments == &[(1, true, 1)])
    );

    // The manifest opens incarnation 2, but its first batch cannot be written.
    cluster.faults.fail_puts_under("log/node-a/0000000002/");
    let result = cluster.start_with(&config).await;
    assert!(
        matches!(result, Err(EngineStartError::Log(_))),
        "{:?}",
        result.err()
    );
    cluster.faults.clear();

    let recovered = cluster.start_with(&config).await.expect("node starts");
    assert_eq!(recovered.committed_count("interrupted-flow").await, 2);
    recovered.commit_prompt("interrupted-flow", 1).await;
    assert_eq!(recovered.committed_count("interrupted-flow").await, 3);
    recovered.node.shutdown().await.expect("lease released");
    assert!(
        segments_by_partition(&cluster)
            .await
            .iter()
            .all(|segments| segments == &[(1, false, 1), (2, true, 1)])
    );
}

#[tokio::test(start_paused = true)]
async fn a_different_node_id_fails_startup_before_taking_a_lease() {
    let mut cluster = TestCluster::new();
    let first = cluster.start_in_memory().await;
    first.commit_prompt("owned-flow", 2).await;
    first.node.shutdown().await.expect("lease released");

    let mut config = cluster.config(LocalStoreConfig::InMemory);
    config.node.node_id = "node-b".to_owned();
    let result = cluster.start_with(&config).await;
    assert!(
        matches!(
            &result,
            Err(EngineStartError::NodeIdentityMismatch { configured, recorded, .. })
                if configured == "node-b" && recorded == "node-a"
        ),
        "{:?}",
        result.err()
    );
    assert!(!has_object(&cluster, "lease/node-b").await);
    assert!(
        !cluster
            .cluster_manifest()
            .await
            .nodes
            .contains_key("node-b")
    );

    let restarted = cluster.start_in_memory().await;
    assert_eq!(restarted.committed_count("owned-flow").await, 2);
    restarted.node.shutdown().await.expect("lease released");
}
