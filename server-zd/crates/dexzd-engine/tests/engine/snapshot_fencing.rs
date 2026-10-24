// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::time::Duration;

use dexzd_engine::{LocalStoreConfig, NodeStopReason, SnapshotError};
use dexzd_object_store::ObjectStore;
use dexzd_protocol::dexzd::v1::SnapshotReference;
use dexzd_storage::RocksDbStoreOptions;

use crate::harness::TestCluster;

/// Long enough for a successor to start, commit, and snapshot while the read waits.
const PUBLICATION_DELAY: Duration = Duration::from_secs(300);

async fn snapshot_pointers(cluster: &TestCluster) -> Vec<Option<SnapshotReference>> {
    cluster
        .cluster_manifest()
        .await
        .partitions
        .into_iter()
        .map(|partition| partition.snapshot)
        .collect()
}

#[tokio::test(start_paused = true)]
async fn a_stale_snapshot_never_replaces_a_successor_snapshot_after_log_cleanup() {
    let mut cluster = TestCluster::new();
    let first = cluster.start_in_memory().await;
    first.commit_prompt("takeover-flow", 3).await;
    cluster
        .faults
        .delay_next_get_of("manifest", PUBLICATION_DELAY);

    let stale_snapshot = first.node.take_snapshot();
    let takeover = async {
        cluster.faults.wait_until_delayed_operation_entered().await;
        cluster.expire_lease_of("node-a").await;
        assert!(matches!(
            first.node.wait_for_stop().await,
            NodeStopReason::LeaseLost { .. }
        ));
        let mut config = cluster.config(LocalStoreConfig::InMemory);
        config.snapshots.deletion_delay = Duration::ZERO;
        let successor = cluster.start_with(&config).await.expect("successor starts");
        assert_eq!(successor.committed_count("takeover-flow").await, 3);
        successor.commit_prompt("takeover-flow", 2).await;
        let reference = successor
            .node
            .take_snapshot()
            .await
            .expect("successor snapshot succeeds");
        let manifest = cluster.cluster_manifest().await;
        assert!(
            manifest
                .partitions
                .iter()
                .flat_map(|partition| &partition.segments)
                .all(|segment| segment.start_seq > 1),
            "the successor's snapshot lets log cleanup delete covered batches"
        );
        (successor, reference)
    };
    let (stale_result, (successor, reference)) = tokio::join!(stale_snapshot, takeover);

    assert!(stale_result.is_err(), "{stale_result:?}");
    assert!(
        snapshot_pointers(&cluster)
            .await
            .iter()
            .all(|pointer| pointer.as_ref() == Some(&reference))
    );
    drop(successor);
    drop(first);
    let restarted = cluster.start_in_memory().await;
    assert_eq!(restarted.committed_count("takeover-flow").await, 5);
    restarted.node.shutdown().await.expect("lease released");
}

#[tokio::test(start_paused = true)]
async fn a_snapshot_is_not_published_after_another_epoch_owns_the_cluster() {
    let mut cluster = TestCluster::new();
    let first = cluster.start_in_memory().await;
    first.commit_prompt("fenced-snapshot-flow", 2).await;
    cluster
        .faults
        .delay_next_get_of("manifest", PUBLICATION_DELAY);

    let stale_snapshot = first.node.take_snapshot();
    let newer_epoch = async {
        cluster.faults.wait_until_delayed_operation_entered().await;
        dexzd_cluster_metadata::bump_node_epoch(
            cluster.object_store.as_ref(),
            cluster.clock.as_ref(),
            &cluster.config(LocalStoreConfig::InMemory).node,
        )
        .await
        .expect("a newer epoch takes ownership")
    };
    let (result, _) = tokio::join!(stale_snapshot, newer_epoch);

    assert!(
        matches!(result, Err(SnapshotError::Fenced { .. })),
        "{result:?}"
    );
    assert!(
        snapshot_pointers(&cluster)
            .await
            .iter()
            .all(Option::is_none)
    );
    assert_eq!(first.node.wait_for_stop().await, NodeStopReason::Fenced);
    first.node.shutdown().await.expect("lease released");
}

#[tokio::test(start_paused = true)]
async fn an_in_flight_snapshot_is_not_published_after_the_node_stops_serving() {
    let mut cluster = TestCluster::new();
    let first = cluster.start_in_memory().await;
    first.commit_prompt("stopped-snapshot-flow", 2).await;
    cluster
        .faults
        .delay_next_get_of("manifest", PUBLICATION_DELAY);

    let in_flight_snapshot = first.node.take_snapshot();
    let lease_loss = async {
        cluster.faults.wait_until_delayed_operation_entered().await;
        cluster.expire_lease_of("node-a").await;
        first.node.wait_for_stop().await
    };
    let (result, stop_reason) = tokio::join!(in_flight_snapshot, lease_loss);

    assert!(matches!(stop_reason, NodeStopReason::LeaseLost { .. }));
    assert!(
        matches!(result, Err(SnapshotError::NodeStopped)),
        "{result:?}"
    );
    assert!(
        snapshot_pointers(&cluster)
            .await
            .iter()
            .all(Option::is_none)
    );
    first.node.shutdown().await.expect("lease released");
}

/// Runs in real time: paused time does not advance while the upload's blocking task runs.
#[tokio::test]
async fn shutdown_abandons_an_in_flight_snapshot_upload_and_releases_the_lease() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let mut cluster = TestCluster::new();
    let mut config = cluster.config(LocalStoreConfig::RocksDb {
        directory: directory.path().join("store"),
        options: RocksDbStoreOptions::default(),
    });
    config.snapshots.log_threshold_bytes = 1;
    let first = cluster.start_with(&config).await.expect("node starts");
    cluster
        .faults
        .delay_next_put_under("snapshot/", Duration::from_secs(60));
    first.commit_prompt("shutdown-flow", 1).await;
    tokio::time::timeout(
        Duration::from_secs(30),
        cluster.faults.wait_until_delayed_operation_entered(),
    )
    .await
    .expect("the scheduled snapshot starts uploading");

    tokio::time::timeout(Duration::from_secs(5), first.node.shutdown())
        .await
        .expect("shutdown does not wait for the upload")
        .expect("lease released");
    let snapshot_keys = cluster
        .object_store
        .list("snapshot/", None, 1000)
        .await
        .expect("list snapshots")
        .objects;
    assert!(
        snapshot_keys
            .iter()
            .all(|object| !object.key.contains("/manifest/")),
        "{snapshot_keys:?}"
    );

    let restart_started = std::time::Instant::now();
    let successor = cluster.start_with(&config).await.expect("successor starts");
    assert!(
        restart_started.elapsed() < Duration::from_secs(3),
        "the successor takes the released lease without waiting for it to expire"
    );
    assert_eq!(successor.committed_count("shutdown-flow").await, 1);
    let reference = successor
        .node
        .take_snapshot()
        .await
        .expect("the abandoned upload left the snapshot number usable");
    assert_eq!(reference.snapshot_number, 1);
    successor.node.shutdown().await.expect("lease released");
}
