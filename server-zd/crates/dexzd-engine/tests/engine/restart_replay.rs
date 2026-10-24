// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! How much of the log a restart reads when no snapshot covers it.

use std::collections::BTreeSet;
use std::time::Duration;

use bytes::Bytes;
use dexzd_engine::{CommitUrgency, EngineNodeConfig, LocalStoreConfig};
use dexzd_log::{BatchContent, BatchRecord, StreamIdentity, encode_batch};
use dexzd_object_store::{ObjectStore, ObjectStoreOperation, WriteCondition};
use dexzd_storage::RocksDbStoreOptions;

use crate::harness::{FAILING_FLOW, TestCluster};

const LOG_PREFIX: &str = "log/";
/// Commits to one Flow, so every other partition stays idle.
const HOT_FLOW: &str = "hot-flow";
/// Longer than the interval at which a serving node records applied positions.
const ADVANCE_WAIT: Duration = Duration::from_secs(2);
/// Recovery may read the batch at the applied position to tell a deleted range from a hole.
const APPLIED_BATCH_READS: usize = 1;

fn rocksdb_config(cluster: &TestCluster, directory: &tempfile::TempDir) -> EngineNodeConfig {
    let mut config = cluster.config(LocalStoreConfig::RocksDb {
        directory: directory.path().join("store"),
        options: RocksDbStoreOptions::default(),
    });
    config.snapshots.interval = Duration::from_secs(24 * 60 * 60);
    config
}

/// Returns how many log batches written before `restart` it reads, and its result.
async fn count_replayed_batches<T>(
    cluster: &mut TestCluster,
    restart: impl AsyncFnOnce(&mut TestCluster) -> T,
) -> (usize, T) {
    let listing = cluster
        .object_store
        .list(LOG_PREFIX, None, 1000)
        .await
        .expect("list the log");
    assert!(listing.next_start_after.is_none(), "one page lists the log");
    let written: BTreeSet<String> = listing
        .objects
        .into_iter()
        .map(|object| object.key)
        .collect();
    let count_reads = |cluster: &TestCluster| {
        cluster
            .faults
            .count_operations(ObjectStoreOperation::Get, |key| written.contains(key))
    };
    let reads_before = count_reads(cluster);
    let restarted = restart(cluster).await;
    (count_reads(cluster) - reads_before, restarted)
}

async fn assert_no_snapshot(cluster: &TestCluster) {
    assert!(
        cluster
            .cluster_manifest()
            .await
            .partitions
            .iter()
            .all(|partition| partition.snapshot.is_none()),
        "no snapshot may cover the log"
    );
}

#[tokio::test(start_paused = true)]
async fn a_graceful_restart_without_a_snapshot_reads_only_the_log_tail() {
    const HOT_COMMANDS: usize = 50;
    let directory = tempfile::tempdir().expect("temporary directory");
    let mut cluster = TestCluster::new();
    let config = rocksdb_config(&cluster, &directory);
    let first = cluster.start_with(&config).await.expect("node starts");
    first.commit_prompt(HOT_FLOW, HOT_COMMANDS).await;
    first.node.shutdown().await.expect("lease released");
    assert_no_snapshot(&cluster).await;

    let (replayed, second) = count_replayed_batches(&mut cluster, async |cluster| {
        cluster.start_with(&config).await.expect("node restarts")
    })
    .await;
    assert!(
        replayed <= APPLIED_BATCH_READS,
        "the restart replayed {replayed} log batches after {HOT_COMMANDS} commits"
    );
    assert!(second.applier.applied_commands().is_empty());
    assert_eq!(second.committed_count(HOT_FLOW).await, HOT_COMMANDS as u64);
    second.commit_prompt(HOT_FLOW, 1).await;
    assert_eq!(
        second.committed_count(HOT_FLOW).await,
        HOT_COMMANDS as u64 + 1
    );
    second.node.shutdown().await.expect("lease released");
}

#[tokio::test(start_paused = true)]
async fn a_restart_after_a_crash_reads_the_log_from_the_last_recorded_position() {
    const RECORDED_COMMANDS: usize = 40;
    const UNRECORDED_COMMANDS: usize = 5;
    let directory = tempfile::tempdir().expect("temporary directory");
    let mut cluster = TestCluster::new();
    let config = rocksdb_config(&cluster, &directory);
    let first = cluster.start_with(&config).await.expect("node starts");
    first.commit_prompt(HOT_FLOW, RECORDED_COMMANDS).await;
    tokio::time::sleep(ADVANCE_WAIT).await;
    first.commit_prompt(HOT_FLOW, UNRECORDED_COMMANDS).await;
    drop(first);
    assert_no_snapshot(&cluster).await;

    let (replayed, second) = count_replayed_batches(&mut cluster, async |cluster| {
        cluster.start_with(&config).await.expect("node restarts")
    })
    .await;
    assert!(
        (UNRECORDED_COMMANDS..=UNRECORDED_COMMANDS + APPLIED_BATCH_READS).contains(&replayed),
        "the restart replayed {replayed} log batches; {UNRECORDED_COMMANDS} followed the last \
         recorded position"
    );
    assert_eq!(
        second.committed_count(HOT_FLOW).await,
        (RECORDED_COMMANDS + UNRECORDED_COMMANDS) as u64
    );
    second.node.shutdown().await.expect("lease released");
}

#[tokio::test(start_paused = true)]
async fn recorded_positions_after_voided_pipelined_batches_keep_every_write() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let mut cluster = TestCluster::new();
    let config = rocksdb_config(&cluster, &directory);
    let first = cluster.start_with(&config).await.expect("node starts");
    first.commit_prompt(HOT_FLOW, 1).await;
    drop(first);
    let cluster_object = dexzd_cluster_metadata::bootstrap_or_validate(
        cluster.object_store.as_ref(),
        cluster.clock.as_ref(),
        config.cluster,
    )
    .await
    .expect("cluster exists");
    let stream = StreamIdentity {
        cluster_id: cluster_object
            .cluster
            .cluster_id
            .as_slice()
            .try_into()
            .expect("16-byte cluster ID"),
        node_id: config.node.node_id.clone(),
        incarnation: config.node.incarnation,
    };
    // The crashed epoch-1 writer pipelined seq 4 and 5 past the gap at seq 3.
    for seq in [4, 5] {
        let stale = encode_batch(&BatchContent {
            stream: stream.clone(),
            epoch: 1,
            seq,
            sealed_at_unix_micros: 0,
            records: vec![BatchRecord {
                partition: 0,
                bytes: Bytes::from_static(b"never acknowledged"),
            }],
            epoch_start_writer: None,
        })
        .expect("encode batch");
        cluster
            .object_store
            .put(&stream.batch_key(seq), stale, WriteCondition::CreateOnly)
            .await
            .expect("put succeeds");
    }

    let second = cluster.start_with(&config).await.expect("node recovers");
    second.commit_prompt(HOT_FLOW, 3).await;
    tokio::time::sleep(ADVANCE_WAIT).await;
    second.commit_prompt(HOT_FLOW, 2).await;
    drop(second);

    let (replayed, third) = count_replayed_batches(&mut cluster, async |cluster| {
        cluster.start_with(&config).await.expect("node restarts")
    })
    .await;
    assert!(
        (2..=2 + APPLIED_BATCH_READS).contains(&replayed),
        "the restart replayed {replayed} log batches; 2 followed the last recorded position"
    );
    assert_eq!(third.committed_count(HOT_FLOW).await, 6);
    third.commit_prompt(HOT_FLOW, 1).await;
    assert_eq!(third.committed_count(HOT_FLOW).await, 7);
    third.node.shutdown().await.expect("lease released");
}

#[tokio::test(start_paused = true)]
async fn a_halted_partition_keeps_its_position_while_the_others_advance() {
    const HEALTHY_COMMANDS: usize = 3;
    let directory = tempfile::tempdir().expect("temporary directory");
    let mut cluster = TestCluster::new();
    let config = rocksdb_config(&cluster, &directory);
    let first = cluster.start_with(&config).await.expect("node starts");
    let healthy_flow = first.flow_in_other_partition(&[FAILING_FLOW]);
    let failing_partition = first.context(FAILING_FLOW).partition;
    let (_, ticket) = first
        .submit(FAILING_FLOW, CommitUrgency::Prompt)
        .expect("submit succeeds");
    assert!(ticket.wait().await.is_err());
    first.commit_prompt(&healthy_flow, HEALTHY_COMMANDS).await;
    tokio::time::sleep(ADVANCE_WAIT).await;
    first.node.shutdown().await.expect("lease released");

    let (replayed, second) = count_replayed_batches(&mut cluster, async |cluster| {
        cluster.start_with(&config).await.expect("node restarts")
    })
    .await;
    // The halted partition resumes before its failed batch, so the restart reads it again.
    assert!(
        replayed > HEALTHY_COMMANDS,
        "the restart replayed {replayed} log batches"
    );
    assert_eq!(second.node.halted_partitions(), vec![failing_partition]);
    assert_eq!(
        second.committed_count(&healthy_flow).await,
        HEALTHY_COMMANDS as u64
    );
    second.commit_prompt(&healthy_flow, 1).await;
    second.node.shutdown().await.expect("lease released");
}
