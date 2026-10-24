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
use dexzd_engine::{
    CommitError, CommitUrgency, EngineNode, EngineNodeDependencies, EngineRuntimeHooks,
    EngineServices, EngineStartError, LocalStoreConfig, NodeStopReason,
};
use dexzd_keyspace::PartitionId;
use dexzd_log::{BatchContent, BatchRecord, StreamIdentity, encode_batch};
use dexzd_object_store::{ObjectStore, WriteCondition};
use dexzd_storage::RocksDbStoreOptions;

use crate::harness::{
    BLOCKING_FLOW, CountingApplier, FAILING_FLOW, PARTITION_COUNT, RecordingRuntime, SinkEvent,
    TestCluster, assert_frames_finish_after_their_commands,
};

/// Writes a barrier this binary does not support, as a newer binary's upgrade would.
async fn raise_version_barrier_as_a_newer_binary(cluster: &TestCluster, behavior_version: u32) {
    dexzd_cluster_metadata::update_manifest(
        cluster.object_store.as_ref(),
        cluster.clock.as_ref(),
        |manifest| {
            manifest
                .version_barrier
                .get_or_insert_default()
                .behavior_version = behavior_version;
            Ok(dexzd_cluster_metadata::ManifestChange::Changed)
        },
    )
    .await
    .expect("barrier raised");
}

fn all_partitions() -> Vec<PartitionId> {
    (0..PARTITION_COUNT)
        .map(|partition| PartitionId(u16::try_from(partition).expect("partition fits u16")))
        .collect()
}

#[tokio::test(start_paused = true)]
async fn restart_after_crash_replays_the_log_into_a_new_store() {
    let mut cluster = TestCluster::new();
    let first = cluster.start_in_memory().await;
    let other_flow = first.flow_in_other_partition(&["crash-flow"]);
    first.commit_prompt("crash-flow", 3).await;
    first.commit_prompt(&other_flow, 2).await;
    let (last_before_crash, _) = first
        .submit("crash-flow", CommitUrgency::Prompt)
        .expect("submit succeeds");
    drop(first);

    let second = cluster.start_in_memory().await;
    let replayed = second.runtime.committed_commands();
    assert!(
        replayed
            .iter()
            .all(|committed| committed.command.epoch == 1)
    );
    let committed = second.committed_count("crash-flow").await;
    assert!(committed == 3 || committed == 4, "{committed}");
    assert_eq!(second.committed_count(&other_flow).await, 2);
    assert_eq!(replayed.len() as u64, committed + 2);
    assert_eq!(second.runtime.rebuilt_partitions(), all_partitions());
    let (after_restart, ticket) = second
        .submit("crash-flow", CommitUrgency::Prompt)
        .expect("submit succeeds");
    ticket.wait().await.expect("command commits");
    assert_eq!(after_restart.epoch, 2);
    assert!(after_restart.proposed_at_micros > last_before_crash.proposed_at_micros);
    assert_eq!(second.committed_count("crash-flow").await, committed + 1);
    second.node.shutdown().await.expect("lease released");
}

#[tokio::test(start_paused = true)]
async fn restart_with_the_same_rocksdb_store_applies_only_new_batches() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let mut cluster = TestCluster::new();
    let config = cluster.config(LocalStoreConfig::RocksDb {
        directory: directory.path().join("store"),
        options: RocksDbStoreOptions::default(),
    });
    let first = cluster
        .start(
            &config,
            Arc::new(CountingApplier::default()),
            Arc::new(RecordingRuntime::default()),
        )
        .await
        .expect("node starts");
    first.commit_prompt("rocks-flow", 4).await;
    first.node.shutdown().await.expect("lease released");

    let second = cluster
        .start(
            &config,
            Arc::new(CountingApplier::default()),
            Arc::new(RecordingRuntime::default()),
        )
        .await
        .expect("node restarts");
    assert!(second.applier.applied_commands().is_empty());
    assert_eq!(second.committed_count("rocks-flow").await, 4);
    second.commit_prompt("rocks-flow", 1).await;
    assert_eq!(second.committed_count("rocks-flow").await, 5);
    second.node.shutdown().await.expect("lease released");
}

#[tokio::test(start_paused = true)]
async fn an_unreadable_rocksdb_store_is_set_aside_and_rebuilt() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store_directory = directory.path().join("store");
    let mut cluster = TestCluster::new();
    let config = cluster.config(LocalStoreConfig::RocksDb {
        directory: store_directory.clone(),
        options: RocksDbStoreOptions::default(),
    });
    let first = cluster
        .start(
            &config,
            Arc::new(CountingApplier::default()),
            Arc::new(RecordingRuntime::default()),
        )
        .await
        .expect("node starts");
    first.commit_prompt("corrupt-flow", 2).await;
    first.node.shutdown().await.expect("lease released");
    std::fs::write(store_directory.join("CURRENT"), b"not a manifest name\n")
        .expect("corrupt the store");

    let second = cluster
        .start(
            &config,
            Arc::new(CountingApplier::default()),
            Arc::new(RecordingRuntime::default()),
        )
        .await
        .expect("node restarts");
    assert_eq!(second.committed_count("corrupt-flow").await, 2);
    let set_aside = std::fs::read_dir(directory.path())
        .expect("list directory")
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().contains(".discarded-"))
        .count();
    assert_eq!(set_aside, 1);
    second.node.shutdown().await.expect("lease released");
}

#[tokio::test(start_paused = true)]
async fn a_halted_partition_stays_halted_after_restart() {
    let mut cluster = TestCluster::new();
    let first = cluster.start_in_memory().await;
    let healthy_flow = first.flow_in_other_partition(&[FAILING_FLOW]);
    let failing_partition = first.context(FAILING_FLOW).partition;
    let (_, ticket) = first
        .submit(FAILING_FLOW, CommitUrgency::Prompt)
        .expect("submit succeeds");
    assert!(ticket.wait().await.is_err());
    first.commit_prompt(&healthy_flow, 1).await;
    first.node.shutdown().await.expect("lease released");

    let second = cluster.start_in_memory().await;
    assert_eq!(second.node.halted_partitions(), vec![failing_partition]);
    assert!(
        !second
            .runtime
            .rebuilt_partitions()
            .contains(&failing_partition)
    );
    assert_eq!(second.committed_count(&healthy_flow).await, 1);
    second.commit_prompt(&healthy_flow, 1).await;
    second.node.shutdown().await.expect("lease released");
}

#[tokio::test(start_paused = true)]
async fn a_failed_runtime_rebuild_fails_startup() {
    let mut cluster = TestCluster::new();
    let config = cluster.config(LocalStoreConfig::InMemory);
    let result = cluster
        .start(
            &config,
            Arc::new(CountingApplier::default()),
            Arc::new(RecordingRuntime::failing_rebuild_of(PartitionId(1))),
        )
        .await;
    assert!(matches!(result, Err(EngineStartError::RuntimeRebuild(_))));
    // The failed start released its lease, so a fail-fast successor takes it at once.
    let mut successor_config = config.clone();
    successor_config.lease.fail_fast = true;
    let successor = cluster
        .start_with(&successor_config)
        .await
        .expect("the successor takes the released lease");
    successor.node.shutdown().await.expect("lease released");
}

/// Runs in real time: paused time does not advance while the blocked apply runs.
#[tokio::test]
async fn shutdown_releases_the_lease_when_an_applier_lane_is_stuck() {
    let mut cluster = TestCluster::new();
    let mut config = cluster.config(LocalStoreConfig::InMemory);
    let first = cluster.start_with(&config).await.expect("node starts");
    let (_, _ticket) = first
        .submit(BLOCKING_FLOW, CommitUrgency::Prompt)
        .expect("submit succeeds");
    first.applier.gate.wait_until_entered().await;
    let shutdown = tokio::time::timeout(Duration::from_secs(10), first.node.shutdown()).await;
    config.lease.fail_fast = true;
    let successor_applier = Arc::new(CountingApplier::default());
    successor_applier.gate.open();
    let successor = cluster
        .start(
            &config,
            successor_applier,
            Arc::new(RecordingRuntime::default()),
        )
        .await;
    // Open before asserting, so a failure does not leave the stuck lane blocked.
    first.applier.gate.open();
    assert!(
        matches!(shutdown, Ok(Ok(()))),
        "shutdown abandons the stuck applier lane and releases the lease"
    );
    let successor = successor.expect("the successor takes the released lease");
    successor.node.shutdown().await.expect("lease released");
}

#[tokio::test(start_paused = true)]
async fn a_newer_version_barrier_fails_startup() {
    let mut cluster = TestCluster::new();
    let first = cluster.start_in_memory().await;
    first.node.shutdown().await.expect("lease released");
    raise_version_barrier_as_a_newer_binary(&cluster, 99).await;
    let config = cluster.config(LocalStoreConfig::InMemory);
    let result = cluster
        .start(
            &config,
            Arc::new(CountingApplier::default()),
            Arc::new(RecordingRuntime::default()),
        )
        .await;
    assert!(matches!(
        result,
        Err(EngineStartError::UnsupportedBehaviorVersion { barrier: 99, .. })
    ));
}

#[tokio::test(start_paused = true)]
async fn a_newer_epoch_in_the_log_fences_the_node() {
    let mut cluster = TestCluster::new();
    let test = cluster.start_in_memory().await;
    let committed = {
        let (_, ticket) = test
            .submit("fenced-flow", CommitUrgency::Prompt)
            .expect("submit succeeds");
        ticket.wait().await.expect("command commits")
    };
    let cluster_object = dexzd_cluster_metadata::bootstrap_or_validate(
        cluster.object_store.as_ref(),
        cluster.clock.as_ref(),
        dexzd_cluster_metadata::ClusterConfig {
            partition_count: PARTITION_COUNT,
        },
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
        node_id: "node-a".to_owned(),
        incarnation: 1,
    };
    let next_seq = committed.position.seq + 1;
    let newer_batch = encode_batch(&BatchContent {
        stream: stream.clone(),
        epoch: 99,
        seq: next_seq,
        sealed_at_unix_micros: 0,
        records: vec![BatchRecord {
            partition: 0,
            bytes: Bytes::from_static(b"newer writer"),
        }],
        epoch_start_writer: None,
    })
    .expect("encode batch");
    cluster
        .object_store
        .put(
            &stream.batch_key(next_seq),
            newer_batch,
            WriteCondition::CreateOnly,
        )
        .await
        .expect("put succeeds");
    let (_, ticket) = test
        .submit("fenced-flow", CommitUrgency::Prompt)
        .expect("submit succeeds");
    assert!(matches!(
        ticket.wait().await,
        Err(CommitError::NodeStopping { .. })
    ));
    assert_eq!(test.node.wait_for_stop().await, NodeStopReason::Fenced);
    test.node.shutdown().await.expect("lease released");
}

#[tokio::test(start_paused = true)]
async fn submissions_during_recovery_are_unavailable() {
    let cluster = TestCluster::new();
    let config = cluster.config(LocalStoreConfig::InMemory);
    let runtime = Arc::new(RecordingRuntime::default());
    let factory_runtime = runtime.clone();
    let mut rejection = None;
    let node = EngineNode::start(
        &config,
        EngineNodeDependencies {
            object_store: cluster.object_store.clone(),
            clock: cluster.clock.clone(),
            applier: Arc::new(CountingApplier::default()),
            id_source: Arc::new(dexzd_engine::SystemEngineIdSource),
        },
        |services: EngineServices| {
            let flow_hash = services
                .partition_hasher
                .flow_hash(dexzd_keyspace::TenantId::DEFAULT, "early-flow");
            let context = dexzd_apply::ApplyContext {
                partition: services.partition_hasher.partition_of(flow_hash),
                flow_hash,
            };
            rejection = services
                .committer
                .submit(
                    context,
                    crate::harness::flow_command("early-flow"),
                    CommitUrgency::Prompt,
                )
                .err();
            EngineRuntimeHooks {
                committed_commands: factory_runtime.clone(),
                rebuilder: factory_runtime,
            }
        },
    )
    .await
    .expect("node starts");
    assert!(
        matches!(&rejection, Some(CommitError::Unavailable { detail }) if detail.contains("recovering")),
        "{rejection:?}"
    );
    node.shutdown().await.expect("lease released");
}

#[tokio::test(start_paused = true)]
async fn replayed_frames_finish_after_their_last_command() {
    let mut cluster = TestCluster::new();
    let first = cluster.start_in_memory().await;
    let tickets: Vec<_> = (0..3)
        .map(|_| {
            first
                .submit("replay-frame-flow", CommitUrgency::Batched)
                .expect("submit succeeds")
                .1
        })
        .collect();
    for ticket in tickets {
        ticket.wait().await.expect("command commits");
    }
    first.commit_prompt("replay-frame-flow", 2).await;
    drop(first);

    let second = cluster.start_in_memory().await;
    let partition = second.context("replay-frame-flow").partition;
    let replayed_events = second.runtime.sink_events_of(partition);
    assert_eq!(
        replayed_events
            .iter()
            .filter(|event| matches!(event, SinkEvent::Applied(_)))
            .count(),
        5
    );
    assert_eq!(
        assert_frames_finish_after_their_commands(&replayed_events),
        3
    );
    second.node.shutdown().await.expect("lease released");
}

#[tokio::test(start_paused = true)]
async fn voided_batches_after_takeover_keep_submission_order() {
    let mut cluster = TestCluster::new();
    let first = cluster.start_in_memory().await;
    first.commit_prompt("void-flow", 1).await;
    let cluster_object = dexzd_cluster_metadata::bootstrap_or_validate(
        cluster.object_store.as_ref(),
        cluster.clock.as_ref(),
        dexzd_cluster_metadata::ClusterConfig {
            partition_count: PARTITION_COUNT,
        },
    )
    .await
    .expect("cluster exists");
    drop(first);
    let stream = StreamIdentity {
        cluster_id: cluster_object
            .cluster
            .cluster_id
            .as_slice()
            .try_into()
            .expect("16-byte cluster ID"),
        node_id: "node-a".to_owned(),
        incarnation: 1,
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
    let second = cluster.start_in_memory().await;
    let submitted: Vec<_> = (0..4)
        .map(|_| {
            second
                .submit("void-flow", CommitUrgency::Prompt)
                .expect("submit succeeds")
        })
        .collect();
    let mut submitted_ids = Vec::new();
    for (command, ticket) in submitted {
        submitted_ids.push(command.command_id);
        ticket.wait().await.expect("command commits");
    }
    assert_eq!(second.committed_count("void-flow").await, 5);
    // Shutdown joins the applier lanes, which hand each command to the runtime after its ticket.
    second.node.shutdown().await.expect("lease released");
    let committed_ids: Vec<Vec<u8>> = second
        .runtime
        .committed_commands()
        .into_iter()
        .filter(|committed| committed.command.epoch == 2)
        .map(|committed| committed.command.command_id)
        .collect();
    assert_eq!(committed_ids, submitted_ids);
}
