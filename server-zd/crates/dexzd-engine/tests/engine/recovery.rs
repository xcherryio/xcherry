// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;

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
    CountingApplier, FAILING_FLOW, PARTITION_COUNT, RecordingRuntime, TestCluster,
};

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
    second.node.shutdown().await;
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
    first.node.shutdown().await;

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
    second.node.shutdown().await;
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
    first.node.shutdown().await;
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
    second.node.shutdown().await;
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
    first.node.shutdown().await;

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
    second.node.shutdown().await;
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
}

#[tokio::test(start_paused = true)]
async fn a_newer_version_barrier_fails_startup() {
    let mut cluster = TestCluster::new();
    let first = cluster.start_in_memory().await;
    first.node.shutdown().await;
    dexzd_cluster_metadata::raise_version_barrier(
        cluster.object_store.as_ref(),
        cluster.clock.as_ref(),
        99,
        1,
    )
    .await
    .expect("barrier raised");
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
    test.node.shutdown().await;
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
    node.shutdown().await;
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
    let committed_ids: Vec<Vec<u8>> = second
        .runtime
        .committed_commands()
        .into_iter()
        .filter(|committed| committed.command.epoch == 2)
        .map(|committed| committed.command.command_id)
        .collect();
    assert_eq!(committed_ids, submitted_ids);
    second.node.shutdown().await;
}
