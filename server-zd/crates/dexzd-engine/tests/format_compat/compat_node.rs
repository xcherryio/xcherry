// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;
use std::time::Duration;

use dexzd_cluster_metadata::{ClusterConfig, LeaseConfig, NodeConfig};
use dexzd_engine::{
    BlockingWorkPlacement, CommitConfig, CommittedCommandSink, CommittedFlowCommand,
    DeterministicCommandApplier, EngineIdSource, EngineNode, EngineNodeConfig,
    EngineNodeDependencies, EngineRuntimeHooks, LocalStoreConfig, PartitionRuntimeRebuilder,
    RecoveryConfig, RuntimeRebuildError, SnapshotConfig,
};
use dexzd_keyspace::PartitionId;
use dexzd_object_store::InMemoryObjectStore;
use futures::future::BoxFuture;

use crate::scripted_clock::ScriptedClock;

/// Longer than every scripted clock jump, so the lease never looks expired.
const LEASE_DURATION: Duration = Duration::from_secs(30 * 24 * 60 * 60);
/// Snapshots happen only when the workload takes one.
const NEVER: Duration = Duration::from_secs(100 * 365 * 24 * 60 * 60);

/// Runtime hooks that run no effects, so only scripted commands reach the log.
pub(crate) struct PassiveRuntime;

impl CommittedCommandSink for PassiveRuntime {
    fn apply_committed(&self, _committed: CommittedFlowCommand) {}
}

impl PartitionRuntimeRebuilder for PassiveRuntime {
    fn rebuild_partition(
        &self,
        _partition: PartitionId,
    ) -> BoxFuture<'_, Result<(), RuntimeRebuildError>> {
        Box::pin(async { Ok(()) })
    }
}

pub(crate) fn engine_node_config(
    node_id: &str,
    incarnation: u32,
    partition_count: u32,
) -> EngineNodeConfig {
    EngineNodeConfig {
        node: NodeConfig {
            node_id: node_id.to_owned(),
            incarnation,
            internal_address: "127.0.0.1:9000".to_owned(),
        },
        cluster: ClusterConfig { partition_count },
        lease: LeaseConfig {
            duration: LEASE_DURATION,
            ..LeaseConfig::default()
        },
        local_store: LocalStoreConfig::InMemory,
        commit: CommitConfig {
            applier_lanes: 2,
            debug_replay_checks: true,
            ..CommitConfig::default()
        },
        snapshots: SnapshotConfig {
            interval: NEVER,
            log_threshold_bytes: u64::MAX,
            ..SnapshotConfig::default()
        },
        recovery: RecoveryConfig::default(),
        blocking_work: BlockingWorkPlacement::BlockingThreadPool,
    }
}

/// Starts a node with the deterministic applier and passive runtime hooks.
pub(crate) async fn start_engine_node(
    config: &EngineNodeConfig,
    object_store: Arc<InMemoryObjectStore>,
    clock: Arc<ScriptedClock>,
    id_source: Arc<dyn EngineIdSource>,
) -> EngineNode {
    EngineNode::start(
        config,
        EngineNodeDependencies {
            object_store,
            clock,
            applier: Arc::new(DeterministicCommandApplier),
            id_source,
        },
        |_services| EngineRuntimeHooks {
            committed_commands: Arc::new(PassiveRuntime),
            rebuilder: Arc::new(PassiveRuntime),
        },
    )
    .await
    .expect("the node starts")
}
