// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::{Arc, Mutex};

use dexzd_keyspace::{PartitionHasher, PartitionId};
use dexzd_protocol::dexzd::v1::LogPosition;
use futures::future::BoxFuture;

use crate::commit::{CommandCommitter, EngineLocalStore};
use crate::engine_id_source::EngineIdSource;
use crate::runtime::CommittedFlowCommand;

/// Engine services handed to the runtime factory before the log is replayed.
pub struct EngineServices {
    /// Submits commands; returns [`crate::CommitError::Unavailable`] until the log is replayed.
    pub committer: CommandCommitter,
    /// Computes Flow hashes and partitions with the cluster's key.
    pub partition_hasher: PartitionHasher,
    /// Hold it around every read-modify-write of the local store's NodeMeta.
    pub node_meta_lock: Arc<Mutex<()>>,
    /// The local store, for runtime state kept in NodeMeta such as pending hard-delete uploads.
    pub local_store: Arc<EngineLocalStore>,
    /// Creates the IDs the runtime and effects persist; pass it to their dependencies.
    pub id_source: Arc<dyn EngineIdSource>,
}

/// What the runtime factory returns: where committed commands go and how partitions rebuild.
pub struct EngineRuntimeHooks {
    /// Receives every committed command, including commands replayed during recovery.
    pub committed_commands: Arc<dyn CommittedCommandSink>,
    /// Rebuilds each partition's runtime work after recovery.
    pub rebuilder: Arc<dyn PartitionRuntimeRebuilder>,
}

/// Receives committed commands in log order per partition; the Flow runtime registry implements it.
pub trait CommittedCommandSink: Send + Sync + 'static {
    /// Handles one applied command without blocking.
    ///
    /// Replayed commands arrive before rebuild with an older epoch; rebuild owns their effects.
    fn apply_committed(&self, committed: CommittedFlowCommand);

    /// Called once after the last [`Self::apply_committed`] of each applied frame, replayed frames included.
    fn finish_frame(&self, _partition: PartitionId, _position: &LogPosition) {}
}

/// Rebuilds a partition's runtime work from committed state; the node already accepts commands.
pub trait PartitionRuntimeRebuilder: Send + Sync + 'static {
    /// Rebuilds the runtime work of `partition`.
    fn rebuild_partition(
        &self,
        partition: PartitionId,
    ) -> BoxFuture<'_, Result<(), RuntimeRebuildError>>;
}

/// A partition's runtime could not be rebuilt; the node does not start.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("rebuilding the runtime of partition {} failed: {detail}", partition.0)]
pub struct RuntimeRebuildError {
    /// The partition.
    pub partition: PartitionId,
    /// What failed.
    pub detail: String,
}
