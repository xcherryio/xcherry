// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;

use dexzd_keyspace::{PartitionHasher, PartitionId};
use futures::future::BoxFuture;

use crate::commit::CommandCommitter;
use crate::runtime::CommittedFlowCommand;

/// Engine services handed to the runtime factory before the log is replayed.
pub struct EngineServices {
    /// Submits commands. It returns [`crate::CommitError::Unavailable`] until recovery ends.
    pub committer: CommandCommitter,
    /// Computes Flow hashes and partitions with the cluster's key.
    pub partition_hasher: PartitionHasher,
}

/// What the runtime factory returns: where committed commands go and how partitions rebuild.
pub struct EngineRuntimeHooks {
    /// Receives every committed command, including commands replayed during recovery.
    pub committed_commands: Arc<dyn CommittedCommandSink>,
    /// Rebuilds each partition's runtime work after recovery.
    pub rebuilder: Arc<dyn PartitionRuntimeRebuilder>,
}

/// Receives committed commands in log order per partition.
///
/// The Flow runtime registry implements it to drop overlay entries, run effects, and wake
/// long-poll waiters.
pub trait CommittedCommandSink: Send + Sync + 'static {
    /// Handles one committed and applied command.
    ///
    /// Partition appliers call it on their threads, in log order per partition, after the
    /// command's state is in the local store. It must not block. During recovery it also
    /// receives commands that a previous process proposed.
    fn apply_committed(&self, committed: CommittedFlowCommand);
}

/// Rebuilds a partition's runtime work from committed state after recovery.
///
/// The engine calls it once per running partition after replaying the log and before
/// serving, so pending invocations, timers, outbox deliveries, and deletions resume.
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
