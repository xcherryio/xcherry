// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_keyspace::{PartitionHasher, PartitionId};
use futures::future::BoxFuture;

use crate::commit::{CommandCommitter, CommittedOutcomeReceiver};

/// Engine services handed to the runtime factory before the log is replayed.
pub struct EngineServices {
    /// Submits commands. It returns [`crate::CommitError::NotServing`] until recovery ends.
    pub committer: CommandCommitter,
    /// Every committed outcome, including replayed ones. Keep receiving during recovery.
    pub committed_outcomes: CommittedOutcomeReceiver,
    /// Computes Flow hashes and partitions with the cluster's key.
    pub partition_hasher: PartitionHasher,
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
