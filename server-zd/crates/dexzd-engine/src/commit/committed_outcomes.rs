// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;

use dexzd_keyspace::{FlowHash, PartitionId, TenantId};
use tokio::sync::mpsc;

use crate::commit::CommittedCommand;

/// One committed and applied command, published in log order per partition.
#[derive(Debug, Clone)]
pub struct CommittedOutcome {
    /// Partition of the command's Flow.
    pub partition: PartitionId,
    /// Tenant of the command's Flow.
    pub tenant_id: TenantId,
    /// Flow ID of the command.
    pub flow_id: String,
    /// Keyed hash of the command's (tenant ID, Flow ID).
    pub flow_hash: FlowHash,
    /// Command ID stamped by [`crate::CommandCommitter::submit`].
    pub command_id: Vec<u8>,
    /// Outcome and log position.
    pub committed: Arc<CommittedCommand>,
    /// True when a previous process proposed the command and recovery replayed it. Effects
    /// of replayed commands are rebuilt from state instead.
    pub is_replayed: bool,
}

/// Receives every committed outcome; the runtime registry is the only consumer.
///
/// Appliers wait when the buffer is full, so the consumer must keep receiving, including
/// while the node recovers.
#[derive(Debug)]
pub struct CommittedOutcomeReceiver {
    receiver: mpsc::Receiver<CommittedOutcome>,
}

impl CommittedOutcomeReceiver {
    pub(crate) fn new(receiver: mpsc::Receiver<CommittedOutcome>) -> Self {
        Self { receiver }
    }

    /// Returns the next committed outcome, or `None` after the node stopped.
    pub async fn recv(&mut self) -> Option<CommittedOutcome> {
        self.receiver.recv().await
    }
}
