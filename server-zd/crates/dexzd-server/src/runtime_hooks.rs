// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;

use dexzd_engine::{CommittedCommandSink, CommittedFlowCommand, FlowRuntimeRegistry};
use dexzd_keyspace::PartitionId;
use dexzd_protocol::dexzd::v1::LogPosition;

use crate::server_node::CommittedSearchFrames;

/// Forwards every committed command from the commit pipeline to the Flow runtime registry.
pub(crate) struct RegistryCommittedCommandSink {
    registry: FlowRuntimeRegistry,
    /// Receives search document changes first, so read-your-writes waits cover a woken waiter's frame.
    search_frames: Option<Arc<CommittedSearchFrames>>,
}

impl RegistryCommittedCommandSink {
    pub(crate) fn new(
        registry: FlowRuntimeRegistry,
        search_frames: Option<Arc<CommittedSearchFrames>>,
    ) -> Self {
        Self {
            registry,
            search_frames,
        }
    }
}

impl CommittedCommandSink for RegistryCommittedCommandSink {
    fn apply_committed(&self, committed: CommittedFlowCommand) {
        if let Some(search_frames) = &self.search_frames {
            search_frames.record_command(&committed);
        }
        self.registry.apply_committed(committed);
    }

    fn finish_frame(&self, partition: PartitionId, position: &LogPosition) {
        if let Some(search_frames) = &self.search_frames {
            search_frames.finish_frame(partition, position);
        }
    }
}
