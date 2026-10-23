// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_engine::{CommittedCommandSink, CommittedFlowCommand, FlowRuntimeRegistry};

/// Forwards every committed command from the commit pipeline to the Flow runtime registry.
pub(crate) struct RegistryCommittedCommandSink {
    registry: FlowRuntimeRegistry,
}

impl RegistryCommittedCommandSink {
    pub(crate) fn new(registry: FlowRuntimeRegistry) -> Self {
        Self { registry }
    }
}

impl CommittedCommandSink for RegistryCommittedCommandSink {
    fn apply_committed(&self, committed: CommittedFlowCommand) {
        self.registry.apply_committed(committed);
    }
}
