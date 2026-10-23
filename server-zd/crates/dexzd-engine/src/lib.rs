// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Runtime that proposes, commits, and applies server-zd commands and runs their effects: dispatch, timers, deliveries, and recovery.

mod command_applier;
mod commit;
mod engine_node;
mod recovery;
mod snapshots;

pub use command_applier::{CommandApplier, DeterministicCommandApplier};
pub use commit::{
    CommandCommitter, CommitConfig, CommitError, CommitTicket, CommitUrgency, CommittedCommand,
    CommittedOutcome, CommittedOutcomeReceiver, CommittedStateView, EngineLocalStore,
    NodeStopReason, ReplayCheck, ReplayMismatch,
};
pub use engine_node::{EngineNode, EngineNodeConfig, EngineNodeDependencies};
pub use recovery::{
    EngineServices, EngineStartError, LocalStoreConfig, PartitionRuntimeRebuilder,
    RuntimeRebuildError,
};
pub use snapshots::{SnapshotConfig, SnapshotError};
