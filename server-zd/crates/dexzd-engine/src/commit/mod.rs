// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Command commit path: stamping, the commit pipeline, partition appliers, and committed views.

mod command_committer;
mod commit_config;
mod commit_state;
mod committed_state_view;
mod log_proposer;
mod partition_applier;
mod replay_checks;
mod staged_state_reader;

pub use command_committer::CommandCommitter;
pub use commit_config::CommitConfig;
pub use commit_state::NodeStopReason;
pub use committed_state_view::CommittedStateView;
pub use replay_checks::{ReplayCheck, ReplayMismatch};

pub(crate) use command_committer::CommandCommitterParts;
pub(crate) use commit_state::CommitState;
pub(crate) use log_proposer::{CommitProgress, CommittedBatchRouter, run_log_proposer};
pub(crate) use partition_applier::{FrameApplier, PartitionApplierPool, PartitionFrame};

/// Local store type the engine runs on: RocksDB in production, in memory in simulations.
pub type EngineLocalStore = dexzd_storage::RestoredLocalStore;
