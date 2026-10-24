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
mod long_poll;
mod recovery;
mod runtime;
mod snapshots;

pub use command_applier::{CommandApplier, DeterministicCommandApplier};
pub use commit::{
    CommandCommitter, CommitConfig, CommittedStateView, EngineLocalStore, NodeStopReason,
    ReplayCheck, ReplayMismatch,
};
pub use engine_node::{EngineNode, EngineNodeConfig, EngineNodeDependencies};
pub use recovery::{
    CommittedCommandSink, EngineRuntimeHooks, EngineServices, EngineStartError, LocalStoreConfig,
    PartitionRuntimeRebuilder, RuntimeRebuildError,
};
pub use runtime::ReplayMismatch as SpeculativeOutcomeMismatch;
pub use runtime::{
    CommitError, CommitTicket, CommitTicketSender, CommitUrgency, CommittedCommand,
    CommittedFlowCommand, FlowAddress, FlowCommandCommitter, FlowRuntimeDependencies,
    FlowRuntimeRegistry, HardDeleteReadError, HardDeleteValueReader, PendingInvocation,
    ProposalResult, RpcInvokeError, RpcWorkerInvoker, RuntimeConfig, RuntimeEffectSink,
    RuntimeError, WorkerRpcInvocation, WorkerRpcResult, default_run_settings, grpc_code,
};
pub use snapshots::{SnapshotConfig, SnapshotError};
