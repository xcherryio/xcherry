// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Runtime that proposes, commits, and applies server-zd commands and runs their effects: dispatch, timers, deliveries, and recovery.

mod blocking_work;
mod command_applier;
mod commit;
mod effects;
mod engine_id_source;
mod engine_node;
mod long_poll;
mod recovery;
mod runtime;
mod snapshots;

pub use blocking_work::BlockingWorkPlacement;
pub use command_applier::{CommandApplier, DeterministicCommandApplier};
pub use commit::{
    CommandCommitter, CommitConfig, CommittedStateView, EngineLocalStore, NodeStopReason,
    ReplayCheck, ReplayMismatch,
};
pub use effects::{
    EffectDependencies, EffectError, EffectRunner, EffectRuntime, EffectRuntimeError, EffectTasks,
    EffectTasksHandle, EffectsConfig, HARD_DELETE_OBJECT_KEY_PREFIX, HardDeleteObjectOwner,
    HardDeleteObjects, HardDeleteUploadLedger, HardDeleteUploads, InvocationStartOutcome,
    MethodDispatchRequest, MethodDispatchSession, MethodDispatchStartError, MethodDispatcher,
    MethodRequest, NodeMetaUploadLedger, decode_hard_delete_value, encode_hard_delete_value,
    hard_delete_placeholder_value,
};
#[cfg(feature = "test-util")]
pub use engine_id_source::SeededEngineIdSourceForTest;
pub use engine_id_source::{EngineIdSource, SystemEngineIdSource};
pub use engine_node::{EngineNode, EngineNodeConfig, EngineNodeDependencies};
pub use long_poll::{
    ATTRIBUTE_REQUEST_TIMEOUT_MESSAGE, STEP_COMPLETION_REQUEST_TIMEOUT_MESSAGE,
    WAIT_CALL_DEADLINE_MESSAGE, WAIT_FOR_FLOW_TIMEOUT_MESSAGE,
    WAIT_FOR_HISTORY_EVENT_TIMEOUT_MESSAGE,
};
pub use recovery::{
    CommittedCommandSink, EngineRuntimeHooks, EngineServices, EngineStartError, LocalStoreConfig,
    PartitionRuntimeRebuilder, RecoveryConfig, RuntimeRebuildError,
};
pub use runtime::HardDeleteUploads as RuntimeHardDeleteUploads;
pub use runtime::ReplayMismatch as SpeculativeOutcomeMismatch;
pub use runtime::{
    CommitError, CommitTicket, CommitTicketSender, CommitUrgency, CommittedCommand,
    CommittedFlowCommand, FlowAddress, FlowCommandCommitter, FlowRuntimeDependencies,
    FlowRuntimeRegistry, HardDeleteReadError, HardDeleteUploadError, HardDeleteUploadOutcome,
    HardDeleteValueReader, HardDeleteValueUploader, PendingInvocation, ProposalResult,
    RpcInvokeError, RpcWorkerInvoker, RuntimeConfig, RuntimeEffectSink, RuntimeError,
    WorkerRpcInvocation, WorkerRpcResult, default_run_settings, grpc_code,
};
pub use snapshots::{SnapshotConfig, SnapshotError};
