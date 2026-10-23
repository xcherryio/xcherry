// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Runtime that proposes, commits, and applies server-zd commands and runs their effects: dispatch, timers, deliveries, and recovery.

mod command_applier;
mod long_poll;
mod runtime;

pub use command_applier::CommandApplier;
pub use runtime::{
    CommitError, CommitTicket, CommitTicketSender, CommitUrgency, CommittedCommand,
    CommittedFlowCommand, FlowAddress, FlowCommandCommitter, FlowRuntimeDependencies,
    FlowRuntimeRegistry, HardDeleteReadError, HardDeleteValueReader, ProposalResult,
    RpcInvokeError, RpcWorkerInvoker, RuntimeConfig, RuntimeEffectSink, RuntimeError,
    WorkerRpcInvocation, WorkerRpcResult, default_run_settings, grpc_code,
};
