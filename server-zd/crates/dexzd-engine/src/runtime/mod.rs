// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Per-Flow runtime: speculative overlay, proposals, RPC locks, and client operations.

mod client_operations;
mod commit_port;
mod flow_address;
mod flow_runtime;
pub(crate) mod read_operations;
mod registry;
mod rpc_locks;
mod runtime_config;
mod runtime_error;
mod runtime_ports;
mod speculative_overlay;

#[cfg(test)]
mod tests;

pub use commit_port::{
    CommitError, CommitTicket, CommitTicketSender, CommitUrgency, CommittedCommand,
    CommittedFlowCommand, FlowCommandCommitter,
};
pub use flow_address::FlowAddress;
pub use registry::{FlowRuntimeDependencies, FlowRuntimeRegistry, ProposalResult};
pub use runtime_config::{RuntimeConfig, default_run_settings};
pub use runtime_error::{RuntimeError, grpc_code};
pub use runtime_ports::{
    HardDeleteReadError, HardDeleteValueReader, RpcInvokeError, RpcWorkerInvoker,
    RuntimeEffectSink, WorkerRpcInvocation, WorkerRpcResult,
};
