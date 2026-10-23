// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! What the effect runner needs from the Flow runtime: proposals, lock-checked attempt starts,
//! and state reads. The runtime registry implements [`EffectRuntime`].

use async_trait::async_trait;
use dexzd_apply::FlowStateReader;
use dexzd_keyspace::{RunId, TenantId};
use dexzd_protocol::dexzd::v1::{CommandReply, FlowCommand, StepInvocationKey};

/// Identifies a Flow by tenant and Flow ID.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EffectFlow {
    /// Tenant of the Flow.
    pub tenant_id: TenantId,
    /// The Flow ID.
    pub flow_id: String,
}

/// How promptly a proposal must be sealed into a log batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectUrgency {
    /// SYNC Step events, cross-Flow messages, and owner maintenance commands.
    Prompt,
    /// ASYNC Step events and heartbeats.
    Batched,
}

/// A proposal or read the runtime could not serve.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EffectRuntimeError {
    /// Backpressure or an object storage outage; retry later.
    #[error("runtime is unavailable: {detail}")]
    Unavailable {
        /// What was unavailable.
        detail: String,
    },
    /// The node is stopping; effect work ends.
    #[error("node is stopping: {detail}")]
    NodeStopping {
        /// Why the node stops.
        detail: String,
    },
}

/// Result of a lock-checked attempt start.
#[derive(Debug, Clone, PartialEq)]
pub enum InvocationStartOutcome {
    /// `StepAttemptStarted` was proposed; its `InvocationStarted` effect follows.
    Started,
    /// A lock key is held, or an earlier waiter queues on it; retry after locks are released.
    LocksBusy,
    /// The invocation is no longer pending with this attempt.
    Stale,
}

/// Flow runtime operations used by the effect runner.
#[async_trait]
pub trait EffectRuntime: Send + Sync + 'static {
    /// Proposes `command` to `flow` and waits until it is committed and applied, returning
    /// apply's reply on committed state.
    async fn propose_committed(
        &self,
        flow: &EffectFlow,
        command: FlowCommand,
        urgency: EffectUrgency,
    ) -> Result<CommandReply, EffectRuntimeError>;

    /// Proposes `command` to `flow` and returns once it is applied to the speculative overlay.
    async fn propose_speculative(
        &self,
        flow: &EffectFlow,
        command: FlowCommand,
        urgency: EffectUrgency,
    ) -> Result<CommandReply, EffectRuntimeError>;

    /// Inside the Flow's actor: proposes `StepAttemptStarted` when every lock key of the
    /// invocation is free in the overlay lock table and the in-memory RPC locks, and no earlier
    /// waiter queues on any of its keys.
    async fn try_start_invocation(
        &self,
        flow: &EffectFlow,
        run_id: RunId,
        run_attempt: u32,
        invocation: &StepInvocationKey,
        attempt: u32,
        task_id: Vec<u8>,
    ) -> Result<InvocationStartOutcome, EffectRuntimeError>;

    /// Returns a read view of `flow`'s state, including uncommitted speculative commands when
    /// `include_speculative` is true.
    fn flow_state_reader(
        &self,
        flow: &EffectFlow,
        include_speculative: bool,
    ) -> Box<dyn FlowStateReader + Send>;

    /// Returns a consistent read view of committed state of every partition.
    fn committed_state_reader(&self) -> Box<dyn FlowStateReader + Send>;
}
