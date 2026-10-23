// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! What the effect runner needs from the Flow runtime: proposals, lock-checked attempt starts,
//! and state reads. The runtime registry implements [`EffectRuntime`].

use dexzd_apply::FlowStateReader;
use dexzd_keyspace::RunId;
use dexzd_protocol::dexzd::v1::{CommandReply, FlowCommand, StepInvocationKey};
use futures::future::BoxFuture;

use crate::runtime::{CommitError, CommitUrgency, FlowAddress};

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

impl From<CommitError> for EffectRuntimeError {
    fn from(error: CommitError) -> Self {
        match error {
            CommitError::Unavailable { detail } => Self::Unavailable { detail },
            CommitError::NodeStopping { detail } => Self::NodeStopping { detail },
        }
    }
}

/// Result of a lock-checked attempt start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvocationStartOutcome {
    /// `StepAttemptStarted` was proposed; its `InvocationStarted` effect follows.
    Started,
    /// A lock key is held, or an earlier waiter queues on it; retry after locks are released.
    LocksBusy,
    /// The invocation is no longer pending with this attempt.
    Stale,
}

/// Flow runtime operations used by the effect runner.
pub trait EffectRuntime: Send + Sync + 'static {
    /// Proposes `command` to `flow` and waits until it is committed and applied.
    ///
    /// Returns apply's reply on committed state, or [`EffectRuntimeError`] when the command was
    /// not committed.
    fn propose_committed<'a>(
        &'a self,
        flow: &'a FlowAddress,
        command: FlowCommand,
        urgency: CommitUrgency,
    ) -> BoxFuture<'a, Result<CommandReply, EffectRuntimeError>>;

    /// Proposes `command` to `flow` and returns once it is applied to the speculative overlay.
    ///
    /// Returns the speculative reply, or [`EffectRuntimeError`] when nothing was proposed.
    fn propose_speculative<'a>(
        &'a self,
        flow: &'a FlowAddress,
        command: FlowCommand,
        urgency: CommitUrgency,
    ) -> BoxFuture<'a, Result<CommandReply, EffectRuntimeError>>;

    /// Inside the Flow's actor: proposes `StepAttemptStarted` with `task_id` when every lock
    /// key of the invocation is free in the overlay lock table and the in-memory RPC locks, and
    /// no earlier waiter queues on any of its keys. The urgency follows the method's durability.
    ///
    /// Returns whether the attempt started, or [`EffectRuntimeError`] when nothing was proposed.
    fn try_start_invocation<'a>(
        &'a self,
        flow: &'a FlowAddress,
        run_id: RunId,
        run_attempt: u32,
        invocation: &'a StepInvocationKey,
        attempt: u32,
        task_id: Vec<u8>,
    ) -> BoxFuture<'a, Result<InvocationStartOutcome, EffectRuntimeError>>;

    /// Returns a read view of `flow`'s state, including uncommitted speculative commands when
    /// `include_speculative` is true.
    ///
    /// Returns [`EffectRuntimeError`] when the local store cannot open a view.
    fn flow_state_reader(
        &self,
        flow: &FlowAddress,
        include_speculative: bool,
    ) -> Result<Box<dyn FlowStateReader + Send>, EffectRuntimeError>;

    /// Returns a consistent read view of committed state of every partition.
    ///
    /// Returns [`EffectRuntimeError`] when the local store cannot open a view.
    fn committed_state_reader(&self)
    -> Result<Box<dyn FlowStateReader + Send>, EffectRuntimeError>;
}
