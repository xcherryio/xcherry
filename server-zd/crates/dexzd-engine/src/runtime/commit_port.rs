// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! The boundary between the Flow runtime and the commit pipeline.

use dexzd_apply::{ApplyContext, ApplyOutcome, FlowStateReader};
use dexzd_protocol::dexzd::v1::{FlowCommand, LogPosition};
use tokio::sync::oneshot;

/// How promptly the commit pipeline should seal the batch that holds a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitUrgency {
    /// SYNC Step events, API writes, cross-Flow messages, Flow close: seal promptly.
    Prompt,
    /// ASYNC Step events: may wait for the batch window.
    Batched,
}

/// A command that is committed and applied to the local store.
#[derive(Debug, Clone, PartialEq)]
pub struct CommittedCommand {
    /// Apply outcome on committed state; byte-identical to the speculative outcome.
    pub outcome: ApplyOutcome,
    /// Log batch that holds the command.
    pub position: LogPosition,
}

/// A committed command together with the Flow it belongs to, as published by the commit
/// pipeline to `FlowRuntimeRegistry::apply_committed` in log order per partition.
#[derive(Debug, Clone, PartialEq)]
pub struct CommittedFlowCommand {
    /// The stamped command; carries tenant ID, Flow ID, and command ID.
    pub command: FlowCommand,
    /// Where the command was applied.
    pub context: ApplyContext,
    /// Its committed outcome and log position.
    pub committed: CommittedCommand,
}

/// Why a command could not be committed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CommitError {
    /// Backpressure or an object storage outage; the caller may retry (`Unavailable`).
    #[error("commit pipeline is unavailable: {detail}")]
    Unavailable {
        /// What is unavailable.
        detail: String,
    },
    /// The pipeline is fenced or failed; the node stops serving and recovers.
    #[error("node is stopping: {detail}")]
    NodeStopping {
        /// Why the node stops.
        detail: String,
    },
}

/// Resolves once a submitted command is committed and applied to the local store.
#[derive(Debug)]
pub struct CommitTicket {
    receiver: oneshot::Receiver<Result<CommittedCommand, CommitError>>,
}

/// Producer side of a [`CommitTicket`], held by the commit pipeline.
#[derive(Debug)]
pub struct CommitTicketSender {
    sender: oneshot::Sender<Result<CommittedCommand, CommitError>>,
}

impl CommitTicket {
    /// Creates a connected sender and ticket.
    pub fn new_pair() -> (CommitTicketSender, CommitTicket) {
        let (sender, receiver) = oneshot::channel();
        (CommitTicketSender { sender }, CommitTicket { receiver })
    }

    /// Waits for the commit result.
    ///
    /// Returns the committed command, the pipeline's error, or
    /// [`CommitError::NodeStopping`] when the pipeline dropped the sender without resolving it.
    pub async fn wait(self) -> Result<CommittedCommand, CommitError> {
        match self.receiver.await {
            Ok(result) => result,
            Err(_) => Err(CommitError::NodeStopping {
                detail: "commit pipeline dropped the commit ticket".to_owned(),
            }),
        }
    }
}

impl CommitTicketSender {
    /// Resolves the ticket. A ticket whose waiter is gone is resolved silently.
    pub fn resolve(self, result: Result<CommittedCommand, CommitError>) {
        // Nobody waits; the outcome stream still carries the command.
        if self.sender.send(result).is_err() {
            tracing::trace!("commit ticket waiter is gone");
        }
    }
}

/// The commit pipeline as the runtime uses it. `CommandCommitter` implements it; tests inject
/// fakes.
///
/// Lock contract: the runtime calls [`Self::submit`] and [`Self::committed_view`] while it holds
/// the Flow's lock, and `FlowRuntimeRegistry::apply_committed` takes that lock. Therefore both
/// methods must return without blocking or waiting for backpressure, and must not take any lock
/// that the pipeline holds while it calls `apply_committed`. The pipeline calls
/// `apply_committed` only after the command's writes are in the local store, never from inside
/// `submit`.
pub trait FlowCommandCommitter: Send + Sync + 'static {
    /// Stamps the command (epoch, behavior version, command ID, proposed_at_micros) and
    /// enqueues it in the commit pipeline in one step, so log order equals stamp order within
    /// a partition.
    ///
    /// Returns the stamped command, which the runtime applies to its speculative overlay, and
    /// a ticket that resolves once the command is committed and applied to the local store.
    /// Returns [`CommitError`] when nothing was enqueued.
    fn submit(
        &self,
        context: ApplyContext,
        command: FlowCommand,
        urgency: CommitUrgency,
    ) -> Result<(FlowCommand, CommitTicket), CommitError>;

    /// Returns a consistent read view of committed state (a local store snapshot).
    ///
    /// Returns [`CommitError`] when the local store cannot open a view.
    fn committed_view(&self) -> Result<Box<dyn FlowStateReader + Send>, CommitError>;
}
