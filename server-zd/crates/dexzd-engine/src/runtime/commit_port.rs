// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! The boundary between the Flow runtime and the commit pipeline.

use std::sync::{Arc, Mutex, MutexGuard};

use dexzd_apply::{ApplyContext, ApplyOutcome, FlowStateReader};
use dexzd_keyspace::PartitionId;
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
    failure_watch: Arc<Mutex<CommitFailureWatch>>,
}

/// Producer side of a [`CommitTicket`], held by the commit pipeline.
///
/// Failing or dropping it runs the ticket's failure observer synchronously, even when nobody waits.
#[derive(Debug)]
pub struct CommitTicketSender {
    sender: Option<oneshot::Sender<Result<CommittedCommand, CommitError>>>,
    failure_watch: Arc<Mutex<CommitFailureWatch>>,
}

/// Runs once with the error of a failed commit.
pub(crate) type CommitFailureObserver = Box<dyn FnOnce(&CommitError) + Send>;

/// Hands a commit failure to the observer, whichever of the two arrives first.
#[derive(Default)]
struct CommitFailureWatch {
    observer: Option<CommitFailureObserver>,
    failure: Option<CommitError>,
}

impl std::fmt::Debug for CommitFailureWatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CommitFailureWatch")
            .field("has_observer", &self.observer.is_some())
            .field("failure", &self.failure)
            .finish()
    }
}

impl CommitTicket {
    /// Creates a connected sender and ticket.
    pub fn new_pair() -> (CommitTicketSender, CommitTicket) {
        let (sender, receiver) = oneshot::channel();
        let failure_watch = Arc::new(Mutex::new(CommitFailureWatch::default()));
        (
            CommitTicketSender {
                sender: Some(sender),
                failure_watch: Arc::clone(&failure_watch),
            },
            CommitTicket {
                receiver,
                failure_watch,
            },
        )
    }

    /// Waits for the commit result.
    ///
    /// Returns the committed command, the pipeline's error, or
    /// [`CommitError::NodeStopping`] when the pipeline dropped the sender without resolving it.
    pub async fn wait(self) -> Result<CommittedCommand, CommitError> {
        match self.receiver.await {
            Ok(result) => result,
            Err(_) => Err(dropped_ticket_error()),
        }
    }

    /// Runs `observer` once if the commit fails, now when it already failed. Survives dropping
    /// the ticket.
    pub(crate) fn observe_failure(&self, observer: CommitFailureObserver) {
        let mut watch = lock_failure_watch(&self.failure_watch);
        match watch.failure.take() {
            Some(failure) => {
                drop(watch);
                observer(&failure);
            }
            None => watch.observer = Some(observer),
        }
    }
}

impl CommitTicketSender {
    /// Resolves the ticket. A ticket whose waiter is gone is resolved silently.
    pub fn resolve(mut self, result: Result<CommittedCommand, CommitError>) {
        if let Err(error) = &result {
            self.report_failure(error.clone());
        }
        let sender = self
            .sender
            .take()
            .expect("present until resolved or dropped");
        // Nobody waits; the outcome stream still carries the command.
        if sender.send(result).is_err() {
            tracing::trace!("commit ticket waiter is gone");
        }
    }

    fn report_failure(&self, error: CommitError) {
        let mut watch = lock_failure_watch(&self.failure_watch);
        match watch.observer.take() {
            Some(observer) => {
                drop(watch);
                observer(&error);
            }
            None => watch.failure = Some(error),
        }
    }
}

impl Drop for CommitTicketSender {
    fn drop(&mut self) {
        if self.sender.is_some() {
            self.report_failure(dropped_ticket_error());
        }
    }
}

fn lock_failure_watch(watch: &Mutex<CommitFailureWatch>) -> MutexGuard<'_, CommitFailureWatch> {
    watch
        .lock()
        .expect("commit failure watch lock is poisoned only if a holder panicked")
}

fn dropped_ticket_error() -> CommitError {
    CommitError::NodeStopping {
        detail: "commit pipeline dropped the commit ticket".to_owned(),
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
///
/// Failing or dropping a ticket halts the runtime on that thread, before the partition commits more.
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

    /// Checks a speculative outcome against its committed outcome, for example in debug replay
    /// checks. The runtime calls it for every committed command it applied speculatively, after
    /// its own byte comparison passed, and halts on an error.
    ///
    /// Returns [`ReplayMismatch`] when the outcomes must be treated as different.
    fn check_speculative_outcome(
        &self,
        _partition: PartitionId,
        _command: &FlowCommand,
        _speculative: &ApplyOutcome,
        _committed: &CommittedCommand,
    ) -> Result<(), ReplayMismatch> {
        Ok(())
    }
}

/// A speculative outcome that differs from its committed outcome.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("speculative and committed outcomes differ: {detail}")]
pub struct ReplayMismatch {
    /// What differs.
    pub detail: String,
}
