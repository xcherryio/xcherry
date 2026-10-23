// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use dexzd_apply::{ApplyContext, ApplyOutcome};
use dexzd_clock::WallClock;
use dexzd_keyspace::PartitionId;
use dexzd_protocol::dexzd::v1::{FlowCommand, LogPosition, LogRecord, log_record};
use prost::Message;
use tokio::sync::{Semaphore, mpsc, oneshot};
use uuid::Uuid;

use crate::commit::EngineLocalStore;
use crate::commit::commit_state::{CommitResponse, CommitState, NodeStopReason, PendingCommand};
use crate::commit::committed_state_view::CommittedStateView;
use crate::commit::replay_checks::{ReplayMismatch, compare_speculative_outcome};

/// How soon a submitted command needs to be durable.
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

/// Why a command was not committed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CommitError {
    /// Too many submitted commands wait for apply. Retryable; clients see `Unavailable`.
    #[error("the commit backlog is full; retry later")]
    Backpressure,
    /// The node is still recovering. Retryable; clients see `Unavailable`.
    #[error("the node is recovering and does not accept commands yet")]
    NotServing,
    /// Apply failed for the partition, which no longer applies commands until the node
    /// restarts. Retryable on another node; clients see `Unavailable`.
    #[error("partition {} stopped applying commands", partition.0)]
    PartitionStopped {
        /// The stopped partition.
        partition: PartitionId,
    },
    /// The node stopped serving and exits; the command may or may not be committed.
    #[error("the node is stopping: {reason}")]
    NodeStopping {
        /// Why the node stopped.
        reason: NodeStopReason,
    },
}

/// Resolves once the submitted command is committed and applied to the local store.
#[derive(Debug)]
pub struct CommitTicket {
    command_id: Vec<u8>,
    receiver: oneshot::Receiver<CommitResponse>,
}

impl CommitTicket {
    /// Returns the command ID stamped on the command.
    pub fn command_id(&self) -> &[u8] {
        &self.command_id
    }

    /// Waits until the command is committed and applied.
    ///
    /// Returns the committed outcome, or [`CommitError::PartitionStopped`] or
    /// [`CommitError::NodeStopping`] when it will not be applied by this node.
    pub async fn wait(self) -> Result<Arc<CommittedCommand>, CommitError> {
        self.receiver.await.unwrap_or_else(|_| {
            Err(CommitError::NodeStopping {
                reason: NodeStopReason::Shutdown,
            })
        })
    }
}

pub(crate) struct LogProposal {
    pub(crate) partition: PartitionId,
    pub(crate) record: Bytes,
    pub(crate) waits_for_commit: bool,
}

struct StampState {
    last_stamped_micros: Vec<i64>,
    proposals: mpsc::UnboundedSender<LogProposal>,
}

/// Stamps commands and submits them to the commit pipeline; shared by every Flow runtime.
#[derive(Clone)]
pub struct CommandCommitter {
    inner: Arc<CommitterInner>,
}

struct CommitterInner {
    state: Arc<CommitState>,
    store: Arc<EngineLocalStore>,
    clock: Arc<dyn WallClock>,
    epoch: u64,
    behavior_version: u32,
    admission: Arc<Semaphore>,
    stamps: Mutex<StampState>,
    is_checking_replay: bool,
}

pub(crate) struct CommandCommitterParts {
    pub(crate) state: Arc<CommitState>,
    pub(crate) store: Arc<EngineLocalStore>,
    pub(crate) clock: Arc<dyn WallClock>,
    pub(crate) epoch: u64,
    pub(crate) behavior_version: u32,
    pub(crate) max_unapplied_bytes: u32,
    pub(crate) is_checking_replay: bool,
    pub(crate) proposals: mpsc::UnboundedSender<LogProposal>,
}

impl CommandCommitter {
    pub(crate) fn new(parts: CommandCommitterParts) -> Self {
        let partition_count = parts.state.partition_count() as usize;
        Self {
            inner: Arc::new(CommitterInner {
                state: parts.state,
                store: parts.store,
                clock: parts.clock,
                epoch: parts.epoch,
                behavior_version: parts.behavior_version,
                admission: Arc::new(Semaphore::new(parts.max_unapplied_bytes as usize)),
                stamps: Mutex::new(StampState {
                    last_stamped_micros: vec![0; partition_count],
                    proposals: parts.proposals,
                }),
                is_checking_replay: parts.is_checking_replay,
            }),
        }
    }

    /// Raises each partition's stamp watermark to its applied command timestamp.
    pub(crate) fn seed_stamp_watermarks(&self, applied_micros: &[(PartitionId, i64)]) {
        let mut stamps = self.lock_stamps();
        for (partition, micros) in applied_micros {
            let watermark = &mut stamps.last_stamped_micros[usize::from(partition.0)];
            *watermark = (*watermark).max(*micros);
        }
    }

    /// Stamps the command (epoch, behavior version from the version barrier, command ID,
    /// proposed_at_micros = max(wall clock, partition watermark + 1)) and enqueues it in the
    /// commit pipeline in one step, so log order equals stamp order within a partition.
    /// Returns the stamped command (apply it to the speculative overlay) and a ticket that
    /// resolves once the command is committed AND applied to the local store.
    ///
    /// Fails without enqueueing with [`CommitError::Backpressure`], [`CommitError::NotServing`],
    /// [`CommitError::PartitionStopped`], or [`CommitError::NodeStopping`].
    pub fn submit(
        &self,
        context: ApplyContext,
        mut command: FlowCommand,
        urgency: CommitUrgency,
    ) -> Result<(FlowCommand, CommitTicket), CommitError> {
        let inner = &*self.inner;
        inner.state.check_serving()?;
        let partition = context.partition;
        assert!(
            u32::from(partition.0) < inner.state.partition_count(),
            "the runtime computes partitions from the cluster's partition count"
        );
        if inner.state.is_partition_halted(partition) {
            return Err(CommitError::PartitionStopped { partition });
        }
        let command_id = Uuid::now_v7().as_bytes().to_vec();
        command.epoch = inner.epoch;
        command.behavior_version = inner.behavior_version;
        command.command_id.clone_from(&command_id);
        let (responder, receiver) = oneshot::channel();
        let mut stamps = self.lock_stamps();
        let watermark = stamps.last_stamped_micros[usize::from(partition.0)];
        command.proposed_at_micros = inner
            .clock
            .now_unix_micros()
            .max(watermark.saturating_add(1));
        let record = Bytes::from(
            LogRecord {
                partition: u32::from(partition.0),
                body: Some(log_record::Body::Flow(command.clone())),
            }
            .encode_to_vec(),
        );
        let admission = u32::try_from(record.len().max(1))
            .ok()
            .and_then(|bytes| inner.admission.clone().try_acquire_many_owned(bytes).ok())
            .ok_or(CommitError::Backpressure)?;
        inner.state.register(
            command_id.clone(),
            PendingCommand {
                responder,
                _admission: admission,
            },
        )?;
        let proposal = LogProposal {
            partition,
            record,
            waits_for_commit: urgency == CommitUrgency::Prompt,
        };
        if stamps.proposals.send(proposal).is_err() {
            drop(inner.state.take(&command_id));
            return Err(CommitError::NodeStopping {
                reason: inner
                    .state
                    .stop_reason()
                    .unwrap_or(NodeStopReason::Shutdown),
            });
        }
        stamps.last_stamped_micros[usize::from(partition.0)] = command.proposed_at_micros;
        drop(stamps);
        Ok((
            command,
            CommitTicket {
                command_id,
                receiver,
            },
        ))
    }

    /// A consistent read view of committed state (local store snapshot) for any Flow.
    ///
    /// Reads are synchronous local store calls; use the view from a blocking-pool thread.
    pub fn committed_view(&self) -> CommittedStateView {
        CommittedStateView::new(self.inner.store.clone())
    }

    /// Compares the speculative outcome of a command with its committed outcome when debug
    /// replay checks are enabled; otherwise returns `Ok` without comparing.
    ///
    /// A mismatch is a determinism bug: the partition stops and the mismatch is returned.
    pub fn check_speculative_outcome(
        &self,
        partition: PartitionId,
        speculative: &ApplyOutcome,
        committed: &CommittedCommand,
    ) -> Result<(), ReplayMismatch> {
        if !self.inner.is_checking_replay {
            return Ok(());
        }
        compare_speculative_outcome(speculative, committed).inspect_err(|mismatch| {
            tracing::error!(partition = partition.0, mismatch = %mismatch, "speculative outcome differs from committed outcome");
            self.inner.state.halt_partition(partition);
        })
    }

    /// Returns why the node stopped, or `None` while it recovers or serves.
    pub fn stop_reason(&self) -> Option<NodeStopReason> {
        self.inner.state.stop_reason()
    }

    fn lock_stamps(&self) -> std::sync::MutexGuard<'_, StampState> {
        self.inner
            .stamps
            .lock()
            .expect("stamp lock is not poisoned")
    }
}
