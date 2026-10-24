// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use dexzd_apply::{ApplyContext, ApplyOutcome, FlowStateReader};
use dexzd_clock::WallClock;
use dexzd_keyspace::PartitionId;
use dexzd_protocol::dexzd::v1::{FlowCommand, LogRecord, log_record};
use prost::Message;
use tokio::sync::{Semaphore, mpsc};
use uuid::Uuid;

use crate::commit::EngineLocalStore;
use crate::commit::commit_state::{
    CommitState, NodeStopReason, PendingCommand, partition_stopped_error,
};
use crate::commit::committed_state_view::CommittedStateView;
use crate::commit::replay_checks::{ReplayMismatch, compare_speculative_outcome};
use crate::runtime::{
    CommitError, CommitTicket, CommitUrgency, CommittedCommand, FlowCommandCommitter,
};

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
    /// Fails without enqueueing with [`CommitError::Unavailable`] while the node recovers,
    /// when the unapplied budget is full, or when the partition stopped, and with
    /// [`CommitError::NodeStopping`] after the node stopped.
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
            return Err(partition_stopped_error(partition));
        }
        let command_id = Uuid::now_v7().as_bytes().to_vec();
        command.epoch = inner.epoch;
        command.behavior_version = inner.behavior_version;
        command.command_id.clone_from(&command_id);
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
            .ok_or_else(|| CommitError::Unavailable {
                detail: "the commit backlog is full".to_owned(),
            })?;
        let (responder, ticket) = CommitTicket::new_pair();
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
            let reason = inner
                .state
                .stop_reason()
                .unwrap_or(NodeStopReason::Shutdown);
            let error = CommitError::NodeStopping {
                detail: reason.to_string(),
            };
            if let Some(pending) = inner.state.take(&command_id) {
                pending.responder.resolve(Err(error.clone()));
            }
            return Err(error);
        }
        stamps.last_stamped_micros[usize::from(partition.0)] = command.proposed_at_micros;
        drop(stamps);
        Ok((command, ticket))
    }

    /// Returns a consistent view of committed state. Reads block, so use it on a
    /// blocking-pool thread.
    ///
    /// Fails with [`CommitError::Unavailable`] when the local store cannot open a view.
    pub fn committed_view(&self) -> Result<CommittedStateView, CommitError> {
        CommittedStateView::open(self.inner.store.clone()).map_err(|error| {
            CommitError::Unavailable {
                detail: format!("local store cannot open a consistent view: {error}"),
            }
        })
    }

    /// Compares speculative and committed outcomes when debug replay checks are on.
    ///
    /// Returns the mismatch after stopping the partition.
    pub fn check_speculative_outcome(
        &self,
        partition: PartitionId,
        command: &FlowCommand,
        speculative: &ApplyOutcome,
        committed: &CommittedCommand,
    ) -> Result<(), ReplayMismatch> {
        if !self.inner.is_checking_replay {
            return Ok(());
        }
        compare_speculative_outcome(speculative, command, partition, committed).inspect_err(|mismatch| {
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

impl FlowCommandCommitter for CommandCommitter {
    fn submit(
        &self,
        context: ApplyContext,
        command: FlowCommand,
        urgency: CommitUrgency,
    ) -> Result<(FlowCommand, CommitTicket), CommitError> {
        CommandCommitter::submit(self, context, command, urgency)
    }

    fn committed_view(&self) -> Result<Box<dyn FlowStateReader + Send>, CommitError> {
        Ok(Box::new(CommandCommitter::committed_view(self)?))
    }

    fn check_speculative_outcome(
        &self,
        partition: PartitionId,
        command: &FlowCommand,
        speculative: &ApplyOutcome,
        committed: &CommittedCommand,
    ) -> Result<(), crate::runtime::ReplayMismatch> {
        CommandCommitter::check_speculative_outcome(
            self,
            partition,
            command,
            speculative,
            committed,
        )
        .map_err(|mismatch| crate::runtime::ReplayMismatch {
            detail: mismatch.to_string(),
        })
    }
}
