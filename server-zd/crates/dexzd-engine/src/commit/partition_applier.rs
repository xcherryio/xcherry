// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use dexzd_apply::{ApplyContext, ApplyOutcome};
use dexzd_keyspace::{PartitionHasher, PartitionId, TenantId};
use dexzd_protocol::dexzd::v1::{FlowCommand, LogPosition, LogRecord, PartitionMeta, log_record};
use dexzd_storage::{LocalReadView, LocalStore};
use prost::Message;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::command_applier::CommandApplier;
use crate::commit::EngineLocalStore;
use crate::commit::commit_state::{
    CommitState, NodeStopReason, PendingCommand, partition_stopped_error,
};
use crate::commit::replay_checks::compare_repeated_apply;
use crate::commit::staged_state_reader::StagedStateReader;
use crate::recovery::CommittedCommandSink;
use crate::runtime::{CommittedCommand, CommittedFlowCommand};

/// The records of one partition in one committed log batch.
pub(crate) struct PartitionFrame {
    pub(crate) partition: PartitionId,
    pub(crate) position: LogPosition,
    /// Epoch of the batch that holds the frame.
    pub(crate) epoch: u64,
    pub(crate) records: Vec<Bytes>,
}

enum LaneWork {
    Frame(PartitionFrame),
    AdvanceAppliedPosition {
        position: LogPosition,
        epoch: u64,
        done: oneshot::Sender<()>,
    },
}

/// Applies committed partition frames on lanes; partition `p` always uses lane `p % lanes`.
pub(crate) struct PartitionApplierPool {
    lanes: Vec<mpsc::Sender<LaneWork>>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    cancellation: CancellationToken,
}

pub(crate) struct FrameApplier {
    pub(crate) store: Arc<EngineLocalStore>,
    pub(crate) applier: Arc<dyn CommandApplier>,
    pub(crate) hasher: PartitionHasher,
    pub(crate) state: Arc<CommitState>,
    pub(crate) committed_commands: Arc<dyn CommittedCommandSink>,
    pub(crate) is_checking_replay: bool,
}

impl PartitionApplierPool {
    pub(crate) fn start(
        frame_applier: FrameApplier,
        lane_count: usize,
        queue_frames: usize,
    ) -> Self {
        assert!(
            lane_count > 0 && queue_frames > 0,
            "configuration validation rejects empty lanes"
        );
        let frame_applier = Arc::new(frame_applier);
        let cancellation = CancellationToken::new();
        let mut lanes = Vec::with_capacity(lane_count);
        let mut tasks = Vec::with_capacity(lane_count);
        for lane_index in 0..lane_count {
            let (sender, receiver) = mpsc::channel(queue_frames);
            lanes.push(sender);
            let lane = ApplierLane {
                frame_applier: frame_applier.clone(),
                lane_index,
                lane_count,
            };
            let lane_task = tokio::spawn(lane.run(receiver, cancellation.clone()));
            tasks.push(lane_task);
        }
        Self {
            lanes,
            tasks: Mutex::new(tasks),
            cancellation,
        }
    }

    /// Queues a frame; frames of one partition are applied in queue order.
    pub(crate) async fn enqueue(&self, frame: PartitionFrame) -> Result<(), ApplierPoolClosed> {
        let lane = usize::from(frame.partition.0) % self.lanes.len();
        self.lanes[lane]
            .send(LaneWork::Frame(frame))
            .await
            .map_err(|_| ApplierPoolClosed)
    }

    /// Marks running partitions applied through `position`. Call it after every batch up to it is queued.
    pub(crate) async fn advance_applied_position(
        &self,
        position: &LogPosition,
        epoch: u64,
    ) -> Result<(), ApplierPoolClosed> {
        let mut completions = Vec::with_capacity(self.lanes.len());
        for lane in &self.lanes {
            let (done, receiver) = oneshot::channel();
            lane.send(LaneWork::AdvanceAppliedPosition {
                position: position.clone(),
                epoch,
                done,
            })
            .await
            .map_err(|_| ApplierPoolClosed)?;
            completions.push(receiver);
        }
        for completion in completions {
            completion.await.map_err(|_| ApplierPoolClosed)?;
        }
        Ok(())
    }

    /// Stops every lane after its current frame; recovery applies the queued frames.
    pub(crate) async fn shutdown(&self) {
        self.cancellation.cancel();
        let tasks =
            std::mem::take(&mut *self.tasks.lock().expect("lane task lock is not poisoned"));
        for task in tasks {
            if let Err(error) = task.await {
                tracing::error!(error = %error, "partition applier lane failed");
            }
        }
    }
}

impl Drop for PartitionApplierPool {
    fn drop(&mut self) {
        self.cancellation.cancel();
        for task in self
            .tasks
            .get_mut()
            .expect("lane task lock is not poisoned")
            .drain(..)
        {
            task.abort();
        }
    }
}

#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("partition applier pool stopped")]
pub(crate) struct ApplierPoolClosed;

struct ApplierLane {
    frame_applier: Arc<FrameApplier>,
    lane_index: usize,
    lane_count: usize,
}

impl ApplierLane {
    async fn run(self, mut receiver: mpsc::Receiver<LaneWork>, cancellation: CancellationToken) {
        loop {
            let work = tokio::select! {
                biased;
                () = cancellation.cancelled() => return,
                work = receiver.recv() => work,
            };
            let Some(work) = work else { return };
            match work {
                LaneWork::Frame(frame) => {
                    let frame_applier = self.frame_applier.clone();
                    let applied =
                        tokio::task::spawn_blocking(move || frame_applier.apply_frame(frame)).await;
                    self.stop_on_task_failure(applied);
                }
                LaneWork::AdvanceAppliedPosition {
                    position,
                    epoch,
                    done,
                } => {
                    let frame_applier = self.frame_applier.clone();
                    let partitions = self.partitions();
                    let advanced = tokio::task::spawn_blocking(move || {
                        frame_applier.advance_applied_position(&partitions, &position, epoch);
                    })
                    .await;
                    self.stop_on_task_failure(advanced);
                    // The waiter may have given up; nothing else observes completion.
                    let _ignored_send = done.send(());
                }
            }
        }
    }

    fn partitions(&self) -> Vec<PartitionId> {
        (self.lane_index..self.frame_applier.state.partition_count() as usize)
            .step_by(self.lane_count)
            .map(|partition| PartitionId(u16::try_from(partition).expect("partition fits u16")))
            .collect()
    }

    fn stop_on_task_failure(&self, result: Result<(), tokio::task::JoinError>) {
        if let Err(error) = result {
            tracing::error!(lane = self.lane_index, error = %error, "partition applier task failed");
            self.frame_applier
                .state
                .stop(NodeStopReason::LocalStoreFailed {
                    detail: format!("applier lane {} failed: {error}", self.lane_index),
                });
        }
    }
}

struct DecodedCommand {
    command: FlowCommand,
    context: ApplyContext,
}

struct AppliedCommand {
    command: FlowCommand,
    context: ApplyContext,
    outcome: ApplyOutcome,
    pending: Option<PendingCommand>,
}

enum FrameFailure {
    Halt(String),
    StoreFailed(String),
}

impl FrameApplier {
    fn apply_frame(&self, frame: PartitionFrame) {
        let partition = frame.partition;
        if self.state.is_partition_halted(partition) {
            self.reject_frame(&frame);
            return;
        }
        match self.try_apply_frame(&frame) {
            Ok(Some(applied)) => self.publish(&frame, applied),
            Ok(None) => {}
            Err(FrameFailure::Halt(detail)) => {
                tracing::error!(partition = partition.0, seq = frame.position.seq, detail = %detail, "partition stopped applying commands");
                self.state.halt_partition(partition);
                self.reject_frame(&frame);
            }
            Err(FrameFailure::StoreFailed(detail)) => {
                self.state.stop(NodeStopReason::LocalStoreFailed { detail });
            }
        }
    }

    /// Returns `None` when the local store already contains the frame or the node stopped.
    fn try_apply_frame(
        &self,
        frame: &PartitionFrame,
    ) -> Result<Option<Vec<AppliedCommand>>, FrameFailure> {
        let store: &EngineLocalStore = &self.store;
        let meta = store
            .partition_meta(frame.partition)
            .map_err(|error| FrameFailure::StoreFailed(error.to_string()))?
            .unwrap_or_else(|| PartitionMeta {
                partition: u32::from(frame.partition.0),
                ..PartitionMeta::default()
            });
        if meta
            .applied
            .as_ref()
            .is_some_and(|applied| is_at_or_after(applied, &frame.position))
        {
            return Ok(None);
        }
        let commands = frame
            .records
            .iter()
            .map(|record| self.decode_command(frame.partition, record))
            .collect::<Result<Vec<_>, _>>()?;
        let mut reader = StagedStateReader::new(store);
        let mut applied = Vec::with_capacity(commands.len());
        let mut last_command_timestamp_micros = meta.last_command_timestamp_micros;
        let mut last_behavior_version = meta.last_behavior_version;
        for DecodedCommand { command, context } in commands {
            let outcome = self.apply_command(&context, &reader, &command)?;
            reader.stage(&outcome.writes);
            last_command_timestamp_micros =
                last_command_timestamp_micros.max(command.proposed_at_micros);
            last_behavior_version = command.behavior_version;
            applied.push(AppliedCommand {
                command,
                context,
                outcome,
                pending: None,
            });
        }
        let mut batch = reader.into_batch();
        batch
            .put_partition_meta(&PartitionMeta {
                partition: u32::from(frame.partition.0),
                applied: Some(frame.position.clone()),
                last_command_timestamp_micros,
                last_behavior_version,
                applied_epoch: frame.epoch,
            })
            .map_err(|error| FrameFailure::StoreFailed(error.to_string()))?;
        // Recovery replays a frame skipped after the stop.
        let Some(_store_write) = self.state.begin_store_write() else {
            return Ok(None);
        };
        store
            .write(batch)
            .map_err(|error| FrameFailure::StoreFailed(error.to_string()))?;
        // Taken under the write admission, so `stop` cannot fail these tickets after the write.
        for command in &mut applied {
            command.pending = self.state.take(&command.command.command_id);
        }
        Ok(Some(applied))
    }

    fn advance_applied_position(
        &self,
        partitions: &[PartitionId],
        position: &LogPosition,
        epoch: u64,
    ) {
        let store: &EngineLocalStore = &self.store;
        let mut batch = dexzd_storage::WriteBatch::new();
        for partition in partitions {
            if self.state.is_partition_halted(*partition) {
                continue;
            }
            let meta = match store.partition_meta(*partition) {
                Ok(meta) => meta,
                Err(error) => {
                    self.state.stop(NodeStopReason::LocalStoreFailed {
                        detail: error.to_string(),
                    });
                    return;
                }
            };
            let mut meta = meta.unwrap_or_else(|| PartitionMeta {
                partition: u32::from(partition.0),
                ..PartitionMeta::default()
            });
            if meta
                .applied
                .as_ref()
                .is_some_and(|applied| is_at_or_after(applied, position))
            {
                continue;
            }
            meta.applied = Some(position.clone());
            meta.applied_epoch = epoch;
            if let Err(error) = batch.put_partition_meta(&meta) {
                self.state.stop(NodeStopReason::LocalStoreFailed {
                    detail: error.to_string(),
                });
                return;
            }
        }
        if batch.operations().is_empty() {
            return;
        }
        // Advancing past a frame skipped after the stop would lose it.
        let written = match self.state.begin_store_write() {
            Some(_store_write) => store.write(batch),
            None => return,
        };
        if let Err(error) = written {
            self.state.stop(NodeStopReason::LocalStoreFailed {
                detail: error.to_string(),
            });
        }
    }

    fn decode_command(
        &self,
        partition: PartitionId,
        record: &Bytes,
    ) -> Result<DecodedCommand, FrameFailure> {
        let record = LogRecord::decode(record.clone())
            .map_err(|error| FrameFailure::Halt(format!("undecodable log record: {error}")))?;
        let Some(log_record::Body::Flow(command)) = record.body else {
            return Err(FrameFailure::Halt("log record has no command".to_owned()));
        };
        let flow_hash = self
            .hasher
            .flow_hash(TenantId(command.tenant_id), &command.flow_id);
        let command_partition = self.hasher.partition_of(flow_hash);
        if record.partition != u32::from(partition.0) || command_partition != partition {
            return Err(FrameFailure::Halt(format!(
                "command of partition {} is in the frame of partition {}",
                command_partition.0, partition.0
            )));
        }
        Ok(DecodedCommand {
            command,
            context: ApplyContext {
                partition,
                flow_hash,
            },
        })
    }

    fn apply_command(
        &self,
        context: &ApplyContext,
        reader: &StagedStateReader<'_>,
        command: &FlowCommand,
    ) -> Result<ApplyOutcome, FrameFailure> {
        let outcome = self.apply_catching_panics(context, reader, command)?;
        if self.is_checking_replay {
            let repeated = self.apply_catching_panics(context, reader, command)?;
            compare_repeated_apply(&outcome, &repeated)
                .map_err(|mismatch| FrameFailure::Halt(mismatch.to_string()))?;
        }
        Ok(outcome)
    }

    fn apply_catching_panics(
        &self,
        context: &ApplyContext,
        reader: &StagedStateReader<'_>,
        command: &FlowCommand,
    ) -> Result<ApplyOutcome, FrameFailure> {
        match catch_unwind(AssertUnwindSafe(|| {
            self.applier.apply(context, reader, command)
        })) {
            Ok(Ok(outcome)) => Ok(outcome),
            Ok(Err(error)) => Err(FrameFailure::Halt(format!(
                "apply of command {} failed: {error}",
                hex(&command.command_id)
            ))),
            Err(_) => Err(FrameFailure::Halt(format!(
                "apply of command {} panicked",
                hex(&command.command_id)
            ))),
        }
    }

    fn publish(&self, frame: &PartitionFrame, applied: Vec<AppliedCommand>) {
        // The sink sees the whole frame before any proposer, so reads after a commit observe it.
        let mut resolved = Vec::new();
        for AppliedCommand {
            command,
            context,
            outcome,
            pending,
        } in applied
        {
            let committed = CommittedCommand {
                outcome,
                position: frame.position.clone(),
            };
            if let Some(pending) = pending {
                resolved.push((pending, committed.clone()));
            }
            self.committed_commands
                .apply_committed(CommittedFlowCommand {
                    command,
                    context,
                    committed,
                });
        }
        self.committed_commands
            .finish_frame(frame.partition, &frame.position);
        for (pending, committed) in resolved {
            pending.responder.resolve(Ok(committed));
        }
    }

    fn reject_frame(&self, frame: &PartitionFrame) {
        for record in &frame.records {
            let Ok(LogRecord {
                body: Some(log_record::Body::Flow(command)),
                ..
            }) = LogRecord::decode(record.clone())
            else {
                continue;
            };
            if let Some(pending) = self.state.take(&command.command_id) {
                pending
                    .responder
                    .resolve(Err(partition_stopped_error(frame.partition)));
            }
        }
    }
}

/// Positions of one node order by incarnation, then seq; incarnations only increase.
fn is_at_or_after(applied: &LogPosition, position: &LogPosition) -> bool {
    (applied.incarnation, applied.seq) >= (position.incarnation, position.seq)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
