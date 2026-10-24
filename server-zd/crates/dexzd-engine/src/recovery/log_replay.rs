// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;

use bytes::Bytes;
use dexzd_clock::WallClock;
use dexzd_cluster_metadata::MetadataError;
use dexzd_keyspace::PartitionId;
use dexzd_log::{DecodedBatch, LogError, StreamIdentity, decode_batch, recover_stream};
use dexzd_object_store::ObjectStore;
use dexzd_protocol::dexzd::v1::LogPosition;
use uuid::Uuid;

use crate::commit::{CommitState, EngineLocalStore, PartitionApplierPool, PartitionFrame};
use crate::recovery::local_store_opening::{AppliedPosition, lowest_applied_position};
use crate::recovery::node_streams::NodeStreams;
use crate::recovery::{EngineStartError, describe_lost_ownership};

/// Where the recovered log stream continues.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReplayedLog {
    pub(crate) epoch_start_seq: u64,
    pub(crate) next_seq: u64,
}

pub(crate) struct LogReplay<'a> {
    pub(crate) object_store: &'a dyn ObjectStore,
    pub(crate) clock: &'a dyn WallClock,
    pub(crate) cluster_id: [u8; 16],
    pub(crate) streams: &'a NodeStreams,
    pub(crate) store: &'a EngineLocalStore,
    pub(crate) appliers: &'a PartitionApplierPool,
    pub(crate) state: &'a CommitState,
    pub(crate) epoch: u64,
}

impl LogReplay<'_> {
    /// Replays every recorded stream after the applied positions and writes EpochStart for
    /// `epoch` in the serving stream.
    ///
    /// Returns where the serving stream continues. When the configured incarnation is newer than
    /// the open segment, the open stream is sealed and closed in the manifest first.
    pub(crate) async fn run(&self) -> Result<ReplayedLog, EngineStartError> {
        let applied = lowest_applied_position(self.store, self.state.partition_count())?;
        for segment in &self.streams.segments {
            let Some(end_seq) = segment.end_seq else {
                continue;
            };
            if applied.is_some_and(|applied| applied.incarnation > segment.incarnation) {
                continue;
            }
            self.replay_closed_stream(
                &self.stream(segment.incarnation),
                resume_point(applied, segment.incarnation),
                end_seq,
            )
            .await?;
        }
        let open_incarnation = self.streams.open_segment().incarnation;
        let open_log = self
            .recover_open_stream(
                &self.stream(open_incarnation),
                resume_point(applied, open_incarnation),
            )
            .await?;
        if !self.streams.is_changing_incarnation() {
            return Ok(open_log);
        }
        self.close_open_segment(open_log.next_seq - 1).await?;
        tracing::info!(
            closed_incarnation = open_incarnation,
            serving_incarnation = self.streams.serving_incarnation,
            "closed the previous log stream; starting a new incarnation"
        );
        self.recover_open_stream(&self.stream(self.streams.serving_incarnation), (0, 0))
            .await
    }

    /// Applies the batches of a closed stream after `(seq, epoch)` up to `end_seq`.
    async fn replay_closed_stream(
        &self,
        stream: &StreamIdentity,
        (applied_seq, applied_epoch): (u64, u64),
        end_seq: u64,
    ) -> Result<(), EngineStartError> {
        let mut effective_epoch = applied_epoch;
        let mut batches = 0_usize;
        for seq in applied_seq.saturating_add(1)..=end_seq {
            let object = self
                .object_store
                .get(&stream.batch_key(seq))
                .await
                .map_err(LogError::from)?
                .ok_or_else(|| {
                    LogError::Corrupt(format!(
                        "closed log stream of incarnation {} has no batch at seq {seq}",
                        stream.incarnation
                    ))
                })?;
            let batch = decode_batch(&object.bytes)?;
            if batch.header.cluster_id != stream.cluster_id
                || batch.header.node_id != stream.node_id
                || batch.header.incarnation != stream.incarnation
                || batch.header.seq != seq
            {
                return Err(LogError::Corrupt(format!(
                    "batch at seq {seq} has a different stream identity"
                ))
                .into());
            }
            // Same rule as stream recovery: an older epoch after an EpochStart is void.
            if batch.header.epoch < effective_epoch {
                continue;
            }
            if let Some(epoch_start) = &batch.header.epoch_start {
                effective_epoch = epoch_start.epoch;
            }
            self.enqueue_batch(stream, batch).await?;
            batches += 1;
        }
        tracing::info!(
            incarnation = stream.incarnation,
            applied_seq,
            end_seq,
            batches,
            "replayed a closed log stream"
        );
        Ok(())
    }

    /// Replays an open stream and writes EpochStart for `epoch`, which also seals it.
    async fn recover_open_stream(
        &self,
        stream: &StreamIdentity,
        (applied_seq, applied_epoch): (u64, u64),
    ) -> Result<ReplayedLog, EngineStartError> {
        let writer_process_id = Uuid::now_v7().to_string();
        let recovered = recover_stream(
            self.object_store,
            self.clock,
            stream,
            applied_seq,
            applied_epoch,
            self.epoch,
            &writer_process_id,
        )
        .await
        .map_err(|error| match error {
            LogError::MissingBatches {
                missing_seq,
                next_existing_seq,
            } => EngineStartError::LogUnavailable {
                incarnation: stream.incarnation,
                first_retained_seq: next_existing_seq,
                resumes_at: missing_seq,
            },
            error => error.into(),
        })?;
        tracing::info!(
            incarnation = stream.incarnation,
            last_applied_seq = applied_seq,
            last_applied_epoch = applied_epoch,
            batches = recovered.batches_to_apply.len(),
            voided_batches = recovered.void_sequences.len(),
            epoch_start_seq = recovered.epoch_start_seq,
            "replaying the log stream"
        );
        for batch in recovered.batches_to_apply {
            self.enqueue_batch(stream, batch).await?;
        }
        self.appliers
            .advance_applied_position(&position(stream, recovered.epoch_start_seq), self.epoch)
            .await
            .map_err(|_| EngineStartError::Stopped {
                reason: self.stop_reason(),
            })?;
        if let Some(reason) = self.state.stop_reason() {
            return Err(EngineStartError::Stopped { reason });
        }
        Ok(ReplayedLog {
            epoch_start_seq: recovered.epoch_start_seq,
            next_seq: recovered.next_seq,
        })
    }

    async fn enqueue_batch(
        &self,
        stream: &StreamIdentity,
        batch: DecodedBatch,
    ) -> Result<(), EngineStartError> {
        let mut records_by_partition = BTreeMap::<u32, Vec<Bytes>>::new();
        for record in batch.records {
            records_by_partition
                .entry(record.partition)
                .or_default()
                .push(record.bytes);
        }
        for (partition, records) in records_by_partition {
            let partition = u16::try_from(partition)
                .ok()
                .filter(|partition| u32::from(*partition) < self.state.partition_count())
                .ok_or_else(|| EngineStartError::InvalidMetadata {
                    detail: format!("log batch {} names partition {partition}", batch.header.seq),
                })?;
            self.appliers
                .enqueue(PartitionFrame {
                    partition: PartitionId(partition),
                    position: position(stream, batch.header.seq),
                    epoch: batch.header.epoch,
                    records,
                })
                .await
                .map_err(|_| EngineStartError::Stopped {
                    reason: self.stop_reason(),
                })?;
        }
        Ok(())
    }

    /// Records the sealed open stream as closed and opens the serving incarnation.
    async fn close_open_segment(&self, end_seq: u64) -> Result<(), EngineStartError> {
        dexzd_cluster_metadata::update_manifest(self.object_store, self.clock, |manifest| {
            if let Some(detail) =
                describe_lost_ownership(manifest, &self.streams.node_id, self.epoch)
            {
                return Err(MetadataError::LeaseLost {
                    node_id: self.streams.node_id.clone(),
                    detail,
                });
            }
            Ok(self.streams.close_open_segment(manifest, end_seq))
        })
        .await?;
        Ok(())
    }

    fn stream(&self, incarnation: u32) -> StreamIdentity {
        StreamIdentity {
            cluster_id: self.cluster_id,
            node_id: self.streams.node_id.clone(),
            incarnation,
        }
    }

    fn stop_reason(&self) -> crate::NodeStopReason {
        self.state
            .stop_reason()
            .unwrap_or(crate::NodeStopReason::Shutdown)
    }
}

/// Returns the applied (seq, epoch) within `incarnation`, or (0, 0) when nothing of it is applied.
fn resume_point(applied: Option<AppliedPosition>, incarnation: u32) -> (u64, u64) {
    match applied {
        Some(applied) if applied.incarnation == incarnation => (applied.seq, applied.epoch),
        _ => (0, 0),
    }
}

fn position(stream: &StreamIdentity, seq: u64) -> LogPosition {
    LogPosition {
        node_id: stream.node_id.clone(),
        incarnation: stream.incarnation,
        seq,
    }
}
