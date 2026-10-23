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
use dexzd_keyspace::PartitionId;
use dexzd_log::{StreamIdentity, recover_stream};
use dexzd_object_store::ObjectStore;
use dexzd_protocol::dexzd::v1::LogPosition;
use uuid::Uuid;

use crate::commit::{CommitState, EngineLocalStore, PartitionApplierPool, PartitionFrame};
use crate::recovery::EngineStartError;
use crate::recovery::local_store_opening::lowest_applied_position;

/// Where the recovered log stream continues.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReplayedLog {
    pub(crate) epoch_start_seq: u64,
    pub(crate) next_seq: u64,
}

pub(crate) struct LogReplay<'a> {
    pub(crate) object_store: &'a dyn ObjectStore,
    pub(crate) clock: &'a dyn WallClock,
    pub(crate) stream: &'a StreamIdentity,
    pub(crate) store: &'a EngineLocalStore,
    pub(crate) appliers: &'a PartitionApplierPool,
    pub(crate) state: &'a CommitState,
    pub(crate) epoch: u64,
}

impl LogReplay<'_> {
    /// Replays committed batches after the applied positions and writes EpochStart for `epoch`.
    pub(crate) async fn run(&self) -> Result<ReplayedLog, EngineStartError> {
        let (last_applied_seq, last_applied_epoch) =
            lowest_applied_position(self.store, self.state.partition_count())?;
        let writer_process_id = Uuid::now_v7().to_string();
        let recovered = recover_stream(
            self.object_store,
            self.clock,
            self.stream,
            last_applied_seq,
            last_applied_epoch,
            self.epoch,
            &writer_process_id,
        )
        .await?;
        tracing::info!(
            last_applied_seq,
            last_applied_epoch,
            batches = recovered.batches_to_apply.len(),
            voided_batches = recovered.void_sequences.len(),
            epoch_start_seq = recovered.epoch_start_seq,
            "replaying the log stream"
        );
        for batch in recovered.batches_to_apply {
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
                        detail: format!(
                            "log batch {} names partition {partition}",
                            batch.header.seq
                        ),
                    })?;
                self.appliers
                    .enqueue(PartitionFrame {
                        partition: PartitionId(partition),
                        position: self.position(batch.header.seq),
                        epoch: batch.header.epoch,
                        records,
                    })
                    .await
                    .map_err(|_| EngineStartError::Stopped {
                        reason: self.stop_reason(),
                    })?;
            }
        }
        self.appliers
            .advance_applied_position(&self.position(recovered.epoch_start_seq), self.epoch)
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

    fn position(&self, seq: u64) -> LogPosition {
        LogPosition {
            node_id: self.stream.node_id.clone(),
            incarnation: self.stream.incarnation,
            seq,
        }
    }

    fn stop_reason(&self) -> crate::NodeStopReason {
        self.state
            .stop_reason()
            .unwrap_or(crate::NodeStopReason::Shutdown)
    }
}
