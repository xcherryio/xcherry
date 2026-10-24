// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard};

use async_trait::async_trait;
use dexzd_apply::StateWrite;
use dexzd_engine::CommittedFlowCommand;
use dexzd_keyspace::{ColumnFamily, PartitionId, RunTable, parse_run_key};
use dexzd_protocol::dexzd::v1::LogPosition;
use dexzd_protocol::dexzd::v1::flow_command::Command;
use dexzd_search::paradedb::{
    ProjectionVisibilityWait, SearchDocumentChange, extract_search_document_changes,
};
use dexzd_search::{SearchError, SearchRequest};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{Notify, mpsc, watch};

/// Estimated memory of one change besides its encoded document: identifiers and enum layout.
const CHANGE_OVERHEAD_BYTES: usize = 64;

/// One committed partition frame's search document changes, numbered in commit order.
pub(crate) struct CommittedSearchFrame {
    pub(crate) sequence: u64,
    pub(crate) partition: PartitionId,
    pub(crate) position: LogPosition,
    pub(crate) changes: Vec<SearchDocumentChange>,
    /// Estimated memory of `changes`, counted against the queue's byte limit until projected.
    pub(crate) size_bytes: usize,
}

/// Feeds committed frames' search document changes to the ParadeDB projection without delaying commits.
pub(crate) struct CommittedSearchFrames {
    tracking: Mutex<FrameTracking>,
    queue: mpsc::Sender<CommittedSearchFrame>,
    /// Frames that would raise the queued bytes above this become rebuild requests instead.
    max_queued_bytes: usize,
    rebuild_requested: Notify,
    /// Highest frame sequence searchable along with all earlier frames; `None` until startup catch-up.
    projected_through: watch::Sender<Option<u64>>,
    permanent_failure: watch::Sender<Option<String>>,
}

#[derive(Default)]
struct FrameTracking {
    open_frames: BTreeMap<PartitionId, OpenSearchFrame>,
    last_sequence: u64,
    /// Estimated bytes of frames queued or being projected.
    queued_bytes: usize,
    /// Open and queued frames; frames waiting for a rebuild move to `rebuild_requests`.
    unprojected: BTreeMap<u64, PartitionId>,
    rebuild_requests: BTreeMap<PartitionId, RebuildRequest>,
}

/// Frame sequences of one partition that only a rebuild can project; a range bounds memory.
struct RebuildRequest {
    first_sequence: u64,
    last_sequence: u64,
}

struct OpenSearchFrame {
    sequence: u64,
    changes: Vec<SearchDocumentChange>,
    requires_rebuild: bool,
}

impl CommittedSearchFrames {
    /// Returns the frames and the receiving end of their queue, bounded by frames and by bytes.
    pub(crate) fn new(
        queue_frames: usize,
        max_queued_bytes: usize,
    ) -> (Self, mpsc::Receiver<CommittedSearchFrame>) {
        let (queue, receiver) = mpsc::channel(queue_frames);
        let frames = Self {
            tracking: Mutex::new(FrameTracking::default()),
            queue,
            max_queued_bytes,
            rebuild_requested: Notify::new(),
            projected_through: watch::Sender::new(None),
            permanent_failure: watch::Sender::new(None),
        };
        (frames, receiver)
    }

    /// Adds one committed command's search document changes to its partition's open frame.
    pub(crate) fn record_command(&self, committed: &CommittedFlowCommand) {
        let partition = committed.context.partition;
        let extracted = extract_search_document_changes(partition, &committed.committed.outcome);
        let is_postponed_purge = matches!(
            committed.command.command.as_ref(),
            Some(Command::RunPurge(_))
        ) && committed.committed.outcome.writes.iter().any(|write| {
            matches!(write, StateWrite::Put { column_family: ColumnFamily::Flow, key, .. }
                if parse_run_key(key).is_ok_and(|parsed| parsed.table == RunTable::RunRecord))
        });
        if matches!(&extracted, Ok(changes) if changes.is_empty()) && !is_postponed_purge {
            return;
        }
        let mut tracking = self.lock_tracking();
        let frame = tracking.open_frame(partition);
        if is_postponed_purge {
            // A postponed purge restores visibility without rewriting its unchanged search document.
            frame.requires_rebuild = true;
        }
        match extracted {
            Ok(changes) => frame.changes.extend(changes),
            Err(error) => {
                tracing::error!(
                    partition = partition.0,
                    %error,
                    "a committed outcome has unreadable search documents; rebuilding its partition"
                );
                frame.requires_rebuild = true;
            }
        }
    }

    /// Queues the partition's open frame for projection, or requests a rebuild when it cannot queue.
    pub(crate) fn finish_frame(&self, partition: PartitionId, position: &LogPosition) {
        let mut tracking = self.lock_tracking();
        let Some(frame) = tracking.open_frames.remove(&partition) else {
            return;
        };
        if frame.requires_rebuild {
            self.request_rebuild(&mut tracking, partition, frame.sequence);
            return;
        }
        let size_bytes = frame.changes.iter().map(change_size_bytes).sum::<usize>();
        if tracking.queued_bytes.saturating_add(size_bytes) > self.max_queued_bytes {
            self.request_rebuild(&mut tracking, partition, frame.sequence);
            return;
        }
        let queued = self.queue.try_send(CommittedSearchFrame {
            sequence: frame.sequence,
            partition,
            position: position.clone(),
            changes: frame.changes,
            size_bytes,
        });
        match queued {
            Ok(()) => tracking.queued_bytes += size_bytes,
            Err(TrySendError::Full(frame)) => {
                self.request_rebuild(&mut tracking, partition, frame.sequence);
            }
            Err(TrySendError::Closed(_)) => {
                tracing::debug!(
                    partition = partition.0,
                    "the search projection stopped; a committed frame was not projected"
                );
            }
        }
    }

    pub(crate) fn rebuild_requested(&self) -> &Notify {
        &self.rebuild_requested
    }

    /// Returns the newest frame sequence handed to the projection.
    pub(crate) fn last_sequence(&self) -> u64 {
        self.lock_tracking().last_sequence
    }

    pub(crate) fn rebuild_requests(&self) -> Vec<PartitionId> {
        self.lock_tracking()
            .rebuild_requests
            .keys()
            .copied()
            .collect()
    }

    pub(crate) fn has_rebuild_request(&self, partition: PartitionId) -> bool {
        self.lock_tracking()
            .rebuild_requests
            .contains_key(&partition)
    }

    /// Marks a dequeued frame searchable and releases its bytes from the queue's limit.
    pub(crate) fn finish_projected_frame(&self, frame: &CommittedSearchFrame) {
        let mut tracking = self.lock_tracking();
        tracking.unprojected.remove(&frame.sequence);
        tracking.queued_bytes = tracking.queued_bytes.saturating_sub(frame.size_bytes);
    }

    /// Marks the partition's frames through `sequence` searchable after a rebuild.
    pub(crate) fn finish_rebuild(&self, partition: PartitionId, sequence: u64) {
        let mut tracking = self.lock_tracking();
        tracking
            .unprojected
            .retain(|frame_sequence, frame_partition| {
                *frame_partition != partition || *frame_sequence > sequence
            });
        if let Some(request) = tracking.rebuild_requests.get_mut(&partition) {
            if request.last_sequence <= sequence {
                tracking.rebuild_requests.remove(&partition);
            } else {
                request.first_sequence = request.first_sequence.max(sequence + 1);
            }
        }
    }

    /// Marks every frame through `sequence` searchable once all partitions caught up after startup.
    pub(crate) fn finish_catch_up(&self, sequence: u64) {
        let mut tracking = self.lock_tracking();
        tracking
            .unprojected
            .retain(|frame_sequence, _| *frame_sequence > sequence);
        tracking.rebuild_requests.retain(|_, request| {
            request.first_sequence = request.first_sequence.max(sequence + 1);
            request.last_sequence > sequence
        });
    }

    /// Lets read-your-writes waits pass for every frame the projection has made searchable.
    pub(crate) fn publish_projected_through(&self) {
        let projected_through = {
            let tracking = self.lock_tracking();
            let first_unprojected = tracking.unprojected.keys().next().copied();
            let first_to_rebuild = tracking
                .rebuild_requests
                .values()
                .map(|request| request.first_sequence)
                .min();
            match first_unprojected.into_iter().chain(first_to_rebuild).min() {
                Some(sequence) => sequence - 1,
                None => tracking.last_sequence,
            }
        };
        self.projected_through.send_if_modified(|published| {
            let is_newer = published.is_none_or(|published| published < projected_through);
            if is_newer {
                *published = Some(projected_through);
            }
            is_newer
        });
    }

    pub(crate) fn record_permanent_failure(&self, partition: PartitionId, error: &str) {
        let reason = format!(
            "ParadeDB projection partition {} failed: {error}",
            partition.0
        );
        tracing::error!(partition = partition.0, %error, "the search projection stopped");
        self.permanent_failure.send_replace(Some(reason));
    }

    fn request_rebuild(&self, tracking: &mut FrameTracking, partition: PartitionId, sequence: u64) {
        tracking.unprojected.remove(&sequence);
        match tracking.rebuild_requests.get_mut(&partition) {
            Some(request) => request.last_sequence = sequence,
            None => {
                tracing::warn!(
                    partition = partition.0,
                    "a committed frame could not be queued for the search projection; rebuilding its partition"
                );
                tracking.rebuild_requests.insert(
                    partition,
                    RebuildRequest {
                        first_sequence: sequence,
                        last_sequence: sequence,
                    },
                );
            }
        }
        self.rebuild_requested.notify_one();
    }

    fn lock_tracking(&self) -> MutexGuard<'_, FrameTracking> {
        self.tracking
            .lock()
            .expect("frame tracking lock is poisoned only if a holder panicked")
    }
}

fn change_size_bytes(change: &SearchDocumentChange) -> usize {
    let document_bytes = match change {
        SearchDocumentChange::Upsert {
            encoded_document, ..
        } => encoded_document.len(),
        _ => 0,
    };
    CHANGE_OVERHEAD_BYTES + document_bytes
}

impl FrameTracking {
    fn open_frame(&mut self, partition: PartitionId) -> &mut OpenSearchFrame {
        if !self.open_frames.contains_key(&partition) {
            self.last_sequence += 1;
            self.unprojected.insert(self.last_sequence, partition);
        }
        let sequence = self.last_sequence;
        self.open_frames
            .entry(partition)
            .or_insert_with(|| OpenSearchFrame {
                sequence,
                changes: Vec::new(),
                requires_rebuild: false,
            })
    }
}

#[async_trait]
impl ProjectionVisibilityWait for CommittedSearchFrames {
    async fn wait_until_visible(&self, _request: SearchRequest<'_>) -> Result<(), SearchError> {
        let mut permanent_failure = self.permanent_failure.subscribe();
        if let Some(reason) = permanent_failure.borrow().as_ref() {
            return Err(SearchError::Internal(reason.clone()));
        }
        let required_sequence = self.last_sequence();
        let mut projected_through = self.projected_through.subscribe();
        let result = tokio::select! {
            projected = projected_through.wait_for(|projected| projected.is_some_and(|through| through >= required_sequence)) => {
                projected.map(|_| ()).map_err(|_| SearchError::Unavailable("the search projection stopped".to_owned()))
            }
            failed = permanent_failure.wait_for(Option::is_some) => {
                let reason = failed.map_err(|_| SearchError::Unavailable("the search projection stopped".to_owned()))?;
                Err(SearchError::Internal(reason.as_ref().expect("failure notification carries a reason").clone()))
            }
        };
        match permanent_failure.borrow().as_ref() {
            Some(reason) => Err(SearchError::Internal(reason.clone())),
            None => result,
        }
    }
}

#[cfg(test)]
mod tests {
    use dexzd_apply::{ApplyContext, ApplyOutcome, StateWrite};
    use dexzd_engine::CommittedCommand;
    use dexzd_keyspace::row_keys::run_singleton_key;
    use dexzd_keyspace::{ColumnFamily, FlowHash, RunId, RunTable};
    use dexzd_protocol::dexzd::v1::{
        CommandReply, FlowCommand, RunPurgeCommand, RunRecord, SearchDocumentRecord, flow_command,
    };
    use prost::Message;

    use super::*;

    const PARTITION: PartitionId = PartitionId(3);
    const DOCUMENT_BYTES: usize = 1024;

    fn committed_upsert(run_number: u8) -> CommittedFlowCommand {
        let flow_hash = FlowHash(7);
        let run_id = RunId([run_number; 16]);
        let document = SearchDocumentRecord {
            tenant_id: 1,
            run_id: run_id.as_bytes().to_vec(),
            flow_type: "x".repeat(DOCUMENT_BYTES),
            ..SearchDocumentRecord::default()
        };
        CommittedFlowCommand {
            command: FlowCommand::default(),
            context: ApplyContext {
                partition: PARTITION,
                flow_hash,
            },
            committed: CommittedCommand {
                outcome: ApplyOutcome {
                    writes: vec![StateWrite::Put {
                        column_family: ColumnFamily::Flow,
                        key: run_singleton_key(flow_hash, run_id, RunTable::SearchDocument),
                        value: document.encode_to_vec(),
                    }],
                    effects: Vec::new(),
                    reply: CommandReply::default(),
                    run_id: Some(run_id),
                },
                position: LogPosition::default(),
            },
        }
    }

    fn commit_frame(frames: &CommittedSearchFrames, run_number: u8) {
        frames.record_command(&committed_upsert(run_number));
        frames.finish_frame(
            PARTITION,
            &LogPosition {
                seq: u64::from(run_number),
                ..LogPosition::default()
            },
        );
    }

    /// A stalled projection, as during a ParadeDB outage, holds at most the byte limit in memory.
    #[test]
    fn queued_bytes_stay_bounded_while_the_projection_is_stalled() {
        let frame_bytes = DOCUMENT_BYTES + CHANGE_OVERHEAD_BYTES + 32;
        let (frames, mut queue) = CommittedSearchFrames::new(1_000, 3 * frame_bytes);
        for run_number in 1..=20 {
            commit_frame(&frames, run_number);
        }
        assert_eq!(
            queue.len(),
            3,
            "frames beyond the byte limit are not queued"
        );
        assert!(frames.lock_tracking().queued_bytes <= 3 * frame_bytes);
        assert_eq!(frames.rebuild_requests(), vec![PARTITION]);
        frames.publish_projected_through();
        assert_eq!(*frames.projected_through.borrow(), Some(0));

        // The projection recovers: it drains the queue, then rebuilds the partition.
        while let Ok(frame) = queue.try_recv() {
            frames.finish_projected_frame(&frame);
        }
        frames.finish_rebuild(PARTITION, frames.last_sequence());
        frames.publish_projected_through();
        assert_eq!(*frames.projected_through.borrow(), Some(20));
        assert_eq!(frames.lock_tracking().queued_bytes, 0);
        assert!(frames.rebuild_requests().is_empty());

        commit_frame(&frames, 21);
        assert_eq!(queue.len(), 1, "freed bytes let new frames queue again");
        assert!(frames.rebuild_requests().is_empty());
    }

    #[test]
    fn frame_count_overflow_coalesces_into_a_partition_rebuild() {
        let max_queued_bytes = 1024 * 1024;
        let (frames, mut queue) = CommittedSearchFrames::new(2, max_queued_bytes);
        for run_number in 1..=20 {
            commit_frame(&frames, run_number);
        }
        assert_eq!(queue.len(), 2);
        assert!(frames.lock_tracking().queued_bytes < max_queued_bytes);
        assert_eq!(frames.rebuild_requests(), vec![PARTITION]);
        assert_eq!(frames.lock_tracking().rebuild_requests.len(), 1);
        frames.publish_projected_through();
        assert_eq!(*frames.projected_through.borrow(), Some(0));

        while let Ok(frame) = queue.try_recv() {
            frames.finish_projected_frame(&frame);
        }
        frames.finish_rebuild(PARTITION, frames.last_sequence());
        frames.publish_projected_through();
        assert_eq!(*frames.projected_through.borrow(), Some(20));
        assert!(frames.rebuild_requests().is_empty());
    }

    #[test]
    fn run_purge_rebuilds_projection_when_retention_is_postponed() {
        let (frames, queue) = CommittedSearchFrames::new(2, 1024);
        let mut committed = committed_upsert(1);
        committed.command.command = Some(flow_command::Command::RunPurge(RunPurgeCommand {}));
        committed.committed.outcome.writes = vec![StateWrite::Put {
            column_family: ColumnFamily::Flow,
            key: run_singleton_key(FlowHash(7), RunId([1; 16]), RunTable::RunRecord),
            value: RunRecord {
                tenant_id: 1,
                run_id: RunId([1; 16]).as_bytes().to_vec(),
                ..RunRecord::default()
            }
            .encode_to_vec(),
        }];
        frames.record_command(&committed);
        frames.finish_frame(PARTITION, &LogPosition::default());
        assert!(queue.is_empty());
        assert_eq!(frames.rebuild_requests(), vec![PARTITION]);
    }

    #[test]
    fn permanent_projection_failure_records_partition_and_sqlstate() {
        let (frames, _queue) = CommittedSearchFrames::new(1, 1024);
        frames.record_permanent_failure(PARTITION, "upsert run failed: SQLSTATE 54023");
        let failure = frames.permanent_failure.borrow();
        let reason = failure.as_ref().expect("the failure is recorded");
        assert!(reason.contains("partition 3"));
        assert!(reason.contains("SQLSTATE 54023"));
    }
}
