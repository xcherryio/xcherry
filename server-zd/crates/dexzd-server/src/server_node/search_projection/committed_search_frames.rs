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
use dexzd_engine::CommittedFlowCommand;
use dexzd_keyspace::PartitionId;
use dexzd_protocol::dexzd::v1::LogPosition;
use dexzd_search::paradedb::{
    ProjectionVisibilityWait, SearchDocumentChange, extract_search_document_changes,
};
use dexzd_search::{SearchError, SearchRequest};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{Notify, mpsc, watch};

/// One committed partition frame's search document changes, numbered in commit order.
pub(crate) struct CommittedSearchFrame {
    pub(crate) sequence: u64,
    pub(crate) partition: PartitionId,
    pub(crate) position: LogPosition,
    pub(crate) changes: Vec<SearchDocumentChange>,
}

/// Feeds committed frames' search document changes to the ParadeDB projection without delaying commits.
pub(crate) struct CommittedSearchFrames {
    tracking: Mutex<FrameTracking>,
    queue: mpsc::Sender<CommittedSearchFrame>,
    rebuild_requested: Notify,
    /// Highest frame sequence searchable along with all earlier frames; `None` until startup catch-up.
    projected_through: watch::Sender<Option<u64>>,
}

#[derive(Default)]
struct FrameTracking {
    open_frames: BTreeMap<PartitionId, OpenSearchFrame>,
    last_sequence: u64,
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
    is_unreadable: bool,
}

impl CommittedSearchFrames {
    /// Returns the frames and the receiving end of their bounded queue.
    pub(crate) fn new(queue_frames: usize) -> (Self, mpsc::Receiver<CommittedSearchFrame>) {
        let (queue, receiver) = mpsc::channel(queue_frames);
        let frames = Self {
            tracking: Mutex::new(FrameTracking::default()),
            queue,
            rebuild_requested: Notify::new(),
            projected_through: watch::Sender::new(None),
        };
        (frames, receiver)
    }

    /// Adds one committed command's search document changes to its partition's open frame.
    pub(crate) fn record_command(&self, committed: &CommittedFlowCommand) {
        let partition = committed.context.partition;
        let extracted = extract_search_document_changes(partition, &committed.committed.outcome);
        if matches!(&extracted, Ok(changes) if changes.is_empty()) {
            return;
        }
        let mut tracking = self.lock_tracking();
        let frame = tracking.open_frame(partition);
        match extracted {
            Ok(changes) => frame.changes.extend(changes),
            Err(error) => {
                tracing::error!(
                    partition = partition.0,
                    %error,
                    "a committed outcome has unreadable search documents; rebuilding its partition"
                );
                frame.is_unreadable = true;
            }
        }
    }

    /// Queues the partition's open frame for projection, or requests a rebuild when it cannot queue.
    pub(crate) fn finish_frame(&self, partition: PartitionId, position: &LogPosition) {
        let mut tracking = self.lock_tracking();
        let Some(frame) = tracking.open_frames.remove(&partition) else {
            return;
        };
        if frame.is_unreadable {
            self.request_rebuild(&mut tracking, partition, frame.sequence);
            return;
        }
        let queued = self.queue.try_send(CommittedSearchFrame {
            sequence: frame.sequence,
            partition,
            position: position.clone(),
            changes: frame.changes,
        });
        match queued {
            Ok(()) => {}
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

    pub(crate) fn finish_projected_frame(&self, sequence: u64) {
        self.lock_tracking().unprojected.remove(&sequence);
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
                is_unreadable: false,
            })
    }
}

#[async_trait]
impl ProjectionVisibilityWait for CommittedSearchFrames {
    async fn wait_until_visible(&self, _request: SearchRequest<'_>) -> Result<(), SearchError> {
        let required_sequence = self.last_sequence();
        let mut projected_through = self.projected_through.subscribe();
        projected_through
            .wait_for(|projected| projected.is_some_and(|through| through >= required_sequence))
            .await
            .map(|_| ())
            .map_err(|_| SearchError::Unavailable("the search projection stopped".to_owned()))
    }
}
