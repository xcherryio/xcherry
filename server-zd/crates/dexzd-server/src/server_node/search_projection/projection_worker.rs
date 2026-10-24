// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dexzd_apply::FlowStateReader;
use dexzd_engine::{CommandCommitter, CommittedStateView};
use dexzd_keyspace::{ColumnFamily, PartitionId, partition_meta_key};
use dexzd_protocol::dexzd::v1::{LogPosition, PartitionMeta};
use dexzd_search::paradedb::{ParadeDbProjector, ProjectionError};
use prost::Message;
use tokio::sync::mpsc;

use super::committed_search_frames::{CommittedSearchFrame, CommittedSearchFrames};

const FIRST_RETRY_DELAY: Duration = Duration::from_millis(100);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(10);

/// Projects committed frames into ParadeDB in commit order and rebuilds partitions it cannot follow.
pub(crate) struct SearchProjectionWorker {
    projector: ParadeDbProjector,
    committer: CommandCommitter,
    frames: Arc<CommittedSearchFrames>,
    queue: mpsc::Receiver<CommittedSearchFrame>,
    partition_count: u32,
    /// The last position projected for each partition; the previous position of its next frame.
    projected_positions: BTreeMap<PartitionId, LogPosition>,
}

#[derive(Debug, thiserror::Error)]
enum SearchProjectionError {
    #[error("read committed partition state: {detail}")]
    CommittedState { detail: String },
    #[error(transparent)]
    Projection(#[from] ProjectionError),
}

impl SearchProjectionError {
    fn is_retryable(&self) -> bool {
        match self {
            Self::CommittedState { .. } => true,
            Self::Projection(error) => error.is_retryable(),
        }
    }
}

impl SearchProjectionWorker {
    pub(crate) fn new(
        projector: ParadeDbProjector,
        committer: CommandCommitter,
        frames: Arc<CommittedSearchFrames>,
        queue: mpsc::Receiver<CommittedSearchFrame>,
        partition_count: u32,
    ) -> Self {
        Self {
            projector,
            committer,
            frames,
            queue,
            partition_count,
            projected_positions: BTreeMap::new(),
        }
    }

    /// Catches up every partition, then projects queued frames until the queue closes.
    pub(crate) async fn run(mut self) {
        if !self.catch_up_partitions().await {
            return;
        }
        loop {
            tokio::select! {
                frame = self.queue.recv() => match frame {
                    Some(frame) => if !self.project_frame(frame).await { return; },
                    None => return,
                },
                () = self.frames.rebuild_requested().notified() => {}
            }
            for partition in self.frames.rebuild_requests() {
                if !self.rebuild_partition(partition).await {
                    return;
                }
            }
            self.frames.publish_projected_through();
        }
    }

    /// Frames lost before startup leave ParadeDB behind the local store, so differing partitions rebuild.
    async fn catch_up_partitions(&mut self) -> bool {
        let caught_up_sequence = self.frames.last_sequence();
        let mut rebuilt_partitions = 0_u32;
        for partition in (0..self.partition_count).map(|partition| {
            PartitionId(u16::try_from(partition).expect("validation limits partitions to 65536"))
        }) {
            let mut retry_delay = RetryDelay::default();
            loop {
                match self.catch_up_partition(partition).await {
                    Ok(was_rebuilt) => {
                        rebuilt_partitions += u32::from(was_rebuilt);
                        break;
                    }
                    Err(error) => {
                        if !error.is_retryable() {
                            self.frames
                                .record_permanent_failure(partition, &error.to_string());
                            return false;
                        }
                        tracing::warn!(
                            partition = partition.0,
                            %error,
                            "catching up the search projection failed; retrying"
                        );
                        retry_delay.wait().await;
                    }
                }
            }
        }
        self.frames.finish_catch_up(caught_up_sequence);
        self.frames.publish_projected_through();
        tracing::info!(
            partitions = self.partition_count,
            rebuilt_partitions,
            "the ParadeDB search projection caught up"
        );
        true
    }

    /// Returns whether the partition had to be rebuilt.
    async fn catch_up_partition(
        &mut self,
        partition: PartitionId,
    ) -> Result<bool, SearchProjectionError> {
        let progress = self.projector.read_progress(partition).await?;
        let (view, committed_position) = self.open_committed_view(partition).await?;
        match (progress, committed_position) {
            (None, None) => Ok(false),
            (Some(progress), None) => {
                tracing::warn!(
                    partition = partition.0,
                    "ParadeDB has search progress for a partition without committed state"
                );
                self.projected_positions.insert(partition, progress);
                Ok(false)
            }
            (Some(progress), Some(committed_position)) if progress == committed_position => {
                self.projected_positions.insert(partition, progress);
                Ok(false)
            }
            (_, Some(committed_position)) => {
                self.projector
                    .rebuild_partition(partition, &committed_position, Arc::new(view))
                    .await?;
                self.projected_positions
                    .insert(partition, committed_position);
                Ok(true)
            }
        }
    }

    async fn project_frame(&mut self, frame: CommittedSearchFrame) -> bool {
        if self.frames.has_rebuild_request(frame.partition)
            && !self.rebuild_partition(frame.partition).await
        {
            return false;
        }
        if !self.is_projected(frame.partition, &frame.position) && !self.apply_frame(&frame).await {
            return false;
        }
        self.frames.finish_projected_frame(&frame);
        true
    }

    async fn apply_frame(&mut self, frame: &CommittedSearchFrame) -> bool {
        let mut retry_delay = RetryDelay::default();
        loop {
            let applied = self
                .projector
                .apply_batch(
                    frame.partition,
                    self.projected_positions.get(&frame.partition),
                    &frame.position,
                    &frame.changes,
                )
                .await;
            match applied {
                Ok(()) => {
                    self.projected_positions
                        .insert(frame.partition, frame.position.clone());
                    return true;
                }
                Err(error @ ProjectionError::PositionMismatch { .. }) => {
                    tracing::warn!(%error, "the search projection diverged; rebuilding the partition");
                    if !self.rebuild_partition(frame.partition).await {
                        return false;
                    }
                    if self.is_projected(frame.partition, &frame.position) {
                        return true;
                    }
                }
                Err(error) => {
                    if !error.is_retryable() {
                        self.frames
                            .record_permanent_failure(frame.partition, &error.to_string());
                        return false;
                    }
                    tracing::warn!(
                        partition = frame.partition.0,
                        %error,
                        "projecting a committed frame into ParadeDB failed; retrying"
                    );
                    retry_delay.wait().await;
                }
            }
        }
    }

    async fn rebuild_partition(&mut self, partition: PartitionId) -> bool {
        match retry_partition_rebuild(self, partition).await {
            Ok(()) => true,
            Err(error) => {
                self.frames
                    .record_permanent_failure(partition, &error.to_string());
                false
            }
        }
    }

    async fn try_rebuild_partition(
        &mut self,
        partition: PartitionId,
    ) -> Result<(), SearchProjectionError> {
        let (view, committed_position) = self.open_committed_view(partition).await?;
        let Some(committed_position) = committed_position else {
            return Ok(());
        };
        self.projector
            .rebuild_partition(partition, &committed_position, Arc::new(view))
            .await?;
        self.projected_positions
            .insert(partition, committed_position);
        Ok(())
    }

    /// Returns a committed view and the partition's applied position in it, if any.
    async fn open_committed_view(
        &self,
        partition: PartitionId,
    ) -> Result<(CommittedStateView, Option<LogPosition>), SearchProjectionError> {
        let committer = self.committer.clone();
        let opened = tokio::task::spawn_blocking(move || {
            let view = committer
                .committed_view()
                .map_err(|error| error.to_string())?;
            let applied_position = view
                .get(ColumnFamily::PartitionMeta, &partition_meta_key(partition))
                .map_err(|error| error.to_string())?
                .map(|bytes| PartitionMeta::decode(bytes.as_slice()))
                .transpose()
                .map_err(|error| format!("decode partition metadata: {error}"))?
                .and_then(|meta| meta.applied);
            Ok((view, applied_position))
        })
        .await
        .map_err(|error| error.to_string())
        .and_then(|opened| opened);
        opened.map_err(|detail| SearchProjectionError::CommittedState { detail })
    }

    /// Positions of one node order by incarnation, then seq; other nodes' positions never compare.
    fn is_projected(&self, partition: PartitionId, position: &LogPosition) -> bool {
        self.projected_positions
            .get(&partition)
            .is_some_and(|projected| {
                projected.node_id == position.node_id
                    && (projected.incarnation, projected.seq)
                        >= (position.incarnation, position.seq)
            })
    }
}

#[async_trait]
trait PartitionRebuilder: Send {
    fn last_sequence(&self) -> u64;
    async fn attempt_rebuild(
        &mut self,
        partition: PartitionId,
    ) -> Result<(), SearchProjectionError>;
    fn finish_rebuild(&self, partition: PartitionId, covered_sequence: u64);
}

#[async_trait]
impl PartitionRebuilder for SearchProjectionWorker {
    fn last_sequence(&self) -> u64 {
        self.frames.last_sequence()
    }

    async fn attempt_rebuild(
        &mut self,
        partition: PartitionId,
    ) -> Result<(), SearchProjectionError> {
        self.try_rebuild_partition(partition).await
    }

    fn finish_rebuild(&self, partition: PartitionId, covered_sequence: u64) {
        self.frames.finish_rebuild(partition, covered_sequence);
    }
}

async fn retry_partition_rebuild(
    rebuilder: &mut impl PartitionRebuilder,
    partition: PartitionId,
) -> Result<(), SearchProjectionError> {
    let mut retry_delay = RetryDelay::default();
    loop {
        // Every frame through this sequence is committed before the view opens.
        let covered_sequence = rebuilder.last_sequence();
        match rebuilder.attempt_rebuild(partition).await {
            Ok(()) => {
                rebuilder.finish_rebuild(partition, covered_sequence);
                return Ok(());
            }
            Err(error) => {
                if !error.is_retryable() {
                    return Err(error);
                }
                tracing::warn!(
                    partition = partition.0,
                    %error,
                    "rebuilding the search projection of a partition failed; retrying"
                );
                retry_delay.wait().await;
            }
        }
    }
}

/// Doubles the wait after each failed attempt, up to [`MAX_RETRY_DELAY`].
struct RetryDelay {
    next: Duration,
}

impl Default for RetryDelay {
    fn default() -> Self {
        Self {
            next: FIRST_RETRY_DELAY,
        }
    }
}

impl RetryDelay {
    async fn wait(&mut self) {
        tokio::time::sleep(self.next).await;
        self.next = (self.next * 2).min(MAX_RETRY_DELAY);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use dexzd_keyspace::PartitionId;

    use dexzd_search::paradedb::ProjectionError;

    use super::{PartitionRebuilder, SearchProjectionError, retry_partition_rebuild};

    struct RecoveringRebuilder {
        attempts: u64,
        finished: Mutex<Option<(PartitionId, u64)>>,
    }

    #[async_trait]
    impl PartitionRebuilder for RecoveringRebuilder {
        fn last_sequence(&self) -> u64 {
            self.attempts
        }

        async fn attempt_rebuild(
            &mut self,
            _partition: PartitionId,
        ) -> Result<(), SearchProjectionError> {
            self.attempts += 1;
            if self.attempts < 3 {
                return Err(SearchProjectionError::CommittedState {
                    detail: "search backend unavailable".into(),
                });
            }
            Ok(())
        }

        fn finish_rebuild(&self, partition: PartitionId, covered_sequence: u64) {
            *self
                .finished
                .lock()
                .expect("fake rebuild record is unlocked") = Some((partition, covered_sequence));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn rebuild_retries_until_the_search_backend_recovers() {
        let partition = PartitionId(3);
        let mut rebuilder = RecoveringRebuilder {
            attempts: 0,
            finished: Mutex::new(None),
        };
        let started = tokio::time::Instant::now();
        retry_partition_rebuild(&mut rebuilder, partition)
            .await
            .expect("the backend recovers");
        assert_eq!(rebuilder.attempts, 3);
        assert_eq!(
            tokio::time::Instant::now() - started,
            std::time::Duration::from_millis(300)
        );
        assert_eq!(
            *rebuilder
                .finished
                .lock()
                .expect("fake rebuild record is unlocked"),
            Some((partition, 2))
        );
    }

    struct PermanentlyFailingRebuilder {
        attempts: u64,
    }

    #[async_trait]
    impl PartitionRebuilder for PermanentlyFailingRebuilder {
        fn last_sequence(&self) -> u64 {
            1
        }

        async fn attempt_rebuild(
            &mut self,
            _partition: PartitionId,
        ) -> Result<(), SearchProjectionError> {
            self.attempts += 1;
            Err(ProjectionError::InvalidState("invalid search document".into()).into())
        }

        fn finish_rebuild(&self, _partition: PartitionId, _covered_sequence: u64) {
            panic!("a failed partition cannot become searchable");
        }
    }

    #[tokio::test]
    async fn permanent_rebuild_failure_does_not_retry() {
        let mut rebuilder = PermanentlyFailingRebuilder { attempts: 0 };
        let error = retry_partition_rebuild(&mut rebuilder, PartitionId(3))
            .await
            .expect_err("invalid committed state cannot recover by retrying");
        assert!(error.to_string().contains("invalid search document"));
        assert_eq!(rebuilder.attempts, 1);
    }
}
