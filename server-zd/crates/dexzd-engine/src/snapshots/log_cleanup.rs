// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;
use std::time::Duration;

use dexzd_clock::WallClock;
use dexzd_cluster_metadata::ManifestChange;
use dexzd_log::StreamIdentity;
use dexzd_object_store::ObjectStore;
use dexzd_protocol::dexzd::v1::{Manifest, SnapshotManifest};

use crate::snapshots::SnapshotError;
use crate::snapshots::ownership_fence::OwnershipFence;
use crate::snapshots::snapshot_retention::StoredSnapshot;

const LISTING_PAGE_SIZE: u32 = 1000;

/// Deletes log batches that every partition applied in a snapshot older than the delay.
pub(crate) struct LogCleanup {
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) clock: Arc<dyn WallClock>,
    /// The stream this node serves; older incarnations of the node are cleaned up too.
    pub(crate) stream: StreamIdentity,
    pub(crate) partition_count: u32,
    pub(crate) deletion_delay: Duration,
}

/// The lowest batch every partition applied, ordered by incarnation, then seq.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct CoveredPosition {
    incarnation: u32,
    seq: u64,
}

impl LogCleanup {
    /// Deletes covered batches.
    ///
    /// Returns the first retained seq, or `None` when no snapshot is old enough.
    ///
    /// Covered batches of older incarnations are deleted too. `snapshots` are the retained
    /// snapshots of this node. `published` is the snapshot the manifest points to, which
    /// recovery restores, so the bound never passes it.
    pub(crate) async fn run_once(
        &self,
        snapshots: &[StoredSnapshot],
        published: &SnapshotManifest,
        fence: &OwnershipFence,
    ) -> Result<Option<u64>, SnapshotError> {
        let Some(covered) = self.covered_position(snapshots, published) else {
            return Ok(None);
        };
        fence
            .update_manifest(|manifest| self.retain_segments_from(manifest, covered))
            .await?;
        fence.ensure_serving()?;
        let deleted = self.delete_batches_before(covered).await?;
        if deleted > 0 {
            tracing::info!(
                incarnation = covered.incarnation,
                first_retained_seq = covered.seq,
                deleted,
                "deleted log batches covered by snapshots"
            );
        }
        Ok(Some(covered.seq))
    }

    /// Returns the position covered by the newest snapshot older than the delay, never beyond
    /// the published snapshot.
    fn covered_position(
        &self,
        snapshots: &[StoredSnapshot],
        published: &SnapshotManifest,
    ) -> Option<CoveredPosition> {
        let delay_micros = i64::try_from(self.deletion_delay.as_micros()).unwrap_or(i64::MAX);
        let newest_allowed_micros = self.clock.now_unix_micros().saturating_sub(delay_micros);
        let old_enough = snapshots
            .iter()
            .rev()
            .map(|snapshot| &snapshot.manifest)
            .filter(|manifest| manifest.created_at_unix_micros <= newest_allowed_micros)
            .find_map(|manifest| self.lowest_applied_position(manifest))?;
        Some(old_enough.min(self.lowest_applied_position(published)?))
    }

    /// Returns `None` unless the manifest holds a position of this node for every partition.
    fn lowest_applied_position(&self, manifest: &SnapshotManifest) -> Option<CoveredPosition> {
        let positions: Vec<CoveredPosition> = manifest
            .partition_positions
            .iter()
            .filter_map(|position| position.applied.as_ref())
            .filter(|applied| applied.node_id == self.stream.node_id)
            .map(|applied| CoveredPosition {
                incarnation: applied.incarnation,
                seq: applied.seq,
            })
            .collect();
        if positions.len() < self.partition_count as usize {
            return None;
        }
        positions.into_iter().min()
    }

    /// Records the bound in the manifest before deleting, so recovery never reads below it.
    ///
    /// Drops closed segments of older incarnations and raises the start of the covered one.
    fn retain_segments_from(
        &self,
        manifest: &mut Manifest,
        covered: CoveredPosition,
    ) -> ManifestChange {
        let mut change = ManifestChange::Unchanged;
        for partition in &mut manifest.partitions {
            let segment_count = partition.segments.len();
            partition.segments.retain(|segment| {
                segment.node_id != self.stream.node_id
                    || segment.is_open
                    || segment.incarnation >= covered.incarnation
            });
            if partition.segments.len() != segment_count {
                change = ManifestChange::Changed;
            }
            for segment in &mut partition.segments {
                if segment.node_id == self.stream.node_id
                    && segment.incarnation == covered.incarnation
                    && segment.start_seq < covered.seq
                {
                    segment.start_seq = covered.seq;
                    change = ManifestChange::Changed;
                }
            }
        }
        change
    }

    /// Deletes every batch of this node's streams that sorts before the covered position.
    async fn delete_batches_before(
        &self,
        covered: CoveredPosition,
    ) -> Result<usize, SnapshotError> {
        let prefix = format!("log/{}/", self.stream.node_id);
        let bound_key = StreamIdentity {
            incarnation: covered.incarnation,
            ..self.stream.clone()
        }
        .batch_key(covered.seq);
        let mut deleted = 0;
        let mut start_after = None;
        loop {
            let page = self
                .object_store
                .list(&prefix, start_after.as_deref(), LISTING_PAGE_SIZE)
                .await?;
            let is_last_page = page.next_start_after.is_none();
            let doomed: Vec<String> = page
                .objects
                .into_iter()
                .map(|object| object.key)
                .take_while(|key| key.as_str() < bound_key.as_str())
                .collect();
            let reached_bound = doomed.len() < LISTING_PAGE_SIZE as usize;
            if let Some(last) = doomed.last() {
                start_after = Some(last.clone());
            }
            if !doomed.is_empty() {
                self.object_store.delete_many(&doomed).await?;
                deleted += doomed.len();
            }
            if is_last_page || reached_bound {
                return Ok(deleted);
            }
        }
    }
}
