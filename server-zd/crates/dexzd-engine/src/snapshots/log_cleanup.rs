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
use dexzd_log::StreamIdentity;
use dexzd_object_store::ObjectStore;
use dexzd_protocol::dexzd::v1::SnapshotManifest;
use prost::Message;

use crate::snapshots::SnapshotError;

const LISTING_PAGE_SIZE: u32 = 1000;

/// Deletes log batches that every partition applied in a snapshot older than the delay.
pub(crate) struct LogCleanup {
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) clock: Arc<dyn WallClock>,
    pub(crate) stream: StreamIdentity,
    pub(crate) partition_count: u32,
    pub(crate) deletion_delay: Duration,
}

impl LogCleanup {
    /// Deletes batches below the bound and returns the first retained seq, or `None` when no
    /// snapshot is old enough to cover every partition.
    pub(crate) async fn run_once(&self, lineage: &str) -> Result<Option<u64>, SnapshotError> {
        let Some(first_retained_seq) = self.covered_bound(lineage).await? else {
            return Ok(None);
        };
        self.record_first_retained_seq(first_retained_seq).await?;
        let deleted = self.delete_batches_before(first_retained_seq).await?;
        if deleted > 0 {
            tracing::info!(
                first_retained_seq,
                deleted,
                "deleted log batches covered by snapshots"
            );
        }
        Ok(Some(first_retained_seq))
    }

    async fn covered_bound(&self, lineage: &str) -> Result<Option<u64>, SnapshotError> {
        let prefix = format!("snapshot/{}/{lineage}/manifest/", self.stream.node_id);
        let keys = self.list_keys(&prefix, None).await?;
        let delay_micros = i64::try_from(self.deletion_delay.as_micros()).unwrap_or(i64::MAX);
        let newest_allowed_micros = self.clock.now_unix_micros().saturating_sub(delay_micros);
        for key in keys.iter().rev() {
            let Some(content) = self.object_store.get(key).await? else {
                continue;
            };
            let manifest = SnapshotManifest::decode(content.bytes)?;
            if manifest.created_at_unix_micros > newest_allowed_micros {
                continue;
            }
            return Ok(self.min_applied_seq(&manifest));
        }
        Ok(None)
    }

    fn min_applied_seq(&self, manifest: &SnapshotManifest) -> Option<u64> {
        let positions: Vec<u64> = manifest
            .partition_positions
            .iter()
            .filter_map(|position| position.applied.as_ref())
            .filter(|applied| {
                applied.node_id == self.stream.node_id
                    && applied.incarnation == self.stream.incarnation
            })
            .map(|applied| applied.seq)
            .collect();
        if positions.len() < self.partition_count as usize {
            return None;
        }
        positions.into_iter().min().filter(|seq| *seq > 1)
    }

    /// Records the bound in the manifest before deleting, so recovery never reads below it.
    async fn record_first_retained_seq(
        &self,
        first_retained_seq: u64,
    ) -> Result<(), SnapshotError> {
        dexzd_cluster_metadata::update_manifest(
            self.object_store.as_ref(),
            self.clock.as_ref(),
            |manifest| {
                let mut change = dexzd_cluster_metadata::Change::Unchanged;
                for segment in manifest
                    .partitions
                    .iter_mut()
                    .flat_map(|partition| partition.segments.iter_mut())
                {
                    if segment.is_open
                        && segment.node_id == self.stream.node_id
                        && segment.incarnation == self.stream.incarnation
                        && segment.start_seq < first_retained_seq
                    {
                        segment.start_seq = first_retained_seq;
                        change = dexzd_cluster_metadata::Change::Changed;
                    }
                }
                Ok(change)
            },
        )
        .await?;
        Ok(())
    }

    async fn delete_batches_before(&self, first_retained_seq: u64) -> Result<usize, SnapshotError> {
        let prefix = format!(
            "log/{}/{:010}/",
            self.stream.node_id, self.stream.incarnation
        );
        let bound_key = self.stream.batch_key(first_retained_seq);
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

    async fn list_keys(
        &self,
        prefix: &str,
        mut start_after: Option<String>,
    ) -> Result<Vec<String>, SnapshotError> {
        let mut keys = Vec::new();
        loop {
            let page = self
                .object_store
                .list(prefix, start_after.as_deref(), LISTING_PAGE_SIZE)
                .await?;
            keys.extend(page.objects.into_iter().map(|object| object.key));
            match page.next_start_after {
                Some(next) => start_after = Some(next),
                None => return Ok(keys),
            }
        }
    }
}
