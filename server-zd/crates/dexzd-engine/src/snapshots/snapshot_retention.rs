// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use dexzd_object_store::ObjectStore;
use dexzd_protocol::dexzd::v1::SnapshotManifest;
use prost::Message;
use uuid::Uuid;

use crate::snapshots::SnapshotError;
use crate::snapshots::ownership_fence::OwnershipFence;

const LISTING_PAGE_SIZE: u32 = 1000;

/// Keeps the newest snapshots of this node across every lineage and deletes the rest.
pub(crate) struct SnapshotRetention {
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) node_id: String,
    pub(crate) retained_count: usize,
    pub(crate) deletion_delay: Duration,
}

/// A snapshot manifest and its object key.
pub(crate) struct StoredSnapshot {
    pub(crate) key: String,
    pub(crate) manifest: SnapshotManifest,
}

impl SnapshotRetention {
    /// Deletes snapshots beyond the newest `retained_count` of every lineage combined, once
    /// the snapshot that displaced each one is older than the deletion delay.
    ///
    /// Snapshots in `referenced_keys` are always kept, and so is every file a kept snapshot
    /// references. Returns the kept snapshots, oldest first.
    pub(crate) async fn run_once(
        &self,
        referenced_keys: &BTreeSet<String>,
        fence: &OwnershipFence,
        now_unix_micros: i64,
    ) -> Result<Vec<StoredSnapshot>, SnapshotError> {
        let mut snapshots = self.list_snapshots().await?;
        snapshots.sort_by(|left, right| creation_order(left).cmp(&creation_order(right)));
        let delay_micros = i64::try_from(self.deletion_delay.as_micros()).unwrap_or(i64::MAX);
        let displaced_count = snapshots.len().saturating_sub(self.retained_count);
        let is_removable: Vec<bool> = snapshots
            .iter()
            .enumerate()
            .map(|(index, snapshot)| {
                index < displaced_count
                    && !referenced_keys.contains(&snapshot.key)
                    && now_unix_micros.saturating_sub(
                        snapshots[index + self.retained_count]
                            .manifest
                            .created_at_unix_micros,
                    ) >= delay_micros
            })
            .collect();
        let (removed, kept): (Vec<_>, Vec<_>) = snapshots
            .into_iter()
            .zip(is_removable)
            .partition(|(_, is_removable)| *is_removable);
        let kept: Vec<StoredSnapshot> = kept.into_iter().map(|(snapshot, _)| snapshot).collect();
        if removed.is_empty() {
            return Ok(kept);
        }
        let kept_files: BTreeSet<&str> = kept
            .iter()
            .flat_map(|snapshot| &snapshot.manifest.files)
            .map(|file| file.object_key.as_str())
            .collect();
        let obsolete_files: Vec<String> = removed
            .iter()
            .flat_map(|(snapshot, _)| &snapshot.manifest.files)
            .map(|file| file.object_key.as_str())
            .filter(|key| !kept_files.contains(key))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(str::to_owned)
            .collect();
        let obsolete_manifests: Vec<String> = removed
            .into_iter()
            .map(|(snapshot, _)| snapshot.key)
            .collect();
        fence.ensure_serving()?;
        // Files go first, so a retry after a partial failure still finds them through the manifests.
        for chunk in obsolete_files.chunks(LISTING_PAGE_SIZE as usize) {
            self.object_store.delete_many(chunk).await?;
        }
        for chunk in obsolete_manifests.chunks(LISTING_PAGE_SIZE as usize) {
            self.object_store.delete_many(chunk).await?;
        }
        tracing::info!(
            manifests = obsolete_manifests.len(),
            files = obsolete_files.len(),
            "deleted snapshots beyond the retained count"
        );
        Ok(kept)
    }

    /// Lists and decodes every snapshot manifest of this node, in any lineage.
    async fn list_snapshots(&self) -> Result<Vec<StoredSnapshot>, SnapshotError> {
        let prefix = format!("snapshot/{}/", self.node_id);
        let mut manifest_keys = Vec::new();
        let mut start_after = None;
        loop {
            let page = self
                .object_store
                .list(&prefix, start_after.as_deref(), LISTING_PAGE_SIZE)
                .await?;
            manifest_keys.extend(
                page.objects
                    .into_iter()
                    .map(|object| object.key)
                    .filter(|key| key.split('/').nth(3) == Some("manifest")),
            );
            match page.next_start_after {
                Some(next) => start_after = Some(next),
                None => break,
            }
        }
        let mut snapshots = Vec::with_capacity(manifest_keys.len());
        for key in manifest_keys {
            // A concurrent cleanup may have deleted it after the listing.
            let Some(content) = self.object_store.get(&key).await? else {
                continue;
            };
            let manifest = SnapshotManifest::decode(content.bytes)?;
            self.validate(&key, &manifest)?;
            snapshots.push(StoredSnapshot { key, manifest });
        }
        Ok(snapshots)
    }

    /// Rejects a manifest whose deletion could remove objects outside this node's snapshots.
    fn validate(&self, key: &str, manifest: &SnapshotManifest) -> Result<(), SnapshotError> {
        let lineage = Uuid::from_slice(&manifest.lineage_id)
            .map_err(|error| SnapshotError::Invalid(format!("{key}: {error}")))?;
        let expected_key = format!(
            "snapshot/{}/{lineage}/manifest/{:020}",
            self.node_id, manifest.snapshot_number
        );
        let files_prefix = format!("snapshot/{}/", self.node_id);
        let has_foreign_file = manifest.files.iter().any(|file| {
            !file.object_key.starts_with(&files_prefix) || !file.object_key.contains("/files/")
        });
        if manifest.node_id != self.node_id || key != expected_key || has_foreign_file {
            return Err(SnapshotError::Invalid(format!(
                "snapshot manifest {key} does not match its node, lineage, number, or file keys"
            )));
        }
        Ok(())
    }
}

fn creation_order(snapshot: &StoredSnapshot) -> (i64, &[u8], u64) {
    (
        snapshot.manifest.created_at_unix_micros,
        &snapshot.manifest.lineage_id,
        snapshot.manifest.snapshot_number,
    )
}
