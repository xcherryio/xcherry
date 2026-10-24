// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::{BTreeMap, BTreeSet};
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

/// Decoded snapshot manifests of this node by object key.
///
/// Manifests never change, so each one is read at most once per process.
#[derive(Default)]
pub(crate) struct SnapshotCatalog {
    manifests: BTreeMap<String, SnapshotManifest>,
}

impl SnapshotCatalog {
    /// Adds a manifest that this process just wrote.
    pub(crate) fn insert(&mut self, key: String, manifest: SnapshotManifest) {
        self.manifests.insert(key, manifest);
    }
}

impl SnapshotRetention {
    /// Deletes snapshots beyond the newest `retained_count` of every lineage combined, once
    /// the snapshot that displaced each one is older than the deletion delay.
    ///
    /// Snapshots in `referenced_keys` are always kept, and so is every file a kept snapshot
    /// references. Returns the kept snapshots, oldest first.
    pub(crate) async fn run_once<'catalog>(
        &self,
        catalog: &'catalog mut SnapshotCatalog,
        referenced_keys: &BTreeSet<String>,
        fence: &OwnershipFence,
        now_unix_micros: i64,
    ) -> Result<Vec<&'catalog SnapshotManifest>, SnapshotError> {
        self.refresh(catalog).await?;
        let (obsolete_manifests, obsolete_files) =
            self.find_obsolete(catalog, referenced_keys, now_unix_micros);
        if !obsolete_manifests.is_empty() {
            fence.ensure_serving()?;
            // Files go first, so a retry after a partial failure still finds them through the manifests.
            for chunk in obsolete_files.chunks(LISTING_PAGE_SIZE as usize) {
                self.object_store.delete_many(chunk).await?;
            }
            for chunk in obsolete_manifests.chunks(LISTING_PAGE_SIZE as usize) {
                self.object_store.delete_many(chunk).await?;
            }
            for key in &obsolete_manifests {
                catalog.manifests.remove(key);
            }
            tracing::info!(
                manifests = obsolete_manifests.len(),
                files = obsolete_files.len(),
                "deleted snapshots beyond the retained count"
            );
        }
        let catalog: &'catalog SnapshotCatalog = catalog;
        let mut kept: Vec<&SnapshotManifest> = catalog.manifests.values().collect();
        kept.sort_by(|left, right| creation_order(left).cmp(&creation_order(right)));
        Ok(kept)
    }

    /// Returns the manifest keys to delete and the files that only those manifests reference.
    fn find_obsolete(
        &self,
        catalog: &SnapshotCatalog,
        referenced_keys: &BTreeSet<String>,
        now_unix_micros: i64,
    ) -> (Vec<String>, Vec<String>) {
        let mut snapshots: Vec<(&String, &SnapshotManifest)> = catalog.manifests.iter().collect();
        snapshots.sort_by(|left, right| creation_order(left.1).cmp(&creation_order(right.1)));
        let delay_micros = i64::try_from(self.deletion_delay.as_micros()).unwrap_or(i64::MAX);
        let displaced_count = snapshots.len().saturating_sub(self.retained_count);
        let is_removable: Vec<bool> = snapshots
            .iter()
            .enumerate()
            .map(|(index, (key, _))| {
                index < displaced_count
                    && !referenced_keys.contains(*key)
                    && now_unix_micros.saturating_sub(
                        snapshots[index + self.retained_count]
                            .1
                            .created_at_unix_micros,
                    ) >= delay_micros
            })
            .collect();
        let kept_files: BTreeSet<&str> = snapshots
            .iter()
            .zip(&is_removable)
            .filter(|(_, is_removable)| !**is_removable)
            .flat_map(|((_, manifest), _)| &manifest.files)
            .map(|file| file.object_key.as_str())
            .collect();
        let removed: Vec<(&String, &SnapshotManifest)> = snapshots
            .iter()
            .zip(&is_removable)
            .filter(|(_, is_removable)| **is_removable)
            .map(|(snapshot, _)| *snapshot)
            .collect();
        let obsolete_files: BTreeSet<&str> = removed
            .iter()
            .flat_map(|(_, manifest)| &manifest.files)
            .map(|file| file.object_key.as_str())
            .filter(|key| !kept_files.contains(key))
            .collect();
        (
            removed.iter().map(|(key, _)| (*key).clone()).collect(),
            obsolete_files.into_iter().map(str::to_owned).collect(),
        )
    }

    /// Reconciles the catalog with a listing and reads only manifests it has not seen.
    async fn refresh(&self, catalog: &mut SnapshotCatalog) -> Result<(), SnapshotError> {
        let prefix = format!("snapshot/{}/", self.node_id);
        let mut listed_keys = BTreeSet::new();
        let mut start_after = None;
        loop {
            let page = self
                .object_store
                .list(&prefix, start_after.as_deref(), LISTING_PAGE_SIZE)
                .await?;
            listed_keys.extend(
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
        catalog.manifests.retain(|key, _| listed_keys.contains(key));
        for key in listed_keys {
            if catalog.manifests.contains_key(&key) {
                continue;
            }
            // A concurrent cleanup may have deleted it after the listing.
            let Some(content) = self.object_store.get(&key).await? else {
                continue;
            };
            let manifest = SnapshotManifest::decode(content.bytes)?;
            self.validate(&key, &manifest)?;
            catalog.manifests.insert(key, manifest);
        }
        Ok(())
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

fn creation_order(manifest: &SnapshotManifest) -> (i64, &[u8], u64) {
    (
        manifest.created_at_unix_micros,
        &manifest.lineage_id,
        manifest.snapshot_number,
    )
}
