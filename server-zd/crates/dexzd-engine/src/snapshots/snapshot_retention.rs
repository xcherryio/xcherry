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

use dexzd_object_store::{ObjectMetadata, ObjectStore};
use dexzd_protocol::dexzd::v1::SnapshotManifest;
use prost::Message;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::snapshots::SnapshotError;
use crate::snapshots::ownership_fence::OwnershipFence;

const LISTING_PAGE_SIZE: u32 = 1000;
const LISTING_PAGES_PER_RUN: usize = 10;
const CANDIDATES_PER_RUN: usize = 64;

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
    pub(crate) operation_lock: Arc<Mutex<()>>,
    manifests: BTreeMap<String, SnapshotManifest>,
    listing_start_after: Option<String>,
    listed_manifest_keys: BTreeSet<String>,
    listed_files: BTreeMap<String, ObjectMetadata>,
    inserted_manifest_keys: BTreeSet<String>,
    orphan_first_seen: BTreeMap<String, OrphanMark>,
    completed_scan_number: u64,
    candidate_cursor: Option<String>,
}

struct OrphanMark {
    etag: String,
    first_seen_unix_micros: i64,
    scan_number: u64,
}

impl SnapshotCatalog {
    /// Adds a manifest that this process just wrote.
    pub(crate) fn insert(&mut self, key: String, manifest: SnapshotManifest) {
        self.inserted_manifest_keys.insert(key.clone());
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
        if !self.refresh(catalog).await? {
            return Ok(Self::kept(catalog));
        }
        let obsolete_manifests: Vec<String> = self
            .find_obsolete(catalog, referenced_keys, now_unix_micros)
            .into_iter()
            .take(CANDIDATES_PER_RUN)
            .collect();
        let orphan_files = self.find_orphan_files(catalog, now_unix_micros).await?;
        catalog.listed_files.clear();
        if !obsolete_manifests.is_empty() {
            // Files follow in a later sweep, so retries find them through the listing after partial manifest deletion.
            for chunk in obsolete_manifests.chunks(LISTING_PAGE_SIZE as usize) {
                fence.ensure_serving()?;
                self.object_store.delete_many(chunk).await?;
            }
            for key in &obsolete_manifests {
                catalog.manifests.remove(key);
            }
            tracing::info!(
                manifests = obsolete_manifests.len(),
                "deleted snapshots beyond the retained count"
            );
        }
        for chunk in orphan_files.chunks(LISTING_PAGE_SIZE as usize) {
            fence.ensure_serving()?;
            self.object_store.delete_many(chunk).await?;
        }
        if !orphan_files.is_empty() {
            tracing::info!(
                files = orphan_files.len(),
                "deleted unreferenced snapshot files"
            );
        }
        Ok(Self::kept(catalog))
    }

    fn kept(catalog: &SnapshotCatalog) -> Vec<&SnapshotManifest> {
        let mut kept: Vec<&SnapshotManifest> = catalog.manifests.values().collect();
        kept.sort_by(|left, right| creation_order(left).cmp(&creation_order(right)));
        kept
    }

    async fn find_orphan_files(
        &self,
        catalog: &mut SnapshotCatalog,
        now_unix_micros: i64,
    ) -> Result<Vec<String>, SnapshotError> {
        let referenced_files: BTreeSet<&str> = catalog
            .manifests
            .values()
            .flat_map(|manifest| &manifest.files)
            .map(|file| file.object_key.as_str())
            .collect();
        catalog.orphan_first_seen.retain(|key, _| {
            catalog.listed_files.contains_key(key) && !referenced_files.contains(key.as_str())
        });
        let delay_micros = i64::try_from(self.deletion_delay.as_micros()).unwrap_or(i64::MAX);
        let mut orphan_files = Vec::new();
        let mut checked_candidates = 0;
        let mut last_candidate = None;
        for (key, listed_file) in &catalog.listed_files {
            if referenced_files.contains(key.as_str()) {
                continue;
            }
            let first_seen = catalog
                .orphan_first_seen
                .entry(key.clone())
                .or_insert_with(|| OrphanMark {
                    etag: listed_file.etag.clone(),
                    first_seen_unix_micros: now_unix_micros,
                    scan_number: catalog.completed_scan_number,
                });
            if first_seen.etag != listed_file.etag {
                *first_seen = OrphanMark {
                    etag: listed_file.etag.clone(),
                    first_seen_unix_micros: now_unix_micros,
                    scan_number: catalog.completed_scan_number,
                };
            }
            if first_seen.scan_number == catalog.completed_scan_number
                || now_unix_micros.saturating_sub(first_seen.first_seen_unix_micros) < delay_micros
            {
                continue;
            }
            let written_at = listed_file
                .last_modified_unix_micros
                .unwrap_or(first_seen.first_seen_unix_micros);
            if now_unix_micros.saturating_sub(written_at) < delay_micros {
                continue;
            }
            if catalog
                .candidate_cursor
                .as_ref()
                .is_some_and(|cursor| key <= cursor)
            {
                continue;
            }
            if checked_candidates == CANDIDATES_PER_RUN {
                break;
            }
            checked_candidates += 1;
            last_candidate = Some(key.clone());
            let Some(current_file) = self.object_store.head(key).await? else {
                continue;
            };
            let current_written_at = current_file
                .last_modified_unix_micros
                .unwrap_or(first_seen.first_seen_unix_micros);
            if current_file.etag == listed_file.etag
                && now_unix_micros.saturating_sub(current_written_at) >= delay_micros
            {
                orphan_files.push(key.clone());
            }
        }
        catalog.candidate_cursor = last_candidate;
        Ok(orphan_files)
    }

    /// Returns manifest keys to delete; files that only they reference enter a later sweep.
    fn find_obsolete(
        &self,
        catalog: &SnapshotCatalog,
        referenced_keys: &BTreeSet<String>,
        now_unix_micros: i64,
    ) -> Vec<String> {
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
        let removed: Vec<(&String, &SnapshotManifest)> = snapshots
            .iter()
            .zip(&is_removable)
            .filter(|(_, is_removable)| **is_removable)
            .map(|(snapshot, _)| *snapshot)
            .collect();
        removed.iter().map(|(key, _)| (*key).clone()).collect()
    }

    /// Reconciles the catalog with a listing and reads only manifests it has not seen.
    async fn refresh(&self, catalog: &mut SnapshotCatalog) -> Result<bool, SnapshotError> {
        let prefix = format!("snapshot/{}/", self.node_id);
        for _ in 0..LISTING_PAGES_PER_RUN {
            let page = self
                .object_store
                .list(
                    &prefix,
                    catalog.listing_start_after.as_deref(),
                    LISTING_PAGE_SIZE,
                )
                .await?;
            for object in page.objects {
                match object.key.split('/').nth(3) {
                    Some("manifest") => {
                        catalog.listed_manifest_keys.insert(object.key);
                    }
                    Some("files") => {
                        catalog.listed_files.insert(object.key.clone(), object);
                    }
                    _ => {}
                }
            }
            match page.next_start_after {
                Some(next) => catalog.listing_start_after = Some(next),
                None => {
                    catalog.listing_start_after = None;
                    break;
                }
            }
        }
        if catalog.listing_start_after.is_some() {
            return Ok(false);
        }
        catalog.completed_scan_number = catalog.completed_scan_number.saturating_add(1);
        catalog.manifests.retain(|key, _| {
            catalog.listed_manifest_keys.contains(key)
                || catalog.inserted_manifest_keys.contains(key)
        });
        let listed_keys = std::mem::take(&mut catalog.listed_manifest_keys);
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
        catalog.inserted_manifest_keys.clear();
        Ok(true)
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use dexzd_clock::SystemWallClock;
    use dexzd_object_store::{
        InMemoryObjectStore, ObjectStore, ObjectStoreOperation, WriteCondition,
    };
    use dexzd_protocol::dexzd::v1::{SnapshotFile, SnapshotManifest};
    use prost::Message;
    use uuid::Uuid;

    use crate::commit::{CommitState, NodeStopReason};
    use crate::snapshots::SnapshotError;
    use crate::snapshots::ownership_fence::OwnershipFence;

    use super::{CANDIDATES_PER_RUN, SnapshotCatalog, SnapshotRetention};

    #[tokio::test]
    async fn orphan_sweep_limits_head_and_delete_work_per_pass() {
        let object_store = Arc::new(InMemoryObjectStore::new());
        for index in 0..(CANDIDATES_PER_RUN + 1) {
            let key = format!("snapshot/node-a/lineage/files/orphan-{index:03}");
            object_store
                .put(&key, Bytes::new(), WriteCondition::CreateOnly)
                .await
                .expect("upload orphan");
        }
        let state = Arc::new(CommitState::new(1));
        assert!(state.start_serving());
        let fence = OwnershipFence {
            object_store: object_store.clone(),
            clock: Arc::new(SystemWallClock),
            state,
            node_id: "node-a".to_owned(),
            epoch: 1,
        };
        let retention = SnapshotRetention {
            object_store: object_store.clone(),
            node_id: "node-a".to_owned(),
            retained_count: 3,
            deletion_delay: Duration::ZERO,
        };
        let mut catalog = SnapshotCatalog::default();
        retention
            .run_once(&mut catalog, &BTreeSet::new(), &fence, i64::MAX)
            .await
            .expect("mark orphans");
        retention
            .run_once(&mut catalog, &BTreeSet::new(), &fence, i64::MAX)
            .await
            .expect("first bounded sweep");
        assert_eq!(
            object_store.call_count(ObjectStoreOperation::Head, "snapshot/node-a/"),
            CANDIDATES_PER_RUN as u64
        );
        assert_eq!(object_store.call_count(ObjectStoreOperation::Delete, ""), 1);
        assert!(
            object_store
                .get("snapshot/node-a/lineage/files/orphan-064")
                .await
                .expect("read remaining orphan")
                .is_some()
        );
        retention
            .run_once(&mut catalog, &BTreeSet::new(), &fence, i64::MAX)
            .await
            .expect("resume sweep");
        assert_eq!(
            object_store.call_count(ObjectStoreOperation::Head, "snapshot/node-a/"),
            (CANDIDATES_PER_RUN + 1) as u64
        );
        assert!(
            object_store
                .get("snapshot/node-a/lineage/files/orphan-064")
                .await
                .expect("read final orphan")
                .is_none()
        );
    }

    #[tokio::test]
    async fn orphan_requires_a_later_manifest_scan_before_deletion() {
        let object_store = Arc::new(InMemoryObjectStore::new());
        let orphan_key = "snapshot/node-a/lineage/files/orphan";
        object_store
            .put(orphan_key, Bytes::new(), WriteCondition::CreateOnly)
            .await
            .expect("upload orphan");
        let state = Arc::new(CommitState::new(1));
        assert!(state.start_serving());
        let fence = OwnershipFence {
            object_store: object_store.clone(),
            clock: Arc::new(SystemWallClock),
            state,
            node_id: "node-a".to_owned(),
            epoch: 1,
        };
        let retention = SnapshotRetention {
            object_store: object_store.clone(),
            node_id: "node-a".to_owned(),
            retained_count: 3,
            deletion_delay: Duration::ZERO,
        };
        let mut catalog = SnapshotCatalog::default();
        retention
            .run_once(&mut catalog, &BTreeSet::new(), &fence, i64::MAX)
            .await
            .expect("mark orphan");
        assert_eq!(object_store.call_count(ObjectStoreOperation::Delete, ""), 0);
        retention
            .run_once(&mut catalog, &BTreeSet::new(), &fence, i64::MAX)
            .await
            .expect("sweep orphan");
        assert!(
            object_store
                .get(orphan_key)
                .await
                .expect("read orphan")
                .is_none()
        );
    }

    #[tokio::test]
    async fn manifest_published_after_mark_protects_its_file_on_sweep() {
        let object_store = Arc::new(InMemoryObjectStore::new());
        let lineage = Uuid::now_v7();
        let file_key = format!("snapshot/node-a/{lineage}/files/file");
        object_store
            .put(&file_key, Bytes::new(), WriteCondition::CreateOnly)
            .await
            .expect("upload file");
        let state = Arc::new(CommitState::new(1));
        assert!(state.start_serving());
        let fence = OwnershipFence {
            object_store: object_store.clone(),
            clock: Arc::new(SystemWallClock),
            state,
            node_id: "node-a".to_owned(),
            epoch: 1,
        };
        let retention = SnapshotRetention {
            object_store: object_store.clone(),
            node_id: "node-a".to_owned(),
            retained_count: 3,
            deletion_delay: Duration::ZERO,
        };
        let mut catalog = SnapshotCatalog::default();
        retention
            .run_once(&mut catalog, &BTreeSet::new(), &fence, i64::MAX)
            .await
            .expect("mark file while manifest is unpublished");
        let manifest = SnapshotManifest {
            node_id: "node-a".to_owned(),
            lineage_id: lineage.as_bytes().to_vec(),
            snapshot_number: 1,
            files: vec![SnapshotFile {
                object_key: file_key.clone(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let manifest_key = format!("snapshot/node-a/{lineage}/manifest/{:020}", 1);
        object_store
            .put(
                &manifest_key,
                Bytes::from(manifest.encode_to_vec()),
                WriteCondition::CreateOnly,
            )
            .await
            .expect("publish delayed manifest");
        retention
            .run_once(&mut catalog, &BTreeSet::new(), &fence, i64::MAX)
            .await
            .expect("sweep after a fresh manifest scan");
        assert!(
            object_store
                .get(&file_key)
                .await
                .expect("read file")
                .is_some()
        );
        assert_eq!(
            object_store.call_count(ObjectStoreOperation::Delete, &file_key),
            0
        );
    }

    #[tokio::test]
    async fn fenced_retention_does_not_delete_an_orphan_file() {
        let object_store = Arc::new(InMemoryObjectStore::new());
        let orphan_key = "snapshot/node-a/lineage/files/orphan";
        object_store
            .put(orphan_key, Bytes::new(), WriteCondition::CreateOnly)
            .await
            .expect("upload orphan");
        let state = Arc::new(CommitState::new(1));
        assert!(state.start_serving());
        let fence = OwnershipFence {
            object_store: object_store.clone(),
            clock: Arc::new(SystemWallClock),
            state,
            node_id: "node-a".to_owned(),
            epoch: 1,
        };
        let retention = SnapshotRetention {
            object_store: object_store.clone(),
            node_id: "node-a".to_owned(),
            retained_count: 3,
            deletion_delay: Duration::ZERO,
        };
        let mut catalog = SnapshotCatalog::default();
        retention
            .run_once(&mut catalog, &BTreeSet::new(), &fence, i64::MAX)
            .await
            .expect("mark orphan");
        fence.state.stop(NodeStopReason::Fenced);
        let result = retention
            .run_once(&mut catalog, &BTreeSet::new(), &fence, i64::MAX)
            .await;
        assert!(matches!(result, Err(SnapshotError::NodeStopped)));
        assert!(
            object_store
                .get(orphan_key)
                .await
                .expect("read orphan")
                .is_some()
        );
        assert_eq!(object_store.call_count(ObjectStoreOperation::Delete, ""), 0);
    }
}
