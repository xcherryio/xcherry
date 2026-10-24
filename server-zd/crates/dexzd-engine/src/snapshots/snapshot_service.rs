// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dexzd_clock::WallClock;
use dexzd_cluster_metadata::ManifestChange;
use dexzd_object_store::ObjectStore;
use dexzd_protocol::dexzd::v1::{LogPosition, Manifest, SnapshotManifest, SnapshotReference};
use dexzd_storage::{
    LocalReadView, LocalStore, RestoredLocalStore, SnapshotIdentity, StorageError, create_snapshot,
};
use tokio::sync::{Mutex, OwnedMutexGuard};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use crate::blocking_work::BlockingWorkPlacement;
use crate::commit::{CommitProgress, EngineLocalStore, PartitionApplierPool};
use crate::engine_id_source::EngineIdSource;
use crate::snapshots::log_cleanup::LogCleanup;
use crate::snapshots::ownership_fence::OwnershipFence;
use crate::snapshots::snapshot_retention::{SnapshotCatalog, SnapshotRetention};
use crate::snapshots::snapshot_upload_store::SnapshotUploadStore;
use crate::snapshots::{SnapshotConfig, SnapshotError};

/// Takes snapshots on schedule, records them in the manifest, and cleans up old objects.
pub(crate) struct SnapshotService {
    pub(crate) store: Arc<EngineLocalStore>,
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) clock: Arc<dyn WallClock>,
    pub(crate) appliers: Arc<PartitionApplierPool>,
    pub(crate) progress: Arc<CommitProgress>,
    pub(crate) log_cleanup: LogCleanup,
    pub(crate) retention: SnapshotRetention,
    pub(crate) fence: OwnershipFence,
    pub(crate) cluster_id: [u8; 16],
    /// Epoch of this process; every batch it routes carries it.
    pub(crate) epoch: u64,
    pub(crate) id_source: Arc<dyn EngineIdSource>,
    pub(crate) node_meta_lock: Arc<std::sync::Mutex<()>>,
    pub(crate) config: SnapshotConfig,
    /// Holds manifests retention has read; its operation lock serializes snapshots and cleanup.
    pub(crate) catalog: Mutex<SnapshotCatalog>,
    /// Routed seq that the last successful snapshot covered; 0 before the first one.
    pub(crate) last_snapshot_routed_seq: AtomicU64,
    pub(crate) blocking_work: BlockingWorkPlacement,
}

impl SnapshotService {
    pub(crate) async fn run(self: Arc<Self>, cancellation: CancellationToken) {
        let mut last_attempt = Instant::now();
        let mut is_retrying = false;
        let mut bytes_at_last_snapshot = self.committed_log_bytes();
        let cleanup_interval = self
            .config
            .deletion_delay
            .min(std::time::Duration::from_secs(60))
            .max(std::time::Duration::from_secs(1));
        let mut snapshot_timer = tokio::time::interval(self.config.check_interval);
        snapshot_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut cleanup_timer = tokio::time::interval(cleanup_interval);
        cleanup_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        cleanup_timer.tick().await;
        loop {
            let should_check_snapshot = tokio::select! {
                biased;
                () = cancellation.cancelled() => return,
                _ = self.fence.state.wait_stopped() => return,
                _ = cleanup_timer.tick() => {
                    if let Err(error) = self.clean_orphans().await {
                        tracing::warn!(error = %error, "snapshot orphan cleanup failed; retrying later");
                    }
                    false
                }
                _ = snapshot_timer.tick() => true,
            };
            if !should_check_snapshot {
                continue;
            }
            let committed_bytes = self.committed_log_bytes();
            let elapsed = last_attempt.elapsed();
            let is_due = if is_retrying {
                elapsed >= self.config.retry_delay
            } else {
                elapsed >= self.config.interval
                    || committed_bytes.saturating_sub(bytes_at_last_snapshot)
                        >= self.config.log_threshold_bytes
            };
            if !is_due || !self.has_unsnapshotted_batches() {
                continue;
            }
            last_attempt = Instant::now();
            match self.take_snapshot().await {
                Ok(_) => {
                    is_retrying = false;
                    bytes_at_last_snapshot = committed_bytes;
                }
                Err(error) => {
                    is_retrying = true;
                    tracing::warn!(error = %error, "snapshot failed; retrying later");
                }
            }
        }
    }

    async fn clean_orphans(&self) -> Result<(), SnapshotError> {
        let _operation_guard = self.acquire_operation_lock().await;
        let cluster_manifest = self
            .fence
            .update_manifest(|_| ManifestChange::Unchanged)
            .await?;
        let referenced_keys: BTreeSet<String> = cluster_manifest
            .partitions
            .iter()
            .filter_map(|partition| partition.snapshot.as_ref())
            .map(|snapshot| snapshot.manifest_key.clone())
            .collect();
        let mut catalog = self.take_catalog().await;
        let result = self
            .retention
            .run_once(
                &mut catalog,
                &referenced_keys,
                &self.fence,
                self.clock.now_unix_micros(),
            )
            .await
            .map(|_| ());
        self.restore_catalog(catalog).await;
        result
    }

    /// Uploads a snapshot, points every partition at it, and deletes covered objects.
    ///
    /// Nothing is published or deleted once the node stops serving or another epoch owns the
    /// cluster.
    pub(crate) async fn take_snapshot(&self) -> Result<SnapshotReference, SnapshotError> {
        let _operation_guard = self.acquire_operation_lock().await;
        self.fence.ensure_serving()?;
        let stream = &self.log_cleanup.stream;
        let last_routed_seq = self.progress.last_routed_seq.load(Ordering::Acquire);
        if last_routed_seq > 0 {
            self.appliers
                .advance_applied_position(
                    &LogPosition {
                        node_id: stream.node_id.clone(),
                        incarnation: stream.incarnation,
                        seq: last_routed_seq,
                    },
                    self.epoch,
                )
                .await
                .map_err(|_| SnapshotError::NodeStopped)?;
        }
        let started = Instant::now();
        let (manifest_key, manifest) = self.upload_snapshot().await?;
        self.catalog
            .lock()
            .await
            .insert(manifest_key.clone(), manifest.clone());
        let reference = SnapshotReference {
            node_id: stream.node_id.clone(),
            lineage_id: manifest.lineage_id.clone(),
            snapshot_number: manifest.snapshot_number,
            manifest_key,
        };
        let cluster_manifest = self.record_snapshot(&reference).await?;
        tracing::info!(
            snapshot_number = manifest.snapshot_number,
            files = manifest.files.len(),
            elapsed_ms = started.elapsed().as_millis(),
            "snapshot complete"
        );
        let referenced_keys: BTreeSet<String> = cluster_manifest
            .partitions
            .iter()
            .filter_map(|partition| partition.snapshot.as_ref())
            .map(|snapshot| snapshot.manifest_key.clone())
            .collect();
        self.last_snapshot_routed_seq
            .store(last_routed_seq, Ordering::Release);
        let mut catalog = self.take_catalog().await;
        let cleanup_result = async {
            let retained = self
                .retention
                .run_once(
                    &mut catalog,
                    &referenced_keys,
                    &self.fence,
                    self.clock.now_unix_micros(),
                )
                .await?;
            self.log_cleanup
                .run_once(&retained, &manifest, &self.fence)
                .await?;
            Ok::<(), SnapshotError>(())
        }
        .await;
        self.restore_catalog(catalog).await;
        cleanup_result?;
        Ok(reference)
    }

    async fn acquire_operation_lock(&self) -> OwnedMutexGuard<()> {
        let operation_lock = self.catalog.lock().await.operation_lock.clone();
        operation_lock.lock_owned().await
    }

    async fn take_catalog(&self) -> SnapshotCatalog {
        let mut catalog_guard = self.catalog.lock().await;
        let catalog = std::mem::take(&mut *catalog_guard);
        catalog_guard.operation_lock = catalog.operation_lock.clone();
        catalog
    }

    async fn restore_catalog(&self, catalog: SnapshotCatalog) {
        *self.catalog.lock().await = catalog;
    }

    async fn upload_snapshot(&self) -> Result<(String, SnapshotManifest), SnapshotError> {
        let should_prepare_snapshot_lineage =
            !self.catalog.lock().await.has_prepared_snapshot_lineage;
        let store = self.store.clone();
        let upload_store =
            SnapshotUploadStore::new(self.object_store.clone(), self.fence.state.clone());
        let identity = SnapshotIdentity {
            cluster_id: self.cluster_id,
            node_id: self.log_cleanup.stream.node_id.clone(),
            created_at_unix_micros: self.clock.now_unix_micros(),
        };
        let node_meta_lock = self.node_meta_lock.clone();
        let id_source = self.id_source.clone();
        let created = match self.blocking_work {
            BlockingWorkPlacement::BlockingThreadPool => {
                let runtime = tokio::runtime::Handle::current();
                // Checkpoints and file hashing block, so the whole upload runs on the blocking pool.
                tokio::task::spawn_blocking(move || {
                    // create_snapshot reads NodeMeta first and writes it back before it publishes the manifest.
                    let _node_meta_guard = node_meta_lock
                        .lock()
                        .expect("node metadata lock is not poisoned");
                    let snapshot_number = prepare_snapshot_number(
                        &store,
                        id_source.as_ref(),
                        should_prepare_snapshot_lineage,
                    )?;
                    runtime.block_on(create_store_snapshot(
                        &store,
                        &upload_store,
                        &identity,
                        snapshot_number,
                    ))
                })
                .await
                .map_err(|error| SnapshotError::Invalid(error.to_string()))?
            }
            #[cfg(feature = "test-util")]
            BlockingWorkPlacement::CallingTaskForTest => {
                // On one thread, create_snapshot's NodeMeta read-modify-write cannot interleave.
                let snapshot_number = {
                    let _node_meta_guard = node_meta_lock
                        .lock()
                        .expect("node metadata lock is not poisoned");
                    prepare_snapshot_number(
                        &store,
                        id_source.as_ref(),
                        should_prepare_snapshot_lineage,
                    )
                };
                match snapshot_number {
                    Ok(snapshot_number) => {
                        create_store_snapshot(&store, &upload_store, &identity, snapshot_number)
                            .await
                    }
                    Err(error) => Err(error),
                }
            }
        };
        let result = created.map_err(|error| match self.fence.state.stop_reason() {
            Some(_) => SnapshotError::NodeStopped,
            None => SnapshotError::from(error),
        });
        if result.is_ok() {
            self.catalog.lock().await.has_prepared_snapshot_lineage = true;
        }
        result
    }

    async fn record_snapshot(
        &self,
        reference: &SnapshotReference,
    ) -> Result<Manifest, SnapshotError> {
        self.fence
            .update_manifest(|manifest| {
                let mut change = ManifestChange::Unchanged;
                for partition in &mut manifest.partitions {
                    if partition.snapshot.as_ref() != Some(reference) {
                        partition.snapshot = Some(reference.clone());
                        change = ManifestChange::Changed;
                    }
                }
                change
            })
            .await
    }

    /// Returns whether a batch was routed after the last successful snapshot.
    fn has_unsnapshotted_batches(&self) -> bool {
        self.progress.last_routed_seq.load(Ordering::Acquire)
            != self.last_snapshot_routed_seq.load(Ordering::Acquire)
    }

    fn committed_log_bytes(&self) -> u64 {
        self.progress.committed_log_bytes.load(Ordering::Relaxed)
    }
}

/// Rotates the lineage before this owner's first upload, then returns the next snapshot number.
///
/// Returns a storage error when NodeMeta cannot be read or written.
fn prepare_snapshot_number(
    store: &EngineLocalStore,
    id_source: &dyn EngineIdSource,
    should_prepare_snapshot_lineage: bool,
) -> Result<u64, StorageError> {
    if should_prepare_snapshot_lineage {
        let mut node_meta = store
            .node_meta()?
            .ok_or_else(|| StorageError::Invalid("store has no node metadata".into()))?;
        // Reused local stores retain their lineage; rotate it before this owner's first upload.
        node_meta.lineage_id = id_source.lineage_id().as_bytes().to_vec();
        node_meta.last_snapshot_number = 0;
        store.put_node_meta(&node_meta)?;
    }
    Ok(store
        .node_meta()?
        .map_or(0, |meta| meta.last_snapshot_number)
        + 1)
}

/// Creates snapshot `snapshot_number` of `store` and uploads it.
///
/// Returns the manifest key and manifest, or the storage error that stopped the upload.
async fn create_store_snapshot(
    store: &EngineLocalStore,
    upload_store: &SnapshotUploadStore,
    identity: &SnapshotIdentity,
    snapshot_number: u64,
) -> Result<(String, SnapshotManifest), StorageError> {
    match store {
        RestoredLocalStore::RocksDb(rocks) => {
            create_snapshot(rocks, upload_store, identity, snapshot_number).await
        }
        RestoredLocalStore::Memory(memory) => {
            create_snapshot(memory, upload_store, identity, snapshot_number).await
        }
    }
}
