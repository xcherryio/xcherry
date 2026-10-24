// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use dexzd_clock::WallClock;
use dexzd_cluster_metadata::ManifestChange;
use dexzd_object_store::ObjectStore;
use dexzd_protocol::dexzd::v1::{LogPosition, Manifest, SnapshotManifest, SnapshotReference};
use dexzd_storage::{LocalReadView, RestoredLocalStore, SnapshotIdentity, create_snapshot};
use tokio::sync::Mutex;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::commit::{CommitProgress, EngineLocalStore, PartitionApplierPool};
use crate::snapshots::log_cleanup::LogCleanup;
use crate::snapshots::ownership_fence::OwnershipFence;
use crate::snapshots::snapshot_retention::SnapshotRetention;
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
    pub(crate) node_meta_lock: Arc<std::sync::Mutex<()>>,
    pub(crate) config: SnapshotConfig,
    pub(crate) snapshot_lock: Mutex<()>,
}

impl SnapshotService {
    pub(crate) async fn run(self: Arc<Self>, cancellation: CancellationToken) {
        let mut last_attempt = Instant::now();
        let mut is_retrying = false;
        let mut bytes_at_last_snapshot = self.committed_log_bytes();
        loop {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return,
                _ = self.fence.state.wait_stopped() => return,
                () = tokio::time::sleep(self.config.check_interval) => {}
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
            if !is_due {
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

    /// Uploads a snapshot, points every partition at it, and deletes covered objects.
    ///
    /// Nothing is published or deleted once the node stops serving or another epoch owns the
    /// cluster.
    pub(crate) async fn take_snapshot(&self) -> Result<SnapshotReference, SnapshotError> {
        let _serialized = self.snapshot_lock.lock().await;
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
        let retained = self
            .retention
            .run_once(&referenced_keys, &self.fence, self.clock.now_unix_micros())
            .await?;
        self.log_cleanup
            .run_once(&retained, &manifest, &self.fence)
            .await?;
        Ok(reference)
    }

    async fn upload_snapshot(&self) -> Result<(String, SnapshotManifest), SnapshotError> {
        let store = self.store.clone();
        let object_store = self.object_store.clone();
        let identity = SnapshotIdentity {
            cluster_id: self.cluster_id,
            node_id: self.log_cleanup.stream.node_id.clone(),
            created_at_unix_micros: self.clock.now_unix_micros(),
        };
        let runtime = tokio::runtime::Handle::current();
        let node_meta_lock = self.node_meta_lock.clone();
        // Checkpoints and file hashing block, so the whole upload runs on the blocking pool.
        tokio::task::spawn_blocking(move || {
            // create_snapshot reads NodeMeta first and writes it back last.
            let _node_meta_guard = node_meta_lock
                .lock()
                .expect("node metadata lock is not poisoned");
            let snapshot_number = store
                .node_meta()?
                .map_or(0, |meta| meta.last_snapshot_number)
                + 1;
            runtime.block_on(async {
                match store.as_ref() {
                    RestoredLocalStore::RocksDb(rocks) => {
                        create_snapshot(rocks, object_store.as_ref(), &identity, snapshot_number)
                            .await
                    }
                    RestoredLocalStore::Memory(memory) => {
                        create_snapshot(memory, object_store.as_ref(), &identity, snapshot_number)
                            .await
                    }
                }
            })
        })
        .await
        .map_err(|error| SnapshotError::Invalid(error.to_string()))?
        .map_err(SnapshotError::from)
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

    fn committed_log_bytes(&self) -> u64 {
        self.progress.committed_log_bytes.load(Ordering::Relaxed)
    }
}
