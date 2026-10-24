// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::path::{Path, PathBuf};
use std::sync::Arc;

use dexzd_object_store::ObjectStore;
use dexzd_protocol::dexzd::v1::SnapshotReference;
use dexzd_storage::{
    InMemoryLocalStore, LocalReadView, RestoredLocalStore, RocksDbLocalStore, RocksDbStoreOptions,
    restore_snapshot,
};
use uuid::Uuid;

use crate::commit::EngineLocalStore;
use crate::recovery::EngineStartError;
use crate::recovery::node_streams::NodeStreams;

/// Where the node keeps its local state.
#[derive(Debug, Clone)]
pub enum LocalStoreConfig {
    /// RocksDB in `directory`. A missing, corrupt, or outdated store is set aside and
    /// restored from the newest snapshot.
    RocksDb {
        /// Store directory; created when missing.
        directory: PathBuf,
        /// RocksDB tuning.
        options: RocksDbStoreOptions,
    },
    /// A new in-memory store restored from the newest snapshot, for simulations and tests.
    InMemory,
}

/// What recovery needs to decide whether the local store can be reused.
pub(crate) struct LocalStoreRequirements<'a> {
    pub(crate) snapshot: Option<&'a SnapshotReference>,
    pub(crate) streams: &'a NodeStreams,
    pub(crate) partition_count: u32,
}

/// The lowest applied batch over every partition, ordered by incarnation, then seq.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AppliedPosition {
    pub(crate) incarnation: u32,
    pub(crate) seq: u64,
    /// Epoch of that batch; see `PartitionMeta.applied_epoch`.
    pub(crate) epoch: u64,
}

pub(crate) async fn open_local_store(
    config: &LocalStoreConfig,
    object_store: &dyn ObjectStore,
    requirements: &LocalStoreRequirements<'_>,
) -> Result<Arc<EngineLocalStore>, EngineStartError> {
    let store = match config {
        LocalStoreConfig::RocksDb { directory, options } => {
            open_rocksdb_store(directory, *options, object_store, requirements).await?
        }
        LocalStoreConfig::InMemory => match requirements.snapshot {
            Some(snapshot) => {
                let directory =
                    std::env::temp_dir().join(format!("dexzd-restore-{}", Uuid::now_v7()));
                let restored = restore_snapshot(
                    object_store,
                    &snapshot.manifest_key,
                    &directory,
                    RocksDbStoreOptions::default(),
                )
                .await?;
                if matches!(restored, RestoredLocalStore::Memory(_))
                    && let Err(error) = std::fs::remove_dir_all(&directory)
                {
                    tracing::warn!(path = %directory.display(), error = %error, "failed to remove snapshot restore directory");
                }
                restored
            }
            None => RestoredLocalStore::Memory(InMemoryLocalStore::new()),
        },
    };
    if let Some(gap) = find_unreplayable_gap(&store, requirements)? {
        return Err(gap);
    }
    Ok(Arc::new(store))
}

async fn open_rocksdb_store(
    directory: &Path,
    options: RocksDbStoreOptions,
    object_store: &dyn ObjectStore,
    requirements: &LocalStoreRequirements<'_>,
) -> Result<RestoredLocalStore, EngineStartError> {
    if is_nonempty_directory(directory)? {
        match RocksDbLocalStore::open(directory, options) {
            Ok(store) => match find_unreplayable_gap(&store, requirements)? {
                None => return Ok(RestoredLocalStore::RocksDb(store)),
                Some(gap) => {
                    tracing::warn!(gap = %gap, "the retained log cannot update the local store; restoring the newest snapshot");
                }
            },
            Err(error) => {
                tracing::warn!(error = %error, "local store cannot be opened; restoring the newest snapshot");
            }
        }
        set_aside(directory)?;
    }
    match requirements.snapshot {
        Some(snapshot) => {
            Ok(restore_snapshot(object_store, &snapshot.manifest_key, directory, options).await?)
        }
        None => Ok(RestoredLocalStore::RocksDb(RocksDbLocalStore::open(
            directory, options,
        )?)),
    }
}

/// Returns why the retained log cannot bring `store` up to date, or `None` when it can.
fn find_unreplayable_gap(
    store: &dyn LocalReadView,
    requirements: &LocalStoreRequirements<'_>,
) -> Result<Option<EngineStartError>, EngineStartError> {
    let applied = lowest_applied_position(store, requirements.partition_count)?;
    let open_incarnation = requirements.streams.open_segment().incarnation;
    if let Some(applied) = applied
        && applied.incarnation > open_incarnation
    {
        return Ok(Some(EngineStartError::InvalidMetadata {
            detail: format!(
                "the local store applied incarnation {}, newer than the recorded incarnation \
                 {open_incarnation}",
                applied.incarnation
            ),
        }));
    }
    for segment in &requirements.streams.segments {
        let resumes_at = match applied {
            Some(applied) if applied.incarnation > segment.incarnation => continue,
            Some(applied) if applied.incarnation == segment.incarnation => applied.seq + 1,
            _ => 1,
        };
        if resumes_at < segment.start_seq {
            return Ok(Some(EngineStartError::LogUnavailable {
                incarnation: segment.incarnation,
                first_retained_seq: segment.start_seq,
                resumes_at,
            }));
        }
    }
    Ok(None)
}

/// Returns the lowest applied position over every partition, or `None` when a partition
/// applied nothing.
pub(crate) fn lowest_applied_position(
    store: &dyn LocalReadView,
    partition_count: u32,
) -> Result<Option<AppliedPosition>, EngineStartError> {
    let metas = store.partition_metas()?;
    if metas.len() < partition_count as usize {
        return Ok(None);
    }
    let mut lowest: Option<AppliedPosition> = None;
    for meta in &metas {
        let Some(applied) = &meta.applied else {
            return Ok(None);
        };
        let position = AppliedPosition {
            incarnation: applied.incarnation,
            seq: applied.seq,
            epoch: meta.applied_epoch,
        };
        if lowest.is_none_or(|lowest| {
            (position.incarnation, position.seq) < (lowest.incarnation, lowest.seq)
        }) {
            lowest = Some(position);
        }
    }
    Ok(lowest)
}

fn is_nonempty_directory(directory: &Path) -> Result<bool, EngineStartError> {
    if !directory.exists() {
        return Ok(false);
    }
    Ok(std::fs::read_dir(directory)?.next().transpose()?.is_some())
}

fn set_aside(directory: &Path) -> Result<(), EngineStartError> {
    let mut discarded = directory.as_os_str().to_owned();
    discarded.push(format!(".discarded-{}", Uuid::now_v7()));
    std::fs::rename(directory, &discarded)?;
    tracing::warn!(path = %PathBuf::from(&discarded).display(), "set aside the previous local store");
    Ok(())
}
