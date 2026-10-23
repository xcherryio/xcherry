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
    pub(crate) first_retained_seq: u64,
    pub(crate) partition_count: u32,
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
    let resumes_at = min_applied_seq(&store, requirements.partition_count)? + 1;
    if resumes_at < requirements.first_retained_seq {
        return Err(EngineStartError::LogUnavailable {
            first_retained_seq: requirements.first_retained_seq,
            resumes_at,
        });
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
            Ok(store) => {
                let resumes_at = min_applied_seq(&store, requirements.partition_count)? + 1;
                if resumes_at >= requirements.first_retained_seq {
                    return Ok(RestoredLocalStore::RocksDb(store));
                }
                tracing::warn!(
                    resumes_at,
                    first_retained_seq = requirements.first_retained_seq,
                    "local store is older than the retained log; restoring the newest snapshot"
                );
            }
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

/// Returns the lowest applied seq over every partition; 0 when a partition applied nothing.
pub(crate) fn min_applied_seq(
    store: &dyn LocalReadView,
    partition_count: u32,
) -> Result<u64, EngineStartError> {
    let metas = store.partition_metas()?;
    if metas.len() < partition_count as usize {
        return Ok(0);
    }
    Ok(metas
        .iter()
        .map(|meta| meta.applied.as_ref().map_or(0, |applied| applied.seq))
        .min()
        .unwrap_or(0))
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
