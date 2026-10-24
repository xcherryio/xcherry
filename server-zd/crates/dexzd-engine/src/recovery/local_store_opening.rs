// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::fmt;
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
#[derive(Clone)]
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
    /// A store an earlier simulated process left; one the retained log cannot update gives way
    /// to the newest snapshot.
    #[cfg(feature = "test-util")]
    ExistingForTest(Arc<EngineLocalStore>),
}

impl fmt::Debug for LocalStoreConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RocksDb { directory, options } => formatter
                .debug_struct("RocksDb")
                .field("directory", directory)
                .field("options", options)
                .finish(),
            Self::InMemory => formatter.write_str("InMemory"),
            #[cfg(feature = "test-util")]
            Self::ExistingForTest(_) => formatter.write_str("ExistingForTest"),
        }
    }
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
        #[cfg(feature = "test-util")]
        LocalStoreConfig::ExistingForTest(store) => {
            match reusable_existing_store(store, requirements)? {
                Some(store) => return Ok(store),
                None => restore_in_memory(object_store, requirements).await?,
            }
        }
        LocalStoreConfig::InMemory => restore_in_memory(object_store, requirements).await?,
    };
    if let Some(gap) = find_unreplayable_gap(&store, requirements)? {
        return Err(gap);
    }
    Ok(Arc::new(store))
}

/// Returns `store` when the retained log can update it, like a reopened RocksDB directory.
#[cfg(feature = "test-util")]
fn reusable_existing_store(
    store: &Arc<EngineLocalStore>,
    requirements: &LocalStoreRequirements<'_>,
) -> Result<Option<Arc<EngineLocalStore>>, EngineStartError> {
    if requirements.snapshot.is_some()
        && lowest_applied_position(store.as_ref(), requirements.partition_count)?.is_none()
    {
        tracing::warn!(
            "a partition of the existing local store applied no batch; restoring the newest snapshot"
        );
        return Ok(None);
    }
    match find_unreplayable_gap(store.as_ref(), requirements)? {
        None => Ok(Some(Arc::clone(store))),
        Some(gap) => {
            tracing::warn!(gap = %gap, "the retained log cannot update the existing local store; restoring the newest snapshot");
            Ok(None)
        }
    }
}

/// Restores the newest snapshot into a new in-memory store, or returns an empty store.
async fn restore_in_memory(
    object_store: &dyn ObjectStore,
    requirements: &LocalStoreRequirements<'_>,
) -> Result<RestoredLocalStore, EngineStartError> {
    Ok(match requirements.snapshot {
        Some(snapshot) => {
            let directory = std::env::temp_dir().join(format!("dexzd-restore-{}", Uuid::now_v7()));
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
    })
}

async fn open_rocksdb_store(
    directory: &Path,
    options: RocksDbStoreOptions,
    object_store: &dyn ObjectStore,
    requirements: &LocalStoreRequirements<'_>,
) -> Result<RestoredLocalStore, EngineStartError> {
    if is_nonempty_directory(directory)? {
        match RocksDbLocalStore::open(directory, options) {
            Ok(store)
                if requirements.snapshot.is_some()
                    && lowest_applied_position(&store, requirements.partition_count)?.is_none() =>
            {
                // Log cleanup may have deleted streams older than the oldest recorded one.
                tracing::warn!(
                    "a partition of the local store applied no batch; restoring the newest snapshot"
                );
            }
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
    let segments = &requirements.streams.segments;
    // Cleanup drops older segments whole, so the rest of an unrecorded stream is gone.
    if let Some(applied) = applied
        && !segments
            .iter()
            .any(|segment| segment.incarnation == applied.incarnation)
    {
        return Ok(Some(EngineStartError::LogStreamDeleted {
            incarnation: applied.incarnation,
            resumes_at: applied.seq + 1,
            oldest_retained_incarnation: segments[0].incarnation,
        }));
    }
    for segment in segments {
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
