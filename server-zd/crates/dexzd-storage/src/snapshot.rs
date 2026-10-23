// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Component, Path};
use std::time::Duration;

use dexzd_keyspace::ColumnFamily;
use dexzd_object_store::{ObjectStore, ObjectStoreErrorKind, WriteCondition, WriteOutcome};
use dexzd_protocol::dexzd::v1::{
    LocalStoreKind, PartitionAppliedPosition, SnapshotFile, SnapshotManifest,
};
use prost::Message;
use uuid::Uuid;
use xxhash_rust::xxh3::Xxh3;

use crate::local_store::{
    LocalReadView, LocalStore, ScanDirection, StorageError, StorageResult, WriteBatch,
    new_node_meta,
};
use crate::{InMemoryLocalStore, RocksDbLocalStore, RocksDbStoreOptions};

pub const DEFAULT_SNAPSHOT_RETENTION: usize = 3;

#[derive(Debug, Clone)]
pub struct SnapshotIdentity {
    pub cluster_id: [u8; 16],
    pub node_id: String,
    pub created_at_unix_micros: i64,
}

pub trait SnapshotSource: LocalStore {
    fn export_snapshot(
        &self,
        directory: &Path,
        snapshot_number: u64,
    ) -> StorageResult<(LocalStoreKind, Vec<PartitionAppliedPosition>)>;
    fn snapshot_temporary_parent(&self) -> Option<&Path> {
        None
    }
}

impl SnapshotSource for RocksDbLocalStore {
    fn snapshot_temporary_parent(&self) -> Option<&Path> {
        self.path().parent()
    }

    fn export_snapshot(
        &self,
        directory: &Path,
        _snapshot_number: u64,
    ) -> StorageResult<(LocalStoreKind, Vec<PartitionAppliedPosition>)> {
        self.checkpoint(directory)?;
        let checkpoint = RocksDbLocalStore::open(
            directory,
            RocksDbStoreOptions {
                block_cache_bytes: 1024 * 1024,
            },
        )?;
        let positions = partition_positions(&checkpoint)?;
        drop(checkpoint);
        Ok((LocalStoreKind::Rocksdb, positions))
    }
}

impl SnapshotSource for InMemoryLocalStore {
    fn export_snapshot(
        &self,
        directory: &Path,
        snapshot_number: u64,
    ) -> StorageResult<(LocalStoreKind, Vec<PartitionAppliedPosition>)> {
        fs::create_dir_all(directory)?;
        let serialized = self.serialize()?;
        let captured = InMemoryLocalStore::deserialize(&serialized)?;
        fs::write(
            directory.join(format!("memory-{snapshot_number:020}.store")),
            serialized,
        )?;
        Ok((LocalStoreKind::Memory, partition_positions(&captured)?))
    }
}

pub enum RestoredLocalStore {
    RocksDb(RocksDbLocalStore),
    Memory(InMemoryLocalStore),
}

impl LocalReadView for RestoredLocalStore {
    fn get(&self, cf: ColumnFamily, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        match self {
            Self::RocksDb(store) => store.get(cf, key),
            Self::Memory(store) => store.get(cf, key),
        }
    }

    fn scan(
        &self,
        cf: ColumnFamily,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        direction: ScanDirection,
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        match self {
            Self::RocksDb(store) => store.scan(cf, start, end, direction, limit),
            Self::Memory(store) => store.scan(cf, start, end, direction, limit),
        }
    }
}

impl LocalStore for RestoredLocalStore {
    fn write(&self, batch: WriteBatch) -> StorageResult<()> {
        match self {
            Self::RocksDb(store) => store.write(batch),
            Self::Memory(store) => store.write(batch),
        }
    }

    fn consistent_view(&self) -> StorageResult<Box<dyn LocalReadView + '_>> {
        match self {
            Self::RocksDb(store) => store.consistent_view(),
            Self::Memory(store) => store.consistent_view(),
        }
    }
}

pub async fn create_snapshot<S: SnapshotSource>(
    store: &S,
    object_store: &dyn ObjectStore,
    identity: &SnapshotIdentity,
    snapshot_number: u64,
) -> StorageResult<(String, SnapshotManifest)> {
    validate_path_segment(&identity.node_id)?;
    let node_meta = store
        .node_meta()?
        .ok_or_else(|| StorageError::Invalid("store has no node metadata".into()))?;
    let lineage = parse_lineage(&node_meta.lineage_id)?;
    if snapshot_number <= node_meta.last_snapshot_number {
        return Err(StorageError::Invalid(
            "snapshot number must increase".into(),
        ));
    }
    let lineage_prefix = format!("snapshot/{}/{lineage}", identity.node_id);
    let temporary = SnapshotTemporaryDirectory::create(store.snapshot_temporary_parent())?;
    let checkpoint_path = temporary.path.join("checkpoint");
    let (kind, partition_positions) = store.export_snapshot(&checkpoint_path, snapshot_number)?;
    let mut files = Vec::new();
    for entry in fs::read_dir(&checkpoint_path)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let file_name = entry
            .file_name()
            .into_string()
            .map_err(|_| StorageError::Invalid("snapshot filename is not UTF-8".into()))?;
        validate_path_segment(&file_name)?;
        let path = entry.path();
        let (size_bytes, checksum) = file_checksum(&path)?;
        let immutable_file = file_name.ends_with(".sst") || file_name.ends_with(".blob");
        let object_name = if immutable_file {
            file_name.clone()
        } else {
            format!("{file_name}.{checksum:016x}")
        };
        let object_key = format!("{lineage_prefix}/files/{object_name}");
        match object_store.head(&object_key).await? {
            Some(_) => {
                verify_uploaded_file(object_store, &object_key, size_bytes, checksum).await?
            }
            None => {
                match object_store
                    .put_file(&object_key, &path, WriteCondition::CreateOnly)
                    .await
                {
                    Ok(WriteOutcome::Written { .. }) => {}
                    Ok(WriteOutcome::PreconditionFailed) => {
                        verify_uploaded_file(object_store, &object_key, size_bytes, checksum)
                            .await?
                    }
                    Err(error) if error.kind == ObjectStoreErrorKind::OutcomeUnknown => {
                        verify_uploaded_file(object_store, &object_key, size_bytes, checksum)
                            .await?
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }
        files.push(SnapshotFile {
            object_key,
            relative_path: file_name,
            size_bytes,
            xxh3_64: checksum,
        });
    }
    files.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    let manifest = SnapshotManifest {
        format_version: 1,
        cluster_id: identity.cluster_id.to_vec(),
        node_id: identity.node_id.clone(),
        lineage_id: node_meta.lineage_id.clone(),
        snapshot_number,
        created_at_unix_micros: identity.created_at_unix_micros,
        store_kind: kind.into(),
        files,
        partition_positions,
    };
    let manifest_key = format!("{lineage_prefix}/manifest/{snapshot_number:020}");
    let bytes = manifest.encode_to_vec();
    match object_store
        .put(
            &manifest_key,
            bytes.clone().into(),
            WriteCondition::CreateOnly,
        )
        .await
    {
        Ok(WriteOutcome::Written { .. }) => {}
        Ok(WriteOutcome::PreconditionFailed)
        | Err(dexzd_object_store::ObjectStoreError {
            kind: ObjectStoreErrorKind::OutcomeUnknown,
            ..
        }) => {
            let existing = object_store
                .get(&manifest_key)
                .await?
                .ok_or_else(|| StorageError::Invalid("snapshot manifest vanished".into()))?;
            if existing.bytes.as_ref() != bytes {
                return Err(StorageError::Invalid(
                    "snapshot manifest number already exists with different content".into(),
                ));
            }
        }
        Err(error) => return Err(error.into()),
    }
    let mut updated_meta = node_meta;
    updated_meta.last_snapshot_number = snapshot_number;
    store.put_node_meta(&updated_meta)?;
    Ok((manifest_key, manifest))
}

async fn verify_uploaded_file(
    object_store: &dyn ObjectStore,
    key: &str,
    expected_size: u64,
    expected_checksum: u64,
) -> StorageResult<()> {
    let existing = object_store
        .get(key)
        .await?
        .ok_or_else(|| StorageError::Invalid(format!("snapshot object vanished: {key}")))?;
    if existing.object_size != expected_size
        || xxhash_rust::xxh3::xxh3_64(&existing.bytes) != expected_checksum
    {
        return Err(StorageError::Invalid(format!(
            "snapshot object differs: {key}"
        )));
    }
    Ok(())
}

pub async fn restore_snapshot(
    object_store: &dyn ObjectStore,
    manifest_key: &str,
    target_directory: &Path,
    rocks_options: RocksDbStoreOptions,
) -> StorageResult<RestoredLocalStore> {
    let content = object_store.get(manifest_key).await?.ok_or_else(|| {
        StorageError::Invalid(format!("snapshot manifest does not exist: {manifest_key}"))
    })?;
    let manifest = SnapshotManifest::decode(content.bytes.as_ref())?;
    validate_manifest(manifest_key, &manifest)?;
    if target_directory.exists()
        && fs::read_dir(target_directory)?
            .next()
            .transpose()?
            .is_some()
    {
        return Err(StorageError::Invalid(
            "snapshot target directory is not empty".into(),
        ));
    }
    fs::create_dir_all(target_directory)?;
    for file in &manifest.files {
        let path = target_directory.join(&file.relative_path);
        let downloaded_size = object_store
            .get_to_file(&file.object_key, &path)
            .await?
            .ok_or_else(|| {
                StorageError::Invalid(format!("snapshot file does not exist: {}", file.object_key))
            })?;
        let (size, checksum) = file_checksum(&path)?;
        if downloaded_size != file.size_bytes || size != file.size_bytes || checksum != file.xxh3_64
        {
            return Err(StorageError::Invalid(format!(
                "snapshot file checksum mismatch: {}",
                file.object_key
            )));
        }
    }
    let restored = match LocalStoreKind::try_from(manifest.store_kind)
        .map_err(|_| StorageError::Invalid("unknown snapshot store kind".into()))?
    {
        LocalStoreKind::Rocksdb => {
            RestoredLocalStore::RocksDb(RocksDbLocalStore::open(target_directory, rocks_options)?)
        }
        LocalStoreKind::Memory => {
            if manifest.files.len() != 1 {
                return Err(StorageError::Invalid(
                    "memory snapshot must contain one file".into(),
                ));
            }
            let bytes = fs::read(target_directory.join(&manifest.files[0].relative_path))?;
            RestoredLocalStore::Memory(InMemoryLocalStore::deserialize(&bytes)?)
        }
        LocalStoreKind::Unspecified => {
            return Err(StorageError::Invalid(
                "unspecified snapshot store kind".into(),
            ));
        }
    };
    let mut meta = restored
        .node_meta()?
        .ok_or_else(|| StorageError::Invalid("snapshot has no node metadata".into()))?;
    if meta.lineage_id != manifest.lineage_id {
        return Err(StorageError::Invalid(
            "snapshot lineage differs from manifest".into(),
        ));
    }
    meta.lineage_id = new_node_meta().lineage_id;
    meta.last_snapshot_number = 0;
    restored.put_node_meta(&meta)?;
    Ok(restored)
}

pub async fn retain_snapshots(
    object_store: &dyn ObjectStore,
    node_id: &str,
    lineage_id: &[u8],
    retain_count: usize,
    delete_delay: Duration,
    now_unix_micros: i64,
) -> StorageResult<()> {
    if retain_count == 0 {
        return Err(StorageError::Invalid(
            "snapshot retention count must be positive".into(),
        ));
    }
    validate_path_segment(node_id)?;
    let lineage = parse_lineage(lineage_id)?;
    let prefix = format!("snapshot/{node_id}/{lineage}/manifest/");
    let mut keys = Vec::new();
    let mut start_after = None;
    loop {
        let page = object_store
            .list(&prefix, start_after.as_deref(), 1000)
            .await?;
        keys.extend(page.objects.into_iter().map(|metadata| metadata.key));
        match page.next_start_after {
            Some(next) => start_after = Some(next),
            None => break,
        }
    }
    keys.sort();
    let cutoff_index = keys.len().saturating_sub(retain_count);
    let mut manifests = Vec::new();
    for key in &keys {
        let content = object_store
            .get(key)
            .await?
            .ok_or_else(|| StorageError::Invalid(format!("listed manifest vanished: {key}")))?;
        let manifest = SnapshotManifest::decode(content.bytes.as_ref())?;
        validate_manifest(key, &manifest)?;
        manifests.push(manifest);
    }
    let delay_micros = i64::try_from(delete_delay.as_micros()).unwrap_or(i64::MAX);
    let removable: Vec<usize> = (0..cutoff_index)
        .filter(|index| {
            now_unix_micros.saturating_sub(manifests[*index + retain_count].created_at_unix_micros)
                >= delay_micros
        })
        .collect();
    let retained_files: BTreeSet<&str> = manifests
        .iter()
        .enumerate()
        .filter(|(index, _)| !removable.contains(index))
        .flat_map(|(_, manifest)| manifest.files.iter().map(|file| file.object_key.as_str()))
        .collect();
    let obsolete_files: BTreeSet<&str> = removable
        .iter()
        .flat_map(|index| {
            manifests[*index]
                .files
                .iter()
                .map(|file| file.object_key.as_str())
        })
        .filter(|key| !retained_files.contains(key))
        .collect();
    for index in removable {
        object_store.delete(&keys[index]).await?;
    }
    for key in obsolete_files {
        object_store.delete(key).await?;
    }
    Ok(())
}

fn partition_positions(store: &impl LocalReadView) -> StorageResult<Vec<PartitionAppliedPosition>> {
    Ok(store
        .partition_metas()?
        .into_iter()
        .filter_map(|meta| {
            meta.applied.map(|applied| PartitionAppliedPosition {
                partition: meta.partition,
                applied: Some(applied),
                applied_epoch: meta.applied_epoch,
            })
        })
        .collect())
}

fn parse_lineage(bytes: &[u8]) -> StorageResult<Uuid> {
    let array: [u8; 16] = bytes
        .try_into()
        .map_err(|_| StorageError::Invalid("lineage ID must contain 16 bytes".into()))?;
    let uuid = Uuid::from_bytes(array);
    if uuid.get_version_num() != 7 {
        return Err(StorageError::Invalid("lineage ID must be UUIDv7".into()));
    }
    Ok(uuid)
}

fn validate_path_segment(segment: &str) -> StorageResult<()> {
    let mut components = Path::new(segment).components();
    if segment.is_empty()
        || !matches!(components.next(), Some(Component::Normal(_)))
        || components.next().is_some()
    {
        return Err(StorageError::Invalid(format!(
            "invalid snapshot path segment: {segment:?}"
        )));
    }
    Ok(())
}

fn validate_manifest(key: &str, manifest: &SnapshotManifest) -> StorageResult<()> {
    let lineage = parse_lineage(&manifest.lineage_id)?;
    validate_path_segment(&manifest.node_id)?;
    if manifest.format_version != 1 || manifest.cluster_id.len() != 16 {
        return Err(StorageError::Invalid(
            "invalid snapshot manifest version or cluster ID".into(),
        ));
    }
    if key
        != format!(
            "snapshot/{}/{lineage}/manifest/{:020}",
            manifest.node_id, manifest.snapshot_number
        )
    {
        return Err(StorageError::Invalid(
            "snapshot manifest key does not match identity".into(),
        ));
    }
    for file in &manifest.files {
        validate_path_segment(&file.relative_path)?;
        let file_key_suffix =
            if file.relative_path.ends_with(".sst") || file.relative_path.ends_with(".blob") {
                file.relative_path.clone()
            } else {
                format!("{}.{:016x}", file.relative_path, file.xxh3_64)
            };
        if !file.object_key.starts_with("snapshot/")
            || !file
                .object_key
                .ends_with(&format!("/files/{file_key_suffix}"))
        {
            return Err(StorageError::Invalid(
                "snapshot file key does not match relative path".into(),
            ));
        }
    }
    Ok(())
}

fn file_checksum(path: &Path) -> StorageResult<(u64, u64)> {
    let mut file = File::open(path)?;
    let mut hasher = Xxh3::new();
    let mut size = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        size += read as u64;
        hasher.update(&buffer[..read]);
    }
    Ok((size, hasher.digest()))
}

struct SnapshotTemporaryDirectory {
    path: std::path::PathBuf,
}

impl SnapshotTemporaryDirectory {
    fn create(parent: Option<&Path>) -> StorageResult<Self> {
        let base = parent.map_or_else(std::env::temp_dir, Path::to_path_buf);
        let path = base.join(format!("dexzd-snapshot-{}", Uuid::now_v7()));
        fs::create_dir(&path)?;
        Ok(Self { path })
    }
}

impl Drop for SnapshotTemporaryDirectory {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.path) {
            tracing::warn!(path = %self.path.display(), %error, "failed to remove snapshot temporary directory");
        }
    }
}
