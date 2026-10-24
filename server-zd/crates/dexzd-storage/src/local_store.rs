// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_keyspace::{ColumnFamily, PartitionId, decode_partition_meta_key, partition_meta_key};
use dexzd_protocol::dexzd::v1::{NodeMeta, PartitionMeta};
use prost::Message;
use uuid::Uuid;

pub type StorageResult<T> = Result<T, StorageError>;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("RocksDB operation failed: {0}")]
    RocksDb(#[from] rocksdb::Error),
    #[error("object storage operation failed: {0}")]
    ObjectStore(#[from] dexzd_object_store::ObjectStoreError),
    #[error("local file operation failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("persistent message decode failed: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("invalid node ID: {0}")]
    NodeIdentifier(#[from] dexzd_keyspace::NodeIdentifierError),
    #[error("existing RocksDB at {path} has no node metadata")]
    MissingNodeMetadata { path: std::path::PathBuf },
    #[error("invalid range delete in {cf:?}: {start:?}..{end:?}")]
    InvalidRange {
        cf: ColumnFamily,
        start: Vec<u8>,
        end: Vec<u8>,
    },
    #[error("snapshot manifest already exists with different content at {key}")]
    SnapshotManifestConflict { key: String },
    #[error("snapshot file checksum mismatch at {key}")]
    SnapshotChecksumMismatch { key: String },
    #[error("snapshot object differs at {key}")]
    SnapshotObjectMismatch { key: String },
    #[error("invalid local store content: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanDirection {
    Forward,
    Reverse,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOperation {
    Put {
        cf: ColumnFamily,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        cf: ColumnFamily,
        key: Vec<u8>,
    },
    DeleteRange {
        cf: ColumnFamily,
        start: Vec<u8>,
        end: Vec<u8>,
    },
}

#[derive(Debug, Default, Clone)]
pub struct WriteBatch {
    operations: Vec<WriteOperation>,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&mut self, cf: ColumnFamily, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        self.operations.push(WriteOperation::Put {
            cf,
            key: key.into(),
            value: value.into(),
        });
    }

    pub fn delete(&mut self, cf: ColumnFamily, key: impl Into<Vec<u8>>) {
        self.operations.push(WriteOperation::Delete {
            cf,
            key: key.into(),
        });
    }

    pub fn delete_range(
        &mut self,
        cf: ColumnFamily,
        start: impl Into<Vec<u8>>,
        end: impl Into<Vec<u8>>,
    ) {
        self.operations.push(WriteOperation::DeleteRange {
            cf,
            start: start.into(),
            end: end.into(),
        });
    }

    pub fn put_partition_meta(&mut self, meta: &PartitionMeta) -> StorageResult<()> {
        let partition = u16::try_from(meta.partition)
            .map_err(|_| StorageError::Invalid("partition exceeds u16 range".into()))?;
        self.put(
            ColumnFamily::PartitionMeta,
            partition_meta_key(PartitionId(partition)),
            meta.encode_to_vec(),
        );
        Ok(())
    }

    pub fn put_node_meta(&mut self, meta: &NodeMeta) {
        self.put(
            ColumnFamily::NodeMeta,
            b"node".to_vec(),
            meta.encode_to_vec(),
        );
    }

    pub fn operations(&self) -> &[WriteOperation] {
        &self.operations
    }
}

pub trait LocalReadView: Send + Sync {
    fn get(&self, cf: ColumnFamily, key: &[u8]) -> StorageResult<Option<Vec<u8>>>;

    fn multi_get(&self, cf: ColumnFamily, keys: &[&[u8]]) -> StorageResult<Vec<Option<Vec<u8>>>> {
        keys.iter().map(|key| self.get(cf, key)).collect()
    }

    fn scan(
        &self,
        cf: ColumnFamily,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        direction: ScanDirection,
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>>;

    fn scan_prefix(
        &self,
        cf: ColumnFamily,
        prefix: &[u8],
        direction: ScanDirection,
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let end = prefix_successor(prefix);
        let rows = self.scan(cf, Some(prefix), end.as_deref(), direction, limit)?;
        Ok(rows
            .into_iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .collect())
    }

    fn partition_meta(&self, partition: PartitionId) -> StorageResult<Option<PartitionMeta>> {
        self.get(ColumnFamily::PartitionMeta, &partition_meta_key(partition))?
            .map(|bytes| PartitionMeta::decode(bytes.as_slice()).map_err(StorageError::from))
            .transpose()
    }

    fn node_meta(&self) -> StorageResult<Option<NodeMeta>> {
        self.get(ColumnFamily::NodeMeta, b"node")?
            .map(|bytes| NodeMeta::decode(bytes.as_slice()).map_err(StorageError::from))
            .transpose()
    }

    fn partition_metas(&self) -> StorageResult<Vec<PartitionMeta>> {
        self.scan(
            ColumnFamily::PartitionMeta,
            None,
            None,
            ScanDirection::Forward,
            usize::MAX,
        )?
        .into_iter()
        .map(|(key, value)| {
            let partition = decode_partition_meta_key(&key)
                .map_err(|error| StorageError::Invalid(error.to_string()))?;
            let meta = PartitionMeta::decode(value.as_slice())?;
            if meta.partition != u32::from(partition.0) {
                return Err(StorageError::Invalid(format!(
                    "partition metadata key/value mismatch for {}",
                    partition.0
                )));
            }
            Ok(meta)
        })
        .collect()
    }
}

pub trait LocalStore: LocalReadView + Send + Sync {
    fn write(&self, batch: WriteBatch) -> StorageResult<()>;
    fn consistent_view(&self) -> StorageResult<Box<dyn LocalReadView + Send + Sync + '_>>;

    fn put_partition_meta(&self, meta: &PartitionMeta) -> StorageResult<()> {
        let mut batch = WriteBatch::new();
        batch.put_partition_meta(meta)?;
        self.write(batch)
    }

    fn put_node_meta(&self, meta: &NodeMeta) -> StorageResult<()> {
        let mut batch = WriteBatch::new();
        batch.put_node_meta(meta);
        self.write(batch)
    }
}

pub(crate) fn new_node_meta() -> NodeMeta {
    NodeMeta {
        lineage_id: Uuid::now_v7().as_bytes().to_vec(),
        last_snapshot_number: 0,
        pending_hard_delete_uploads: Vec::new(),
    }
}

fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    for index in (0..end.len()).rev() {
        if end[index] != u8::MAX {
            end[index] += 1;
            end.truncate(index + 1);
            return Some(end);
        }
    }
    None
}
