// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::path::{Path, PathBuf};

use dexzd_keyspace::ColumnFamily;
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DB, DBCompressionType, Direction,
    IteratorMode, Options, Snapshot, WriteOptions,
};

use crate::local_store::{
    LocalReadView, LocalStore, ScanDirection, StorageError, StorageResult, WriteBatch,
    WriteOperation, new_node_meta,
};

#[derive(Debug, Clone, Copy)]
pub struct RocksDbStoreOptions {
    pub block_cache_bytes: usize,
}

impl Default for RocksDbStoreOptions {
    fn default() -> Self {
        Self {
            block_cache_bytes: 128 * 1024 * 1024,
        }
    }
}

pub struct RocksDbLocalStore {
    db: DB,
    path: PathBuf,
}

impl RocksDbLocalStore {
    pub fn open(path: impl AsRef<Path>, options: RocksDbStoreOptions) -> StorageResult<Self> {
        let is_new_store = !path.as_ref().exists()
            || std::fs::read_dir(path.as_ref())?
                .next()
                .transpose()?
                .is_none();
        let mut db_options = Options::default();
        db_options.create_if_missing(true);
        db_options.create_missing_column_families(true);
        let cache = Cache::new_lru_cache(options.block_cache_bytes);
        let descriptors = ColumnFamily::ALL.into_iter().map(|cf| {
            let mut cf_options = Options::default();
            cf_options.set_compression_type(DBCompressionType::Lz4);
            let mut block_options = BlockBasedOptions::default();
            block_options.set_block_cache(&cache);
            cf_options.set_block_based_table_factory(&block_options);
            if cf == ColumnFamily::Value {
                cf_options.set_enable_blob_files(true);
                cf_options.set_min_blob_size(64 * 1024);
                cf_options.set_enable_blob_gc(true);
            }
            ColumnFamilyDescriptor::new(cf.name(), cf_options)
        });
        let db = DB::open_cf_descriptors(&db_options, path.as_ref(), descriptors)?;
        let store = Self {
            db,
            path: path.as_ref().to_path_buf(),
        };
        if store.node_meta()?.is_none() {
            if !is_new_store {
                return Err(StorageError::Invalid(
                    "existing RocksDB has no node metadata".into(),
                ));
            }
            store.put_node_meta(&new_node_meta())?;
        }
        Ok(store)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn checkpoint(&self, directory: &Path) -> StorageResult<()> {
        rocksdb::checkpoint::Checkpoint::new(&self.db)?.create_checkpoint(directory)?;
        Ok(())
    }

    fn cf_handle(&self, cf: ColumnFamily) -> &impl rocksdb::AsColumnFamilyRef {
        self.db
            .cf_handle(cf.name())
            .expect("all column families are opened")
    }
}

impl LocalReadView for RocksDbLocalStore {
    fn get(&self, cf: ColumnFamily, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        Ok(self.db.get_cf(self.cf_handle(cf), key)?)
    }

    fn scan(
        &self,
        cf: ColumnFamily,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        direction: ScanDirection,
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        scan_rocksdb(
            self.db
                .iterator_cf(self.cf_handle(cf), iterator_mode(start, end, direction)),
            start,
            end,
            limit,
        )
    }
}

impl LocalStore for RocksDbLocalStore {
    fn write(&self, batch: WriteBatch) -> StorageResult<()> {
        let mut rocks_batch = rocksdb::WriteBatch::default();
        for operation in batch.operations() {
            match operation {
                WriteOperation::Put { cf, key, value } => {
                    rocks_batch.put_cf(self.cf_handle(*cf), key, value)
                }
                WriteOperation::Delete { cf, key } => {
                    rocks_batch.delete_cf(self.cf_handle(*cf), key)
                }
                WriteOperation::DeleteRange { cf, start, end } => {
                    if start >= end {
                        return Err(StorageError::Invalid(
                            "range delete requires start < end".into(),
                        ));
                    }
                    rocks_batch.delete_range_cf(self.cf_handle(*cf), start, end);
                }
            }
        }
        let mut options = WriteOptions::default();
        options.disable_wal(false);
        self.db.write_opt(rocks_batch, &options)?;
        Ok(())
    }

    fn consistent_view(&self) -> StorageResult<Box<dyn LocalReadView + '_>> {
        Ok(Box::new(RocksView {
            store: self,
            snapshot: self.db.snapshot(),
        }))
    }
}

struct RocksView<'a> {
    store: &'a RocksDbLocalStore,
    snapshot: Snapshot<'a>,
}

impl LocalReadView for RocksView<'_> {
    fn get(&self, cf: ColumnFamily, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        Ok(self.snapshot.get_cf(self.store.cf_handle(cf), key)?)
    }

    fn scan(
        &self,
        cf: ColumnFamily,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        direction: ScanDirection,
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        scan_rocksdb(
            self.snapshot.iterator_cf(
                self.store.cf_handle(cf),
                iterator_mode(start, end, direction),
            ),
            start,
            end,
            limit,
        )
    }
}

fn iterator_mode<'a>(
    start: Option<&'a [u8]>,
    end: Option<&'a [u8]>,
    direction: ScanDirection,
) -> IteratorMode<'a> {
    match direction {
        ScanDirection::Forward => start.map_or(IteratorMode::Start, |bound| {
            IteratorMode::From(bound, Direction::Forward)
        }),
        ScanDirection::Reverse => end.map_or(IteratorMode::End, |bound| {
            IteratorMode::From(bound, Direction::Reverse)
        }),
    }
}

fn scan_rocksdb(
    iterator: rocksdb::DBIterator<'_>,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
    limit: usize,
) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut rows = Vec::new();
    if limit == 0 {
        return Ok(rows);
    }
    for result in iterator {
        let (key, value) = result?;
        if start.is_some_and(|bound| key.as_ref() < bound)
            || end.is_some_and(|bound| key.as_ref() >= bound)
        {
            continue;
        }
        rows.push((key.to_vec(), value.to_vec()));
        if rows.len() == limit {
            break;
        }
    }
    Ok(rows)
}
