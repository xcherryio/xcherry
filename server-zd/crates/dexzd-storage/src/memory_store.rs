// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;
use std::sync::RwLock;

use dexzd_keyspace::ColumnFamily;

use crate::local_store::{
    LocalReadView, LocalStore, ScanDirection, StorageError, StorageResult, WriteBatch,
    WriteOperation, new_node_meta,
};

type StoreMaps = BTreeMap<ColumnFamily, BTreeMap<Vec<u8>, Vec<u8>>>;

pub struct InMemoryLocalStore {
    maps: RwLock<StoreMaps>,
}

impl Default for InMemoryLocalStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryLocalStore {
    pub fn new() -> Self {
        let mut maps = empty_maps();
        maps.get_mut(&ColumnFamily::NodeMeta)
            .expect("node metadata column family exists")
            .insert(
                b"node".to_vec(),
                prost::Message::encode_to_vec(&new_node_meta()),
            );
        Self {
            maps: RwLock::new(maps),
        }
    }

    pub(crate) fn serialize(&self) -> StorageResult<Vec<u8>> {
        let view = self.consistent_view()?;
        let mut bytes = b"DEXZDMEM1".to_vec();
        for cf in ColumnFamily::ALL {
            let rows = view.scan(cf, None, None, ScanDirection::Forward, usize::MAX)?;
            append_u64(&mut bytes, rows.len() as u64);
            for (key, value) in rows {
                append_u64(&mut bytes, key.len() as u64);
                bytes.extend_from_slice(&key);
                append_u64(&mut bytes, value.len() as u64);
                bytes.extend_from_slice(&value);
            }
        }
        Ok(bytes)
    }

    pub(crate) fn deserialize(bytes: &[u8]) -> StorageResult<Self> {
        let mut cursor = MemoryCursor { bytes, position: 0 };
        if cursor.take(9)? != b"DEXZDMEM1" {
            return Err(StorageError::Invalid(
                "memory snapshot has invalid header".into(),
            ));
        }
        let mut maps = empty_maps();
        for cf in ColumnFamily::ALL {
            let count = cursor.take_u64()?;
            let map = maps.get_mut(&cf).expect("column family exists");
            for _ in 0..count {
                let key = cursor.take_length_prefixed()?.to_vec();
                let value = cursor.take_length_prefixed()?.to_vec();
                if map.insert(key, value).is_some() {
                    return Err(StorageError::Invalid(
                        "duplicate memory snapshot key".into(),
                    ));
                }
            }
        }
        if cursor.position != bytes.len() {
            return Err(StorageError::Invalid(
                "memory snapshot has trailing bytes".into(),
            ));
        }
        Ok(Self {
            maps: RwLock::new(maps),
        })
    }
}

impl LocalReadView for InMemoryLocalStore {
    fn get(&self, cf: ColumnFamily, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        let maps = self.maps.read().expect("memory store lock is not poisoned");
        Ok(maps
            .get(&cf)
            .expect("column family exists")
            .get(key)
            .cloned())
    }

    fn scan(
        &self,
        cf: ColumnFamily,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        direction: ScanDirection,
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let maps = self.maps.read().expect("memory store lock is not poisoned");
        Ok(scan_map(
            maps.get(&cf).expect("column family exists"),
            start,
            end,
            direction,
            limit,
        ))
    }
}

impl LocalStore for InMemoryLocalStore {
    fn write(&self, batch: WriteBatch) -> StorageResult<()> {
        for operation in batch.operations() {
            if let WriteOperation::DeleteRange { start, end, .. } = operation
                && start >= end
            {
                return Err(StorageError::Invalid(
                    "range delete requires start < end".into(),
                ));
            }
        }
        let mut maps = self
            .maps
            .write()
            .expect("memory store lock is not poisoned");
        for operation in batch.operations() {
            match operation {
                WriteOperation::Put { cf, key, value } => {
                    maps.get_mut(cf)
                        .expect("column family exists")
                        .insert(key.clone(), value.clone());
                }
                WriteOperation::Delete { cf, key } => {
                    maps.get_mut(cf).expect("column family exists").remove(key);
                }
                WriteOperation::DeleteRange { cf, start, end } => {
                    let map = maps.get_mut(cf).expect("column family exists");
                    let keys: Vec<Vec<u8>> = map
                        .range(start.clone()..end.clone())
                        .map(|(key, _)| key.clone())
                        .collect();
                    for key in keys {
                        map.remove(&key);
                    }
                }
            }
        }
        Ok(())
    }

    fn consistent_view(&self) -> StorageResult<Box<dyn LocalReadView + '_>> {
        let maps = self
            .maps
            .read()
            .expect("memory store lock is not poisoned")
            .clone();
        Ok(Box::new(MemoryView { maps }))
    }
}

struct MemoryView {
    maps: StoreMaps,
}

impl LocalReadView for MemoryView {
    fn get(&self, cf: ColumnFamily, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        Ok(self
            .maps
            .get(&cf)
            .expect("column family exists")
            .get(key)
            .cloned())
    }

    fn scan(
        &self,
        cf: ColumnFamily,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        direction: ScanDirection,
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(scan_map(
            self.maps.get(&cf).expect("column family exists"),
            start,
            end,
            direction,
            limit,
        ))
    }
}

fn empty_maps() -> StoreMaps {
    ColumnFamily::ALL
        .into_iter()
        .map(|cf| (cf, BTreeMap::new()))
        .collect()
}

fn scan_map(
    map: &BTreeMap<Vec<u8>, Vec<u8>>,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
    direction: ScanDirection,
    limit: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let rows = map.iter().filter(|(key, _)| {
        start.is_none_or(|bound| key.as_slice() >= bound)
            && end.is_none_or(|bound| key.as_slice() < bound)
    });
    match direction {
        ScanDirection::Forward => rows
            .take(limit)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        ScanDirection::Reverse => rows
            .rev()
            .take(limit)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    }
}

fn append_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

struct MemoryCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> MemoryCursor<'a> {
    fn take(&mut self, length: usize) -> StorageResult<&'a [u8]> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| StorageError::Invalid("memory snapshot length overflow".into()))?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| StorageError::Invalid("truncated memory snapshot".into()))?;
        self.position = end;
        Ok(value)
    }

    fn take_u64(&mut self) -> StorageResult<u64> {
        let bytes: [u8; 8] = self.take(8)?.try_into().expect("eight bytes were read");
        Ok(u64::from_le_bytes(bytes))
    }

    fn take_length_prefixed(&mut self) -> StorageResult<&'a [u8]> {
        let length = usize::try_from(self.take_u64()?)
            .map_err(|_| StorageError::Invalid("memory snapshot length exceeds usize".into()))?;
        self.take(length)
    }
}
