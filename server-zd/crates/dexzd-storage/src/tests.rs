// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use dexzd_keyspace::{ColumnFamily, PartitionId};
use dexzd_object_store::{
    ByteRange, ObjectContent, ObjectListing, ObjectMetadata, ObjectStore, ObjectStoreResult,
    WriteCondition, WriteOutcome,
};
use dexzd_protocol::dexzd::v1::{LogPosition, PartitionMeta};

use crate::{
    InMemoryLocalStore, LocalReadView, LocalStore, RestoredLocalStore, RocksDbLocalStore,
    RocksDbStoreOptions, ScanDirection, SnapshotIdentity, WriteBatch, create_snapshot,
    restore_snapshot, retain_snapshots,
};

#[derive(Default)]
struct TestObjectStore {
    objects: Mutex<BTreeMap<String, Vec<u8>>>,
    uploads: Mutex<Vec<String>>,
}

impl TestObjectStore {
    fn corrupt(&self, key: &str) {
        let mut objects = self
            .objects
            .lock()
            .expect("test object store lock is not poisoned");
        let object = objects.get_mut(key).expect("snapshot object exists");
        object.push(42);
    }

    fn uploaded_files(&self) -> usize {
        self.uploads
            .lock()
            .expect("test upload lock is not poisoned")
            .len()
    }
}

#[async_trait]
impl ObjectStore for TestObjectStore {
    async fn put(
        &self,
        key: &str,
        body: Bytes,
        condition: WriteCondition,
    ) -> ObjectStoreResult<WriteOutcome> {
        let mut objects = self
            .objects
            .lock()
            .expect("test object store lock is not poisoned");
        if matches!(condition, WriteCondition::CreateOnly) && objects.contains_key(key) {
            return Ok(WriteOutcome::PreconditionFailed);
        }
        if let WriteCondition::ReplaceIfEtagMatches { etag } = condition
            && objects.get(key).map(|value| value.len().to_string()) != Some(etag)
        {
            return Ok(WriteOutcome::PreconditionFailed);
        }
        let value = body.to_vec();
        let etag = value.len().to_string();
        objects.insert(key.to_owned(), value);
        Ok(WriteOutcome::Written { etag })
    }

    async fn put_file(
        &self,
        key: &str,
        path: &Path,
        condition: WriteCondition,
    ) -> ObjectStoreResult<WriteOutcome> {
        let bytes = std::fs::read(path).expect("test snapshot file is readable");
        let outcome = self.put(key, bytes.into(), condition).await?;
        if matches!(outcome, WriteOutcome::Written { .. }) {
            self.uploads
                .lock()
                .expect("test upload lock is not poisoned")
                .push(key.to_owned());
        }
        Ok(outcome)
    }

    async fn get(&self, key: &str) -> ObjectStoreResult<Option<ObjectContent>> {
        Ok(self
            .objects
            .lock()
            .expect("test object store lock is not poisoned")
            .get(key)
            .map(|value| ObjectContent {
                bytes: Bytes::copy_from_slice(value),
                etag: value.len().to_string(),
                object_size: value.len() as u64,
            }))
    }

    async fn get_range(
        &self,
        key: &str,
        range: ByteRange,
    ) -> ObjectStoreResult<Option<ObjectContent>> {
        Ok(self.get(key).await?.map(|mut content| {
            let start = usize::try_from(range.start)
                .expect("test range fits usize")
                .min(content.bytes.len());
            let end = usize::try_from(range.end_inclusive)
                .expect("test range fits usize")
                .saturating_add(1)
                .min(content.bytes.len());
            content.bytes = if start <= end {
                content.bytes.slice(start..end)
            } else {
                Bytes::new()
            };
            content
        }))
    }

    async fn get_to_file(&self, key: &str, path: &Path) -> ObjectStoreResult<Option<u64>> {
        match self.get(key).await? {
            Some(content) => {
                std::fs::write(path, &content.bytes).expect("test restore target is writable");
                Ok(Some(content.object_size))
            }
            None => Ok(None),
        }
    }

    async fn head(&self, key: &str) -> ObjectStoreResult<Option<ObjectMetadata>> {
        Ok(self.get(key).await?.map(|content| ObjectMetadata {
            key: key.to_owned(),
            size: content.object_size,
            etag: content.etag,
        }))
    }

    async fn delete(&self, key: &str) -> ObjectStoreResult<()> {
        self.objects
            .lock()
            .expect("test object store lock is not poisoned")
            .remove(key);
        Ok(())
    }

    async fn delete_many(&self, keys: &[String]) -> ObjectStoreResult<()> {
        for key in keys {
            self.delete(key).await?;
        }
        Ok(())
    }

    async fn list(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        max_keys: u32,
    ) -> ObjectStoreResult<ObjectListing> {
        let objects = self
            .objects
            .lock()
            .expect("test object store lock is not poisoned");
        let mut matching: Vec<ObjectMetadata> = objects
            .iter()
            .filter(|(key, _)| {
                key.starts_with(prefix)
                    && start_after.is_none_or(|previous| key.as_str() > previous)
            })
            .map(|(key, value)| ObjectMetadata {
                key: key.clone(),
                size: value.len() as u64,
                etag: value.len().to_string(),
            })
            .collect();
        let next_start_after = if matching.len() > max_keys as usize {
            Some(matching[max_keys as usize - 1].key.clone())
        } else {
            None
        };
        matching.truncate(max_keys as usize);
        Ok(ObjectListing {
            objects: matching,
            next_start_after,
        })
    }
}

fn exercise_store(store: &dyn LocalStore) {
    let original_lineage = store
        .node_meta()
        .expect("node metadata is readable")
        .expect("node metadata exists")
        .lineage_id;
    assert_eq!(original_lineage.len(), 16);
    let mut batch = WriteBatch::new();
    batch.put(ColumnFamily::Flow, b"a".to_vec(), b"one".to_vec());
    batch.put(ColumnFamily::History, b"a".to_vec(), b"history".to_vec());
    batch.put(ColumnFamily::Flow, b"b".to_vec(), b"two".to_vec());
    batch.put(ColumnFamily::Flow, b"c".to_vec(), b"three".to_vec());
    batch.put(ColumnFamily::Flow, b"pre/a".to_vec(), b"first".to_vec());
    batch.put(ColumnFamily::Flow, b"pre/b".to_vec(), b"second".to_vec());
    batch.put(ColumnFamily::Flow, b"pre/c".to_vec(), b"third".to_vec());
    store
        .write(batch)
        .expect("multi-column-family batch writes");
    assert_eq!(
        store
            .multi_get(
                ColumnFamily::Flow,
                &[b"a".as_slice(), b"b".as_slice(), b"missing".as_slice()]
            )
            .expect("multi-get reads"),
        vec![Some(b"one".to_vec()), Some(b"two".to_vec()), None]
    );
    assert_eq!(
        store
            .get(ColumnFamily::History, b"a")
            .expect("history read"),
        Some(b"history".to_vec())
    );
    let view = store.consistent_view().expect("consistent view opens");
    assert_eq!(
        keys(
            store
                .scan(
                    ColumnFamily::Flow,
                    Some(b"a"),
                    Some(b"c"),
                    ScanDirection::Forward,
                    10
                )
                .expect("forward scan")
        ),
        vec![b"a".to_vec(), b"b".to_vec()]
    );
    assert_eq!(
        keys(
            store
                .scan(
                    ColumnFamily::Flow,
                    Some(b"a"),
                    Some(b"c"),
                    ScanDirection::Reverse,
                    10
                )
                .expect("reverse scan")
        ),
        vec![b"b".to_vec(), b"a".to_vec()]
    );
    assert_eq!(
        keys(
            store
                .scan_prefix(ColumnFamily::Flow, b"pre/", ScanDirection::Forward, 2)
                .expect("forward prefix scan")
        ),
        vec![b"pre/a".to_vec(), b"pre/b".to_vec()]
    );
    assert_eq!(
        keys(
            store
                .scan_prefix(ColumnFamily::Flow, b"pre/", ScanDirection::Reverse, 2)
                .expect("reverse prefix scan")
        ),
        vec![b"pre/c".to_vec(), b"pre/b".to_vec()]
    );
    let mut deletion = WriteBatch::new();
    deletion.delete_range(ColumnFamily::Flow, b"a".to_vec(), b"c".to_vec());
    deletion.delete(ColumnFamily::History, b"a".to_vec());
    store.write(deletion).expect("range deletion batch writes");
    assert_eq!(store.get(ColumnFamily::Flow, b"a").expect("new read"), None);
    assert_eq!(
        store
            .get(ColumnFamily::Flow, b"c")
            .expect("end-exclusive read"),
        Some(b"three".to_vec())
    );
    assert_eq!(
        view.get(ColumnFamily::Flow, b"a").expect("snapshot read"),
        Some(b"one".to_vec())
    );
    assert_eq!(
        view.get(ColumnFamily::History, b"a")
            .expect("snapshot history read"),
        Some(b"history".to_vec())
    );
    let mut invalid_batch = WriteBatch::new();
    invalid_batch.put(ColumnFamily::Flow, b"unwritten".to_vec(), b"value".to_vec());
    invalid_batch.delete_range(ColumnFamily::Flow, b"z".to_vec(), b"a".to_vec());
    assert!(store.write(invalid_batch).is_err());
    assert_eq!(
        store
            .get(ColumnFamily::Flow, b"unwritten")
            .expect("rejected batch read"),
        None
    );
    assert_eq!(
        store
            .node_meta()
            .expect("node metadata read")
            .expect("node metadata exists")
            .lineage_id,
        original_lineage
    );
}

fn keys(rows: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<Vec<u8>> {
    rows.into_iter().map(|(key, _)| key).collect()
}

#[test]
fn memory_store_matches_local_store_contract() {
    exercise_store(&InMemoryLocalStore::new());
}

#[test]
fn rocksdb_store_matches_local_store_contract() {
    let directory = tempfile::tempdir().expect("temporary directory exists");
    let store =
        RocksDbLocalStore::open(directory.path().join("db"), RocksDbStoreOptions::default())
            .expect("RocksDB opens");
    exercise_store(&store);
    let lineage = store
        .node_meta()
        .expect("node metadata reads")
        .expect("node metadata exists")
        .lineage_id;
    drop(store);
    let reopened =
        RocksDbLocalStore::open(directory.path().join("db"), RocksDbStoreOptions::default())
            .expect("RocksDB reopens");
    assert_eq!(
        reopened
            .get(ColumnFamily::Flow, b"c")
            .expect("persisted row reads"),
        Some(b"three".to_vec())
    );
    assert_eq!(
        reopened
            .node_meta()
            .expect("node metadata reads")
            .expect("node metadata exists")
            .lineage_id,
        lineage
    );
}

fn identity(created_at_unix_micros: i64) -> SnapshotIdentity {
    SnapshotIdentity {
        cluster_id: [7; 16],
        node_id: "node-a".into(),
        created_at_unix_micros,
    }
}

fn set_partition_meta(store: &impl LocalStore) {
    store
        .put_partition_meta(&PartitionMeta {
            partition: 4,
            applied: Some(LogPosition {
                node_id: "node-a".into(),
                incarnation: 1,
                seq: 8,
            }),
            last_command_timestamp_micros: 20,
            last_behavior_version: 1,
        })
        .expect("partition metadata writes");
    assert_eq!(
        store
            .partition_meta(PartitionId(4))
            .expect("partition metadata reads")
            .expect("partition metadata exists")
            .partition,
        4
    );
}

#[tokio::test]
async fn rocksdb_snapshot_restores_all_column_families_and_changes_lineage() {
    let directory = tempfile::tempdir().expect("temporary directory exists");
    let store =
        RocksDbLocalStore::open(directory.path().join("db"), RocksDbStoreOptions::default())
            .expect("RocksDB opens");
    let object_store = TestObjectStore::default();
    let mut batch = WriteBatch::new();
    for cf in [
        ColumnFamily::Flow,
        ColumnFamily::History,
        ColumnFamily::Value,
        ColumnFamily::PartitionIndex,
    ] {
        batch.put(cf, b"sample".to_vec(), b"data".to_vec());
    }
    store.write(batch).expect("sample rows write");
    set_partition_meta(&store);
    let original_lineage = store
        .node_meta()
        .expect("node metadata reads")
        .expect("node metadata exists")
        .lineage_id;
    let (manifest_key, manifest) = create_snapshot(&store, &object_store, &identity(10), 1)
        .await
        .expect("snapshot uploads");
    assert_eq!(manifest.partition_positions.len(), 1);
    assert_eq!(
        manifest.partition_positions[0]
            .applied
            .as_ref()
            .expect("applied position exists")
            .seq,
        8
    );
    let restored = restore_snapshot(
        &object_store,
        &manifest_key,
        &directory.path().join("restored"),
        RocksDbStoreOptions::default(),
    )
    .await
    .expect("snapshot restores");
    assert!(matches!(restored, RestoredLocalStore::RocksDb(_)));
    for cf in [
        ColumnFamily::Flow,
        ColumnFamily::History,
        ColumnFamily::Value,
        ColumnFamily::PartitionIndex,
    ] {
        assert_eq!(
            restored.get(cf, b"sample").expect("restored row reads"),
            Some(b"data".to_vec())
        );
    }
    assert_eq!(
        restored
            .partition_meta(PartitionId(4))
            .expect("partition metadata reads"),
        store
            .partition_meta(PartitionId(4))
            .expect("partition metadata reads")
    );
    assert_ne!(
        restored
            .node_meta()
            .expect("restored metadata reads")
            .expect("restored metadata exists")
            .lineage_id,
        original_lineage
    );
}

#[tokio::test]
async fn memory_snapshot_restores_and_changes_lineage() {
    let directory = tempfile::tempdir().expect("temporary directory exists");
    let store = InMemoryLocalStore::new();
    let object_store = TestObjectStore::default();
    let mut batch = WriteBatch::new();
    batch.put(ColumnFamily::Value, b"value".to_vec(), b"payload".to_vec());
    store.write(batch).expect("memory value writes");
    let original_lineage = store
        .node_meta()
        .expect("metadata reads")
        .expect("metadata exists")
        .lineage_id;
    let (manifest_key, _) = create_snapshot(&store, &object_store, &identity(10), 1)
        .await
        .expect("memory snapshot uploads");
    let restored = restore_snapshot(
        &object_store,
        &manifest_key,
        &directory.path().join("restore"),
        RocksDbStoreOptions::default(),
    )
    .await
    .expect("memory snapshot restores");
    assert!(matches!(restored, RestoredLocalStore::Memory(_)));
    assert_eq!(
        restored
            .get(ColumnFamily::Value, b"value")
            .expect("value reads"),
        Some(b"payload".to_vec())
    );
    assert_ne!(
        restored
            .node_meta()
            .expect("metadata reads")
            .expect("metadata exists")
            .lineage_id,
        original_lineage
    );
}

#[tokio::test]
async fn incremental_snapshot_reuses_files_and_rejects_corruption() {
    let directory = tempfile::tempdir().expect("temporary directory exists");
    let store =
        RocksDbLocalStore::open(directory.path().join("db"), RocksDbStoreOptions::default())
            .expect("RocksDB opens");
    let object_store = TestObjectStore::default();
    let mut batch = WriteBatch::new();
    batch.put(ColumnFamily::Value, b"large".to_vec(), vec![9; 128 * 1024]);
    store.write(batch).expect("large value writes");
    let (_, first) = create_snapshot(&store, &object_store, &identity(10), 1)
        .await
        .expect("first snapshot uploads");
    let first_uploads = object_store.uploaded_files();
    let (_, second) = create_snapshot(&store, &object_store, &identity(20), 2)
        .await
        .expect("second snapshot uploads");
    let second_uploads = object_store.uploaded_files() - first_uploads;
    assert!(second_uploads < second.files.len());
    assert!(first.files.iter().any(|file| {
        second
            .files
            .iter()
            .any(|next| next.object_key == file.object_key)
    }));
    let corrupted_file = second
        .files
        .iter()
        .find(|file| file.relative_path.ends_with(".sst"))
        .expect("snapshot has SST file");
    object_store.corrupt(&corrupted_file.object_key);
    let manifest_key = format!(
        "snapshot/node-a/{}/manifest/{:020}",
        uuid::Uuid::from_slice(&second.lineage_id).expect("lineage is UUID"),
        second.snapshot_number
    );
    assert!(
        restore_snapshot(
            &object_store,
            &manifest_key,
            &directory.path().join("corrupt-restore"),
            RocksDbStoreOptions::default()
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn retention_deletes_old_manifests_and_unreferenced_files_after_delay() {
    let store = InMemoryLocalStore::new();
    let object_store = TestObjectStore::default();
    let lineage_id = store
        .node_meta()
        .expect("metadata reads")
        .expect("metadata exists")
        .lineage_id;
    let mut keys = Vec::new();
    for number in 1..=4 {
        let mut batch = WriteBatch::new();
        batch.put(ColumnFamily::Flow, b"key".to_vec(), vec![number as u8]);
        store.write(batch).expect("snapshot state changes");
        let (key, manifest) =
            create_snapshot(&store, &object_store, &identity(number as i64 * 10), number)
                .await
                .expect("snapshot uploads");
        keys.push((key, manifest.files[0].object_key.clone()));
    }
    retain_snapshots(
        &object_store,
        "node-a",
        &lineage_id,
        3,
        Duration::from_micros(30),
        35,
    )
    .await
    .expect("early retention succeeds");
    assert!(
        object_store
            .head(&keys[0].0)
            .await
            .expect("head succeeds")
            .is_some()
    );
    retain_snapshots(
        &object_store,
        "node-a",
        &lineage_id,
        3,
        Duration::from_micros(30),
        75,
    )
    .await
    .expect("late retention succeeds");
    assert!(
        object_store
            .head(&keys[0].0)
            .await
            .expect("head succeeds")
            .is_none()
    );
    assert!(
        object_store
            .head(&keys[0].1)
            .await
            .expect("head succeeds")
            .is_none()
    );
    for (manifest_key, _) in keys.iter().skip(1) {
        assert!(
            object_store
                .head(manifest_key)
                .await
                .expect("head succeeds")
                .is_some()
        );
    }
}
