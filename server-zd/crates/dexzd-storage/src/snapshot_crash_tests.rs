// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use dexzd_object_store::{
    FaultAction, FaultInjector, InMemoryObjectStore, ObjectStore, ObjectStoreOperation,
    WriteCondition,
};
use tokio::sync::Notify;

use crate::{
    InMemoryLocalStore, LocalReadView, RocksDbLocalStore, RocksDbStoreOptions, SnapshotIdentity,
    StorageError, create_snapshot,
};

/// Lands the armed manifest with a lost response, then stalls the read that reconciles it.
#[derive(Default)]
struct ManifestCrashFaults {
    armed_manifest_key: Mutex<Option<String>>,
    manifest_read_started: Notify,
}

impl ManifestCrashFaults {
    fn arm(&self, manifest_key: &str) {
        *self
            .armed_manifest_key
            .lock()
            .expect("fault lock is not poisoned") = Some(manifest_key.to_owned());
    }

    fn disarm(&self) {
        self.armed_manifest_key
            .lock()
            .expect("fault lock is not poisoned")
            .take();
    }
}

impl FaultInjector for ManifestCrashFaults {
    fn action(&self, operation: ObjectStoreOperation, key: &str) -> FaultAction {
        let armed_manifest_key = self
            .armed_manifest_key
            .lock()
            .expect("fault lock is not poisoned");
        if armed_manifest_key.as_deref() != Some(key) {
            return FaultAction::Proceed;
        }
        match operation {
            ObjectStoreOperation::Put => FaultAction::ApplyThenOutcomeUnknown,
            ObjectStoreOperation::Get => {
                self.manifest_read_started.notify_one();
                FaultAction::Delay(Duration::from_secs(3600))
            }
            _ => FaultAction::Proceed,
        }
    }
}

#[tokio::test]
async fn a_crash_after_the_manifest_lands_leaves_the_next_snapshot_number_usable() {
    let directory = tempfile::tempdir().expect("temporary directory exists");
    let path = directory.path().join("db");
    let faults = Arc::new(ManifestCrashFaults::default());
    let object_store = InMemoryObjectStore::with_injector(faults.clone());
    let store =
        RocksDbLocalStore::open(&path, RocksDbStoreOptions::default()).expect("RocksDB opens");
    let orphan_key = manifest_key(&store, 1);
    faults.arm(&orphan_key);
    let crashing_identity = identity(10);
    // Dropping the upload once the manifest landed stands in for the process dying.
    tokio::select! {
        biased;
        result = create_snapshot(&store, &object_store, &crashing_identity, 1) => {
            panic!("the upload must stall after the manifest lands: {result:?}");
        }
        () = faults.manifest_read_started.notified() => {}
    }
    faults.disarm();
    assert!(
        object_store
            .head(&orphan_key)
            .await
            .expect("head succeeds")
            .is_some()
    );
    drop(store);

    let store =
        RocksDbLocalStore::open(&path, RocksDbStoreOptions::default()).expect("RocksDB reopens");
    let (manifest_key, manifest) = create_snapshot(
        &store,
        &object_store,
        &identity(20),
        next_snapshot_number(&store),
    )
    .await
    .expect("the restarted node takes a snapshot");
    assert_ne!(manifest_key, orphan_key);
    assert_eq!(manifest.snapshot_number, 2);
}

/// A reservation lost with unsynced local writes costs one attempt, not every later one.
#[tokio::test]
async fn a_conflicting_manifest_costs_only_one_snapshot_attempt() {
    let store = InMemoryLocalStore::new();
    let object_store = InMemoryObjectStore::new();
    object_store
        .put(
            &manifest_key(&store, 1),
            Bytes::from_static(b"manifest of a lost reservation"),
            WriteCondition::CreateOnly,
        )
        .await
        .expect("put succeeds");
    assert!(matches!(
        create_snapshot(
            &store,
            &object_store,
            &identity(10),
            next_snapshot_number(&store)
        )
        .await,
        Err(StorageError::SnapshotManifestConflict { .. })
    ));
    let (_, manifest) = create_snapshot(
        &store,
        &object_store,
        &identity(20),
        next_snapshot_number(&store),
    )
    .await
    .expect("the retry uses the next snapshot number");
    assert_eq!(manifest.snapshot_number, 2);
}

fn identity(created_at_unix_micros: i64) -> SnapshotIdentity {
    SnapshotIdentity {
        cluster_id: [7; 16],
        node_id: "node-a".into(),
        created_at_unix_micros,
    }
}

fn manifest_key(store: &impl LocalReadView, snapshot_number: u64) -> String {
    let lineage_id = store
        .node_meta()
        .expect("node metadata reads")
        .expect("node metadata exists")
        .lineage_id;
    let lineage = uuid::Uuid::from_slice(&lineage_id).expect("lineage is a UUID");
    format!("snapshot/node-a/{lineage}/manifest/{snapshot_number:020}")
}

/// Numbers the next snapshot the way the engine does.
fn next_snapshot_number(store: &impl LocalReadView) -> u64 {
    store
        .node_meta()
        .expect("node metadata reads")
        .expect("node metadata exists")
        .last_snapshot_number
        + 1
}
