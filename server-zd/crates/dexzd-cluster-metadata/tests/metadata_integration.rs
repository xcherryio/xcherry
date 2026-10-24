// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use dexzd_clock::WallClock;
use dexzd_cluster_metadata::{
    AttributeIndexDeclaration, ClusterConfig, LeaseConfig, LeaseStatus, ManifestChange,
    MetadataError, NodeConfig, acquire_lease, bootstrap_or_validate, bump_node_epoch,
    create_initial_manifest, raise_version_barrier, read_attribute_index_registry, read_manifest,
    register_attribute_indexes, update_manifest, update_partition_snapshot,
};
use dexzd_object_store::{
    ByteRange, ObjectContent, ObjectListing, ObjectMetadata, ObjectStore, ObjectStoreError,
    ObjectStoreErrorKind, ObjectStoreOperation, ObjectStoreResult, WriteCondition, WriteOutcome,
};
use dexzd_protocol::dexzd::v1::{
    ClusterObject, ControllerLease, Manifest, NodeLease, SnapshotReference,
};
use prost::Message;
use tokio::sync::{Barrier, Notify};
use tokio::time::Instant;

#[derive(Default)]
struct MemoryStore {
    objects: Mutex<BTreeMap<String, (Bytes, String)>>,
    next_etag: AtomicU64,
    unknown_next_manifest_write: AtomicBool,
    unknown_next_create_key: Mutex<Option<&'static str>>,
    block_next_lease_renewal: AtomicBool,
    fail_next_lease_renewal: AtomicBool,
    pause_next_lease_release: AtomicBool,
    release_started: Notify,
    continue_release: Notify,
    unknown_next_release_before_write: AtomicBool,
    unknown_next_release_after_write: AtomicBool,
    manifest_read_barrier: Mutex<Option<(Arc<Barrier>, usize)>>,
}

impl MemoryStore {
    fn object_count(&self) -> usize {
        self.objects.lock().expect("test store mutex").len()
    }
}

#[async_trait]
impl ObjectStore for MemoryStore {
    async fn put(
        &self,
        key: &str,
        body: Bytes,
        condition: WriteCondition,
    ) -> ObjectStoreResult<WriteOutcome> {
        let is_lease_release = key.starts_with("lease/")
            && NodeLease::decode(body.clone())
                .is_ok_and(|lease| lease.expires_at_unix_micros == i64::MIN);
        if is_lease_release && self.pause_next_lease_release.swap(false, Ordering::SeqCst) {
            self.release_started.notify_one();
            self.continue_release.notified().await;
        }
        if is_lease_release
            && self
                .unknown_next_release_before_write
                .swap(false, Ordering::SeqCst)
        {
            return Err(ObjectStoreError::new(
                ObjectStoreOperation::Put,
                key,
                ObjectStoreErrorKind::OutcomeUnknown,
                "injected lost release response without write",
            ));
        }
        if matches!(&condition, WriteCondition::CreateOnly) {
            let mut unknown_key = self
                .unknown_next_create_key
                .lock()
                .expect("test store mutex");
            if unknown_key
                .as_ref()
                .is_some_and(|unknown_key| *unknown_key == key)
            {
                *unknown_key = None;
                return Err(ObjectStoreError::new(
                    ObjectStoreOperation::Put,
                    key,
                    ObjectStoreErrorKind::OutcomeUnknown,
                    "injected lost response without write",
                ));
            }
        }
        if key.starts_with("lease/")
            && matches!(&condition, WriteCondition::ReplaceIfEtagMatches { .. })
            && self.block_next_lease_renewal.swap(false, Ordering::SeqCst)
        {
            return std::future::pending().await;
        }
        if key.starts_with("lease/")
            && matches!(condition, WriteCondition::ReplaceIfEtagMatches { .. })
            && self.fail_next_lease_renewal.swap(false, Ordering::SeqCst)
        {
            return Err(ObjectStoreError::new(
                ObjectStoreOperation::Put,
                key,
                ObjectStoreErrorKind::Transient,
                "injected lease renewal failure",
            ));
        }
        let mut objects = self.objects.lock().expect("test store mutex");
        let existing = objects.get(key);
        let matches_condition = match condition {
            WriteCondition::Unconditional => true,
            WriteCondition::CreateOnly => existing.is_none(),
            WriteCondition::ReplaceIfEtagMatches { etag } => {
                existing.is_some_and(|(_, current_etag)| *current_etag == etag)
            }
        };
        if !matches_condition {
            return Ok(WriteOutcome::PreconditionFailed);
        }
        let etag = self.next_etag.fetch_add(1, Ordering::SeqCst).to_string();
        objects.insert(key.to_owned(), (body, etag.clone()));
        if is_lease_release
            && self
                .unknown_next_release_after_write
                .swap(false, Ordering::SeqCst)
        {
            return Err(ObjectStoreError::new(
                ObjectStoreOperation::Put,
                key,
                ObjectStoreErrorKind::OutcomeUnknown,
                "injected lost release response after write",
            ));
        }
        if key == "manifest"
            && self
                .unknown_next_manifest_write
                .swap(false, Ordering::SeqCst)
        {
            return Err(ObjectStoreError::new(
                ObjectStoreOperation::Put,
                key,
                ObjectStoreErrorKind::OutcomeUnknown,
                "injected lost response after write",
            ));
        }
        Ok(WriteOutcome::Written { etag })
    }

    async fn put_file(
        &self,
        key: &str,
        path: &Path,
        condition: WriteCondition,
    ) -> ObjectStoreResult<WriteOutcome> {
        let body = tokio::fs::read(path).await.map_err(|error| {
            ObjectStoreError::new(
                ObjectStoreOperation::Put,
                key,
                ObjectStoreErrorKind::Permanent,
                error.to_string(),
            )
        })?;
        self.put(key, Bytes::from(body), condition).await
    }

    async fn get(&self, key: &str) -> ObjectStoreResult<Option<ObjectContent>> {
        let content =
            self.objects
                .lock()
                .expect("test store mutex")
                .get(key)
                .map(|(body, etag)| ObjectContent {
                    bytes: body.clone(),
                    etag: etag.clone(),
                    object_size: body.len() as u64,
                });
        if key == "manifest" {
            let barrier = {
                let mut pending = self.manifest_read_barrier.lock().expect("test store mutex");
                if let Some((barrier, remaining)) = pending.as_mut() {
                    let barrier = Arc::clone(barrier);
                    *remaining -= 1;
                    if *remaining == 0 {
                        drop(pending.take());
                    }
                    Some(barrier)
                } else {
                    None
                }
            };
            if let Some(barrier) = barrier {
                barrier.wait().await;
            }
        }
        Ok(content)
    }

    async fn get_range(
        &self,
        key: &str,
        range: ByteRange,
    ) -> ObjectStoreResult<Option<ObjectContent>> {
        Ok(self.get(key).await?.map(|mut content| {
            let start = (range.start as usize).min(content.bytes.len());
            let end = (range.end_inclusive as usize)
                .saturating_add(1)
                .min(content.bytes.len());
            content.bytes = content.bytes.slice(start..end.max(start));
            content
        }))
    }

    async fn get_to_file(&self, key: &str, path: &Path) -> ObjectStoreResult<Option<u64>> {
        let Some(content) = self.get(key).await? else {
            return Ok(None);
        };
        tokio::fs::write(path, &content.bytes)
            .await
            .map_err(|error| {
                ObjectStoreError::new(
                    ObjectStoreOperation::Get,
                    key,
                    ObjectStoreErrorKind::Permanent,
                    error.to_string(),
                )
            })?;
        Ok(Some(content.object_size))
    }

    async fn head(&self, key: &str) -> ObjectStoreResult<Option<ObjectMetadata>> {
        Ok(self.get(key).await?.map(|content| ObjectMetadata {
            key: key.to_owned(),
            size: content.object_size,
            etag: content.etag,
            last_modified_unix_micros: None,
        }))
    }

    async fn delete(&self, key: &str) -> ObjectStoreResult<()> {
        self.objects.lock().expect("test store mutex").remove(key);
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
        let objects = self.objects.lock().expect("test store mutex");
        let matching: Vec<_> = objects
            .iter()
            .filter(|(key, _)| {
                key.starts_with(prefix) && start_after.is_none_or(|after| key.as_str() > after)
            })
            .take(max_keys as usize)
            .map(|(key, (body, etag))| ObjectMetadata {
                key: key.clone(),
                size: body.len() as u64,
                etag: etag.clone(),
                last_modified_unix_micros: None,
            })
            .collect();
        Ok(ObjectListing {
            objects: matching,
            next_start_after: None,
        })
    }
}

struct TestClock {
    start: Instant,
    unix_micros: i64,
    offset_micros: AtomicI64,
}

impl Default for TestClock {
    fn default() -> Self {
        Self {
            start: Instant::now(),
            unix_micros: 1_800_000_000_000_000,
            offset_micros: AtomicI64::new(0),
        }
    }
}

impl WallClock for TestClock {
    fn now_unix_micros(&self) -> i64 {
        self.unix_micros
            + self.start.elapsed().as_micros() as i64
            + self.offset_micros.load(Ordering::SeqCst)
    }
}

fn node() -> NodeConfig {
    NodeConfig {
        node_id: "node-a".to_owned(),
        incarnation: 1,
        internal_address: "127.0.0.1:9000".to_owned(),
    }
}

#[tokio::test(start_paused = true)]
async fn first_bootstrap_and_second_node_validate_immutable_cluster() {
    let store = MemoryStore::default();
    let clock = TestClock::default();
    let first = bootstrap_or_validate(&store, &clock, ClusterConfig::default())
        .await
        .unwrap();
    let second = bootstrap_or_validate(&store, &clock, ClusterConfig::default())
        .await
        .unwrap();
    assert_eq!(first.cluster, second.cluster);
    assert_eq!(first.partition_hasher.partition_count(), 1024);
    assert_eq!(first.cluster.partition_hash_key.len(), 16);
    assert_eq!(store.object_count(), 1);
    let error = bootstrap_or_validate(
        &store,
        &clock,
        ClusterConfig {
            partition_count: 512,
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        MetadataError::ClusterMismatch {
            field: "partition count",
            ..
        }
    ));
    let mut stored =
        ClusterObject::decode(store.get("cluster").await.unwrap().unwrap().bytes).unwrap();
    stored.partition_hash_algorithm = 0;
    store
        .put(
            "cluster",
            Bytes::from(stored.encode_to_vec()),
            WriteCondition::Unconditional,
        )
        .await
        .unwrap();
    assert!(matches!(
        bootstrap_or_validate(&store, &clock, ClusterConfig::default()).await,
        Err(MetadataError::ClusterMismatch {
            field: "partition hash algorithm",
            ..
        })
    ));
    stored.partition_hash_algorithm = first.cluster.partition_hash_algorithm;
    stored.key_layout_version += 1;
    store
        .put(
            "cluster",
            Bytes::from(stored.encode_to_vec()),
            WriteCondition::Unconditional,
        )
        .await
        .unwrap();
    assert!(matches!(
        bootstrap_or_validate(&store, &clock, ClusterConfig::default()).await,
        Err(MetadataError::ClusterMismatch {
            field: "key layout version",
            ..
        })
    ));
}

#[tokio::test(start_paused = true)]
async fn rejected_node_ids_never_enter_manifest_or_lease_keys() {
    let store = Arc::new(MemoryStore::default());
    let clock = Arc::new(TestClock::default());
    let invalid_node = NodeConfig {
        node_id: "Node_A".to_owned(),
        ..node()
    };
    assert!(matches!(
        create_initial_manifest(store.as_ref(), clock.as_ref(), 2, &invalid_node).await,
        Err(MetadataError::InvalidNodeIdentifier(_))
    ));
    assert!(matches!(
        bump_node_epoch(store.as_ref(), clock.as_ref(), &invalid_node).await,
        Err(MetadataError::InvalidNodeIdentifier(_))
    ));
    assert!(matches!(
        acquire_lease(store.clone(), clock, &invalid_node, &LeaseConfig::default()).await,
        Err(MetadataError::InvalidNodeIdentifier(_))
    ));
    assert_eq!(store.object_count(), 0);
}

#[tokio::test(start_paused = true)]
async fn create_only_retries_when_unknown_write_did_not_land() {
    let store = MemoryStore::default();
    let clock = TestClock::default();
    *store.unknown_next_create_key.lock().unwrap() = Some("cluster");
    let cluster = bootstrap_or_validate(&store, &clock, ClusterConfig::default())
        .await
        .unwrap();
    assert_eq!(cluster.partition_hasher.partition_count(), 1024);
    *store.unknown_next_create_key.lock().unwrap() = Some("manifest");
    let manifest = create_initial_manifest(&store, &clock, 2, &node())
        .await
        .unwrap();
    assert_eq!(manifest.version, 1);
    assert_eq!(store.object_count(), 2);
}

#[tokio::test(start_paused = true)]
async fn concurrent_manifest_edits_both_apply_with_increasing_versions() {
    let store = Arc::new(MemoryStore::default());
    let clock = Arc::new(TestClock::default());
    let initial = create_initial_manifest(store.as_ref(), clock.as_ref(), 4, &node())
        .await
        .unwrap();
    *store.manifest_read_barrier.lock().unwrap() = Some((Arc::new(Barrier::new(2)), 2));
    assert_eq!(initial.version, 1);
    assert_eq!(initial.partitions.len(), 4);
    assert!(
        initial
            .partitions
            .iter()
            .all(|partition| partition.owner_node_id == "node-a" && partition.segments[0].is_open)
    );
    let first_store = Arc::clone(&store);
    let first_clock = Arc::clone(&clock);
    let first = tokio::spawn(async move {
        update_manifest(first_store.as_ref(), first_clock.as_ref(), |manifest| {
            manifest.nodes.get_mut("node-a").unwrap().internal_address =
                "127.0.0.1:9001".to_owned();
            Ok(ManifestChange::Changed)
        })
        .await
    });
    let second_store = Arc::clone(&store);
    let second_clock = Arc::clone(&clock);
    let second = tokio::spawn(async move {
        update_manifest(second_store.as_ref(), second_clock.as_ref(), |manifest| {
            manifest.controller_lease = Some(ControllerLease {
                node_id: "node-a".to_owned(),
                expires_at_unix_micros: 100,
            });
            Ok(ManifestChange::Changed)
        })
        .await
    });
    let first_result = first.await.unwrap().unwrap();
    let second_result = second.await.unwrap().unwrap();
    assert_ne!(first_result.version, second_result.version);
    let final_manifest = read_manifest(store.as_ref()).await.unwrap().unwrap();
    assert_eq!(final_manifest.version, 3);
    assert_eq!(
        final_manifest.nodes["node-a"].internal_address,
        "127.0.0.1:9001"
    );
    assert!(final_manifest.controller_lease.is_some());
    let epoch = bump_node_epoch(store.as_ref(), clock.as_ref(), &node())
        .await
        .unwrap();
    assert_eq!(epoch, 1);
    assert!(
        read_manifest(store.as_ref())
            .await
            .unwrap()
            .unwrap()
            .partitions
            .iter()
            .all(|partition| partition.owner_epoch == 1)
    );
}

#[tokio::test(start_paused = true)]
async fn manifest_write_with_unknown_outcome_is_applied_once() {
    let store = MemoryStore::default();
    let clock = TestClock::default();
    create_initial_manifest(&store, &clock, 2, &node())
        .await
        .unwrap();
    store
        .unknown_next_manifest_write
        .store(true, Ordering::SeqCst);
    let result = update_manifest(&store, &clock, |manifest: &mut Manifest| {
        manifest.nodes.get_mut("node-a").unwrap().internal_address = "new-address".to_owned();
        Ok(ManifestChange::Changed)
    })
    .await
    .unwrap();
    assert_eq!(result.version, 2);
    assert_eq!(read_manifest(&store).await.unwrap().unwrap().version, 2);
}

#[tokio::test(start_paused = true)]
async fn snapshot_and_version_barrier_updates_reject_unsupported_versions() {
    let store = MemoryStore::default();
    let clock = TestClock::default();
    create_initial_manifest(&store, &clock, 2, &node())
        .await
        .unwrap();
    let snapshot = SnapshotReference {
        node_id: "node-a".to_owned(),
        lineage_id: vec![7; 16],
        snapshot_number: 1,
        manifest_key: "snapshot/node-a/manifest/1".to_owned(),
    };
    let updated = update_partition_snapshot(&store, &clock, 0, snapshot.clone())
        .await
        .unwrap();
    assert_eq!(updated.version, 2);
    assert_eq!(
        update_partition_snapshot(&store, &clock, 0, snapshot)
            .await
            .unwrap()
            .version,
        2
    );
    assert_eq!(
        raise_version_barrier(&store, &clock, 1, 1)
            .await
            .unwrap()
            .version,
        2
    );
    assert!(matches!(
        raise_version_barrier(&store, &clock, 2, 1).await,
        Err(MetadataError::UnsupportedVersionBarrier { .. })
    ));
    let mut future_manifest = read_manifest(&store).await.unwrap().unwrap();
    future_manifest
        .version_barrier
        .as_mut()
        .unwrap()
        .format_version = 2;
    store
        .put(
            "manifest",
            Bytes::from(future_manifest.encode_to_vec()),
            WriteCondition::Unconditional,
        )
        .await
        .unwrap();
    assert!(matches!(
        update_manifest(&store, &clock, |_| Ok(ManifestChange::Changed)).await,
        Err(MetadataError::UnsupportedVersionBarrier { .. })
    ));
}

#[tokio::test(start_paused = true)]
async fn lease_acquires_renews_and_rejects_second_process_with_same_node_id() {
    let store = Arc::new(MemoryStore::default());
    let clock = Arc::new(TestClock::default());
    create_initial_manifest(store.as_ref(), clock.as_ref(), 2, &node())
        .await
        .unwrap();
    let config = LeaseConfig {
        fail_fast: true,
        ..LeaseConfig::default()
    };
    let first = acquire_lease(store.clone(), clock.clone(), &node(), &config)
        .await
        .unwrap();
    assert_eq!(first.epoch(), 1);
    let mut status = first.subscribe();
    let first_deadline = match *status.borrow() {
        LeaseStatus::ValidUntil(deadline) => deadline,
        LeaseStatus::Lost(_) => panic!("expected valid lease"),
    };
    assert!(matches!(
        acquire_lease(store.clone(), clock.clone(), &node(), &config).await,
        Err(MetadataError::LeaseHeld { .. })
    ));
    assert_eq!(
        read_manifest(store.as_ref()).await.unwrap().unwrap().nodes["node-a"].epoch,
        1
    );
    tokio::time::advance(Duration::from_secs(4)).await;
    status.changed().await.unwrap();
    let second_deadline = match *status.borrow() {
        LeaseStatus::ValidUntil(deadline) => deadline,
        LeaseStatus::Lost(_) => panic!("expected renewal"),
    };
    assert!(second_deadline > first_deadline);
    first.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn lease_takeover_after_expiry_and_grace() {
    let store = Arc::new(MemoryStore::default());
    let clock = Arc::new(TestClock::default());
    create_initial_manifest(store.as_ref(), clock.as_ref(), 2, &node())
        .await
        .unwrap();
    let config = LeaseConfig {
        fail_fast: true,
        ..LeaseConfig::default()
    };
    let first = acquire_lease(store.clone(), clock.clone(), &node(), &config)
        .await
        .unwrap();
    drop(first);
    tokio::time::advance(Duration::from_secs(12)).await;
    let second = acquire_lease(store.clone(), clock.clone(), &node(), &config)
        .await
        .unwrap();
    assert_eq!(second.epoch(), 2);
    assert!(matches!(
        *second.subscribe().borrow(),
        LeaseStatus::ValidUntil(_)
    ));
    second.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn lease_waiter_acquires_only_after_expiry_and_grace() {
    let store = Arc::new(MemoryStore::default());
    let clock = Arc::new(TestClock::default());
    create_initial_manifest(store.as_ref(), clock.as_ref(), 2, &node())
        .await
        .unwrap();
    let first = acquire_lease(
        store.clone(),
        clock.clone(),
        &node(),
        &LeaseConfig::default(),
    )
    .await
    .unwrap();
    drop(first);
    let second_store = Arc::clone(&store);
    let second_clock = Arc::clone(&clock);
    let waiter = tokio::spawn(async move {
        acquire_lease(second_store, second_clock, &node(), &LeaseConfig::default()).await
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(10)).await;
    assert!(!waiter.is_finished());
    tokio::time::advance(Duration::from_secs(1)).await;
    let second = waiter.await.unwrap().unwrap();
    second.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn lease_release_allows_immediate_reacquire() {
    let store = Arc::new(MemoryStore::default());
    let clock = Arc::new(TestClock::default());
    create_initial_manifest(store.as_ref(), clock.as_ref(), 2, &node())
        .await
        .unwrap();
    let config = LeaseConfig {
        fail_fast: true,
        ..LeaseConfig::default()
    };
    let first = acquire_lease(store.clone(), clock.clone(), &node(), &config)
        .await
        .unwrap();
    let first_epoch = first.epoch();
    let original = store.get("lease/node-a").await.unwrap().unwrap();
    let original_lease = NodeLease::decode(original.bytes).unwrap();
    first.shutdown().await;
    let released = store.get("lease/node-a").await.unwrap().unwrap();
    let released_lease = NodeLease::decode(released.bytes).unwrap();
    assert_eq!(released_lease.expires_at_unix_micros, i64::MIN);
    assert_eq!(released_lease.version, original_lease.version + 1);
    assert_eq!(
        released_lease.holder_process_id,
        original_lease.holder_process_id
    );
    let second = acquire_lease(store, clock, &node(), &config).await.unwrap();
    assert_eq!(second.epoch(), first_epoch + 1);
    second.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn release_after_takeover_preserves_new_holder() {
    let store = Arc::new(MemoryStore::default());
    let clock = Arc::new(TestClock::default());
    create_initial_manifest(store.as_ref(), clock.as_ref(), 2, &node())
        .await
        .unwrap();
    let config = LeaseConfig {
        fail_fast: true,
        ..LeaseConfig::default()
    };
    let old_holder = acquire_lease(store.clone(), clock.clone(), &node(), &config)
        .await
        .unwrap();
    clock.offset_micros.store(12_000_000, Ordering::SeqCst);
    let new_holder = acquire_lease(store.clone(), clock, &node(), &config)
        .await
        .unwrap();
    let before = store.get("lease/node-a").await.unwrap().unwrap();
    old_holder.shutdown().await;
    let after = store.get("lease/node-a").await.unwrap().unwrap();
    assert_eq!(after.etag, before.etag);
    assert_eq!(after.bytes, before.bytes);
    assert!(matches!(
        *new_holder.subscribe().borrow(),
        LeaseStatus::ValidUntil(_)
    ));
    new_holder.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn release_conflict_preserves_a_concurrent_new_holder() {
    let store = Arc::new(MemoryStore::default());
    let clock = Arc::new(TestClock::default());
    create_initial_manifest(store.as_ref(), clock.as_ref(), 2, &node())
        .await
        .unwrap();
    let config = LeaseConfig {
        fail_fast: true,
        ..LeaseConfig::default()
    };
    let old_holder = acquire_lease(store.clone(), clock.clone(), &node(), &config)
        .await
        .unwrap();
    let mut old_status = old_holder.subscribe();
    store.pause_next_lease_release.store(true, Ordering::SeqCst);
    let shutdown = tokio::spawn(old_holder.shutdown());
    old_status.changed().await.unwrap();
    assert!(matches!(&*old_status.borrow(), LeaseStatus::Lost(_)));
    store.release_started.notified().await;
    clock.offset_micros.store(12_000_000, Ordering::SeqCst);
    let new_holder = acquire_lease(store.clone(), clock, &node(), &config)
        .await
        .unwrap();
    let before = store.get("lease/node-a").await.unwrap().unwrap();
    store.continue_release.notify_one();
    shutdown.await.unwrap();
    let after = store.get("lease/node-a").await.unwrap().unwrap();
    assert_eq!(after.etag, before.etag);
    assert_eq!(after.bytes, before.bytes);
    new_holder.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn subscribers_see_loss_before_a_blocked_release_times_out() {
    let store = Arc::new(MemoryStore::default());
    let clock = Arc::new(TestClock::default());
    create_initial_manifest(store.as_ref(), clock.as_ref(), 2, &node())
        .await
        .unwrap();
    let holder = acquire_lease(store.clone(), clock, &node(), &LeaseConfig::default())
        .await
        .unwrap();
    let mut status = holder.subscribe();
    let before = store.get("lease/node-a").await.unwrap().unwrap();
    store.block_next_lease_renewal.store(true, Ordering::SeqCst);
    let shutdown = tokio::spawn(holder.shutdown());
    status.changed().await.unwrap();
    assert!(matches!(&*status.borrow(), LeaseStatus::Lost(_)));
    assert!(!shutdown.is_finished());
    tokio::time::advance(Duration::from_secs(11)).await;
    shutdown.await.unwrap();
    let after = store.get("lease/node-a").await.unwrap().unwrap();
    assert_eq!(after.etag, before.etag);
    assert_eq!(after.bytes, before.bytes);
}

#[tokio::test(start_paused = true)]
async fn release_handles_unknown_outcomes_with_and_without_a_write() {
    for was_written in [true, false] {
        let store = Arc::new(MemoryStore::default());
        let clock = Arc::new(TestClock::default());
        create_initial_manifest(store.as_ref(), clock.as_ref(), 2, &node())
            .await
            .unwrap();
        let holder = acquire_lease(store.clone(), clock, &node(), &LeaseConfig::default())
            .await
            .unwrap();
        let before = store.get("lease/node-a").await.unwrap().unwrap();
        if was_written {
            store
                .unknown_next_release_after_write
                .store(true, Ordering::SeqCst);
        } else {
            store
                .unknown_next_release_before_write
                .store(true, Ordering::SeqCst);
        }
        holder.shutdown().await;
        let after = store.get("lease/node-a").await.unwrap().unwrap();
        let released = NodeLease::decode(after.bytes).unwrap();
        assert_eq!(released.expires_at_unix_micros, i64::MIN);
        assert_ne!(after.etag, before.etag);
    }
}

#[tokio::test(start_paused = true)]
async fn waiter_stays_pending_while_holder_renews() {
    let store = Arc::new(MemoryStore::default());
    let clock = Arc::new(TestClock::default());
    create_initial_manifest(store.as_ref(), clock.as_ref(), 2, &node())
        .await
        .unwrap();
    let holder = acquire_lease(
        store.clone(),
        clock.clone(),
        &node(),
        &LeaseConfig::default(),
    )
    .await
    .unwrap();
    let waiter =
        tokio::spawn(
            async move { acquire_lease(store, clock, &node(), &LeaseConfig::default()).await },
        );
    tokio::task::yield_now().await;
    for _ in 0..6 {
        tokio::time::advance(Duration::from_secs(4)).await;
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
    }
    holder.shutdown().await;
    waiter.abort();
    assert!(matches!(waiter.await, Err(error) if error.is_cancelled()));
}

#[tokio::test(start_paused = true)]
async fn old_holder_reports_loss_after_takeover() {
    let store = Arc::new(MemoryStore::default());
    let clock = Arc::new(TestClock::default());
    create_initial_manifest(store.as_ref(), clock.as_ref(), 2, &node())
        .await
        .unwrap();
    let config = LeaseConfig {
        fail_fast: true,
        ..LeaseConfig::default()
    };
    let old_holder = acquire_lease(store.clone(), clock.clone(), &node(), &config)
        .await
        .unwrap();
    let mut old_status = old_holder.subscribe();
    clock.offset_micros.store(12_000_000, Ordering::SeqCst);
    let new_holder = acquire_lease(store, clock, &node(), &config).await.unwrap();
    assert_eq!(new_holder.epoch(), old_holder.epoch() + 1);
    tokio::time::advance(Duration::from_secs(4)).await;
    old_status.changed().await.unwrap();
    assert!(
        matches!(&*old_status.borrow(), LeaseStatus::Lost(detail) if detail.contains("precondition failed"))
    );
    old_holder.shutdown().await;
    new_holder.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn hung_renewal_reports_loss_at_the_local_deadline() {
    let store = Arc::new(MemoryStore::default());
    let clock = Arc::new(TestClock::default());
    create_initial_manifest(store.as_ref(), clock.as_ref(), 2, &node())
        .await
        .unwrap();
    let holder = acquire_lease(store.clone(), clock, &node(), &LeaseConfig::default())
        .await
        .unwrap();
    let mut status = holder.subscribe();
    store.block_next_lease_renewal.store(true, Ordering::SeqCst);
    tokio::time::advance(Duration::from_secs(9)).await;
    assert!(matches!(*status.borrow(), LeaseStatus::ValidUntil(_)));
    tokio::time::advance(Duration::from_secs(2)).await;
    status.changed().await.unwrap();
    assert!(matches!(&*status.borrow(), LeaseStatus::Lost(detail) if detail.contains("deadline")));
    holder.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn renewal_failure_reports_lease_loss() {
    let store = Arc::new(MemoryStore::default());
    let clock = Arc::new(TestClock::default());
    create_initial_manifest(store.as_ref(), clock.as_ref(), 2, &node())
        .await
        .unwrap();
    let handle = acquire_lease(store.clone(), clock, &node(), &LeaseConfig::default())
        .await
        .unwrap();
    let mut status = handle.subscribe();
    store.fail_next_lease_renewal.store(true, Ordering::SeqCst);
    tokio::time::advance(Duration::from_secs(4)).await;
    status.changed().await.unwrap();
    assert!(
        matches!(&*status.borrow(), LeaseStatus::Lost(detail) if detail.contains("injected lease renewal failure"))
    );
    handle.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn registry_adds_keys_and_rejects_type_conflicts_without_noop_writes() {
    let store = MemoryStore::default();
    let clock = TestClock::default();
    let declaration = AttributeIndexDeclaration {
        index_key: "status".to_owned(),
        index_type: 1,
    };
    let first = register_attribute_indexes(&store, &clock, 7, std::slice::from_ref(&declaration))
        .await
        .unwrap();
    assert_eq!(first.version, 1);
    let write_count = store.next_etag.load(Ordering::SeqCst);
    let second = register_attribute_indexes(&store, &clock, 7, &[declaration])
        .await
        .unwrap();
    assert_eq!(second.version, 1);
    assert_eq!(store.next_etag.load(Ordering::SeqCst), write_count);
    assert_eq!(
        read_attribute_index_registry(&store)
            .await
            .unwrap()
            .registrations
            .len(),
        1
    );
    let conflicting = AttributeIndexDeclaration {
        index_key: "status".to_owned(),
        index_type: 2,
    };
    assert!(matches!(
        register_attribute_indexes(&store, &clock, 7, &[conflicting]).await,
        Err(MetadataError::AttributeIndexConflict { .. })
    ));
    let fresh_store = MemoryStore::default();
    assert!(matches!(
        register_attribute_indexes(
            &fresh_store,
            &clock,
            7,
            &[
                AttributeIndexDeclaration {
                    index_key: "status".to_owned(),
                    index_type: 1
                },
                AttributeIndexDeclaration {
                    index_key: "status".to_owned(),
                    index_type: 2
                },
            ],
        )
        .await,
        Err(MetadataError::AttributeIndexConflict { .. })
    ));
    assert_eq!(fresh_store.object_count(), 0);
}
