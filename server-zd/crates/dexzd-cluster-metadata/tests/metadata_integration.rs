// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use dexzd_clock::WallClock;
use dexzd_cluster_metadata::{
    AttributeIndexDeclaration, Change, ClusterConfig, LeaseConfig, LeaseStatus, MetadataError,
    NodeConfig, acquire_lease, bootstrap_or_validate, bump_node_epoch, create_initial_manifest,
    read_manifest, read_registry, register, update_manifest,
};
use dexzd_object_store::{
    ByteRange, ObjectContent, ObjectListing, ObjectMetadata, ObjectStore, ObjectStoreError,
    ObjectStoreErrorKind, ObjectStoreOperation, ObjectStoreResult, WriteCondition, WriteOutcome,
};
use dexzd_protocol::dexzd::v1::{ClusterObject, ControllerLease, Manifest};
use prost::Message;
use tokio::sync::Barrier;
use tokio::time::Instant;

#[derive(Default)]
struct MemoryStore {
    objects: Mutex<BTreeMap<String, (Bytes, String)>>,
    next_etag: AtomicU64,
    unknown_next_manifest_write: AtomicBool,
    fail_next_lease_renewal: AtomicBool,
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
}

impl Default for TestClock {
    fn default() -> Self {
        Self {
            start: Instant::now(),
            unix_micros: 1_800_000_000_000_000,
        }
    }
}

impl WallClock for TestClock {
    fn now_unix_micros(&self) -> i64 {
        self.unix_micros + self.start.elapsed().as_micros() as i64
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
            Ok(Change::Changed)
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
            Ok(Change::Changed)
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
        Ok(Change::Changed)
    })
    .await
    .unwrap();
    assert_eq!(result.version, 2);
    assert_eq!(read_manifest(&store).await.unwrap().unwrap().version, 2);
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
    first.shutdown().await;
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
    first.shutdown().await;
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
    let first = register(&store, &clock, 7, std::slice::from_ref(&declaration))
        .await
        .unwrap();
    assert_eq!(first.version, 1);
    let write_count = store.next_etag.load(Ordering::SeqCst);
    let second = register(&store, &clock, 7, &[declaration]).await.unwrap();
    assert_eq!(second.version, 1);
    assert_eq!(store.next_etag.load(Ordering::SeqCst), write_count);
    assert_eq!(read_registry(&store).await.unwrap().registrations.len(), 1);
    let conflicting = AttributeIndexDeclaration {
        index_key: "status".to_owned(),
        index_type: 2,
    };
    assert!(matches!(
        register(&store, &clock, 7, &[conflicting]).await,
        Err(MetadataError::AttributeIndexConflict { .. })
    ));
}
