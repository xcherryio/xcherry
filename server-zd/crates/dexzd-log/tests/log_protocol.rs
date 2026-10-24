// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use dexzd_clock::WallClock;
use dexzd_log::{
    BatchContent, BatchRecord, CommitPipeline, CommitPipelineConfig, LogError, ProposalFailure,
    StreamIdentity, decode_batch, encode_batch, read_partition_records, recover_stream,
    recover_stream_from,
};
use dexzd_object_store::{
    ByteRange, ObjectContent, ObjectListing, ObjectMetadata, ObjectStore, ObjectStoreError,
    ObjectStoreErrorKind, ObjectStoreOperation, ObjectStoreResult, WriteCondition, WriteOutcome,
};
use dexzd_protocol::dexzd::v1::LogBatchHeader;
use prost::Message;
use tokio::sync::{Mutex, Notify};

struct FixedClock;

impl WallClock for FixedClock {
    fn now_unix_micros(&self) -> i64 {
        1_700_000_000_000_000
    }
}

#[derive(Default)]
struct AdvancingClock(AtomicI64);

impl WallClock for AdvancingClock {
    fn now_unix_micros(&self) -> i64 {
        self.0.fetch_add(1_000, Ordering::SeqCst) + 1_700_000_000_000_000
    }
}

#[derive(Default)]
struct MemoryStore {
    state: Mutex<StoreState>,
    blocked_seq: Mutex<HashMap<u64, Arc<Notify>>>,
    put_count: AtomicUsize,
    range_count: AtomicUsize,
    multipart_count: AtomicUsize,
}

#[derive(Default)]
struct StoreState {
    objects: BTreeMap<String, Bytes>,
    unknown_after_write: HashSet<String>,
    unknown_then_transient_get: HashSet<String>,
    transient_get_once: HashSet<String>,
    race_insert: HashMap<String, Bytes>,
    version: u64,
}

impl MemoryStore {
    async fn insert(&self, key: String, body: Bytes) {
        self.state.lock().await.objects.insert(key, body);
    }

    async fn block_seq(&self, seq: u64) -> Arc<Notify> {
        let release = Arc::new(Notify::new());
        self.blocked_seq.lock().await.insert(seq, release.clone());
        release
    }

    async fn inject_unknown_after_write(&self, key: String) {
        self.state.lock().await.unknown_after_write.insert(key);
    }

    async fn inject_unknown_then_transient_readback(&self, key: String) {
        let mut state = self.state.lock().await;
        state.unknown_after_write.insert(key.clone());
        state.unknown_then_transient_get.insert(key);
    }

    async fn inject_race(&self, key: String, body: Bytes) {
        self.state.lock().await.race_insert.insert(key, body);
    }

    async fn corrupt(&self, key: &str, offset: usize) {
        let mut state = self.state.lock().await;
        let mut body = state.objects.get(key).expect("test object exists").to_vec();
        body[offset] ^= 1;
        state.objects.insert(key.to_owned(), Bytes::from(body));
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
        self.put_count.fetch_add(1, Ordering::SeqCst);
        let seq = key
            .rsplit('/')
            .next()
            .and_then(|value| value.parse::<u64>().ok());
        if let Some(seq) = seq {
            let release = self.blocked_seq.lock().await.get(&seq).cloned();
            if let Some(release) = release {
                release.notified().await;
            }
        }
        let mut state = self.state.lock().await;
        if let Some(race_body) = state.race_insert.remove(key) {
            state.objects.insert(key.to_owned(), race_body);
        }
        let can_write = match &condition {
            WriteCondition::Unconditional => true,
            WriteCondition::CreateOnly => !state.objects.contains_key(key),
            WriteCondition::ReplaceIfEtagMatches { etag } => {
                state.objects.get(key).is_some_and(|current| {
                    etag == &format!("{:x}", xxhash_rust::xxh3::xxh3_64(current))
                })
            }
        };
        if !can_write {
            return Ok(WriteOutcome::PreconditionFailed);
        }
        if body.len() > 8 * 1024 * 1024 {
            self.multipart_count.fetch_add(1, Ordering::SeqCst);
        }
        let etag = format!("{:x}", xxhash_rust::xxh3::xxh3_64(&body));
        state.objects.insert(key.to_owned(), body);
        state.version += 1;
        if state.unknown_after_write.remove(key) {
            if state.unknown_then_transient_get.remove(key) {
                state.transient_get_once.insert(key.to_owned());
            }
            return Err(ObjectStoreError::new(
                ObjectStoreOperation::Put,
                key,
                ObjectStoreErrorKind::OutcomeUnknown,
                "injected lost response",
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
        let mut state = self.state.lock().await;
        if state.transient_get_once.remove(key) {
            return Err(ObjectStoreError::new(
                ObjectStoreOperation::Get,
                key,
                ObjectStoreErrorKind::Transient,
                "injected read failure",
            ));
        }
        Ok(state.objects.get(key).map(|body| ObjectContent {
            bytes: body.clone(),
            etag: format!("{:x}", xxhash_rust::xxh3::xxh3_64(body)),
            object_size: body.len() as u64,
        }))
    }

    async fn get_range(
        &self,
        key: &str,
        range: ByteRange,
    ) -> ObjectStoreResult<Option<ObjectContent>> {
        self.range_count.fetch_add(1, Ordering::SeqCst);
        Ok(self.get(key).await?.map(|mut object| {
            let start = (range.start as usize).min(object.bytes.len());
            let end = (range.end_inclusive as usize + 1).min(object.bytes.len());
            object.bytes = object.bytes.slice(start..end.max(start));
            object
        }))
    }

    async fn get_to_file(&self, key: &str, path: &Path) -> ObjectStoreResult<Option<u64>> {
        let Some(object) = self.get(key).await? else {
            return Ok(None);
        };
        tokio::fs::write(path, &object.bytes)
            .await
            .map_err(|error| {
                ObjectStoreError::new(
                    ObjectStoreOperation::Get,
                    key,
                    ObjectStoreErrorKind::Permanent,
                    error.to_string(),
                )
            })?;
        Ok(Some(object.object_size))
    }

    async fn head(&self, key: &str) -> ObjectStoreResult<Option<ObjectMetadata>> {
        Ok(self.get(key).await?.map(|object| ObjectMetadata {
            key: key.to_owned(),
            size: object.object_size,
            etag: object.etag,
        }))
    }

    async fn delete(&self, key: &str) -> ObjectStoreResult<()> {
        self.state.lock().await.objects.remove(key);
        Ok(())
    }

    async fn delete_many(&self, keys: &[String]) -> ObjectStoreResult<()> {
        let mut state = self.state.lock().await;
        for key in keys {
            state.objects.remove(key);
        }
        Ok(())
    }

    async fn list(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        max_keys: u32,
    ) -> ObjectStoreResult<ObjectListing> {
        let state = self.state.lock().await;
        let objects: Vec<_> = state
            .objects
            .iter()
            .filter(|(key, _)| {
                key.starts_with(prefix) && start_after.is_none_or(|after| key.as_str() > after)
            })
            .take(max_keys as usize)
            .map(|(key, body)| ObjectMetadata {
                key: key.clone(),
                size: body.len() as u64,
                etag: format!("{:x}", xxhash_rust::xxh3::xxh3_64(body)),
            })
            .collect();
        Ok(ObjectListing {
            objects,
            next_start_after: None,
        })
    }
}

fn stream() -> StreamIdentity {
    StreamIdentity {
        cluster_id: [7; 16],
        node_id: "node-a".to_owned(),
        incarnation: 3,
    }
}

fn batch(epoch: u64, seq: u64, data: &[u8]) -> Bytes {
    encode_batch(&BatchContent {
        stream: stream(),
        epoch,
        seq,
        sealed_at_unix_micros: 42,
        records: vec![BatchRecord {
            partition: 2,
            bytes: Bytes::copy_from_slice(data),
        }],
        epoch_start_writer: None,
    })
    .expect("test batch encodes")
}

fn epoch_start(epoch: u64, seq: u64) -> Bytes {
    encode_batch(&BatchContent {
        stream: stream(),
        epoch,
        seq,
        sealed_at_unix_micros: 42,
        records: Vec::new(),
        epoch_start_writer: Some("writer".to_owned()),
    })
    .expect("test EpochStart encodes")
}

fn rebuild_batch_with_header(original: &[u8], mut header: LogBatchHeader, unknown: &[u8]) -> Bytes {
    let original_header_length =
        u32::from_le_bytes(original[10..14].try_into().expect("test header length")) as usize;
    let original_offsets: Vec<_> = header
        .partition_frames
        .iter()
        .map(|frame| frame.byte_offset)
        .collect();
    let mut offset_delta = 0_i64;
    let header_bytes = loop {
        for (frame, original_offset) in header.partition_frames.iter_mut().zip(&original_offsets) {
            frame.byte_offset = u64::try_from(*original_offset as i64 + offset_delta)
                .expect("test offset remains positive");
        }
        let encoded = header.encode_to_vec();
        let next_delta =
            encoded.len() as i64 + unknown.len() as i64 - original_header_length as i64;
        if next_delta == offset_delta {
            break encoded;
        }
        offset_delta = next_delta;
    };
    let mut rebuilt = Vec::new();
    rebuilt.extend_from_slice(&original[..10]);
    rebuilt.extend_from_slice(
        &u32::try_from(header_bytes.len() + unknown.len())
            .expect("test header length fits")
            .to_le_bytes(),
    );
    rebuilt.extend_from_slice(&header_bytes);
    rebuilt.extend_from_slice(unknown);
    rebuilt.extend_from_slice(&original[14 + original_header_length..original.len() - 16]);
    rebuilt.extend_from_slice(&xxhash_rust::xxh3::xxh3_128(&rebuilt).to_le_bytes());
    Bytes::from(rebuilt)
}

#[tokio::test]
async fn codec_round_trip_range_read_and_corruption() {
    let store = MemoryStore::default();
    let content = BatchContent {
        stream: stream(),
        epoch: 2,
        seq: 1,
        sealed_at_unix_micros: 55,
        records: vec![
            BatchRecord {
                partition: 9,
                bytes: Bytes::from_static(b"nine"),
            },
            BatchRecord {
                partition: 2,
                bytes: Bytes::from_static(b"two"),
            },
            BatchRecord {
                partition: 2,
                bytes: Bytes::from_static(b"also two"),
            },
        ],
        epoch_start_writer: None,
    };
    let body = encode_batch(&content).unwrap();
    let decoded = decode_batch(&body).unwrap();
    assert_eq!(decoded.records.len(), 3);
    assert_eq!(decoded.records[0].bytes, "two");
    assert_eq!(decoded.header.partition_frames[0].partition, 2);
    store.insert(stream().batch_key(1), body.clone()).await;
    let (_, records) = read_partition_records(&store, &stream().batch_key(1), 9)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].bytes, "nine");
    assert_eq!(store.range_count.load(Ordering::SeqCst), 2);
    store.corrupt(&stream().batch_key(1), body.len() - 1).await;
    assert!(matches!(
        decode_batch(
            &store
                .get(&stream().batch_key(1))
                .await
                .unwrap()
                .unwrap()
                .bytes
        ),
        Err(LogError::Corrupt(_))
    ));
    let frame_offset = decoded.header.partition_frames[0].byte_offset as usize;
    store.insert(stream().batch_key(1), body).await;
    store.corrupt(&stream().batch_key(1), frame_offset).await;
    assert!(matches!(
        read_partition_records(&store, &stream().batch_key(1), 2).await,
        Err(LogError::Corrupt(_))
    ));
}

#[tokio::test]
async fn decoder_accepts_additive_unknown_header_fields() {
    let store = MemoryStore::default();
    let original = batch(1, 1, b"future header");
    let header_length = u32::from_le_bytes(original[10..14].try_into().unwrap()) as usize;
    let header = LogBatchHeader::decode(&original[14..14 + header_length]).unwrap();
    let future = rebuild_batch_with_header(&original, header, &[0xa0, 0x06, 0x01]);
    assert_eq!(
        decode_batch(&future).unwrap().records[0].bytes,
        "future header"
    );
    store.insert(stream().batch_key(1), future).await;
    let (_, records) = read_partition_records(&store, &stream().batch_key(1), 2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(records[0].bytes, "future header");
}

#[tokio::test]
async fn node_ids_are_validated_before_key_use_and_after_decode() {
    let mut invalid_stream = stream();
    invalid_stream.node_id = "node/a".to_owned();
    let invalid_content = BatchContent {
        stream: invalid_stream.clone(),
        epoch: 1,
        seq: 1,
        sealed_at_unix_micros: 0,
        records: vec![BatchRecord {
            partition: 2,
            bytes: Bytes::from_static(b"value"),
        }],
        epoch_start_writer: None,
    };
    assert!(matches!(
        encode_batch(&invalid_content),
        Err(LogError::Invalid(_))
    ));
    let store = Arc::new(MemoryStore::default());
    assert!(matches!(
        CommitPipeline::start(
            store.clone(),
            Arc::new(FixedClock),
            invalid_stream.clone(),
            1,
            1,
            CommitPipelineConfig::default()
        ),
        Err(LogError::Invalid(_))
    ));
    assert!(matches!(
        recover_stream(
            store.as_ref(),
            &FixedClock,
            &invalid_stream,
            0,
            0,
            1,
            "writer"
        )
        .await,
        Err(LogError::Invalid(_))
    ));

    let original = batch(1, 1, b"value");
    let header_length = u32::from_le_bytes(original[10..14].try_into().unwrap()) as usize;
    let mut header = LogBatchHeader::decode(&original[14..14 + header_length]).unwrap();
    header.node_id = "node/a".to_owned();
    let forged = rebuild_batch_with_header(&original, header, &[]);
    assert!(matches!(decode_batch(&forged), Err(LogError::Corrupt(_))));
}

#[tokio::test]
async fn recovery_seals_first_hole_and_voids_older_pipelined_object() {
    let store = Arc::new(MemoryStore::default());
    store
        .insert(stream().batch_key(1), batch(1, 1, b"committed"))
        .await;
    store
        .insert(stream().batch_key(3), batch(1, 3, b"unacknowledged"))
        .await;
    store
        .insert(
            stream().batch_key(4),
            batch(1, 4, b"another unacknowledged"),
        )
        .await;
    let recovered = recover_stream(
        store.as_ref(),
        &FixedClock,
        &stream(),
        0,
        0,
        2,
        "new-writer",
    )
    .await
    .unwrap();
    assert_eq!(recovered.epoch_start_seq, 2);
    assert_eq!(recovered.batches_to_apply.len(), 1);
    assert_eq!(recovered.void_sequences, vec![3, 4]);
    assert_eq!(recovered.next_seq, 5);
    let (pipeline, mut committed) = CommitPipeline::start(
        store.clone(),
        Arc::new(FixedClock),
        stream(),
        2,
        recovered.next_seq,
        CommitPipelineConfig::default(),
    )
    .unwrap();
    let first = pipeline
        .propose(2, Bytes::from_static(b"first replacement"), true)
        .await
        .unwrap();
    let second = pipeline
        .propose(2, Bytes::from_static(b"second replacement"), true)
        .await
        .unwrap();
    assert_eq!(first.committed.await.unwrap().unwrap(), 5);
    assert_eq!(second.committed.await.unwrap().unwrap(), 6);
    assert_eq!(committed.recv().await.unwrap().seq, 5);
    assert_eq!(committed.recv().await.unwrap().seq, 6);
    assert_eq!(
        decode_batch(
            &store
                .get(&stream().batch_key(5))
                .await
                .unwrap()
                .unwrap()
                .bytes
        )
        .unwrap()
        .records[0]
            .bytes,
        "first replacement"
    );
    assert_eq!(
        decode_batch(
            &store
                .get(&stream().batch_key(6))
                .await
                .unwrap()
                .unwrap()
                .bytes
        )
        .unwrap()
        .records[0]
            .bytes,
        "second replacement"
    );
    drop(pipeline);
    let recovered_again = recover_stream(
        store.as_ref(),
        &FixedClock,
        &stream(),
        0,
        0,
        3,
        "third-writer",
    )
    .await
    .unwrap();
    assert_eq!(recovered_again.void_sequences, vec![3, 4]);
    assert_eq!(
        recovered_again
            .batches_to_apply
            .iter()
            .map(|batch| batch.header.seq)
            .collect::<Vec<_>>(),
        vec![1, 2, 5, 6]
    );
}

#[tokio::test]
async fn racing_older_writer_restarts_pipeline_without_resequencing() {
    let store = Arc::new(MemoryStore::default());
    store
        .insert(stream().batch_key(1), batch(1, 1, b"old committed"))
        .await;
    let recovered = recover_stream(store.as_ref(), &FixedClock, &stream(), 0, 0, 2, "new")
        .await
        .unwrap();
    assert_eq!(recovered.next_seq, 3);
    store
        .inject_race(stream().batch_key(3), batch(1, 3, b"racing old writer"))
        .await;
    let release = store.block_seq(3).await;
    let config = CommitPipelineConfig {
        max_in_flight: 1,
        max_batch_records: 1,
        ..Default::default()
    };
    let (pipeline, mut committed) = CommitPipeline::start(
        store.clone(),
        Arc::new(FixedClock),
        stream(),
        2,
        recovered.next_seq,
        config,
    )
    .unwrap();
    let first = pipeline
        .propose(2, Bytes::from_static(b"first"), true)
        .await
        .unwrap();
    let second = pipeline
        .propose(2, Bytes::from_static(b"second"), true)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while store.put_count.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    release.notify_one();
    assert_eq!(
        first.committed.await.unwrap(),
        Err(ProposalFailure::Restart)
    );
    assert_eq!(
        second.committed.await.unwrap(),
        Err(ProposalFailure::Restart)
    );
    assert!(committed.recv().await.is_none());
    assert!(store.get(&stream().batch_key(4)).await.unwrap().is_none());
}

#[tokio::test]
async fn empty_store_starts_at_manifest_segment_sequence() {
    let store = MemoryStore::default();
    let recovered = recover_stream_from(&store, &FixedClock, &stream(), 42, 0, 1, "first-writer")
        .await
        .unwrap();
    assert_eq!(recovered.epoch_start_seq, 42);
    assert_eq!(recovered.next_seq, 43);
    assert!(store.get(&stream().batch_key(1)).await.unwrap().is_none());
}

#[tokio::test]
async fn recovery_handles_all_precondition_outcomes() {
    let store = MemoryStore::default();
    store
        .inject_race(stream().batch_key(1), batch(1, 1, b"old committed"))
        .await;
    let recovered = recover_stream(&store, &FixedClock, &stream(), 0, 0, 2, "new")
        .await
        .unwrap();
    assert_eq!(recovered.epoch_start_seq, 2);
    assert_eq!(
        recovered.batches_to_apply[0].records[0].bytes,
        "old committed"
    );

    let lost = MemoryStore::default();
    lost.inject_unknown_after_write(stream().batch_key(1)).await;
    let recovered = recover_stream(&lost, &FixedClock, &stream(), 0, 0, 2, "new")
        .await
        .unwrap();
    assert_eq!(recovered.epoch_start_seq, 1);

    let same = MemoryStore::default();
    let own_epoch_start = encode_batch(&BatchContent {
        stream: stream(),
        epoch: 2,
        seq: 1,
        sealed_at_unix_micros: FixedClock.now_unix_micros(),
        records: Vec::new(),
        epoch_start_writer: Some("new".to_owned()),
    })
    .unwrap();
    same.inject_race(stream().batch_key(1), own_epoch_start)
        .await;
    same.insert(stream().batch_key(2), batch(1, 2, b"old tail"))
        .await;
    let recovered = recover_stream(&same, &FixedClock, &stream(), 0, 0, 2, "new")
        .await
        .unwrap();
    assert_eq!(recovered.epoch_start_seq, 1);
    assert_eq!(recovered.void_sequences, vec![2]);
    assert_eq!(recovered.next_seq, 3);

    let fenced = MemoryStore::default();
    fenced
        .inject_race(stream().batch_key(1), epoch_start(2, 1))
        .await;
    assert!(matches!(
        recover_stream(&fenced, &FixedClock, &stream(), 0, 0, 2, "new").await,
        Err(LogError::Fenced)
    ));

    let fenced_after_barrier = MemoryStore::default();
    fenced_after_barrier
        .insert(stream().batch_key(2), batch(2, 2, b"competing writer"))
        .await;
    assert!(matches!(
        recover_stream(
            &fenced_after_barrier,
            &FixedClock,
            &stream(),
            0,
            0,
            2,
            "new"
        )
        .await,
        Err(LogError::Fenced)
    ));
}

#[tokio::test]
async fn epoch_start_retry_uses_the_original_bytes() {
    let store = MemoryStore::default();
    store
        .inject_unknown_then_transient_readback(stream().batch_key(1))
        .await;
    let clock = AdvancingClock::default();
    let recovered = recover_stream(&store, &clock, &stream(), 0, 0, 1, "writer")
        .await
        .unwrap();
    assert_eq!(recovered.epoch_start_seq, 1);
    assert_eq!(recovered.next_seq, 2);
    assert!(store.put_count.load(Ordering::SeqCst) >= 2);
}

#[tokio::test]
async fn recovery_rejects_corrupt_objects() {
    let store = MemoryStore::default();
    let key = stream().batch_key(1);
    let body = batch(1, 1, b"value");
    store.insert(key.clone(), body.clone()).await;
    store.corrupt(&key, body.len() - 1).await;
    assert!(matches!(
        recover_stream(&store, &FixedClock, &stream(), 0, 0, 2, "new").await,
        Err(LogError::Corrupt(_))
    ));
}

#[tokio::test]
async fn recovery_resumes_after_a_cleaned_applied_position() {
    let store = MemoryStore::default();
    store
        .insert(stream().batch_key(2), batch(1, 2, b"void tail"))
        .await;
    store
        .insert(stream().batch_key(3), batch(2, 3, b"valid tail"))
        .await;
    let recovered = recover_stream(&store, &FixedClock, &stream(), 1, 2, 3, "new")
        .await
        .unwrap();
    assert_eq!(recovered.void_sequences, vec![2]);
    assert_eq!(recovered.batches_to_apply[0].records[0].bytes, "valid tail");
    assert_eq!(recovered.epoch_start_seq, 4);
    assert!(store.get(&stream().batch_key(1)).await.unwrap().is_none());
}

#[tokio::test]
async fn recovery_rejects_a_deleted_range_after_the_applied_position() {
    let store = MemoryStore::default();
    for seq in 1..=10 {
        store
            .insert(stream().batch_key(seq), batch(1, seq, b"acknowledged"))
            .await;
    }
    // Cleanup after a newer snapshot deleted seq 1 through 6, including the applied seq 3.
    let deleted: Vec<String> = (1..=6).map(|seq| stream().batch_key(seq)).collect();
    store.delete_many(&deleted).await.unwrap();
    let result = recover_stream(&store, &FixedClock, &stream(), 3, 1, 2, "stale-state").await;
    assert!(
        matches!(
            result,
            Err(LogError::MissingBatches {
                missing_seq: 4,
                next_existing_seq: 7
            })
        ),
        "{result:?}"
    );
    assert_eq!(store.put_count.load(Ordering::SeqCst), 0);
    assert!(store.get(&stream().batch_key(4)).await.unwrap().is_none());
}

#[tokio::test]
async fn recovery_seals_a_pipelined_hole_at_the_snapshot_boundary() {
    let store = MemoryStore::default();
    // Cleanup kept the snapshot's applied seq 3; seq 4 never landed, but pipelined 5 and 6 did.
    for seq in [3, 5, 6] {
        store
            .insert(stream().batch_key(seq), batch(1, seq, b"pipelined"))
            .await;
    }
    let recovered = recover_stream(&store, &FixedClock, &stream(), 3, 1, 2, "replacement")
        .await
        .unwrap();
    assert!(recovered.batches_to_apply.is_empty());
    assert_eq!(recovered.epoch_start_seq, 4);
    assert_eq!(recovered.void_sequences, vec![5, 6]);
    assert_eq!(recovered.next_seq, 7);
}

#[tokio::test]
async fn older_epoch_start_after_barrier_cannot_lower_effective_epoch() {
    let store = MemoryStore::default();
    store.insert(stream().batch_key(1), epoch_start(2, 1)).await;
    store.insert(stream().batch_key(2), epoch_start(1, 2)).await;
    store
        .insert(
            stream().batch_key(3),
            batch(1, 3, b"void after older marker"),
        )
        .await;
    let recovered = recover_stream(&store, &FixedClock, &stream(), 0, 0, 3, "replacement")
        .await
        .unwrap();
    assert_eq!(recovered.void_sequences, vec![2, 3]);
    assert_eq!(recovered.batches_to_apply.len(), 1);
    assert_eq!(recovered.epoch_start_seq, 4);
}

#[tokio::test]
async fn pipeline_reports_out_of_order_puts_in_sequence_order() {
    let store = Arc::new(MemoryStore::default());
    let release = store.block_seq(1).await;
    let config = CommitPipelineConfig {
        max_batch_records: 1,
        ..Default::default()
    };
    let (pipeline, mut committed) =
        CommitPipeline::start(store.clone(), Arc::new(FixedClock), stream(), 1, 1, config).unwrap();
    let first = pipeline
        .propose(2, Bytes::from_static(b"first"), true)
        .await
        .unwrap();
    let mut second = pipeline
        .propose(2, Bytes::from_static(b"second"), true)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while store.put_count.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::task::yield_now().await;
    assert!(second.committed.try_recv().is_err());
    assert!(committed.try_recv().is_err());
    release.notify_one();
    assert_eq!(first.committed.await.unwrap().unwrap(), 1);
    assert_eq!(second.committed.await.unwrap().unwrap(), 2);
    assert_eq!(committed.recv().await.unwrap().seq, 1);
    assert_eq!(committed.recv().await.unwrap().seq, 2);
}

#[tokio::test(start_paused = true)]
async fn nonwaiting_proposals_share_a_window_batch() {
    let store = Arc::new(MemoryStore::default());
    let (pipeline, mut committed) = CommitPipeline::start(
        store.clone(),
        Arc::new(FixedClock),
        stream(),
        1,
        1,
        CommitPipelineConfig::default(),
    )
    .unwrap();
    let mut first = pipeline
        .propose(2, Bytes::from_static(b"first"), false)
        .await
        .unwrap();
    let second = pipeline
        .propose(9, Bytes::from_static(b"second"), false)
        .await
        .unwrap();
    tokio::task::yield_now().await;
    assert!(first.committed.try_recv().is_err());
    assert_eq!(store.put_count.load(Ordering::SeqCst), 0);
    tokio::time::advance(Duration::from_millis(20)).await;
    assert_eq!(first.committed.await.unwrap().unwrap(), 1);
    assert_eq!(second.committed.await.unwrap().unwrap(), 1);
    let batch = committed.recv().await.unwrap();
    assert_eq!(batch.proposal_tickets.len(), 2);
    assert_eq!(batch.records_by_partition.len(), 2);
}

#[tokio::test]
async fn in_flight_limit_delays_later_puts() {
    let store = Arc::new(MemoryStore::default());
    let release = store.block_seq(1).await;
    let config = CommitPipelineConfig {
        max_in_flight: 1,
        max_batch_records: 1,
        ..Default::default()
    };
    let (pipeline, mut committed) =
        CommitPipeline::start(store.clone(), Arc::new(FixedClock), stream(), 1, 1, config).unwrap();
    let first = pipeline
        .propose(2, Bytes::from_static(b"first"), true)
        .await
        .unwrap();
    let second = pipeline
        .propose(2, Bytes::from_static(b"second"), true)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while store.put_count.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(store.put_count.load(Ordering::SeqCst), 1);
    release.notify_one();
    assert_eq!(first.committed.await.unwrap().unwrap(), 1);
    assert_eq!(second.committed.await.unwrap().unwrap(), 2);
    assert_eq!(committed.recv().await.unwrap().seq, 1);
    assert_eq!(committed.recv().await.unwrap().seq, 2);
}

#[tokio::test]
async fn pipeline_handles_lost_response_fencing_and_backpressure() {
    let store = Arc::new(MemoryStore::default());
    store
        .inject_unknown_after_write(stream().batch_key(1))
        .await;
    let (pipeline, mut committed) = CommitPipeline::start(
        store.clone(),
        Arc::new(FixedClock),
        stream(),
        1,
        1,
        CommitPipelineConfig::default(),
    )
    .unwrap();
    let ticket = pipeline
        .propose(2, Bytes::from_static(b"value"), true)
        .await
        .unwrap();
    assert_eq!(ticket.committed.await.unwrap().unwrap(), 1);
    assert_eq!(committed.recv().await.unwrap().seq, 1);
    drop(pipeline);

    let same = Arc::new(MemoryStore::default());
    let own_body = encode_batch(&BatchContent {
        stream: stream(),
        epoch: 1,
        seq: 1,
        sealed_at_unix_micros: FixedClock.now_unix_micros(),
        records: vec![BatchRecord {
            partition: 2,
            bytes: Bytes::from_static(b"same"),
        }],
        epoch_start_writer: None,
    })
    .unwrap();
    same.inject_race(stream().batch_key(1), own_body).await;
    let (pipeline, mut committed) = CommitPipeline::start(
        same,
        Arc::new(FixedClock),
        stream(),
        1,
        1,
        CommitPipelineConfig::default(),
    )
    .unwrap();
    let ticket = pipeline
        .propose(2, Bytes::from_static(b"same"), true)
        .await
        .unwrap();
    assert_eq!(ticket.committed.await.unwrap().unwrap(), 1);
    assert_eq!(committed.recv().await.unwrap().seq, 1);
    drop(pipeline);

    let fenced = Arc::new(MemoryStore::default());
    fenced
        .insert(stream().batch_key(1), epoch_start(2, 1))
        .await;
    let (pipeline, _) = CommitPipeline::start(
        fenced,
        Arc::new(FixedClock),
        stream(),
        1,
        1,
        CommitPipelineConfig::default(),
    )
    .unwrap();
    let ticket = pipeline
        .propose(2, Bytes::from_static(b"stale"), true)
        .await
        .unwrap();
    assert_eq!(
        ticket.committed.await.unwrap(),
        Err(ProposalFailure::Fenced)
    );

    let slow = Arc::new(MemoryStore::default());
    let release = slow.block_seq(1).await;
    let config = CommitPipelineConfig {
        max_pending_bytes: 4,
        admission_timeout: Duration::from_millis(10),
        ..Default::default()
    };
    let (pipeline, _) =
        CommitPipeline::start(slow, Arc::new(FixedClock), stream(), 1, 1, config).unwrap();
    let _first = pipeline
        .propose(2, Bytes::from_static(b"full"), true)
        .await
        .unwrap();
    assert!(matches!(
        pipeline.propose(2, Bytes::from_static(b"more"), true).await,
        Err(LogError::Retryable)
    ));
    release.notify_waiters();
}

#[tokio::test]
async fn large_batch_uses_store_multipart_path() {
    let store = Arc::new(MemoryStore::default());
    let mut seed = 9_u64;
    let mut random = vec![0_u8; 9 * 1024 * 1024];
    for byte in &mut random {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        *byte = seed as u8;
    }
    let (pipeline, mut committed) = CommitPipeline::start(
        store.clone(),
        Arc::new(FixedClock),
        stream(),
        1,
        1,
        CommitPipelineConfig::default(),
    )
    .unwrap();
    let ticket = pipeline
        .propose(2, Bytes::from(random), true)
        .await
        .unwrap();
    assert_eq!(ticket.committed.await.unwrap().unwrap(), 1);
    assert_eq!(committed.recv().await.unwrap().seq, 1);
    assert_eq!(store.multipart_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn oversized_records_are_invalid_before_write() {
    let oversized = Bytes::from(vec![0_u8; 32 * 1024 * 1024 + 1]);
    let content = BatchContent {
        stream: stream(),
        epoch: 1,
        seq: 1,
        sealed_at_unix_micros: 0,
        records: vec![BatchRecord {
            partition: 2,
            bytes: oversized.clone(),
        }],
        epoch_start_writer: None,
    };
    assert!(matches!(encode_batch(&content), Err(LogError::Invalid(_))));
    let store = Arc::new(MemoryStore::default());
    let (pipeline, _) = CommitPipeline::start(
        store.clone(),
        Arc::new(FixedClock),
        stream(),
        1,
        1,
        CommitPipelineConfig::default(),
    )
    .unwrap();
    assert!(matches!(
        pipeline.propose(2, oversized, true).await,
        Err(LogError::Invalid(_))
    ));
    assert_eq!(store.put_count.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn pipeline_seals_before_partition_frame_exceeds_decoder_limit() {
    let record = Bytes::from(vec![0_u8; 32 * 1024 * 1024]);
    let content = BatchContent {
        stream: stream(),
        epoch: 1,
        seq: 1,
        sealed_at_unix_micros: 0,
        records: vec![
            BatchRecord {
                partition: 2,
                bytes: record.clone(),
            },
            BatchRecord {
                partition: 2,
                bytes: record.clone(),
            },
        ],
        epoch_start_writer: None,
    };
    assert!(matches!(encode_batch(&content), Err(LogError::Invalid(_))));

    let store = Arc::new(MemoryStore::default());
    let config = CommitPipelineConfig {
        batch_window: Duration::from_secs(1),
        target_batch_bytes: usize::MAX,
        ..Default::default()
    };
    let (pipeline, mut committed) =
        CommitPipeline::start(store.clone(), Arc::new(FixedClock), stream(), 1, 1, config).unwrap();
    let first = pipeline.propose(2, record.clone(), false).await.unwrap();
    let second = pipeline.propose(2, record, false).await.unwrap();
    while store.put_count.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(first.committed.await.unwrap().unwrap(), 1);
    assert_eq!(second.committed.await.unwrap().unwrap(), 2);
    assert_eq!(committed.recv().await.unwrap().seq, 1);
    assert_eq!(committed.recv().await.unwrap().seq, 2);
    assert_eq!(
        decode_batch(
            &store
                .get(&stream().batch_key(1))
                .await
                .unwrap()
                .unwrap()
                .bytes
        )
        .unwrap()
        .records
        .len(),
        1
    );
    assert_eq!(
        decode_batch(
            &store
                .get(&stream().batch_key(2))
                .await
                .unwrap()
                .unwrap()
                .bytes
        )
        .unwrap()
        .records
        .len(),
        1
    );
}

#[tokio::test(start_paused = true)]
async fn pipeline_fails_when_head_put_exceeds_deadline() {
    let store = Arc::new(MemoryStore::default());
    let _release = store.block_seq(1).await;
    let config = CommitPipelineConfig {
        head_deadline: Duration::from_secs(2),
        ..Default::default()
    };
    let (pipeline, _) =
        CommitPipeline::start(store, Arc::new(FixedClock), stream(), 1, 1, config).unwrap();
    let ticket = pipeline
        .propose(2, Bytes::from_static(b"blocked"), true)
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(3)).await;
    assert_eq!(
        ticket.committed.await.unwrap(),
        Err(ProposalFailure::Unavailable)
    );
}

#[tokio::test]
async fn crashed_pipeline_leaves_a_void_tail_and_stale_writer_is_fenced() {
    let store = Arc::new(MemoryStore::default());
    let release = store.block_seq(1).await;
    let config = CommitPipelineConfig {
        max_batch_records: 1,
        ..Default::default()
    };
    let (pipeline, _) =
        CommitPipeline::start(store.clone(), Arc::new(FixedClock), stream(), 1, 1, config).unwrap();
    let first = pipeline
        .propose(2, Bytes::from_static(b"never acknowledged"), true)
        .await
        .unwrap();
    let second = pipeline
        .propose(2, Bytes::from_static(b"pipelined"), true)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while store.get(&stream().batch_key(2)).await.unwrap().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(pipeline);
    drop(first);
    drop(second);
    store.blocked_seq.lock().await.remove(&1);
    release.notify_waiters();

    let recovered = recover_stream(store.as_ref(), &FixedClock, &stream(), 0, 0, 2, "takeover")
        .await
        .unwrap();
    assert_eq!(recovered.epoch_start_seq, 1);
    assert!(recovered.batches_to_apply.is_empty());
    let stale_store = store.clone();
    let (stale, _) = CommitPipeline::start(
        stale_store,
        Arc::new(FixedClock),
        stream(),
        1,
        1,
        CommitPipelineConfig::default(),
    )
    .unwrap();
    let ticket = stale
        .propose(2, Bytes::from_static(b"stale write"), true)
        .await
        .unwrap();
    assert_eq!(
        ticket.committed.await.unwrap(),
        Err(ProposalFailure::Fenced)
    );
    let recovered_again = recover_stream(store.as_ref(), &FixedClock, &stream(), 0, 0, 3, "next")
        .await
        .unwrap();
    assert_eq!(recovered_again.void_sequences, vec![2]);
}

#[tokio::test]
async fn seeded_takeovers_recover_exactly_the_acknowledged_prefix() {
    let store = Arc::new(MemoryStore::default());
    let mut seed = 0x8f6c_2a19_u64;
    let mut acknowledged = Vec::<Bytes>::new();
    let mut next_gap = 1_u64;
    for epoch in 1..=12 {
        if epoch > 1 && seed & 1 == 1 {
            store
                .insert(
                    stream().batch_key(next_gap + 1),
                    batch(epoch - 1, next_gap + 1, b"stale unacknowledged"),
                )
                .await;
        }
        let recovered = recover_stream(
            store.as_ref(),
            &FixedClock,
            &stream(),
            0,
            0,
            epoch,
            "replacement",
        )
        .await
        .unwrap();
        let replayed: Vec<_> = recovered
            .batches_to_apply
            .iter()
            .flat_map(|batch| batch.records.iter().map(|record| record.bytes.clone()))
            .collect();
        assert_eq!(
            replayed, acknowledged,
            "seeded epoch {epoch} changed the acknowledged prefix"
        );
        let config = CommitPipelineConfig {
            max_batch_records: 1,
            ..Default::default()
        };
        let (pipeline, mut committed) = CommitPipeline::start(
            store.clone(),
            Arc::new(FixedClock),
            stream(),
            epoch,
            recovered.next_seq,
            config,
        )
        .unwrap();
        let count = 1 + (seed % 4) as usize;
        for record_index in 0..count {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let value = Bytes::from(format!("epoch-{epoch}-record-{record_index}-seed-{seed}"));
            if seed & 3 == 0 {
                store
                    .inject_unknown_after_write(
                        stream().batch_key(recovered.next_seq + record_index as u64),
                    )
                    .await;
            }
            let ticket = pipeline.propose(2, value.clone(), true).await.unwrap();
            let committed_seq = ticket.committed.await.unwrap().unwrap();
            assert_eq!(committed.recv().await.unwrap().seq, committed_seq);
            acknowledged.push(value);
        }
        drop(pipeline);
        next_gap = store
            .state
            .lock()
            .await
            .objects
            .keys()
            .filter_map(|key| {
                key.rsplit('/')
                    .next()
                    .and_then(|part| part.parse::<u64>().ok())
            })
            .max()
            .unwrap()
            + 1;
    }
    let final_recovery = recover_stream(store.as_ref(), &FixedClock, &stream(), 0, 0, 13, "final")
        .await
        .unwrap();
    let replayed: Vec<_> = final_recovery
        .batches_to_apply
        .iter()
        .flat_map(|batch| batch.records.iter().map(|record| record.bytes.clone()))
        .collect();
    assert_eq!(replayed, acknowledged);
}
