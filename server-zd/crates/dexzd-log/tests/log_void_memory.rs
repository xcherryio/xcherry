// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use bytes::Bytes;
use dexzd_clock::WallClock;
use dexzd_log::{
    BatchContent, BatchRecord, LogReadAheadConfig, StreamIdentity, StreamRecovery,
    StreamRecoveryInputs, StreamRecoveryStep, encode_batch,
};
use dexzd_object_store::{InMemoryObjectStore, ObjectStore, WriteCondition};

#[global_allocator]
static ALLOCATOR: dhat::Alloc = dhat::Alloc;

/// Older batches after the EpochStart at seq 2, voided while recovery reads them.
const VOIDED_WHILE_READING: u64 = 48_000;
/// Older pipelined batches after the first missing seq, voided once the EpochStart is written.
const VOIDED_AFTER_EPOCH_START: u64 = 8_000;
/// Recovery here peaks near 170 KiB; one `u64` per voided seq alone exceeds this.
const MAX_RECOVERY_HEAP_BYTES: usize = 384 * 1024;

struct FixedClock;

impl WallClock for FixedClock {
    fn now_unix_micros(&self) -> i64 {
        1_700_000_000_000_000
    }
}

fn stream() -> StreamIdentity {
    StreamIdentity {
        cluster_id: [7; 16],
        node_id: "node-a".to_owned(),
        incarnation: 3,
    }
}

fn batch(epoch: u64, seq: u64, epoch_start_writer: Option<&str>) -> Bytes {
    let records = if epoch_start_writer.is_some() {
        Vec::new()
    } else {
        vec![BatchRecord {
            partition: 2,
            bytes: Bytes::from_static(b"record"),
        }]
    };
    encode_batch(&BatchContent {
        stream: stream(),
        epoch,
        seq,
        sealed_at_unix_micros: 42,
        records,
        epoch_start_writer: epoch_start_writer.map(str::to_owned),
    })
    .expect("test batch encodes")
}

/// Seq 1 holds epoch 1 and seq 2 starts epoch 2, so the epoch-1 batches after it are void. One
/// epoch-2 batch follows them, then a missing seq and a pipelined tail of epoch-1 batches.
async fn void_heavy_store() -> (InMemoryObjectStore, u64) {
    let store = InMemoryObjectStore::new();
    let last_read_seq = 3 + VOIDED_WHILE_READING;
    let mut objects = vec![
        (1, batch(1, 1, None)),
        (2, batch(2, 2, Some("earlier-writer"))),
        (last_read_seq, batch(2, last_read_seq, None)),
    ];
    objects.extend((3..last_read_seq).map(|seq| (seq, batch(1, seq, None))));
    let first_tail_seq = last_read_seq + 2;
    objects.extend(
        (first_tail_seq..first_tail_seq + VOIDED_AFTER_EPOCH_START)
            .map(|seq| (seq, batch(1, seq, None))),
    );
    let stream = stream();
    for (seq, body) in objects {
        store
            .put(&stream.batch_key(seq), body, WriteCondition::CreateOnly)
            .await
            .expect("test batch is stored");
    }
    // The store counts calls per key; reading each key first keeps new counters out of the peak.
    for seq in 1..first_tail_seq + VOIDED_AFTER_EPOCH_START {
        store
            .get(&stream.batch_key(seq))
            .await
            .expect("test batch reads");
    }
    (store, last_read_seq)
}

#[tokio::test]
async fn recovery_heap_stays_bounded_however_many_batches_are_voided() {
    let (store, last_read_seq) = void_heavy_store().await;
    let stream = stream();
    let profiler = dhat::Profiler::builder().testing().build();
    let mut recovery = StreamRecovery::from_seq(
        StreamRecoveryInputs {
            store: &store,
            clock: &FixedClock,
            stream: &stream,
            new_epoch: 3,
            writer_process_id: "recovering-writer",
            read_ahead: LogReadAheadConfig::default(),
        },
        1,
        0,
    )
    .expect("recovery starts");
    let mut applied = Vec::new();
    let recovered = loop {
        match recovery.next_step().await.expect("recovery step succeeds") {
            StreamRecoveryStep::Apply(batch) => applied.push(batch.header.seq),
            StreamRecoveryStep::Recovered(recovered) => break recovered,
        }
    };
    let heap = dhat::HeapStats::get();
    drop(recovery);
    drop(profiler);
    assert_eq!(applied, vec![1, 2, last_read_seq]);
    assert_eq!(
        recovered.voided_batches,
        VOIDED_WHILE_READING + VOIDED_AFTER_EPOCH_START
    );
    assert_eq!(recovered.epoch_start_seq, last_read_seq + 1);
    assert_eq!(
        recovered.next_seq,
        last_read_seq + 2 + VOIDED_AFTER_EPOCH_START
    );
    assert!(
        heap.max_bytes < MAX_RECOVERY_HEAP_BYTES,
        "recovery peaked at {} heap bytes",
        heap.max_bytes
    );
}
