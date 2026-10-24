// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use dexzd_object_store::{
    FaultAction, FaultInjector, InMemoryObjectStore, ObjectStore, ObjectStoreOperation,
    WriteCondition,
};

use super::{LogReadAheadConfig, OrderedBatchReader};
use crate::batch_codec::{
    BatchContent, BatchRecord, LogError, StreamIdentity, decode_batch, encode_batch,
};

const SMALL_RECORD_BYTES: usize = 1_024;
const FULL_BUDGET_RECORD_BYTES: usize = 64 * 1024;
const SLOW_GET: Duration = Duration::from_millis(50);

/// Chooses the fault of each batch GET by its seq.
struct BatchGetFaults<F>(F);

impl<F: Fn(u64) -> FaultAction + Send + Sync> FaultInjector for BatchGetFaults<F> {
    fn action(&self, operation: ObjectStoreOperation, key: &str) -> FaultAction {
        if operation != ObjectStoreOperation::Get || !key.starts_with("log/") {
            return FaultAction::Proceed;
        }
        let seq = key
            .rsplit('/')
            .next()
            .and_then(|seq| seq.parse::<u64>().ok())
            .expect("a batch key ends with its seq");
        (self.0)(seq)
    }
}

fn stream() -> StreamIdentity {
    StreamIdentity {
        cluster_id: [7; 16],
        node_id: "node-a".to_owned(),
        incarnation: 3,
    }
}

/// Returns a batch whose one record holds `record_bytes` pseudo-random bytes, so its size follows
/// `record_bytes`.
fn batch(seq: u64, record_bytes: usize) -> Bytes {
    let mut state = seq.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let record: Vec<u8> = (0..record_bytes)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect();
    encode_batch(&BatchContent {
        stream: stream(),
        epoch: 1,
        seq,
        sealed_at_unix_micros: 42,
        records: vec![BatchRecord {
            partition: 2,
            bytes: Bytes::from(record),
        }],
        epoch_start_writer: None,
    })
    .expect("test batch encodes")
}

async fn store_with(
    fault_of_seq: impl Fn(u64) -> FaultAction + Send + Sync + 'static,
    objects: Vec<(u64, Bytes)>,
) -> InMemoryObjectStore {
    let store = InMemoryObjectStore::with_injector(Arc::new(BatchGetFaults(fault_of_seq)));
    for (seq, body) in objects {
        store
            .put(&stream().batch_key(seq), body, WriteCondition::CreateOnly)
            .await
            .expect("test batch is stored");
    }
    store
}

/// Stores `first_batch` at seq 1 and, at seq 2, a batch whose size is the returned budget.
async fn store_failing_seq_before_full_budget_batch(
    fault_of_seq: impl Fn(u64) -> FaultAction + Send + Sync + 'static,
    first_batch: Bytes,
) -> (InMemoryObjectStore, usize) {
    let full_budget_batch = batch(2, FULL_BUDGET_RECORD_BYTES);
    let budget = full_budget_batch.len();
    let store = store_with(fault_of_seq, vec![(1, first_batch), (2, full_budget_batch)]).await;
    (store, budget)
}

/// Reads seq 1, which fails while `during_first_read` runs, then seq 2, which needs the whole
/// budget.
///
/// Returns the error of seq 1 after checking that it left no bytes reserved.
async fn read_failure_then_full_budget_batch(
    store: &InMemoryObjectStore,
    budget: usize,
    during_first_read: impl Future<Output = ()>,
) -> LogError {
    let stream = stream();
    let config = LogReadAheadConfig {
        max_read_ahead_batches: 64,
        max_buffered_bytes: budget,
    };
    let mut reader = OrderedBatchReader::new(store, &stream, 1, Some(2), config);
    let (first_read, ()) = tokio::join!(reader.next_batch(), during_first_read);
    let error = first_read.expect_err("seq 1 fails");
    // Seq 2 fits only once seq 1 holds nothing, so it reserving the budget proves the release.
    assert_eq!(reader.returned_reservation, 0);
    assert_eq!(reader.reservations, [budget]);
    assert_eq!(reader.reserved_bytes, budget);

    let (seq, batch) = reader
        .next_batch()
        .await
        .expect("seq 2 reads")
        .expect("seq 2 is in range");
    assert_eq!((seq, batch.map(|batch| batch.header.seq)), (2, Some(2)));
    assert!(
        reader
            .next_batch()
            .await
            .expect("the reader ends")
            .is_none()
    );
    assert_eq!(reader.reserved_bytes, 0);
    error
}

#[tokio::test(start_paused = true)]
async fn a_failed_get_releases_its_reservation_when_returned() {
    let (store, budget) = store_failing_seq_before_full_budget_batch(
        |seq| match seq {
            1 => FaultAction::FailBeforeEffect,
            _ => FaultAction::Proceed,
        },
        batch(1, SMALL_RECORD_BYTES),
    )
    .await;

    let error = read_failure_then_full_budget_batch(&store, budget, async {}).await;
    assert!(matches!(error, LogError::Store(_)), "{error:?}");
}

#[tokio::test(start_paused = true)]
async fn a_batch_that_differs_from_its_listed_size_releases_its_reservation_when_returned() {
    let (store, budget) = store_failing_seq_before_full_budget_batch(
        |seq| match seq {
            1 => FaultAction::Delay(SLOW_GET),
            _ => FaultAction::Proceed,
        },
        batch(1, SMALL_RECORD_BYTES),
    )
    .await;

    let rewrite_seq_1 = async {
        tokio::time::sleep(SLOW_GET / 5).await;
        let key = stream().batch_key(1);
        store.delete(&key).await.expect("seq 1 is deleted");
        store
            .put(
                &key,
                batch(1, 2 * SMALL_RECORD_BYTES),
                WriteCondition::CreateOnly,
            )
            .await
            .expect("seq 1 is rewritten");
    };
    let error = read_failure_then_full_budget_batch(&store, budget, rewrite_seq_1).await;
    assert!(
        matches!(&error, LogError::Corrupt(detail) if detail.contains("listing showed")),
        "{error:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn an_undecodable_batch_releases_its_reservation_when_returned() {
    let undecodable = Bytes::from(vec![0xAB; SMALL_RECORD_BYTES]);
    let decode_error = decode_batch(&undecodable).expect_err("the bytes are not a batch");
    let (store, budget) =
        store_failing_seq_before_full_budget_batch(|_| FaultAction::Proceed, undecodable).await;

    let error = read_failure_then_full_budget_batch(&store, budget, async {}).await;
    assert_eq!(error.to_string(), decode_error.to_string());
}

#[tokio::test(start_paused = true)]
async fn a_batch_of_another_seq_releases_its_reservation_when_returned() {
    let (store, budget) = store_failing_seq_before_full_budget_batch(
        |_| FaultAction::Proceed,
        batch(5, SMALL_RECORD_BYTES),
    )
    .await;

    let error = read_failure_then_full_budget_batch(&store, budget, async {}).await;
    assert!(
        matches!(&error, LogError::Corrupt(detail) if detail.contains("different stream identity")),
        "{error:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_cancelled_read_leaves_only_unreturned_reads_reserved() {
    let objects = (1..=4)
        .map(|seq| (seq, batch(seq, SMALL_RECORD_BYTES)))
        .collect();
    let store = store_with(
        |seq| match seq {
            2 => FaultAction::Delay(SLOW_GET),
            4 => FaultAction::FailBeforeEffect,
            _ => FaultAction::Proceed,
        },
        objects,
    )
    .await;
    let stream = stream();
    let mut reader =
        OrderedBatchReader::new(&store, &stream, 1, Some(4), LogReadAheadConfig::default());
    let (seq, _) = reader
        .next_batch()
        .await
        .expect("seq 1 reads")
        .expect("seq 1 is in range");
    assert_eq!(seq, 1);
    assert!(reader.returned_reservation > 0);

    tokio::time::timeout(SLOW_GET / 5, reader.next_batch())
        .await
        .expect_err("seq 2 is still being read");
    assert_eq!(reader.returned_reservation, 0);
    assert_eq!(reader.reservations.len(), 3);
    assert_eq!(
        reader.reserved_bytes,
        reader.reservations.iter().sum::<usize>()
    );

    for expected_seq in [2, 3] {
        let (seq, _) = reader
            .next_batch()
            .await
            .expect("the batch reads")
            .expect("the seq is in range");
        assert_eq!(seq, expected_seq);
    }
    let error = reader.next_batch().await.expect_err("seq 4 fails");
    assert!(matches!(error, LogError::Store(_)), "{error:?}");
    assert_eq!(reader.reserved_bytes, 0);
    assert!(reader.reservations.is_empty());
    assert!(
        reader
            .next_batch()
            .await
            .expect("the reader ends")
            .is_none()
    );
}
