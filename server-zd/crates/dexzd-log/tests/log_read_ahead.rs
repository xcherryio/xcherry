// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use dexzd_clock::WallClock;
use dexzd_log::{
    BatchContent, BatchRecord, LogError, LogReadAheadConfig, OrderedBatchReader, StreamIdentity,
    StreamRecovery, StreamRecoveryInputs, StreamRecoveryStep, decode_batch, encode_batch,
};
use dexzd_object_store::{
    FaultAction, FaultInjector, InMemoryObjectStore, ObjectStore, ObjectStoreOperation,
    WriteCondition,
};
use tokio::time::Instant;

const GET_DELAY: Duration = Duration::from_millis(10);

struct FixedClock;

impl WallClock for FixedClock {
    fn now_unix_micros(&self) -> i64 {
        1_700_000_000_000_000
    }
}

/// Delays each batch GET by an amount chosen per seq, fails GETs of chosen seqs while armed, and
/// counts GETs.
struct BatchGetFaults {
    delay_of_seq: Box<dyn Fn(u64) -> Duration + Send + Sync>,
    failing_seqs: Vec<u64>,
    /// Set by [`Self::fail_until_next_batch_put`] and cleared by the next batch PUT.
    is_failing: AtomicBool,
    failed_gets: AtomicUsize,
    started_gets: AtomicUsize,
}

impl BatchGetFaults {
    fn delaying(delay_of_seq: impl Fn(u64) -> Duration + Send + Sync + 'static) -> Arc<Self> {
        Self::failing(delay_of_seq, Vec::new())
    }

    fn failing(
        delay_of_seq: impl Fn(u64) -> Duration + Send + Sync + 'static,
        failing_seqs: Vec<u64>,
    ) -> Arc<Self> {
        Arc::new(Self {
            delay_of_seq: Box::new(delay_of_seq),
            failing_seqs,
            is_failing: AtomicBool::new(false),
            failed_gets: AtomicUsize::new(0),
            started_gets: AtomicUsize::new(0),
        })
    }

    fn fail_until_next_batch_put(&self) {
        self.is_failing.store(true, Ordering::SeqCst);
    }

    fn started_gets(&self) -> usize {
        self.started_gets.load(Ordering::SeqCst)
    }

    fn failed_gets(&self) -> usize {
        self.failed_gets.load(Ordering::SeqCst)
    }
}

impl FaultInjector for BatchGetFaults {
    fn action(&self, operation: ObjectStoreOperation, key: &str) -> FaultAction {
        if operation == ObjectStoreOperation::Put && key.starts_with("log/") {
            self.is_failing.store(false, Ordering::SeqCst);
        }
        if operation != ObjectStoreOperation::Get || !key.starts_with("log/") {
            return FaultAction::Proceed;
        }
        self.started_gets.fetch_add(1, Ordering::SeqCst);
        let seq = key
            .rsplit('/')
            .next()
            .and_then(|seq| seq.parse::<u64>().ok())
            .expect("a batch key ends with its seq");
        if self.failing_seqs.contains(&seq) && self.is_failing.load(Ordering::SeqCst) {
            self.failed_gets.fetch_add(1, Ordering::SeqCst);
            return FaultAction::FailBeforeEffect;
        }
        match (self.delay_of_seq)(seq) {
            delay if delay.is_zero() => FaultAction::Proceed,
            delay => FaultAction::Delay(delay),
        }
    }
}

fn stream() -> StreamIdentity {
    StreamIdentity {
        cluster_id: [7; 16],
        node_id: "node-a".to_owned(),
        incarnation: 3,
    }
}

fn batch(epoch: u64, seq: u64, record_bytes: usize) -> Bytes {
    encode_batch(&BatchContent {
        stream: stream(),
        epoch,
        seq,
        sealed_at_unix_micros: 42,
        records: vec![BatchRecord {
            partition: 2,
            bytes: Bytes::from(vec![
                u8::try_from(seq % 251).expect("fits u8");
                record_bytes
            ]),
        }],
        epoch_start_writer: None,
    })
    .expect("test batch encodes")
}

/// Returns a batch whose one record holds `record_bytes` pseudo-random bytes that zstd cannot shrink.
fn incompressible_batch(epoch: u64, seq: u64, record_bytes: usize) -> Bytes {
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
        epoch,
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

fn epoch_start(epoch: u64, seq: u64) -> Bytes {
    encode_batch(&BatchContent {
        stream: stream(),
        epoch,
        seq,
        sealed_at_unix_micros: 42,
        records: Vec::new(),
        epoch_start_writer: Some("earlier-writer".to_owned()),
    })
    .expect("test EpochStart encodes")
}

async fn store_with(
    faults: Arc<BatchGetFaults>,
    objects: Vec<(u64, Bytes)>,
) -> InMemoryObjectStore {
    store_with_injector(faults, objects).await
}

async fn store_with_injector(
    injector: Arc<dyn FaultInjector>,
    objects: Vec<(u64, Bytes)>,
) -> InMemoryObjectStore {
    let store = InMemoryObjectStore::with_injector(injector);
    for (seq, body) in objects {
        store
            .put(&stream().batch_key(seq), body, WriteCondition::CreateOnly)
            .await
            .expect("test batch is stored");
    }
    store
}

/// Seqs 1-11 hold epoch 1 and seq 12 starts epoch 2, so later epoch-1 batches are void. Seq 31
/// starts epoch 3 the same way. Seq 41 is a pipelined hole with batches at 42 and 44 after it.
fn mixed_epoch_log() -> Vec<(u64, Bytes)> {
    let mut objects = Vec::new();
    for seq in 1..=11 {
        objects.push((seq, batch(1, seq, 64)));
    }
    objects.push((12, epoch_start(2, 12)));
    for seq in 13..=30 {
        let epoch = if seq % 3 == 0 { 1 } else { 2 };
        objects.push((seq, batch(epoch, seq, 64)));
    }
    objects.push((31, epoch_start(3, 31)));
    for seq in 32..=40 {
        let epoch = if seq % 2 == 0 { 2 } else { 3 };
        objects.push((seq, batch(epoch, seq, 64)));
    }
    objects.push((42, batch(3, 42, 64)));
    objects.push((44, batch(2, 44, 64)));
    objects
}

/// The applied seqs, voided seqs, EpochStart seq, and next seq of one recovery.
#[derive(Debug, PartialEq, Eq)]
struct RecoveryOutcome {
    applied: Vec<u64>,
    voided: Vec<u64>,
    epoch_start_seq: u64,
    next_seq: u64,
}

async fn recover(
    store: &InMemoryObjectStore,
    from_seq: u64,
    new_epoch: u64,
    read_ahead: LogReadAheadConfig,
) -> Result<RecoveryOutcome, LogError> {
    let stream = stream();
    let mut recovery = StreamRecovery::from_seq(
        StreamRecoveryInputs {
            store,
            clock: &FixedClock,
            stream: &stream,
            new_epoch,
            writer_process_id: "recovering-writer",
            read_ahead,
        },
        from_seq,
        0,
    )?;
    let mut applied = Vec::new();
    loop {
        match recovery.next_step().await? {
            StreamRecoveryStep::Apply(batch) => applied.push(batch.header.seq),
            StreamRecoveryStep::Recovered(result) => {
                return Ok(RecoveryOutcome {
                    applied,
                    voided: result.void_sequences,
                    epoch_start_seq: result.epoch_start_seq,
                    next_seq: result.next_seq,
                });
            }
        }
    }
}

fn read_ahead(max_read_ahead_batches: usize) -> LogReadAheadConfig {
    LogReadAheadConfig {
        max_read_ahead_batches,
        ..LogReadAheadConfig::default()
    }
}

/// Returns pseudo-random GET delays of 0 to 19 ms that differ per seq and per seed.
fn seeded_delay(seed: u64) -> impl Fn(u64) -> Duration + Send + Sync {
    move |seq| {
        let mixed = (seq ^ seed)
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        Duration::from_millis((mixed >> 33) % 20)
    }
}

#[tokio::test(start_paused = true)]
async fn recovery_order_and_voiding_are_exact_under_reordered_get_completion() {
    let mut expected_applied: Vec<u64> = (1..=12).collect();
    expected_applied.extend((13..=30).filter(|seq| seq % 3 != 0));
    expected_applied.push(31);
    expected_applied.extend((32..=40).filter(|seq| seq % 2 != 0));
    let mut expected_voided: Vec<u64> = (13..=30).filter(|seq| seq % 3 == 0).collect();
    expected_voided.extend((32..=40).filter(|seq| seq % 2 == 0));
    expected_voided.extend([42, 44]);
    let expected = RecoveryOutcome {
        applied: expected_applied,
        voided: expected_voided,
        epoch_start_seq: 41,
        next_seq: 45,
    };
    let baseline_faults = BatchGetFaults::delaying(|_| Duration::ZERO);
    let baseline_store = store_with(baseline_faults, mixed_epoch_log()).await;
    let baseline = recover(&baseline_store, 1, 4, read_ahead(1))
        .await
        .expect("sequential recovery succeeds");
    assert_eq!(baseline, expected);

    // `None` completes later seqs first; a seed shuffles completion pseudo-randomly.
    for (order, seed) in [
        ("reversed", None),
        ("seed 7", Some(7)),
        ("seed 1009", Some(1_009)),
    ] {
        for depth in [2, 32, 64] {
            let faults = match seed {
                None => BatchGetFaults::delaying(|seq| {
                    Duration::from_millis(200_u64.saturating_sub(seq))
                }),
                Some(seed) => BatchGetFaults::delaying(seeded_delay(seed)),
            };
            let store = store_with(faults, mixed_epoch_log()).await;
            let outcome = recover(&store, 1, 4, read_ahead(depth))
                .await
                .unwrap_or_else(|error| panic!("{order} at depth {depth}: {error}"));
            assert_eq!(outcome, expected, "{order} at depth {depth}");
            let filler = store
                .get(&stream().batch_key(43))
                .await
                .expect("read the filled hole")
                .expect("the hole at seq 43 is filled");
            let filler = decode_batch(&filler.bytes).expect("filler decodes");
            assert_eq!(filler.header.epoch, 4, "{order} at depth {depth}");
            assert!(filler.records.is_empty(), "{order} at depth {depth}");
        }
    }
}

#[tokio::test(start_paused = true)]
async fn recovery_fences_on_a_newer_epoch_under_reordered_get_completion() {
    for depth in [1, 2, 64] {
        let faults = BatchGetFaults::delaying(seeded_delay(3));
        let store = store_with(faults, mixed_epoch_log()).await;
        let result = recover(&store, 1, 3, read_ahead(depth)).await;
        assert!(
            matches!(result, Err(LogError::Fenced)),
            "depth {depth}: {result:?}"
        );
        assert!(
            store
                .get(&stream().batch_key(41))
                .await
                .expect("read the first missing seq")
                .is_none(),
            "depth {depth}: a fenced recovery writes no EpochStart"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn reads_past_the_first_missing_seq_never_reach_recovery() {
    let objects: Vec<(u64, Bytes)> = (1..=10).map(|seq| (seq, batch(1, seq, 64))).collect();
    for depth in [1, 64] {
        // Seq 6 is listed, then deleted before its delayed GET, so reads ahead pass the gap.
        let faults = BatchGetFaults::failing(
            |seq| {
                if seq == 6 {
                    Duration::from_millis(10)
                } else {
                    Duration::ZERO
                }
            },
            // Sequential recovery reads these seqs only after it writes the EpochStart at 6.
            vec![8, 9],
        );
        let store = Arc::new(store_with(faults.clone(), objects.clone()).await);
        faults.fail_until_next_batch_put();
        let deletion = tokio::spawn({
            let store = store.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(1)).await;
                store
                    .delete(&stream().batch_key(6))
                    .await
                    .expect("seq 6 is deleted");
            }
        });
        let outcome = recover(&store, 1, 2, read_ahead(depth))
            .await
            .unwrap_or_else(|error| panic!("depth {depth}: {error}"));
        deletion.await.expect("the deletion finishes");
        assert_eq!(
            outcome,
            RecoveryOutcome {
                applied: vec![1, 2, 3, 4, 5],
                voided: vec![7, 8, 9, 10],
                epoch_start_seq: 6,
                next_seq: 11,
            },
            "depth {depth}"
        );
        if depth > 1 {
            assert_eq!(faults.failed_gets(), 2, "depth {depth} read past the gap");
        } else {
            assert_eq!(faults.failed_gets(), 0, "depth {depth}");
        }
    }
}

#[tokio::test(start_paused = true)]
async fn replay_throughput_scales_with_read_ahead_depth_at_ten_ms_gets() {
    const BATCHES: u64 = 640;
    let mut rates = Vec::new();
    for depth in [1, 8, 32, 64] {
        let faults = BatchGetFaults::delaying(|_| GET_DELAY);
        let objects = (1..=BATCHES)
            .map(|seq| (seq, batch(1, seq, 1_024)))
            .collect();
        let store = store_with(faults, objects).await;
        let started = Instant::now();
        let outcome = recover(&store, 1, 2, read_ahead(depth))
            .await
            .expect("recovery succeeds");
        let elapsed = started.elapsed();
        assert_eq!(outcome.applied, (1..=BATCHES).collect::<Vec<_>>());
        assert_eq!(outcome.epoch_start_seq, BATCHES + 1);
        let batches_per_second = BATCHES as f64 / elapsed.as_secs_f64();
        // Each round trip of `depth` concurrent GETs takes 10 ms, plus the EpochStart write.
        let ideal = depth as f64 * 100.0;
        assert!(
            batches_per_second >= 0.8 * ideal,
            "depth {depth}: {batches_per_second:.0} batches/s in {elapsed:?}, ideal {ideal:.0}"
        );
        rates.push(batches_per_second);
    }
    assert!(
        rates.windows(2).all(|pair| pair[1] > pair[0]),
        "throughput grows with depth: {rates:?}"
    );
    assert!(
        rates[3] >= 50.0 * 78.0,
        "depth 64 reaches {:.0} batches/s",
        rates[3]
    );
}

#[tokio::test(start_paused = true)]
async fn closed_range_read_ahead_scales_and_never_reads_past_the_last_seq() {
    const BATCHES: u64 = 640;
    for (depth, minimum_rate) in [(1, 80.0), (64, 5_000.0)] {
        let faults = BatchGetFaults::delaying(|_| GET_DELAY);
        let objects = (1..=BATCHES + 10)
            .map(|seq| (seq, batch(1, seq, 1_024)))
            .collect();
        let store = store_with(faults.clone(), objects).await;
        let stream = stream();
        let mut reader =
            OrderedBatchReader::new(&store, &stream, 1, Some(BATCHES), read_ahead(depth));
        let started = Instant::now();
        let mut seqs = Vec::new();
        while let Some((seq, batch)) = reader.next_batch().await.expect("batch reads") {
            assert_eq!(batch.expect("every seq holds a batch").header.seq, seq);
            seqs.push(seq);
        }
        let batches_per_second = BATCHES as f64 / started.elapsed().as_secs_f64();
        assert_eq!(seqs, (1..=BATCHES).collect::<Vec<_>>());
        assert_eq!(faults.started_gets(), BATCHES as usize, "depth {depth}");
        assert!(
            batches_per_second >= minimum_rate,
            "depth {depth}: {batches_per_second:.0} batches/s"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn read_ahead_stays_within_its_window_and_byte_budget_on_a_large_log() {
    const BATCHES: u64 = 5_000;
    const RECORD_BYTES: usize = 4_096;
    let objects: Vec<(u64, Bytes)> = (1..=BATCHES)
        .map(|seq| (seq, incompressible_batch(1, seq, RECORD_BYTES)))
        .collect();
    let largest_object = objects
        .iter()
        .map(|(_, body)| body.len())
        .max()
        .expect("the log holds batches");
    let cases = [
        // Only the window binds: at most 32 batches are read but not yet returned.
        (32, 64 * 1024 * 1024, 32),
        // The byte budget binds: it holds the batch being applied and 8 read ahead.
        (64, 9 * largest_object, 8),
    ];
    for (depth, max_buffered_bytes, steady_bound) in cases {
        let faults = BatchGetFaults::delaying(seeded_delay(11));
        let store = store_with(faults.clone(), objects.clone()).await;
        let stream = stream();
        let mut reader = OrderedBatchReader::new(
            &store,
            &stream,
            1,
            Some(BATCHES),
            LogReadAheadConfig {
                max_read_ahead_batches: depth,
                max_buffered_bytes,
            },
        );
        let mut returned = 0_usize;
        let mut most_outstanding = 0_usize;
        while let Some((seq, batch)) = reader.next_batch().await.expect("batch reads") {
            assert!(batch.is_some(), "seq {seq} holds a batch");
            returned += 1;
            // A slow apply lets read-ahead fill everything its limits allow.
            reader
                .read_ahead_while(tokio::time::sleep(Duration::from_millis(5)))
                .await;
            let outstanding = faults.started_gets() - returned;
            assert!(outstanding <= depth, "{outstanding} batches read ahead");
            if returned > depth {
                assert!(
                    outstanding <= steady_bound,
                    "{outstanding} batches read ahead after {returned}"
                );
                most_outstanding = most_outstanding.max(outstanding);
            }
        }
        assert_eq!(returned, BATCHES as usize);
        assert_eq!(most_outstanding, steady_bound, "read-ahead fills its limit");
    }
}

/// Tracks the object bytes of batch GETs that started before the caller released their batch.
///
/// The caller releases a returned batch by asking for the next one, which the test reports
/// through [`Self::release_through`] just before each call.
struct BufferedBytesProbe {
    object_sizes: BTreeMap<u64, usize>,
    max_buffered_bytes: usize,
    delay_of_seq: Box<dyn Fn(u64) -> Duration + Send + Sync>,
    state: Mutex<BufferedBytes>,
}

#[derive(Default)]
struct BufferedBytes {
    released_through: u64,
    unreleased_seqs: BTreeSet<u64>,
    peak_bytes: usize,
    peak_bytes_of_several_batches: usize,
    /// GET starts that held more than the budget in more than one batch.
    overruns: Vec<String>,
}

impl BufferedBytesProbe {
    fn new(
        objects: &[(u64, Bytes)],
        max_buffered_bytes: usize,
        delay_of_seq: impl Fn(u64) -> Duration + Send + Sync + 'static,
    ) -> Arc<Self> {
        Arc::new(Self {
            object_sizes: objects
                .iter()
                .map(|(seq, body)| (*seq, body.len()))
                .collect(),
            max_buffered_bytes,
            delay_of_seq: Box::new(delay_of_seq),
            state: Mutex::new(BufferedBytes::default()),
        })
    }

    fn release_through(&self, seq: u64) {
        let mut state = self.state.lock().expect("probe lock");
        state.released_through = seq;
        state.unreleased_seqs.retain(|unreleased| *unreleased > seq);
    }

    /// Returns the peak bytes held, the peak held by more than one batch, and the overruns.
    fn report(&self) -> (usize, usize, Vec<String>) {
        let state = self.state.lock().expect("probe lock");
        (
            state.peak_bytes,
            state.peak_bytes_of_several_batches,
            state.overruns.clone(),
        )
    }
}

impl FaultInjector for BufferedBytesProbe {
    fn action(&self, operation: ObjectStoreOperation, key: &str) -> FaultAction {
        if operation != ObjectStoreOperation::Get || !key.starts_with("log/") {
            return FaultAction::Proceed;
        }
        let seq = key
            .rsplit('/')
            .next()
            .and_then(|seq| seq.parse::<u64>().ok())
            .expect("a batch key ends with its seq");
        let mut state = self.state.lock().expect("probe lock");
        if seq > state.released_through {
            state.unreleased_seqs.insert(seq);
        }
        let held: usize = state
            .unreleased_seqs
            .iter()
            .map(|seq| self.object_sizes.get(seq).copied().unwrap_or(0))
            .sum();
        state.peak_bytes = state.peak_bytes.max(held);
        if state.unreleased_seqs.len() > 1 {
            state.peak_bytes_of_several_batches = state.peak_bytes_of_several_batches.max(held);
            if held > self.max_buffered_bytes {
                let overrun = format!(
                    "GET of seq {seq} holds {held} bytes in seqs {:?}",
                    state.unreleased_seqs
                );
                state.overruns.push(overrun);
            }
        }
        match (self.delay_of_seq)(seq) {
            delay if delay.is_zero() => FaultAction::Proceed,
            delay => FaultAction::Delay(delay),
        }
    }
}

const UNEVEN_BUDGET: usize = 512 * 1024;
const UNEVEN_LAST_SEQ: u64 = 160;
const OVERSIZED_SEQ: u64 = 40;

/// Batches of 1 KiB, 32 KiB, and 128 KiB in pseudo-random order, and one above the budget.
fn uneven_log() -> Vec<(u64, Bytes)> {
    (1..=UNEVEN_LAST_SEQ)
        .map(|seq| {
            let record_bytes = if seq == OVERSIZED_SEQ {
                3 * UNEVEN_BUDGET / 2
            } else {
                [1_024, 32 * 1024, 128 * 1024][(seeded_delay(17)(seq).as_millis() % 3) as usize]
            };
            (seq, incompressible_batch(1, seq, record_bytes))
        })
        .collect()
}

/// Delays the first GET by a second, so every later read finishes before it.
fn slow_first_get(seq: u64) -> Duration {
    if seq == 1 {
        Duration::from_secs(1)
    } else {
        seeded_delay(5)(seq)
    }
}

fn assert_within_budget(probe: &BufferedBytesProbe, path: &str) {
    let (peak_bytes, peak_bytes_of_several_batches, overruns) = probe.report();
    assert!(overruns.is_empty(), "{path}: {overruns:#?}");
    assert!(
        peak_bytes >= 3 * UNEVEN_BUDGET / 2,
        "{path}: the oversized batch was read, peak {peak_bytes}"
    );
    assert!(
        peak_bytes_of_several_batches >= UNEVEN_BUDGET / 2,
        "{path}: read-ahead used the budget, peak {peak_bytes_of_several_batches}"
    );
}

#[tokio::test(start_paused = true)]
async fn uneven_batches_behind_a_slow_first_get_stay_within_the_byte_budget() {
    let config = LogReadAheadConfig {
        max_read_ahead_batches: 64,
        max_buffered_bytes: UNEVEN_BUDGET,
    };
    let objects = uneven_log();
    let stream = stream();

    let probe = BufferedBytesProbe::new(&objects, UNEVEN_BUDGET, slow_first_get);
    let store = store_with_injector(probe.clone(), objects.clone()).await;
    let mut reader = OrderedBatchReader::new(&store, &stream, 1, Some(UNEVEN_LAST_SEQ), config);
    let mut returned = Vec::new();
    loop {
        probe.release_through(returned.last().copied().unwrap_or(0));
        let Some((seq, batch)) = reader.next_batch().await.expect("batch reads") else {
            break;
        };
        assert_eq!(batch.expect("every seq holds a batch").header.seq, seq);
        returned.push(seq);
        reader
            .read_ahead_while(tokio::time::sleep(Duration::from_millis(2)))
            .await;
    }
    assert_eq!(returned, (1..=UNEVEN_LAST_SEQ).collect::<Vec<_>>());
    assert_within_budget(&probe, "closed stream");

    let probe = BufferedBytesProbe::new(&objects, UNEVEN_BUDGET, slow_first_get);
    let store = store_with_injector(probe.clone(), objects).await;
    let mut recovery = StreamRecovery::from_seq(
        StreamRecoveryInputs {
            store: &store,
            clock: &FixedClock,
            stream: &stream,
            new_epoch: 2,
            writer_process_id: "recovering-writer",
            read_ahead: config,
        },
        1,
        0,
    )
    .expect("recovery starts");
    let mut applied = Vec::new();
    let recovered = loop {
        probe.release_through(applied.last().copied().unwrap_or(0));
        match recovery.next_step().await.expect("recovery step succeeds") {
            StreamRecoveryStep::Apply(batch) => {
                applied.push(batch.header.seq);
                recovery
                    .read_ahead_while(tokio::time::sleep(Duration::from_millis(2)))
                    .await;
            }
            StreamRecoveryStep::Recovered(recovered) => break recovered,
        }
    };
    assert_eq!(applied, (1..=UNEVEN_LAST_SEQ).collect::<Vec<_>>());
    assert_eq!(recovered.epoch_start_seq, UNEVEN_LAST_SEQ + 1);
    assert_within_budget(&probe, "open stream");
}

#[tokio::test(start_paused = true)]
async fn a_batch_rewritten_after_its_listing_fails_at_its_seq() {
    let objects: Vec<(u64, Bytes)> = (1..=5)
        .map(|seq| (seq, incompressible_batch(1, seq, 1_024)))
        .collect();
    let faults = BatchGetFaults::delaying(|seq| {
        if seq == 3 {
            Duration::from_millis(50)
        } else {
            Duration::ZERO
        }
    });
    let store = store_with(faults, objects).await;
    let stream = stream();
    let mut reader = OrderedBatchReader::new(&store, &stream, 1, Some(5), read_ahead(64));
    let (seq, _) = reader
        .next_batch()
        .await
        .expect("seq 1 reads")
        .expect("seq 1 is in range");
    assert_eq!(seq, 1);
    // The GET of seq 3 started at its listed size and reads the larger object after its delay.
    reader
        .read_ahead_while(async {
            let key = stream.batch_key(3);
            store.delete(&key).await.expect("seq 3 is deleted");
            store
                .put(
                    &key,
                    incompressible_batch(1, 3, 64 * 1024),
                    WriteCondition::CreateOnly,
                )
                .await
                .expect("seq 3 is rewritten");
        })
        .await;
    let (seq, _) = reader
        .next_batch()
        .await
        .expect("seq 2 reads")
        .expect("seq 2 is in range");
    assert_eq!(seq, 2);
    let result = reader.next_batch().await;
    assert!(
        matches!(&result, Err(LogError::Corrupt(detail)) if detail.contains("seq 3")),
        "{result:?}"
    );
}
