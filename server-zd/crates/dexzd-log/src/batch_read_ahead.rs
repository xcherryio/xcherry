// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;
use std::future::{Future, pending};
use std::pin::pin;

use dexzd_object_store::ObjectStore;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};

use crate::batch_codec::{DecodedBatch, LogError, StreamIdentity, decode_batch};
use crate::stream_recovery::validate_batch_identity;

/// How far log replay reads batches ahead of the batch it applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogReadAheadConfig {
    /// Defaults to 64; most batches read but not yet returned, counting reads in flight.
    ///
    /// Replay throughput is about this many batches per object-store GET round trip, so 64 at a
    /// 10 ms GET latency allows about 6,400 batches per second. Must be positive.
    pub max_read_ahead_batches: usize,
    /// Defaults to 64 MiB; record bytes that batches read ahead may hold.
    ///
    /// A read starts only while the fetched batches waiting to be returned, plus the largest batch
    /// seen so far for each read in flight, fit in this budget. Read-ahead memory therefore stays
    /// near this budget while batch sizes are steady, and below this budget plus
    /// `max_read_ahead_batches` times the largest batch in any case, whatever the length of the
    /// log. Must be positive.
    pub max_buffered_bytes: usize,
}

impl Default for LogReadAheadConfig {
    fn default() -> Self {
        Self {
            max_read_ahead_batches: 64,
            max_buffered_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Reads consecutive batches of one stream with concurrent GETs and returns them in seq order.
///
/// Results, including read and decode errors, are returned strictly in seq order, so an error at
/// a seq after one that the caller stops at is never seen.
pub struct OrderedBatchReader<'a> {
    store: &'a dyn ObjectStore,
    stream: &'a StreamIdentity,
    config: LogReadAheadConfig,
    /// Inclusive bound of the seqs read.
    last_seq: u64,
    /// Next seq to return, or `None` after `last_seq` was returned.
    next_seq_to_return: Option<u64>,
    /// Next seq to read, or `None` after `last_seq` was read.
    next_seq_to_read: Option<u64>,
    in_flight: FuturesUnordered<BoxFuture<'a, (u64, BatchRead)>>,
    fetched: BTreeMap<u64, BatchRead>,
    fetched_bytes: usize,
    /// Record bytes of the largest batch fetched so far, charged to each read in flight.
    largest_batch_bytes: usize,
}

/// One GET of a batch: the decoded batch and its record bytes, or `None` when the seq is empty.
type BatchRead = Result<Option<(DecodedBatch, usize)>, LogError>;

impl<'a> OrderedBatchReader<'a> {
    /// Creates a reader of `stream` from `first_seq` through `last_seq`, or without an upper
    /// bound when `last_seq` is `None`.
    ///
    /// No read starts before the first call to [`Self::next_batch`].
    pub fn new(
        store: &'a dyn ObjectStore,
        stream: &'a StreamIdentity,
        first_seq: u64,
        last_seq: Option<u64>,
        config: LogReadAheadConfig,
    ) -> Self {
        let last_seq = last_seq.unwrap_or(u64::MAX);
        let first_seq = (first_seq <= last_seq).then_some(first_seq);
        Self {
            store,
            stream,
            config,
            last_seq,
            next_seq_to_return: first_seq,
            next_seq_to_read: first_seq,
            in_flight: FuturesUnordered::new(),
            fetched: BTreeMap::new(),
            fetched_bytes: 0,
            largest_batch_bytes: 0,
        }
    }

    /// Returns the next seq with its batch, or with `None` when the store has no batch there.
    ///
    /// Returns `Ok(None)` once every seq through the last one was returned, and the first read,
    /// decode, or stream identity error in seq order.
    pub async fn next_batch(&mut self) -> Result<Option<(u64, Option<DecodedBatch>)>, LogError> {
        let Some(seq) = self.next_seq_to_return else {
            return Ok(None);
        };
        loop {
            self.start_reads();
            if let Some(read) = self.fetched.remove(&seq) {
                self.next_seq_to_return = seq.checked_add(1).filter(|next| *next <= self.last_seq);
                let batch = read?.map(|(batch, bytes)| {
                    self.fetched_bytes -= bytes;
                    batch
                });
                self.start_reads();
                return Ok(Some((seq, batch)));
            }
            let (read_seq, read) = self
                .in_flight
                .next()
                .await
                .expect("the read of the next seq to return is in flight until it is fetched");
            self.keep_fetched(read_seq, read);
        }
    }

    /// Runs `work` to completion while reads in flight keep finishing and new reads start.
    ///
    /// Returns the output of `work`.
    pub async fn read_ahead_while<F: Future>(&mut self, work: F) -> F::Output {
        let mut work = pin!(work);
        loop {
            tokio::select! {
                biased;
                output = &mut work => return output,
                () = self.finish_one_read() => {}
            }
        }
    }

    /// Waits for one read in flight, keeps its result, and starts more reads within the budget.
    ///
    /// Never finishes while no read is in flight. Cancel safe: it awaits only the next read.
    async fn finish_one_read(&mut self) {
        let Some((seq, read)) = self.in_flight.next().await else {
            return pending().await;
        };
        self.keep_fetched(seq, read);
        self.start_reads();
    }

    fn keep_fetched(&mut self, seq: u64, read: BatchRead) {
        if let Ok(Some((_, bytes))) = &read {
            self.fetched_bytes += bytes;
            self.largest_batch_bytes = self.largest_batch_bytes.max(*bytes);
        }
        self.fetched.insert(seq, read);
    }

    /// Starts reads in seq order while the read-ahead window and byte budget allow.
    fn start_reads(&mut self) {
        while let Some(seq) = self.next_seq_to_read {
            let outstanding = self.in_flight.len() + self.fetched.len();
            let projected_bytes = self.fetched_bytes.saturating_add(
                (self.in_flight.len() + 1).saturating_mul(self.largest_batch_bytes),
            );
            // One outstanding read is always allowed, so a batch above the budget still loads.
            if outstanding > 0
                && (outstanding >= self.config.max_read_ahead_batches
                    || projected_bytes > self.config.max_buffered_bytes)
            {
                return;
            }
            self.in_flight
                .push(Box::pin(read_batch(self.store, self.stream, seq)));
            self.next_seq_to_read = seq.checked_add(1).filter(|next| *next <= self.last_seq);
        }
    }
}

async fn read_batch(
    store: &dyn ObjectStore,
    stream: &StreamIdentity,
    seq: u64,
) -> (u64, BatchRead) {
    let read = async {
        let Some(object) = store.get(&stream.batch_key(seq)).await? else {
            return Ok(None);
        };
        let batch = decode_batch(&object.bytes)?;
        validate_batch_identity(&batch, stream, seq)?;
        let bytes = batch.records.iter().map(|record| record.bytes.len()).sum();
        Ok(Some((batch, bytes)))
    };
    (seq, read.await)
}
