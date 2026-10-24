// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::time::Duration;

use dexzd_clock::WallClock;
use dexzd_keyspace::validate_node_identifier;
use dexzd_object_store::{ObjectStore, ObjectStoreErrorKind, WriteCondition, WriteOutcome};
use tokio::time::{Instant, sleep, timeout_at};
use xxhash_rust::xxh3::xxh3_128;

use crate::batch_codec::{
    BatchContent, DecodedBatch, LogError, StreamIdentity, decode_batch, encode_batch,
};

const RECOVERY_WRITE_DEADLINE: Duration = Duration::from_secs(10);
const LIST_PAGE_KEYS: u32 = 1_000;

#[derive(Debug)]
pub struct RecoveredStream {
    pub batches_to_apply: Vec<DecodedBatch>,
    pub void_sequences: Vec<u64>,
    pub epoch_start_seq: u64,
    pub next_seq: u64,
}

pub async fn recover_stream(
    store: &dyn ObjectStore,
    clock: &dyn WallClock,
    stream: &StreamIdentity,
    last_applied_seq: u64,
    last_applied_epoch: u64,
    new_epoch: u64,
    writer_process_id: &str,
) -> Result<RecoveredStream, LogError> {
    if (last_applied_seq == 0) != (last_applied_epoch == 0) {
        return Err(LogError::Invalid(
            "invalid applied position or epoch".into(),
        ));
    }
    let from_seq = last_applied_seq
        .checked_add(1)
        .ok_or_else(|| LogError::Invalid("stream sequence exhausted".into()))?;
    recover_stream_from(
        store,
        clock,
        stream,
        from_seq,
        last_applied_epoch,
        new_epoch,
        writer_process_id,
    )
    .await
}

pub async fn recover_stream_from(
    store: &dyn ObjectStore,
    clock: &dyn WallClock,
    stream: &StreamIdentity,
    from_seq: u64,
    effective_epoch_at_start: u64,
    new_epoch: u64,
    writer_process_id: &str,
) -> Result<RecoveredStream, LogError> {
    if from_seq == 0 || new_epoch == 0 || writer_process_id.is_empty() {
        return Err(LogError::Invalid(
            "invalid stream position, epoch, or writer process ID".into(),
        ));
    }
    validate_node_identifier(&stream.node_id)
        .map_err(|error| LogError::Invalid(error.to_string()))?;
    if effective_epoch_at_start >= new_epoch {
        return Err(LogError::Fenced);
    }
    let mut result = RecoveredStream {
        batches_to_apply: Vec::new(),
        void_sequences: Vec::new(),
        epoch_start_seq: 0,
        next_seq: 0,
    };
    let mut effective_epoch = effective_epoch_at_start;
    let mut seq = from_seq;
    loop {
        let key = stream.batch_key(seq);
        let Some(object) = store.get(&key).await? else {
            break;
        };
        let batch = decode_batch(&object.bytes)?;
        validate_batch_identity(&batch, stream, seq)?;
        accept_batch(&mut result, batch, seq, &mut effective_epoch);
        seq = seq
            .checked_add(1)
            .ok_or_else(|| LogError::Invalid("stream sequence exhausted".into()))?;
    }
    if effective_epoch >= new_epoch {
        return Err(LogError::Fenced);
    }
    if seq == from_seq {
        ensure_batches_after_start_were_not_deleted(store, stream, seq).await?;
    }
    let writer = EpochStartWriter {
        store,
        clock,
        stream,
        new_epoch,
        writer_process_id,
    };
    let deadline = Instant::now() + RECOVERY_WRITE_DEADLINE;
    loop {
        match writer.create_epoch_start(seq, deadline).await? {
            RecoveryWrite::Written => {
                result.epoch_start_seq = seq;
                return writer.finish_epoch_start(result).await;
            }
            RecoveryWrite::Found(batch) => {
                if batch.header.epoch >= new_epoch {
                    return Err(LogError::Fenced);
                }
                accept_batch(&mut result, batch, seq, &mut effective_epoch);
                seq = next_seq_after(seq)?;
            }
        }
    }
}

/// What a create-only recovery write found at its seq.
enum RecoveryWrite {
    /// The seq holds the batch this recovery wrote.
    Written,
    /// The seq already held another batch.
    Found(DecodedBatch),
}

/// Writes the EpochStart batches of one recovery.
struct EpochStartWriter<'a> {
    store: &'a dyn ObjectStore,
    clock: &'a dyn WallClock,
    stream: &'a StreamIdentity,
    new_epoch: u64,
    writer_process_id: &'a str,
}

impl EpochStartWriter<'_> {
    /// Voids older batches after the EpochStart and fills every hole below the last listed batch.
    ///
    /// Returns `result` with the voided seqs and `next_seq` after the last batch, or
    /// [`LogError::Fenced`] when that range holds a batch of this or a newer epoch.
    async fn finish_epoch_start(
        &self,
        mut result: RecoveredStream,
    ) -> Result<RecoveredStream, LogError> {
        let first_seq = next_seq_after(result.epoch_start_seq)?;
        let Some(last_seq) = self.last_listed_seq_after(result.epoch_start_seq).await? else {
            result.next_seq = first_seq;
            return Ok(result);
        };
        let deadline = Instant::now() + RECOVERY_WRITE_DEADLINE;
        for seq in first_seq..=last_seq {
            let found = match self.store.get(&self.stream.batch_key(seq)).await? {
                Some(object) => {
                    let batch = decode_batch(&object.bytes)?;
                    validate_batch_identity(&batch, self.stream, seq)?;
                    Some(batch)
                }
                // No empty ordinary batch decodes, so an EpochStart of this epoch fills the hole.
                None => match self.create_epoch_start(seq, deadline).await? {
                    RecoveryWrite::Written => None,
                    RecoveryWrite::Found(batch) => Some(batch),
                },
            };
            if let Some(batch) = found {
                if batch.header.epoch >= self.new_epoch {
                    return Err(LogError::Fenced);
                }
                result.void_sequences.push(seq);
            }
        }
        result.next_seq = next_seq_after(last_seq)?;
        Ok(result)
    }

    /// Creates an EpochStart at `seq` unless a batch is there; retries reuse its bytes until `deadline`.
    ///
    /// Returns [`RecoveryWrite::Written`] when `seq` holds these bytes, including after an unknown
    /// write outcome, and [`RecoveryWrite::Found`] with the batch that another writer stored there.
    async fn create_epoch_start(
        &self,
        seq: u64,
        deadline: Instant,
    ) -> Result<RecoveryWrite, LogError> {
        let key = self.stream.batch_key(seq);
        let body = encode_batch(&BatchContent {
            stream: self.stream.clone(),
            epoch: self.new_epoch,
            seq,
            sealed_at_unix_micros: self.clock.now_unix_micros(),
            records: Vec::new(),
            epoch_start_writer: Some(self.writer_process_id.to_owned()),
        })?;
        let ours = xxh3_128(&body[..body.len() - 16]);
        let mut backoff = Duration::from_millis(10);
        loop {
            if Instant::now() >= deadline {
                return Err(LogError::Deadline);
            }
            let should_read_back = match timeout_at(
                deadline,
                self.store
                    .put(&key, body.clone(), WriteCondition::CreateOnly),
            )
            .await
            {
                Err(_) => return Err(LogError::Deadline),
                Ok(write_result) => match write_result {
                    Ok(WriteOutcome::Written { .. }) => return Ok(RecoveryWrite::Written),
                    Ok(WriteOutcome::PreconditionFailed) => true,
                    Err(error) if error.kind == ObjectStoreErrorKind::OutcomeUnknown => true,
                    Err(error) if error.kind == ObjectStoreErrorKind::Transient => false,
                    Err(error) => return Err(error.into()),
                },
            };
            if should_read_back {
                match timeout_at(deadline, self.store.get(&key)).await {
                    Err(_) => return Err(LogError::Deadline),
                    Ok(read_result) => match read_result {
                        Ok(Some(object)) => {
                            let batch = decode_batch(&object.bytes)?;
                            validate_batch_identity(&batch, self.stream, seq)?;
                            if batch.trailer == ours {
                                return Ok(RecoveryWrite::Written);
                            }
                            return Ok(RecoveryWrite::Found(batch));
                        }
                        Ok(None) => {}
                        Err(error) if error.kind == ObjectStoreErrorKind::Transient => {}
                        Err(error) => return Err(error.into()),
                    },
                }
            }
            sleep(backoff.min(deadline.saturating_duration_since(Instant::now()))).await;
            backoff = (backoff * 2).min(Duration::from_millis(500));
        }
    }

    /// Returns the highest seq that a listing of the stream shows after `seq`, if any.
    async fn last_listed_seq_after(&self, seq: u64) -> Result<Option<u64>, LogError> {
        let start_key = self.stream.batch_key(seq);
        let prefix_length = batch_key_prefix_length(&start_key);
        let mut start_after = start_key.clone();
        let mut last_key = None;
        loop {
            let page = self
                .store
                .list(
                    &start_key[..prefix_length],
                    Some(&start_after),
                    LIST_PAGE_KEYS,
                )
                .await?;
            if let Some(object) = page.objects.last() {
                last_key = Some(object.key.clone());
            }
            match page.next_start_after {
                Some(next_start_after) => start_after = next_start_after,
                None => break,
            }
        }
        last_key
            .map(|key| parse_listed_seq(&key, prefix_length))
            .transpose()
    }
}

/// Fails with [`LogError::MissingBatches`] when `missing_seq` was deleted and later batches remain.
async fn ensure_batches_after_start_were_not_deleted(
    store: &dyn ObjectStore,
    stream: &StreamIdentity,
    missing_seq: u64,
) -> Result<(), LogError> {
    // Cleanup deletes only batches below a published snapshot's applied position, so a replayable
    // applied position keeps its batch.
    if missing_seq == 1
        || store
            .head(&stream.batch_key(missing_seq - 1))
            .await?
            .is_some()
    {
        return Ok(());
    }
    let missing_key = stream.batch_key(missing_seq);
    let prefix_length = batch_key_prefix_length(&missing_key);
    let listing = store
        .list(&missing_key[..prefix_length], Some(&missing_key), 1)
        .await?;
    let Some(next) = listing.objects.first() else {
        return Ok(());
    };
    let next_existing_seq = parse_listed_seq(&next.key, prefix_length)?;
    Err(LogError::MissingBatches {
        missing_seq,
        next_existing_seq,
    })
}

/// Returns the length of the stream prefix of `batch_key`, up to its last `/`.
fn batch_key_prefix_length(batch_key: &str) -> usize {
    batch_key.rfind('/').expect("a batch key ends with /<seq>") + 1
}

/// Returns the seq that a listed key of the stream names, or [`LogError::Corrupt`] for another name.
fn parse_listed_seq(key: &str, prefix_length: usize) -> Result<u64, LogError> {
    key[prefix_length..]
        .parse::<u64>()
        .map_err(|_| LogError::Corrupt(format!("log stream holds an object named {key}")))
}

fn next_seq_after(seq: u64) -> Result<u64, LogError> {
    seq.checked_add(1)
        .ok_or_else(|| LogError::Invalid("stream sequence exhausted".into()))
}

pub(crate) fn validate_batch_identity(
    batch: &DecodedBatch,
    stream: &StreamIdentity,
    seq: u64,
) -> Result<(), LogError> {
    if batch.header.cluster_id != stream.cluster_id
        || batch.header.node_id != stream.node_id
        || batch.header.incarnation != stream.incarnation
        || batch.header.seq != seq
    {
        return Err(LogError::Corrupt(format!(
            "batch at seq {seq} has a different stream identity"
        )));
    }
    Ok(())
}

fn accept_batch(
    result: &mut RecoveredStream,
    batch: DecodedBatch,
    seq: u64,
    effective_epoch: &mut u64,
) {
    if batch.header.epoch < *effective_epoch {
        result.void_sequences.push(seq);
        return;
    }
    if let Some(epoch_start) = &batch.header.epoch_start {
        *effective_epoch = epoch_start.epoch;
    }
    result.batches_to_apply.push(batch);
}
