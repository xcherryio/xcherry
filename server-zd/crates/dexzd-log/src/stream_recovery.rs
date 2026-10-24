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
    let deadline = Instant::now() + RECOVERY_WRITE_DEADLINE;
    let mut backoff = Duration::from_millis(10);
    let mut encoded_seq = 0;
    let mut body = bytes::Bytes::new();
    let mut ours = 0;
    loop {
        if Instant::now() >= deadline {
            return Err(LogError::Deadline);
        }
        let key = stream.batch_key(seq);
        if encoded_seq != seq {
            let epoch_start = BatchContent {
                stream: stream.clone(),
                epoch: new_epoch,
                seq,
                sealed_at_unix_micros: clock.now_unix_micros(),
                records: Vec::new(),
                epoch_start_writer: Some(writer_process_id.to_owned()),
            };
            body = encode_batch(&epoch_start)?;
            ours = xxh3_128(&body[..body.len() - 16]);
            encoded_seq = seq;
        }
        let should_read_back = match timeout_at(
            deadline,
            store.put(&key, body.clone(), WriteCondition::CreateOnly),
        )
        .await
        {
            Err(_) => return Err(LogError::Deadline),
            Ok(write_result) => match write_result {
                Ok(WriteOutcome::Written { .. }) => {
                    result.epoch_start_seq = seq;
                    return finish_epoch_start(store, stream, new_epoch, result).await;
                }
                Ok(WriteOutcome::PreconditionFailed) => true,
                Err(error) if error.kind == ObjectStoreErrorKind::OutcomeUnknown => true,
                Err(error) if error.kind == ObjectStoreErrorKind::Transient => false,
                Err(error) => return Err(error.into()),
            },
        };
        if should_read_back {
            match timeout_at(deadline, store.get(&key)).await {
                Err(_) => return Err(LogError::Deadline),
                Ok(read_result) => match read_result {
                    Ok(Some(object)) => {
                        let batch = decode_batch(&object.bytes)?;
                        validate_batch_identity(&batch, stream, seq)?;
                        if batch.trailer == ours {
                            result.epoch_start_seq = seq;
                            return finish_epoch_start(store, stream, new_epoch, result).await;
                        }
                        if batch.header.epoch >= new_epoch {
                            return Err(LogError::Fenced);
                        }
                        accept_batch(&mut result, batch, seq, &mut effective_epoch);
                        seq = seq
                            .checked_add(1)
                            .ok_or_else(|| LogError::Invalid("stream sequence exhausted".into()))?;
                        backoff = Duration::from_millis(10);
                        continue;
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

async fn finish_epoch_start(
    store: &dyn ObjectStore,
    stream: &StreamIdentity,
    new_epoch: u64,
    mut result: RecoveredStream,
) -> Result<RecoveredStream, LogError> {
    let mut seq = result
        .epoch_start_seq
        .checked_add(1)
        .ok_or_else(|| LogError::Invalid("stream sequence exhausted".into()))?;
    loop {
        let key = stream.batch_key(seq);
        let Some(object) = store.get(&key).await? else {
            result.next_seq = seq;
            return Ok(result);
        };
        let batch = decode_batch(&object.bytes)?;
        validate_batch_identity(&batch, stream, seq)?;
        if batch.header.epoch >= new_epoch {
            return Err(LogError::Fenced);
        }
        result.void_sequences.push(seq);
        seq = seq
            .checked_add(1)
            .ok_or_else(|| LogError::Invalid("stream sequence exhausted".into()))?;
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
    let prefix_length = missing_key
        .rfind('/')
        .expect("a batch key ends with /<seq>")
        + 1;
    let listing = store
        .list(&missing_key[..prefix_length], Some(&missing_key), 1)
        .await?;
    let Some(next) = listing.objects.first() else {
        return Ok(());
    };
    let next_existing_seq = next.key[prefix_length..]
        .parse::<u64>()
        .map_err(|_| LogError::Corrupt(format!("log stream holds an object named {}", next.key)))?;
    Err(LogError::MissingBatches {
        missing_seq,
        next_existing_seq,
    })
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
