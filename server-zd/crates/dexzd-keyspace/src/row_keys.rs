// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Key suffixes of run tables and keys of partition indexes. Apply writes these keys and the
//! engine and read paths scan them, so both sides must use these functions.

use crate::{
    FlowHash, IndexKind, PartitionId, RunId, RunTable, TenantId, ValueId, partition_index_key,
    partition_index_prefix, run_key,
};

/// Separator between a variable-length name and the rest of a key. Names never contain NUL.
pub const NAME_SEPARATOR: u8 = 0x00;

/// Hard-delete table sub-key kind for the current object of an Attribute.
pub const HARD_DELETE_CURRENT_OBJECT: u8 = 0x00;
/// Hard-delete table sub-key kind for an object waiting for deletion.
pub const HARD_DELETE_PENDING_OBJECT: u8 = 0x01;

/// Encodes a signed timestamp so that byte order equals numeric order.
pub fn ordered_timestamp_bytes(micros: i64) -> [u8; 8] {
    ((micros as u64) ^ (1_u64 << 63)).to_be_bytes()
}

/// Decodes [`ordered_timestamp_bytes`].
pub fn timestamp_from_ordered_bytes(bytes: [u8; 8]) -> i64 {
    (u64::from_be_bytes(bytes) ^ (1_u64 << 63)) as i64
}

/// Key of a single-row run table (RunRecord, PermissionSet, SearchDocument).
pub fn run_singleton_key(flow_hash: FlowHash, run_id: RunId, table: RunTable) -> Vec<u8> {
    run_key(flow_hash, run_id, table, &[])
}

/// Key of an AttemptRecord.
pub fn attempt_record_key(flow_hash: FlowHash, run_id: RunId, attempt: u32) -> Vec<u8> {
    run_key(
        flow_hash,
        run_id,
        RunTable::AttemptRecord,
        &attempt.to_be_bytes(),
    )
}

/// Key of a row whose suffix is one name: Attribute, ChannelMeta, StepExecution, StepCounter,
/// LockTable, SubFlowResult.
pub fn run_named_key(flow_hash: FlowHash, run_id: RunId, table: RunTable, name: &str) -> Vec<u8> {
    run_key(flow_hash, run_id, table, name.as_bytes())
}

/// Key of a pending Channel message: channel name, NUL, enqueue seq.
pub fn channel_message_key(
    flow_hash: FlowHash,
    run_id: RunId,
    channel_name: &str,
    enqueued_seq: u64,
) -> Vec<u8> {
    let mut suffix = channel_name_prefix(channel_name);
    suffix.extend_from_slice(&enqueued_seq.to_be_bytes());
    run_key(flow_hash, run_id, RunTable::ChannelMessage, &suffix)
}

/// Suffix prefix of every message of one channel.
pub fn channel_name_prefix(channel_name: &str) -> Vec<u8> {
    let mut suffix = channel_name.as_bytes().to_vec();
    suffix.push(NAME_SEPARATOR);
    suffix
}

/// Key of a channel waiter: channel name, NUL, Step execution ID.
pub fn channel_waiter_key(
    flow_hash: FlowHash,
    run_id: RunId,
    channel_name: &str,
    step_execution_id: &str,
) -> Vec<u8> {
    let mut suffix = channel_name_prefix(channel_name);
    suffix.extend_from_slice(step_execution_id.as_bytes());
    run_key(flow_hash, run_id, RunTable::Waiter, &suffix)
}

/// Key of a timer row; `encoded_timer_id` is the protobuf encoding of the TimerId.
pub fn timer_key(flow_hash: FlowHash, run_id: RunId, encoded_timer_id: &[u8]) -> Vec<u8> {
    run_key(flow_hash, run_id, RunTable::Timer, encoded_timer_id)
}

/// Key of an outbox row.
pub fn outbox_key(flow_hash: FlowHash, run_id: RunId, message_id: &[u8]) -> Vec<u8> {
    run_key(flow_hash, run_id, RunTable::Outbox, message_id)
}

/// Key of an inbox deduplication row.
pub fn inbox_dedup_key(
    flow_hash: FlowHash,
    run_id: RunId,
    source_run_id: &[u8],
    message_id: &[u8],
) -> Vec<u8> {
    let mut suffix = source_run_id.to_vec();
    suffix.extend_from_slice(message_id);
    run_key(flow_hash, run_id, RunTable::InboxDedup, &suffix)
}

/// Kinds of requests deduplicated by request ID. Byte values are permanent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum RequestKind {
    /// StartFlow.
    StartFlow = 0x01,
    /// SetAttributes.
    SetAttributes = 0x02,
    /// DeleteChannelMessage.
    DeleteChannelMessage = 0x03,
    /// InvokeRPC.
    InvokeRpc = 0x04,
}

/// Key of a request deduplication row.
pub fn request_dedup_key(
    flow_hash: FlowHash,
    run_id: RunId,
    kind: RequestKind,
    request_id: &str,
) -> Vec<u8> {
    let mut suffix = vec![kind as u8];
    suffix.extend_from_slice(request_id.as_bytes());
    run_key(flow_hash, run_id, RunTable::RequestDedup, &suffix)
}

/// Key of a request deduplication order row (recorded seq).
pub fn request_dedup_order_key(flow_hash: FlowHash, run_id: RunId, recorded_seq: u64) -> Vec<u8> {
    run_key(
        flow_hash,
        run_id,
        RunTable::RequestDedupOrder,
        &recorded_seq.to_be_bytes(),
    )
}

/// Key of a value-table row, a value birth row, or the prefix of a value's holders.
pub fn value_row_key(
    flow_hash: FlowHash,
    run_id: RunId,
    table: RunTable,
    value_id: ValueId,
) -> Vec<u8> {
    run_key(flow_hash, run_id, table, &value_id.to_key_suffix())
}

/// Key of one holder of a value.
pub fn value_holder_key(
    flow_hash: FlowHash,
    run_id: RunId,
    value_id: ValueId,
    holder: &[u8],
) -> Vec<u8> {
    let mut suffix = value_id.to_key_suffix().to_vec();
    suffix.extend_from_slice(holder);
    run_key(flow_hash, run_id, RunTable::ValueHolder, &suffix)
}

/// Key of a garbage row: the seq at which the value lost its last holder, then the value ID.
pub fn value_garbage_key(
    flow_hash: FlowHash,
    run_id: RunId,
    lost_last_holder_seq: u64,
    value_id: ValueId,
) -> Vec<u8> {
    let mut suffix = lost_last_holder_seq.to_be_bytes().to_vec();
    suffix.extend_from_slice(&value_id.to_key_suffix());
    run_key(flow_hash, run_id, RunTable::ValueGarbage, &suffix)
}

/// Key of the current hard-delete object of an Attribute.
pub fn hard_delete_current_key(flow_hash: FlowHash, run_id: RunId, attribute_key: &str) -> Vec<u8> {
    let mut suffix = vec![HARD_DELETE_CURRENT_OBJECT];
    suffix.extend_from_slice(attribute_key.as_bytes());
    run_key(flow_hash, run_id, RunTable::HardDeleteObject, &suffix)
}

/// Key of a hard-delete object waiting for deletion.
pub fn hard_delete_pending_key(flow_hash: FlowHash, run_id: RunId, object_id: &str) -> Vec<u8> {
    let mut suffix = vec![HARD_DELETE_PENDING_OBJECT];
    suffix.extend_from_slice(object_id.as_bytes());
    run_key(flow_hash, run_id, RunTable::HardDeleteObject, &suffix)
}

/// Key of a history record.
pub fn history_record_key(flow_hash: FlowHash, run_id: RunId, seq: u64) -> Vec<u8> {
    run_key(
        flow_hash,
        run_id,
        RunTable::HistoryRecord,
        &seq.to_be_bytes(),
    )
}

/// Key of a checkpoint.
pub fn checkpoint_key(flow_hash: FlowHash, run_id: RunId, attempt: u32, seq: u64) -> Vec<u8> {
    let mut suffix = attempt.to_be_bytes().to_vec();
    suffix.extend_from_slice(&seq.to_be_bytes());
    run_key(flow_hash, run_id, RunTable::Checkpoint, &suffix)
}

fn flow_run_suffix(flow_hash: FlowHash, run_id: RunId) -> Vec<u8> {
    let mut suffix = flow_hash.to_be_bytes().to_vec();
    suffix.extend_from_slice(run_id.as_bytes());
    suffix
}

/// Timer index key: fire time, Flow hash, run ID, encoded TimerId.
pub fn timer_index_key(
    partition: PartitionId,
    fire_at_micros: i64,
    flow_hash: FlowHash,
    run_id: RunId,
    encoded_timer_id: &[u8],
) -> Vec<u8> {
    let mut rest = ordered_timestamp_bytes(fire_at_micros).to_vec();
    rest.extend_from_slice(&flow_run_suffix(flow_hash, run_id));
    rest.extend_from_slice(encoded_timer_id);
    partition_index_key(partition, IndexKind::Timer, &rest)
}

/// A decoded timer index key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerIndexEntry {
    /// Fire time in microseconds since the Unix epoch.
    pub fire_at_micros: i64,
    /// Flow of the timer.
    pub flow_hash: FlowHash,
    /// Run of the timer.
    pub run_id: RunId,
    /// Protobuf encoding of the TimerId.
    pub encoded_timer_id: Vec<u8>,
}

/// Decodes a timer index key; returns `None` for keys of another kind or a malformed key.
pub fn decode_timer_index_key(key: &[u8]) -> Option<TimerIndexEntry> {
    if key.len() < 3 + 8 + 8 + 16 || key[2] != IndexKind::Timer as u8 {
        return None;
    }
    let fire_at_micros = timestamp_from_ordered_bytes(key[3..11].try_into().ok()?);
    let flow_hash = FlowHash(u64::from_be_bytes(key[11..19].try_into().ok()?));
    let run_id = RunId(key[19..35].try_into().ok()?);
    Some(TimerIndexEntry {
        fire_at_micros,
        flow_hash,
        run_id,
        encoded_timer_id: key[35..].to_vec(),
    })
}

/// Flow list index key: tenant, Flow type, NUL, status, Flow hash, run ID.
pub fn flow_list_index_key(
    partition: PartitionId,
    tenant_id: TenantId,
    flow_type: &str,
    status: u8,
    flow_hash: FlowHash,
    run_id: RunId,
) -> Vec<u8> {
    let mut rest = tenant_id.to_be_bytes().to_vec();
    rest.extend_from_slice(flow_type.as_bytes());
    rest.push(NAME_SEPARATOR);
    rest.push(status);
    rest.extend_from_slice(&flow_run_suffix(flow_hash, run_id));
    partition_index_key(partition, IndexKind::FlowList, &rest)
}

/// Index key keyed by tenant, Flow hash, and run ID (SearchDocument, TenantRuns).
pub fn tenant_run_index_key(
    partition: PartitionId,
    kind: IndexKind,
    tenant_id: TenantId,
    flow_hash: FlowHash,
    run_id: RunId,
) -> Vec<u8> {
    let mut rest = tenant_id.to_be_bytes().to_vec();
    rest.extend_from_slice(&flow_run_suffix(flow_hash, run_id));
    partition_index_key(partition, kind, &rest)
}

/// Index key keyed by Flow hash, run ID, and an item ID (OutboxPending, HardDeletePending).
pub fn run_item_index_key(
    partition: PartitionId,
    kind: IndexKind,
    flow_hash: FlowHash,
    run_id: RunId,
    item_id: &[u8],
) -> Vec<u8> {
    let mut rest = flow_run_suffix(flow_hash, run_id);
    rest.extend_from_slice(item_id);
    partition_index_key(partition, kind, &rest)
}

/// Retention index key: deletion time, Flow hash, run ID.
pub fn retention_index_key(
    partition: PartitionId,
    delete_at_micros: i64,
    flow_hash: FlowHash,
    run_id: RunId,
) -> Vec<u8> {
    let mut rest = ordered_timestamp_bytes(delete_at_micros).to_vec();
    rest.extend_from_slice(&flow_run_suffix(flow_hash, run_id));
    partition_index_key(partition, IndexKind::Retention, &rest)
}

/// Active work index key: runs with pending or running method calls.
pub fn active_work_index_key(
    partition: PartitionId,
    flow_hash: FlowHash,
    run_id: RunId,
) -> Vec<u8> {
    partition_index_key(
        partition,
        IndexKind::ActiveWork,
        &flow_run_suffix(flow_hash, run_id),
    )
}

/// Decodes the Flow hash and run ID that follow the kind byte of a partition index key.
pub fn decode_flow_run_after_prefix(key: &[u8], prefix_length: usize) -> Option<(FlowHash, RunId)> {
    let rest = key.get(prefix_length..)?;
    let flow_hash = FlowHash(u64::from_be_bytes(rest.get(..8)?.try_into().ok()?));
    let run_id = RunId(rest.get(8..24)?.try_into().ok()?);
    Some((flow_hash, run_id))
}

/// Returns the `[start, end)` range of one index of one partition.
pub fn partition_index_range(partition: PartitionId, kind: IndexKind) -> (Vec<u8>, Vec<u8>) {
    let start = partition_index_prefix(partition, kind);
    let mut end = start.clone();
    let last = end.len() - 1;
    end[last] += 1;
    (start, end)
}

/// Returns the `[start, end)` range of keys that start with `prefix`.
pub fn prefix_range(prefix: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let start = prefix.to_vec();
    let mut end = prefix.to_vec();
    while let Some(last) = end.last_mut() {
        if *last < u8::MAX {
            *last += 1;
            return (start, end);
        }
        end.pop();
    }
    (start, vec![u8::MAX; 64])
}

#[cfg(test)]
mod tests {
    use super::{
        decode_timer_index_key, ordered_timestamp_bytes, prefix_range, timer_index_key,
        timestamp_from_ordered_bytes,
    };
    use crate::{FlowHash, PartitionId, RunId};

    #[test]
    fn ordered_timestamps_sort_numerically() {
        let values = [i64::MIN, -5, 0, 7, i64::MAX];
        for pair in values.windows(2) {
            assert!(ordered_timestamp_bytes(pair[0]) < ordered_timestamp_bytes(pair[1]));
        }
        for value in values {
            assert_eq!(
                timestamp_from_ordered_bytes(ordered_timestamp_bytes(value)),
                value
            );
        }
    }

    #[test]
    fn timer_index_key_round_trips() {
        let key = timer_index_key(
            PartitionId(3),
            1_700_000,
            FlowHash(9),
            RunId([4; 16]),
            b"tid",
        );
        let entry = decode_timer_index_key(&key).unwrap();
        assert_eq!(entry.fire_at_micros, 1_700_000);
        assert_eq!(entry.flow_hash, FlowHash(9));
        assert_eq!(entry.run_id, RunId([4; 16]));
        assert_eq!(entry.encoded_timer_id, b"tid");
    }

    #[test]
    fn prefix_range_handles_trailing_max_bytes() {
        assert_eq!(prefix_range(&[1, 2]), (vec![1, 2], vec![1, 3]));
        assert_eq!(prefix_range(&[1, 0xff]), (vec![1, 0xff], vec![2]));
    }
}
