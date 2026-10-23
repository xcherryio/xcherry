// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! RocksDB key layout, version [`crate::KEY_LAYOUT_VERSION`].
//!
//! Flow-scoped column families (`flow`, `history`, `value`) start every key with the 8-byte
//! [`FlowHash`]. One partition is therefore one contiguous key range.
//!
//! Partition-scoped column families (`partition_index`, `partition_meta`) start with the 2-byte
//! [`PartitionId`].

use crate::{FlowHash, PartitionId, RunId, TenantId};

/// Errors from decoding a key read from the local store.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeyLayoutError {
    /// The key is shorter than its fixed-size prefix.
    #[error("key of length {length} is too short for a {kind} key")]
    TooShort {
        /// Kind of key being decoded.
        kind: &'static str,
        /// The key length.
        length: usize,
    },
    /// The key has an unknown tag or table byte.
    #[error("key has unknown {kind} byte {value:#04x}")]
    UnknownByte {
        /// Which byte was unknown.
        kind: &'static str,
        /// The byte value.
        value: u8,
    },
    /// A partition number is outside the cluster's partition count.
    #[error("partition {partition} is invalid for {partition_bits} partition bits")]
    InvalidPartition {
        /// The rejected partition.
        partition: u16,
        /// Partition bits of the cluster.
        partition_bits: u32,
    },
}

/// Column families of the local store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ColumnFamily {
    /// Flow directory and run state except history and large values.
    Flow,
    /// Append-mostly history records and attempt checkpoints.
    History,
    /// Values larger than the inline threshold; tuned with blob files.
    Value,
    /// Derived per-partition indexes (timers, Flow lists, search documents, pending work).
    PartitionIndex,
    /// Per-partition applied position and command timestamp watermark.
    PartitionMeta,
    /// Node-level metadata such as the store lineage.
    NodeMeta,
}

impl ColumnFamily {
    /// Every column family, in creation order.
    pub const ALL: [Self; 6] = [
        Self::Flow,
        Self::History,
        Self::Value,
        Self::PartitionIndex,
        Self::PartitionMeta,
        Self::NodeMeta,
    ];

    /// Returns the RocksDB column family name.
    pub fn name(self) -> &'static str {
        match self {
            Self::Flow => "flow",
            Self::History => "history",
            Self::Value => "value",
            Self::PartitionIndex => "partition_index",
            Self::PartitionMeta => "partition_meta",
            Self::NodeMeta => "node_meta",
        }
    }

    /// Returns true when keys start with a [`FlowHash`] rather than a [`PartitionId`].
    pub fn is_flow_scoped(self) -> bool {
        matches!(self, Self::Flow | Self::History | Self::Value)
    }
}

/// Tag byte after the Flow hash for a Flow directory key.
pub const FLOW_DIRECTORY_KEY_TAG: u8 = 0x01;
/// Tag byte after the Flow hash for a run key.
pub const RUN_KEY_TAG: u8 = 0x02;

/// Tables inside one run. Byte values are permanent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum RunTable {
    /// Run-scoped record: status, attempt counter, config, limits fixed at start.
    RunRecord = 0x01,
    /// One record per run attempt: parent point, start seq, start checkpoint.
    AttemptRecord = 0x02,
    /// Attribute values of the current attempt.
    Attribute = 0x03,
    /// Pending Channel messages of the current attempt.
    ChannelMessage = 0x04,
    /// Per-Channel counters of the current attempt.
    ChannelMeta = 0x05,
    /// Step executions of the current attempt.
    StepExecution = 0x06,
    /// Per-Step-type counters that assign `StepType-N`.
    StepCounter = 0x07,
    /// Attribute locks held by running attempts and their FIFO wait queues.
    LockTable = 0x08,
    /// Wait-condition waiters indexed by what they wait for.
    Waiter = 0x09,
    /// Timers of the current attempt and run-scoped timers.
    Timer = 0x0a,
    /// Committed cross-Flow messages waiting for acknowledgment.
    Outbox = 0x0b,
    /// Deduplication of delivered cross-Flow messages.
    InboxDedup = 0x0c,
    /// Request ID deduplication with request fingerprints.
    RequestDedup = 0x0d,
    /// Results of completed child Flows.
    SubFlowResult = 0x0e,
    /// Accumulated work queue permissions.
    PermissionSet = 0x0f,
    /// Values larger than the inline threshold (stored in the `value` column family).
    Value = 0x10,
    /// Value birth records: size and writing seq (table B).
    ValueBirth = 0x11,
    /// Value holder sets (table R).
    ValueHolder = 0x12,
    /// Values without holders, keyed by the seq at which they lost their last holder (table G).
    ValueGarbage = 0x13,
    /// Current hard-delete Attribute objects and objects pending deletion.
    HardDeleteObject = 0x14,
    /// History records, one per command applied to the run (`history` column family).
    HistoryRecord = 0x15,
    /// Attempt checkpoints (`history` column family).
    Checkpoint = 0x16,
    /// The run's search document.
    SearchDocument = 0x17,
}

impl RunTable {
    /// Every table, in byte order.
    pub const ALL: [Self; 23] = [
        Self::RunRecord,
        Self::AttemptRecord,
        Self::Attribute,
        Self::ChannelMessage,
        Self::ChannelMeta,
        Self::StepExecution,
        Self::StepCounter,
        Self::LockTable,
        Self::Waiter,
        Self::Timer,
        Self::Outbox,
        Self::InboxDedup,
        Self::RequestDedup,
        Self::SubFlowResult,
        Self::PermissionSet,
        Self::Value,
        Self::ValueBirth,
        Self::ValueHolder,
        Self::ValueGarbage,
        Self::HardDeleteObject,
        Self::HistoryRecord,
        Self::Checkpoint,
        Self::SearchDocument,
    ];

    /// Returns the column family that stores this table.
    pub fn column_family(self) -> ColumnFamily {
        match self {
            Self::Value => ColumnFamily::Value,
            Self::HistoryRecord | Self::Checkpoint => ColumnFamily::History,
            _ => ColumnFamily::Flow,
        }
    }

    /// Returns the table with byte `value`.
    pub fn from_byte(value: u8) -> Result<Self, KeyLayoutError> {
        Self::ALL
            .into_iter()
            .find(|table| *table as u8 == value)
            .ok_or(KeyLayoutError::UnknownByte {
                kind: "run table",
                value,
            })
    }
}

/// Derived per-partition indexes. Byte values are permanent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum IndexKind {
    /// `fire_at(8) ‖ flow_hash(8) ‖ run_id(16) ‖ timer_id`; times are sign-biased big-endian.
    Timer = 0x01,
    /// `tenant_id(8) ‖ type_length(2) ‖ flow_type ‖ status(1) ‖ flow_hash(8) ‖ run_id(16)`.
    FlowList = 0x02,
    /// `tenant_id(8) ‖ flow_hash(8) ‖ run_id(16)`; value is the search document version.
    SearchDocument = 0x03,
    /// `tenant_id(8) ‖ flow_hash(8) ‖ run_id(16)`.
    TenantRuns = 0x04,
    /// `flow_hash(8) ‖ run_id(16) ‖ message_id`.
    OutboxPending = 0x05,
    /// `flow_hash(8) ‖ run_id(16) ‖ object_id`.
    HardDeletePending = 0x06,
    /// `delete_at(8) ‖ flow_hash(8) ‖ run_id(16)`; times are sign-biased big-endian.
    Retention = 0x07,
    /// `flow_hash(8) ‖ run_id(16)`: runs with pending or running method calls, for recovery.
    ActiveWork = 0x08,
}

impl IndexKind {
    /// Every index kind, in byte order.
    pub const ALL: [Self; 8] = [
        Self::Timer,
        Self::FlowList,
        Self::SearchDocument,
        Self::TenantRuns,
        Self::OutboxPending,
        Self::HardDeletePending,
        Self::Retention,
        Self::ActiveWork,
    ];
}

/// A run key split into its parts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedRunKey<'a> {
    /// Hash of the run's Flow.
    pub flow_hash: FlowHash,
    /// The run.
    pub run_id: RunId,
    /// The table inside the run.
    pub table: RunTable,
    /// Table-specific suffix.
    pub suffix: &'a [u8],
}

/// Returns `flow_hash(8) ‖ 0x01 ‖ tenant_id(8) ‖ flow_id`.
pub fn flow_directory_key(flow_hash: FlowHash, tenant_id: TenantId, flow_id: &str) -> Vec<u8> {
    let mut key = flow_directory_prefix(flow_hash);
    key.extend_from_slice(&tenant_id.to_be_bytes());
    key.extend_from_slice(flow_id.as_bytes());
    key
}

/// Returns `flow_hash(8) ‖ 0x01`, the prefix of every directory key with this hash.
pub fn flow_directory_prefix(flow_hash: FlowHash) -> Vec<u8> {
    let mut key = Vec::with_capacity(9);
    key.extend_from_slice(&flow_hash.to_be_bytes());
    key.push(FLOW_DIRECTORY_KEY_TAG);
    key
}

/// Returns `flow_hash(8) ‖ 0x02 ‖ run_id(16)`, the prefix of every key of a run.
pub fn run_prefix(flow_hash: FlowHash, run_id: RunId) -> Vec<u8> {
    let mut key = Vec::with_capacity(25);
    key.extend_from_slice(&flow_hash.to_be_bytes());
    key.push(RUN_KEY_TAG);
    key.extend_from_slice(run_id.as_bytes());
    key
}

/// Returns the prefix of every key of one table in a run.
pub fn run_table_prefix(flow_hash: FlowHash, run_id: RunId, table: RunTable) -> Vec<u8> {
    let mut key = run_prefix(flow_hash, run_id);
    key.push(table as u8);
    key
}

/// Returns the key of one row of a run table.
pub fn run_key(flow_hash: FlowHash, run_id: RunId, table: RunTable, suffix: &[u8]) -> Vec<u8> {
    let mut key = run_table_prefix(flow_hash, run_id, table);
    key.extend_from_slice(suffix);
    key
}

/// Splits a run key into its parts.
///
/// Returns an error when the key is too short or has an unknown tag or table byte.
pub fn parse_run_key(key: &[u8]) -> Result<ParsedRunKey<'_>, KeyLayoutError> {
    if key.len() < 26 {
        return Err(KeyLayoutError::TooShort {
            kind: "run",
            length: key.len(),
        });
    }
    if key[8] != RUN_KEY_TAG {
        return Err(KeyLayoutError::UnknownByte {
            kind: "Flow key tag",
            value: key[8],
        });
    }
    Ok(ParsedRunKey {
        flow_hash: FlowHash(u64::from_be_bytes(key[..8].try_into().expect("8 bytes"))),
        run_id: RunId(key[9..25].try_into().expect("16 bytes")),
        table: RunTable::from_byte(key[25])?,
        suffix: &key[26..],
    })
}

/// Returns `partition(2) ‖ kind(1)`, the prefix of one index of one partition.
pub fn partition_index_prefix(partition: PartitionId, kind: IndexKind) -> Vec<u8> {
    let mut key = Vec::with_capacity(3);
    key.extend_from_slice(&partition.to_be_bytes());
    key.push(kind as u8);
    key
}

/// Returns `partition(2) ‖ kind(1) ‖ rest`.
pub fn partition_index_key(partition: PartitionId, kind: IndexKind, rest: &[u8]) -> Vec<u8> {
    let mut key = partition_index_prefix(partition, kind);
    key.extend_from_slice(rest);
    key
}

/// Returns the `partition_meta` key of a partition.
pub fn partition_meta_key(partition: PartitionId) -> [u8; 2] {
    partition.to_be_bytes()
}

/// Decodes a `partition_meta` key.
///
/// Returns an error when the key is not 2 bytes.
pub fn decode_partition_meta_key(key: &[u8]) -> Result<PartitionId, KeyLayoutError> {
    let bytes: [u8; 2] = key.try_into().map_err(|_| KeyLayoutError::TooShort {
        kind: "partition meta",
        length: key.len(),
    })?;
    Ok(PartitionId(u16::from_be_bytes(bytes)))
}

/// A Flow-scoped key range: `start` inclusive, `end` exclusive (`None` means unbounded).
pub type FlowKeyRange = (Vec<u8>, Option<Vec<u8>>);

/// Returns the `[start, end)` range of Flow-scoped keys that belong to `partition`; `end` is
/// `None` for the last partition.
///
/// Returns an error when `partition_bits` exceeds 16 or `partition` is outside the cluster.
/// A corrupt partition number must never select another partition's keys.
pub fn partition_key_range(
    partition: PartitionId,
    partition_bits: u32,
) -> Result<FlowKeyRange, KeyLayoutError> {
    if partition_bits > 16 || u32::from(partition.0) >= (1_u32 << partition_bits) {
        return Err(KeyLayoutError::InvalidPartition {
            partition: partition.0,
            partition_bits,
        });
    }
    if partition_bits == 0 {
        return Ok((Vec::new(), None));
    }
    let shift = 64 - partition_bits;
    let start = u64::from(partition.0) << shift;
    let next = u64::from(partition.0) + 1;
    let end = (next < (1_u64 << partition_bits)).then(|| (next << shift).to_be_bytes().to_vec());
    Ok((start.to_be_bytes().to_vec(), end))
}

#[cfg(test)]
mod tests {
    use super::{
        ColumnFamily, IndexKind, RunTable, flow_directory_key, parse_run_key, partition_index_key,
        partition_key_range, run_key,
    };
    use crate::{FlowHash, PartitionHasher, PartitionId, RunId, TenantId};

    #[test]
    fn run_key_round_trips() {
        let run_id = RunId([3; 16]);
        let key = run_key(FlowHash(42), run_id, RunTable::Attribute, b"status");
        let parsed = parse_run_key(&key).unwrap();
        assert_eq!(parsed.flow_hash, FlowHash(42));
        assert_eq!(parsed.run_id, run_id);
        assert_eq!(parsed.table, RunTable::Attribute);
        assert_eq!(parsed.suffix, b"status");
    }

    #[test]
    fn table_bytes_are_unique_and_permanent() {
        let bytes: Vec<u8> = RunTable::ALL.iter().map(|table| *table as u8).collect();
        assert_eq!(bytes, (1..=23).collect::<Vec<u8>>());
        let kinds: Vec<u8> = IndexKind::ALL.iter().map(|kind| *kind as u8).collect();
        assert_eq!(kinds, (1..=8).collect::<Vec<u8>>());
        assert_eq!(RunTable::Value.column_family(), ColumnFamily::Value);
        assert_eq!(RunTable::Checkpoint.column_family(), ColumnFamily::History);
    }

    #[test]
    fn flow_keys_fall_inside_their_partition_range() {
        let hasher = PartitionHasher::new(&[9; 16], 1024).unwrap();
        for flow_id in ["a", "order-17", "SubFlow:x-S-1-0"] {
            let hash = hasher.flow_hash(TenantId::DEFAULT, flow_id);
            let partition = hasher.partition_of(hash);
            let (start, end) = partition_key_range(partition, hasher.partition_bits()).unwrap();
            let key = flow_directory_key(hash, TenantId::DEFAULT, flow_id);
            assert!(key.as_slice() >= start.as_slice());
            if let Some(end) = end {
                assert!(key.as_slice() < end.as_slice());
            }
        }
    }

    #[test]
    fn last_partition_range_is_unbounded() {
        let (start, end) = partition_key_range(PartitionId(1023), 10).unwrap();
        assert_eq!(start, (1023_u64 << 54).to_be_bytes().to_vec());
        assert!(end.is_none());
        let (_, end) = partition_key_range(PartitionId(0), 10).unwrap();
        assert_eq!(end.unwrap(), (1_u64 << 54).to_be_bytes().to_vec());
    }

    #[test]
    fn invalid_partitions_are_rejected() {
        assert!(partition_key_range(PartitionId(2), 1).is_err());
        assert!(partition_key_range(PartitionId(1), 0).is_err());
        assert!(partition_key_range(PartitionId(0), 17).is_err());
        assert_eq!(
            partition_key_range(PartitionId(0), 0).unwrap(),
            (Vec::new(), None)
        );
        assert!(
            partition_key_range(PartitionId(65_535), 16)
                .unwrap()
                .1
                .is_none()
        );
    }

    #[test]
    fn partition_index_keys_group_by_partition_and_kind() {
        let key = partition_index_key(PartitionId(5), IndexKind::Timer, &[1, 2]);
        assert_eq!(key, vec![0, 5, IndexKind::Timer as u8, 1, 2]);
    }
}
