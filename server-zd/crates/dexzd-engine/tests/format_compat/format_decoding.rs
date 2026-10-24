// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::{BTreeMap, BTreeSet};

use dexzd_engine::{HardDeleteObjectOwner, decode_hard_delete_value};
use dexzd_keyspace::{
    ColumnFamily, FLOW_DIRECTORY_KEY_TAG, RunTable, decode_partition_meta_key, parse_run_key,
};
use dexzd_log::decode_batch;
use dexzd_protocol::dexzd::v1::{
    AttemptRecord, AttributeIndexRegistry, AttributeRecord, ChannelMessageRecord,
    ChannelMetaRecord, ChannelWaiterRecord, CheckpointRecord, ClusterObject, FlowCommand,
    FlowDirectoryRecord, HardDeleteObjectRecord, HistoryRecord, HistoryRowMetadata,
    InboxDedupRecord, LocalStoreKind, LockRecord, LogRecord, Manifest, NodeMeta, OutboxRecord,
    PartitionMeta, PermissionSetRecord, RequestDedupRecord, RunRecord, SearchDocumentRecord,
    SnapshotManifest, StepCounterRecord, StepExecutionRecord, SubFlowResultRecord, TimerRecord,
    ValueBirthRecord, ValueGarbageRecord, ValueRecord, log_record,
};
use prost::Message;
use xxhash_rust::xxh3::xxh3_64;

use crate::state_dump::hex;

const CLUSTER_KEY: &str = "cluster";
const MANIFEST_KEY: &str = "manifest";
const ATTRIBUTE_INDEX_REGISTRY_KEY: &str = "registry/attribute-indexes";
const LOG_PREFIX: &str = "log/";
const SNAPSHOT_PREFIX: &str = "snapshot/";
const HARD_DELETE_PREFIX: &str = "hard-delete/";
const MEMORY_SNAPSHOT_MAGIC: &[u8] = b"DEXZDMEM1";

/// Every recorded object decoded with the current protobuf types.
pub(crate) struct DecodedObjects {
    pub(crate) cluster: ClusterObject,
    pub(crate) manifest: Manifest,
    pub(crate) commands: Vec<FlowCommand>,
    pub(crate) log_batches: usize,
    /// Most partition frames in one batch.
    pub(crate) widest_batch_frames: usize,
    /// Most records in one partition frame.
    pub(crate) largest_frame_records: u32,
    pub(crate) snapshot_rows: usize,
    pub(crate) hard_delete_values: usize,
}

/// Decodes every object, log record, command, and snapshot row; unknown object kinds panic.
pub(crate) fn decode_objects(objects: &BTreeMap<String, Vec<u8>>) -> DecodedObjects {
    let cluster_bytes = objects.get(CLUSTER_KEY).expect("the cluster object exists");
    let cluster = ClusterObject::decode(cluster_bytes.as_slice()).expect("ClusterObject decodes");
    let manifest_bytes = objects.get(MANIFEST_KEY).expect("the manifest exists");
    let manifest = Manifest::decode(manifest_bytes.as_slice()).expect("Manifest decodes");
    let mut decoded = DecodedObjects {
        cluster,
        manifest,
        commands: Vec::new(),
        log_batches: 0,
        widest_batch_frames: 0,
        largest_frame_records: 0,
        snapshot_rows: 0,
        hard_delete_values: 0,
    };
    let mut snapshot_files = BTreeSet::new();
    let mut referenced_snapshot_files = BTreeSet::new();
    for (key, body) in objects {
        if key == CLUSTER_KEY || key == MANIFEST_KEY {
            continue;
        }
        if key == ATTRIBUTE_INDEX_REGISTRY_KEY {
            AttributeIndexRegistry::decode(body.as_slice())
                .expect("AttributeIndexRegistry decodes");
        } else if key.starts_with(LOG_PREFIX) {
            decode_log_batch(key, body, &mut decoded);
            decoded.log_batches += 1;
        } else if key.starts_with(SNAPSHOT_PREFIX) && key.contains("/manifest/") {
            let snapshot = SnapshotManifest::decode(body.as_slice())
                .unwrap_or_else(|error| panic!("SnapshotManifest {key} decodes: {error}"));
            decoded.snapshot_rows += decode_snapshot_files(key, &snapshot, objects);
            referenced_snapshot_files
                .extend(snapshot.files.into_iter().map(|file| file.object_key));
        } else if key.starts_with(SNAPSHOT_PREFIX) && key.contains("/files/") {
            snapshot_files.insert(key.clone());
        } else if key.starts_with(HARD_DELETE_PREFIX) {
            HardDeleteObjectOwner::parse_object_key(key)
                .unwrap_or_else(|| panic!("hard-delete object key {key} parses"));
            decode_hard_delete_value(key, body)
                .unwrap_or_else(|error| panic!("hard-delete object {key} decodes: {error}"));
            decoded.hard_delete_values += 1;
        } else {
            panic!("recorded object {key} has an unknown kind; decode it in format_decoding.rs");
        }
    }
    assert_eq!(
        snapshot_files, referenced_snapshot_files,
        "every snapshot file belongs to a recorded snapshot manifest"
    );
    decoded
}

fn decode_log_batch(key: &str, body: &[u8], decoded: &mut DecodedObjects) {
    let batch =
        decode_batch(body).unwrap_or_else(|error| panic!("log batch {key} decodes: {error}"));
    assert_eq!(
        batch.header.cluster_id, decoded.cluster.cluster_id,
        "log batch {key} belongs to the recorded cluster"
    );
    let frames = &batch.header.partition_frames;
    decoded.widest_batch_frames = decoded.widest_batch_frames.max(frames.len());
    decoded.largest_frame_records = frames
        .iter()
        .map(|frame| frame.record_count)
        .fold(decoded.largest_frame_records, u32::max);
    for record in batch.records {
        let log_record = LogRecord::decode(record.bytes.as_ref())
            .unwrap_or_else(|error| panic!("LogRecord in {key} decodes: {error}"));
        assert_eq!(
            log_record.partition, record.partition,
            "frame partition in {key}"
        );
        let Some(log_record::Body::Flow(command)) = log_record.body else {
            panic!("LogRecord in {key} has no Flow command");
        };
        assert!(
            command.command.is_some(),
            "FlowCommand in {key} has a known command body"
        );
        decoded.commands.push(command);
    }
}

/// Checks a snapshot's files and decodes every memory snapshot row.
///
/// Returns the number of decoded rows.
fn decode_snapshot_files(
    manifest_key: &str,
    snapshot: &SnapshotManifest,
    objects: &BTreeMap<String, Vec<u8>>,
) -> usize {
    assert_eq!(
        snapshot.store_kind,
        LocalStoreKind::Memory as i32,
        "snapshot {manifest_key} uses the deterministic memory store kind"
    );
    let mut rows = 0;
    for file in &snapshot.files {
        let body = objects
            .get(&file.object_key)
            .unwrap_or_else(|| panic!("snapshot file {} is recorded", file.object_key));
        assert_eq!(
            body.len() as u64,
            file.size_bytes,
            "size of {}",
            file.object_key
        );
        assert_eq!(
            xxh3_64(body),
            file.xxh3_64,
            "checksum of {}",
            file.object_key
        );
        for (column_family, key, value) in memory_snapshot_rows(body) {
            decode_state_row(column_family, &key, &value);
            rows += 1;
        }
    }
    rows
}

/// Splits a `DEXZDMEM1` memory snapshot into its rows.
fn memory_snapshot_rows(body: &[u8]) -> Vec<(ColumnFamily, Vec<u8>, Vec<u8>)> {
    let mut cursor = body
        .strip_prefix(MEMORY_SNAPSHOT_MAGIC)
        .expect("memory snapshot starts with its magic");
    let mut rows = Vec::new();
    for column_family in ColumnFamily::ALL {
        let count = take_u64(&mut cursor);
        for _ in 0..count {
            let key = take_length_prefixed(&mut cursor);
            let value = take_length_prefixed(&mut cursor);
            rows.push((column_family, key, value));
        }
    }
    assert!(cursor.is_empty(), "memory snapshot has no trailing bytes");
    rows
}

fn take_u64(cursor: &mut &[u8]) -> u64 {
    let (bytes, rest) = cursor
        .split_first_chunk::<8>()
        .expect("memory snapshot is not truncated");
    *cursor = rest;
    u64::from_le_bytes(*bytes)
}

fn take_length_prefixed(cursor: &mut &[u8]) -> Vec<u8> {
    let length = usize::try_from(take_u64(cursor)).expect("length fits usize");
    let (bytes, rest) = cursor
        .split_at_checked(length)
        .expect("memory snapshot is not truncated");
    *cursor = rest;
    bytes.to_vec()
}

/// Decodes a state row as the message its column family and key name.
pub(crate) fn decode_state_row(column_family: ColumnFamily, key: &[u8], value: &[u8]) {
    let describe = || format!("{} row {}", column_family.name(), hex(key));
    match column_family {
        ColumnFamily::PartitionMeta => {
            decode_partition_meta_key(key)
                .unwrap_or_else(|error| panic!("{}: {error}", describe()));
            decode_as::<PartitionMeta>(value, &describe);
        }
        ColumnFamily::NodeMeta => decode_as::<NodeMeta>(value, &describe),
        // Index rows carry no message: their keys are the data.
        ColumnFamily::PartitionIndex => {}
        ColumnFamily::Flow | ColumnFamily::History | ColumnFamily::Value => {
            if key.get(8) == Some(&FLOW_DIRECTORY_KEY_TAG) {
                decode_as::<FlowDirectoryRecord>(value, &describe);
                return;
            }
            let parsed =
                parse_run_key(key).unwrap_or_else(|error| panic!("{}: {error}", describe()));
            decode_run_row(parsed.table, value, &describe);
        }
    }
}

fn decode_run_row(table: RunTable, value: &[u8], describe: &dyn Fn() -> String) {
    match table {
        RunTable::RunRecord => decode_as::<RunRecord>(value, describe),
        RunTable::AttemptRecord => decode_as::<AttemptRecord>(value, describe),
        RunTable::Attribute => decode_as::<AttributeRecord>(value, describe),
        RunTable::ChannelMessage => decode_as::<ChannelMessageRecord>(value, describe),
        RunTable::ChannelMeta => decode_as::<ChannelMetaRecord>(value, describe),
        RunTable::StepExecution => decode_as::<StepExecutionRecord>(value, describe),
        RunTable::StepCounter => decode_as::<StepCounterRecord>(value, describe),
        RunTable::LockTable => decode_as::<LockRecord>(value, describe),
        RunTable::Waiter => decode_as::<ChannelWaiterRecord>(value, describe),
        RunTable::Timer => decode_as::<TimerRecord>(value, describe),
        RunTable::Outbox => decode_as::<OutboxRecord>(value, describe),
        RunTable::InboxDedup => decode_as::<InboxDedupRecord>(value, describe),
        RunTable::RequestDedup => decode_as::<RequestDedupRecord>(value, describe),
        RunTable::SubFlowResult => decode_as::<SubFlowResultRecord>(value, describe),
        RunTable::PermissionSet => decode_as::<PermissionSetRecord>(value, describe),
        RunTable::Value => decode_as::<ValueRecord>(value, describe),
        RunTable::ValueBirth => decode_as::<ValueBirthRecord>(value, describe),
        RunTable::ValueGarbage => decode_as::<ValueGarbageRecord>(value, describe),
        RunTable::HardDeleteObject => decode_as::<HardDeleteObjectRecord>(value, describe),
        RunTable::HistoryRecord => decode_as::<HistoryRecord>(value, describe),
        RunTable::HistoryRowMetadata => decode_as::<HistoryRowMetadata>(value, describe),
        RunTable::Checkpoint => decode_as::<CheckpointRecord>(value, describe),
        RunTable::SearchDocument => decode_as::<SearchDocumentRecord>(value, describe),
        // Holder and dedup order rows carry no message.
        RunTable::ValueHolder | RunTable::RequestDedupOrder => {}
    }
}

fn decode_as<RowMessage: Message + Default>(value: &[u8], describe: &dyn Fn() -> String) {
    if let Err(error) = RowMessage::decode(value) {
        panic!(
            "{} does not decode as {}: {error}",
            describe(),
            std::any::type_name::<RowMessage>()
        );
    }
}
