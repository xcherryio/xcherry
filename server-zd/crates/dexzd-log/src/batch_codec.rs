// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;
use std::io::{Cursor, Read};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use dexzd_object_store::{ByteRange, ObjectStore};
use dexzd_protocol::dexzd::v1::{LogBatchHeader, LogEpochStart, LogPartitionFrame};
use prost::Message;
use thiserror::Error;
use xxhash_rust::xxh3::{xxh3_64, xxh3_128};

const MAGIC: &[u8; 8] = b"DEXZDLOG";
const FORMAT_VERSION: u16 = 1;
const PREFIX_LENGTH: usize = 14;
const TRAILER_LENGTH: usize = 16;
const MAX_HEADER_LENGTH: usize = 1_048_576;
const MAX_UNCOMPRESSED_FRAME_LENGTH: u64 = 64 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum LogError {
    #[error("corrupt log batch: {0}")]
    Corrupt(String),
    #[error("invalid log batch: {0}")]
    Invalid(String),
    #[error("log stream fenced by a newer epoch")]
    Fenced,
    #[error("log pipeline is unavailable; retry the proposal")]
    Retryable,
    #[error("log pipeline deadline expired")]
    Deadline,
    #[error(transparent)]
    Store(#[from] dexzd_object_store::ObjectStoreError),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamIdentity {
    pub cluster_id: [u8; 16],
    pub node_id: String,
    pub incarnation: u32,
}

impl StreamIdentity {
    pub fn batch_key(&self, seq: u64) -> String {
        format!("log/{}/{:010}/{:020}", self.node_id, self.incarnation, seq)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BatchRecord {
    pub partition: u32,
    pub bytes: Bytes,
}

#[derive(Clone, Debug)]
pub struct BatchContent {
    pub stream: StreamIdentity,
    pub epoch: u64,
    pub seq: u64,
    pub sealed_at_unix_micros: i64,
    pub records: Vec<BatchRecord>,
    pub epoch_start_writer: Option<String>,
}

#[derive(Clone, Debug)]
pub struct DecodedBatch {
    pub header: LogBatchHeader,
    pub records: Vec<BatchRecord>,
    pub trailer: u128,
}

pub fn encode_batch(content: &BatchContent) -> Result<Bytes, LogError> {
    if content.stream.node_id.is_empty() || content.seq == 0 || content.epoch == 0 {
        return Err(LogError::Invalid(
            "stream identity, epoch, and seq must be set".into(),
        ));
    }
    if content.epoch_start_writer.is_some() && !content.records.is_empty() {
        return Err(LogError::Invalid(
            "EpochStart cannot contain partition records".into(),
        ));
    }
    if content.epoch_start_writer.is_none() && content.records.is_empty() {
        return Err(LogError::Invalid("ordinary batches require records".into()));
    }
    let mut grouped = BTreeMap::<u32, Vec<&Bytes>>::new();
    for record in &content.records {
        grouped
            .entry(record.partition)
            .or_default()
            .push(&record.bytes);
    }
    let mut frames = Vec::with_capacity(grouped.len());
    let mut indexes = Vec::with_capacity(grouped.len());
    for (partition, records) in grouped {
        let mut plain = BytesMut::new();
        for record in &records {
            prost::encoding::encode_varint(record.len() as u64, &mut plain);
            plain.extend_from_slice(record);
        }
        let compressed = zstd::stream::encode_all(Cursor::new(&plain), 3).map_err(|error| {
            LogError::Invalid(format!("compress partition {partition}: {error}"))
        })?;
        indexes.push(LogPartitionFrame {
            partition,
            record_count: u32::try_from(records.len())
                .map_err(|_| LogError::Invalid("too many records in partition".into()))?,
            byte_offset: 0,
            compressed_length: compressed.len() as u64,
            uncompressed_length: plain.len() as u64,
            compressed_xxh3_64: xxh3_64(&compressed),
        });
        frames.push(compressed);
    }
    let mut header = LogBatchHeader {
        cluster_id: content.stream.cluster_id.to_vec(),
        node_id: content.stream.node_id.clone(),
        incarnation: content.stream.incarnation,
        epoch: content.epoch,
        seq: content.seq,
        record_count: u32::try_from(content.records.len())
            .map_err(|_| LogError::Invalid("too many batch records".into()))?,
        partition_frames: indexes,
        epoch_start: content
            .epoch_start_writer
            .as_ref()
            .map(|writer_process_id| LogEpochStart {
                epoch: content.epoch,
                writer_process_id: writer_process_id.clone(),
            }),
        sealed_at_unix_micros: content.sealed_at_unix_micros,
    };
    let mut header_length = header.encoded_len();
    loop {
        let mut offset = PREFIX_LENGTH + header_length;
        for (index, frame) in header.partition_frames.iter_mut().zip(&frames) {
            index.byte_offset = offset as u64;
            offset = offset
                .checked_add(frame.len())
                .ok_or_else(|| LogError::Invalid("batch too large".into()))?;
        }
        let next_length = header.encoded_len();
        if next_length == header_length {
            break;
        }
        header_length = next_length;
    }
    if header_length > MAX_HEADER_LENGTH {
        return Err(LogError::Invalid("batch header too large".into()));
    }
    let mut output = BytesMut::with_capacity(
        PREFIX_LENGTH + header_length + frames.iter().map(Vec::len).sum::<usize>() + TRAILER_LENGTH,
    );
    output.extend_from_slice(MAGIC);
    output.put_u16_le(FORMAT_VERSION);
    output.put_u32_le(header_length as u32);
    header
        .encode(&mut output)
        .map_err(|error| LogError::Invalid(format!("encode header: {error}")))?;
    for frame in frames {
        output.extend_from_slice(&frame);
    }
    let trailer = xxh3_128(&output);
    output.extend_from_slice(&trailer.to_le_bytes());
    Ok(output.freeze())
}

pub fn decode_batch(body: &[u8]) -> Result<DecodedBatch, LogError> {
    let header = decode_header(body)?;
    let frames_end = validate_header(&header, body.len() as u64)?;
    if frames_end as usize + TRAILER_LENGTH != body.len() {
        return Err(LogError::Corrupt(
            "trailer is not immediately after frames".into(),
        ));
    }
    let stored_trailer = u128::from_le_bytes(
        body[body.len() - TRAILER_LENGTH..]
            .try_into()
            .map_err(|_| LogError::Corrupt("missing trailer".into()))?,
    );
    if stored_trailer != xxh3_128(&body[..body.len() - TRAILER_LENGTH]) {
        return Err(LogError::Corrupt("batch trailer checksum differs".into()));
    }
    let mut records = Vec::with_capacity(header.record_count as usize);
    for index in &header.partition_frames {
        let start = index.byte_offset as usize;
        let end = start + index.compressed_length as usize;
        records.extend(decode_frame(index, &body[start..end])?);
    }
    Ok(DecodedBatch {
        header,
        records,
        trailer: stored_trailer,
    })
}

pub async fn read_partition_records(
    store: &dyn ObjectStore,
    key: &str,
    partition: u32,
) -> Result<Option<(LogBatchHeader, Vec<BatchRecord>)>, LogError> {
    let Some(prefix) = store
        .get_range(
            key,
            ByteRange {
                start: 0,
                end_inclusive: (PREFIX_LENGTH + MAX_HEADER_LENGTH - 1) as u64,
            },
        )
        .await?
    else {
        return Ok(None);
    };
    let header = decode_header(&prefix.bytes)?;
    validate_header(&header, prefix.object_size)?;
    let Some(index) = header
        .partition_frames
        .iter()
        .find(|frame| frame.partition == partition)
    else {
        return Ok(Some((header, Vec::new())));
    };
    let Some(frame) = store
        .get_range(
            key,
            ByteRange {
                start: index.byte_offset,
                end_inclusive: index.byte_offset + index.compressed_length - 1,
            },
        )
        .await?
    else {
        return Err(LogError::Corrupt(
            "frame disappeared after header read".into(),
        ));
    };
    if frame.object_size != prefix.object_size || frame.etag != prefix.etag {
        return Err(LogError::Corrupt(
            "object changed between range reads".into(),
        ));
    }
    let records = decode_frame(index, &frame.bytes)?;
    Ok(Some((header, records)))
}

fn decode_header(body: &[u8]) -> Result<LogBatchHeader, LogError> {
    if body.len() < PREFIX_LENGTH || &body[..8] != MAGIC {
        return Err(LogError::Corrupt(
            "invalid batch magic or short prefix".into(),
        ));
    }
    if u16::from_le_bytes([body[8], body[9]]) != FORMAT_VERSION {
        return Err(LogError::Corrupt("unsupported batch format version".into()));
    }
    let length = u32::from_le_bytes(
        body[10..14]
            .try_into()
            .map_err(|_| LogError::Corrupt("short header length".into()))?,
    ) as usize;
    if length > MAX_HEADER_LENGTH || PREFIX_LENGTH + length > body.len() {
        return Err(LogError::Corrupt(
            "invalid or truncated batch header".into(),
        ));
    }
    LogBatchHeader::decode(&body[PREFIX_LENGTH..PREFIX_LENGTH + length])
        .map_err(|error| LogError::Corrupt(format!("decode batch header: {error}")))
}

fn validate_header(header: &LogBatchHeader, object_size: u64) -> Result<u64, LogError> {
    if header.cluster_id.len() != 16
        || header.node_id.is_empty()
        || header.seq == 0
        || header.epoch == 0
    {
        return Err(LogError::Corrupt(
            "invalid stream identity or position".into(),
        ));
    }
    if let Some(epoch_start) = &header.epoch_start {
        if epoch_start.epoch != header.epoch
            || !header.partition_frames.is_empty()
            || header.record_count != 0
        {
            return Err(LogError::Corrupt("invalid EpochStart batch".into()));
        }
    } else if header.record_count == 0 {
        return Err(LogError::Corrupt("ordinary batch has no records".into()));
    }
    let header_length = header.encoded_len();
    let mut next_offset = (PREFIX_LENGTH + header_length) as u64;
    let mut previous_partition = None;
    let mut record_count = 0_u64;
    for index in &header.partition_frames {
        if previous_partition.is_some_and(|previous| index.partition <= previous)
            || index.byte_offset != next_offset
            || index.compressed_length == 0
            || index.uncompressed_length > MAX_UNCOMPRESSED_FRAME_LENGTH
            || u64::from(index.record_count) > index.uncompressed_length
        {
            return Err(LogError::Corrupt("invalid partition frame index".into()));
        }
        next_offset = next_offset
            .checked_add(index.compressed_length)
            .ok_or_else(|| LogError::Corrupt("frame offset overflow".into()))?;
        record_count += u64::from(index.record_count);
        previous_partition = Some(index.partition);
    }
    if record_count != u64::from(header.record_count)
        || next_offset.checked_add(TRAILER_LENGTH as u64) != Some(object_size)
    {
        return Err(LogError::Corrupt(
            "batch count or size differs from header".into(),
        ));
    }
    Ok(next_offset)
}

fn decode_frame(
    index: &LogPartitionFrame,
    compressed: &[u8],
) -> Result<Vec<BatchRecord>, LogError> {
    if compressed.len() as u64 != index.compressed_length
        || xxh3_64(compressed) != index.compressed_xxh3_64
    {
        return Err(LogError::Corrupt(format!(
            "partition {} frame checksum differs",
            index.partition
        )));
    }
    let decoder = zstd::stream::read::Decoder::new(Cursor::new(compressed)).map_err(|error| {
        LogError::Corrupt(format!("open partition {}: {error}", index.partition))
    })?;
    let mut plain = Vec::new();
    decoder
        .take(index.uncompressed_length + 1)
        .read_to_end(&mut plain)
        .map_err(|error| {
            LogError::Corrupt(format!("decompress partition {}: {error}", index.partition))
        })?;
    if plain.len() as u64 != index.uncompressed_length {
        return Err(LogError::Corrupt(format!(
            "partition {} uncompressed length differs",
            index.partition
        )));
    }
    let mut remaining = plain.as_slice();
    let mut records = Vec::with_capacity(index.record_count as usize);
    while !remaining.is_empty() {
        let length = prost::encoding::decode_varint(&mut remaining).map_err(|error| {
            LogError::Corrupt(format!(
                "partition {} record length: {error}",
                index.partition
            ))
        })?;
        let length = usize::try_from(length)
            .map_err(|_| LogError::Corrupt("record length overflows usize".into()))?;
        if length > remaining.remaining() {
            return Err(LogError::Corrupt("record exceeds partition frame".into()));
        }
        records.push(BatchRecord {
            partition: index.partition,
            bytes: Bytes::copy_from_slice(&remaining[..length]),
        });
        remaining.advance(length);
    }
    if records.len() != index.record_count as usize {
        return Err(LogError::Corrupt("partition record count differs".into()));
    }
    Ok(records)
}
