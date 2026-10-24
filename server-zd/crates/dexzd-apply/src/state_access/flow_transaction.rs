// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;

use dexzd_keyspace::row_keys::history_row_metadata_key;
use dexzd_keyspace::{ColumnFamily, RunTable, parse_run_key};
use dexzd_protocol::dexzd::v1::HistoryRowMetadata;
use prost::Message;

use crate::state_access::StateRow;
use crate::{ApplyContext, ApplyError, FlowStateReader, StateWrite};

/// Rows fetched from the reader per page while merging a scan with buffered writes.
const SCAN_PAGE_ROWS: usize = 256;

/// One buffered row change with its committed value.
pub(crate) struct StateRowChange {
    pub(crate) column_family: ColumnFamily,
    pub(crate) key: Vec<u8>,
    pub(crate) previous: Option<Vec<u8>>,
    pub(crate) current: Option<Vec<u8>>,
}

/// Buffered writes of one apply call on top of a [`FlowStateReader`].
///
/// Reads see the buffered writes. [`Self::into_writes`] returns the writes in a deterministic
/// order: range deletes in call order, then row writes in (column family, key) order.
pub(crate) struct FlowTransaction<'reader> {
    reader: &'reader dyn FlowStateReader,
    context: ApplyContext,
    rows: BTreeMap<(ColumnFamily, Vec<u8>), Option<Vec<u8>>>,
    range_deletes: Vec<(ColumnFamily, Vec<u8>, Vec<u8>)>,
}

impl<'reader> FlowTransaction<'reader> {
    pub(crate) fn new(reader: &'reader dyn FlowStateReader, context: ApplyContext) -> Self {
        Self {
            reader,
            context,
            rows: BTreeMap::new(),
            range_deletes: Vec::new(),
        }
    }

    pub(crate) fn context(&self) -> ApplyContext {
        self.context
    }

    /// Returns the row at `key`, including buffered writes.
    pub(crate) fn get(
        &self,
        column_family: ColumnFamily,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, ApplyError> {
        self.check_key_scope(column_family, key);
        if let Some(buffered) = self.rows.get(&(column_family, key.to_vec())) {
            return Ok(buffered.clone());
        }
        if self.is_range_deleted(column_family, key) {
            return Ok(None);
        }
        Ok(self.reader.get(column_family, key)?)
    }

    /// Returns up to `limit` rows with keys in `[start, end)`, ascending, including buffered
    /// writes.
    pub(crate) fn scan(
        &self,
        column_family: ColumnFamily,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<StateRow>, ApplyError> {
        self.check_key_scope(column_family, start);
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut merged: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut page_start = start.to_vec();
        let page_limit = limit.min(SCAN_PAGE_ROWS);
        loop {
            let page = self
                .reader
                .scan(column_family, &page_start, end, page_limit)?;
            let is_last_page = page.len() < page_limit;
            for (key, value) in page {
                page_start = key.clone();
                page_start.push(0);
                let is_overwritten = self.rows.contains_key(&(column_family, key.clone()));
                if !is_overwritten && !self.is_range_deleted(column_family, &key) {
                    merged.insert(key, value);
                }
            }
            // Buffered rows can only remove reader rows, so stop once enough survived.
            if is_last_page || merged.len() >= limit {
                break;
            }
        }
        let buffered = self
            .rows
            .range((column_family, start.to_vec())..(column_family, end.to_vec()))
            .filter_map(|((_, key), value)| {
                value.as_ref().map(|value| (key.clone(), value.clone()))
            });
        merged.extend(buffered);
        Ok(merged.into_iter().take(limit).collect())
    }

    pub(crate) fn put(&mut self, column_family: ColumnFamily, key: Vec<u8>, value: Vec<u8>) {
        self.check_key_scope(column_family, &key);
        if column_family == ColumnFamily::History {
            let metadata_key = metadata_key_for_history_row(&key);
            let row_size_bytes = u64::try_from(key.len() + value.len())
                .expect("a stored history row fits in a u64 byte count");
            let buffered_metadata = self
                .rows
                .get(&(ColumnFamily::Flow, metadata_key.clone()))
                .and_then(Option::as_ref)
                .map(|bytes| {
                    HistoryRowMetadata::decode(bytes.as_slice())
                        .expect("buffered history metadata is valid")
                })
                .unwrap_or_default();
            self.rows.insert(
                (ColumnFamily::Flow, metadata_key),
                Some(
                    HistoryRowMetadata {
                        row_size_bytes,
                        ..buffered_metadata
                    }
                    .encode_to_vec(),
                ),
            );
        }
        self.rows.insert((column_family, key), Some(value));
    }

    pub(crate) fn delete(&mut self, column_family: ColumnFamily, key: Vec<u8>) {
        self.check_key_scope(column_family, &key);
        if column_family == ColumnFamily::History {
            self.rows.insert(
                (ColumnFamily::Flow, metadata_key_for_history_row(&key)),
                None,
            );
        }
        self.rows.insert((column_family, key), None);
    }

    /// Deletes every key in `[start, end)`, including rows buffered earlier in this call.
    pub(crate) fn delete_range(
        &mut self,
        column_family: ColumnFamily,
        start: Vec<u8>,
        end: Vec<u8>,
    ) {
        self.check_key_scope(column_family, &start);
        let buffered: Vec<_> = self
            .rows
            .range((column_family, start.clone())..(column_family, end.clone()))
            .map(|(row_key, _)| row_key.clone())
            .collect();
        for row_key in buffered {
            self.rows.remove(&row_key);
        }
        self.range_deletes.push((column_family, start, end));
    }

    /// Decodes the protobuf row at `key`.
    pub(crate) fn get_message<RowMessage: Message + Default>(
        &self,
        column_family: ColumnFamily,
        key: &[u8],
        table: &'static str,
    ) -> Result<Option<RowMessage>, ApplyError> {
        let Some(bytes) = self.get(column_family, key)? else {
            return Ok(None);
        };
        RowMessage::decode(bytes.as_slice())
            .map(Some)
            .map_err(|error| corrupt_row(table, key, &error))
    }

    /// Decodes every protobuf row with keys in `[start, end)`, up to `limit` rows.
    pub(crate) fn scan_messages<RowMessage: Message + Default>(
        &self,
        column_family: ColumnFamily,
        start: &[u8],
        end: &[u8],
        limit: usize,
        table: &'static str,
    ) -> Result<Vec<(Vec<u8>, RowMessage)>, ApplyError> {
        self.scan(column_family, start, end, limit)?
            .into_iter()
            .map(|(key, bytes)| {
                RowMessage::decode(bytes.as_slice())
                    .map(|row| (key.clone(), row))
                    .map_err(|error| corrupt_row(table, &key, &error))
            })
            .collect()
    }

    pub(crate) fn put_message<RowMessage: Message>(
        &mut self,
        column_family: ColumnFamily,
        key: Vec<u8>,
        row: &RowMessage,
    ) {
        self.put(column_family, key, row.encode_to_vec());
    }

    /// Returns buffered changes beneath one run prefix with their committed values.
    pub(crate) fn changed_run_rows(
        &self,
        run_prefix: &[u8],
    ) -> Result<Vec<StateRowChange>, ApplyError> {
        let mut changes = BTreeMap::new();
        for ((column_family, key), current) in &self.rows {
            if *column_family == ColumnFamily::Flow && key.starts_with(run_prefix) {
                changes.insert(
                    (*column_family, key.clone()),
                    StateRowChange {
                        column_family: *column_family,
                        key: key.clone(),
                        previous: self.reader.get(*column_family, key)?,
                        current: current.clone(),
                    },
                );
            }
        }
        for (column_family, start, end) in &self.range_deletes {
            if *column_family != ColumnFamily::Flow {
                continue;
            }
            let mut page_start = start.clone();
            loop {
                let page = self
                    .reader
                    .scan(*column_family, &page_start, end, SCAN_PAGE_ROWS)?;
                if page.is_empty() {
                    break;
                }
                for (key, previous) in page {
                    page_start = key.clone();
                    page_start.push(0);
                    if key.starts_with(run_prefix) {
                        changes
                            .entry((*column_family, key.clone()))
                            .or_insert(StateRowChange {
                                column_family: *column_family,
                                key,
                                previous: Some(previous),
                                current: None,
                            });
                    }
                }
            }
        }
        Ok(changes.into_values().collect())
    }

    /// Returns individual buffered row writes under one table prefix with their committed values.
    pub(crate) fn buffered_row_changes_with_prefix(
        &self,
        column_family: ColumnFamily,
        prefix: &[u8],
    ) -> Result<Vec<StateRowChange>, ApplyError> {
        let (start, end) = dexzd_keyspace::row_keys::prefix_range(prefix);
        self.rows
            .range((column_family, start)..(column_family, end))
            .map(|((_, key), current)| {
                Ok(StateRowChange {
                    column_family,
                    key: key.clone(),
                    previous: self.reader.get(column_family, key)?,
                    current: current.clone(),
                })
            })
            .collect()
    }

    /// Returns the rows this call put in `column_family`, in key order.
    pub(crate) fn buffered_puts(
        &self,
        column_family: ColumnFamily,
    ) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.rows
            .range((column_family, Vec::new())..)
            .take_while(move |((family, _), _)| *family == column_family)
            .filter_map(|((_, key), value)| value.as_deref().map(|value| (key.as_slice(), value)))
    }

    /// Returns the buffered writes in deterministic order.
    pub(crate) fn into_writes(self) -> Vec<StateWrite> {
        let range_deletes = self
            .range_deletes
            .into_iter()
            .map(|(column_family, start, end)| StateWrite::DeleteRange {
                column_family,
                start,
                end,
            });
        let rows = self
            .rows
            .into_iter()
            .map(|((column_family, key), value)| match value {
                Some(value) => StateWrite::Put {
                    column_family,
                    key,
                    value,
                },
                None => StateWrite::Delete { column_family, key },
            });
        range_deletes.chain(rows).collect()
    }

    fn is_range_deleted(&self, column_family: ColumnFamily, key: &[u8]) -> bool {
        self.range_deletes
            .iter()
            .any(|(deleted_family, start, end)| {
                *deleted_family == column_family && key >= start.as_slice() && key < end.as_slice()
            })
    }

    /// Apply may touch only its Flow's keys and its partition's indexes; anything else is a
    /// programming error that would break partition isolation.
    fn check_key_scope(&self, column_family: ColumnFamily, key: &[u8]) {
        match column_family {
            ColumnFamily::Flow | ColumnFamily::History | ColumnFamily::Value => assert!(
                key.starts_with(&self.context.flow_hash.to_be_bytes()),
                "apply accessed a {column_family:?} key outside its Flow"
            ),
            ColumnFamily::PartitionIndex => assert!(
                key.starts_with(&self.context.partition.to_be_bytes()),
                "apply accessed a partition index key outside its partition"
            ),
            ColumnFamily::PartitionMeta | ColumnFamily::NodeMeta => {
                panic!("apply must not access {column_family:?}; the partition applier owns it")
            }
        }
    }
}

fn metadata_key_for_history_row(key: &[u8]) -> Vec<u8> {
    let parsed = parse_run_key(key).expect("history writes use valid run keys");
    let (seq, attempt) = match parsed.table {
        RunTable::HistoryRecord => {
            assert_eq!(
                parsed.suffix.len(),
                8,
                "history records have eight-byte seqs"
            );
            (
                u64::from_be_bytes(parsed.suffix.try_into().expect("eight-byte seq")),
                0,
            )
        }
        RunTable::Checkpoint => {
            assert_eq!(parsed.suffix.len(), 12, "checkpoints have attempt and seq");
            (
                u64::from_be_bytes(parsed.suffix[4..].try_into().expect("eight-byte seq")),
                u32::from_be_bytes(parsed.suffix[..4].try_into().expect("four-byte attempt")),
            )
        }
        _ => panic!("the History column family stores only records and checkpoints"),
    };
    history_row_metadata_key(parsed.flow_hash, parsed.run_id, seq, parsed.table, attempt)
}

fn corrupt_row(table: &'static str, key: &[u8], error: &prost::DecodeError) -> ApplyError {
    ApplyError::CorruptState {
        table,
        key: key.iter().map(|byte| format!("{byte:02x}")).collect(),
        detail: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use dexzd_keyspace::row_keys::{history_record_key, history_row_metadata_key};
    use dexzd_keyspace::{ColumnFamily, FlowHash, PartitionId, RunId, RunTable};
    use dexzd_protocol::dexzd::v1::{HistoryRecord, HistoryRowMetadata};
    use prost::Message;

    use super::FlowTransaction;
    use crate::{ApplyContext, FlowStateReader, StateReadError, StateRow};

    struct EmptyState;

    impl FlowStateReader for EmptyState {
        fn get(
            &self,
            _column_family: ColumnFamily,
            _key: &[u8],
        ) -> Result<Option<Vec<u8>>, StateReadError> {
            Ok(None)
        }

        fn scan(
            &self,
            _column_family: ColumnFamily,
            _start: &[u8],
            _end: &[u8],
            _limit: usize,
        ) -> Result<Vec<StateRow>, StateReadError> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn absorbing_history_after_metadata_preserves_semantic_flags() {
        let flow_hash = FlowHash(7);
        let run_id = RunId([3; 16]);
        let mut transaction = FlowTransaction::new(
            &EmptyState,
            ApplyContext {
                partition: PartitionId(0),
                flow_hash,
            },
        );
        let history_key = history_record_key(flow_hash, run_id, 9);
        let metadata_key =
            history_row_metadata_key(flow_hash, run_id, 9, RunTable::HistoryRecord, 0);
        let metadata = HistoryRowMetadata {
            is_semantic: true,
            is_external_input: true,
            attempt: 3,
            ..HistoryRowMetadata::default()
        };
        let record = HistoryRecord {
            seq: 9,
            attempt: 3,
            is_semantic: true,
            is_external_input: true,
            ..HistoryRecord::default()
        };
        let encoded_record = record.encode_to_vec();
        transaction.put(
            ColumnFamily::Flow,
            metadata_key.clone(),
            metadata.encode_to_vec(),
        );
        transaction.put(
            ColumnFamily::History,
            history_key.clone(),
            encoded_record.clone(),
        );
        let buffered: HistoryRowMetadata = transaction
            .get_message(ColumnFamily::Flow, &metadata_key, "history metadata")
            .expect("metadata decodes")
            .expect("metadata remains buffered");
        assert!(buffered.is_semantic);
        assert!(buffered.is_external_input);
        assert_eq!(buffered.attempt, 3);
        assert_eq!(
            buffered.row_size_bytes,
            (history_key.len() + encoded_record.len()) as u64
        );
    }
}
