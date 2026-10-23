// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;

use dexzd_keyspace::ColumnFamily;
use prost::Message;

use crate::state_access::StateRow;
use crate::{ApplyContext, ApplyError, FlowStateReader, StateWrite};

/// Rows fetched from the reader per page while merging a scan with buffered writes.
const SCAN_PAGE_ROWS: usize = 256;

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
        let mut merged: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut page_start = start.to_vec();
        loop {
            let page = self
                .reader
                .scan(column_family, &page_start, end, SCAN_PAGE_ROWS)?;
            let is_last_page = page.len() < SCAN_PAGE_ROWS;
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
        self.rows.insert((column_family, key), Some(value));
    }

    pub(crate) fn delete(&mut self, column_family: ColumnFamily, key: Vec<u8>) {
        self.check_key_scope(column_family, &key);
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

    /// Returns the rows this call put in `column_family`, in key order.
    pub(crate) fn buffered_puts(
        &self,
        column_family: ColumnFamily,
    ) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.rows
            .range((column_family, Vec::new())..)
            .take_while(move |((family, _), _)| *family == column_family)
            .filter_map(|((_, key), value)| {
                value.as_deref().map(|value| (key.as_slice(), value))
            })
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

fn corrupt_row(table: &'static str, key: &[u8], error: &prost::DecodeError) -> ApplyError {
    ApplyError::CorruptState {
        table,
        key: key.iter().map(|byte| format!("{byte:02x}")).collect(),
        detail: error.to_string(),
    }
}
