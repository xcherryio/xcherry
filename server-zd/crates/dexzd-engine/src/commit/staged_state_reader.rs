// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;

use dexzd_apply::{FlowStateReader, StateReadError, StateRow, StateWrite};
use dexzd_keyspace::ColumnFamily;
use dexzd_storage::{LocalReadView, WriteBatch};

use crate::commit::committed_state_view::{read_row, scan_rows};

struct RangeTombstone {
    column_family: ColumnFamily,
    start: Vec<u8>,
    end: Vec<u8>,
}

/// Committed state plus the writes of earlier commands in the same partition frame.
pub(crate) struct StagedStateReader<'a> {
    base: &'a dyn LocalReadView,
    staged: BTreeMap<(ColumnFamily, Vec<u8>), Option<Vec<u8>>>,
    range_tombstones: Vec<RangeTombstone>,
    batch: WriteBatch,
}

impl<'a> StagedStateReader<'a> {
    pub(crate) fn new(base: &'a dyn LocalReadView) -> Self {
        Self {
            base,
            staged: BTreeMap::new(),
            range_tombstones: Vec::new(),
            batch: WriteBatch::new(),
        }
    }

    pub(crate) fn stage(&mut self, writes: &[StateWrite]) {
        for write in writes {
            match write {
                StateWrite::Put {
                    column_family,
                    key,
                    value,
                } => {
                    self.staged
                        .insert((*column_family, key.clone()), Some(value.clone()));
                    self.batch.put(*column_family, key.clone(), value.clone());
                }
                StateWrite::Delete { column_family, key } => {
                    self.staged.insert((*column_family, key.clone()), None);
                    self.batch.delete(*column_family, key.clone());
                }
                StateWrite::DeleteRange {
                    column_family,
                    start,
                    end,
                } => {
                    // An empty range deletes nothing; the stores reject it as invalid.
                    if start >= end {
                        continue;
                    }
                    for (_, value) in self
                        .staged
                        .range_mut((*column_family, start.clone())..(*column_family, end.clone()))
                    {
                        *value = None;
                    }
                    self.range_tombstones.push(RangeTombstone {
                        column_family: *column_family,
                        start: start.clone(),
                        end: end.clone(),
                    });
                    self.batch
                        .delete_range(*column_family, start.clone(), end.clone());
                }
            }
        }
    }

    pub(crate) fn into_batch(self) -> WriteBatch {
        self.batch
    }

    fn is_tombstoned(&self, column_family: ColumnFamily, key: &[u8]) -> bool {
        self.range_tombstones.iter().any(|tombstone| {
            tombstone.column_family == column_family
                && tombstone.start.as_slice() <= key
                && key < tombstone.end.as_slice()
        })
    }

    fn is_hidden_store_row(&self, column_family: ColumnFamily, key: &[u8]) -> bool {
        self.staged.contains_key(&(column_family, key.to_vec()))
            || self.is_tombstoned(column_family, key)
    }
}

impl FlowStateReader for StagedStateReader<'_> {
    fn get(
        &self,
        column_family: ColumnFamily,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StateReadError> {
        if let Some(value) = self.staged.get(&(column_family, key.to_vec())) {
            return Ok(value.clone());
        }
        if self.is_tombstoned(column_family, key) {
            return Ok(None);
        }
        read_row(self.base, column_family, key)
    }

    fn scan(
        &self,
        column_family: ColumnFamily,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<StateRow>, StateReadError> {
        if start >= end || limit == 0 {
            return Ok(Vec::new());
        }
        let mut merged = BTreeMap::new();
        let mut cursor = start.to_vec();
        let mut visible_store_rows = 0_usize;
        // Hidden store rows shorten a page, so keep reading until enough rows are visible.
        while visible_store_rows < limit {
            let page = scan_rows(self.base, column_family, &cursor, end, limit)?;
            let is_last_page = page.len() < limit;
            if let Some((last_key, _)) = page.last() {
                cursor.clone_from(last_key);
                cursor.push(0);
            }
            for (key, value) in page {
                if !self.is_hidden_store_row(column_family, &key) {
                    merged.insert(key, value);
                    visible_store_rows += 1;
                }
            }
            if is_last_page {
                break;
            }
        }
        for ((_, key), value) in self
            .staged
            .range((column_family, start.to_vec())..(column_family, end.to_vec()))
        {
            if let Some(value) = value {
                merged.insert(key.clone(), value.clone());
            }
        }
        Ok(merged.into_iter().take(limit).collect())
    }
}
