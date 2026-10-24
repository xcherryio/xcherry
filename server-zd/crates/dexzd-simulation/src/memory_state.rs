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

use crate::row_decoding::{RowDecodeError, reencode_row};

/// An ordered in-memory store that apply reads and the simulation commits to.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemoryFlowState {
    rows: BTreeMap<(ColumnFamily, Vec<u8>), Vec<u8>>,
}

impl MemoryFlowState {
    /// Applies the writes of one outcome in order.
    pub fn commit(&mut self, writes: &[StateWrite]) {
        for write in writes {
            match write {
                StateWrite::Put {
                    column_family,
                    key,
                    value,
                } => {
                    self.rows
                        .insert((*column_family, key.clone()), value.clone());
                }
                StateWrite::Delete { column_family, key } => {
                    self.rows.remove(&(*column_family, key.clone()));
                }
                StateWrite::DeleteRange {
                    column_family,
                    start,
                    end,
                } => {
                    let deleted_keys: Vec<_> = self
                        .rows
                        .range((*column_family, start.clone())..(*column_family, end.clone()))
                        .map(|(key, _)| key.clone())
                        .collect();
                    for key in deleted_keys {
                        self.rows.remove(&key);
                    }
                }
            }
        }
    }

    /// Commits `writes`, decoding each put value as its table's message and encoding it again.
    ///
    /// Returns the first row that does not decode; nothing is committed then.
    pub fn commit_redecoded(&mut self, writes: &[StateWrite]) -> Result<(), RowDecodeError> {
        let mut redecoded = Vec::with_capacity(writes.len());
        for write in writes {
            let write = match write {
                StateWrite::Put {
                    column_family,
                    key,
                    value,
                } => StateWrite::Put {
                    column_family: *column_family,
                    key: key.clone(),
                    value: reencode_row(*column_family, key, value)?,
                },
                other => other.clone(),
            };
            redecoded.push(write);
        }
        self.commit(&redecoded);
        Ok(())
    }

    /// Returns every row of `column_family` whose key starts with `prefix`, in key order.
    pub fn rows_with_prefix<'state>(
        &'state self,
        column_family: ColumnFamily,
        prefix: &'state [u8],
    ) -> impl Iterator<Item = (&'state [u8], &'state [u8])> + 'state {
        self.rows
            .range((column_family, prefix.to_vec())..)
            .take_while(move |((family, key), _)| {
                *family == column_family && key.starts_with(prefix)
            })
            .map(|((_, key), value)| (key.as_slice(), value.as_slice()))
    }

    /// Returns the row at `key`.
    pub fn row(&self, column_family: ColumnFamily, key: &[u8]) -> Option<&[u8]> {
        self.rows
            .get(&(column_family, key.to_vec()))
            .map(Vec::as_slice)
    }

    /// Number of stored rows.
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
}

impl FlowStateReader for MemoryFlowState {
    fn get(
        &self,
        column_family: ColumnFamily,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StateReadError> {
        Ok(self.row(column_family, key).map(<[u8]>::to_vec))
    }

    fn scan(
        &self,
        column_family: ColumnFamily,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<StateRow>, StateReadError> {
        if start >= end {
            return Ok(Vec::new());
        }
        Ok(self
            .rows
            .range((column_family, start.to_vec())..(column_family, end.to_vec()))
            .take(limit)
            .map(|((_, key), value)| (key.clone(), value.clone()))
            .collect())
    }
}
