// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Uncommitted commands of one Flow, layered over committed state (plan 6.4).

use std::collections::{BTreeMap, VecDeque};

use dexzd_apply::{ApplyOutcome, FlowStateReader, StateReadError, StateRow, StateWrite};
use dexzd_keyspace::ColumnFamily;
use prost::Message;

/// One proposed, uncommitted command and its speculative outcome.
#[derive(Debug, Clone)]
pub(crate) struct OverlayEntry {
    pub(crate) command_id: Vec<u8>,
    pub(crate) outcome: ApplyOutcome,
    pub(crate) size_bytes: usize,
}

impl OverlayEntry {
    pub(crate) fn new(command_id: Vec<u8>, outcome: ApplyOutcome) -> Self {
        let size_bytes = outcome.writes.iter().map(write_size_bytes).sum::<usize>()
            + outcome.reply.encoded_len();
        Self {
            command_id,
            outcome,
            size_bytes,
        }
    }
}

/// Speculative outcomes of uncommitted commands, oldest first.
#[derive(Debug, Default)]
pub(crate) struct SpeculativeOverlay {
    entries: VecDeque<OverlayEntry>,
    total_bytes: usize,
}

/// Result of matching a committed command against the overlay.
#[derive(Debug)]
pub(crate) enum CommittedMatch {
    /// The oldest entry belongs to the committed command; it was removed.
    Head(OverlayEntry),
    /// The command was never proposed through this overlay (for example, replayed in recovery).
    Foreign,
    /// The command is in the overlay but not at its head: commit order differs from proposal
    /// order.
    OutOfOrder,
}

impl SpeculativeOverlay {
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    pub(crate) fn push(&mut self, entry: OverlayEntry) {
        self.total_bytes += entry.size_bytes;
        self.entries.push_back(entry);
    }

    /// Removes the head entry when it belongs to `command_id`.
    pub(crate) fn take_committed(&mut self, command_id: &[u8]) -> CommittedMatch {
        match self.entries.front() {
            Some(head) if head.command_id == command_id => {
                let entry = self.entries.pop_front().expect("front exists");
                self.total_bytes -= entry.size_bytes;
                CommittedMatch::Head(entry)
            }
            _ if self
                .entries
                .iter()
                .any(|entry| entry.command_id == command_id) =>
            {
                CommittedMatch::OutOfOrder
            }
            _ => CommittedMatch::Foreign,
        }
    }

    /// Drops every entry and returns the bytes they held.
    pub(crate) fn clear(&mut self) -> usize {
        self.entries.clear();
        std::mem::take(&mut self.total_bytes)
    }

    /// Returns a reader that layers the overlay over `committed`.
    pub(crate) fn reader<'a>(
        &'a self,
        committed: &'a dyn FlowStateReader,
    ) -> OverlayStateReader<'a> {
        OverlayStateReader {
            overlay: self,
            committed,
        }
    }

    /// Returns the newest overlay decision for `key`.
    fn resolve(&self, column_family: ColumnFamily, key: &[u8]) -> OverlayResolution<'_> {
        for entry in self.entries.iter().rev() {
            for write in entry.outcome.writes.iter().rev() {
                match write {
                    StateWrite::Put {
                        column_family: family,
                        key: written,
                        value,
                    } if *family == column_family && written.as_slice() == key => {
                        return OverlayResolution::Present(value);
                    }
                    StateWrite::Delete {
                        column_family: family,
                        key: written,
                    } if *family == column_family && written.as_slice() == key => {
                        return OverlayResolution::Deleted;
                    }
                    StateWrite::DeleteRange {
                        column_family: family,
                        start,
                        end,
                    } if *family == column_family
                        && key >= start.as_slice()
                        && key < end.as_slice() =>
                    {
                        return OverlayResolution::Deleted;
                    }
                    _ => {}
                }
            }
        }
        OverlayResolution::Untouched
    }

    /// Returns the rows in `[start, end)` that the overlay itself provides.
    fn present_rows(
        &self,
        column_family: ColumnFamily,
        start: &[u8],
        end: &[u8],
    ) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let mut rows = BTreeMap::new();
        for entry in &self.entries {
            for write in &entry.outcome.writes {
                if let StateWrite::Put {
                    column_family: family,
                    key,
                    ..
                } = write
                    && *family == column_family
                    && key.as_slice() >= start
                    && key.as_slice() < end
                    && !rows.contains_key(key)
                    && let OverlayResolution::Present(value) = self.resolve(column_family, key)
                {
                    rows.insert(key.clone(), value.clone());
                }
            }
        }
        rows
    }
}

enum OverlayResolution<'a> {
    Untouched,
    Present(&'a Vec<u8>),
    Deleted,
}

/// A [`FlowStateReader`] that sees committed state plus every uncommitted overlay entry.
pub(crate) struct OverlayStateReader<'a> {
    overlay: &'a SpeculativeOverlay,
    committed: &'a dyn FlowStateReader,
}

impl FlowStateReader for OverlayStateReader<'_> {
    fn get(
        &self,
        column_family: ColumnFamily,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StateReadError> {
        match self.overlay.resolve(column_family, key) {
            OverlayResolution::Present(value) => Ok(Some(value.clone())),
            OverlayResolution::Deleted => Ok(None),
            OverlayResolution::Untouched => self.committed.get(column_family, key),
        }
    }

    fn scan(
        &self,
        column_family: ColumnFamily,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<StateRow>, StateReadError> {
        let mut rows = Vec::new();
        if limit == 0 || start >= end {
            return Ok(rows);
        }
        let mut overlay_rows = self
            .overlay
            .present_rows(column_family, start, end)
            .into_iter()
            .peekable();
        let mut page_start = start.to_vec();
        let mut is_committed_exhausted = false;
        while rows.len() < limit && !is_committed_exhausted {
            let page = self
                .committed
                .scan(column_family, &page_start, end, limit)?;
            is_committed_exhausted = page.len() < limit;
            for (key, value) in page {
                page_start.clone_from(&key);
                page_start.push(0);
                while rows.len() < limit
                    && overlay_rows
                        .peek()
                        .is_some_and(|(overlay_key, _)| *overlay_key < key)
                {
                    rows.push(overlay_rows.next().expect("peeked"));
                }
                if rows.len() >= limit {
                    break;
                }
                // Overlay rows with this key are emitted from `overlay_rows` instead.
                if matches!(
                    self.overlay.resolve(column_family, &key),
                    OverlayResolution::Untouched
                ) {
                    rows.push((key, value));
                }
            }
        }
        while rows.len() < limit {
            let Some(row) = overlay_rows.next() else {
                break;
            };
            rows.push(row);
        }
        Ok(rows)
    }
}

fn write_size_bytes(write: &StateWrite) -> usize {
    match write {
        StateWrite::Put { key, value, .. } => key.len() + value.len(),
        StateWrite::Delete { key, .. } => key.len(),
        StateWrite::DeleteRange { start, end, .. } => start.len() + end.len(),
    }
}
