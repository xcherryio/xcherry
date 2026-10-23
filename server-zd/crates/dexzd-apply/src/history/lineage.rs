// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Attempt lineages and history record scans.
//!
//! Attempts cover disjoint, contiguous seq ranges because only the current attempt receives
//! commands. The lineage of attempt `a` is `a` itself plus every ancestor up to the point it
//! was forked from.

use dexzd_keyspace::row_keys::{history_record_key, prefix_range};
use dexzd_keyspace::{ColumnFamily, RunTable, run_table_prefix};
use dexzd_protocol::dexzd::v1::{AttemptRecord, HistoryRecord};

use crate::ApplyError;
use crate::state_access::{FlowTransaction, RunRows};

/// History records read per scan page.
const HISTORY_PAGE_RECORDS: usize = 64;

/// The attempts of one lineage, from the selected attempt back to attempt 1, each with the
/// largest seq that belongs to the lineage.
#[derive(Debug, Clone)]
pub(crate) struct Lineage {
    segments: Vec<(u32, u64)>,
}

impl Lineage {
    /// Builds the lineage of `attempt` from the attempt records.
    pub(crate) fn of_attempt(
        transaction: &FlowTransaction<'_>,
        rows: RunRows,
        attempt: u32,
    ) -> Result<Self, ApplyError> {
        let mut segments = Vec::new();
        let mut current = attempt;
        let mut max_seq = u64::MAX;
        while current != 0 {
            let record = required_attempt_record(transaction, rows, current)?;
            segments.push((current, max_seq));
            if record.parent_attempt >= current {
                return Err(ApplyError::InvariantViolation {
                    detail: format!(
                        "run attempt {current} has parent attempt {}",
                        record.parent_attempt
                    ),
                });
            }
            max_seq = record.parent_seq;
            current = record.parent_attempt;
        }
        Ok(Self { segments })
    }

    pub(crate) fn contains(&self, attempt: u32, seq: u64) -> bool {
        self.segments
            .iter()
            .any(|(segment_attempt, max_seq)| *segment_attempt == attempt && seq <= *max_seq)
    }
}

pub(crate) fn required_attempt_record(
    transaction: &FlowTransaction<'_>,
    rows: RunRows,
    attempt: u32,
) -> Result<AttemptRecord, ApplyError> {
    rows.attempt_record(transaction, attempt)?
        .ok_or_else(|| ApplyError::InvariantViolation {
            detail: format!("run attempt {attempt} has no attempt record"),
        })
}

/// Seq of the FlowStart command, the genesis point.
pub(crate) fn genesis_seq(
    transaction: &FlowTransaction<'_>,
    rows: RunRows,
) -> Result<u64, ApplyError> {
    Ok(required_attempt_record(transaction, rows, 1)?.start_seq)
}

/// Returns up to one page of history records with seq >= `from_seq`, ascending.
pub(crate) fn history_page(
    transaction: &FlowTransaction<'_>,
    rows: RunRows,
    from_seq: u64,
) -> Result<Vec<HistoryRecord>, ApplyError> {
    let start = history_record_key(rows.flow_hash, rows.run_id, from_seq);
    let prefix = run_table_prefix(rows.flow_hash, rows.run_id, RunTable::HistoryRecord);
    let (_, end) = prefix_range(&prefix);
    Ok(transaction
        .scan_messages(
            ColumnFamily::History,
            &start,
            &end,
            HISTORY_PAGE_RECORDS,
            "history record",
        )?
        .into_iter()
        .map(|(_, record)| record)
        .collect())
}

/// Reads every history record with seq in `[from_seq, to_seq]`, ascending.
pub(crate) fn history_records(
    transaction: &FlowTransaction<'_>,
    rows: RunRows,
    from_seq: u64,
    to_seq: u64,
) -> Result<Vec<HistoryRecord>, ApplyError> {
    let mut records = Vec::new();
    let mut next_seq = from_seq;
    while next_seq <= to_seq {
        let page = history_page(transaction, rows, next_seq)?;
        let is_last_page = page.len() < HISTORY_PAGE_RECORDS;
        for record in page {
            if record.seq > to_seq {
                return Ok(records);
            }
            next_seq = record.seq.saturating_add(1);
            records.push(record);
        }
        if is_last_page {
            break;
        }
    }
    Ok(records)
}
