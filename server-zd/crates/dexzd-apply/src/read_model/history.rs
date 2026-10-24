// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;

use dexzd_keyspace::row_keys::prefix_range;
use dexzd_keyspace::{ColumnFamily, FlowHash, RunId, RunTable, run_table_prefix};
use dexzd_protocol::dex::GetHistoryEventsResponse;
use dexzd_protocol::dexzd::v1::HistoryRecord;

use crate::FlowStateReader;
use crate::read_model::{ReadModelError, RunView};

/// Reads one page of events from an attempt's lineage, using event IDs as cursors.
pub fn get_history_events(
    reader: &dyn FlowStateReader,
    flow_hash: FlowHash,
    run_id: RunId,
    start_event_id: i64,
    page_size: i32,
    next_page_token: &[u8],
    run_attempt: i32,
) -> Result<GetHistoryEventsResponse, ReadModelError> {
    if start_event_id < 0 || page_size < 0 || run_attempt < 0 {
        return Err(ReadModelError::InvalidRequest {
            detail: "event ID, page size, and run attempt must be non-negative".to_owned(),
        });
    }
    let mut first_event_id = start_event_id.max(1);
    if !next_page_token.is_empty() {
        let token: [u8; 8] =
            next_page_token
                .try_into()
                .map_err(|_| ReadModelError::InvalidRequest {
                    detail: "history page token must contain one event ID".to_owned(),
                })?;
        let last_event_id = i64::from_be_bytes(token);
        if last_event_id < 1 {
            return Err(ReadModelError::InvalidRequest {
                detail: "history page token has an invalid event ID".to_owned(),
            });
        }
        first_event_id = first_event_id.max(last_event_id.saturating_add(1));
    }
    let view = RunView::open(reader, flow_hash, run_id)?;
    let selected_attempt = if run_attempt == 0 {
        view.record.current_attempt
    } else {
        u32::try_from(run_attempt).unwrap_or(u32::MAX)
    };
    let lineage = lineage_cutoffs(&view, selected_attempt)?;
    let prefix = run_table_prefix(flow_hash, run_id, RunTable::HistoryRecord);
    let (start, end) = prefix_range(&prefix);
    let records: Vec<(Vec<u8>, HistoryRecord)> = view.transaction.scan_messages(
        ColumnFamily::History,
        &start,
        &end,
        usize::MAX,
        "history record",
    )?;
    let mut events = Vec::new();
    for (_, record) in records {
        if !lineage
            .get(&record.attempt)
            .is_some_and(|cutoff| record.seq <= *cutoff)
        {
            continue;
        }
        for mut event in record.events {
            if event.event_id >= first_event_id {
                if event.run_attempt == 0 {
                    event.run_attempt = i32::try_from(record.attempt).unwrap_or(i32::MAX);
                }
                events.push(event);
            }
        }
    }
    events.sort_by_key(|event| event.event_id);
    let size = if page_size == 0 {
        100_usize
    } else {
        usize::try_from(page_size).unwrap_or(usize::MAX)
    };
    let has_more = events.len() > size;
    events.truncate(size);
    let next_internal_event_id = events
        .last()
        .map_or(first_event_id, |event| event.event_id.saturating_add(1));
    let next_page_token = if has_more {
        events
            .last()
            .map_or_else(Vec::new, |event| event.event_id.to_be_bytes().to_vec())
    } else {
        Vec::new()
    };
    Ok(GetHistoryEventsResponse {
        events,
        next_page_token,
        next_internal_event_id,
    })
}

fn lineage_cutoffs(
    view: &RunView<'_>,
    selected_attempt: u32,
) -> Result<BTreeMap<u32, u64>, ReadModelError> {
    let mut lineage = BTreeMap::new();
    let mut attempt = selected_attempt;
    let mut cutoff = u64::MAX;
    while attempt != 0 {
        if lineage.insert(attempt, cutoff).is_some() {
            return Err(ReadModelError::State(
                crate::ApplyError::InvariantViolation {
                    detail: "run attempt lineage contains a cycle".to_owned(),
                },
            ));
        }
        let Some(record) = view.rows.attempt_record(&view.transaction, attempt)? else {
            return Err(ReadModelError::InvalidRequest {
                detail: format!("run attempt {attempt} does not exist"),
            });
        };
        cutoff = record.parent_seq;
        attempt = record.parent_attempt;
    }
    Ok(lineage)
}
