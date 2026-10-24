// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeSet;

use dexzd_apply::{ApplyOutcome, StateWrite};
use dexzd_keyspace::row_keys::{decode_flow_run_after_prefix, prefix_range, run_singleton_key};
use dexzd_keyspace::{
    ColumnFamily, FlowHash, IndexKind, PartitionId, RunId, RunTable, TenantId, parse_run_key,
    partition_index_prefix, run_prefix,
};
use dexzd_protocol::dexzd::v1::{RunRecord, SearchDocumentRecord};
use prost::Message;

use super::projector::ProjectionError;

/// One committed search document change to project into ParadeDB.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SearchDocumentChange {
    /// Store the encoded document from a committed apply outcome.
    Upsert {
        /// Tenant owning the run.
        tenant_id: TenantId,
        /// Run being changed.
        run_id: RunId,
        /// Encoded SearchDocumentRecord.
        encoded_document: Vec<u8>,
    },
    /// Remove a purged run's document.
    Delete {
        /// Tenant owning the run.
        tenant_id: TenantId,
        /// Run being purged.
        run_id: RunId,
    },
}

/// Extracts ordered document writes and purge deletes from one committed apply outcome.
pub fn extract_search_document_changes(
    partition_id: PartitionId,
    outcome: &ApplyOutcome,
) -> Result<Vec<SearchDocumentChange>, ProjectionError> {
    let mut changes = Vec::new();
    let mut purged_runs = BTreeSet::new();
    let mut purging_runs = BTreeSet::new();
    let mut indexed_deletes = BTreeSet::new();
    for write in &outcome.writes {
        match write {
            StateWrite::Put {
                column_family: ColumnFamily::Flow,
                key,
                value,
            } => {
                if let Some((_, run_id)) = run_record_key(key)? {
                    let record = RunRecord::decode(value.as_slice()).map_err(|error| {
                        ProjectionError::InvalidState(format!(
                            "decode committed run record for {run_id}: {error}"
                        ))
                    })?;
                    if record.run_id != run_id.as_bytes() {
                        return Err(ProjectionError::InvalidState(format!(
                            "committed run record ID differs from key {run_id}"
                        )));
                    }
                    if record.is_purging {
                        purging_runs.insert((TenantId(record.tenant_id), run_id));
                    }
                }
                if let Some((_, run_id)) = search_document_key(key)? {
                    let record =
                        SearchDocumentRecord::decode(value.as_slice()).map_err(|error| {
                            ProjectionError::InvalidState(format!(
                                "decode committed search document for {run_id}: {error}"
                            ))
                        })?;
                    if record.run_id != run_id.as_bytes() {
                        return Err(ProjectionError::InvalidState(format!(
                            "committed search document run ID differs from key {run_id}"
                        )));
                    }
                    changes.push(SearchDocumentChange::Upsert {
                        tenant_id: TenantId(record.tenant_id),
                        run_id,
                        encoded_document: value.clone(),
                    });
                }
            }
            StateWrite::Delete {
                column_family: ColumnFamily::PartitionIndex,
                key,
            } => {
                if let Some((tenant_id, flow_hash, run_id)) =
                    decode_search_index_key(partition_id, key)?
                {
                    indexed_deletes.insert((flow_hash, run_id));
                    changes.push(SearchDocumentChange::Delete { tenant_id, run_id });
                }
            }
            StateWrite::DeleteRange {
                column_family: ColumnFamily::Flow,
                start,
                end,
            } => {
                if let Some(identity) = purged_run(start, end) {
                    purged_runs.insert(identity);
                }
            }
            _ => {}
        }
    }
    if !purged_runs.is_subset(&indexed_deletes) {
        return Err(ProjectionError::InvalidState(
            "run purge did not delete its SearchDocument index".into(),
        ));
    }
    changes.retain(|change| {
        !matches!(change, SearchDocumentChange::Upsert { tenant_id, run_id, .. }
            if purging_runs.contains(&(*tenant_id, *run_id)))
    });
    changes.extend(
        purging_runs
            .into_iter()
            .map(|(tenant_id, run_id)| SearchDocumentChange::Delete { tenant_id, run_id }),
    );
    Ok(changes)
}

/// Concatenates every outcome in one committed partition batch in apply order.
pub fn extract_batch_search_document_changes(
    partition_id: PartitionId,
    outcomes: &[ApplyOutcome],
) -> Result<Vec<SearchDocumentChange>, ProjectionError> {
    let mut changes = Vec::new();
    for outcome in outcomes {
        changes.extend(extract_search_document_changes(partition_id, outcome)?);
    }
    Ok(changes)
}

fn search_document_key(key: &[u8]) -> Result<Option<(FlowHash, RunId)>, ProjectionError> {
    let Ok(parsed) = parse_run_key(key) else {
        return Ok(None);
    };
    if parsed.table != RunTable::SearchDocument {
        return Ok(None);
    }
    if !parsed.suffix.is_empty()
        || key != run_singleton_key(parsed.flow_hash, parsed.run_id, RunTable::SearchDocument)
    {
        return Err(ProjectionError::InvalidState(
            "malformed committed SearchDocument key".into(),
        ));
    }
    Ok(Some((parsed.flow_hash, parsed.run_id)))
}

fn run_record_key(key: &[u8]) -> Result<Option<(FlowHash, RunId)>, ProjectionError> {
    let Ok(parsed) = parse_run_key(key) else {
        return Ok(None);
    };
    if parsed.table != RunTable::RunRecord {
        return Ok(None);
    }
    if !parsed.suffix.is_empty()
        || key != run_singleton_key(parsed.flow_hash, parsed.run_id, RunTable::RunRecord)
    {
        return Err(ProjectionError::InvalidState(
            "malformed committed RunRecord key".into(),
        ));
    }
    Ok(Some((parsed.flow_hash, parsed.run_id)))
}

pub(super) fn decode_search_index_key(
    partition_id: PartitionId,
    key: &[u8],
) -> Result<Option<(TenantId, FlowHash, RunId)>, ProjectionError> {
    let prefix = partition_index_prefix(partition_id, IndexKind::SearchDocument);
    if !key.starts_with(&prefix) {
        return Ok(None);
    }
    if key.len() != prefix.len() + 32 {
        return Err(ProjectionError::InvalidState(
            "malformed committed SearchDocument index delete".into(),
        ));
    }
    let tenant_end = prefix.len() + 8;
    let tenant_bytes: [u8; 8] = key[prefix.len()..tenant_end]
        .try_into()
        .map_err(|_| ProjectionError::InvalidState("malformed search tenant ID".into()))?;
    let (flow_hash, run_id) = decode_flow_run_after_prefix(key, tenant_end)
        .ok_or_else(|| ProjectionError::InvalidState("malformed search run identity".into()))?;
    Ok(Some((
        TenantId(u64::from_be_bytes(tenant_bytes)),
        flow_hash,
        run_id,
    )))
}

fn purged_run(start: &[u8], end: &[u8]) -> Option<(FlowHash, RunId)> {
    if start.len() != 25 || prefix_range(start).1 != end {
        return None;
    }
    let mut document_key = start.to_vec();
    document_key.push(RunTable::SearchDocument as u8);
    let parsed = parse_run_key(&document_key).ok()?;
    (run_prefix(parsed.flow_hash, parsed.run_id) == start)
        .then_some((parsed.flow_hash, parsed.run_id))
}
