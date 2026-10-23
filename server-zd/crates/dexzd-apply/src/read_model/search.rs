// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_keyspace::row_keys::{length_prefixed_name, prefix_range};
use dexzd_keyspace::{
    ColumnFamily, FlowHash, IndexKind, PartitionId, RunId, TenantId, partition_index_prefix,
};
use dexzd_protocol::dexzd::v1::SearchDocumentRecord;

use crate::read_model::ReadModelError;
use crate::state_access::{FlowTransaction, RunRows};
use crate::{ApplyContext, ApplyError, FlowStateReader};

/// Reads one run's current search document.
pub fn get_search_document(
    reader: &dyn FlowStateReader,
    flow_hash: FlowHash,
    run_id: RunId,
) -> Result<Option<SearchDocumentRecord>, ReadModelError> {
    let transaction = FlowTransaction::new(
        reader,
        ApplyContext {
            partition: PartitionId(0),
            flow_hash,
        },
    );
    Ok(RunRows::new(flow_hash, run_id).search_document(&transaction)?)
}

/// Scans one partition's search documents, optionally narrowed by Flow type.
pub fn list_search_documents(
    reader: &dyn FlowStateReader,
    partition: PartitionId,
    tenant_id: TenantId,
    flow_type: Option<&str>,
    limit: usize,
) -> Result<Vec<SearchDocumentRecord>, ReadModelError> {
    if limit == 0 {
        return Err(ReadModelError::InvalidRequest {
            detail: "search document scan limit must be positive".to_owned(),
        });
    }
    let index_kind = if flow_type.is_some() {
        IndexKind::FlowList
    } else {
        IndexKind::SearchDocument
    };
    let mut prefix = partition_index_prefix(partition, index_kind);
    prefix.extend_from_slice(&tenant_id.to_be_bytes());
    if let Some(flow_type) = flow_type {
        prefix.extend_from_slice(&length_prefixed_name(flow_type));
    }
    let (start, end) = prefix_range(&prefix);
    let index_rows = reader
        .scan(ColumnFamily::PartitionIndex, &start, &end, limit)
        .map_err(ApplyError::from)?;
    let mut documents = Vec::with_capacity(index_rows.len());
    for (key, version_bytes) in index_rows {
        let suffix = key.get(prefix.len()..).ok_or_else(|| corrupt_index(&key))?;
        let suffix = if flow_type.is_some() {
            suffix.get(1..).ok_or_else(|| corrupt_index(&key))?
        } else {
            suffix
        };
        if suffix.len() != 24 {
            return Err(corrupt_index(&key));
        }
        let flow_hash = FlowHash(u64::from_be_bytes(
            suffix[..8].try_into().map_err(|_| corrupt_index(&key))?,
        ));
        let run_id = RunId::from_slice(&suffix[8..]).map_err(|_| corrupt_index(&key))?;
        let transaction = FlowTransaction::new(
            reader,
            ApplyContext {
                partition,
                flow_hash,
            },
        );
        let Some(run) = RunRows::new(flow_hash, run_id).run_record(&transaction)? else {
            return Err(corrupt_index(&key));
        };
        if run.is_purging {
            continue;
        }
        let Some(document) = get_search_document(reader, flow_hash, run_id)? else {
            return Err(corrupt_index(&key));
        };
        if index_kind == IndexKind::SearchDocument {
            let version: [u8; 8] = version_bytes
                .as_slice()
                .try_into()
                .map_err(|_| corrupt_index(&key))?;
            if document.version != u64::from_be_bytes(version) {
                return Err(corrupt_index(&key));
            }
        }
        documents.push(document);
    }
    Ok(documents)
}

fn corrupt_index(key: &[u8]) -> ReadModelError {
    ReadModelError::State(ApplyError::CorruptState {
        table: "search document index",
        key: key.iter().map(|byte| format!("{byte:02x}")).collect(),
        detail: "malformed or dangling search document index".to_owned(),
    })
}
