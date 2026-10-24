// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Tenant-scoped SearchFlows over committed local run documents.

pub(crate) mod document;

use std::cmp::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
use dexzd_apply::FlowStateReader;
use dexzd_keyspace::row_keys::{
    decode_flow_run_after_prefix, length_prefixed_name, prefix_range, run_singleton_key,
};
use dexzd_keyspace::{
    ColumnFamily, IndexKind, PartitionId, RunTable, TenantId, partition_index_prefix,
};
use dexzd_visibility_query::{
    Expression, FieldRegistry, Literal, Operator, Query, compare_documents, evaluate,
    parse_and_check,
};

use self::document::IndexedDocument;
use crate::page_token::PagePosition;
use crate::search_backend::{DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE};
use crate::{SearchBackend, SearchError, SearchPage, SearchRequest};

/// Default maximum number of indexed documents examined by one builtin search.
pub const DEFAULT_SCAN_BUDGET: usize = 100_000;

/// Largest partition count; partition IDs are `u16`.
const MAX_PARTITION_COUNT: u32 = 65_536;

/// Opens a consistent committed state view for each SearchFlows request.
pub trait CommittedSearchViewProvider: Send + Sync {
    /// Returns one view that remains consistent through all partition scans of a search.
    fn committed_view(&self) -> Result<Arc<dyn FlowStateReader + Send + Sync>, SearchError>;
}

/// Searches all local partitions through one committed state view.
#[derive(Clone)]
pub struct BuiltinSearchBackend {
    view_provider: Arc<dyn CommittedSearchViewProvider>,
    partition_count: u32,
    scan_budget: usize,
}

impl BuiltinSearchBackend {
    /// Creates a backend using the default 100,000-document scan budget.
    pub fn new(
        view_provider: Arc<dyn CommittedSearchViewProvider>,
        partition_count: u32,
    ) -> Result<Self, SearchError> {
        Self::with_scan_budget(view_provider, partition_count, DEFAULT_SCAN_BUDGET)
    }

    /// Creates a backend with a specified document scan budget.
    pub fn with_scan_budget(
        view_provider: Arc<dyn CommittedSearchViewProvider>,
        partition_count: u32,
        scan_budget: usize,
    ) -> Result<Self, SearchError> {
        if partition_count == 0 || partition_count > MAX_PARTITION_COUNT || scan_budget == 0 {
            return Err(SearchError::Internal(
                "builtin search needs a partition count in 1..=65536 and a positive scan budget"
                    .into(),
            ));
        }
        Ok(Self {
            view_provider,
            partition_count,
            scan_budget,
        })
    }

    fn search_committed(&self, request: OwnedSearchRequest) -> Result<SearchPage, SearchError> {
        let query = parse_and_check(&request.query, &request.field_registry)
            .map_err(|error| SearchError::InvalidArgument(error.to_string()))?;
        let cursor = request
            .page_token
            .as_deref()
            .map(|token| PagePosition::decode(token, request.tenant_id, &request.query))
            .transpose()?;
        let flow_type = pinned_flow_type(query.expression());
        let committed_view = self.view_provider.committed_view()?;
        let mut partition_matches = Vec::new();
        let mut scan_state = ScanState {
            committed_view: committed_view.as_ref(),
            scanned: 0,
        };
        for partition_number in 0..self.partition_count {
            let partition_number = u16::try_from(partition_number)
                .expect("with_scan_budget limits the partition count to 65536");
            let mut matches = self.scan_partition(
                PartitionId(partition_number),
                request.tenant_id,
                flow_type,
                &query,
                cursor.as_ref(),
                &mut scan_state,
            )?;
            matches.sort_by(|left, right| compare_documents(&query, left, right));
            partition_matches.push(matches);
        }
        Ok(merge_pages(
            partition_matches,
            &query,
            request.tenant_id,
            &request.query,
            request.page_size,
        ))
    }

    fn scan_partition(
        &self,
        partition: PartitionId,
        tenant_id: TenantId,
        flow_type: Option<&str>,
        query: &Query,
        cursor: Option<&PagePosition>,
        scan_state: &mut ScanState<'_>,
    ) -> Result<Vec<IndexedDocument>, SearchError> {
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
        let (mut start, end) = prefix_range(&prefix);
        let identity_offset = prefix.len() + usize::from(flow_type.is_some());
        let mut matches = Vec::new();
        loop {
            let remaining = self.scan_budget - scan_state.scanned;
            let index_rows = scan_state
                .committed_view
                .scan(
                    ColumnFamily::PartitionIndex,
                    &start,
                    &end,
                    remaining.saturating_add(1),
                )
                .map_err(|error| SearchError::Unavailable(error.to_string()))?;
            if index_rows.len() > remaining {
                return Err(SearchError::ResourceExhausted(format!(
                    "builtin SearchFlows scan exceeds {} documents; configure ParadeDB",
                    self.scan_budget
                )));
            }
            if index_rows.is_empty() {
                break;
            }
            scan_state.scanned += index_rows.len();
            let last_key = index_rows.last().expect("nonempty index batch").0.clone();
            if last_key < start {
                return Err(SearchError::Internal("search index scan regressed".into()));
            }
            start = last_key;
            start.push(0);
            for (index_key, _) in index_rows {
                if index_key.len() != identity_offset + 24 {
                    return Err(SearchError::Internal("malformed search index key".into()));
                }
                let (flow_hash, run_id) = decode_flow_run_after_prefix(&index_key, identity_offset)
                    .ok_or_else(|| SearchError::Internal("malformed search index key".into()))?;
                let document_key = run_singleton_key(flow_hash, run_id, RunTable::SearchDocument);
                let encoded = scan_state
                    .committed_view
                    .get(ColumnFamily::Flow, &document_key)
                    .map_err(|error| SearchError::Unavailable(error.to_string()))?
                    .ok_or_else(|| {
                        SearchError::Internal("search index has no run document".into())
                    })?;
                let document = IndexedDocument::decode(&encoded, tenant_id, run_id)?;
                if evaluate(query, &document)
                    && cursor.is_none_or(|cursor| {
                        compare_documents(query, &document, cursor) == Ordering::Greater
                    })
                {
                    matches.push(document);
                }
            }
        }
        Ok(matches)
    }
}

#[async_trait]
impl SearchBackend for BuiltinSearchBackend {
    async fn search(&self, request: SearchRequest<'_>) -> Result<SearchPage, SearchError> {
        let page_size = if request.page_size == 0 {
            DEFAULT_PAGE_SIZE
        } else {
            request.page_size.min(MAX_PAGE_SIZE)
        };
        let backend = self.clone();
        let owned = OwnedSearchRequest {
            tenant_id: request.tenant_id,
            query: request.query.to_owned(),
            page_size,
            page_token: request.page_token.map(str::to_owned),
            field_registry: request.field_registry.clone(),
        };
        tokio::task::spawn_blocking(move || backend.search_committed(owned))
            .await
            .map_err(|error| {
                SearchError::Internal(format!("builtin search task failed: {error}"))
            })?
    }
}

struct OwnedSearchRequest {
    tenant_id: TenantId,
    query: String,
    page_size: usize,
    page_token: Option<String>,
    field_registry: FieldRegistry,
}

struct ScanState<'a> {
    committed_view: &'a (dyn FlowStateReader + Send + Sync),
    scanned: usize,
}

fn pinned_flow_type(expression: &Expression) -> Option<&str> {
    match expression {
        Expression::Predicate(predicate)
            if predicate.field() == "FlowType" && predicate.operator() == Operator::Equal =>
        {
            match predicate.literals().first() {
                Some(Literal::String(value)) => Some(value),
                _ => None,
            }
        }
        Expression::And(children) => children.iter().find_map(pinned_flow_type),
        _ => None,
    }
}

fn merge_pages(
    partitions: Vec<Vec<IndexedDocument>>,
    query: &Query,
    tenant_id: TenantId,
    query_text: &str,
    page_size: usize,
) -> SearchPage {
    let mut heads = vec![0; partitions.len()];
    let mut page = Vec::with_capacity(page_size.saturating_add(1));
    loop {
        let next_partition = partitions
            .iter()
            .enumerate()
            .filter(|(partition, documents)| heads[*partition] < documents.len())
            .min_by(|(left_partition, left), (right_partition, right)| {
                compare_documents(
                    query,
                    &left[heads[*left_partition]],
                    &right[heads[*right_partition]],
                )
            })
            .map(|(partition, _)| partition);
        let Some(next_partition) = next_partition else {
            break;
        };
        page.push(&partitions[next_partition][heads[next_partition]]);
        heads[next_partition] += 1;
        if page.len() > page_size {
            break;
        }
    }
    let has_next_page = page.len() > page_size;
    if has_next_page {
        page.pop();
    }
    let next_page_token = if has_next_page {
        page.last()
            .map(|document| document.page_position().encode(tenant_id, query_text))
    } else {
        None
    };
    SearchPage {
        entries: page
            .into_iter()
            .map(IndexedDocument::response_entry)
            .collect(),
        next_page_token,
    }
}

#[cfg(test)]
mod tests;
