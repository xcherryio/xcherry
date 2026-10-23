// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use async_trait::async_trait;
use dexzd_keyspace::TenantId;
use dexzd_protocol::dex::SearchFlowsResponseEntry;
use dexzd_visibility_query::FieldRegistry;
use thiserror::Error;

pub(crate) const DEFAULT_PAGE_SIZE: usize = 1_000;
pub(crate) const MAX_PAGE_SIZE: usize = 1_000;

/// Inputs to one tenant-scoped SearchFlows page.
#[derive(Debug, Clone, Copy)]
pub struct SearchRequest<'a> {
    /// Tenant whose runs may be returned.
    pub tenant_id: TenantId,
    /// Visibility query text, including optional ordering.
    pub query: &'a str,
    /// Maximum entries to return; zero defaults to 1,000, and larger requests cap at 1,000.
    pub page_size: usize,
    /// Opaque position returned by the previous page.
    pub page_token: Option<&'a str>,
    /// Types of registered application attributes.
    pub field_registry: &'a FieldRegistry,
}

/// One page of SearchFlows results.
#[derive(Debug, Clone)]
pub struct SearchPage {
    /// Runs in visibility order.
    pub entries: Vec<SearchFlowsResponseEntry>,
    /// Position for the next page, if more runs remain.
    pub next_page_token: Option<String>,
}

/// Search failure category for API status mapping.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SearchError {
    /// Query or token rejected as client input.
    #[error("invalid SearchFlows argument: {0}")]
    InvalidArgument(String),
    /// Local scan exceeded its configured document budget.
    #[error("SearchFlows resource exhausted: {0}")]
    ResourceExhausted(String),
    /// Search storage could not be read.
    #[error("SearchFlows unavailable: {0}")]
    Unavailable(String),
    /// Stored search state violates a trusted invariant.
    #[error("SearchFlows internal error: {0}")]
    Internal(String),
}

/// Searches a tenant's indexed Flow runs.
#[async_trait]
pub trait SearchBackend: Send + Sync {
    /// Returns one ordered page or a typed search failure.
    async fn search(&self, request: SearchRequest<'_>) -> Result<SearchPage, SearchError>;
}
