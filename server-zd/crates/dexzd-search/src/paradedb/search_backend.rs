// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use dexzd_keyspace::{RunId, TenantId};
use dexzd_visibility_query::parse_and_check;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, Config, NoTls};

use super::{PageCursor, SqlTranslationError, translate_query};
use crate::page_token::PagePosition;
use crate::{SearchBackend, SearchError, SearchPage, SearchRequest};

use super::search_result::response_entry;

/// Waits for an application-defined projection watermark before reading search results.
#[async_trait]
pub trait ProjectionVisibilityWait: Send + Sync {
    /// Waits until the required writes for this request are visible.
    async fn wait_until_visible(&self, request: SearchRequest<'_>) -> Result<(), SearchError>;
}

/// Searches tenant runs through a bounded pool of ParadeDB connections.
pub struct ParadeDbSearchBackend {
    clients: Vec<Client>,
    connection_tasks: Vec<JoinHandle<()>>,
    next_client: AtomicUsize,
    visibility_wait: Option<Arc<dyn ProjectionVisibilityWait>>,
}

impl ParadeDbSearchBackend {
    /// Opens a fixed-size pool; the caller owns the database configuration and optional write barrier.
    pub async fn connect(
        configuration: &Config,
        pool_size: usize,
        visibility_wait: Option<Arc<dyn ProjectionVisibilityWait>>,
    ) -> Result<Self, SearchError> {
        if pool_size == 0 {
            return Err(SearchError::Internal(
                "ParadeDB search pool size must be positive".into(),
            ));
        }
        let mut backend = Self {
            clients: Vec::with_capacity(pool_size),
            connection_tasks: Vec::with_capacity(pool_size),
            next_client: AtomicUsize::new(0),
            visibility_wait,
        };
        for _ in 0..pool_size {
            let (client, connection) = configuration.connect(NoTls).await.map_err(|error| {
                SearchError::Unavailable(format!("connect to ParadeDB: {error}"))
            })?;
            let connection_task = tokio::spawn(async move {
                if let Err(error) = connection.await {
                    tracing::error!(%error, "ParadeDB search connection closed");
                }
            });
            backend.clients.push(client);
            backend.connection_tasks.push(connection_task);
        }
        Ok(backend)
    }

    fn client(&self) -> &Client {
        let index = self.next_client.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        &self.clients[index]
    }
}

impl Drop for ParadeDbSearchBackend {
    fn drop(&mut self) {
        for connection_task in &self.connection_tasks {
            connection_task.abort();
        }
    }
}

#[async_trait]
impl SearchBackend for ParadeDbSearchBackend {
    async fn search(&self, request: SearchRequest<'_>) -> Result<SearchPage, SearchError> {
        if request.page_size == 0 {
            return Err(SearchError::InvalidArgument(
                "SearchFlows page size must be positive".into(),
            ));
        }
        let query = parse_and_check(request.query, request.field_registry)
            .map_err(|error| SearchError::InvalidArgument(error.to_string()))?;
        let position = request
            .page_token
            .map(|token| PagePosition::decode(token, request.tenant_id, request.query))
            .transpose()?;
        let cursor = position.as_ref().map(|position| PageCursor {
            is_running: position.is_running,
            close_time_ns: position.close_time_ns,
            start_time_ns: position.start_time_ns,
            run_id: position.run_id.to_string(),
        });
        let limit = request.page_size.checked_add(1).ok_or_else(|| {
            SearchError::InvalidArgument("SearchFlows page size is too large".into())
        })?;
        let limit = i64::try_from(limit).map_err(|_| {
            SearchError::InvalidArgument("SearchFlows page size is too large".into())
        })?;
        let statement = translate_query(
            &query,
            &request.tenant_id.0.to_string(),
            limit,
            cursor.as_ref(),
        )
        .map_err(translation_error)?;
        if let Some(visibility_wait) = &self.visibility_wait {
            visibility_wait.wait_until_visible(request).await?;
        }
        let mut rows = self
            .client()
            .query(&statement.sql, &statement.parameter_refs())
            .await
            .map_err(|error| {
                SearchError::Unavailable(format!("query ParadeDB visibility runs: {error}"))
            })?;
        let has_next_page = rows.len() > request.page_size;
        if has_next_page {
            rows.truncate(request.page_size);
        }
        let next_page_token = if has_next_page {
            rows.last()
                .map(|row| row_position(row, request.tenant_id, request.query))
                .transpose()?
        } else {
            None
        };
        let entries = rows.iter().map(response_entry).collect::<Result<_, _>>()?;
        Ok(SearchPage {
            entries,
            next_page_token,
        })
    }
}

fn row_position(
    row: &tokio_postgres::Row,
    tenant_id: TenantId,
    query: &str,
) -> Result<String, SearchError> {
    let run_id: String = row
        .try_get("run_id")
        .map_err(|error| SearchError::Internal(format!("read ParadeDB run ID: {error}")))?;
    let run_id = run_id
        .parse::<RunId>()
        .map_err(|error| SearchError::Internal(format!("parse ParadeDB run ID: {error}")))?;
    let execution_status: String = row.try_get("execution_status").map_err(|error| {
        SearchError::Internal(format!("read ParadeDB execution status: {error}"))
    })?;
    let close_time_ns: Option<i64> = row
        .try_get("close_time_ns")
        .map_err(|error| SearchError::Internal(format!("read ParadeDB close time: {error}")))?;
    let start_time_ns: i64 = row
        .try_get("start_time_ns")
        .map_err(|error| SearchError::Internal(format!("read ParadeDB start time: {error}")))?;
    Ok(PagePosition {
        is_running: execution_status == "Running",
        close_time_ns,
        start_time_ns,
        run_id,
    }
    .encode(tenant_id, query))
}

fn translation_error(error: SqlTranslationError) -> SearchError {
    SearchError::InvalidArgument(error.to_string())
}
