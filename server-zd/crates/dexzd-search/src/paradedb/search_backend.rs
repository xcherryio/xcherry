// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use dexzd_keyspace::{RunId, TenantId};
use dexzd_visibility_query::parse_and_check;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, Config, NoTls, Row};

use super::{PageCursor, SqlStatement, SqlTranslationError, tenant_column_value, translate_query};
use crate::page_token::PagePosition;
use crate::search_backend::{DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE};
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
    configuration: Config,
    slots: Vec<Mutex<ConnectionSlot>>,
    next_slot: AtomicUsize,
    visibility_wait: Option<Arc<dyn ProjectionVisibilityWait>>,
    visibility_wait_timeout: Duration,
}

impl ParadeDbSearchBackend {
    /// Opens a fixed pool with a positive projection wait cap and an optional write barrier.
    pub async fn connect(
        configuration: &Config,
        pool_size: usize,
        visibility_wait: Option<Arc<dyn ProjectionVisibilityWait>>,
        visibility_wait_timeout: Duration,
    ) -> Result<Self, SearchError> {
        if pool_size == 0 || visibility_wait_timeout.is_zero() {
            return Err(SearchError::Internal(
                "ParadeDB search pool size and visibility wait timeout must be positive".into(),
            ));
        }
        let mut backend = Self {
            configuration: configuration.clone(),
            slots: Vec::with_capacity(pool_size),
            next_slot: AtomicUsize::new(0),
            visibility_wait,
            visibility_wait_timeout,
        };
        for _ in 0..pool_size {
            backend
                .slots
                .push(Mutex::new(ConnectionSlot::open(configuration).await?));
        }
        Ok(backend)
    }

    async fn query(&self, statement: &SqlStatement) -> Result<Vec<Row>, SearchError> {
        let index = self.next_slot.fetch_add(1, Ordering::Relaxed) % self.slots.len();
        let mut slot = self.slots[index].lock().await;
        if slot.client.is_closed() {
            slot.reconnect(&self.configuration).await?;
        }
        match slot
            .client
            .query(&statement.sql, &statement.parameter_refs())
            .await
        {
            Ok(rows) => Ok(rows),
            Err(error) if is_connection_error(&error) => {
                slot.reconnect(&self.configuration).await?;
                slot.client
                    .query(&statement.sql, &statement.parameter_refs())
                    .await
                    .map_err(|error| {
                        classify_database_error("query ParadeDB visibility runs", error)
                    })
            }
            Err(error) => Err(classify_database_error(
                "query ParadeDB visibility runs",
                error,
            )),
        }
    }
}

struct ConnectionSlot {
    client: Client,
    connection_task: JoinHandle<()>,
}

impl ConnectionSlot {
    async fn open(configuration: &Config) -> Result<Self, SearchError> {
        let (client, connection) = configuration
            .connect(NoTls)
            .await
            .map_err(|error| classify_database_error("connect to ParadeDB", error))?;
        let connection_task = tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::error!(%error, "ParadeDB search connection closed");
            }
        });
        Ok(Self {
            client,
            connection_task,
        })
    }

    async fn reconnect(&mut self, configuration: &Config) -> Result<(), SearchError> {
        let replacement = Self::open(configuration).await?;
        *self = replacement;
        Ok(())
    }
}

impl Drop for ConnectionSlot {
    fn drop(&mut self) {
        self.connection_task.abort();
    }
}

#[async_trait]
impl SearchBackend for ParadeDbSearchBackend {
    async fn search(&self, request: SearchRequest<'_>) -> Result<SearchPage, SearchError> {
        let page_size = if request.page_size == 0 {
            DEFAULT_PAGE_SIZE
        } else {
            request.page_size.min(MAX_PAGE_SIZE)
        };
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
        let limit = (page_size + 1) as i64;
        let statement = translate_query(
            &query,
            &tenant_column_value(request.tenant_id),
            limit,
            cursor.as_ref(),
        )
        .map_err(translation_error)?;
        if let Some(visibility_wait) = &self.visibility_wait {
            tokio::time::timeout(
                self.visibility_wait_timeout,
                visibility_wait.wait_until_visible(request),
            )
            .await
            .map_err(|_| {
                SearchError::Unavailable(format!(
                    "wait for ParadeDB projection visibility timed out after {:?}",
                    self.visibility_wait_timeout
                ))
            })??;
        }
        let mut rows = self.query(&statement).await?;
        let has_next_page = rows.len() > page_size;
        if has_next_page {
            rows.truncate(page_size);
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

fn is_connection_error(error: &tokio_postgres::Error) -> bool {
    error.is_closed()
        || error.as_db_error().is_some_and(|database_error| {
            let sqlstate = database_error.code().code();
            sqlstate.starts_with("08") || sqlstate.starts_with("57P0")
        })
}

fn classify_database_error(operation: &str, error: tokio_postgres::Error) -> SearchError {
    let sqlstate = error
        .as_db_error()
        .map_or("none", |database_error| database_error.code().code());
    let message = format!("{operation} (SQLSTATE {sqlstate}): {error}");
    if is_connection_error(&error) {
        SearchError::Unavailable(message)
    } else {
        SearchError::Internal(message)
    }
}
