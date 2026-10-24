// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::Mutex as StandardMutex;
use std::time::Duration;

use async_trait::async_trait;
use dexzd_keyspace::{RunId, TenantId};
use dexzd_visibility_query::parse_and_check;
use tokio::sync::{Mutex, MutexGuard, Semaphore, SemaphorePermit};
use tokio::task::JoinHandle;
use tokio_postgres::{Client, Config, NoTls, Row};

use super::{
    PageCursor, SchemaError, SqlStatement, SqlTranslationError, ensure_schema, tenant_column_value,
    translate_query,
};
use crate::page_token::PagePosition;
use crate::search_backend::{DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE};
use crate::{SearchBackend, SearchError, SearchPage, SearchRequest};

use super::search_result::response_entry;

pub(super) const DATABASE_OPERATION_TIMEOUT: Duration = Duration::from_secs(15);
const DATABASE_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Waits for an application-defined projection watermark before reading search results.
#[async_trait]
pub trait ProjectionVisibilityWait: Send + Sync {
    /// Waits until the required writes for this request are visible.
    async fn wait_until_visible(&self, request: SearchRequest<'_>) -> Result<(), SearchError>;
}

/// Searches tenant runs through a bounded pool of PgBouncer-compatible connections.
pub struct ParadeDbSearchBackend {
    configuration: Config,
    slots: Vec<Mutex<ConnectionSlot>>,
    free_slots: StandardMutex<VecDeque<usize>>,
    available_slots: Semaphore,
    visibility_wait: Option<Arc<dyn ProjectionVisibilityWait>>,
    visibility_wait_timeout: Duration,
}

impl ParadeDbSearchBackend {
    /// Opens `pool_size` connections, ensures the schema, and bounds projection visibility waits.
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
        let mut slots = Vec::with_capacity(pool_size);
        for slot_index in 0..pool_size {
            let mut slot = ConnectionSlot::open(configuration).await?;
            if slot_index == 0 {
                ensure_schema(&mut slot.client)
                    .await
                    .map_err(schema_search_error)?;
            }
            slots.push(Mutex::new(slot));
        }
        Ok(Self {
            configuration: configuration.clone(),
            slots,
            free_slots: StandardMutex::new((0..pool_size).collect()),
            available_slots: Semaphore::new(pool_size),
            visibility_wait,
            visibility_wait_timeout,
        })
    }

    async fn query(&self, statement: &SqlStatement) -> Result<Vec<Row>, SearchError> {
        let mut slot = self.lock_next_slot().await;
        if slot.is_disconnected() {
            slot.reconnect(&self.configuration).await?;
        }
        for attempt in 0..2 {
            match tokio::time::timeout(
                DATABASE_OPERATION_TIMEOUT,
                slot.client
                    .query_typed(&statement.sql, &statement.typed_parameter_refs()),
            )
            .await
            {
                Ok(Ok(rows)) => return Ok(rows),
                Ok(Err(error)) if is_connection_error(&error) && attempt == 0 => {
                    slot.reconnect(&self.configuration).await?;
                }
                Ok(Err(error)) => {
                    return Err(classify_database_error(
                        "query ParadeDB visibility runs",
                        error,
                    ));
                }
                Err(_) => {
                    slot.invalidate();
                    if attempt == 0 {
                        slot.reconnect(&self.configuration).await?;
                    } else {
                        return Err(SearchError::Unavailable(format!(
                            "query ParadeDB visibility runs timed out after {DATABASE_OPERATION_TIMEOUT:?}"
                        )));
                    }
                }
            }
        }
        unreachable!("the query returns on its second attempt")
    }

    pub(super) async fn lock_next_slot(&self) -> PooledConnectionSlot<'_> {
        let permit = self
            .available_slots
            .acquire()
            .await
            .expect("the owned connection pool never closes its semaphore");
        let slot_index = self
            .free_slots
            .lock()
            .expect("the connection pool slot queue cannot be poisoned")
            .pop_front()
            .expect("a semaphore permit guarantees one free connection slot");
        let slot = self.slots[slot_index]
            .try_lock()
            .expect("a free slot is unlocked before its permit is released");
        PooledConnectionSlot {
            slot,
            slot_index,
            backend: self,
            _permit: permit,
        }
    }

    pub(super) fn configuration(&self) -> &Config {
        &self.configuration
    }
}

pub(super) struct PooledConnectionSlot<'a> {
    slot: MutexGuard<'a, ConnectionSlot>,
    slot_index: usize,
    backend: &'a ParadeDbSearchBackend,
    _permit: SemaphorePermit<'a>,
}

impl Deref for PooledConnectionSlot<'_> {
    type Target = ConnectionSlot;

    fn deref(&self) -> &Self::Target {
        &self.slot
    }
}

impl DerefMut for PooledConnectionSlot<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.slot
    }
}

impl Drop for PooledConnectionSlot<'_> {
    fn drop(&mut self) {
        self.backend
            .free_slots
            .lock()
            .expect("the connection pool slot queue cannot be poisoned")
            .push_back(self.slot_index);
    }
}

pub(super) struct ConnectionSlot {
    pub(super) client: Client,
    connection_task: JoinHandle<()>,
    is_invalidated: bool,
}

impl ConnectionSlot {
    pub(super) async fn open(configuration: &Config) -> Result<Self, SearchError> {
        let connect_timeout = configuration
            .get_connect_timeout()
            .copied()
            .unwrap_or(DATABASE_CONNECT_TIMEOUT);
        let (client, connection) =
            tokio::time::timeout(connect_timeout, configuration.connect(NoTls))
                .await
                .map_err(|_| {
                    SearchError::Unavailable(format!(
                        "connect to ParadeDB timed out after {connect_timeout:?}"
                    ))
                })?
                .map_err(|error| classify_database_error("connect to ParadeDB", error))?;
        let connection_task = tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::error!(%error, "ParadeDB search connection closed");
            }
        });
        Ok(Self {
            client,
            connection_task,
            is_invalidated: false,
        })
    }

    pub(super) async fn reconnect(&mut self, configuration: &Config) -> Result<(), SearchError> {
        let replacement = Self::open(configuration).await?;
        *self = replacement;
        Ok(())
    }

    pub(super) fn is_disconnected(&self) -> bool {
        self.is_invalidated || self.client.is_closed() || self.connection_task.is_finished()
    }

    pub(super) fn invalidate(&mut self) {
        self.is_invalidated = true;
        self.connection_task.abort();
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

pub(super) fn is_connection_error(error: &tokio_postgres::Error) -> bool {
    error.is_closed()
        || error.as_db_error().is_some_and(|database_error| {
            let sqlstate = database_error.code().code();
            sqlstate.starts_with("08") || sqlstate.starts_with("57P0")
        })
}

pub(super) fn classify_database_error(
    operation: &str,
    error: tokio_postgres::Error,
) -> SearchError {
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

fn schema_search_error(error: SchemaError) -> SearchError {
    match error {
        SchemaError::Database { operation, source } => classify_database_error(operation, source),
        SchemaError::NewerVersion { .. } => SearchError::Internal(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::ConnectionSlot;

    #[tokio::test]
    async fn reconnects_after_a_stalled_postgres_handshake() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("fake PostgreSQL listener binds");
        let port = listener
            .local_addr()
            .expect("listener has an address")
            .port();
        let server = tokio::spawn(async move {
            let mut connections = Vec::new();
            for connection_number in 0..3 {
                let (mut stream, _) = listener.accept().await.expect("client connects");
                let startup_length = stream.read_u32().await.expect("startup has a length");
                let mut startup = vec![0; startup_length as usize - 4];
                stream
                    .read_exact(&mut startup)
                    .await
                    .expect("client sends startup parameters");
                if connection_number != 1 {
                    stream
                        .write_all(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0, b'Z', 0, 0, 0, 5, b'I'])
                        .await
                        .expect("server accepts startup");
                }
                connections.push(stream);
            }
            pending::<()>().await;
        });
        let mut configuration = tokio_postgres::Config::new();
        configuration
            .host("127.0.0.1")
            .port(port)
            .user("test")
            .dbname("test")
            .connect_timeout(Duration::from_secs(1));

        let mut slot = ConnectionSlot::open(&configuration)
            .await
            .expect("initial connection opens");
        slot.invalidate();
        assert!(slot.reconnect(&configuration).await.is_err());
        assert!(slot.is_disconnected());
        slot.reconnect(&configuration)
            .await
            .expect("a fresh connection replaces the failed slot");
        assert!(!slot.is_disconnected());
        server.abort();
        let error = server.await.expect_err("fake server is aborted");
        assert!(error.is_cancelled());
    }
}
