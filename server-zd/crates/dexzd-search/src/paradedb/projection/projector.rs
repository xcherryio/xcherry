// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;

use dexzd_apply::FlowStateReader;
use dexzd_keyspace::row_keys::{
    decode_flow_run_after_prefix, partition_index_range, run_singleton_key,
};
use dexzd_keyspace::{ColumnFamily, IndexKind, PartitionId, RunId, RunTable, TenantId};
use dexzd_protocol::dex::IndexType;
use dexzd_protocol::dexzd::v1::SearchDocumentRecord;
use prost::Message;
use thiserror::Error;
use tokio_postgres::Transaction;
use tokio_postgres::types::{ToSql, Type};

use super::super::search_backend::{ParadeDbSearchBackend, is_connection_error};
use super::super::{SearchRow, SqlStatement, upsert_statement};
use super::changes::SearchDocumentChange;
use crate::SearchError;
use crate::builtin::document::IndexedDocument;

const REBUILD_SCAN_PAGE_SIZE: usize = 256;

/// A failure while projecting committed run documents.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ProjectionError {
    /// A committed row or index violates its expected format.
    #[error("invalid committed search state: {0}")]
    InvalidState(String),
    /// A log batch was omitted and requires a partition rebuild.
    #[error(
        "ParadeDB projection partition {partition_id} expected position {expected}, got {actual}"
    )]
    PositionGap {
        /// Partition being projected.
        partition_id: PartitionId,
        /// Next required log position.
        expected: u64,
        /// Position offered by the caller.
        actual: u64,
    },
    /// A rebuild source is older than the committed projection.
    #[error(
        "ParadeDB partition {partition_id} is at position {current}, newer than rebuild position {offered}"
    )]
    StaleRebuild {
        /// Partition being rebuilt.
        partition_id: PartitionId,
        /// Committed projection position.
        current: u64,
        /// Offered rebuild position.
        offered: u64,
    },
    /// Postgres could not complete a projection operation.
    #[error("{operation} ParadeDB projection: {source}")]
    Database {
        /// Failed operation.
        operation: &'static str,
        /// Database failure.
        #[source]
        source: tokio_postgres::Error,
    },
    /// A pooled connection could not be reopened.
    #[error("reconnect ParadeDB projector: {0}")]
    Connection(#[from] SearchError),
}

/// Projects committed partition documents through the search backend's shared connection pool.
pub struct ParadeDbProjector {
    backend: Arc<ParadeDbSearchBackend>,
}

impl ParadeDbProjector {
    /// Shares the backend's `pool_size` connections with SearchFlows requests.
    #[must_use]
    pub fn new(backend: Arc<ParadeDbSearchBackend>) -> Self {
        Self { backend }
    }

    /// Returns the last committed log position for one partition, if any.
    pub async fn read_progress(
        &self,
        partition_id: PartitionId,
    ) -> Result<Option<u64>, ProjectionError> {
        let partition_number = i32::from(partition_id.0);
        let mut attempted_retry = false;
        loop {
            let mut slot = self.backend.lock_next_slot().await;
            if slot.client.is_closed() {
                slot.reconnect(self.backend.configuration()).await?;
            }
            let result = slot
                .client
                .query_typed_opt(
                    "SELECT applied_position FROM dexzd_visibility_progress WHERE partition_id = $1::integer",
                    &[(&partition_number as &(dyn ToSql + Sync), Type::INT4)],
                )
                .await
                .map_err(|source| database_error("read progress from", source))
                .and_then(|row| row.map(|row| position_from_row(&row)).transpose());
            if should_retry(&result, attempted_retry) {
                attempted_retry = true;
                slot.reconnect(self.backend.configuration()).await?;
                continue;
            }
            return result;
        }
    }

    /// Applies one consecutive committed log batch and its progress in one transaction.
    pub async fn apply_batch(
        &self,
        partition_id: PartitionId,
        applied_position: u64,
        changes: &[SearchDocumentChange],
    ) -> Result<(), ProjectionError> {
        let position = database_position(applied_position)?;
        let mut attempted_retry = false;
        loop {
            let mut slot = self.backend.lock_next_slot().await;
            if slot.client.is_closed() {
                slot.reconnect(self.backend.configuration()).await?;
            }
            let result = apply_batch_transaction(
                &mut slot.client,
                partition_id,
                applied_position,
                position,
                changes,
            )
            .await;
            if should_retry(&result, attempted_retry) {
                attempted_retry = true;
                slot.reconnect(self.backend.configuration()).await?;
                continue;
            }
            return result;
        }
    }

    /// Rebuilds one partition from a consistent committed local view at `applied_position`.
    pub async fn rebuild_partition(
        &self,
        partition_id: PartitionId,
        applied_position: u64,
        committed_view: &dyn FlowStateReader,
    ) -> Result<(), ProjectionError> {
        let position = database_position(applied_position)?;
        let mut attempted_retry = false;
        loop {
            let mut slot = self.backend.lock_next_slot().await;
            if slot.client.is_closed() {
                slot.reconnect(self.backend.configuration()).await?;
            }
            let result = rebuild_transaction(
                &mut slot.client,
                partition_id,
                applied_position,
                position,
                committed_view,
            )
            .await;
            if should_retry(&result, attempted_retry) {
                attempted_retry = true;
                slot.reconnect(self.backend.configuration()).await?;
                continue;
            }
            return result;
        }
    }
}

async fn apply_batch_transaction(
    client: &mut tokio_postgres::Client,
    partition_id: PartitionId,
    applied_position: u64,
    position: i64,
    changes: &[SearchDocumentChange],
) -> Result<(), ProjectionError> {
    let transaction = client
        .transaction()
        .await
        .map_err(|source| database_error("begin", source))?;
    let current = lock_progress(&transaction, partition_id).await?;
    if applied_position <= current {
        return transaction
            .commit()
            .await
            .map_err(|source| database_error("commit replay of", source));
    }
    let expected = current.checked_add(1).ok_or_else(|| {
        ProjectionError::InvalidState("ParadeDB projection position overflow".into())
    })?;
    if applied_position != expected {
        return Err(ProjectionError::PositionGap {
            partition_id,
            expected,
            actual: applied_position,
        });
    }
    for change in changes {
        apply_change(&transaction, partition_id, change).await?;
    }
    update_progress(&transaction, partition_id, position).await?;
    transaction
        .commit()
        .await
        .map_err(|source| database_error("commit", source))
}

async fn rebuild_transaction(
    client: &mut tokio_postgres::Client,
    partition_id: PartitionId,
    applied_position: u64,
    position: i64,
    committed_view: &dyn FlowStateReader,
) -> Result<(), ProjectionError> {
    let transaction = client
        .transaction()
        .await
        .map_err(|source| database_error("begin rebuild of", source))?;
    let current = lock_progress(&transaction, partition_id).await?;
    if applied_position < current {
        return Err(ProjectionError::StaleRebuild {
            partition_id,
            current,
            offered: applied_position,
        });
    }
    let partition_number = i32::from(partition_id.0);
    transaction
        .execute_typed(
            "DELETE FROM dexzd_visibility_runs WHERE partition_id = $1::integer",
            &[(&partition_number as &(dyn ToSql + Sync), Type::INT4)],
        )
        .await
        .map_err(|source| database_error("remove stale rows from", source))?;
    let (mut start, end) = partition_index_range(partition_id, IndexKind::SearchDocument);
    loop {
        let rows = committed_view
            .scan(
                ColumnFamily::PartitionIndex,
                &start,
                &end,
                REBUILD_SCAN_PAGE_SIZE,
            )
            .map_err(|error| {
                ProjectionError::InvalidState(format!("scan committed search index: {error}"))
            })?;
        if rows.is_empty() {
            break;
        }
        let last_key = rows.last().expect("nonempty search index scan").0.clone();
        if last_key < start {
            return Err(ProjectionError::InvalidState(
                "search index scan regressed".into(),
            ));
        }
        start = last_key;
        start.push(0);
        for (index_key, _) in rows {
            if index_key.len() != 35 {
                return Err(ProjectionError::InvalidState(
                    "malformed search index key".into(),
                ));
            }
            let tenant_bytes: [u8; 8] = index_key[3..11].try_into().map_err(|_| {
                ProjectionError::InvalidState("malformed search index tenant".into())
            })?;
            let tenant_id = TenantId(u64::from_be_bytes(tenant_bytes));
            let (flow_hash, run_id) =
                decode_flow_run_after_prefix(&index_key, 11).ok_or_else(|| {
                    ProjectionError::InvalidState("malformed search index identity".into())
                })?;
            let document_key = run_singleton_key(flow_hash, run_id, RunTable::SearchDocument);
            let encoded_document = committed_view
                .get(ColumnFamily::Flow, &document_key)
                .map_err(|error| {
                    ProjectionError::InvalidState(format!(
                        "read committed search document: {error}"
                    ))
                })?
                .ok_or_else(|| {
                    ProjectionError::InvalidState(format!(
                        "missing committed search document for {run_id}"
                    ))
                })?;
            apply_change(
                &transaction,
                partition_id,
                &SearchDocumentChange::Upsert {
                    tenant_id,
                    run_id,
                    encoded_document,
                },
            )
            .await?;
        }
    }
    update_progress(&transaction, partition_id, position).await?;
    transaction
        .commit()
        .await
        .map_err(|source| database_error("commit rebuild of", source))
}

async fn apply_change(
    transaction: &Transaction<'_>,
    partition_id: PartitionId,
    change: &SearchDocumentChange,
) -> Result<(), ProjectionError> {
    match change {
        SearchDocumentChange::Upsert {
            tenant_id,
            run_id,
            encoded_document,
        } => {
            let row = row_from_document(partition_id, *tenant_id, *run_id, encoded_document)?;
            let statement = upsert_statement(&row);
            execute_statement(transaction, &statement).await?;
        }
        SearchDocumentChange::Delete { tenant_id, run_id } => {
            let tenant_column = super::super::tenant_column_value(*tenant_id);
            let partition_number = i32::from(partition_id.0);
            let run_column = run_id.to_string();
            transaction
                .execute_typed(
                    "DELETE FROM dexzd_visibility_runs WHERE tenant_id = $1::text AND partition_id = $2::integer AND run_id = $3::text",
                    &[
                        (&tenant_column as &(dyn ToSql + Sync), Type::TEXT),
                        (&partition_number, Type::INT4),
                        (&run_column, Type::TEXT),
                    ],
                )
                .await
                .map_err(|source| database_error("delete run from", source))?;
        }
    }
    Ok(())
}

fn row_from_document(
    partition_id: PartitionId,
    tenant_id: TenantId,
    run_id: RunId,
    encoded_document: &[u8],
) -> Result<SearchRow, ProjectionError> {
    let record = SearchDocumentRecord::decode(encoded_document).map_err(|error| {
        ProjectionError::InvalidState(format!("decode committed search document: {error}"))
    })?;
    let version = i64::try_from(record.version).map_err(|_| {
        ProjectionError::InvalidState("search document version exceeds bigint".into())
    })?;
    let application_fields = record
        .attributes
        .iter()
        .map(|attribute| {
            IndexType::try_from(attribute.index_type)
                .map(|index_type| (attribute.index_key.clone(), index_type))
                .map_err(|_| {
                    ProjectionError::InvalidState(format!(
                        "search attribute {} has unknown index type",
                        attribute.index_key
                    ))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let document = IndexedDocument::decode(encoded_document, tenant_id, run_id)
        .map_err(|error| ProjectionError::InvalidState(error.to_string()))?;
    let mut row = SearchRow::from_document(tenant_id, version, &document, &application_fields)
        .and_then(|row| row.with_wire_attributes(&record.attributes))
        .map_err(|error| ProjectionError::InvalidState(error.to_string()))?;
    row.partition_id = i32::from(partition_id.0);
    Ok(row)
}

async fn execute_statement(
    transaction: &Transaction<'_>,
    statement: &SqlStatement,
) -> Result<(), ProjectionError> {
    transaction
        .execute_typed(&statement.sql, &statement.typed_parameter_refs())
        .await
        .map_err(|source| database_error("upsert run into", source))?;
    Ok(())
}

async fn lock_progress(
    transaction: &Transaction<'_>,
    partition_id: PartitionId,
) -> Result<u64, ProjectionError> {
    let partition_number = i32::from(partition_id.0);
    transaction
        .execute_typed(
            "INSERT INTO dexzd_visibility_progress (partition_id, applied_position) VALUES ($1::integer, 0) ON CONFLICT (partition_id) DO NOTHING",
            &[(&partition_number as &(dyn ToSql + Sync), Type::INT4)],
        )
        .await
        .map_err(|source| database_error("initialize progress for", source))?;
    let row = transaction
        .query_typed_one(
            "SELECT applied_position FROM dexzd_visibility_progress WHERE partition_id = $1::integer FOR UPDATE",
            &[(&partition_number as &(dyn ToSql + Sync), Type::INT4)],
        )
        .await
        .map_err(|source| database_error("lock progress for", source))?;
    position_from_row(&row)
}

async fn update_progress(
    transaction: &Transaction<'_>,
    partition_id: PartitionId,
    position: i64,
) -> Result<(), ProjectionError> {
    let partition_number = i32::from(partition_id.0);
    transaction
        .execute_typed(
            "UPDATE dexzd_visibility_progress SET applied_position = $2::bigint WHERE partition_id = $1::integer",
            &[
                (&partition_number as &(dyn ToSql + Sync), Type::INT4),
                (&position, Type::INT8),
            ],
        )
        .await
        .map_err(|source| database_error("update progress for", source))?;
    Ok(())
}

fn position_from_row(row: &tokio_postgres::Row) -> Result<u64, ProjectionError> {
    let position: i64 = row
        .try_get(0)
        .map_err(|source| database_error("decode progress from", source))?;
    u64::try_from(position)
        .map_err(|_| ProjectionError::InvalidState("negative ParadeDB projection position".into()))
}

fn database_position(position: u64) -> Result<i64, ProjectionError> {
    i64::try_from(position)
        .map_err(|_| ProjectionError::InvalidState("projection position exceeds bigint".into()))
}

fn database_error(operation: &'static str, source: tokio_postgres::Error) -> ProjectionError {
    ProjectionError::Database { operation, source }
}

fn should_retry(result: &Result<impl Sized, ProjectionError>, attempted_retry: bool) -> bool {
    !attempted_retry
        && matches!(result, Err(ProjectionError::Database { source, .. }) if is_connection_error(source))
}
