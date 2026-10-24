// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;

use dexzd_apply::{FlowStateReader, StateReadError};
use dexzd_keyspace::row_keys::{partition_index_range, run_singleton_key};
use dexzd_keyspace::{ColumnFamily, IndexKind, PartitionId, RunId, RunTable, TenantId};
use dexzd_protocol::dex::IndexType;
use dexzd_protocol::dexzd::v1::{LogPosition, SearchDocumentRecord};
use prost::Message;
use thiserror::Error;
use tokio_postgres::Transaction;
use tokio_postgres::types::{ToSql, Type};

use super::super::search_backend::{
    DATABASE_OPERATION_TIMEOUT, ParadeDbSearchBackend, database_error_message,
    is_transient_database_error,
};
use super::super::{SearchRow, SqlStatement, upsert_statement};
use super::changes::{SearchDocumentChange, decode_search_index_key};
use crate::SearchError;
use crate::builtin::document::IndexedDocument;

const REBUILD_SCAN_PAGE_SIZE: usize = 256;

struct RebuildPage {
    next_start: Vec<u8>,
    changes: Vec<SearchDocumentChange>,
}

struct SkippedSearchDocument {
    partition_id: PartitionId,
    tenant_id: TenantId,
    run_id: RunId,
    reason: String,
}

/// Receives a countable event for each committed search document omitted from ParadeDB.
pub trait ProjectionMetricRecorder: Send + Sync {
    /// Records a document omitted because its committed value cannot be indexed.
    fn record_skipped_search_document(
        &self,
        partition_id: PartitionId,
        tenant_id: TenantId,
        run_id: RunId,
    );
}

/// A failure while projecting committed run documents.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ProjectionError {
    /// A committed row or index violates its expected format.
    #[error("invalid committed search state: {0}")]
    InvalidState(String),
    /// The stored position differs from the caller's previous position.
    #[error(
        "ParadeDB projection partition {partition_id} has position {stored:?}, expected {expected:?} before {offered:?}"
    )]
    PositionMismatch {
        /// Partition being projected.
        partition_id: PartitionId,
        /// Position stored in ParadeDB.
        stored: Option<LogPosition>,
        /// Previous position expected by the caller.
        expected: Option<LogPosition>,
        /// New position offered by the caller.
        offered: LogPosition,
    },
    /// A rebuild source is older than the committed projection.
    #[error(
        "ParadeDB partition {partition_id} is at position {current:?}, newer than rebuild position {offered:?}"
    )]
    StaleRebuild {
        /// Partition being rebuilt.
        partition_id: PartitionId,
        /// Committed projection position.
        current: LogPosition,
        /// Offered rebuild position.
        offered: LogPosition,
    },
    /// Postgres could not complete a projection operation.
    #[error("{operation} ParadeDB projection: {}", database_error_message(source))]
    Database {
        /// Failed operation.
        operation: &'static str,
        /// Database failure.
        #[source]
        source: tokio_postgres::Error,
    },
    /// Postgres rejected one committed run's upsert.
    #[error("upsert run {run_id} in partition {} into ParadeDB projection: {}", partition_id.0, database_error_message(source))]
    RowDatabase {
        /// Partition containing the run.
        partition_id: PartitionId,
        /// Run whose upsert failed.
        run_id: RunId,
        /// Database failure.
        #[source]
        source: tokio_postgres::Error,
    },
    /// A pooled connection could not be reopened.
    #[error("reconnect ParadeDB projector: {0}")]
    Connection(#[from] SearchError),
    /// Reading a committed local-store row failed.
    #[error("read committed search state: {0}")]
    StateRead(#[from] StateReadError),
}

impl ProjectionError {
    /// Returns whether reopening a failed connection can recover this projection operation.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Connection(SearchError::Unavailable(_)) => true,
            Self::Database { source, .. } | Self::RowDatabase { source, .. } => {
                is_transient_database_error(source)
            }
            Self::StateRead(_) => true,
            _ => false,
        }
    }
}

/// Projects committed partition documents through the search backend's shared connection pool.
pub struct ParadeDbProjector {
    backend: Arc<ParadeDbSearchBackend>,
    metric_recorder: Option<Arc<dyn ProjectionMetricRecorder>>,
}

impl ParadeDbProjector {
    /// Shares the backend's `pool_size` connections with SearchFlows requests.
    #[must_use]
    pub fn new(backend: Arc<ParadeDbSearchBackend>) -> Self {
        Self {
            backend,
            metric_recorder: None,
        }
    }

    /// Shares backend connections and reports committed poison documents to `metric_recorder`.
    #[must_use]
    pub fn new_with_metric_recorder(
        backend: Arc<ParadeDbSearchBackend>,
        metric_recorder: Arc<dyn ProjectionMetricRecorder>,
    ) -> Self {
        Self {
            backend,
            metric_recorder: Some(metric_recorder),
        }
    }

    /// Returns the last committed log position for one partition, if any.
    pub async fn read_progress(
        &self,
        partition_id: PartitionId,
    ) -> Result<Option<LogPosition>, ProjectionError> {
        let partition_number = i32::from(partition_id.0);
        let mut has_retried = false;
        loop {
            let mut slot = self.backend.lock_next_slot().await;
            if slot.is_disconnected() {
                slot.reconnect(self.backend.configuration()).await?;
            }
            let result = tokio::time::timeout(
                DATABASE_OPERATION_TIMEOUT,
                slot.client.query_typed_opt(
                    "SELECT applied_node_id, applied_incarnation, applied_seq, applied_position FROM dexzd_visibility_progress WHERE partition_id = $1::integer",
                    &[(&partition_number as &(dyn ToSql + Sync), Type::INT4)],
                ),
            )
            .await;
            let result = match result {
                Ok(result) => result
                    .map_err(|source| database_error("read progress from", source))
                    .and_then(|row| {
                        row.map(|row| position_from_row(&row))
                            .transpose()
                            .map(Option::flatten)
                    }),
                Err(_) => {
                    slot.invalidate();
                    Err(operation_timeout("read progress from"))
                }
            };
            if should_retry(&result, has_retried) {
                has_retried = true;
                slot.reconnect(self.backend.configuration()).await?;
                continue;
            }
            return result;
        }
    }

    /// Applies one committed partition batch after `previous_position` in one transaction.
    pub async fn apply_batch(
        &self,
        partition_id: PartitionId,
        previous_position: Option<&LogPosition>,
        applied_position: &LogPosition,
        changes: &[SearchDocumentChange],
    ) -> Result<(), ProjectionError> {
        validate_position(applied_position)?;
        let mut has_retried = false;
        loop {
            let mut slot = self.backend.lock_next_slot().await;
            if slot.is_disconnected() {
                slot.reconnect(self.backend.configuration()).await?;
            }
            let result = tokio::time::timeout(
                DATABASE_OPERATION_TIMEOUT,
                apply_batch_transaction(
                    &mut slot.client,
                    partition_id,
                    previous_position,
                    applied_position,
                    changes,
                ),
            )
            .await;
            let result = match result {
                Ok(result) => result,
                Err(_) => {
                    slot.invalidate();
                    Err(operation_timeout("apply batch to"))
                }
            };
            if should_retry(&result, has_retried) {
                has_retried = true;
                slot.reconnect(self.backend.configuration()).await?;
                continue;
            }
            drop(slot);
            return result.map(|skipped| self.record_skipped_documents(skipped));
        }
    }

    /// Rebuilds one partition from a consistent committed local view at `applied_position`.
    pub async fn rebuild_partition(
        &self,
        partition_id: PartitionId,
        applied_position: &LogPosition,
        committed_view: Arc<dyn FlowStateReader + Send + Sync>,
    ) -> Result<(), ProjectionError> {
        validate_position(applied_position)?;
        let mut has_retried = false;
        loop {
            let mut slot = self.backend.lock_next_slot().await;
            if slot.is_disconnected() {
                slot.reconnect(self.backend.configuration()).await?;
            }
            let result = tokio::time::timeout(
                DATABASE_OPERATION_TIMEOUT,
                rebuild_transaction(
                    &mut slot.client,
                    partition_id,
                    applied_position,
                    committed_view.clone(),
                ),
            )
            .await;
            let result = match result {
                Ok(result) => result,
                Err(_) => {
                    slot.invalidate();
                    Err(operation_timeout("rebuild partition in"))
                }
            };
            if should_retry(&result, has_retried) {
                has_retried = true;
                slot.reconnect(self.backend.configuration()).await?;
                continue;
            }
            drop(slot);
            return result.map(|skipped| self.record_skipped_documents(skipped));
        }
    }

    fn record_skipped_documents(&self, skipped_documents: Vec<SkippedSearchDocument>) {
        for skipped in skipped_documents {
            tracing::error!(
                partition_id = skipped.partition_id.0,
                tenant_id = skipped.tenant_id.0,
                run_id = %skipped.run_id,
                reason = %skipped.reason,
                "committed search document omitted from ParadeDB projection"
            );
            if let Some(recorder) = &self.metric_recorder {
                recorder.record_skipped_search_document(
                    skipped.partition_id,
                    skipped.tenant_id,
                    skipped.run_id,
                );
            }
        }
    }
}

async fn apply_batch_transaction(
    client: &mut tokio_postgres::Client,
    partition_id: PartitionId,
    previous_position: Option<&LogPosition>,
    applied_position: &LogPosition,
    changes: &[SearchDocumentChange],
) -> Result<Vec<SkippedSearchDocument>, ProjectionError> {
    let transaction = client
        .transaction()
        .await
        .map_err(|source| database_error("begin", source))?;
    let current = lock_progress(&transaction, partition_id).await?;
    if is_replay(current.as_ref(), applied_position) {
        transaction
            .commit()
            .await
            .map_err(|source| database_error("commit replay of", source))?;
        return Ok(Vec::new());
    }
    if current.as_ref() != previous_position {
        return Err(ProjectionError::PositionMismatch {
            partition_id,
            stored: current,
            expected: previous_position.cloned(),
            offered: applied_position.clone(),
        });
    }
    let mut skipped_documents = Vec::new();
    for change in changes {
        if let Some(skipped) = apply_change(&transaction, partition_id, change).await? {
            skipped_documents.push(skipped);
        }
    }
    update_progress(&transaction, partition_id, applied_position).await?;
    transaction
        .commit()
        .await
        .map_err(|source| database_error("commit", source))?;
    Ok(skipped_documents)
}

async fn rebuild_transaction(
    client: &mut tokio_postgres::Client,
    partition_id: PartitionId,
    applied_position: &LogPosition,
    committed_view: Arc<dyn FlowStateReader + Send + Sync>,
) -> Result<Vec<SkippedSearchDocument>, ProjectionError> {
    let transaction = client
        .transaction()
        .await
        .map_err(|source| database_error("begin rebuild of", source))?;
    let current = lock_progress(&transaction, partition_id).await?;
    if current.as_ref().is_some_and(|stored| {
        same_stream(stored, applied_position) && stored.seq > applied_position.seq
    }) {
        return Err(ProjectionError::StaleRebuild {
            partition_id,
            current: current.expect("stale comparison requires stored position"),
            offered: applied_position.clone(),
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
    transaction
        .execute_typed(
            "DELETE FROM dexzd_visibility_skipped_documents WHERE partition_id = $1::integer",
            &[(&partition_number as &(dyn ToSql + Sync), Type::INT4)],
        )
        .await
        .map_err(|source| database_error("remove stale skipped documents from", source))?;
    let (mut start, end) = partition_index_range(partition_id, IndexKind::SearchDocument);
    let mut skipped_documents = Vec::new();
    loop {
        let view = committed_view.clone();
        let scan_start = start.clone();
        let scan_end = end.clone();
        let page = tokio::task::spawn_blocking(move || {
            read_rebuild_page(view.as_ref(), partition_id, &scan_start, &scan_end)
        })
        .await
        .map_err(|error| {
            ProjectionError::InvalidState(format!("search rebuild reader task failed: {error}"))
        })??;
        let Some(RebuildPage {
            next_start,
            changes,
        }) = page
        else {
            break;
        };
        start = next_start;
        for change in changes {
            if let Some(skipped) = apply_change(&transaction, partition_id, &change).await? {
                skipped_documents.push(skipped);
            }
        }
    }
    update_progress(&transaction, partition_id, applied_position).await?;
    transaction
        .commit()
        .await
        .map_err(|source| database_error("commit rebuild of", source))?;
    Ok(skipped_documents)
}

fn read_rebuild_page(
    committed_view: &dyn FlowStateReader,
    partition_id: PartitionId,
    start: &[u8],
    end: &[u8],
) -> Result<Option<RebuildPage>, ProjectionError> {
    let rows = committed_view.scan(
        ColumnFamily::PartitionIndex,
        start,
        end,
        REBUILD_SCAN_PAGE_SIZE,
    )?;
    let Some((last_key, _)) = rows.last() else {
        return Ok(None);
    };
    if last_key.as_slice() < start {
        return Err(ProjectionError::InvalidState(
            "search index scan regressed".into(),
        ));
    }
    let mut next_start = last_key.clone();
    next_start.push(0);
    let mut changes = Vec::with_capacity(rows.len());
    for (index_key, _) in rows {
        let (tenant_id, flow_hash, run_id) = decode_search_index_key(partition_id, &index_key)?
            .ok_or_else(|| ProjectionError::InvalidState("search index left partition".into()))?;
        let document_key = run_singleton_key(flow_hash, run_id, RunTable::SearchDocument);
        let encoded_document = committed_view
            .get(ColumnFamily::Flow, &document_key)?
            .ok_or_else(|| {
                ProjectionError::InvalidState(format!(
                    "missing committed search document for {run_id}"
                ))
            })?;
        changes.push(SearchDocumentChange::Upsert {
            tenant_id,
            run_id,
            encoded_document,
        });
    }
    Ok(Some(RebuildPage {
        next_start,
        changes,
    }))
}

async fn apply_change(
    transaction: &Transaction<'_>,
    partition_id: PartitionId,
    change: &SearchDocumentChange,
) -> Result<Option<SkippedSearchDocument>, ProjectionError> {
    match change {
        SearchDocumentChange::Upsert {
            tenant_id,
            run_id,
            encoded_document,
        } => match row_from_document(partition_id, *tenant_id, *run_id, encoded_document) {
            Ok(row) => {
                let statement = upsert_statement(&row);
                execute_statement(transaction, partition_id, *run_id, &statement).await?;
                delete_skipped_document(transaction, *tenant_id, *run_id).await?;
            }
            Err(reason) => {
                delete_search_row(transaction, partition_id, *tenant_id, *run_id).await?;
                record_skipped_document(transaction, partition_id, *tenant_id, *run_id, &reason)
                    .await?;
                return Ok(Some(SkippedSearchDocument {
                    partition_id,
                    tenant_id: *tenant_id,
                    run_id: *run_id,
                    reason: reason.to_string(),
                }));
            }
        },
        SearchDocumentChange::Delete { tenant_id, run_id } => {
            delete_search_row(transaction, partition_id, *tenant_id, *run_id).await?;
            delete_skipped_document(transaction, *tenant_id, *run_id).await?;
        }
    }
    Ok(None)
}

async fn record_skipped_document(
    transaction: &Transaction<'_>,
    partition_id: PartitionId,
    tenant_id: TenantId,
    run_id: RunId,
    reason: &ProjectionError,
) -> Result<(), ProjectionError> {
    let tenant_column = super::super::tenant_column_value(tenant_id);
    let partition_number = i32::from(partition_id.0);
    let run_column = run_id.to_string();
    let reason_column = reason.to_string();
    transaction
        .execute_typed(
            "INSERT INTO dexzd_visibility_skipped_documents (tenant_id, partition_id, run_id, reason) VALUES ($1::text, $2::integer, $3::text, $4::text) ON CONFLICT (tenant_id, run_id) DO UPDATE SET partition_id = EXCLUDED.partition_id, reason = EXCLUDED.reason",
            &[
                (&tenant_column as &(dyn ToSql + Sync), Type::TEXT),
                (&partition_number, Type::INT4),
                (&run_column, Type::TEXT),
                (&reason_column, Type::TEXT),
            ],
        )
        .await
        .map_err(|source| database_error("record skipped document in", source))?;
    Ok(())
}

async fn delete_skipped_document(
    transaction: &Transaction<'_>,
    tenant_id: TenantId,
    run_id: RunId,
) -> Result<(), ProjectionError> {
    let tenant_column = super::super::tenant_column_value(tenant_id);
    let run_column = run_id.to_string();
    transaction
        .execute_typed(
            "DELETE FROM dexzd_visibility_skipped_documents WHERE tenant_id = $1::text AND run_id = $2::text",
            &[
                (&tenant_column as &(dyn ToSql + Sync), Type::TEXT),
                (&run_column, Type::TEXT),
            ],
        )
        .await
        .map_err(|source| database_error("clear skipped document from", source))?;
    Ok(())
}

async fn delete_search_row(
    transaction: &Transaction<'_>,
    partition_id: PartitionId,
    tenant_id: TenantId,
    run_id: RunId,
) -> Result<(), ProjectionError> {
    let tenant_column = super::super::tenant_column_value(tenant_id);
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
    let document = IndexedDocument::from_record(record.clone(), tenant_id, run_id)
        .map_err(|error| ProjectionError::InvalidState(error.to_string()))?;
    let mut row = SearchRow::from_document(tenant_id, version, &document, &application_fields)
        .and_then(|row| row.with_wire_attributes(&record.attributes))
        .map_err(|error| ProjectionError::InvalidState(error.to_string()))?;
    row.partition_id = i32::from(partition_id.0);
    Ok(row)
}

async fn execute_statement(
    transaction: &Transaction<'_>,
    partition_id: PartitionId,
    run_id: RunId,
    statement: &SqlStatement,
) -> Result<(), ProjectionError> {
    transaction
        .execute_typed(&statement.sql, &statement.typed_parameter_refs())
        .await
        .map_err(|source| ProjectionError::RowDatabase {
            partition_id,
            run_id,
            source,
        })?;
    Ok(())
}

async fn lock_progress(
    transaction: &Transaction<'_>,
    partition_id: PartitionId,
) -> Result<Option<LogPosition>, ProjectionError> {
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
            "SELECT applied_node_id, applied_incarnation, applied_seq, applied_position FROM dexzd_visibility_progress WHERE partition_id = $1::integer FOR UPDATE",
            &[(&partition_number as &(dyn ToSql + Sync), Type::INT4)],
        )
        .await
        .map_err(|source| database_error("lock progress for", source))?;
    position_from_row(&row)
}

async fn update_progress(
    transaction: &Transaction<'_>,
    partition_id: PartitionId,
    position: &LogPosition,
) -> Result<(), ProjectionError> {
    let sequence = i64::try_from(position.seq)
        .map_err(|_| ProjectionError::InvalidState("log sequence exceeds bigint".into()))?;
    let partition_number = i32::from(partition_id.0);
    let incarnation = i64::from(position.incarnation);
    transaction
        .execute_typed(
            "UPDATE dexzd_visibility_progress SET applied_position = $2::bigint, applied_node_id = $3::text, applied_incarnation = $4::bigint, applied_seq = $5::bigint WHERE partition_id = $1::integer",
            &[
                (&partition_number as &(dyn ToSql + Sync), Type::INT4),
                (&sequence, Type::INT8),
                (&position.node_id, Type::TEXT),
                (&incarnation, Type::INT8),
                (&sequence, Type::INT8),
            ],
        )
        .await
        .map_err(|source| database_error("update progress for", source))?;
    Ok(())
}

fn position_from_row(row: &tokio_postgres::Row) -> Result<Option<LogPosition>, ProjectionError> {
    let node_id: Option<String> = row
        .try_get(0)
        .map_err(|source| database_error("decode progress node from", source))?;
    let incarnation: Option<i64> = row
        .try_get(1)
        .map_err(|source| database_error("decode progress incarnation from", source))?;
    let sequence: Option<i64> = row
        .try_get(2)
        .map_err(|source| database_error("decode progress sequence from", source))?;
    let legacy_sequence: i64 = row
        .try_get(3)
        .map_err(|source| database_error("decode legacy progress from", source))?;
    match (node_id, incarnation, sequence) {
        (None, None, None) if legacy_sequence == 0 => Ok(None),
        (Some(node_id), Some(incarnation), Some(sequence)) => Ok(Some(LogPosition {
            node_id,
            incarnation: u32::try_from(incarnation).map_err(|_| {
                ProjectionError::InvalidState("invalid ParadeDB progress incarnation".into())
            })?,
            seq: u64::try_from(sequence).map_err(|_| {
                ProjectionError::InvalidState("invalid ParadeDB progress sequence".into())
            })?,
        })),
        _ => Err(ProjectionError::InvalidState(
            "incomplete ParadeDB projection position".into(),
        )),
    }
}

fn validate_position(position: &LogPosition) -> Result<(), ProjectionError> {
    if position.node_id.is_empty() || position.seq == 0 {
        return Err(ProjectionError::InvalidState("invalid log position".into()));
    }
    i64::try_from(position.seq)
        .map(|_| ())
        .map_err(|_| ProjectionError::InvalidState("log sequence exceeds bigint".into()))
}

fn same_stream(left: &LogPosition, right: &LogPosition) -> bool {
    left.node_id == right.node_id && left.incarnation == right.incarnation
}

fn is_replay(stored: Option<&LogPosition>, offered: &LogPosition) -> bool {
    stored.is_some_and(|stored| same_stream(stored, offered) && stored.seq >= offered.seq)
}

fn database_error(operation: &'static str, source: tokio_postgres::Error) -> ProjectionError {
    ProjectionError::Database { operation, source }
}

fn operation_timeout(operation: &'static str) -> ProjectionError {
    ProjectionError::Connection(SearchError::Unavailable(format!(
        "{operation} ParadeDB projection timed out after {DATABASE_OPERATION_TIMEOUT:?}"
    )))
}

fn should_retry(result: &Result<impl Sized, ProjectionError>, has_retried: bool) -> bool {
    !has_retried
        && matches!(result, Err(ProjectionError::Database { source, .. } | ProjectionError::RowDatabase { source, .. }) if is_transient_database_error(source))
}

#[cfg(test)]
mod tests {
    use dexzd_protocol::dexzd::v1::LogPosition;

    use super::validate_position;

    #[test]
    fn permits_zero_incarnation_with_positive_sequence() {
        let position = LogPosition {
            node_id: "node".into(),
            incarnation: 0,
            seq: 1,
        };
        assert!(validate_position(&position).is_ok());
    }
}
