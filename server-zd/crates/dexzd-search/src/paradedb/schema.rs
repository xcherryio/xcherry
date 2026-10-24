// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use thiserror::Error;
use tokio_postgres::Client;
use tokio_postgres::Transaction;
use tokio_postgres::types::{ToSql, Type};

use super::MIGRATION_SQL;

const SCHEMA_VERSION: i32 = 2;
const SCHEMA_ADVISORY_LOCK: i64 = 5_589_034_107_693_961_330;

/// A failure while creating or checking the ParadeDB visibility schema.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SchemaError {
    /// The database has a schema newer than this binary supports.
    #[error("ParadeDB visibility schema version {found} exceeds supported version {supported}")]
    NewerVersion {
        /// Version stored in ParadeDB.
        found: i32,
        /// Highest version this binary supports.
        supported: i32,
    },
    /// A schema operation failed in Postgres.
    #[error("{operation} ParadeDB visibility schema: {source}")]
    Database {
        /// Failed schema operation.
        operation: &'static str,
        /// Postgres failure.
        #[source]
        source: tokio_postgres::Error,
    },
}

/// Installs or upgrades the visibility schema atomically and rejects newer versions.
pub async fn ensure_schema(client: &mut Client) -> Result<(), SchemaError> {
    let transaction = client
        .transaction()
        .await
        .map_err(|source| SchemaError::Database {
            operation: "begin transaction for",
            source,
        })?;
    transaction
        .batch_execute("SET LOCAL lock_timeout = '5s'")
        .await
        .map_err(|source| SchemaError::Database {
            operation: "set lock timeout for",
            source,
        })?;
    if is_current_version(schema_version(&transaction).await?)? {
        return commit_schema(transaction).await;
    }
    transaction
        .query_typed_one(
            "SELECT pg_advisory_xact_lock($1::bigint)",
            &[(&SCHEMA_ADVISORY_LOCK as &(dyn ToSql + Sync), Type::INT8)],
        )
        .await
        .map_err(|source| SchemaError::Database {
            operation: "lock migration of",
            source,
        })?;
    if is_current_version(schema_version(&transaction).await?)? {
        return commit_schema(transaction).await;
    }
    transaction
        .batch_execute("CREATE TABLE IF NOT EXISTS dexzd_visibility_schema_version (id integer PRIMARY KEY CHECK (id = 1), version integer NOT NULL)")
        .await
        .map_err(|source| SchemaError::Database {
            operation: "create version table for",
            source,
        })?;
    transaction
        .batch_execute(MIGRATION_SQL)
        .await
        .map_err(|source| SchemaError::Database {
            operation: "migrate",
            source,
        })?;
    transaction
        .batch_execute("TRUNCATE TABLE dexzd_visibility_runs, dexzd_visibility_progress")
        .await
        .map_err(|source| SchemaError::Database {
            operation: "clear older projection in",
            source,
        })?;
    transaction
        .execute_typed(
            "INSERT INTO dexzd_visibility_schema_version (id, version) VALUES (1, $1::integer) ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version",
            &[(&SCHEMA_VERSION as &(dyn ToSql + Sync), Type::INT4)],
        )
        .await
        .map_err(|source| SchemaError::Database {
            operation: "record version of",
            source,
        })?;
    commit_schema(transaction).await
}

async fn schema_version(transaction: &Transaction<'_>) -> Result<Option<i32>, SchemaError> {
    let table_row = transaction
        .query_typed_one(
            "SELECT to_regclass('dexzd_visibility_schema_version') IS NOT NULL",
            &[],
        )
        .await
        .map_err(|source| SchemaError::Database {
            operation: "find version table for",
            source,
        })?;
    if !table_row.get::<_, bool>(0) {
        return Ok(None);
    }
    transaction
        .query_typed_opt(
            "SELECT version FROM dexzd_visibility_schema_version WHERE id = 1",
            &[],
        )
        .await
        .map(|row| row.map(|row| row.get(0)))
        .map_err(|source| SchemaError::Database {
            operation: "read version of",
            source,
        })
}

fn is_current_version(version: Option<i32>) -> Result<bool, SchemaError> {
    if let Some(found) = version {
        if found > SCHEMA_VERSION {
            return Err(SchemaError::NewerVersion {
                found,
                supported: SCHEMA_VERSION,
            });
        }
        return Ok(found == SCHEMA_VERSION);
    }
    Ok(false)
}

async fn commit_schema(transaction: Transaction<'_>) -> Result<(), SchemaError> {
    transaction
        .commit()
        .await
        .map_err(|source| SchemaError::Database {
            operation: "commit",
            source,
        })
}
