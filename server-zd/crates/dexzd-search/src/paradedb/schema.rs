// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use thiserror::Error;
use tokio_postgres::Client;

use super::MIGRATION_SQL;

const SCHEMA_VERSION: i32 = 1;

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
        .batch_execute("SELECT pg_advisory_xact_lock(5589034107693961330); CREATE TABLE IF NOT EXISTS dexzd_visibility_schema_version (id integer PRIMARY KEY CHECK (id = 1), version integer NOT NULL); INSERT INTO dexzd_visibility_schema_version (id, version) VALUES (1, 1) ON CONFLICT (id) DO NOTHING")
        .await
        .map_err(|source| SchemaError::Database {
            operation: "lock and initialize",
            source,
        })?;
    let version_row = transaction
        .query_typed_one(
            "SELECT version FROM dexzd_visibility_schema_version WHERE id = 1 FOR UPDATE",
            &[],
        )
        .await
        .map_err(|source| SchemaError::Database {
            operation: "read version of",
            source,
        })?;
    let version: i32 = version_row.get(0);
    if version > SCHEMA_VERSION {
        return Err(SchemaError::NewerVersion {
            found: version,
            supported: SCHEMA_VERSION,
        });
    }
    transaction
        .batch_execute(MIGRATION_SQL)
        .await
        .map_err(|source| SchemaError::Database {
            operation: "migrate",
            source,
        })?;
    transaction
        .commit()
        .await
        .map_err(|source| SchemaError::Database {
            operation: "commit",
            source,
        })
}
