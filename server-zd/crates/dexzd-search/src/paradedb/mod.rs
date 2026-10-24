// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! ParadeDB schema, projection rows, and parameterized SearchFlows SQL.

mod projection;
mod query;
mod row;
mod schema;
mod search_backend;
mod search_result;
mod upsert;
mod wire_value;

use dexzd_keyspace::TenantId;
use tokio_postgres::types::{ToSql, Type};

pub use projection::{
    ParadeDbProjector, ProjectionError, SearchDocumentChange,
    extract_batch_search_document_changes, extract_search_document_changes,
};
pub use query::{PageCursor, SqlTranslationError, translate_query};
pub use row::{SearchRow, SearchRowError};
pub use schema::{SchemaError, ensure_schema};
pub use search_backend::{ParadeDbSearchBackend, ProjectionVisibilityWait};
pub use upsert::upsert_statement;

pub(crate) fn tenant_column_value(tenant_id: TenantId) -> String {
    tenant_id.0.to_string()
}

/// Fresh ParadeDB migration for versioned Dex run documents.
pub const MIGRATION_SQL: &str = include_str!("migration.sql");

/// One parameterized SQL statement and its owned bind values.
#[derive(Debug, Clone, PartialEq)]
pub struct SqlStatement {
    /// SQL text with positional placeholders.
    pub sql: String,
    /// Values in placeholder order.
    pub parameters: Vec<SqlValue>,
}

impl SqlStatement {
    /// Borrows parameters in the form expected by tokio-postgres.
    #[must_use]
    pub fn parameter_refs(&self) -> Vec<&(dyn ToSql + Sync)> {
        self.parameters.iter().map(SqlValue::as_to_sql).collect()
    }

    pub(crate) fn typed_parameter_refs(&self) -> Vec<(&(dyn ToSql + Sync), Type)> {
        self.parameters
            .iter()
            .map(|parameter| (parameter.as_to_sql(), parameter.postgres_type()))
            .collect()
    }
}

/// A supported typed SQL bind value.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum SqlValue {
    /// Required text.
    Text(String),
    /// Nullable text.
    OptionalText(Option<String>),
    /// Required signed integer.
    Int(i64),
    /// Required 32-bit integer.
    Int32(i32),
    /// Nullable signed integer.
    OptionalInt(Option<i64>),
    /// Required double.
    Double(f64),
    /// Required boolean.
    Bool(bool),
    /// Required text array.
    TextArray(Vec<String>),
    /// Nullable text array.
    OptionalTextArray(Option<Vec<String>>),
}

impl SqlValue {
    fn as_to_sql(&self) -> &(dyn ToSql + Sync) {
        match self {
            Self::Text(value) => value,
            Self::OptionalText(value) => value,
            Self::Int(value) => value,
            Self::Int32(value) => value,
            Self::OptionalInt(value) => value,
            Self::Double(value) => value,
            Self::Bool(value) => value,
            Self::TextArray(value) => value,
            Self::OptionalTextArray(value) => value,
        }
    }

    fn postgres_type(&self) -> Type {
        match self {
            Self::Text(_) | Self::OptionalText(_) => Type::TEXT,
            Self::Int(_) | Self::OptionalInt(_) => Type::INT8,
            Self::Int32(_) => Type::INT4,
            Self::Double(_) => Type::FLOAT8,
            Self::Bool(_) => Type::BOOL,
            Self::TextArray(_) | Self::OptionalTextArray(_) => Type::TEXT_ARRAY,
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod integration;
