// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;

use dexzd_keyspace::TenantId;
use dexzd_protocol::dex::IndexType;
use dexzd_protocol::dexzd::v1::SearchAttributeValue;
use dexzd_visibility_query::{SearchDocument, SearchValue};
use thiserror::Error;

use super::tenant_column_value;
use super::wire_value::encode_wire_value;

/// A document ready for a versioned ParadeDB upsert.
#[derive(Debug, Clone)]
pub struct SearchRow {
    /// Tenant scope of the run.
    pub tenant_id: String,
    /// Stable run identifier.
    pub run_id: String,
    /// Flow identifier.
    pub workflow_id: String,
    /// Workflow type name.
    pub workflow_type: String,
    /// Dex Flow type name.
    pub flow_type: String,
    /// Public execution status name.
    pub execution_status: String,
    /// Start time in Unix nanoseconds.
    pub start_time_ns: i64,
    /// Close time in Unix nanoseconds, if closed.
    pub close_time_ns: Option<i64>,
    /// Parent Flow identifier, if present.
    pub dex_parent_flow_id: Option<String>,
    /// Active Step type names, if indexed.
    pub active_step_types: Option<Vec<String>>,
    /// Work Queue permission names, if indexed.
    pub dex_work_queue_permissions: Option<Vec<String>>,
    /// Monotonically increasing projection version.
    pub version: i64,
    /// Keyword and keyword-array application attributes.
    pub sa_keyword: BTreeMap<String, SearchValue>,
    /// Full-text application attributes.
    pub sa_text: BTreeMap<String, SearchValue>,
    /// Integer and double application attributes.
    pub sa_number: BTreeMap<String, SearchValue>,
    /// Recorded numeric index types for exact response encoding.
    pub sa_number_types: BTreeMap<String, SearchValue>,
    /// Datetime application attributes.
    pub sa_datetime: BTreeMap<String, SearchValue>,
    /// Boolean application attributes.
    pub sa_bool: BTreeMap<String, SearchValue>,
    /// Original encoded attribute values for response fidelity.
    pub sa_wire: BTreeMap<String, SearchValue>,
}

/// Invalid projection input that cannot be stored as a ParadeDB row.
#[derive(Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum SearchRowError {
    /// A required identity or timestamp is absent.
    #[error("missing required search field {field}")]
    MissingField { field: &'static str },
    /// A field's value differs from its registered index type.
    #[error("search field {field} does not match index type {index_type:?}")]
    TypeMismatch {
        field: String,
        index_type: IndexType,
    },
    /// A double cannot be represented as a JSON number.
    #[error("search field {field} must be a finite double")]
    NonFiniteDouble { field: String },
    /// A registered application field attempts to shadow a system field.
    #[error("application field {field} shadows a system field")]
    SystemFieldCollision { field: String },
    /// A projected attribute lacks its stored wire value.
    #[error("search field {field} has no stored wire value")]
    MissingWireValue { field: String },
}

impl SearchRow {
    /// Maps a typed search document and application field definitions into one versioned row.
    pub fn from_document(
        tenant_id: TenantId,
        version: i64,
        document: &impl SearchDocument,
        application_fields: &[(String, IndexType)],
    ) -> Result<Self, SearchRowError> {
        let mut row = Self {
            tenant_id: tenant_column_value(tenant_id),
            run_id: required_string(document, "RunId")?,
            workflow_id: required_string(document, "WorkflowId")?,
            workflow_type: required_string(document, "WorkflowType")?,
            flow_type: required_string(document, "FlowType")?,
            execution_status: required_string(document, "ExecutionStatus")?,
            start_time_ns: required_datetime(document, "StartTime")?,
            close_time_ns: optional_datetime(document, "CloseTime")?,
            dex_parent_flow_id: optional_string(document, "DexParentFlowID")?,
            active_step_types: optional_string_array(document, "ActiveStepTypes")?,
            dex_work_queue_permissions: optional_string_array(document, "DexWorkQueuePermissions")?,
            version,
            sa_keyword: BTreeMap::new(),
            sa_text: BTreeMap::new(),
            sa_number: BTreeMap::new(),
            sa_number_types: BTreeMap::new(),
            sa_datetime: BTreeMap::new(),
            sa_bool: BTreeMap::new(),
            sa_wire: BTreeMap::new(),
        };
        for (field, index_type) in application_fields {
            if is_system_field(field) {
                return Err(SearchRowError::SystemFieldCollision {
                    field: field.clone(),
                });
            }
            let Some(value) = document.field_value(field) else {
                continue;
            };
            if *index_type == IndexType::Double
                && matches!(&value, SearchValue::Double(number) if !number.is_finite())
            {
                return Err(SearchRowError::NonFiniteDouble {
                    field: field.clone(),
                });
            }
            let bucket = match index_type {
                IndexType::Keyword if matches!(value, SearchValue::String(_)) => {
                    &mut row.sa_keyword
                }
                IndexType::KeywordArray if matches!(value, SearchValue::StringArray(_)) => {
                    &mut row.sa_keyword
                }
                IndexType::Text if matches!(value, SearchValue::String(_)) => &mut row.sa_text,
                IndexType::Int if matches!(value, SearchValue::Int(_)) => &mut row.sa_number,
                IndexType::Double if matches!(value, SearchValue::Double(_)) => &mut row.sa_number,
                IndexType::Datetime if matches!(value, SearchValue::Datetime(_)) => {
                    &mut row.sa_datetime
                }
                IndexType::Bool if matches!(value, SearchValue::Bool(_)) => &mut row.sa_bool,
                _ => {
                    return Err(SearchRowError::TypeMismatch {
                        field: field.clone(),
                        index_type: *index_type,
                    });
                }
            };
            bucket.insert(field.clone(), value);
            if matches!(index_type, IndexType::Int | IndexType::Double) {
                row.sa_number_types.insert(
                    field.clone(),
                    SearchValue::Int(i64::from(*index_type as i32)),
                );
            }
        }
        Ok(row)
    }

    /// Preserves original values when projecting a stored search document record.
    pub fn with_wire_attributes(
        mut self,
        attributes: &[SearchAttributeValue],
    ) -> Result<Self, SearchRowError> {
        for attribute in attributes {
            let value =
                attribute
                    .value
                    .as_ref()
                    .ok_or_else(|| SearchRowError::MissingWireValue {
                        field: attribute.index_key.clone(),
                    })?;
            self.sa_wire
                .insert(attribute.index_key.clone(), encode_wire_value(value));
        }
        Ok(self)
    }
}

fn required_string(
    document: &impl SearchDocument,
    field: &'static str,
) -> Result<String, SearchRowError> {
    match document.field_value(field) {
        Some(SearchValue::String(value)) => Ok(value),
        Some(_) => Err(SearchRowError::TypeMismatch {
            field: field.to_owned(),
            index_type: IndexType::Keyword,
        }),
        None => Err(SearchRowError::MissingField { field }),
    }
}

fn optional_string(
    document: &impl SearchDocument,
    field: &'static str,
) -> Result<Option<String>, SearchRowError> {
    match document.field_value(field) {
        Some(SearchValue::String(value)) => Ok(Some(value)),
        Some(_) => Err(SearchRowError::TypeMismatch {
            field: field.to_owned(),
            index_type: IndexType::Keyword,
        }),
        None => Ok(None),
    }
}

fn required_datetime(
    document: &impl SearchDocument,
    field: &'static str,
) -> Result<i64, SearchRowError> {
    match document.field_value(field) {
        Some(SearchValue::Datetime(value)) => Ok(value),
        Some(_) => Err(SearchRowError::TypeMismatch {
            field: field.to_owned(),
            index_type: IndexType::Datetime,
        }),
        None => Err(SearchRowError::MissingField { field }),
    }
}

fn optional_datetime(
    document: &impl SearchDocument,
    field: &'static str,
) -> Result<Option<i64>, SearchRowError> {
    match document.field_value(field) {
        Some(SearchValue::Datetime(value)) => Ok(Some(value)),
        Some(_) => Err(SearchRowError::TypeMismatch {
            field: field.to_owned(),
            index_type: IndexType::Datetime,
        }),
        None => Ok(None),
    }
}

fn optional_string_array(
    document: &impl SearchDocument,
    field: &'static str,
) -> Result<Option<Vec<String>>, SearchRowError> {
    match document.field_value(field) {
        Some(SearchValue::StringArray(value)) => Ok(Some(value)),
        Some(_) => Err(SearchRowError::TypeMismatch {
            field: field.to_owned(),
            index_type: IndexType::KeywordArray,
        }),
        None => Ok(None),
    }
}

fn is_system_field(field: &str) -> bool {
    matches!(
        field,
        "WorkflowId"
            | "RunId"
            | "WorkflowType"
            | "FlowType"
            | "ExecutionStatus"
            | "StartTime"
            | "CloseTime"
            | "DexParentFlowID"
            | "ActiveStepTypes"
            | "DexWorkQueuePermissions"
    )
}
