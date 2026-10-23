// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Parse, type-check, and evaluate Dex SearchFlows visibility queries.

mod evaluator;
mod lexer;
mod parser;

use std::collections::BTreeMap;

use dexzd_protocol::dex::IndexType;
use thiserror::Error;

pub use evaluator::{SearchDocument, SearchValue, compare_documents, evaluate};

/// A visibility query error with its byte offset in the source query.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("visibility query at byte {offset}: {message}")]
pub struct QueryError {
    /// Byte offset of the invalid or unsupported input.
    pub offset: usize,
    /// Explanation suitable for an INVALID_ARGUMENT response.
    pub message: String,
}

impl QueryError {
    pub(crate) fn new(offset: usize, message: impl Into<String>) -> Self {
        Self {
            offset,
            message: message.into(),
        }
    }
}

/// Application search attributes and their public Dex index types.
#[derive(Debug, Default, Clone)]
pub struct FieldRegistry {
    fields: BTreeMap<String, IndexType>,
}

impl FieldRegistry {
    /// Creates an empty application field registry; system fields are available automatically.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers an application field by its exact name and index type.
    pub fn insert(&mut self, name: impl Into<String>, index_type: IndexType) {
        self.fields.insert(name.into(), index_type);
    }

    fn index_type(&self, name: &str) -> Option<IndexType> {
        system_index_type(name).or_else(|| self.fields.get(name).copied())
    }
}

fn system_index_type(name: &str) -> Option<IndexType> {
    match name {
        "WorkflowId" | "RunId" | "WorkflowType" | "FlowType" | "ExecutionStatus"
        | "DexParentFlowID" => Some(IndexType::Keyword),
        "StartTime" | "CloseTime" => Some(IndexType::Datetime),
        "ActiveStepTypes" | "DexWorkQueuePermissions" => Some(IndexType::KeywordArray),
        _ => None,
    }
}

/// A parsed and type-checked SearchFlows query.
#[derive(Debug, Clone)]
pub struct Query {
    expression: Expression,
    order_by: Option<OrderBy>,
}

impl Query {
    /// Borrows the validated filter expression for backend translation.
    #[must_use]
    pub fn expression(&self) -> &Expression {
        &self.expression
    }

    /// Returns the explicit sort field and direction, when ORDER BY was supplied.
    #[must_use]
    pub fn order_by(&self) -> Option<(&str, SortDirection)> {
        self.order_by
            .as_ref()
            .map(|order| (order.field.as_str(), order.direction))
    }
}

/// Direction of an explicit ORDER BY clause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    /// Ascending order.
    Ascending,
    /// Descending order.
    Descending,
}

#[derive(Debug, Clone)]
struct OrderBy {
    field: String,
    direction: SortDirection,
}

/// A validated Boolean visibility expression.
#[derive(Debug, Clone)]
pub enum Expression {
    /// Matches every document.
    True,
    /// Negates an expression.
    Not(Box<Expression>),
    /// Requires both expressions.
    And(Box<Expression>, Box<Expression>),
    /// Requires either expression.
    Or(Box<Expression>, Box<Expression>),
    /// Tests one indexed field.
    Predicate(Predicate),
}

/// A validated comparison against one indexed field.
#[derive(Debug, Clone)]
pub struct Predicate {
    field: String,
    index_type: IndexType,
    operator: Operator,
    literals: Vec<Literal>,
}

impl Predicate {
    /// Returns the registered field name.
    #[must_use]
    pub fn field(&self) -> &str {
        &self.field
    }

    /// Returns the registered field type.
    #[must_use]
    pub fn index_type(&self) -> IndexType {
        self.index_type
    }

    /// Returns the validated operator.
    #[must_use]
    pub fn operator(&self) -> Operator {
        self.operator
    }

    /// Returns the typed comparison literals.
    #[must_use]
    pub fn literals(&self) -> &[Literal] {
        &self.literals
    }
}

/// A validated visibility comparison operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operator {
    /// Equal.
    Equal,
    /// Not equal.
    NotEqual,
    /// Greater than.
    Greater,
    /// Greater than or equal.
    GreaterEqual,
    /// Less than.
    Less,
    /// Less than or equal.
    LessEqual,
    /// Membership in a literal list.
    In,
    /// Exclusion from a literal list.
    NotIn,
    /// Inclusive range.
    Between,
    /// Prefix match.
    StartsWith,
    /// Missing field.
    IsNull,
    /// Present field.
    IsNotNull,
}

/// A literal checked against its field's index type.
#[derive(Debug, Clone)]
pub enum Literal {
    /// Keyword, text, or array-member string.
    String(String),
    /// Signed integer.
    Int(i64),
    /// Double-precision number.
    Double(f64),
    /// Boolean value.
    Bool(bool),
    /// Unix nanoseconds.
    Datetime(i64),
}

/// Parses and type-checks a SearchFlows query. Empty input matches all documents; errors identify source byte offsets.
pub fn parse_and_check(input: &str, fields: &FieldRegistry) -> Result<Query, QueryError> {
    parser::parse(input, fields)
}
