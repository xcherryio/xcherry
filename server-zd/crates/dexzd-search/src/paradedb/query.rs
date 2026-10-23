// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_protocol::dex::IndexType;
use dexzd_visibility_query::{Expression, Literal, Operator, Predicate, Query, SortDirection};
use thiserror::Error;

use super::{SearchRow, SqlStatement, SqlValue};

pub(super) const SELECT_COLUMNS: &str = "tenant_id, run_id, workflow_id, workflow_type, flow_type, execution_status, start_time_ns, close_time_ns, dex_parent_flow_id, active_step_types, dex_work_queue_permissions, version, sa_keyword::text AS sa_keyword, sa_text::text AS sa_text, sa_number::text AS sa_number, sa_number_types::text AS sa_number_types, sa_datetime::text AS sa_datetime, sa_bool::text AS sa_bool, sa_wire::text AS sa_wire";
pub(super) const DEFAULT_ORDER: &str = "ORDER BY is_running DESC, close_time_ns DESC NULLS LAST, start_time_ns DESC, run_id COLLATE \"C\" ASC";

/// Keyset position for one returned run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageCursor {
    /// Whether the run was running when the page was read.
    pub is_running: bool,
    /// Close time in Unix nanoseconds, if closed.
    pub close_time_ns: Option<i64>,
    /// Start time in Unix nanoseconds.
    pub start_time_ns: i64,
    /// Stable run identifier used to break sort ties.
    pub run_id: String,
}

impl PageCursor {
    /// Captures the sort position of a projected row.
    #[must_use]
    pub fn from_row(row: &SearchRow) -> Self {
        Self {
            is_running: row.execution_status == "Running",
            close_time_ns: row.close_time_ns,
            start_time_ns: row.start_time_ns,
            run_id: row.run_id.clone(),
        }
    }
}

/// Invalid input or unsupported expression for ParadeDB translation.
#[derive(Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum SqlTranslationError {
    /// The query uses a feature this backend cannot express.
    #[error("unsupported ParadeDB visibility query: {feature}")]
    Unsupported { feature: String },
    /// The page size, tenant ID, or cursor is invalid.
    #[error("invalid ParadeDB pagination: {reason}")]
    InvalidPagination { reason: &'static str },
}

/// Selects tenant runs with bound values, query order, and an optional keyset cursor.
pub fn translate_query(
    query: &Query,
    tenant_id: &str,
    page_size: i64,
    cursor: Option<&PageCursor>,
) -> Result<SqlStatement, SqlTranslationError> {
    if tenant_id.is_empty() {
        return Err(SqlTranslationError::InvalidPagination {
            reason: "tenant ID is empty",
        });
    }
    if page_size <= 0 {
        return Err(SqlTranslationError::InvalidPagination {
            reason: "page size must be positive",
        });
    }
    if cursor.is_some_and(|cursor| cursor.run_id.is_empty()) {
        return Err(SqlTranslationError::InvalidPagination {
            reason: "cursor run ID is empty",
        });
    }
    let mut renderer = Renderer {
        parameters: vec![SqlValue::Text(tenant_id.to_owned())],
    };
    let predicate = renderer.render_expression(query.expression())?;
    let mut sql = format!(
        "SELECT {SELECT_COLUMNS} FROM dexzd_visibility_runs WHERE tenant_id = $1::text AND coalesce(({predicate}), FALSE)"
    );
    if contains_application_field(query.expression()) {
        sql.push_str(" AND tenant_id @@@ pdb.term($1::text)");
    }
    if let Some(cursor) = cursor {
        let cursor_sql = renderer.render_cursor(query, cursor)?;
        sql.push_str(" AND (");
        sql.push_str(&cursor_sql);
        sql.push(')');
    }
    let ordering = match query.order_by() {
        None => DEFAULT_ORDER,
        Some(("StartTime", SortDirection::Ascending)) => {
            "ORDER BY start_time_ns ASC, run_id COLLATE \"C\" ASC"
        }
        Some(("StartTime", SortDirection::Descending)) => {
            "ORDER BY start_time_ns DESC, run_id COLLATE \"C\" ASC"
        }
        Some(("CloseTime", SortDirection::Ascending)) => {
            "ORDER BY close_time_ns ASC NULLS LAST, run_id COLLATE \"C\" ASC"
        }
        Some(("CloseTime", SortDirection::Descending)) => {
            "ORDER BY close_time_ns DESC NULLS LAST, run_id COLLATE \"C\" ASC"
        }
        Some((field, _)) => {
            return Err(SqlTranslationError::Unsupported {
                feature: format!("ORDER BY {field}"),
            });
        }
    };
    sql.push(' ');
    sql.push_str(ordering);
    let limit = renderer.bind(SqlValue::Int(page_size), "bigint");
    sql.push_str(&format!(" LIMIT {limit}"));
    Ok(SqlStatement {
        sql,
        parameters: renderer.parameters,
    })
}

struct Renderer {
    parameters: Vec<SqlValue>,
}

impl Renderer {
    fn bind(&mut self, value: SqlValue, sql_type: &str) -> String {
        self.parameters.push(value);
        format!("${}::{sql_type}", self.parameters.len())
    }

    fn render_expression(
        &mut self,
        expression: &Expression,
    ) -> Result<String, SqlTranslationError> {
        match expression {
            Expression::True => Ok("TRUE".to_owned()),
            Expression::Not(inner) => Ok(format!("NOT ({})", self.render_expression(inner)?)),
            Expression::And(children) => self.render_children(children, "AND"),
            Expression::Or(children) => self.render_children(children, "OR"),
            Expression::Predicate(predicate) => self.render_predicate(predicate),
            _ => Err(SqlTranslationError::Unsupported {
                feature: "unknown expression".to_owned(),
            }),
        }
    }

    fn render_children(
        &mut self,
        children: &[Expression],
        operator: &str,
    ) -> Result<String, SqlTranslationError> {
        let mut expressions = Vec::with_capacity(children.len());
        for child in children {
            expressions.push(format!("({})", self.render_expression(child)?));
        }
        Ok(expressions.join(&format!(" {operator} ")))
    }

    fn render_predicate(&mut self, predicate: &Predicate) -> Result<String, SqlTranslationError> {
        if predicate.field() == "WorkflowType"
            && predicate.operator() == Operator::Equal
            && matches!(predicate.literals().first(), Some(Literal::String(value)) if value == "Engine")
        {
            return Ok("TRUE".to_owned());
        }
        if predicate.field() == "ExecutionStatus"
            && predicate.operator() == Operator::Equal
            && matches!(predicate.literals().first(), Some(Literal::String(value)) if value == "ContinuedAsNew")
        {
            return Ok("FALSE".to_owned());
        }
        let source = self.field_source(predicate)?;
        match predicate.operator() {
            Operator::IsNull => return Ok(format!("{} IS NULL", source.expression)),
            Operator::IsNotNull => return Ok(format!("{} IS NOT NULL", source.expression)),
            _ => {}
        }
        if predicate.index_type() == IndexType::Text {
            return self.render_text(predicate, &source.expression);
        }
        if predicate.index_type() == IndexType::KeywordArray {
            return self.render_array(predicate, &source);
        }
        self.render_scalar(predicate, &source.expression)
    }

    fn field_source(&mut self, predicate: &Predicate) -> Result<FieldSource, SqlTranslationError> {
        let system_column = match predicate.field() {
            "WorkflowId" => Some("workflow_id"),
            "RunId" => Some("run_id"),
            "WorkflowType" => Some("workflow_type"),
            "FlowType" => Some("flow_type"),
            "ExecutionStatus" => Some("execution_status"),
            "StartTime" => Some("start_time_ns"),
            "CloseTime" => Some("close_time_ns"),
            "DexParentFlowID" => Some("dex_parent_flow_id"),
            "ActiveStepTypes" => Some("active_step_types"),
            "DexWorkQueuePermissions" => Some("dex_work_queue_permissions"),
            _ => None,
        };
        if let Some(column) = system_column {
            return Ok(FieldSource {
                expression: column.to_owned(),
                is_json_array: false,
            });
        }
        let bucket = match predicate.index_type() {
            IndexType::Keyword | IndexType::KeywordArray => "sa_keyword",
            IndexType::Text => "sa_text",
            IndexType::Int | IndexType::Double => "sa_number",
            IndexType::Datetime => "sa_datetime",
            IndexType::Bool => "sa_bool",
            IndexType::Unspecified => {
                return Err(SqlTranslationError::Unsupported {
                    feature: format!("index type for {}", predicate.field()),
                });
            }
        };
        let name = self.bind(SqlValue::Text(predicate.field().to_owned()), "text");
        let expression = if predicate.index_type() == IndexType::KeywordArray {
            format!("{bucket} -> {name}")
        } else {
            let text_value = format!("{bucket} ->> {name}");
            match predicate.index_type() {
                IndexType::Int | IndexType::Datetime => format!("({text_value})::bigint"),
                IndexType::Double => format!("({text_value})::double precision"),
                IndexType::Bool => format!("({text_value})::boolean"),
                _ => text_value,
            }
        };
        Ok(FieldSource {
            expression,
            is_json_array: predicate.index_type() == IndexType::KeywordArray,
        })
    }

    fn render_scalar(
        &mut self,
        predicate: &Predicate,
        expression: &str,
    ) -> Result<String, SqlTranslationError> {
        let mut comparison =
            |literal: &Literal, operator: &str| -> Result<String, SqlTranslationError> {
                let (parameter, sql_type) = literal_parameter(literal)?;
                let bound = self.bind(parameter, sql_type);
                let left = if predicate.index_type() == IndexType::Keyword {
                    format!("({expression}) COLLATE \"C\"")
                } else {
                    expression.to_owned()
                };
                Ok(format!("{left} {operator} {bound}"))
            };
        let first = &predicate.literals()[0];
        let result = match predicate.operator() {
            Operator::Equal => comparison(first, "=")?,
            Operator::NotEqual => format!("NOT ({})", comparison(first, "=")?),
            Operator::Greater => comparison(first, ">")?,
            Operator::GreaterEqual => comparison(first, ">=")?,
            Operator::Less => comparison(first, "<")?,
            Operator::LessEqual => comparison(first, "<=")?,
            Operator::StartsWith => {
                let (parameter, _) = literal_parameter(first)?;
                let bound = self.bind(parameter, "text");
                format!("left({expression}, char_length({bound})) = {bound}")
            }
            Operator::Between => format!(
                "({}) AND ({})",
                comparison(first, ">=")?,
                comparison(&predicate.literals()[1], "<=")?
            ),
            Operator::In | Operator::NotIn => {
                let parts: Vec<String> = predicate
                    .literals()
                    .iter()
                    .map(|literal| comparison(literal, "="))
                    .collect::<Result<_, _>>()?;
                let any = format!("({})", parts.join(" OR "));
                if predicate.operator() == Operator::NotIn {
                    format!("NOT {any}")
                } else {
                    any
                }
            }
            Operator::IsNull | Operator::IsNotNull => {
                unreachable!("NULL operator handled before scalar rendering")
            }
            _ => {
                return Err(SqlTranslationError::Unsupported {
                    feature: format!("operator {:?}", predicate.operator()),
                });
            }
        };
        Ok(result)
    }

    fn render_array(
        &mut self,
        predicate: &Predicate,
        source: &FieldSource,
    ) -> Result<String, SqlTranslationError> {
        let mut contains = |literal: &Literal| -> Result<String, SqlTranslationError> {
            let (parameter, _) = literal_parameter(literal)?;
            let bound = self.bind(parameter, "text");
            if source.is_json_array {
                Ok(format!(
                    "CASE WHEN ({}) IS NULL THEN NULL ELSE EXISTS (SELECT 1 FROM jsonb_array_elements_text({}) AS element(value) WHERE element.value = {bound}) END",
                    source.expression, source.expression
                ))
            } else {
                Ok(format!("{bound} = ANY({})", source.expression))
            }
        };
        let positive = match predicate.operator() {
            Operator::Equal | Operator::NotEqual => contains(&predicate.literals()[0])?,
            Operator::In | Operator::NotIn => {
                let parts: Vec<String> = predicate
                    .literals()
                    .iter()
                    .map(&mut contains)
                    .collect::<Result<_, _>>()?;
                format!("({})", parts.join(" OR "))
            }
            _ => {
                return Err(SqlTranslationError::Unsupported {
                    feature: format!("{:?} on KEYWORD_ARRAY", predicate.operator()),
                });
            }
        };
        if matches!(predicate.operator(), Operator::NotEqual | Operator::NotIn) {
            Ok(format!("NOT ({positive})"))
        } else {
            Ok(positive)
        }
    }

    fn render_text(
        &mut self,
        predicate: &Predicate,
        expression: &str,
    ) -> Result<String, SqlTranslationError> {
        let mut matches_terms = |literal: &Literal| {
            let Literal::String(value) = literal else {
                return format!("CASE WHEN {expression} IS NULL THEN NULL ELSE FALSE END");
            };
            let terms = tokenize_text(value);
            if terms.is_empty() {
                return format!("CASE WHEN {expression} IS NULL THEN NULL ELSE FALSE END");
            }
            let bound = self.bind(SqlValue::TextArray(terms), "text[]");
            format!("regexp_split_to_array(lower({expression}), '[^[:alnum:]]+') @> {bound}")
        };
        let positive = match predicate.operator() {
            Operator::Equal | Operator::NotEqual => matches_terms(&predicate.literals()[0]),
            Operator::In | Operator::NotIn => {
                let parts: Vec<String> = predicate
                    .literals()
                    .iter()
                    .map(&mut matches_terms)
                    .collect();
                format!("({})", parts.join(" OR "))
            }
            _ => {
                return Err(SqlTranslationError::Unsupported {
                    feature: format!("{:?} on TEXT", predicate.operator()),
                });
            }
        };
        if matches!(predicate.operator(), Operator::NotEqual | Operator::NotIn) {
            Ok(format!("NOT ({positive})"))
        } else {
            Ok(positive)
        }
    }

    fn render_cursor(
        &mut self,
        query: &Query,
        cursor: &PageCursor,
    ) -> Result<String, SqlTranslationError> {
        let run_id = self.bind(SqlValue::Text(cursor.run_id.clone()), "text");
        match query.order_by() {
            None => {
                let running = self.bind(SqlValue::Bool(cursor.is_running), "boolean");
                let close = self.bind(SqlValue::OptionalInt(cursor.close_time_ns), "bigint");
                let start = self.bind(SqlValue::Int(cursor.start_time_ns), "bigint");
                Ok(format!(
                    "is_running < {running} OR (is_running = {running} AND ((close_time_ns IS NULL AND {close} IS NOT NULL) OR close_time_ns < {close} OR (close_time_ns IS NOT DISTINCT FROM {close} AND (start_time_ns < {start} OR (start_time_ns = {start} AND run_id COLLATE \"C\" > {run_id})))))"
                ))
            }
            Some(("StartTime", direction)) => {
                let start = self.bind(SqlValue::Int(cursor.start_time_ns), "bigint");
                let operator = if direction == SortDirection::Ascending {
                    ">"
                } else {
                    "<"
                };
                Ok(format!(
                    "start_time_ns {operator} {start} OR (start_time_ns = {start} AND run_id COLLATE \"C\" > {run_id})"
                ))
            }
            Some(("CloseTime", direction)) => {
                let close = self.bind(SqlValue::OptionalInt(cursor.close_time_ns), "bigint");
                let operator = if direction == SortDirection::Ascending {
                    ">"
                } else {
                    "<"
                };
                Ok(format!(
                    "(close_time_ns IS NULL AND {close} IS NOT NULL) OR (close_time_ns {operator} {close}) OR (close_time_ns IS NOT DISTINCT FROM {close} AND run_id COLLATE \"C\" > {run_id})"
                ))
            }
            Some((field, _)) => Err(SqlTranslationError::Unsupported {
                feature: format!("ORDER BY {field}"),
            }),
        }
    }
}

struct FieldSource {
    expression: String,
    is_json_array: bool,
}

fn literal_parameter(literal: &Literal) -> Result<(SqlValue, &'static str), SqlTranslationError> {
    match literal {
        Literal::String(value) => Ok((SqlValue::Text(value.clone()), "text")),
        Literal::Int(value) | Literal::Datetime(value) => Ok((SqlValue::Int(*value), "bigint")),
        Literal::Double(value) => Ok((SqlValue::Double(*value), "double precision")),
        Literal::Bool(value) => Ok((SqlValue::Bool(*value), "boolean")),
        _ => Err(SqlTranslationError::Unsupported {
            feature: "unknown literal".to_owned(),
        }),
    }
}

fn tokenize_text(value: &str) -> Vec<String> {
    value
        .to_lowercase()
        .split(|character: char| !character.is_alphanumeric())
        .filter(|term| !term.is_empty())
        .map(str::to_owned)
        .collect()
}

fn contains_application_field(expression: &Expression) -> bool {
    match expression {
        Expression::True => false,
        Expression::Not(inner) => contains_application_field(inner),
        Expression::And(children) | Expression::Or(children) => {
            children.iter().any(contains_application_field)
        }
        Expression::Predicate(predicate) => !matches!(
            predicate.field(),
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
        ),
        _ => false,
    }
}
