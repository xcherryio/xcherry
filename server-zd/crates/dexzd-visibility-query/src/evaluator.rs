// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::cmp::Ordering;

use crate::{Expression, Literal, Operator, Predicate, Query, SortDirection};
use dexzd_protocol::dex::IndexType;

/// A typed value stored in a SearchFlows document.
#[derive(Debug, Clone, PartialEq)]
pub enum SearchValue {
    /// A KEYWORD or TEXT field.
    String(String),
    /// A KEYWORD_ARRAY field.
    StringArray(Vec<String>),
    /// An INT field.
    Int(i64),
    /// A DOUBLE field.
    Double(f64),
    /// A BOOL field.
    Bool(bool),
    /// A DATETIME field as Unix nanoseconds.
    Datetime(i64),
}

/// Supplies typed visibility values for one indexed Flow run.
pub trait SearchDocument {
    /// Returns None for NULL; empty KEYWORD_ARRAY values remain StringArray. Dex runs expose WorkflowType as Engine.
    fn field_value(&self, field: &str) -> Option<SearchValue>;
}

/// Evaluates a checked query against a search document.
#[must_use]
pub fn evaluate(query: &Query, document: &impl SearchDocument) -> bool {
    evaluate_expression(&query.expression, document) == Some(true)
}

/// Compares documents by query order; defaults to running, CloseTime descending, StartTime descending, then RunId.
#[must_use]
pub fn compare_documents(
    query: &Query,
    left: &impl SearchDocument,
    right: &impl SearchDocument,
) -> Ordering {
    if let Some(order) = &query.order_by {
        let comparison = compare_ordered_values(
            left.field_value(&order.field),
            right.field_value(&order.field),
            order.direction,
        );
        return comparison.then_with(|| compare_run_ids(left, right));
    }
    let left_running = matches!(left.field_value("ExecutionStatus"), Some(SearchValue::String(value)) if value == "Running");
    let right_running = matches!(right.field_value("ExecutionStatus"), Some(SearchValue::String(value)) if value == "Running");
    right_running
        .cmp(&left_running)
        .then_with(|| {
            compare_ordered_values(
                left.field_value("CloseTime"),
                right.field_value("CloseTime"),
                SortDirection::Descending,
            )
        })
        .then_with(|| {
            compare_ordered_values(
                left.field_value("StartTime"),
                right.field_value("StartTime"),
                SortDirection::Descending,
            )
        })
        .then_with(|| compare_run_ids(left, right))
}

fn compare_run_ids(left: &impl SearchDocument, right: &impl SearchDocument) -> Ordering {
    compare_optional_values(left.field_value("RunId"), right.field_value("RunId"))
}

fn compare_ordered_values(
    left: Option<SearchValue>,
    right: Option<SearchValue>,
    direction: SortDirection,
) -> Ordering {
    match (&left, &right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(_), Some(_)) => {
            let comparison = compare_optional_values(left, right);
            if direction == SortDirection::Descending {
                comparison.reverse()
            } else {
                comparison
            }
        }
    }
}

fn compare_optional_values(left: Option<SearchValue>, right: Option<SearchValue>) -> Ordering {
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(SearchValue::String(left)), Some(SearchValue::String(right))) => left.cmp(&right),
        (Some(SearchValue::Datetime(left)), Some(SearchValue::Datetime(right))) => left.cmp(&right),
        _ => Ordering::Equal,
    }
}

fn evaluate_expression(expression: &Expression, document: &impl SearchDocument) -> Option<bool> {
    match expression {
        Expression::True => Some(true),
        Expression::Not(inner) => evaluate_expression(inner, document).map(|value| !value),
        Expression::And(children) => {
            let mut has_null = false;
            for child in children {
                match evaluate_expression(child, document) {
                    Some(false) => return Some(false),
                    None => has_null = true,
                    Some(true) => {}
                }
            }
            if has_null { None } else { Some(true) }
        }
        Expression::Or(children) => {
            let mut has_null = false;
            for child in children {
                match evaluate_expression(child, document) {
                    Some(true) => return Some(true),
                    None => has_null = true,
                    Some(false) => {}
                }
            }
            if has_null { None } else { Some(false) }
        }
        Expression::Predicate(predicate) => evaluate_predicate(predicate, document),
    }
}

fn evaluate_predicate(predicate: &Predicate, document: &impl SearchDocument) -> Option<bool> {
    let value = document.field_value(&predicate.field);
    if predicate.operator == Operator::IsNull {
        return Some(value.is_none());
    }
    if predicate.operator == Operator::IsNotNull {
        return Some(value.is_some());
    }
    if predicate.field == "WorkflowType"
        && predicate.operator == Operator::Equal
        && matches!(predicate.literals.first(), Some(Literal::String(value)) if value == "Engine")
    {
        return Some(true);
    }
    if predicate.field == "ExecutionStatus"
        && predicate.operator == Operator::Equal
        && matches!(predicate.literals.first(), Some(Literal::String(value)) if value == "ContinuedAsNew")
    {
        return Some(false);
    }
    let value = value?;
    Some(match predicate.operator {
        Operator::Equal => equals(&value, &predicate.literals[0], predicate.index_type),
        Operator::NotEqual => !equals(&value, &predicate.literals[0], predicate.index_type),
        Operator::In => predicate
            .literals
            .iter()
            .any(|literal| equals(&value, literal, predicate.index_type)),
        Operator::NotIn => predicate
            .literals
            .iter()
            .all(|literal| !equals(&value, literal, predicate.index_type)),
        Operator::Greater => compare_value(&value, &predicate.literals[0])
            .is_some_and(|order| order == Ordering::Greater),
        Operator::GreaterEqual => compare_value(&value, &predicate.literals[0])
            .is_some_and(|order| order != Ordering::Less),
        Operator::Less => compare_value(&value, &predicate.literals[0])
            .is_some_and(|order| order == Ordering::Less),
        Operator::LessEqual => compare_value(&value, &predicate.literals[0])
            .is_some_and(|order| order != Ordering::Greater),
        Operator::Between => {
            compare_value(&value, &predicate.literals[0])
                .is_some_and(|order| order != Ordering::Less)
                && compare_value(&value, &predicate.literals[1])
                    .is_some_and(|order| order != Ordering::Greater)
        }
        Operator::StartsWith => {
            matches!((&value, &predicate.literals[0]), (SearchValue::String(value), Literal::String(prefix)) if value.starts_with(prefix))
        }
        Operator::IsNull => false,
        Operator::IsNotNull => true,
    })
}

fn equals(value: &SearchValue, literal: &Literal, index_type: IndexType) -> bool {
    if index_type == IndexType::Text {
        return matches!((value, literal), (SearchValue::String(value), Literal::String(query)) if text_matches(value, query));
    }
    match (value, literal) {
        (SearchValue::String(value), Literal::String(expected)) => value == expected,
        (SearchValue::StringArray(values), Literal::String(expected)) => values.contains(expected),
        (SearchValue::Int(value), Literal::Int(expected)) => value == expected,
        (SearchValue::Double(value), Literal::Double(expected)) => value == expected,
        (SearchValue::Bool(value), Literal::Bool(expected)) => value == expected,
        (SearchValue::Datetime(value), Literal::Datetime(expected)) => value == expected,
        _ => false,
    }
}

fn compare_value(value: &SearchValue, literal: &Literal) -> Option<Ordering> {
    match (value, literal) {
        (SearchValue::String(value), Literal::String(expected)) => Some(value.cmp(expected)),
        (SearchValue::Int(value), Literal::Int(expected)) => Some(value.cmp(expected)),
        (SearchValue::Double(value), Literal::Double(expected)) => value.partial_cmp(expected),
        (SearchValue::Datetime(value), Literal::Datetime(expected)) => Some(value.cmp(expected)),
        _ => None,
    }
}

pub(crate) fn text_matches(value: &str, query: &str) -> bool {
    let field_terms: Vec<String> = tokenize_text(value);
    let query_terms = tokenize_text(query);
    !query_terms.is_empty() && query_terms.iter().all(|term| field_terms.contains(term))
}

fn tokenize_text(value: &str) -> Vec<String> {
    value
        .to_lowercase()
        .split(|character: char| !character.is_alphanumeric())
        .filter(|term| !term.is_empty())
        .map(str::to_owned)
        .collect()
}
