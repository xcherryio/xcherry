// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_keyspace::{FlowHash, RunId};
use dexzd_protocol::dex::{
    AttributeMatch, AttributeMatchOperator, FlowStatus, Value, WaitForHistoryEventResponse, value,
};
use dexzd_protocol::dexzd::v1::{StepExecutionPhase, stored_value};
use prost::Message;

use crate::FlowStateReader;
use crate::read_model::history::get_history_events;
use crate::read_model::{ReadModelError, RunView};

/// Returns whether the current run attempt is closed.
pub fn is_flow_closed(
    reader: &dyn FlowStateReader,
    flow_hash: FlowHash,
    run_id: RunId,
) -> Result<bool, ReadModelError> {
    let view = RunView::open(reader, flow_hash, run_id)?;
    Ok(view.record.status != FlowStatus::Running as i32)
}

/// Returns whether a started Step execution finished or was canceled in this attempt.
pub fn is_step_execution_finished(
    reader: &dyn FlowStateReader,
    flow_hash: FlowHash,
    run_id: RunId,
    step_type: &str,
    step_execution_number: u32,
) -> Result<bool, ReadModelError> {
    if step_type.is_empty() || step_execution_number == 0 {
        return Err(ReadModelError::InvalidRequest {
            detail: "Step type and positive execution number are required".to_owned(),
        });
    }
    let view = RunView::open(reader, flow_hash, run_id)?;
    let counter = view.rows.step_counter(&view.transaction, step_type)?;
    if counter.last_step_number < step_execution_number {
        return Ok(false);
    }
    let step_execution_id = format!("{step_type}-{step_execution_number}");
    let Some(step) = view
        .rows
        .step_execution(&view.transaction, &step_execution_id)?
    else {
        return Ok(true);
    };
    Ok(matches!(
        StepExecutionPhase::try_from(step.phase),
        Ok(StepExecutionPhase::Completed | StepExecutionPhase::Canceled)
    ))
}

/// Returns the matching Attribute value, or None while the wait predicate is false.
pub fn match_attribute(
    reader: &dyn FlowStateReader,
    flow_hash: FlowHash,
    run_id: RunId,
    condition: &AttributeMatch,
) -> Result<Option<Value>, ReadModelError> {
    let operator = AttributeMatchOperator::try_from(condition.operator).map_err(|_| {
        ReadModelError::InvalidRequest {
            detail: "attribute match operator is invalid".to_owned(),
        }
    })?;
    if operator == AttributeMatchOperator::Unspecified {
        return Err(ReadModelError::InvalidRequest {
            detail: "attribute match operator is invalid".to_owned(),
        });
    }
    let operand = condition
        .operand
        .as_ref()
        .ok_or_else(|| ReadModelError::InvalidRequest {
            detail: "attribute match operand is required".to_owned(),
        })?;
    validate_operand(operand, operator)?;
    let view = RunView::open(reader, flow_hash, run_id)?;
    let Some(attribute) = view.rows.attribute(&view.transaction, &condition.key)? else {
        return Ok(None);
    };
    let Some(stored) = attribute.value.as_ref() else {
        return Ok(None);
    };
    let Some(stored_value::Kind::InlineValue(current)) = stored.kind.as_ref() else {
        return Err(ReadModelError::UnsupportedAttributeValue {
            key: condition.key.clone(),
        });
    };
    if current.encoded_len() > 100 {
        return Err(ReadModelError::UnsupportedAttributeValue {
            key: condition.key.clone(),
        });
    }
    let is_match = match (current.kind.as_ref(), operand.kind.as_ref()) {
        (Some(value::Kind::StringValue(left)), Some(value::Kind::StringValue(right))) => {
            compare_equal(left == right, operator)
        }
        (Some(value::Kind::BoolValue(left)), Some(value::Kind::BoolValue(right))) => {
            compare_equal(left == right, operator)
        }
        (Some(value::Kind::IntValue(left)), Some(value::Kind::IntValue(right))) => {
            compare_order(*left, *right, operator)
        }
        (Some(value::Kind::DoubleValue(left)), Some(value::Kind::DoubleValue(right))) => {
            if !left.is_finite() {
                return Err(ReadModelError::UnsupportedAttributeValue {
                    key: condition.key.clone(),
                });
            }
            compare_order(*left, *right, operator)
        }
        (
            Some(
                value::Kind::StringValue(_)
                | value::Kind::BoolValue(_)
                | value::Kind::IntValue(_)
                | value::Kind::DoubleValue(_),
            ),
            _,
        ) => false,
        _ => {
            return Err(ReadModelError::UnsupportedAttributeValue {
                key: condition.key.clone(),
            });
        }
    };
    Ok(is_match.then(|| current.clone()))
}

/// Returns whether an event in the current lineage reaches the requested event ID.
pub fn is_history_event_available(
    reader: &dyn FlowStateReader,
    flow_hash: FlowHash,
    run_id: RunId,
    next_event_id: i64,
) -> Result<WaitForHistoryEventResponse, ReadModelError> {
    if next_event_id <= 0 {
        return Err(ReadModelError::InvalidRequest {
            detail: "next event ID must be positive".to_owned(),
        });
    }
    let view = RunView::open(reader, flow_hash, run_id)?;
    let page = get_history_events(reader, flow_hash, run_id, next_event_id, 1, &[], 0)?;
    let available_id = page.events.first().map_or(0, |event| event.event_id);
    Ok(WaitForHistoryEventResponse {
        event_available: available_id > 0,
        available_internal_event_id: available_id,
        flow_status: view.record.status,
    })
}

fn validate_operand(
    operand: &Value,
    operator: AttributeMatchOperator,
) -> Result<(), ReadModelError> {
    let is_ordering = !matches!(
        operator,
        AttributeMatchOperator::Equal | AttributeMatchOperator::NotEqual
    );
    let is_valid = match operand.kind.as_ref() {
        Some(value::Kind::StringValue(_) | value::Kind::BoolValue(_)) => !is_ordering,
        Some(value::Kind::IntValue(_)) => true,
        Some(value::Kind::DoubleValue(number)) => number.is_finite(),
        _ => false,
    };
    if is_valid {
        Ok(())
    } else {
        Err(ReadModelError::InvalidRequest {
            detail: "attribute match operand or operator is unsupported".to_owned(),
        })
    }
}

fn compare_equal(is_equal: bool, operator: AttributeMatchOperator) -> bool {
    match operator {
        AttributeMatchOperator::Equal => is_equal,
        AttributeMatchOperator::NotEqual => !is_equal,
        _ => false,
    }
}

fn compare_order<T: PartialOrd + PartialEq>(
    left: T,
    right: T,
    operator: AttributeMatchOperator,
) -> bool {
    match operator {
        AttributeMatchOperator::Equal => left == right,
        AttributeMatchOperator::NotEqual => left != right,
        AttributeMatchOperator::GreaterThan => left > right,
        AttributeMatchOperator::GreaterThanOrEqual => left >= right,
        AttributeMatchOperator::LessThan => left < right,
        AttributeMatchOperator::LessThanOrEqual => left <= right,
        AttributeMatchOperator::Unspecified => false,
    }
}
