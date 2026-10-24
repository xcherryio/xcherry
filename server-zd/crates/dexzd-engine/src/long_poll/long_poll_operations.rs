// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! WaitForFlow, WaitForStepCompletion, WaitForAttribute, and WaitForHistoryEvent (plan 9.7).

use std::time::Duration;

use dexzd_apply::read_model::{ReadModelError, StepCompletionState, step_completion_state};
use dexzd_apply::{ApplyContext, FlowStateReader};
use dexzd_keyspace::{ColumnFamily, RunTable, TenantId, run_key};
use dexzd_protocol::dex::{
    AttributeMatch, AttributeMatchOperator, ErrorSubStatus, FlowResult, FlowStatus,
    StepCompletionOutput, Value, WaitForAttributeRequest, WaitForAttributeResponse,
    WaitForFlowRequest, WaitForHistoryEventRequest, WaitForHistoryEventResponse,
    WaitForStepCompletionRequest, WaitForStepCompletionResponse, value,
};
use dexzd_protocol::dexzd::v1::{AttributeRecord, stored_value};
use dexzd_request_validation::{ClientRequest, validate_client_request};
use tokio::time::Instant;

use crate::long_poll::WaitInterest;
use crate::runtime::read_operations::state_rows::{
    ResolvedRun, WireValue, decode_row, is_closed_status, read_run, response_value,
};
use crate::runtime::{FlowAddress, FlowRuntimeRegistry, RuntimeError, grpc_code};

/// The Go Server's WaitForFlow message when the wait time or the client deadline ends first.
pub const WAIT_FOR_FLOW_TIMEOUT_MESSAGE: &str =
    "flow is still running and waiting exceeded the timeout";
/// The Go Server's WaitForHistoryEvent message when the wait ends without a new event.
pub const WAIT_FOR_HISTORY_EVENT_TIMEOUT_MESSAGE: &str = "history long poll exceeded the deadline";
/// The Go Server's message when a step completion or Attribute wait call ends before its request timeout.
pub const WAIT_CALL_DEADLINE_MESSAGE: &str = "context deadline exceeded";
/// The Go Server's WaitForStepCompletion message when `request_timeout_seconds` passes.
pub const STEP_COMPLETION_REQUEST_TIMEOUT_MESSAGE: &str = "step completion request timed out";
/// The Go Server's WaitForAttribute message when `request_timeout_seconds` passes.
pub const ATTRIBUTE_REQUEST_TIMEOUT_MESSAGE: &str = "attribute request timed out";

/// When a long-poll call gives up.
struct LongPollDeadline {
    /// End of this call; the client polls again with the same request ID.
    call_deadline: Instant,
    /// End of the whole request; reported as `REQUEST_TIMEOUT`.
    request_deadline: Option<RequestDeadline>,
}

/// The end of a whole wait request and the message reported when it passes.
struct RequestDeadline {
    expires_at: Instant,
    timeout_message: &'static str,
}

impl FlowRuntimeRegistry {
    /// Waits until the run closes and returns its result (plan 9.7).
    ///
    /// Returns `DEADLINE_EXCEEDED` + `LONG_POLL_TIME_OUT` when the run is still running at the
    /// end of the wait time (60 s when `wait_time_seconds` is 0).
    pub async fn wait_for_flow(
        &self,
        tenant_id: TenantId,
        request: WaitForFlowRequest,
    ) -> Result<FlowResult, RuntimeError> {
        if request.flow_id.is_empty() || request.wait_time_seconds < 0 {
            return Err(RuntimeError::invalid_argument(
                "valid flow ID and non-negative wait time are required",
            ));
        }
        let flow = FlowAddress::new(tenant_id, request.flow_id.clone());
        let deadline = LongPollDeadline {
            call_deadline: Instant::now() + self.wait_time(request.wait_time_seconds),
            request_deadline: None,
        };
        let is_hydrating = self.inner.config.hydrate_value_references;
        self.long_poll(
            &flow,
            WaitInterest::FlowClose,
            &deadline,
            WAIT_FOR_FLOW_TIMEOUT_MESSAGE,
            |state, context| {
                let run = read_run(state, context, &flow, &request.run_id)?;
                if !is_closed_status(run.record.status) {
                    return Ok(None);
                }
                let result =
                    flow_result(state, context, &run, request.needs_results, is_hydrating)?;
                Ok(Some(result))
            },
        )
        .await
    }

    /// Waits until a Step execution of the current run attempt has finished (completed or
    /// canceled). A Step that failed the Flow never finishes.
    ///
    /// Returns `REQUEST_TIMEOUT` when `request_timeout_seconds` passes, `LONG_POLL_TIME_OUT`
    /// after one call's wait so the client polls again, and `FLOW_NOT_EXISTS` once the run
    /// closed without the Step finishing.
    pub async fn wait_for_step_completion(
        &self,
        tenant_id: TenantId,
        request: WaitForStepCompletionRequest,
    ) -> Result<WaitForStepCompletionResponse, RuntimeError> {
        validate_client_request(
            ClientRequest::WaitForStepCompletion(&request),
            &self.inner.config.validation,
        )?;
        let step_execution_number: u32 = request
            .step_execution_number
            .parse()
            .ok()
            .filter(|number| *number > 0)
            .ok_or_else(|| {
                RuntimeError::invalid_argument("step execution number must be a positive integer")
            })?;
        let step_execution_id = format!("{}-{step_execution_number}", request.step_type);
        let flow = FlowAddress::new(tenant_id, request.flow_id.clone());
        let deadline = self.request_deadline(
            request.request_timeout_seconds,
            STEP_COMPLETION_REQUEST_TIMEOUT_MESSAGE,
        );
        self.long_poll(
            &flow,
            WaitInterest::StepExecution(step_execution_id),
            &deadline,
            WAIT_CALL_DEADLINE_MESSAGE,
            |state, context| {
                let run = read_run(state, context, &flow, "")?;
                let completion = step_completion_state(
                    state,
                    context.flow_hash,
                    run.run_id,
                    &request.step_type,
                    step_execution_number,
                )
                .map_err(|error| read_model_error(&flow, error))?;
                match completion {
                    StepCompletionState::Completed => Ok(Some(WaitForStepCompletionResponse {})),
                    StepCompletionState::NotCompleted => Ok(None),
                    // Go reports a wait on a closed Flow as a missing Flow.
                    StepCompletionState::EndedWithoutCompletion => {
                        Err(RuntimeError::flow_not_found(&flow.flow_id))
                    }
                }
            },
        )
        .await
    }

    /// Waits until an Attribute of the current run matches and returns the matched value.
    ///
    /// Returns `FAILED_PRECONDITION` when the stored value is above the inline threshold, a
    /// hard-delete value, or not comparable; timeouts behave like WaitForStepCompletion.
    pub async fn wait_for_attribute(
        &self,
        tenant_id: TenantId,
        request: WaitForAttributeRequest,
    ) -> Result<WaitForAttributeResponse, RuntimeError> {
        validate_client_request(
            ClientRequest::WaitForAttribute(&request),
            &self.inner.config.validation,
        )?;
        let attribute_match = request
            .r#match
            .clone()
            .ok_or_else(|| RuntimeError::invalid_argument("attribute match is required"))?;
        let flow = FlowAddress::new(tenant_id, request.flow_id.clone());
        let deadline = self.request_deadline(
            request.request_timeout_seconds,
            ATTRIBUTE_REQUEST_TIMEOUT_MESSAGE,
        );
        self.long_poll(
            &flow,
            WaitInterest::Attribute(attribute_match.key.clone()),
            &deadline,
            WAIT_CALL_DEADLINE_MESSAGE,
            |state, context| {
                let run = read_run(state, context, &flow, "")?;
                if is_closed_status(run.record.status) {
                    return Err(RuntimeError::flow_not_found(&flow.flow_id));
                }
                let record: Option<AttributeRecord> = decode_row(
                    state,
                    ColumnFamily::Flow,
                    &run_key(
                        context.flow_hash,
                        run.run_id,
                        RunTable::Attribute,
                        attribute_match.key.as_bytes(),
                    ),
                    "Attribute",
                )?;
                let Some(stored) = record.and_then(|record| record.value) else {
                    return Ok(None);
                };
                let current = match stored.kind {
                    Some(stored_value::Kind::InlineValue(value)) => value,
                    Some(_) => {
                        return Err(attribute_precondition(format!(
                            "stored attribute {:?} is blob-backed",
                            attribute_match.key
                        )));
                    }
                    None => return Ok(None),
                };
                if is_attribute_matched(&current, &attribute_match)? {
                    return Ok(Some(WaitForAttributeResponse {
                        matched_value: Some(current),
                    }));
                }
                Ok(None)
            },
        )
        .await
    }

    /// Waits until history of the run contains an event at or after `next_internal_event_id`,
    /// or the run closes (`event_available` is then false).
    ///
    /// Returns `DEADLINE_EXCEEDED` + `LONG_POLL_TIME_OUT` after the default wait time.
    pub async fn wait_for_history_event(
        &self,
        tenant_id: TenantId,
        request: WaitForHistoryEventRequest,
    ) -> Result<WaitForHistoryEventResponse, RuntimeError> {
        if request.flow_id.is_empty()
            || request.run_id.is_empty()
            || request.next_internal_event_id <= 0
        {
            return Err(RuntimeError::invalid_argument(
                "flow ID, run ID, and positive next event ID are required",
            ));
        }
        let flow = FlowAddress::new(tenant_id, request.flow_id.clone());
        let deadline = LongPollDeadline {
            call_deadline: Instant::now() + self.inner.config.long_poll_default_wait,
            request_deadline: None,
        };
        let next_event_id = request.next_internal_event_id;
        self.long_poll(
            &flow,
            WaitInterest::HistoryEvent(next_event_id),
            &deadline,
            WAIT_FOR_HISTORY_EVENT_TIMEOUT_MESSAGE,
            |state, context| {
                let run = read_run(state, context, &flow, &request.run_id)?;
                let last_event_id = run.record.next_event_id - 1;
                let is_available = last_event_id >= next_event_id;
                if !is_available && !is_closed_status(run.record.status) {
                    return Ok(None);
                }
                Ok(Some(WaitForHistoryEventResponse {
                    event_available: is_available,
                    available_internal_event_id: last_event_id,
                    flow_status: run.record.status,
                }))
            },
        )
        .await
    }

    /// Registers first, then rechecks committed state after each relevant change until `check` succeeds.
    async fn long_poll<T>(
        &self,
        flow: &FlowAddress,
        interest: WaitInterest,
        deadline: &LongPollDeadline,
        long_poll_timeout_message: &str,
        mut check: impl FnMut(&dyn FlowStateReader, ApplyContext) -> Result<Option<T>, RuntimeError>,
    ) -> Result<T, RuntimeError> {
        self.check_running()?;
        let context = self.flow_context(flow);
        let registration = self.inner.waiters.register(flow, interest)?;
        let call_deadline = match &deadline.request_deadline {
            Some(request_deadline) => request_deadline.expires_at.min(deadline.call_deadline),
            None => deadline.call_deadline,
        };
        let halted = self.halted();
        loop {
            let result = {
                let committed = self.inner.dependencies.committer.committed_view()?;
                check(committed.as_ref(), context)?
            };
            if let Some(result) = result {
                return Ok(result);
            }
            self.check_running()?;
            if self.inner.waiters.is_closed() {
                return Err(RuntimeError::Unavailable {
                    detail: "long poll ended because the node is stopping; retry".to_owned(),
                });
            }
            // Biased for reproducible simulation; the halt and the deadline fire once, so changes
            // cannot starve.
            tokio::select! {
                biased;
                () = halted.cancelled() => return Err(self.check_running().expect_err("halted")),
                () = tokio::time::sleep_until(call_deadline) => {
                    let expired_request = deadline
                        .request_deadline
                        .as_ref()
                        .filter(|request_deadline| request_deadline.expires_at <= call_deadline);
                    return Err(if let Some(expired_request) = expired_request {
                        RuntimeError::rejected(
                            grpc_code::DEADLINE_EXCEEDED,
                            ErrorSubStatus::RequestTimeout,
                            expired_request.timeout_message,
                        )
                    } else {
                        RuntimeError::rejected(
                            grpc_code::DEADLINE_EXCEEDED,
                            ErrorSubStatus::LongPollTimeOut,
                            long_poll_timeout_message,
                        )
                    });
                }
                () = registration.changed() => {}
            }
        }
    }

    fn wait_time(&self, wait_time_seconds: i32) -> Duration {
        if wait_time_seconds <= 0 {
            return self.inner.config.long_poll_default_wait;
        }
        Duration::from_secs(u64::from(wait_time_seconds.unsigned_abs()))
            .min(self.inner.config.long_poll_max_wait)
    }

    fn request_deadline(
        &self,
        request_timeout_seconds: i32,
        timeout_message: &'static str,
    ) -> LongPollDeadline {
        let now = Instant::now();
        LongPollDeadline {
            call_deadline: now + self.inner.config.long_poll_default_wait,
            request_deadline: (request_timeout_seconds > 0).then(|| RequestDeadline {
                expires_at: now
                    + Duration::from_secs(u64::from(request_timeout_seconds.unsigned_abs())),
                timeout_message,
            }),
        }
    }
}

/// Returns the run's result; Step outputs above the inline threshold are value references that the
/// client loads with LoadBlobs, unless `is_hydrating` loads them here.
fn flow_result(
    state: &dyn FlowStateReader,
    context: ApplyContext,
    run: &ResolvedRun,
    needs_results: bool,
    is_hydrating: bool,
) -> Result<FlowResult, RuntimeError> {
    let close = run.record.close.clone().unwrap_or_default();
    let mut result = FlowResult {
        flow_status: run.record.status,
        results: Vec::new(),
        error_type: close.error_type,
        error_message: close.error_message,
    };
    if needs_results && run.record.status == FlowStatus::Completed as i32 {
        for output in close.results {
            let value = match output.completed_step_output.as_ref() {
                Some(stored) => {
                    response_value(state, context.flow_hash, run.run_id, stored, is_hydrating)?
                }
                None => None,
            };
            result.results.push(StepCompletionOutput {
                completed_step_type: output.completed_step_type,
                completed_step_execution_id: output.completed_step_execution_id,
                completed_step_output: match value {
                    Some(WireValue::Value(value)) => Some(value),
                    Some(WireValue::HardDeleteObject(_)) | None => None,
                },
            });
        }
    }
    Ok(result)
}

/// Matches like the Go Server: strings and booleans by equality; type mismatches never match.
fn is_attribute_matched(
    current: &Value,
    attribute_match: &AttributeMatch,
) -> Result<bool, RuntimeError> {
    let operator = AttributeMatchOperator::try_from(attribute_match.operator)
        .map_err(|_| RuntimeError::invalid_argument("attribute match operator is invalid"))?;
    let operand = attribute_match
        .operand
        .as_ref()
        .and_then(|operand| operand.kind.as_ref());
    let current = current.kind.as_ref();
    match current {
        Some(
            value::Kind::StringValue(_) | value::Kind::BoolValue(_) | value::Kind::IntValue(_),
        ) => {}
        Some(value::Kind::DoubleValue(double)) if double.is_finite() => {}
        _ => {
            return Err(attribute_precondition(
                "stored attribute must be a string, boolean, integer, or finite double",
            ));
        }
    }
    let ordering = match (current, operand) {
        (Some(value::Kind::StringValue(current)), Some(value::Kind::StringValue(operand))) => {
            return Ok(is_equality_matched(current == operand, operator));
        }
        (Some(value::Kind::BoolValue(current)), Some(value::Kind::BoolValue(operand))) => {
            return Ok(is_equality_matched(current == operand, operator));
        }
        (Some(value::Kind::IntValue(current)), Some(value::Kind::IntValue(operand))) => {
            current.partial_cmp(operand)
        }
        (Some(value::Kind::DoubleValue(current)), Some(value::Kind::DoubleValue(operand))) => {
            current.partial_cmp(operand)
        }
        _ => return Ok(false),
    };
    let Some(ordering) = ordering else {
        return Ok(false);
    };
    Ok(match operator {
        AttributeMatchOperator::Equal => ordering.is_eq(),
        AttributeMatchOperator::NotEqual => ordering.is_ne(),
        AttributeMatchOperator::GreaterThan => ordering.is_gt(),
        AttributeMatchOperator::GreaterThanOrEqual => ordering.is_ge(),
        AttributeMatchOperator::LessThan => ordering.is_lt(),
        AttributeMatchOperator::LessThanOrEqual => ordering.is_le(),
        AttributeMatchOperator::Unspecified => false,
    })
}

fn is_equality_matched(is_equal: bool, operator: AttributeMatchOperator) -> bool {
    match operator {
        AttributeMatchOperator::Equal => is_equal,
        AttributeMatchOperator::NotEqual => !is_equal,
        _ => false,
    }
}

fn attribute_precondition(message: impl Into<String>) -> RuntimeError {
    RuntimeError::rejected(
        grpc_code::FAILED_PRECONDITION,
        ErrorSubStatus::Uncategorized,
        message,
    )
}

/// Maps a read-model error of a long poll to the client error Go returns.
fn read_model_error(flow: &FlowAddress, error: ReadModelError) -> RuntimeError {
    match error {
        ReadModelError::RunNotFound { .. } => RuntimeError::flow_not_found(&flow.flow_id),
        ReadModelError::InvalidRequest { detail } => RuntimeError::invalid_argument(detail),
        ReadModelError::State(error) => error.into(),
        error @ ReadModelError::UnsupportedAttributeValue { .. } => RuntimeError::Internal {
            detail: error.to_string(),
        },
    }
}
