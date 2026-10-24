// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Checks a Worker's final method frame before the owner proposes it: shape, request
//! validation, and the per-run value size limit.

use dexzd_protocol::dex::{
    FlowConfig, InvokeExecuteMethodResponse, InvokeWaitForMethodResponse, Kv, PollWorkerTaskFrame,
    PullRouting, Value, invoke_execute_method_output, invoke_wait_for_method_output,
    poll_worker_task_frame, value,
};
use dexzd_protocol::dexzd::v1::{
    HardDeleteObjectReference, StepAttemptFailureKind, StepInvocationKey, StepMethod,
    StepMethodCompletedCommand, step_method_completed_command,
};
use dexzd_request_validation::{
    ValidationCategory, ValidationConfig, WorkerResponse, validate_worker_response,
};

use crate::engine_id_source::EngineIdSource;

/// A Worker method result that passed every check.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum MethodResult {
    WaitFor(InvokeWaitForMethodResponse),
    Execute(InvokeExecuteMethodResponse),
}

impl MethodResult {
    /// Builds the command that records this result.
    pub(crate) fn into_command(
        self,
        invocation: StepInvocationKey,
        attempt: u32,
        app_worker_id: String,
        hard_delete_objects: Vec<HardDeleteObjectReference>,
        pull_routing: PullRouting,
    ) -> StepMethodCompletedCommand {
        let response = match self {
            Self::WaitFor(response) => step_method_completed_command::Response::WaitFor(response),
            Self::Execute(response) => step_method_completed_command::Response::Execute(response),
        };
        StepMethodCompletedCommand {
            invocation: Some(invocation),
            attempt,
            response: Some(response),
            app_worker_id,
            hard_delete_objects,
            pull_routing: Some(pull_routing),
        }
    }

    /// Replaces every channel publication's message ID with a new UUIDv7, like the Go
    /// Server's `channelmessage.AssignIDs`.
    pub(crate) fn assign_channel_message_ids(&mut self, id_source: &dyn EngineIdSource) {
        let publications = match self {
            Self::WaitFor(response) => &mut response.publish_to_channel,
            Self::Execute(response) => &mut response.publish_to_channel,
        };
        for publication in publications {
            publication.message_id = id_source.channel_message_id().hyphenated().to_string();
        }
    }
}

/// Outcome of checking a Worker's final frame.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum WorkerResultCheck {
    Valid(Box<MethodResult>),
    Invalid {
        kind: StepAttemptFailureKind,
        detail: String,
        is_non_retryable: bool,
    },
}

/// Checks the final frame of a `method` attempt.
pub(crate) fn check_worker_result(
    frame: PollWorkerTaskFrame,
    method: i32,
    validation_config: &ValidationConfig,
    flow_config: &FlowConfig,
    max_value_bytes: u64,
) -> WorkerResultCheck {
    let result = match (method, frame.output) {
        (method, Some(poll_worker_task_frame::Output::WaitFor(output)))
            if method == StepMethod::WaitFor as i32 =>
        {
            match output.output {
                Some(invoke_wait_for_method_output::Output::Result(response)) => {
                    MethodResult::WaitFor(response)
                }
                _ => return invalid_shape("WaitFor frame carries no result"),
            }
        }
        (method, Some(poll_worker_task_frame::Output::Execute(output)))
            if method == StepMethod::Execute as i32 =>
        {
            match output.output {
                Some(invoke_execute_method_output::Output::Result(response)) => {
                    MethodResult::Execute(response)
                }
                _ => return invalid_shape("Execute frame carries no result"),
            }
        }
        _ => return invalid_shape("final frame does not match the dispatched method"),
    };
    let validation = match &result {
        MethodResult::WaitFor(response) => {
            let response = WorkerResponse::WaitFor {
                response: Some(response),
                parent_flow_config: flow_config,
            };
            validate_worker_response(response, validation_config)
        }
        MethodResult::Execute(response) => {
            validate_worker_response(WorkerResponse::Execute(Some(response)), validation_config)
        }
    };
    if let Err(error) = validation {
        let is_non_retryable = error.category == ValidationCategory::NonRetryableWorkerResponse;
        return WorkerResultCheck::Invalid {
            kind: if is_non_retryable {
                StepAttemptFailureKind::UnsupportedFeature
            } else {
                StepAttemptFailureKind::InvalidResponse
            },
            detail: format!("{}: {}", error.field_path, error.message),
            is_non_retryable,
        };
    }
    let largest_value_bytes = result_values(&result)
        .map(value_size_bytes)
        .max()
        .unwrap_or(0);
    if largest_value_bytes > max_value_bytes {
        return WorkerResultCheck::Invalid {
            kind: StepAttemptFailureKind::ValueTooLarge,
            detail: format!(
                "a value of {largest_value_bytes} bytes exceeds the {max_value_bytes} byte limit"
            ),
            is_non_retryable: true,
        };
    }
    WorkerResultCheck::Valid(Box::new(result))
}

fn invalid_shape(detail: &str) -> WorkerResultCheck {
    WorkerResultCheck::Invalid {
        kind: StepAttemptFailureKind::InvalidResponse,
        detail: detail.to_owned(),
        is_non_retryable: false,
    }
}

/// Every value a result writes or passes on.
fn result_values(result: &MethodResult) -> Box<dyn Iterator<Item = &Value> + '_> {
    fn key_values(values: &[Kv]) -> impl Iterator<Item = &Value> {
        values.iter().filter_map(|kv| kv.value.as_ref())
    }
    match result {
        MethodResult::WaitFor(response) => Box::new(
            response
                .upsert_attributes
                .iter()
                .filter_map(|write| write.value.as_ref())
                .chain(key_values(&response.upsert_step_exe_locals))
                .chain(key_values(&response.record_events))
                .chain(
                    response
                        .publish_to_channel
                        .iter()
                        .filter_map(|message| message.value.as_ref()),
                ),
        ),
        MethodResult::Execute(response) => {
            let decision = response.step_decision.as_ref();
            Box::new(
                response
                    .upsert_attributes
                    .iter()
                    .filter_map(|write| write.value.as_ref())
                    .chain(key_values(&response.upsert_step_exe_locals))
                    .chain(key_values(&response.record_events))
                    .chain(
                        response
                            .publish_to_channel
                            .iter()
                            .filter_map(|message| message.value.as_ref()),
                    )
                    .chain(
                        decision
                            .into_iter()
                            .flat_map(|decision| &decision.next_steps)
                            .filter_map(|step| step.step_input.as_ref()),
                    )
                    .chain(
                        decision
                            .and_then(|decision| decision.close_decision.as_ref())
                            .and_then(|close| close.close_input.as_ref()),
                    ),
            )
        }
    }
}

/// Returns the size that the value limit applies to: string bytes or object payload bytes.
fn value_size_bytes(value: &Value) -> u64 {
    match value.kind.as_ref() {
        Some(value::Kind::StringValue(text)) => text.len() as u64,
        Some(value::Kind::ObjValue(object)) => object.payload.len() as u64,
        _ => 0,
    }
}
