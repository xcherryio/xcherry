// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::HashSet;

use dexzd_protocol::dex::{
    self, AttributeWrite, ChannelMessage, ChannelMessageDeletion, CloseDecision,
    InvokeExecuteMethodResponse, InvokeWaitForMethodResponse, InvokeWorkerRpcResponse,
    StepDecision, value,
};

use crate::shared::{
    ValidationCategory, ValidationConfig, ValidationError, invalid, validate_action_permissions,
    validate_blob_value, validate_identifier, validate_kv_blob_values, validate_step_options,
};
use crate::waiting::validate_waiting_condition;

/// A Worker result to validate before the engine proposes a command.
pub enum WorkerResponse<'a> {
    /// Result of a Step WaitFor method.
    WaitFor(Option<&'a InvokeWaitForMethodResponse>),
    /// Result of a Step Execute method.
    Execute(Option<&'a InvokeExecuteMethodResponse>),
    /// Result of a Flow RPC method.
    Rpc(Option<&'a InvokeWorkerRpcResponse>),
}

/// Validates a Worker result and marks unsupported failure policies as non-retryable.
pub fn validate_worker_response(
    response: WorkerResponse<'_>,
    config: &ValidationConfig,
) -> Result<(), ValidationError> {
    let result = match response {
        WorkerResponse::WaitFor(response) => validate_wait_for(response, config),
        WorkerResponse::Execute(response) => validate_execute(response, config),
        WorkerResponse::Rpc(response) => validate_rpc(response, config),
    };
    result.map_err(|mut error| {
        if error.category == ValidationCategory::InvalidArgument {
            error.category = ValidationCategory::RetryableWorkerResponse;
        }
        error
    })
}

fn validate_wait_for(
    response: Option<&InvokeWaitForMethodResponse>,
    config: &ValidationConfig,
) -> Result<(), ValidationError> {
    let response =
        response.ok_or_else(|| invalid("response", "nil InvokeWaitForMethodResponse"))?;
    validate_waiting_condition(response.waiting_condition.as_ref(), config)?;
    validate_common_response(
        &response.upsert_attributes,
        &response.record_events,
        &response.upsert_step_exe_locals,
        &response.publish_to_channel,
        &response.delete_from_channel,
        response.action_permission_mappings.as_ref(),
        config,
    )
}

fn validate_execute(
    response: Option<&InvokeExecuteMethodResponse>,
    config: &ValidationConfig,
) -> Result<(), ValidationError> {
    let response =
        response.ok_or_else(|| invalid("response", "nil InvokeExecuteMethodResponse"))?;
    validate_step_decision(response.step_decision.as_ref(), config)?;
    validate_common_response(
        &response.upsert_attributes,
        &response.record_events,
        &response.upsert_step_exe_locals,
        &response.publish_to_channel,
        &response.delete_from_channel,
        response.action_permission_mappings.as_ref(),
        config,
    )
}

fn validate_rpc(
    response: Option<&InvokeWorkerRpcResponse>,
    config: &ValidationConfig,
) -> Result<(), ValidationError> {
    let response = response.ok_or_else(|| invalid("response", "nil InvokeWorkerRPCResponse"))?;
    validate_blob_value("output", response.output.as_ref())?;
    validate_common_response(
        &response.upsert_attributes,
        &response.record_events,
        &[],
        &response.publish_to_channel,
        &response.delete_from_channel,
        response.action_permission_mappings.as_ref(),
        config,
    )?;
    if let Some(decision) = &response.step_decision {
        if decision.close_decision.is_some() {
            return Err(invalid(
                "step_decision.close_decision",
                "closing flow in RPC is not supported yet",
            ));
        }
        validate_next_steps(decision, config)?;
        validate_cancel_types(
            &decision.cancel_step_types,
            "step_decision.cancel_step_types",
            "cancel step type",
        )?;
        if !decision.cancel_sibling_step_types.is_empty() {
            return Err(invalid(
                "step_decision.cancel_sibling_step_types",
                "canceling sibling step executions in RPC is not supported",
            ));
        }
    }
    Ok(())
}

fn validate_common_response(
    attributes: &[AttributeWrite],
    events: &[dex::Kv],
    locals: &[dex::Kv],
    publications: &[ChannelMessage],
    deletions: &[ChannelMessageDeletion],
    permissions: Option<&dex::ActionPermissionMappings>,
    config: &ValidationConfig,
) -> Result<(), ValidationError> {
    for (index, write) in attributes.iter().enumerate() {
        let path = format!("upsert_attributes[{index}]");
        validate_blob_value(&format!("{path}.value"), write.value.as_ref())?;
        if write.key == "DexWorkQueuePermissions"
            || write
                .index_config
                .as_ref()
                .is_some_and(|index| index.index_key == "DexWorkQueuePermissions")
        {
            return Err(invalid(
                path.clone(),
                "DexWorkQueuePermissions is managed by the Server",
            ));
        }
        if write.is_hard_delete_on_null {
            match write.value.as_ref().and_then(|value| value.kind.as_ref()) {
                None if write.value.is_none() => {}
                None => {
                    return Err(invalid(
                        path.clone(),
                        format!(
                            "hard-delete attribute {:?} has an empty value; use null to delete it",
                            write.key
                        ),
                    ));
                }
                Some(
                    value::Kind::NullValue(_)
                    | value::Kind::StringValue(_)
                    | value::Kind::ObjValue(_),
                ) => {}
                Some(_) => {
                    return Err(invalid(
                        path.clone(),
                        format!(
                            "hard-delete attribute {:?} must be a string, an object, or null",
                            write.key
                        ),
                    ));
                }
            }
            if write
                .index_config
                .as_ref()
                .is_some_and(|index| index.enable)
            {
                return Err(invalid(
                    path.clone(),
                    format!("hard-delete attribute {:?} must not be indexed", write.key),
                ));
            }
            if write.sync_config.as_ref().is_some_and(|sync| sync.enabled) {
                return Err(invalid(
                    path,
                    format!(
                        "hard-delete attribute {:?} must not sync to an Attribute Store",
                        write.key
                    ),
                ));
            }
            if !config.has_blob_store {
                return Err(invalid(
                    format!("upsert_attributes[{index}]"),
                    "hard-delete Attributes require an enabled blob store",
                ));
            }
        }
    }
    validate_action_permissions("action_permission_mappings", permissions)?;
    validate_kv_blob_values("record_events", events)?;
    validate_kv_blob_values("upsert_step_exe_locals", locals)?;
    for (index, message) in publications.iter().enumerate() {
        let path = format!("publish_to_channel[{index}]");
        if message.channel_name.is_empty() {
            return Err(invalid(
                path,
                format!("Channel publication at index {index} is invalid"),
            ));
        }
        validate_blob_value(&format!("{path}.value"), message.value.as_ref())?;
    }
    for (index, deletion) in deletions.iter().enumerate() {
        if deletion.channel_name.is_empty() || deletion.message_id.is_empty() {
            return Err(invalid(
                format!("delete_from_channel[{index}]"),
                format!("Channel deletion at index {index} is invalid"),
            ));
        }
    }
    Ok(())
}

fn validate_step_decision(
    decision: Option<&StepDecision>,
    config: &ValidationConfig,
) -> Result<(), ValidationError> {
    let decision = decision.ok_or_else(|| invalid("step_decision", "step decision is nil"))?;
    if decision.next_steps.is_empty() && decision.close_decision.is_none() {
        return Err(invalid(
            "step_decision",
            "empty step decision is not supported",
        ));
    }
    validate_next_steps(decision, config)?;
    validate_cancel_types(
        &decision.cancel_step_types,
        "step_decision.cancel_step_types",
        "cancel step type",
    )?;
    validate_cancel_types(
        &decision.cancel_sibling_step_types,
        "step_decision.cancel_sibling_step_types",
        "cancel sibling step type",
    )?;
    if let Some(close) = &decision.close_decision {
        validate_close_decision(close, decision.next_steps.len())?;
    }
    Ok(())
}

fn validate_next_steps(
    decision: &StepDecision,
    config: &ValidationConfig,
) -> Result<(), ValidationError> {
    for (index, movement) in decision.next_steps.iter().enumerate() {
        let path = format!("step_decision.next_steps[{index}]");
        if movement.step_type.is_empty() {
            return Err(invalid(
                path,
                format!("next step at index {index} is invalid"),
            ));
        }
        validate_identifier(
            &format!("{path}.step_type"),
            "step type",
            &movement.step_type,
        )?;
        validate_step_options(
            &format!("{path}.step_options"),
            movement.step_options.as_ref(),
            config,
        )?;
    }
    Ok(())
}

fn validate_cancel_types(types: &[String], path: &str, name: &str) -> Result<(), ValidationError> {
    for (index, step_type) in types.iter().enumerate() {
        let item_path = format!("{path}[{index}]");
        if step_type.is_empty() {
            return Err(invalid(
                item_path,
                format!("{name} at index {index} is invalid"),
            ));
        }
        validate_identifier(&item_path, "step type", step_type)?;
    }
    Ok(())
}

fn validate_close_decision(
    close: &CloseDecision,
    next_step_count: usize,
) -> Result<(), ValidationError> {
    let close_type = close.close_decision_type;
    if close_type != 1 {
        if next_step_count > 0 {
            return Err(invalid(
                "step_decision.close_decision",
                "close decision cannot be combined with next steps",
            ));
        }
        if !close.conditional_channel_names.is_empty() {
            return Err(invalid(
                "step_decision.close_decision.conditional_channel_names",
                "conditional channel names require a conditional close decision",
            ));
        }
    }
    match close_type {
        1 => {
            if next_step_count == 0 {
                return Err(invalid(
                    "step_decision.close_decision",
                    "conditional close decision requires at least one next step",
                ));
            }
            if close.conditional_channel_names.is_empty() {
                return Err(invalid(
                    "step_decision.close_decision.conditional_channel_names",
                    "conditional close decision requires at least one channel",
                ));
            }
            let mut seen = HashSet::new();
            for (index, channel) in close.conditional_channel_names.iter().enumerate() {
                let path =
                    format!("step_decision.close_decision.conditional_channel_names[{index}]");
                if channel.is_empty() {
                    return Err(invalid(path, "conditional close channel name is empty"));
                }
                if !seen.insert(channel) {
                    return Err(invalid(
                        path,
                        format!("duplicate conditional close channel {channel:?}"),
                    ));
                }
            }
        }
        2 | 3 => {}
        4 => {
            if close
                .close_input
                .as_ref()
                .is_some_and(|value| !matches!(value.kind, Some(value::Kind::StringValue(_))))
            {
                return Err(invalid(
                    "step_decision.close_decision.close_input",
                    "force fail close input must be a string",
                ));
            }
        }
        5 => {
            if close.close_input.is_some() {
                return Err(invalid(
                    "step_decision.close_decision.close_input",
                    "dead end close decision cannot have close input",
                ));
            }
        }
        _ => {
            return Err(invalid(
                "step_decision.close_decision.close_decision_type",
                "close decision type is unspecified",
            ));
        }
    }
    Ok(())
}
