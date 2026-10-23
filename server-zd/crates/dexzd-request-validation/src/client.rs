// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_protocol::dex::{
    AttributeMatch, DeleteChannelMessageRequest, FlowConfig, InvokeRpcRequest,
    PublishToChannelRequest, ResetFlowRequest, SetAttributesRequest, SkipTimerRequest,
    StartFlowRequest, UpdateFlowConfigRequest, WaitForAttributeRequest,
    WaitForStepCompletionRequest, value,
};

use crate::shared::{
    ValidationCategory, ValidationConfig, ValidationError, invalid, validate_action_permissions,
    validate_attribute_writes, validate_blob_value, validate_flow_config,
    validate_flow_timeout_policy, validate_identifier, validate_start_options,
    validate_state_selections, validate_step_options, validate_timeout_handler,
};

/// A supported public client request before the API proposes its command.
pub enum ClientRequest<'a> {
    /// Start a Flow.
    StartFlow(&'a StartFlowRequest),
    /// Write Attributes to a Flow.
    SetAttributes(&'a SetAttributesRequest),
    /// Publish Channel messages.
    PublishToChannel(&'a PublishToChannelRequest),
    /// Delete a Channel message.
    DeleteChannelMessage(&'a DeleteChannelMessageRequest),
    /// Invoke a Flow RPC.
    InvokeRpc(&'a InvokeRpcRequest),
    /// Change Flow configuration.
    UpdateFlowConfig(&'a UpdateFlowConfigRequest),
    /// Skip a pending timer.
    SkipTimer(&'a SkipTimerRequest),
    /// Reset a Flow.
    ResetFlow(&'a ResetFlowRequest),
    /// Wait for an Attribute match.
    WaitForAttribute(&'a WaitForAttributeRequest),
    /// Wait for a Step execution to finish.
    WaitForStepCompletion(&'a WaitForStepCompletionRequest),
}

/// Validates a public request and returns an INVALID_ARGUMENT category on failure.
pub fn validate_client_request(
    request: ClientRequest<'_>,
    config: &ValidationConfig,
) -> Result<(), ValidationError> {
    let result = match request {
        ClientRequest::StartFlow(request) => validate_start_flow(request, config),
        ClientRequest::SetAttributes(request) => validate_set_attributes(request, config),
        ClientRequest::PublishToChannel(request) => validate_publish_to_channel(request),
        ClientRequest::DeleteChannelMessage(request) => validate_delete_channel_message(request),
        ClientRequest::InvokeRpc(request) => validate_invoke_rpc(request),
        ClientRequest::UpdateFlowConfig(request) => validate_update_flow_config(request, config),
        ClientRequest::SkipTimer(request) => validate_skip_timer(request),
        ClientRequest::ResetFlow(request) => validate_reset_flow(request),
        ClientRequest::WaitForAttribute(request) => validate_wait_for_attribute(request),
        ClientRequest::WaitForStepCompletion(request) => validate_wait_for_step_completion(request),
    };
    result.map_err(|mut error| {
        error.category = ValidationCategory::InvalidArgument;
        error
    })
}

fn validate_start_flow(
    request: &StartFlowRequest,
    config: &ValidationConfig,
) -> Result<(), ValidationError> {
    if request.flow_id.is_empty() || request.flow_type.is_empty() {
        return Err(invalid("flow_id", "flow ID and flow type are required"));
    }
    if request.request_id.is_empty() {
        return Err(invalid("request_id", "request ID is required"));
    }
    if request.flow_id.len() > config.maximum_flow_id_bytes {
        return Err(invalid(
            "flow_id",
            format!(
                "flow ID exceeds maximum length of {} bytes",
                config.maximum_flow_id_bytes
            ),
        ));
    }
    let policy = validate_flow_timeout_policy(
        "flow_timeout_policy",
        request.flow_timeout_seconds,
        request.flow_timeout_policy,
    )?;
    validate_blob_value("step_input", request.step_input.as_ref())?;
    let options = request.flow_start_options.as_ref();
    validate_timeout_handler(
        "flow_start_options.timeout_handler_options",
        request.flow_timeout_seconds,
        policy,
        options.and_then(|options| options.timeout_handler_options.as_ref()),
        config,
    )?;
    if let Some(options) = options {
        validate_attribute_writes(
            "flow_start_options.attributes",
            &options.attributes,
            config,
            false,
        )?;
    }
    validate_identifier("flow_id", "flow ID", &request.flow_id)?;
    validate_identifier("start_step_type", "step type", &request.start_step_type)?;
    validate_step_options("step_options", request.step_options.as_ref(), config)?;

    let mut effective = config.default_flow_config.clone();
    if let Some(override_config) = options.and_then(|options| options.flow_config_override.as_ref())
    {
        apply_flow_config_override(&mut effective, override_config);
    }
    validate_flow_config(
        "flow_start_options.flow_config_override",
        &effective,
        config,
        true,
    )?;
    if let Some(options) = options {
        if !(0..=4).contains(&options.id_reuse_policy) {
            return Err(invalid(
                "flow_start_options.id_reuse_policy",
                "unknown ID reuse policy",
            ));
        }
        validate_start_options(
            "flow_start_options",
            options.flow_start_delay_seconds,
            options.retry_policy.is_some(),
        )?;
    }
    Ok(())
}

fn apply_flow_config_override(effective: &mut FlowConfig, override_config: &FlowConfig) {
    if override_config.active_step_search_mode.is_some() {
        effective.active_step_search_mode = override_config.active_step_search_mode;
    }
    if override_config.continue_as_new_threshold.is_some() {
        effective.continue_as_new_threshold = override_config.continue_as_new_threshold;
    }
    if override_config.continue_as_new_page_size_in_bytes.is_some() {
        effective.continue_as_new_page_size_in_bytes =
            override_config.continue_as_new_page_size_in_bytes;
    }
    if override_config.step_durability.is_some() {
        effective.step_durability = override_config.step_durability;
    }
    if override_config.worker_target.is_some() {
        effective
            .worker_target
            .clone_from(&override_config.worker_target);
    }
    if override_config.attribute_store_names.is_some() {
        effective
            .attribute_store_names
            .clone_from(&override_config.attribute_store_names);
    }
}

fn validate_set_attributes(
    request: &SetAttributesRequest,
    config: &ValidationConfig,
) -> Result<(), ValidationError> {
    if request.flow_id.is_empty() {
        return Err(invalid("flow_id", "flow ID is required"));
    }
    if request.request_id.is_empty() {
        return Err(invalid("request_id", "request ID is required"));
    }
    validate_attribute_writes("attributes", &request.attributes, config, true)?;
    validate_action_permissions(
        "action_permission_mappings",
        request.action_permission_mappings.as_ref(),
    )
}

fn validate_publish_to_channel(request: &PublishToChannelRequest) -> Result<(), ValidationError> {
    if request.flow_id.is_empty() {
        return Err(invalid("flow_id", "flow ID is required"));
    }
    for (index, message) in request.messages.iter().enumerate() {
        let path = format!("messages[{index}]");
        if message.channel_name.is_empty() {
            return Err(invalid(path, "channel name is required"));
        }
        validate_blob_value(&format!("{path}.value"), message.value.as_ref())?;
    }
    Ok(())
}

fn validate_delete_channel_message(
    request: &DeleteChannelMessageRequest,
) -> Result<(), ValidationError> {
    if request.flow_id.is_empty()
        || request.channel_name.is_empty()
        || request.message_id.is_empty()
    {
        return Err(invalid(
            "flow_id",
            "flow ID, channel name, and message ID are required",
        ));
    }
    if request.request_id.is_empty() {
        return Err(invalid("request_id", "request ID is required"));
    }
    Ok(())
}

fn validate_invoke_rpc(request: &InvokeRpcRequest) -> Result<(), ValidationError> {
    if request.flow_id.is_empty() || request.rpc_name.is_empty() {
        return Err(invalid("flow_id", "flow ID and RPC name are required"));
    }
    if request.request_id.is_empty() {
        return Err(invalid("request_id", "request ID is required"));
    }
    if request.timeout_seconds < 0 {
        return Err(invalid(
            "timeout_seconds",
            "RPC timeout must be non-negative",
        ));
    }
    validate_state_selections(
        "load",
        &request.load_attribute_map_instances,
        &request.load_channel_names,
        &request.load_channel_map_instances,
        "",
    )?;
    validate_blob_value("input", request.input.as_ref())
}

fn validate_update_flow_config(
    request: &UpdateFlowConfigRequest,
    config: &ValidationConfig,
) -> Result<(), ValidationError> {
    if request.flow_id.is_empty() || request.flow_config.is_none() {
        return Err(invalid("flow_id", "flow ID and flow config are required"));
    }
    let flow_config = request
        .flow_config
        .as_ref()
        .ok_or_else(|| invalid("flow_config", "flow ID and flow config are required"))?;
    validate_flow_config("flow_config", flow_config, config, false)
}

fn validate_skip_timer(request: &SkipTimerRequest) -> Result<(), ValidationError> {
    if request.flow_id.is_empty() || request.step_execution_id.is_empty() {
        return Err(invalid(
            "flow_id",
            "flow ID and step execution ID are required",
        ));
    }
    if request.timer_condition_id.is_empty() && request.timer_condition_index.is_none() {
        return Err(invalid(
            "timer_condition_id",
            "timer condition ID or index is required",
        ));
    }
    Ok(())
}

fn validate_reset_flow(request: &ResetFlowRequest) -> Result<(), ValidationError> {
    if request.flow_id.is_empty() {
        return Err(invalid("flow_id", "flow ID is required"));
    }
    match request.reset_type {
        1 => {
            if request.step_method != 0 {
                return Err(invalid(
                    "step_method",
                    "step method is only valid for a Step execution ID",
                ));
            }
        }
        2 => {
            if request.history_event_time.is_empty() {
                return Err(invalid(
                    "history_event_time",
                    "history event time is required",
                ));
            }
            if request.step_method != 0 {
                return Err(invalid(
                    "step_method",
                    "step method is only valid for a Step execution ID",
                ));
            }
        }
        3 => {
            if request.step_type.is_empty() {
                return Err(invalid("step_type", "step type is required"));
            }
            if request.step_method != 0 {
                return Err(invalid(
                    "step_method",
                    "step method is only valid for a Step execution ID",
                ));
            }
        }
        4 => {
            if request.step_execution_id.is_empty() {
                return Err(invalid(
                    "step_execution_id",
                    "step execution ID is required",
                ));
            }
            if request.step_method != 1 && request.step_method != 2 {
                return Err(invalid(
                    "step_method",
                    "step method must be WaitFor or Execute",
                ));
            }
        }
        _ => return Err(invalid("reset_type", "time travel type is required")),
    }
    Ok(())
}

fn validate_wait_request(
    flow_id: &str,
    request_timeout: i32,
    internal_timeout: i32,
) -> Result<(), ValidationError> {
    if flow_id.is_empty() || request_timeout < 0 || internal_timeout < 0 {
        return Err(invalid(
            "flow_id",
            "valid flow ID and non-negative request and internal handler timeouts are required",
        ));
    }
    Ok(())
}

fn validate_wait_for_attribute(request: &WaitForAttributeRequest) -> Result<(), ValidationError> {
    validate_wait_request(
        &request.flow_id,
        request.request_timeout_seconds,
        request.internal_handler_timeout_seconds,
    )?;
    let Some(attribute_match) = request.r#match.as_ref() else {
        return Err(invalid("match", "attribute match is required"));
    };
    if attribute_match.operand.is_none() {
        return Err(invalid("match", "attribute match is required"));
    }
    validate_blob_value("match.operand", attribute_match.operand.as_ref())?;
    validate_attribute_match(attribute_match)
}

fn validate_attribute_match(attribute_match: &AttributeMatch) -> Result<(), ValidationError> {
    if attribute_match.key.is_empty()
        || attribute_match
            .operand
            .as_ref()
            .is_none_or(|operand| operand.kind.is_none())
    {
        return Err(invalid(
            "match",
            "attribute match key, operator, and scalar operand are required",
        ));
    }
    if !(1..=6).contains(&attribute_match.operator) {
        return Err(invalid(
            "match.operator",
            "attribute match operator is invalid",
        ));
    }
    let operand = attribute_match.operand.as_ref().ok_or_else(|| {
        invalid(
            "match.operand",
            "attribute match key, operator, and scalar operand are required",
        )
    })?;
    match operand.kind.as_ref() {
        Some(value::Kind::StringValue(_) | value::Kind::BoolValue(_)) => {
            if attribute_match.operator >= 3 {
                return Err(invalid(
                    "match.operand",
                    "attribute match ordering requires an integer or double operand",
                ));
            }
        }
        Some(value::Kind::IntValue(_)) => {}
        Some(value::Kind::DoubleValue(number)) => {
            if !number.is_finite() {
                return Err(invalid(
                    "match.operand",
                    "attribute match double operand must be finite",
                ));
            }
        }
        _ => {
            return Err(invalid(
                "match.operand",
                "attribute match supports only string, boolean, integer, or double operands",
            ));
        }
    }
    Ok(())
}

fn validate_wait_for_step_completion(
    request: &WaitForStepCompletionRequest,
) -> Result<(), ValidationError> {
    validate_wait_request(
        &request.flow_id,
        request.request_timeout_seconds,
        request.internal_handler_timeout_seconds,
    )?;
    if request.step_type.is_empty() || request.step_execution_number.is_empty() {
        return Err(invalid(
            "step_type",
            "step type and step execution number are required",
        ));
    }
    if request
        .step_execution_number
        .parse::<i32>()
        .ok()
        .is_none_or(|number| number <= 0)
    {
        return Err(invalid(
            "step_execution_number",
            "step execution number must be a positive integer",
        ));
    }
    Ok(())
}
