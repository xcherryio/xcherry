// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::HashSet;

use dexzd_protocol::dex::{FlowConfig, SubFlowReusePolicy, WaitingCondition, WaitingConditionType};

use crate::shared::{
    ValidationCategory, ValidationConfig, ValidationError, apply_flow_config_override, invalid,
    validate_attribute_writes, validate_blob_value, validate_flow_config,
    validate_flow_timeout_policy, validate_identifier, validate_start_options,
    validate_step_options, validate_timeout_handler,
};

/// Validates a Worker's WaitFor conditions, including nested SubFlow options.
pub fn validate_waiting_condition(
    waiting: Option<&WaitingCondition>,
    parent_flow_config: &FlowConfig,
    config: &ValidationConfig,
) -> Result<(), ValidationError> {
    do_validate_waiting_condition(waiting, parent_flow_config, config).map_err(|mut error| {
        if error.category == ValidationCategory::InvalidArgument {
            error.category = ValidationCategory::RetryableWorkerResponse;
        }
        error
    })
}

fn do_validate_waiting_condition(
    waiting: Option<&WaitingCondition>,
    parent_flow_config: &FlowConfig,
    config: &ValidationConfig,
) -> Result<(), ValidationError> {
    let Some(waiting) = waiting else {
        return Ok(());
    };
    let mut declared_ids = HashSet::new();
    let requires_ids =
        waiting.waiting_condition_type == WaitingConditionType::AnyCombinationCompleted as i32;
    for (index, timer) in waiting.timer_conditions.iter().enumerate() {
        let path = format!("waiting_condition.timer_conditions[{index}]");
        register_id(
            &mut declared_ids,
            &path,
            &timer.condition_id,
            "timer",
            requires_ids,
        )?;
        if timer.duration_seconds < 0 {
            return Err(invalid(
                format!("{path}.duration_seconds"),
                format!(
                    "timer condition {:?} has negative duration_seconds {}",
                    timer.condition_id, timer.duration_seconds
                ),
            ));
        }
        if timer.firing_unix_timestamp_seconds != 0 {
            return Err(invalid(
                format!("{path}.firing_unix_timestamp_seconds"),
                format!(
                    "timer condition {:?} sets server-owned firing_unix_timestamp_seconds",
                    timer.condition_id
                ),
            ));
        }
    }
    for (index, channel) in waiting.channel_conditions.iter().enumerate() {
        let path = format!("waiting_condition.channel_conditions[{index}]");
        register_id(
            &mut declared_ids,
            &path,
            &channel.condition_id,
            "channel",
            requires_ids,
        )?;
        if channel.channel_name.is_empty() {
            return Err(invalid(
                format!("{path}.channel_name"),
                format!(
                    "channel condition {:?} has an empty channel_name",
                    channel.condition_id
                ),
            ));
        }
        if let Some(at_least) = channel.at_least
            && at_least < 0
        {
            return Err(invalid(
                format!("{path}.at_least"),
                format!(
                    "channel condition {:?} has negative at_least {at_least}",
                    channel.condition_id
                ),
            ));
        }
        if let Some(at_most) = channel.at_most {
            if at_most < 0 {
                return Err(invalid(
                    format!("{path}.at_most"),
                    format!(
                        "channel condition {:?} has negative at_most {at_most}",
                        channel.condition_id
                    ),
                ));
            }
            if at_most < channel.at_least.unwrap_or_default() {
                return Err(invalid(
                    format!("{path}.at_most"),
                    format!(
                        "channel condition {:?} has at_most {at_most} < at_least {}",
                        channel.condition_id,
                        channel.at_least.unwrap_or_default()
                    ),
                ));
            }
        }
    }
    for (index, subflow) in waiting.sub_flow_conditions.iter().enumerate() {
        let path = format!("waiting_condition.sub_flow_conditions[{index}]");
        register_id(
            &mut declared_ids,
            &path,
            &subflow.condition_id,
            "SubFlow",
            requires_ids,
        )?;
        if subflow.sub_flow_type.is_empty() || subflow.start_step_type.is_empty() {
            return Err(invalid(
                path.clone(),
                format!("SubFlow condition at index {index} requires Flow and starting Step types"),
            ));
        }
        validate_identifier(
            &format!("{path}.start_step_type"),
            "step type",
            &subflow.start_step_type,
        )?;
        validate_step_options(
            &format!("{path}.step_options"),
            subflow.step_options.as_ref(),
            config,
        )?;
        if subflow.sub_flow_index != index as i32 {
            return Err(invalid(
                format!("{path}.sub_flow_index"),
                format!(
                    "SubFlow condition at index {index} has unstable index {}",
                    subflow.sub_flow_index
                ),
            ));
        }
        if let Some(options) = &subflow.options {
            let options_path = format!("{path}.options");
            let timeout_policy = validate_flow_timeout_policy(
                &format!("{options_path}.flow_timeout_policy"),
                options.flow_timeout_seconds,
                options.flow_timeout_policy,
            )?;
            validate_timeout_handler(
                &format!("{options_path}.timeout_handler_options"),
                options.flow_timeout_seconds,
                timeout_policy,
                options.timeout_handler_options.as_ref(),
                config,
            )?;
            if options.flow_start_delay_seconds < 0 {
                return Err(invalid(
                    format!("{options_path}.flow_start_delay_seconds"),
                    format!("SubFlow condition at index {index} has a negative start delay"),
                ));
            }
            validate_start_options(
                &options_path,
                options.flow_start_delay_seconds,
                options.retry_policy.is_some(),
            )?;
            if SubFlowReusePolicy::try_from(options.reuse_policy).is_err() {
                return Err(invalid(
                    format!("{options_path}.reuse_policy"),
                    format!("SubFlow condition at index {index} has an unknown reuse policy"),
                ));
            }
            validate_blob_value(&format!("{path}.step_input"), subflow.step_input.as_ref())?;
            validate_attribute_writes(
                &format!("{options_path}.attributes"),
                &options.attributes,
                config,
                false,
            )?;
        } else {
            validate_blob_value(&format!("{path}.step_input"), subflow.step_input.as_ref())?;
        }
        let mut effective = parent_flow_config.clone();
        if let Some(override_config) = subflow
            .options
            .as_ref()
            .and_then(|options| options.flow_config_override.as_ref())
        {
            apply_flow_config_override(&mut effective, override_config);
        }
        validate_flow_config(
            &format!("{path}.options.flow_config_override"),
            &effective,
            config,
            true,
        )?;
    }
    match WaitingConditionType::try_from(waiting.waiting_condition_type) {
        Ok(WaitingConditionType::AllCompleted | WaitingConditionType::AnyCompleted) => {
            if !waiting.condition_combinations.is_empty() {
                return Err(invalid(
                    "waiting_condition.condition_combinations",
                    "condition_combinations are only valid for ANY_COMBINATION_COMPLETED",
                ));
            }
        }
        Ok(WaitingConditionType::AnyCombinationCompleted) => {
            validate_combinations(waiting, &declared_ids)?
        }
        _ => {
            return Err(invalid(
                "waiting_condition.waiting_condition_type",
                format!(
                    "unknown waiting_condition_type {}",
                    waiting.waiting_condition_type
                ),
            ));
        }
    }
    Ok(())
}

fn register_id<'a>(
    declared: &mut HashSet<&'a str>,
    path: &str,
    condition_id: &'a str,
    kind: &str,
    required: bool,
) -> Result<(), ValidationError> {
    if condition_id.is_empty() {
        if required {
            return Err(invalid(
                format!("{path}.condition_id"),
                format!("{kind} condition has an empty condition_id"),
            ));
        }
        return Ok(());
    }
    if !declared.insert(condition_id) {
        return Err(invalid(
            format!("{path}.condition_id"),
            format!("duplicate condition_id {condition_id:?}"),
        ));
    }
    Ok(())
}

fn validate_combinations(
    waiting: &WaitingCondition,
    declared: &HashSet<&str>,
) -> Result<(), ValidationError> {
    if waiting.condition_combinations.is_empty() {
        return Err(invalid(
            "waiting_condition.condition_combinations",
            "ANY_COMBINATION_COMPLETED requires at least one condition_combination",
        ));
    }
    for (index, combination) in waiting.condition_combinations.iter().enumerate() {
        let path = format!("waiting_condition.condition_combinations[{index}]");
        if combination.condition_ids.is_empty() {
            return Err(invalid(
                path,
                format!("condition_combination at index {index} is empty"),
            ));
        }
        let mut seen = HashSet::new();
        for (condition_index, condition_id) in combination.condition_ids.iter().enumerate() {
            let id_path = format!("{path}.condition_ids[{condition_index}]");
            if !declared.contains(condition_id.as_str()) {
                return Err(invalid(
                    id_path,
                    format!(
                        "condition_combination at index {index} references undeclared condition_id {condition_id:?}"
                    ),
                ));
            }
            if !seen.insert(condition_id) {
                return Err(invalid(
                    id_path,
                    format!(
                        "condition_combination at index {index} has duplicate condition_id {condition_id:?}"
                    ),
                ));
            }
        }
    }
    Ok(())
}
