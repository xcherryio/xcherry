// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::HashSet;
use std::time::Duration;

use dexzd_protocol::dex::{
    self, ActionPermissionMappings, ActiveStepSearchMode, AttributeWrite,
    ExecuteMethodFailurePolicy, FlowConfig, FlowTimeoutHandlerOptions, FlowTimeoutPolicy,
    StepDurability, StepOptions, Value, WaitForMethodFailurePolicy, WorkerTarget, value,
};

/// Identifies how the caller handles a validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValidationCategory {
    /// A client supplied an invalid request; the API returns INVALID_ARGUMENT.
    InvalidArgument,
    /// A Worker supplied an invalid response; the attempt may be retried.
    RetryableWorkerResponse,
    /// A Worker used an unsupported failure policy; the attempt must not be retried.
    NonRetryableWorkerResponse,
}

/// Carries a rejected field, its reason, and the caller's error category.
#[derive(Debug, thiserror::Error, Eq, PartialEq)]
#[error("{message}")]
pub struct ValidationError {
    /// Proto-style path to the field or collection that failed validation.
    pub field_path: String,
    /// Human-readable error matching the Go Server's meaning.
    pub message: String,
    /// Error handling category for the API or engine.
    pub category: ValidationCategory,
}

/// Supplies limits and server capabilities used by pure validators.
#[derive(Clone, Debug)]
pub struct ValidationConfig {
    /// Maximum UTF-8 byte length of an application Flow ID; defaults to 1000.
    pub maximum_flow_id_bytes: usize,
    /// Minimum positive Step heartbeat timeout; defaults to 10 seconds, matching the Go Server.
    pub minimum_step_heartbeat_timeout: Duration,
    /// Default Flow configuration applied before a StartFlow override.
    pub default_flow_config: FlowConfig,
    /// Configured Attribute Store names available to Flows.
    pub attribute_store_names: HashSet<String>,
    /// Whether hard-delete Attribute writes have blob storage.
    pub has_blob_store: bool,
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            maximum_flow_id_bytes: 1000,
            minimum_step_heartbeat_timeout: Duration::from_secs(10),
            default_flow_config: FlowConfig {
                active_step_search_mode: Some(ActiveStepSearchMode::Disabled as i32),
                continue_as_new_threshold: Some(100),
                step_durability: Some(StepDurability::Sync as i32),
                ..FlowConfig::default()
            },
            attribute_store_names: HashSet::new(),
            has_blob_store: false,
        }
    }
}

pub(crate) fn invalid(path: impl Into<String>, message: impl Into<String>) -> ValidationError {
    ValidationError {
        field_path: path.into(),
        message: message.into(),
        category: ValidationCategory::InvalidArgument,
    }
}

fn unsupported(path: impl Into<String>, feature: &str) -> ValidationError {
    invalid(path, format!("server-zd does not support {feature}"))
}

fn unsupported_step_policy(path: impl Into<String>, feature: &str) -> ValidationError {
    ValidationError {
        field_path: path.into(),
        message: format!("server-zd does not support {feature}"),
        category: ValidationCategory::NonRetryableWorkerResponse,
    }
}

pub(crate) fn validate_identifier(
    path: &str,
    name: &str,
    value: &str,
) -> Result<(), ValidationError> {
    for character in ['/', '$', ':'] {
        if value.contains(character) {
            return Err(invalid(
                path,
                format!("{name} contains reserved character {character:?}"),
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_blob_value(
    path: &str,
    value: Option<&Value>,
) -> Result<(), ValidationError> {
    if matches!(
        value.and_then(|value| value.kind.as_ref()),
        Some(value::Kind::InternalBlobIdForStringValue(_))
            | Some(value::Kind::InternalBlobIdForObjValue(_))
    ) {
        return Err(invalid(
            path,
            "worker response must not contain internal_blob_id arms",
        ));
    }
    Ok(())
}

pub(crate) fn validate_attribute_writes(
    path: &str,
    writes: &[AttributeWrite],
    config: &ValidationConfig,
    is_runtime: bool,
) -> Result<(), ValidationError> {
    let mut seen = HashSet::new();
    for (index, write) in writes.iter().enumerate() {
        let item_path = format!("{path}[{index}]");
        if write.key.is_empty()
            || write
                .value
                .as_ref()
                .is_none_or(|value| value.kind.is_none())
        {
            return Err(invalid(item_path, format!("attribute {index} is invalid")));
        }
        if !seen.insert(&write.key) {
            return Err(invalid(item_path, "attribute keys must be unique"));
        }
        validate_blob_value(&format!("{item_path}.value"), write.value.as_ref())?;
        if is_runtime
            && (write.key == "DexWorkQueuePermissions"
                || write
                    .index_config
                    .as_ref()
                    .is_some_and(|index| index.index_key == "DexWorkQueuePermissions"))
        {
            return Err(invalid(
                item_path.clone(),
                "DexWorkQueuePermissions is managed by the Server",
            ));
        }
        validate_hard_delete_write(&item_path, write, config, false)?;
    }
    Ok(())
}

pub(crate) fn validate_hard_delete_write(
    path: &str,
    write: &AttributeWrite,
    config: &ValidationConfig,
    allow_missing_value: bool,
) -> Result<(), ValidationError> {
    if !write.is_hard_delete_on_null {
        return Ok(());
    }
    match write.value.as_ref().and_then(|value| value.kind.as_ref()) {
        None if allow_missing_value && write.value.is_none() => {}
        Some(
            value::Kind::NullValue(_) | value::Kind::StringValue(_) | value::Kind::ObjValue(_),
        ) => {}
        None => {
            return Err(invalid(
                path,
                format!(
                    "hard-delete attribute {:?} has an empty value; use null to delete it",
                    write.key
                ),
            ));
        }
        Some(_) => {
            return Err(invalid(
                path,
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
            path,
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
            path,
            "hard-delete Attributes require an enabled blob store",
        ));
    }
    Ok(())
}

pub(crate) fn validate_action_permissions(
    path: &str,
    mappings: Option<&ActionPermissionMappings>,
) -> Result<(), ValidationError> {
    let Some(mappings) = mappings else {
        return Ok(());
    };
    for (index, mapping) in mappings.mappings.iter().enumerate() {
        let item_path = format!("{path}.mappings[{index}]");
        if mapping.attribute_key.is_empty() {
            return Err(invalid(
                item_path,
                format!("Action permission mapping {index} has an empty Attribute key"),
            ));
        }
        if mapping.attribute_key == "DexWorkQueuePermissions" {
            return Err(invalid(
                item_path,
                format!(
                    "Action permission mapping {index} uses server-managed Attribute DexWorkQueuePermissions"
                ),
            ));
        }
        let permission = &mapping.required_permission;
        let segments: Vec<_> = permission.split(['.', '-']).collect();
        let is_valid = segments.iter().enumerate().all(|(segment_index, segment)| {
            let mut characters = segment.chars();
            characters.next().is_some_and(|character| {
                character.is_ascii_lowercase() || (segment_index > 0 && character.is_ascii_digit())
            }) && characters
                .all(|character| character.is_ascii_lowercase() || character.is_ascii_digit())
        });
        if !is_valid {
            return Err(invalid(
                item_path,
                format!("Action permission mapping {index} has invalid permission {permission:?}"),
            ));
        }
        if mapping.equal_values.is_empty() {
            return Err(invalid(
                item_path,
                format!("Action permission mapping {index} has no equal values"),
            ));
        }
        for (value_index, value) in mapping.equal_values.iter().enumerate() {
            if !matches!(
                value.kind.as_ref(),
                Some(value::Kind::StringValue(_))
                    | Some(value::Kind::IntValue(_))
                    | Some(value::Kind::BoolValue(_))
            ) && !matches!(value.kind.as_ref(), Some(value::Kind::DoubleValue(number)) if number.is_finite())
            {
                return Err(invalid(
                    format!("{item_path}.equal_values[{value_index}]"),
                    format!(
                        "Action permission mapping {index} equal value {value_index} is not a finite scalar"
                    ),
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_flow_timeout_policy(
    path: &str,
    timeout_seconds: i32,
    policy: i32,
) -> Result<i32, ValidationError> {
    if timeout_seconds < 0 {
        return Err(invalid(path, "flow timeout must be non-negative"));
    }
    if timeout_seconds == 0 {
        if policy != FlowTimeoutPolicy::Unspecified as i32 {
            return Err(invalid(
                path,
                "flow timeout policy requires a positive timeout",
            ));
        }
        return Ok(FlowTimeoutPolicy::Unspecified as i32);
    }
    match FlowTimeoutPolicy::try_from(policy) {
        Ok(FlowTimeoutPolicy::Unspecified) => Ok(FlowTimeoutPolicy::Fail as i32),
        Ok(_) => Ok(policy),
        Err(_) => Err(invalid(path, "unknown flow timeout policy")),
    }
}

pub(crate) fn validate_timeout_handler(
    path: &str,
    timeout_seconds: i32,
    policy: i32,
    options: Option<&FlowTimeoutHandlerOptions>,
    config: &ValidationConfig,
) -> Result<(), ValidationError> {
    let Some(options) = options else {
        return Ok(());
    };
    if timeout_seconds <= 0 || policy != FlowTimeoutPolicy::Handler as i32 {
        return Err(invalid(
            path,
            "Flow timeout handler options require the Handler policy and a positive timeout",
        ));
    }
    if options.method_timeout_seconds < 0 {
        return Err(invalid(
            format!("{path}.method_timeout_seconds"),
            "Flow timeout handler method timeout must be non-negative",
        ));
    }
    if options.failure_policy == ExecuteMethodFailurePolicy::ProceedToConfiguredStep as i32 {
        return Err(unsupported_step_policy(
            format!("{path}.failure_policy"),
            "Flow timeout handler proceed failure policy",
        ));
    }
    if !options.failure_proceed_step_type.is_empty() {
        return Err(unsupported_step_policy(
            format!("{path}.failure_proceed_step_type"),
            "Flow timeout handler failure_proceed_step_type",
        ));
    }
    if options.failure_proceed_step_options.is_some()
        && matches!(
            ExecuteMethodFailurePolicy::try_from(options.failure_policy),
            Ok(ExecuteMethodFailurePolicy::Unspecified
                | ExecuteMethodFailurePolicy::FailFlowOnExecuteMethodFailure)
        )
    {
        return Err(invalid(
            format!("{path}.failure_proceed_step_options"),
            "Flow timeout handler failure target requires the proceed policy",
        ));
    }
    let mapped = StepOptions {
        execute_timeout_seconds: options.method_timeout_seconds,
        execute_retry_policy: options.retry_policy,
        execute_failure_proceed_step_options: options
            .failure_proceed_step_options
            .clone()
            .map(Box::new),
        execute_failure_policy: options.failure_policy,
        heartbeat_timeout_seconds: options.heartbeat_timeout_seconds,
        execute_durability_override: options.durability_override,
        execute_lock_attribute_keys: options.lock_attribute_keys.clone(),
        execute_load_attribute_map_instances: options.load_attribute_map_instances.clone(),
        execute_load_channel_names: options.load_channel_names.clone(),
        execute_load_channel_map_instances: options.load_channel_map_instances.clone(),
        ..StepOptions::default()
    };
    validate_step_options(path, Some(&mapped), config)?;
    if ExecuteMethodFailurePolicy::try_from(options.failure_policy).is_err() {
        return Err(invalid(
            format!("{path}.failure_policy"),
            "unknown Flow timeout handler failure policy",
        ));
    }
    Ok(())
}

pub(crate) fn validate_step_options(
    path: &str,
    options: Option<&StepOptions>,
    config: &ValidationConfig,
) -> Result<(), ValidationError> {
    let mut current = options;
    let mut current_path = path.to_owned();
    while let Some(options) = current {
        if options.wait_for_failure_policy == WaitForMethodFailurePolicy::ProceedOnFailure as i32 {
            return Err(unsupported_step_policy(
                format!("{current_path}.wait_for_failure_policy"),
                "WAIT_FOR_METHOD_FAILURE_POLICY_PROCEED_ON_FAILURE",
            ));
        }
        if options.execute_failure_policy
            == ExecuteMethodFailurePolicy::ProceedToConfiguredStep as i32
        {
            return Err(unsupported_step_policy(
                format!("{current_path}.execute_failure_policy"),
                "EXECUTE_METHOD_FAILURE_POLICY_PROCEED_TO_CONFIGURED_STEP",
            ));
        }
        if !options.execute_failure_proceed_step_type.is_empty() {
            return Err(unsupported_step_policy(
                format!("{current_path}.execute_failure_proceed_step_type"),
                "execute_failure_proceed_step_type",
            ));
        }
        validate_identifier(
            &format!("{current_path}.execute_failure_proceed_step_type"),
            "step type",
            &options.execute_failure_proceed_step_type,
        )?;
        let heartbeat = options.heartbeat_timeout_seconds;
        if heartbeat < 0
            || (heartbeat > 0
                && Duration::from_secs(heartbeat as u64) < config.minimum_step_heartbeat_timeout)
        {
            return Err(invalid(
                format!("{current_path}.heartbeat_timeout_seconds"),
                format!(
                    "heartbeat timeout must be zero or at least {}",
                    format_duration(config.minimum_step_heartbeat_timeout)
                ),
            ));
        }
        validate_state_selections(
            &format!("{current_path}.wait_for"),
            &options.wait_for_load_attribute_map_instances,
            &options.wait_for_load_channel_names,
            &options.wait_for_load_channel_map_instances,
            "WaitFor",
        )?;
        validate_state_selections(
            &format!("{current_path}.execute"),
            &options.execute_load_attribute_map_instances,
            &options.execute_load_channel_names,
            &options.execute_load_channel_map_instances,
            "Execute",
        )?;
        current = options.execute_failure_proceed_step_options.as_deref();
        current_path.push_str(".execute_failure_proceed_step_options");
    }
    Ok(())
}

fn format_duration(duration: Duration) -> String {
    if duration.is_zero() {
        "0s".to_owned()
    } else {
        format!("{duration:?}")
    }
}

pub(crate) fn validate_state_selections(
    path: &str,
    maps: &[String],
    channels: &[String],
    channel_maps: &[String],
    prefix: &str,
) -> Result<(), ValidationError> {
    for (kind, values, is_map) in [
        ("AttributeMap", maps, true),
        ("Channel", channels, false),
        ("ChannelMap", channel_maps, true),
    ] {
        let mut seen = HashSet::new();
        for (index, value) in values.iter().enumerate() {
            let item_path = format!("{path}.{kind}[{index}]");
            let message = if value.trim().is_empty() {
                Some(if is_map {
                    format!("state load {kind} instance is empty")
                } else {
                    format!("state load {kind} name is empty")
                })
            } else if is_map
                && (value.find('/').is_none_or(|position| position == 0)
                    || value.matches('/').count() != 1)
            {
                Some(format!(
                    "state load {kind} instance {value:?} must contain one '/' after the definition name"
                ))
            } else if !is_map && value.contains('/') {
                Some(format!("state load {kind} name {value:?} contains '/'"))
            } else if !seen.insert(value) {
                Some(format!(
                    "state load {kind} {} {value:?} is duplicated",
                    if is_map { "instance" } else { "name" }
                ))
            } else {
                None
            };
            if let Some(message) = message {
                let message = if prefix.is_empty() {
                    message
                } else {
                    format!("{prefix} {message}")
                };
                return Err(invalid(item_path, message));
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_flow_config(
    path: &str,
    flow_config: &FlowConfig,
    config: &ValidationConfig,
    require_target: bool,
) -> Result<(), ValidationError> {
    if let Some(threshold) = flow_config.continue_as_new_threshold
        && threshold < 0
    {
        return Err(invalid(
            format!("{path}.continue_as_new_threshold"),
            format!("continue_as_new_threshold must be >= 0, got {threshold}"),
        ));
    }
    if let Some(page_size) = flow_config.continue_as_new_page_size_in_bytes
        && page_size < 0
    {
        return Err(invalid(
            format!("{path}.continue_as_new_page_size_in_bytes"),
            format!("continue_as_new_page_size_in_bytes must be >= 0, got {page_size}"),
        ));
    }
    if flow_config
        .step_durability
        .is_some_and(|value| StepDurability::try_from(value).is_err())
    {
        return Err(invalid(
            format!("{path}.step_durability"),
            format!(
                "unknown step_durability enum value {}",
                flow_config.step_durability.unwrap_or_default()
            ),
        ));
    }
    if flow_config
        .active_step_search_mode
        .is_some_and(|value| ActiveStepSearchMode::try_from(value).is_err())
    {
        return Err(invalid(
            format!("{path}.active_step_search_mode"),
            format!(
                "unknown active_step_search_mode enum value {}",
                flow_config.active_step_search_mode.unwrap_or_default()
            ),
        ));
    }
    if require_target || flow_config.worker_target.is_some() {
        normalize_worker_target(
            &format!("{path}.worker_target"),
            flow_config.worker_target.as_ref(),
        )?;
    }
    if let Some(stores) = &flow_config.attribute_store_names {
        let mut seen = HashSet::new();
        for (index, name) in stores.names.iter().enumerate() {
            let item_path = format!("{path}.attribute_store_names.names[{index}]");
            if name.is_empty() {
                return Err(invalid(item_path, "Attribute Store name must not be empty"));
            }
            if !seen.insert(name) {
                return Err(invalid(
                    item_path,
                    format!("duplicate Attribute Store {name:?}"),
                ));
            }
            if !config.attribute_store_names.contains(name) {
                return Err(invalid(
                    item_path,
                    format!("unknown Attribute Store {name:?}"),
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn apply_flow_config_override(effective: &mut FlowConfig, override_config: &FlowConfig) {
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

pub(crate) fn normalize_worker_target(
    path: &str,
    target: Option<&WorkerTarget>,
) -> Result<WorkerTarget, ValidationError> {
    let Some(target) = target else {
        return Err(invalid(path, "worker_target is required"));
    };
    if !target.address.is_empty() {
        return Err(unsupported(
            format!("{path}.address"),
            "Worker target addresses",
        ));
    }
    if !target.is_pull_mode {
        return Err(invalid(path, "worker_target is empty"));
    }
    if target.is_headless_address {
        return Err(invalid(
            path,
            "pull worker_target cannot use headless address",
        ));
    }
    Ok(WorkerTarget {
        is_pull_mode: true,
        ..WorkerTarget::default()
    })
}

pub(crate) fn validate_start_options(
    path: &str,
    delay: i32,
    has_retry_policy: bool,
) -> Result<(), ValidationError> {
    if delay < 0 {
        return Err(invalid(
            format!("{path}.flow_start_delay_seconds"),
            "flow start delay must be non-negative",
        ));
    }
    if delay > 0 {
        return Err(unsupported(
            format!("{path}.flow_start_delay_seconds"),
            "flow_start_delay_seconds > 0",
        ));
    }
    if has_retry_policy {
        return Err(unsupported(
            format!("{path}.retry_policy"),
            "Flow retry_policy",
        ));
    }
    Ok(())
}

pub(crate) fn validate_kv_blob_values(
    path: &str,
    values: &[dex::Kv],
) -> Result<(), ValidationError> {
    for (index, value) in values.iter().enumerate() {
        validate_blob_value(&format!("{path}[{index}].value"), value.value.as_ref())?;
    }
    Ok(())
}
