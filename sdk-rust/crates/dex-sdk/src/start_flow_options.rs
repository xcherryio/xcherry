// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::time::Duration;

use std::sync::Arc;

use dex_protocol::dex::AttributeWrite;

use crate::attribute::{
    AttributeWriteSettings, require_registered_hard_delete_setting,
    validate_hard_delete_attribute_write,
};
use crate::client::invalid;
use crate::persistence::PersistenceDefinition;
use crate::registry::physical_name;
use crate::step::{ErasedValue, TypedValue};
use crate::{
    Attribute, AttributeMap, FlowConfig, FlowTimeoutHandlerOptions, RetryPolicy, SdkResult, Value,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Controls whether [`crate::Client::start_flow_with_options`] may reuse a Flow ID.
pub enum IdReusePolicy {
    /// Uses the Dex server's configured reuse policy.
    Default,
    /// Allows reuse only when the previous run failed.
    AllowIfPreviousFailed,
    /// Allows reuse when no run with the ID is currently active.
    AllowIfNotRunning,
    /// Rejects reuse regardless of the previous run status.
    Disallow,
    /// Terminates an active run before starting the replacement.
    TerminateIfRunning,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Controls how Dex responds when a positive soft Flow timeout expires.
pub enum FlowTimeoutPolicy {
    /// Uses the Flow's registered timeout handler when present, and fails otherwise.
    Default,
    /// Fails with [`crate::FlowErrorType::FlowTimeout`] and permits Flow retries.
    Fail,
    /// Cancels without retrying the Flow.
    Cancel,
    /// Invokes the registered [`crate::FlowTimeoutHandler`] as one retryable logical execution.
    Handler,
}

#[derive(Clone)]
/// Configures a new Flow execution.
///
/// All optional fields are absent by default. Initial Attribute values are encoded when the start
/// request is sent. Use a stable request ID to make retries of the same logical request idempotent.
///
/// A stable request ID alone does not make a retry succeed. Set
/// [`StartFlowOptions::request_id`] and [`StartFlowOptions::ignore_already_started`] together, and
/// reuse the same values on every retry of one logical start, for example after a lost response.
///
/// # Examples
///
/// ```
/// use dex_sdk::{Attribute, IdReusePolicy, StartFlowOptions};
/// use std::time::Duration;
///
/// let customer = Attribute::<String>::new("customer");
/// let options = StartFlowOptions::new()
///     .timeout(Duration::from_secs(3_600))
///     .id_reuse_policy(IdReusePolicy::AllowIfNotRunning)
///     .initial_attribute(&customer, "customer-42".to_owned())
///     .request_id("create-order-42");
/// ```
///
/// Retry-safe start options:
///
/// ```
/// use dex_sdk::StartFlowOptions;
///
/// let order_id = "42";
/// let options = StartFlowOptions::new()
///     .ignore_already_started(true)
///     .request_id(format!("start-order-{order_id}"));
/// ```
pub struct StartFlowOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) timeout_policy: FlowTimeoutPolicy,
    pub(crate) timeout_handler_options: Option<FlowTimeoutHandlerOptions>,
    pub(crate) start_delay: Option<Duration>,
    pub(crate) id_reuse_policy: IdReusePolicy,
    pub(crate) retry_policy: Option<RetryPolicy>,
    pub(crate) config_override: Option<FlowConfig>,
    pub(crate) ignore_already_started: bool,
    pub(crate) request_id: Option<String>,
    pub(crate) attributes: Vec<InitialAttribute>,
}

#[derive(Clone)]
pub(crate) struct InitialAttribute {
    pub(crate) key: String,
    pub(crate) attribute_name: String,
    pub(crate) value: Arc<dyn ErasedValue>,
    pub(crate) write_settings: AttributeWriteSettings,
}

impl InitialAttribute {
    pub(crate) fn for_attribute<T: Value>(attribute: &Attribute<T>, value: T) -> Self {
        Self {
            key: attribute.name().to_string(),
            attribute_name: attribute.name().to_string(),
            value: Arc::new(TypedValue(value)),
            write_settings: attribute.write_settings(),
        }
    }

    pub(crate) fn for_attribute_map<T: Value>(
        attribute: &AttributeMap<T>,
        instance: &str,
        value: T,
    ) -> Self {
        Self {
            key: physical_name(attribute.name(), instance),
            attribute_name: attribute.name().to_string(),
            value: Arc::new(TypedValue(value)),
            write_settings: attribute.write_settings(),
        }
    }

    /// Returns the encoded write after checking it against the registered `definition`.
    ///
    /// Returns [`crate::SdkError::InvalidArgument`] for a hard-delete mismatch or an invalid
    /// hard-delete combination or value. Returns [`crate::SdkError::ValueMapping`] when the value
    /// cannot be encoded.
    pub(crate) fn encode_attribute_write(
        &self,
        definition: &PersistenceDefinition,
    ) -> SdkResult<AttributeWrite> {
        require_registered_hard_delete_setting(
            &self.attribute_name,
            definition.is_hard_delete_on_null,
            self.write_settings.is_hard_delete_on_null,
        )
        .map_err(invalid)?;
        let write = self
            .write_settings
            .clone()
            .attribute_write(self.key.clone(), self.value.encode()?);
        validate_hard_delete_attribute_write(&write).map_err(invalid)?;
        Ok(write)
    }
}

impl StartFlowOptions {
    /// Creates start options that preserve all server defaults.
    pub fn new() -> Self {
        Self {
            timeout: None,
            timeout_policy: FlowTimeoutPolicy::Default,
            timeout_handler_options: None,
            start_delay: None,
            id_reuse_policy: IdReusePolicy::Default,
            retry_policy: None,
            config_override: None,
            ignore_already_started: false,
            request_id: None,
            attributes: Vec::new(),
        }
    }

    /// Sets Dex's durable soft timeout. A zero duration disables it.
    pub fn timeout(mut self, value: Duration) -> Self {
        self.timeout = Some(value);
        self
    }

    /// Sets the action taken when the positive Flow timeout expires.
    ///
    /// Handler is rejected before start when the registered Flow has no timeout handler.
    pub fn timeout_policy(mut self, value: FlowTimeoutPolicy) -> Self {
        self.timeout_policy = value;
        self
    }

    /// Configures execution and selective state loading for the Flow timeout handler.
    ///
    /// Dex rejects this option unless the Flow has a positive timeout and resolves to
    /// [`FlowTimeoutPolicy::Handler`].
    pub fn timeout_handler_options(mut self, value: FlowTimeoutHandlerOptions) -> Self {
        self.timeout_handler_options = Some(value);
        self
    }

    /// Delays the first Step after the server accepts the Flow.
    pub fn start_delay(mut self, value: Duration) -> Self {
        self.start_delay = Some(value);
        self
    }

    /// Sets how the server handles an existing Flow with the same ID.
    pub fn id_reuse_policy(mut self, value: IdReusePolicy) -> Self {
        self.id_reuse_policy = value;
        self
    }

    /// Sets the whole-Flow retry policy after a terminal failure.
    pub fn retry_policy(mut self, value: RetryPolicy) -> Self {
        self.retry_policy = Some(value);
        self
    }

    /// Adds an initial value for a declared Attribute.
    pub fn initial_attribute<T: Value>(mut self, attribute: &Attribute<T>, value: T) -> Self {
        self.attributes
            .push(InitialAttribute::for_attribute(attribute, value));
        self
    }

    /// Adds an initial value for one Attribute-map instance.
    /// Slash is prohibited in instance keys because it is a reserved character.
    pub fn initial_attribute_map<T: Value>(
        mut self,
        attribute: &AttributeMap<T>,
        instance: &str,
        value: T,
    ) -> Self {
        self.attributes.push(InitialAttribute::for_attribute_map(
            attribute, instance, value,
        ));
        self
    }

    /// Overrides the registered Flow configuration for this execution.
    pub fn config_override(mut self, value: FlowConfig) -> Self {
        self.config_override = Some(value);
        self
    }

    /// Returns the existing run ID instead of an error when the Flow already exists.
    ///
    /// It applies only when [`StartFlowOptions::request_id`] matches the request ID that started
    /// the existing run. A different or generated request ID still returns
    /// [`SdkError::FlowAlreadyStarted`](crate::SdkError::FlowAlreadyStarted). See
    /// [`StartFlowOptions`] for an example that sets both options.
    pub fn ignore_already_started(mut self, value: bool) -> Self {
        self.ignore_already_started = value;
        self
    }

    /// Sets the idempotency key for this start request.
    ///
    /// When omitted, the SDK generates a new request ID for every call. A retried start returns the
    /// existing run only when [`StartFlowOptions::ignore_already_started`] is `true` and this
    /// request ID matches the request that started that run. Without that option, the retry returns
    /// [`SdkError::FlowAlreadyStarted`](crate::SdkError::FlowAlreadyStarted) even when the first
    /// attempt created the Flow. The match applies only when the ID reuse policy rejects the start.
    /// A policy that permits another run starts a new run instead. Dex compares only request IDs,
    /// not input or other options, so never reuse a request ID for a different logical start. See
    /// [`StartFlowOptions`] for an example that sets both options.
    pub fn request_id(mut self, value: impl Into<String>) -> Self {
        self.request_id = Some(value.into());
        self
    }
}

impl Default for StartFlowOptions {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::StartFlowOptions;
    use crate::persistence::{PersistenceDefinition, PersistenceKind};
    use crate::{Attribute, AttributeMap, SdkError};

    #[test]
    fn initial_attributes_retain_sync_configuration() {
        let plain = Attribute::<String>::new("plain");
        let synced = Attribute::<String>::new("synced").sync_to_attribute_store();
        let synced_map = AttributeMap::<String>::new("map").sync_to_attribute_store();
        let options = StartFlowOptions::new()
            .initial_attribute(&plain, "plain".to_string())
            .initial_attribute(&synced, "synced".to_string())
            .initial_attribute_map(&synced_map, "tenant-1", "mapped".to_string());

        assert!(options.attributes[0].write_settings.sync_config.is_none());
        assert_eq!(
            options.attributes[1]
                .write_settings
                .sync_config
                .as_ref()
                .map(|config| config.enabled),
            Some(true)
        );
        assert_eq!(
            options.attributes[2]
                .write_settings
                .sync_config
                .as_ref()
                .map(|config| config.enabled),
            Some(true)
        );
    }

    #[derive(serde::Deserialize, serde::Serialize)]
    struct NumericNewtype(u64);

    #[test]
    fn initial_hard_delete_attributes_set_the_flag_and_validate_values() {
        let secret = Attribute::<String>::new("secret").hard_delete_on_null();
        let secrets = AttributeMap::<Option<String>>::new("secrets").hard_delete_on_null();
        let numeric = Attribute::<NumericNewtype>::new("numeric").hard_delete_on_null();
        let plain_secret = Attribute::<String>::new("secret");
        let options = StartFlowOptions::new()
            .initial_attribute(&secret, "key-1".to_string())
            .initial_attribute_map(&secrets, "tenant-1", None)
            .initial_attribute(&numeric, NumericNewtype(7))
            .initial_attribute(&plain_secret, "leak".to_string());
        let secret_definition = PersistenceDefinition {
            is_hard_delete_on_null: true,
            ..PersistenceDefinition::new("secret", PersistenceKind::Attribute)
        };
        let secrets_definition = PersistenceDefinition {
            is_hard_delete_on_null: true,
            ..PersistenceDefinition::new("secrets", PersistenceKind::AttributeMap)
        };
        let numeric_definition = PersistenceDefinition {
            is_hard_delete_on_null: true,
            ..PersistenceDefinition::new("numeric", PersistenceKind::Attribute)
        };

        let write = options.attributes[0]
            .encode_attribute_write(&secret_definition)
            .expect("string write");
        assert!(write.is_hard_delete_on_null);
        assert!(write.index_config.is_none() && write.sync_config.is_none());
        let deletion = options.attributes[1]
            .encode_attribute_write(&secrets_definition)
            .expect("null write");
        assert!(deletion.is_hard_delete_on_null);
        assert_eq!(deletion.key, "secrets/tenant-1");
        for (result, expected) in [
            (
                options.attributes[2].encode_attribute_write(&numeric_definition),
                "numeric and Boolean values are rejected",
            ),
            (
                options.attributes[3].encode_attribute_write(&secret_definition),
                "does not match its registered definition",
            ),
            (
                options.attributes[0].encode_attribute_write(&PersistenceDefinition::new(
                    "secret",
                    PersistenceKind::Attribute,
                )),
                "does not match its registered definition",
            ),
        ] {
            let Err(SdkError::InvalidArgument { message }) = result else {
                panic!("expected InvalidArgument");
            };
            assert!(message.contains(expected), "{message}");
        }
    }
}
