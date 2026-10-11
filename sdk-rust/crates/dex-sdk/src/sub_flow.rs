// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::any::TypeId;
use std::time::Duration;

use crate::start_flow_options::InitialAttribute;
use crate::wait::SubFlowDefinition;
use crate::{
    Attribute, AttributeMap, Condition, Context, Flow, FlowConfig, FlowResult,
    FlowTimeoutHandlerOptions, FlowTimeoutPolicy, HandlerResult, RetryPolicy, SdkResult, Value,
    value_mapper,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Controls how a generated SubFlow Flow ID resolves an existing execution.
pub enum SubFlowReusePolicy {
    /// Attaches to a running execution or returns its existing terminal result.
    Attach,
    /// Restarts abnormal executions, attaches while running, and returns completed results.
    RestartIfPreviousExitsAbnormally,
    /// Replaces any different existing execution, including a running one.
    AlwaysRestart,
}

#[derive(Clone)]
/// Configures one durable SubFlow Condition.
///
/// The SubFlow inherits its parent's effective Flow configuration. Dex generates its Flow ID and
/// request ID. Builder methods preserve normal Flow start defaults when omitted.
pub struct SubFlowOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) timeout_policy: FlowTimeoutPolicy,
    pub(crate) timeout_handler_options: Option<FlowTimeoutHandlerOptions>,
    pub(crate) start_delay: Option<Duration>,
    pub(crate) retry_policy: Option<RetryPolicy>,
    pub(crate) config_override: Option<FlowConfig>,
    pub(crate) attributes: Vec<InitialAttribute>,
    pub(crate) reuse_policy: SubFlowReusePolicy,
    pub(crate) condition_id: Option<String>,
}

impl SubFlowOptions {
    /// Creates options using the abnormal-restart reuse policy and normal start defaults.
    pub fn new() -> Self {
        Self {
            timeout: None,
            timeout_policy: FlowTimeoutPolicy::Default,
            timeout_handler_options: None,
            start_delay: None,
            retry_policy: None,
            config_override: None,
            attributes: Vec::new(),
            reuse_policy: SubFlowReusePolicy::RestartIfPreviousExitsAbnormally,
            condition_id: None,
        }
    }

    /// Sets the maximum total SubFlow execution duration.
    pub fn timeout(mut self, value: Duration) -> Self {
        self.timeout = Some(value);
        self
    }

    /// Selects what Dex does when the positive soft SubFlow timeout expires.
    pub fn timeout_policy(mut self, value: FlowTimeoutPolicy) -> Self {
        self.timeout_policy = value;
        self
    }

    /// Configures execution and selective state loading for the target Flow's timeout handler.
    pub fn timeout_handler_options(mut self, value: FlowTimeoutHandlerOptions) -> Self {
        self.timeout_handler_options = Some(value);
        self
    }

    /// Delays the SubFlow starting Step after server acceptance.
    pub fn start_delay(mut self, value: Duration) -> Self {
        self.start_delay = Some(value);
        self
    }

    /// Sets whole-Flow retry behavior after abnormal completion.
    pub fn retry_policy(mut self, value: RetryPolicy) -> Self {
        self.retry_policy = Some(value);
        self
    }

    /// Applies fields over the inherited parent Flow configuration.
    pub fn config_override(mut self, value: FlowConfig) -> Self {
        self.config_override = Some(value);
        self
    }

    /// Selects existing-execution resolution behavior.
    pub fn reuse_policy(mut self, value: SubFlowReusePolicy) -> Self {
        self.reuse_policy = value;
        self
    }

    /// Assigns the stable ID required by [`crate::Wait::any_combination_of`].
    pub fn condition_id(mut self, value: impl Into<String>) -> Self {
        self.condition_id = Some(value.into());
        self
    }

    /// Adds one initial singleton Attribute owned by the target SubFlow.
    pub fn initial_attribute<T: Value>(mut self, attribute: &Attribute<T>, value: T) -> Self {
        self.attributes
            .push(InitialAttribute::for_attribute(attribute, value));
        self
    }

    /// Adds one initial Attribute-map value for an instance.
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
}

impl Default for SubFlowOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Creates durable SubFlow Conditions and reads their Execute results.
pub struct SubFlow;

impl SubFlow {
    /// Creates a SubFlow Condition with default options.
    ///
    /// The server starts the SubFlow after `wait_for` returns and retries the start within its
    /// configured retry budget. When the start still fails, the waiting Step's
    /// [`crate::WaitForFailurePolicy`] decides the outcome. The default
    /// [`crate::WaitForFailurePolicy::FailFlow`] fails the parent Flow.
    /// [`crate::WaitForFailurePolicy::Proceed`] runs `execute`, and the SubFlow result reports
    /// [`crate::FlowErrorType::SubFlowStartFailed`].
    ///
    /// # Errors
    ///
    /// Returns a value-mapping error when `input` cannot be encoded.
    pub fn run<SomeFlow: Flow>(
        flow: &SomeFlow,
        input: SomeFlow::StartInput,
    ) -> SdkResult<Condition> {
        Self::run_with_options(flow, input, SubFlowOptions::new())
    }

    /// Creates a SubFlow Condition with explicit options.
    ///
    /// # Errors
    ///
    /// Returns a value-mapping error when `input` cannot be encoded. Worker mapping later validates
    /// the exact registered Rust Flow type, starting Step, Attributes, and option durations.
    pub fn run_with_options<SomeFlow: Flow>(
        flow: &SomeFlow,
        input: SomeFlow::StartInput,
        options: SubFlowOptions,
    ) -> SdkResult<Condition> {
        Ok(Condition::sub_flow(SubFlowDefinition {
            flow_type: flow.flow_type(),
            type_id: TypeId::of::<SomeFlow>(),
            input: value_mapper::encode(&input)?,
            options,
        }))
    }

    /// Returns the first SubFlow result during Step `execute`.
    pub fn condition_result(context: &Context) -> HandlerResult<FlowResult> {
        Self::condition_result_at(context, 0)
    }

    /// Returns one stable-indexed SubFlow result during Step `execute`.
    ///
    /// A SubFlow that the server could not start returns [`crate::FlowStatus::Failed`] with
    /// [`crate::FlowErrorType::SubFlowStartFailed`] and the start error as its error message.
    /// `execute` sees this result only when the Step uses [`crate::WaitForFailurePolicy::Proceed`].
    /// Check the error type to tell a start failure apart from a SubFlow that started and then
    /// failed.
    ///
    /// ```ignore
    /// let charge = SubFlow::condition_result_at(context, 0)?;
    /// if charge.error_type() == Some(FlowErrorType::SubFlowStartFailed) {
    ///     let detail = charge.error_message().unwrap_or_default();
    ///     return Ok(StepDecision::force_fail(format!("SubFlow did not start: {detail}")));
    /// }
    /// ```
    pub fn condition_result_at(context: &Context, index: usize) -> HandlerResult<FlowResult> {
        context.sub_flow_result(index)
    }

    /// Returns the generated Flow ID for the first SubFlow Condition during Step `execute`.
    pub fn flow_id(context: &Context) -> HandlerResult<String> {
        Self::flow_id_at(context, 0)
    }

    /// Returns the generated Flow ID for one stable-indexed SubFlow Condition during `execute`.
    pub fn flow_id_at(context: &Context, index: usize) -> HandlerResult<String> {
        context.sub_flow_id(index)
    }
}
