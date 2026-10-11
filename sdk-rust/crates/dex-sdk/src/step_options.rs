// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::marker::PhantomData;
use std::time::Duration;

use crate::attribute::AttributeLock;
use crate::attribute::AttributeMapLoad;
use crate::channel::{ChannelLoad, ChannelMapLoad};
use crate::{AttributeMap, Channel, ChannelMap, RetryPolicy, Step, Value};

/// Configures one Step's handler execution and persistence behavior.
///
/// New options preserve server timeout, retry, and durability defaults. `wait_for` failures fail the
/// Flow by default. Attribute locks apply only to the matching handler call. The type system checks
/// that an execute-failure recovery Step accepts the same input type.
///
/// # Examples
///
/// ```
/// use dex_sdk::{Attribute, RetryPolicy, StepOptions, WaitForFailurePolicy};
/// use std::time::Duration;
///
/// let balance = Attribute::<i64>::new("balance");
/// let options = StepOptions::<String>::new()
///     .execute_method_timeout(Duration::from_secs(30))
///     .execute_retry(RetryPolicy::new().maximum_attempts(3))
///     .wait_for_failure(WaitForFailurePolicy::Proceed)
///     .execute_lock(balance.lock());
/// ```
pub struct StepOptions<Input> {
    pub(crate) wait_for_method_timeout: Option<Duration>,
    pub(crate) execute_method_timeout: Option<Duration>,
    pub(crate) heartbeat_timeout: Option<Duration>,
    pub(crate) wait_for_retry: Option<RetryPolicy>,
    pub(crate) execute_retry: Option<RetryPolicy>,
    pub(crate) wait_for_failure: WaitForFailurePolicy,
    pub(crate) wait_for_durability: StepDurability,
    pub(crate) execute_durability: StepDurability,
    pub(crate) wait_for_locks: Vec<AttributeLock>,
    pub(crate) execute_locks: Vec<AttributeLock>,
    pub(crate) wait_for_load_attribute_maps: Vec<AttributeMapLoad>,
    pub(crate) wait_for_load_channels: Vec<ChannelLoad>,
    pub(crate) wait_for_load_channel_maps: Vec<ChannelMapLoad>,
    pub(crate) execute_load_attribute_maps: Vec<AttributeMapLoad>,
    pub(crate) execute_load_channels: Vec<ChannelLoad>,
    pub(crate) execute_load_channel_maps: Vec<ChannelMapLoad>,
    pub(crate) execute_failure_step: Option<&'static str>,
    marker: PhantomData<fn(Input)>,
}

#[derive(Clone)]
pub(crate) struct ErasedStepOptions {
    pub(crate) wait_for_method_timeout: Option<Duration>,
    pub(crate) execute_method_timeout: Option<Duration>,
    pub(crate) heartbeat_timeout: Option<Duration>,
    pub(crate) wait_for_retry: Option<RetryPolicy>,
    pub(crate) execute_retry: Option<RetryPolicy>,
    pub(crate) wait_for_failure: WaitForFailurePolicy,
    pub(crate) wait_for_durability: StepDurability,
    pub(crate) execute_durability: StepDurability,
    pub(crate) wait_for_locks: Vec<AttributeLock>,
    pub(crate) execute_locks: Vec<AttributeLock>,
    pub(crate) wait_for_load_attribute_maps: Vec<AttributeMapLoad>,
    pub(crate) wait_for_load_channels: Vec<ChannelLoad>,
    pub(crate) wait_for_load_channel_maps: Vec<ChannelMapLoad>,
    pub(crate) execute_load_attribute_maps: Vec<AttributeMapLoad>,
    pub(crate) execute_load_channels: Vec<ChannelLoad>,
    pub(crate) execute_load_channel_maps: Vec<ChannelMapLoad>,
    pub(crate) execute_failure_step: Option<&'static str>,
}

impl<Input> From<StepOptions<Input>> for ErasedStepOptions {
    fn from(options: StepOptions<Input>) -> Self {
        Self {
            wait_for_method_timeout: options.wait_for_method_timeout,
            execute_method_timeout: options.execute_method_timeout,
            heartbeat_timeout: options.heartbeat_timeout,
            wait_for_retry: options.wait_for_retry,
            execute_retry: options.execute_retry,
            wait_for_failure: options.wait_for_failure,
            wait_for_durability: options.wait_for_durability,
            execute_durability: options.execute_durability,
            wait_for_locks: options.wait_for_locks,
            execute_locks: options.execute_locks,
            wait_for_load_attribute_maps: options.wait_for_load_attribute_maps,
            wait_for_load_channels: options.wait_for_load_channels,
            wait_for_load_channel_maps: options.wait_for_load_channel_maps,
            execute_load_attribute_maps: options.execute_load_attribute_maps,
            execute_load_channels: options.execute_load_channels,
            execute_load_channel_maps: options.execute_load_channel_maps,
            execute_failure_step: options.execute_failure_step,
        }
    }
}

impl<Input: Value> StepOptions<Input> {
    /// Creates options that preserve server defaults and fail on exhausted `wait_for` retries.
    pub fn new() -> Self {
        Self {
            wait_for_method_timeout: None,
            execute_method_timeout: None,
            heartbeat_timeout: None,
            wait_for_retry: None,
            execute_retry: None,
            wait_for_failure: WaitForFailurePolicy::FailFlow,
            wait_for_durability: StepDurability::Default,
            execute_durability: StepDurability::Default,
            wait_for_locks: Vec::new(),
            execute_locks: Vec::new(),
            wait_for_load_attribute_maps: Vec::new(),
            wait_for_load_channels: Vec::new(),
            wait_for_load_channel_maps: Vec::new(),
            execute_load_attribute_maps: Vec::new(),
            execute_load_channels: Vec::new(),
            execute_load_channel_maps: Vec::new(),
            execute_failure_step: None,
            marker: PhantomData,
        }
    }

    /// Sets the maximum duration of one `wait_for` handler attempt.
    pub fn wait_for_method_timeout(mut self, value: Duration) -> Self {
        self.wait_for_method_timeout = Some(value);
        self
    }

    /// Sets the maximum duration of one `execute` handler attempt.
    pub fn execute_method_timeout(mut self, value: Duration) -> Self {
        self.execute_method_timeout = Some(value);
        self
    }

    /// Sets the heartbeat timeout for regular `wait_for` and `execute` activities.
    ///
    /// Zero selects the server default of one minute. Positive values must be whole seconds and at
    /// least the server-configured minimum, which defaults to ten seconds. Local activities ignore
    /// this option; an asynchronous fallback to a regular activity uses it.
    pub fn heartbeat_timeout(mut self, value: Duration) -> Self {
        self.heartbeat_timeout = Some(value);
        self
    }

    /// Sets the retry policy for failed `wait_for` attempts.
    pub fn wait_for_retry(mut self, value: RetryPolicy) -> Self {
        self.wait_for_retry = Some(value);
        self
    }

    /// Sets the retry policy for failed `execute` attempts.
    pub fn execute_retry(mut self, value: RetryPolicy) -> Self {
        self.execute_retry = Some(value);
        self
    }

    /// Selects whether exhausted `wait_for` retries fail or advance the Flow.
    pub fn wait_for_failure(mut self, value: WaitForFailurePolicy) -> Self {
        self.wait_for_failure = value;
        self
    }

    /// Routes exhausted `execute` failures to a recovery Step with the same input type.
    pub fn on_execute_failure_proceed_to<RecoveryStep>(mut self, step: &RecoveryStep) -> Self
    where
        RecoveryStep: Step<Input = Input>,
    {
        self.execute_failure_step = Some(step.step_type());
        self
    }

    /// Overrides persistence durability for writes staged by `wait_for`.
    pub fn wait_for_durability(mut self, value: StepDurability) -> Self {
        self.wait_for_durability = value;
        self
    }

    /// Overrides persistence durability for writes staged by `execute`.
    pub fn execute_durability(mut self, value: StepDurability) -> Self {
        self.execute_durability = value;
        self
    }

    /// Adds an Attribute lock held for the `wait_for` invocation.
    pub fn wait_for_lock(mut self, value: AttributeLock) -> Self {
        self.wait_for_locks.push(value);
        self
    }

    /// Adds an Attribute lock held for the `execute` invocation.
    pub fn execute_lock(mut self, value: AttributeLock) -> Self {
        self.execute_locks.push(value);
        self
    }

    /// Loads every current instance of `attribute_map` for `wait_for`.
    pub fn wait_for_load_attribute_map<T>(mut self, attribute_map: &AttributeMap<T>) -> Self {
        self.wait_for_load_attribute_maps.push(AttributeMapLoad {
            name: attribute_map.name().to_owned(),
            instance: None,
        });
        self
    }

    /// Loads one AttributeMap instance for `wait_for`.
    pub fn wait_for_load_attribute_map_instance(mut self, load: AttributeMapLoad) -> Self {
        self.wait_for_load_attribute_maps.push(load);
        self
    }

    /// Loads one Channel's pending messages for `wait_for`.
    pub fn wait_for_load_channel<T>(mut self, channel: &Channel<T>) -> Self {
        self.wait_for_load_channels.push(ChannelLoad {
            name: channel.name().to_owned(),
        });
        self
    }

    /// Loads every current ChannelMap instance's pending messages for `wait_for`.
    pub fn wait_for_load_channel_map<T>(mut self, channel_map: &ChannelMap<T>) -> Self {
        self.wait_for_load_channel_maps.push(ChannelMapLoad {
            name: channel_map.name().to_owned(),
            instance: None,
        });
        self
    }

    /// Loads one ChannelMap instance's pending messages for `wait_for`.
    pub fn wait_for_load_channel_map_instance(mut self, load: ChannelMapLoad) -> Self {
        self.wait_for_load_channel_maps.push(load);
        self
    }

    /// Loads every current instance of `attribute_map` for `execute`.
    pub fn execute_load_attribute_map<T>(mut self, attribute_map: &AttributeMap<T>) -> Self {
        self.execute_load_attribute_maps.push(AttributeMapLoad {
            name: attribute_map.name().to_owned(),
            instance: None,
        });
        self
    }

    /// Loads one AttributeMap instance for `execute`.
    pub fn execute_load_attribute_map_instance(mut self, load: AttributeMapLoad) -> Self {
        self.execute_load_attribute_maps.push(load);
        self
    }

    /// Loads one Channel's pending messages for `execute`.
    pub fn execute_load_channel<T>(mut self, channel: &Channel<T>) -> Self {
        self.execute_load_channels.push(ChannelLoad {
            name: channel.name().to_owned(),
        });
        self
    }

    /// Loads every current ChannelMap instance's pending messages for `execute`.
    pub fn execute_load_channel_map<T>(mut self, channel_map: &ChannelMap<T>) -> Self {
        self.execute_load_channel_maps.push(ChannelMapLoad {
            name: channel_map.name().to_owned(),
            instance: None,
        });
        self
    }

    /// Loads one ChannelMap instance's pending messages for `execute`.
    pub fn execute_load_channel_map_instance(mut self, load: ChannelMapLoad) -> Self {
        self.execute_load_channel_maps.push(load);
        self
    }
}

impl<Input: Value> Default for StepOptions<Input> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Selects the terminal behavior after `wait_for` exhausts its retry policy.
pub enum WaitForFailurePolicy {
    /// Fails the Flow.
    FailFlow,
    /// Skips the failed wait and invokes `execute` with failure visible in [`crate::Context`].
    ///
    /// This also covers a SubFlow Condition that the server cannot start within its retry budget.
    /// `execute` then runs immediately with [`crate::Context::wait_for_method_failed`] true. Each
    /// SubFlow that did not start reports [`crate::FlowStatus::Failed`] with
    /// [`crate::FlowErrorType::SubFlowStartFailed`]. SubFlows that did start keep running, and their
    /// results stay nonterminal.
    Proceed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Controls when staged Step persistence must be durably acknowledged.
pub enum StepDurability {
    /// Uses the Flow or server default.
    Default,
    /// Waits for durable persistence before completing the handler request.
    Sync,
    /// Allows asynchronous persistence after accepting the handler result.
    Async,
}
