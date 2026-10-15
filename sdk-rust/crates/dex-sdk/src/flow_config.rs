// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use crate::{StepDurability, WorkerTarget};

#[derive(Clone, Debug, Default)]
/// Overrides runtime behavior for one Flow.
///
/// Omitted fields use Dex server defaults. Pass a value to [`crate::StartFlowOptions::config_override`]
/// for one execution or to [`crate::Client::update_flow_config`] for an active Flow.
pub struct FlowConfig {
    pub(crate) active_step_search_mode: Option<ActiveStepSearchMode>,
    pub(crate) attribute_store_names: Option<Vec<String>>,
    pub(crate) continue_as_new_threshold: Option<u32>,
    pub(crate) continue_as_new_page_size_bytes: Option<u32>,
    pub(crate) step_durability: Option<StepDurability>,
    pub(crate) worker_target: Option<WorkerTarget>,
}

impl FlowConfig {
    /// Creates a config with every field delegated to server defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Controls which active Step types Dex places in the search index.
    pub fn active_step_search_mode(mut self, value: ActiveStepSearchMode) -> Self {
        self.active_step_search_mode = Some(value);
        self
    }

    /// Selects server-configured Attribute Stores for opted-in Attribute writes.
    ///
    /// Omitting this builder preserves the existing Flow setting. Passing an empty vector retains
    /// protocol presence and disables later synchronization. Each enabled Attribute write is
    /// projected to every selected store; an external store failure does not roll back the Flow
    /// Attribute write.
    #[must_use]
    pub fn attribute_store_names(mut self, values: Vec<String>) -> Self {
        self.attribute_store_names = Some(values);
        self
    }

    /// Sets the event-count threshold that triggers Continue-As-New.
    pub fn continue_as_new_threshold(mut self, value: u32) -> Self {
        self.continue_as_new_threshold = Some(value);
        self
    }

    /// Sets the maximum history page size, in bytes, carried into Continue-As-New.
    pub fn continue_as_new_page_size_bytes(mut self, value: u32) -> Self {
        self.continue_as_new_page_size_bytes = Some(value);
        self
    }

    /// Sets the default persistence durability for Step handler writes.
    pub fn step_durability(mut self, value: StepDurability) -> Self {
        self.step_durability = Some(value);
        self
    }

    /// Routes future Step and RPC invocations to this Worker target.
    ///
    /// Use [`WorkerTarget::pull`] for a pull Worker. Invalid combinations return
    /// [`crate::SdkError::FlowDefinition`] when the configuration is sent.
    pub fn worker_target(mut self, value: WorkerTarget) -> Self {
        self.worker_target = Some(value);
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Controls which active Step types Dex indexes for Flow search.
pub enum ActiveStepSearchMode {
    /// Indexes every active Step type, including execute-only Steps.
    All,
    /// Indexes a Step type only after its `wait_for` handler runs.
    WithWaitFor,
    /// Disables active-Step indexing for the Flow.
    Disabled,
}
