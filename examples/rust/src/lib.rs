// Copyright (c) 2022-2026 Super Durable, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod patterns;
pub mod primitives;
pub mod production;
pub mod products;
pub mod server;
pub mod shared;

use std::sync::Arc;

use dex_sdk::{Client, Registry, SdkResult};

use crate::patterns::polling::FakeJobService;

pub const PRODUCT_FLOW_TYPES: [&str; 8] = [
    "MoneyTransferFlow",
    "OrderProcessingFlow",
    "OrchestrationFlow",
    "DealDSLFlow",
    "EngagementFlow",
    "SubscriptionFlow",
    "UserOnboardingFlow",
    "JobPostingFlow",
];

pub const PATTERN_FLOW_TYPES: [&str; 23] = [
    "CronScheduleFlow",
    "DrainInternalChannelFlow",
    "DrainingExternalChannelFlow",
    "InterruptibleFlow",
    "ManualRecoveryFlow",
    "StaticParallelStepsFlow",
    "DynamicParallelStepsFlow",
    "AwaitParallelStepsFlow",
    "FirstWinParallelStepsFlow",
    "ExampleSubFlow",
    "BasicParentFlow",
    "AdvancedLongLiveParentFlow",
    "AdvancedShortLiveParentFlow",
    "SubmitRequestFlow",
    "PollingFlow",
    "FailureRecoveryFlow",
    "ReminderFlow",
    "InactivenessTrackerFlow",
    "UserProfileFlow",
    "ChunkedSubscriberFlow",
    "CustomerDirectoryFlow",
    "FlowGracefulTimeout",
    "WaitForStepCompletionFlow",
];

pub fn create_example_registry(polling_jobs: Arc<FakeJobService>) -> SdkResult<Registry> {
    products::register(Registry::new())
        .and_then(|registry| patterns::register(registry, polling_jobs))
        .and_then(primitives::register)
}

pub fn create_worker_registry(
    client: Arc<Client>,
    polling_jobs: Arc<FakeJobService>,
) -> SdkResult<Registry> {
    products::register(Registry::new())
        .and_then(|registry| patterns::register_worker(registry, client, polling_jobs))
        .and_then(primitives::register)
}
