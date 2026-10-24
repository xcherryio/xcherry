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

use std::error::Error;
use std::fmt;
use std::time::Duration;

use dex_sdk::PullWorkerOptions;

/// Environment variable that selects how example Workers receive tasks.
pub const WORKER_MODE_ENVIRONMENT_VARIABLE: &str = "DEX_EXAMPLES_WORKER_MODE";

/// Upper bound for a pull Worker to register every queue with Dex.
pub const PULL_REGISTRATION_TIMEOUT: Duration = Duration::from_secs(30);

/// How an example Worker receives tasks from Dex.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerMode {
    /// Listens on a Worker address that Dex calls.
    Push,
    /// Opens outbound task streams to Dex.
    Pull,
}

impl WorkerMode {
    /// Parses a `DEX_EXAMPLES_WORKER_MODE` value. Unset or empty selects push.
    pub fn parse(value: Option<&str>) -> Result<Self, UnknownWorkerModeError> {
        match value.unwrap_or_default() {
            "" | "push" => Ok(Self::Push),
            "pull" => Ok(Self::Pull),
            unknown => Err(UnknownWorkerModeError {
                value: unknown.to_string(),
            }),
        }
    }
}

/// Pull options sized for every sample Flow sharing one Worker process.
pub fn example_pull_worker_options() -> PullWorkerOptions {
    PullWorkerOptions::new().max_concurrent_tasks(64)
}

/// A `DEX_EXAMPLES_WORKER_MODE` value other than push or pull.
pub struct UnknownWorkerModeError {
    value: String,
}

impl fmt::Display for UnknownWorkerModeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{WORKER_MODE_ENVIRONMENT_VARIABLE} must be push or pull, got {:?}",
            self.value
        )
    }
}

// Debug repeats Display so `main` reports the actionable message.
impl fmt::Debug for UnknownWorkerModeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl Error for UnknownWorkerModeError {}
