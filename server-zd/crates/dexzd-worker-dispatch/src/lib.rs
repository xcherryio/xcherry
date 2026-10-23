// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Pull Worker protocol server for server-zd: Worker registration, task queues, matching, cancellation, and keepalive.

mod matching_config;
mod worker_registry;

pub use matching_config::MatchingConfig;
pub use worker_registry::{
    DispatchEvent, DispatchHandle, DispatchOutcome, DispatchRequest, DispatchTaskPayload,
    WorkerDispatch, WorkerTaskService,
};
