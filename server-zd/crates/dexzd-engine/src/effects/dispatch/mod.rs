// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Method dispatch: starting pending invocations when their locks are free, and running each
//! started attempt on a Worker.

mod attempt_failure;
mod dispatch_scheduler;
mod method_attempt;
mod method_dispatcher;
mod worker_result;

pub(crate) use dispatch_scheduler::{DispatchScheduler, InvocationStartResult};
pub use method_dispatcher::{
    MethodDispatchRequest, MethodDispatchSession, MethodDispatchStartError, MethodDispatcher,
};
