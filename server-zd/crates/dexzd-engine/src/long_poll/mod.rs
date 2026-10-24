// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Long-poll waiters woken only by committed outcomes (plan 9.7).

mod long_poll_operations;
mod waiter_registry;

pub use long_poll_operations::{
    ATTRIBUTE_REQUEST_TIMEOUT_MESSAGE, STEP_COMPLETION_REQUEST_TIMEOUT_MESSAGE,
    WAIT_CALL_DEADLINE_MESSAGE, WAIT_FOR_FLOW_TIMEOUT_MESSAGE,
    WAIT_FOR_HISTORY_EVENT_TIMEOUT_MESSAGE,
};
pub(crate) use waiter_registry::{WaitInterest, WaiterRegistry};
