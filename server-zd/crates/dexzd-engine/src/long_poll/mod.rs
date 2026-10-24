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

pub(crate) use waiter_registry::{WaitInterest, WaiterRegistry};
