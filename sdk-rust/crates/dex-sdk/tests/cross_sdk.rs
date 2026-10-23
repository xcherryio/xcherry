// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

#[expect(
    dead_code,
    reason = "Cross-SDK tests share support with integration scenarios that need additional helpers."
)]
#[path = "integ/support.rs"]
mod support;

#[path = "cross_sdk/channels.rs"]
mod channels;
#[path = "cross_sdk/handler_context.rs"]
mod handler_context;
#[path = "cross_sdk/operations.rs"]
mod operations;
#[path = "cross_sdk/persistence.rs"]
mod persistence;
#[path = "cross_sdk/rpc.rs"]
mod rpc;
#[path = "cross_sdk/state.rs"]
mod state;
