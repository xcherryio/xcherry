// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Validate untrusted Dex client requests and Worker responses before state changes.

mod client;
mod shared;
mod waiting;
mod worker;

pub use client::{ClientRequest, validate_client_request};
pub use shared::{ValidationCategory, ValidationConfig, ValidationError};
pub use waiting::validate_waiting_condition;
pub use worker::{WorkerResponse, validate_worker_response};
