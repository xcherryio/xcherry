// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! S3-compatible HTTP server that serves an s3mock store over path-style URLs.

mod aws_chunked;
mod config;
mod controls;
mod handlers;
mod request_headers;
mod routing;
mod s3_error;
mod server;
mod signature;
mod xml;

pub use config::{DurabilityFlag, EtagModeFlag, ServerConfig, SignatureVerification};
pub use server::{ServerHandle, start, start_with_config};
