// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! SearchFlows backends for server-zd: a builtin evaluator over local search documents and a ParadeDB projection.

mod page_token;
mod search_backend;

pub use search_backend::{SearchBackend, SearchError, SearchPage, SearchRequest};

/// Builtin SearchFlows over committed local run documents.
pub mod builtin;

/// ParadeDB schema, projection rows, and visibility query translation.
pub mod paradedb;
