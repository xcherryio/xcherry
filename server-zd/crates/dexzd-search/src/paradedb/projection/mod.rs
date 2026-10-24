// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

mod changes;
mod projector;

pub use changes::{SearchDocumentChange, extract_search_document_changes};
pub use projector::{ParadeDbProjector, ProjectionError};

#[cfg(test)]
mod integration;

#[cfg(test)]
pub(super) use integration::run_projector_scenarios_for_test;
