// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use uuid::Uuid;

/// Creates the IDs the engine persists; tests may inject a seeded source for byte-identical runs.
pub trait EngineIdSource: Send + Sync + 'static {
    /// Returns a new UUIDv7 that names a stamped command.
    fn command_id(&self) -> Uuid;

    /// Returns the process ID that an EpochStart batch records for diagnostics.
    fn writer_process_id(&self) -> String;

    /// Returns a new UUIDv7 that names a local store lineage.
    fn lineage_id(&self) -> Uuid;
}

/// [`EngineIdSource`] backed by the system clock and random numbers.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemEngineIdSource;

impl EngineIdSource for SystemEngineIdSource {
    fn command_id(&self) -> Uuid {
        Uuid::now_v7()
    }

    fn writer_process_id(&self) -> String {
        Uuid::now_v7().to_string()
    }

    fn lineage_id(&self) -> Uuid {
        Uuid::now_v7()
    }
}
