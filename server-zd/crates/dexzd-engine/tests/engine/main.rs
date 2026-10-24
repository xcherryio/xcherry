// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Engine tests with paused time, in-memory object storage, and a counting applier.

mod commit_path;
mod harness;
mod lease_lifecycle;
mod node_identity;
mod recovery;
mod snapshot_fencing;
mod snapshots;
mod standby_takeover;
