// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Durable cross-Flow delivery, child Flow reuse, and run deletion.

mod inbox;
mod outbox;
mod run_purge;

pub(crate) use inbox::apply_inbox_message;
#[expect(unused_imports, reason = "A3 calls this after the wait engine lands.")]
pub(crate) use outbox::{
    apply_outbox_ack, on_outbox_retention_fired, report_child_closed, start_child_flows,
};
pub(crate) use run_purge::{apply_hard_delete_objects_deleted, apply_run_purge};
