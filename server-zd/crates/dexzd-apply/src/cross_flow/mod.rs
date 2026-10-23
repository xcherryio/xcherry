// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Cross-Flow messages through the outbox and inbox (plan section 9.5): child Flow starts and
//! completion reports. Implemented by task A6; the backbone only defines the entry points.

use crate::ApplyError;
use crate::command_handlers::CommandScope;
use crate::command_handlers::run_cursor::RunCursor;
use crate::state_access::FlowTransaction;

/// Writes a completion report to the parent's inbox through the outbox when this run is a
/// child Flow.
pub(crate) fn report_child_closed(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    scope: &CommandScope<'_>,
) -> Result<(), ApplyError> {
    let _ = (transaction, cursor, scope);
    Ok(())
}

/// An acknowledged outbox message's retention passed; delete it.
pub(crate) fn on_outbox_retention_fired(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    timer: &dexzd_protocol::dexzd::v1::OutboxRetentionTimerId,
) -> Result<(), ApplyError> {
    let _ = (transaction, cursor, timer);
    Ok(())
}
