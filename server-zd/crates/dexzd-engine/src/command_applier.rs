// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Calls apply through a trait so tests can replace the deterministic state machine.

use dexzd_apply::{ApplyContext, ApplyError, ApplyOutcome, FlowStateReader};
use dexzd_protocol::dexzd::v1::FlowCommand;

/// Applies one stamped command to a state view. Production uses
/// `dexzd_apply::apply_flow_command`; tests inject fakes.
pub trait CommandApplier: Send + Sync + 'static {
    /// Applies `command` to `state` and returns its writes, effects, and reply.
    ///
    /// Must return byte-identical outcomes for the same state and command. An `Err` stops the
    /// command's partition.
    fn apply(
        &self,
        context: &ApplyContext,
        state: &dyn FlowStateReader,
        command: &FlowCommand,
    ) -> Result<ApplyOutcome, ApplyError>;
}

/// [`CommandApplier`] backed by the deterministic `dexzd-apply` state machine.
#[derive(Debug, Default, Clone, Copy)]
pub struct DeterministicCommandApplier;

impl CommandApplier for DeterministicCommandApplier {
    fn apply(
        &self,
        context: &ApplyContext,
        state: &dyn FlowStateReader,
        command: &FlowCommand,
    ) -> Result<ApplyOutcome, ApplyError> {
        dexzd_apply::apply_flow_command(context, state, command)
    }
}
