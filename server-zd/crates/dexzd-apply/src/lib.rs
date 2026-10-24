// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Deterministic state machine that applies one server-zd command to Flow state and returns its writes, effects, and reply.
//!
//! [`apply_flow_command`] is a pure function of (behavior version, state, command). It never
//! reads clocks, random numbers, configuration, or the network, and it iterates only ordered
//! collections. The same call on the same state always returns byte-identical writes, so a
//! node can replay the command log and the speculative overlay can be checked against the
//! committed result.

mod apply_context;
mod apply_outcome;
mod command_handlers;
mod cross_flow;
mod history;
mod projections;
pub mod read_model;
mod state_access;
mod steps;
pub mod values;

pub use apply_context::{ApplyContext, BehaviorVersion};
pub use apply_outcome::{ApplyEffect, ApplyError, ApplyOutcome, StateWrite, WaitableChange};
pub use history::diagnostics;
pub use state_access::{FlowStateReader, StateReadError, StateRow};

use dexzd_protocol::dexzd::v1::FlowCommand;

/// Applies `command` to the state of its Flow and returns the writes to commit atomically, the
/// effects for the runtime, and the reply for the proposer.
///
/// `state` must expose the committed (or speculative) state of the command's Flow. Rejections
/// that clients or Workers caused are not errors: they come back as a [`CommandReply`] error
/// with no state writes (except deduplication records where documented). An `Err` means the
/// stored state is corrupt or a trusted invariant is broken; the partition must stop.
pub fn apply_flow_command(
    context: &ApplyContext,
    state: &dyn FlowStateReader,
    command: &FlowCommand,
) -> Result<ApplyOutcome, ApplyError> {
    let behavior_version = BehaviorVersion::from_command(command.behavior_version)?;
    command_handlers::apply_command(context, state, command, behavior_version)
}
