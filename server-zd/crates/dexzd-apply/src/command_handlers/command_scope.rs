// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_keyspace::{RunId, TenantId};
use dexzd_protocol::dex::FlowHistoryEvent;
use dexzd_protocol::dexzd::v1::{CommandReply, FlowCommand};

use crate::{ApplyEffect, ApplyOutcome, BehaviorVersion, StateWrite};

/// Read-only facts of the command being applied.
pub(crate) struct CommandScope<'command> {
    pub(crate) command: &'command FlowCommand,
    pub(crate) behavior_version: BehaviorVersion,
    pub(crate) tenant_id: TenantId,
    /// The only time apply may use.
    pub(crate) now_micros: i64,
}

impl<'command> CommandScope<'command> {
    pub(crate) fn new(command: &'command FlowCommand, behavior_version: BehaviorVersion) -> Self {
        Self {
            command,
            behavior_version,
            tenant_id: TenantId(command.tenant_id),
            now_micros: command.proposed_at_micros,
        }
    }

    pub(crate) fn flow_id(&self) -> &str {
        &self.command.flow_id
    }
}

/// What a handler produced besides state writes.
pub(crate) struct HandlerOutput {
    pub(crate) reply: CommandReply,
    pub(crate) effects: Vec<ApplyEffect>,
    /// History events to record for this command, without event IDs yet.
    pub(crate) events: Vec<FlowHistoryEvent>,
    /// The run the command applied to or created.
    pub(crate) run_id: Option<RunId>,
    /// Semantic commands are time travel targets (plan section 8.4).
    pub(crate) is_semantic: bool,
    /// Commands carrying external input that time travel may reapply.
    pub(crate) is_external_input: bool,
}

impl HandlerOutput {
    /// A rejection that changes nothing.
    pub(crate) fn rejected(reply: CommandReply) -> Self {
        Self {
            reply,
            effects: Vec::new(),
            events: Vec::new(),
            run_id: None,
            is_semantic: false,
            is_external_input: false,
        }
    }

    pub(crate) fn into_outcome(self, writes: Vec<StateWrite>) -> ApplyOutcome {
        ApplyOutcome {
            writes,
            effects: self.effects,
            reply: self.reply,
            run_id: self.run_id,
        }
    }
}
