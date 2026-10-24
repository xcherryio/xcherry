// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! History H, attempt checkpoints, the time travel window, and the TimeTravel command (plan
//! sections 7 and 8.4).

mod attempt_scope;
mod checkpoints;
pub mod diagnostics;
mod lineage;
mod replay_state;
mod reset_target;
mod state_rebuild;
mod time_travel;
mod window;

pub(crate) use checkpoints::{after_command, pending_step_methods};
pub(crate) use time_travel::apply_time_travel;
pub(crate) use window::{WindowAdvanceTarget, advance_window, prune_window_history};
