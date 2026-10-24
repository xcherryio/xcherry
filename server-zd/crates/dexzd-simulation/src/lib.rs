// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Deterministic simulation tests that run the server-zd engine against simulated object storage, clocks, Workers, and crashes.
//!
//! This first stage drives `dexzd-apply` alone with a seeded model of clients, the owner, and Workers.
//!
//! [`ApplyChecker`] checks determinism and the plan's state invariants after every command.
//!
//! [`run_engine_seed`] runs the second stage: the engine itself on simulated storage and crashes.

mod apply_checker;
mod apply_simulation;
mod engine_simulation;
mod invalid_client_inputs;
mod invariants;
mod memory_state;
mod model;
mod row_decoding;
mod state_view;

pub use apply_checker::ApplyChecker;
pub use apply_simulation::{
    SimulationFailure, SimulationSummary, command_kind, replay_commands, run_seed,
};
pub use engine_simulation::{
    EngineSimulationConfig, EngineSimulationFailure, EngineSimulationFinalState,
    EngineSimulationSummary, run_engine_seed, run_engine_seed_with_final_state,
};
pub use invalid_client_inputs::{
    INVALID_CLIENT_INPUT_CLASSES, OVERSIZED_ATTRIBUTE_VALUE, OVERSIZED_CHANNEL_MESSAGE,
    check_invalid_client_input_rejected, invalid_client_input,
};
pub use invariants::{
    CheckerMemory, GUARDED_ATTRIBUTE_KEYS, InvariantViolation, check_flow, check_fresh_snapshot,
};
pub use memory_state::MemoryFlowState;
pub use model::{CommandGenerator, FLOW_IDS};
pub use row_decoding::{RowDecodeError, reencode_row};
pub use state_view::{FlowView, RunSelection, RunView, StateViewError};
