// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! A seeded model of clients, the owner, and pull Workers that proposes commands.
//!
//! It also proposes stale, duplicate, and conflicting commands that apply must reject.

mod client_model;
mod value_generation;
mod worker_model;

pub(crate) use value_generation::MAX_VALUE_BYTES;

use std::collections::BTreeSet;

use dexzd_keyspace::RunId;
use dexzd_protocol::dex::FlowStatus;
use dexzd_protocol::dexzd::v1::{FlowCommand, StepInvocationPhase, TimerId, flow_command::Command};
use rand::{RngExt, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::apply_checker::ApplyChecker;
use crate::invariants::InvariantViolation;
use crate::model::worker_model::Call;
use crate::state_view::{FlowView, RunSelection, RunView};

/// Flow IDs the simulated clients use.
pub const FLOW_IDS: &[&str] = &["order-1", "order-2"];
/// Proposal time of the first command: 2027-01-15 in microseconds.
const START_MICROS: i64 = 1_800_000_000_000_000;
/// Result commands remembered for redelivery.
const REMEMBERED_RESULTS: usize = 16;

/// Proposes the next command from the committed state and a seeded random source.
pub struct CommandGenerator {
    rng: ChaCha8Rng,
    now_micros: i64,
    sent_results: Vec<FlowCommand>,
}

/// One possible next command and its weight.
enum Proposal<'view> {
    StartFlow,
    ClientWrite {
        run_id: RunId,
        run: &'view RunView,
    },
    StartAttempt(Call<'view>),
    WorkerOutcome(Call<'view>),
    StaleAttempt(Call<'view>),
    FireTimer {
        run_id: RunId,
        timer: TimerId,
        fire_at_micros: i64,
    },
    RedeliverResult,
    LaterHandler {
        run_id: RunId,
        run: &'view RunView,
    },
    TravelToWindowPoint {
        run_id: RunId,
        timestamp_micros: i64,
    },
}

impl CommandGenerator {
    /// Creates a generator whose every choice derives from `seed`.
    pub fn new(seed: u64) -> Self {
        Self {
            rng: ChaCha8Rng::seed_from_u64(seed),
            now_micros: START_MICROS,
            sent_results: Vec::new(),
        }
    }

    /// Proposes the next command for the checker's committed state.
    ///
    /// Returns a violation when the committed state cannot be decoded.
    pub fn next_command(
        &mut self,
        checker: &ApplyChecker,
    ) -> Result<FlowCommand, InvariantViolation> {
        let flow_id = FLOW_IDS[self.rng.random_range(0..FLOW_IDS.len())];
        let directory_view = checker.view(flow_id, RunSelection::Only(&BTreeSet::new()))?;
        let run_ids: Vec<RunId> = directory_view
            .directory
            .iter()
            .flat_map(|directory| &directory.runs)
            .filter_map(|run| RunId::from_slice(&run.run_id).ok())
            .collect();
        let mut selected = BTreeSet::new();
        if let Some(latest) = run_ids.last() {
            selected.insert(*latest);
        }
        if run_ids.len() > 1 && self.rng.random_bool(0.3) {
            selected.insert(run_ids[self.rng.random_range(0..run_ids.len() - 1)]);
        }
        let view = checker.view(flow_id, RunSelection::Only(&selected))?;
        let proposals = self.proposals(&view, checker);
        let total: u32 = proposals.iter().map(|(weight, _)| weight).sum();
        let mut ticket = self.rng.random_range(0..total);
        let mut chosen = None;
        for (weight, proposal) in proposals {
            if ticket < weight {
                chosen = Some(proposal);
                break;
            }
            ticket -= weight;
        }
        let proposal = chosen.expect("the ticket is below the total weight");
        Ok(self.realize(flow_id, proposal))
    }

    fn proposals<'view>(
        &mut self,
        view: &'view FlowView,
        checker: &ApplyChecker,
    ) -> Vec<(u32, Proposal<'view>)> {
        let mut proposals = vec![(1, Proposal::StartFlow)];
        if !self.sent_results.is_empty() {
            proposals.push((2, Proposal::RedeliverResult));
        }
        let Some((latest_run_id, latest_run)) = view.latest_run() else {
            proposals[0].0 = 20;
            return proposals;
        };
        let is_running = latest_run
            .record
            .as_ref()
            .is_some_and(|record| record.status == FlowStatus::Running as i32);
        if !is_running {
            proposals[0].0 = 12;
        }
        for (run_id, run) in view.runs.iter().filter(|(_, run)| run.is_complete) {
            let is_latest = *run_id == latest_run_id;
            let client_weight = if is_latest { 8 } else { 1 };
            proposals.push((
                client_weight,
                Proposal::ClientWrite {
                    run_id: *run_id,
                    run,
                },
            ));
            if is_latest
                && let Some(timestamp_micros) = checker.earlier_window_value_point(*run_id, run)
            {
                proposals.push((
                    3,
                    Proposal::TravelToWindowPoint {
                        run_id: *run_id,
                        timestamp_micros,
                    },
                ));
            }
            if is_latest {
                proposals.push((
                    1,
                    Proposal::LaterHandler {
                        run_id: *run_id,
                        run,
                    },
                ));
            }
            for (_, timer) in &run.timers {
                let Some(timer_id) = timer.timer_id.clone() else {
                    continue;
                };
                proposals.push((
                    3,
                    Proposal::FireTimer {
                        run_id: *run_id,
                        timer: timer_id,
                        fire_at_micros: timer.fire_at_micros,
                    },
                ));
            }
            for step in run.step_executions.values() {
                let Some(invocation) = &step.invocation else {
                    continue;
                };
                let call = || Call {
                    run_id: *run_id,
                    run,
                    step,
                    invocation,
                };
                let phase = StepInvocationPhase::try_from(invocation.phase);
                match phase {
                    Ok(StepInvocationPhase::Pending) => {
                        let weight = if call().can_take_locks() { 10 } else { 1 };
                        proposals.push((weight, Proposal::StartAttempt(call())));
                    }
                    Ok(StepInvocationPhase::Running) => {
                        proposals.push((3, Proposal::WorkerOutcome(call())));
                        proposals.push((1, Proposal::StaleAttempt(call())));
                    }
                    _ => proposals.push((1, Proposal::StaleAttempt(call()))),
                }
            }
        }
        proposals
    }

    fn realize(&mut self, flow_id: &str, proposal: Proposal<'_>) -> FlowCommand {
        let rng = &mut self.rng;
        match proposal {
            Proposal::StartFlow => {
                let body = client_model::start_flow(rng, flow_id);
                self.envelope(flow_id, Vec::new(), body)
            }
            Proposal::ClientWrite { run_id, run } => {
                let body = match rng.random_range(0..40) {
                    0..=13 => client_model::set_attributes(rng),
                    14..=27 => client_model::publish(rng),
                    28..=33 => client_model::delete_message(rng, run),
                    34..=35 => client_model::stop(rng),
                    36 => client_model::config_update(),
                    _ => client_model::start_flow(rng, flow_id),
                };
                let run_id = match body {
                    Command::FlowStart(_) => Vec::new(),
                    _ => run_id.as_bytes().to_vec(),
                };
                self.envelope(flow_id, run_id, body)
            }
            Proposal::LaterHandler { run_id, run } => {
                let body = client_model::later_handler_command(rng, flow_id, run_id, run);
                self.envelope(flow_id, run_id.as_bytes().to_vec(), body)
            }
            Proposal::TravelToWindowPoint {
                run_id,
                timestamp_micros,
            } => {
                let body = client_model::time_travel_to_window_point(flow_id, timestamp_micros);
                self.envelope(flow_id, run_id.as_bytes().to_vec(), body)
            }
            Proposal::StartAttempt(call) => {
                let body = worker_model::start_attempt(rng, &call);
                self.envelope(flow_id, call.run_id.as_bytes().to_vec(), body)
            }
            Proposal::WorkerOutcome(call) => {
                let body = match rng.random_range(0..10) {
                    0..=5 => worker_model::complete(rng, &call),
                    6..=7 => worker_model::fail(rng, &call),
                    _ => worker_model::heartbeat(rng, &call),
                };
                let command = self.envelope(flow_id, call.run_id.as_bytes().to_vec(), body);
                self.remember_result(&command);
                command
            }
            Proposal::StaleAttempt(call) => {
                let body = worker_model::stale_attempt_command(rng, &call);
                self.envelope(flow_id, call.run_id.as_bytes().to_vec(), body)
            }
            Proposal::FireTimer {
                run_id,
                mut timer,
                fire_at_micros,
            } => {
                self.now_micros = self.now_micros.max(fire_at_micros);
                if rng.random_bool(0.05) {
                    timer.run_attempt += 1;
                }
                let body = worker_model::fire_timers(vec![timer]);
                self.envelope(flow_id, run_id.as_bytes().to_vec(), body)
            }
            Proposal::RedeliverResult => {
                let index = rng.random_range(0..self.sent_results.len());
                let previous = self.sent_results[index].clone();
                let body = previous
                    .command
                    .expect("remembered results are generated with a body");
                self.envelope(&previous.flow_id, previous.run_id, body)
            }
        }
    }

    fn envelope(&mut self, flow_id: &str, run_id: Vec<u8>, body: Command) -> FlowCommand {
        self.now_micros += self.rng.random_range(1_000..3_000_000);
        let expected_run_attempt = if self.rng.random_bool(0.05) {
            self.rng.random_range(1..3)
        } else {
            0
        };
        FlowCommand {
            flow_id: flow_id.to_owned(),
            run_id,
            expected_run_attempt,
            epoch: 1,
            behavior_version: dexzd_apply::BehaviorVersion::CURRENT.number(),
            proposed_at_micros: self.now_micros,
            command_id: value_generation::random_bytes(&mut self.rng).to_vec(),
            tenant_id: 0,
            command: Some(body),
        }
    }

    fn remember_result(&mut self, command: &FlowCommand) {
        if self.sent_results.len() == REMEMBERED_RESULTS {
            self.sent_results.remove(0);
        }
        self.sent_results.push(command.clone());
    }
}
