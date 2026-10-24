// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! One seed's schedule: clients, crashes, restarts, takeovers, clock jumps, and invariant checks.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dexzd_api::FlowOperations;
use dexzd_clock::WallClock;
use dexzd_cluster_metadata::MetadataError;
use dexzd_engine::{EngineLocalStore, EngineStartError, SeededEngineIdSourceForTest};
use dexzd_keyspace::TenantId;
use dexzd_object_store::{
    FaultAction, FaultInjector, InMemoryObjectStore, ObjectStore, ObjectStoreOperation,
};
use dexzd_protocol::dex::FlowAlreadyStartedOptions;
use rand::{RngExt, SeedableRng};
use rand_chacha::ChaCha8Rng;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::engine_simulation::committed_log::CommittedLog;
use crate::engine_simulation::engine_invariants::{
    check_acknowledged_writes, check_drained, check_recovered_flows,
    check_running_attempts_redispatched, check_store_matches_shadow, run_count, running_attempts,
};
use crate::engine_simulation::event_trace::EventTrace;
use crate::engine_simulation::simulated_clients::{
    ClientRecords, ENGINE_FLOW_IDS, ServingNode, SimulatedClient,
};
use crate::engine_simulation::simulated_clock::{
    SimulatedWallClock, duration_micros, elapsed_micros,
};
use crate::engine_simulation::simulated_node::{
    ClusterEnvironment, LEASE_TAKEOVER_GRACE, LocalStoreStart, NodeProcess, ProcessStart,
    copy_local_store,
};
use crate::engine_simulation::simulated_object_store::StorageObservations;
use crate::engine_simulation::simulated_workers::SimulatedWorkers;
use crate::engine_simulation::workload::{OPERATION_MARKER_PREFIX, start_flow_request};
use crate::engine_simulation::{
    EngineSimulationConfig, EngineSimulationFailure, EngineSimulationFinalState,
    EngineSimulationSummary,
};
use crate::invariants::InvariantViolation;

/// Starts of one restart before the node counts as unable to recover.
const MAX_START_ATTEMPTS: u32 = 40;
/// Local store copies kept as earlier consistent points for rolled-back restarts.
const KEPT_STORE_COPIES: usize = 3;
/// Interval of the periodic invariant checks.
const CHECK_INTERVAL: Duration = Duration::from_secs(2);
/// Longest life of an old-epoch process after its successor took over.
const OLD_EPOCH_LIFETIME: Duration = Duration::from_secs(15);
/// A due timer may wait this long before the calm tail counts it as never fired.
const TIMER_FIRING_SLACK: Duration = Duration::from_secs(10);
/// Closed runs of half the seeds outlive the seed; the rest are purged within it.
const LONG_RETENTION: Duration = Duration::from_secs(60 * 60);
const SHORT_RETENTION: Duration = Duration::from_secs(30);
/// Probability that a process crashes at a random point while it starts.
const START_CRASH_PROBABILITY: f64 = 0.15;
/// Longest wait after the calm tail for unfinished work to finish.
const DRAIN_GRACE: Duration = Duration::from_secs(60);
/// Object storage fault probabilities a seed draws from; some seeds run without faults.
const FAULT_PROBABILITIES: [f64; 5] = [0.0, 0.001, 0.005, 0.01, 0.02];
/// Starts see fewer faults: one transient read error fails a start, and the log grows.
const START_FAULT_DIVISOR: f64 = 4.0;

/// The shared store applies every call; faults come from each process's own layer.
struct NoInjectedFaults;

impl FaultInjector for NoInjectedFaults {
    fn action(&self, _operation: ObjectStoreOperation, _key: &str) -> FaultAction {
        FaultAction::Proceed
    }
}

#[derive(Debug, Clone, Copy)]
/// What a restarted process finds on its local disk.
enum RestartKind {
    Kept,
    Lost,
    RolledBack,
}

impl RestartKind {
    fn name(self) -> &'static str {
        match self {
            Self::Kept => "kept",
            Self::Lost => "lost",
            Self::RolledBack => "rolled-back",
        }
    }
}

struct Simulation {
    seed: u64,
    config: EngineSimulationConfig,
    rng: ChaCha8Rng,
    environment: ClusterEnvironment,
    serving: Option<NodeProcess>,
    old_epoch_processes: Vec<(NodeProcess, Instant)>,
    serving_node: ServingNode,
    client_records: Arc<Mutex<ClientRecords>>,
    store_copies: VecDeque<Arc<EngineLocalStore>>,
    crashed_process_tasks: Vec<JoinHandle<()>>,
    started_processes: u64,
    fault_probability: f64,
    summary: EngineSimulationSummary,
}

/// Runs one seed; returns what it exercised and its final state, or the first broken invariant.
pub(crate) async fn run_simulation(
    seed: u64,
    config: EngineSimulationConfig,
) -> Result<(EngineSimulationSummary, EngineSimulationFinalState), Box<EngineSimulationFailure>> {
    let mut simulation = Simulation::new(seed, config);
    let outcome = simulation.run().await;
    let simulated_micros = elapsed_micros(simulation.environment.origin);
    // Hung calls of crashed processes never finish; the runtime drops them.
    for task in simulation.crashed_process_tasks.drain(..) {
        task.abort();
    }
    match outcome {
        Ok(()) => {
            let final_state = simulation.final_state().await;
            Ok((simulation.finish_summary(), final_state))
        }
        Err(violation) => {
            simulation
                .environment
                .trace
                .record(&format!("failed: {violation}"));
            Err(Box::new(EngineSimulationFailure {
                seed,
                config,
                simulated_micros,
                violation,
                recent_events: simulation.environment.trace.recent_events(),
            }))
        }
    }
}

impl Simulation {
    fn new(seed: u64, config: EngineSimulationConfig) -> Self {
        let origin = Instant::now();
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let trace = Arc::new(EventTrace::new(origin));
        let storage_observations = Arc::new(Mutex::new(StorageObservations::default()));
        let id_clock: Arc<dyn WallClock> = Arc::new(SimulatedWallClock::new(origin, 0));
        let storage_clock: Arc<dyn WallClock> = Arc::new(SimulatedWallClock::new(origin, 0));
        let shared_store = Arc::new(InMemoryObjectStore::with_injector_and_clock(
            Arc::new(NoInjectedFaults),
            storage_clock,
        ));
        let workers = Arc::new(SimulatedWorkers::new(rng.random(), Arc::clone(&trace)));
        let fault_probability = FAULT_PROBABILITIES[rng.random_range(0..FAULT_PROBABILITIES.len())];
        let retention_after_close = if rng.random_bool(0.5) {
            LONG_RETENTION
        } else {
            SHORT_RETENTION
        };
        Self {
            seed,
            config,
            environment: ClusterEnvironment {
                shared_store,
                workers,
                committed_log: Arc::new(CommittedLog::new(
                    Arc::clone(&trace),
                    Arc::clone(&storage_observations),
                )),
                trace,
                storage_observations,
                id_source: Arc::new(SeededEngineIdSourceForTest::new(seed, id_clock)),
                retention_after_close,
                origin,
            },
            rng,
            serving: None,
            old_epoch_processes: Vec::new(),
            serving_node: ServingNode::default(),
            client_records: Arc::new(Mutex::new(ClientRecords::default())),
            store_copies: VecDeque::new(),
            crashed_process_tasks: Vec::new(),
            started_processes: 0,
            fault_probability,
            summary: EngineSimulationSummary::default(),
        }
    }

    fn trace(&self, event: &str) {
        self.environment.trace.record(event);
    }

    async fn run(&mut self) -> Result<(), InvariantViolation> {
        let cluster_id: [u8; 16] = self.rng.random();
        self.environment.bootstrap_cluster_object(cluster_id).await;
        self.trace(&format!(
            "seed {} fault probability {}",
            self.seed, self.fault_probability
        ));
        let first = self
            .start_until_serving(LocalStoreStart::Lost, 0, "first")
            .await?;
        self.serve(first);
        let active_end = Instant::now() + Duration::from_secs(self.config.active_seconds);
        let client_tasks = self.spawn_clients(active_end);
        self.run_active_phase(active_end).await?;
        for task in client_tasks {
            if let Err(error) = task.await
                && error.is_panic()
            {
                std::panic::resume_unwind(error.into_panic());
            }
        }
        self.run_calm_tail().await
    }

    fn spawn_clients(&mut self, active_end: Instant) -> Vec<JoinHandle<()>> {
        let client_count = self.rng.random_range(2..=3);
        let mut client_tasks = Vec::with_capacity(client_count);
        for index in 0..client_count {
            let client = SimulatedClient::new(
                format!("c{index}"),
                self.rng.random(),
                self.serving_node.clone(),
                Arc::clone(&self.environment.committed_log),
                Arc::clone(&self.client_records),
                Arc::clone(&self.environment.trace),
            );
            let client_task = tokio::spawn(client.run_until(active_end));
            client_tasks.push(client_task);
        }
        client_tasks
    }

    async fn run_active_phase(&mut self, active_end: Instant) -> Result<(), InvariantViolation> {
        let mut next_check = Instant::now() + CHECK_INTERVAL;
        let mut next_copy = self.later(3_000..30_000);
        let mut next_jump = self.later(10_000..40_000);
        let mut next_crash = self.later(8_000..30_000);
        loop {
            let wake = next_check
                .min(next_copy)
                .min(next_jump)
                .min(next_crash)
                .min(active_end);
            tokio::time::sleep_until(wake).await;
            let now = Instant::now();
            if now >= active_end {
                return Ok(());
            }
            self.replace_stopped_processes().await?;
            if now >= next_check {
                self.check_now()?;
                next_check = now + CHECK_INTERVAL;
            }
            if now >= next_copy {
                self.copy_serving_store();
                next_copy = self.later(3_000..30_000);
            }
            if now >= next_jump {
                self.jump_serving_clock();
                next_jump = self.later(10_000..40_000);
            }
            if now >= next_crash {
                if self.old_epoch_processes.is_empty() && self.rng.random_bool(0.25) {
                    self.take_over_with_overlap().await?;
                } else {
                    let kind = self.draw_restart_kind();
                    self.crash_and_restart(kind).await?;
                }
                next_crash = self.later(8_000..30_000);
            }
        }
    }

    async fn run_calm_tail(&mut self) -> Result<(), InvariantViolation> {
        self.trace("calm tail begins");
        for (process, _) in std::mem::take(&mut self.old_epoch_processes) {
            self.crash(process);
        }
        self.fault_probability = 0.0;
        if let Some(serving) = &self.serving {
            serving.object_store.set_fault_probability(0.0);
        }
        self.environment.workers.set_calm(true);
        tokio::time::sleep(Duration::from_secs(5)).await;
        self.replace_stopped_processes().await?;
        self.check_now()?;
        // A crash at a quiet point: the successor must dispatch every running attempt again.
        self.environment.workers.set_holding(true);
        self.start_held_runs().await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        let kind = self.draw_restart_kind();
        self.crash_and_restart(kind).await?;
        let running_at_recovery = self
            .environment
            .committed_log
            .with_shadow(running_attempts)?;
        tokio::time::sleep(Duration::from_secs(3)).await;
        let running_now = self
            .environment
            .committed_log
            .with_shadow(running_attempts)?;
        let serving_label = self.serving_label();
        check_running_attempts_redispatched(
            &serving_label,
            &self.environment.workers.dispatch_history(&serving_label),
            &running_at_recovery,
            &running_now,
        )?;
        self.summary.dispatch_comparisons += 1;
        self.summary.attempts_compared_after_quiet_crash += running_at_recovery.len() as u64;
        self.environment.workers.set_holding(false);
        let tail = Duration::from_secs(self.config.calm_tail_seconds);
        tokio::time::sleep(tail).await;
        self.wait_until_drained().await?;
        let records = self
            .client_records
            .lock()
            .expect("client records lock is not poisoned");
        check_acknowledged_writes(&records, &self.environment.committed_log)
    }

    /// Starts a run of every Flow without one, so running attempts exist at the quiet crash.
    async fn start_held_runs(&mut self) {
        let Some(serving) = &self.serving else {
            return;
        };
        let registry = serving.registry.clone();
        for flow_id in ENGINE_FLOW_IDS {
            let marker = format!("{OPERATION_MARKER_PREFIX}tail-{flow_id}");
            let mut request = start_flow_request(&mut self.rng, flow_id, &marker);
            if let Some(options) = request.flow_start_options.as_mut() {
                options.flow_already_started_options = Some(FlowAlreadyStartedOptions {
                    ignore_already_started_error: true,
                });
            }
            let started = tokio::time::timeout(
                Duration::from_secs(5),
                FlowOperations::start_flow(&registry, TenantId::DEFAULT, request),
            )
            .await;
            let outcome = match started {
                Ok(Ok(_)) => "ok".to_owned(),
                Ok(Err(error)) => error.to_string(),
                Err(_) => "timeout".to_owned(),
            };
            self.trace(&format!("tail StartFlow {flow_id}: {outcome}"));
        }
    }

    /// Polls until no method call is unfinished and no due timer is unfired, within
    /// [`DRAIN_GRACE`].
    async fn wait_until_drained(&mut self) -> Result<(), InvariantViolation> {
        let deadline = Instant::now() + DRAIN_GRACE;
        loop {
            self.replace_stopped_processes().await?;
            self.check_now()?;
            let now_micros = self
                .serving
                .as_ref()
                .map_or(0, |serving| serving.clock.now_unix_micros());
            let drained = self.environment.committed_log.with_shadow(|shadow| {
                check_drained(shadow, now_micros, duration_micros(TIMER_FIRING_SLACK))
            });
            match drained {
                Ok(()) => return Ok(()),
                Err(violation) if Instant::now() >= deadline => return Err(violation),
                Err(_) => tokio::time::sleep(Duration::from_secs(1)).await,
            }
        }
    }

    /// Checks every invariant that holds at any instant.
    fn check_now(&mut self) -> Result<(), InvariantViolation> {
        if let Some(violation) = self.environment.committed_log.first_violation() {
            return Err(violation);
        }
        {
            let records = self
                .client_records
                .lock()
                .expect("client records lock is not poisoned");
            if let Some(violation) = &records.first_violation {
                return Err(violation.clone());
            }
            check_acknowledged_writes(&records, &self.environment.committed_log)?;
        }
        let Some(serving) = &self.serving else {
            return Ok(());
        };
        let halted = serving.halted_partitions();
        if !halted.is_empty() {
            return Err(InvariantViolation::new(
                "partition-halted",
                format!(
                    "{} halted partitions {halted:?} after an apply failure",
                    serving.process_label
                ),
            ));
        }
        // Old-epoch processes may still apply frames, so only one live process is compared.
        if self.old_epoch_processes.is_empty() {
            let shadow = self.environment.committed_log.shadow_state();
            check_store_matches_shadow(&serving.local_store, &shadow)?;
            self.summary.state_comparisons += 1;
        }
        Ok(())
    }

    /// Crashes processes that stopped by themselves or outlived their overlap, and restarts
    /// the serving process when it stopped.
    async fn replace_stopped_processes(&mut self) -> Result<(), InvariantViolation> {
        let now = Instant::now();
        let mut still_running = Vec::new();
        for (process, deadline) in std::mem::take(&mut self.old_epoch_processes) {
            if process.stop_reason().is_some() || now >= deadline {
                if process.stop_reason().is_some() {
                    self.summary.self_stopped_processes += 1;
                }
                self.crash(process);
            } else {
                still_running.push((process, deadline));
            }
        }
        self.old_epoch_processes = still_running;
        let has_stopped = self
            .serving
            .as_ref()
            .is_some_and(|serving| serving.stop_reason().is_some() || serving.is_registry_halted());
        if has_stopped {
            let reason = self
                .serving
                .as_ref()
                .and_then(NodeProcess::stop_reason)
                .map_or_else(|| "registry halted".to_owned(), |reason| reason.to_string());
            self.trace(&format!(
                "{} stopped by itself: {reason}",
                self.serving_label()
            ));
            self.summary.self_stopped_processes += 1;
            self.crash_and_restart(RestartKind::Kept).await?;
        }
        Ok(())
    }

    fn draw_restart_kind(&mut self) -> RestartKind {
        match self.rng.random_range(0..10) {
            0..=3 => RestartKind::Kept,
            4..=6 => RestartKind::Lost,
            _ if self.store_copies.is_empty() => RestartKind::Kept,
            _ => RestartKind::RolledBack,
        }
    }

    async fn crash_and_restart(&mut self, kind: RestartKind) -> Result<(), InvariantViolation> {
        let Some(serving) = self.serving.take() else {
            return Ok(());
        };
        self.serving_node.clear();
        let clock_offset = serving.clock.offset_micros();
        let local_store = Arc::clone(&serving.local_store);
        self.trace(&format!(
            "crash {} and restart with a {} local store",
            serving.process_label,
            kind.name()
        ));
        self.crash(serving);
        let start = match kind {
            RestartKind::Kept => LocalStoreStart::Existing(copy_local_store(&local_store)),
            RestartKind::Lost => {
                self.store_copies.clear();
                LocalStoreStart::Lost
            }
            RestartKind::RolledBack => {
                let index = self.rng.random_range(0..self.store_copies.len());
                LocalStoreStart::Existing(copy_local_store(&self.store_copies[index]))
            }
        };
        *self
            .summary
            .restarts_by_local_store
            .entry(kind.name().to_owned())
            .or_default() += 1;
        let process = self
            .start_until_serving(start, clock_offset, kind.name())
            .await?;
        self.serve(process);
        self.check_recovered_process()
    }

    /// Starts a successor whose clock is past the lease while the old process keeps running.
    async fn take_over_with_overlap(&mut self) -> Result<(), InvariantViolation> {
        let Some(old) = self.serving.take() else {
            return Ok(());
        };
        let skew = duration_micros(super::simulated_node::LEASE_DURATION + LEASE_TAKEOVER_GRACE)
            + self.rng.random_range(500_000..3_000_000);
        let successor_offset = old.clock.offset_micros() + skew;
        self.trace(&format!(
            "{} keeps running while a successor takes over",
            old.process_label
        ));
        self.store_copies.clear();
        self.environment
            .storage_observations
            .lock()
            .expect("storage observation lock is not poisoned")
            .old_epoch
            .await_successor(&old.process_label);
        let deadline = Instant::now() + OLD_EPOCH_LIFETIME;
        self.old_epoch_processes.push((old, deadline));
        *self
            .summary
            .restarts_by_local_store
            .entry(RestartKind::Lost.name().to_owned())
            .or_default() += 1;
        let process = self
            .start_until_serving(LocalStoreStart::Lost, successor_offset, "takeover")
            .await?;
        self.serve(process);
        Ok(())
    }

    /// Recovery checks: the recovered store equals the shadow and keeps the stage-1 invariants.
    fn check_recovered_process(&mut self) -> Result<(), InvariantViolation> {
        self.check_now()?;
        let Some(serving) = &self.serving else {
            return Ok(());
        };
        self.environment
            .committed_log
            .with_shadow(|shadow| check_recovered_flows(&serving.local_store, shadow))
    }

    /// Starts processes until one serves. A failed start is retried; endless failure is a bug.
    async fn start_until_serving(
        &mut self,
        local_store: LocalStoreStart,
        clock_offset_micros: i64,
        start_kind: &str,
    ) -> Result<NodeProcess, InvariantViolation> {
        let mut existing = match &local_store {
            LocalStoreStart::Lost => None,
            LocalStoreStart::Existing(store) => Some(Arc::clone(store)),
        };
        let mut last_error = String::new();
        for _ in 0..MAX_START_ATTEMPTS {
            self.started_processes += 1;
            let process_label = format!("p{}", self.started_processes);
            let object_store = self.environment.node_object_store(
                &process_label,
                start_kind,
                self.rng.random(),
                self.fault_probability / START_FAULT_DIVISOR,
            );
            let start = ProcessStart {
                process_label: process_label.clone(),
                local_store: existing.as_ref().map_or(LocalStoreStart::Lost, |store| {
                    LocalStoreStart::Existing(Arc::clone(store))
                }),
                clock_offset_micros,
                object_store: Arc::clone(&object_store),
            };
            let clock = SimulatedWallClock::new(self.environment.origin, clock_offset_micros);
            let crash_after = self
                .rng
                .random_bool(START_CRASH_PROBABILITY)
                .then(|| Duration::from_millis(self.rng.random_range(0..2_000)));
            let started = match crash_after {
                Some(delay) => tokio::select! {
                    biased;
                    started = NodeProcess::start(&self.environment, start) => Some(started),
                    () = tokio::time::sleep(delay) => None,
                },
                None => Some(NodeProcess::start(&self.environment, start).await),
            };
            let Some(started) = started else {
                // Recovery died at an arbitrary await point; its disk keeps what it wrote so far.
                object_store.cut_off();
                existing = existing.map(|store| copy_local_store(&store));
                self.summary.crashes_during_start += 1;
                self.trace(&format!("{process_label} crashes while it starts"));
                continue;
            };
            match started {
                Ok(process) => {
                    process.object_store.mark_serving();
                    process
                        .object_store
                        .set_fault_probability(self.fault_probability);
                    self.trace(&format!("{process_label} serves"));
                    return Ok(process);
                }
                Err(error) => {
                    let kind = start_error_kind(&error);
                    self.trace(&format!("{process_label} failed to start: {kind}: {error}"));
                    *self
                        .summary
                        .start_failures
                        .entry(kind.to_owned())
                        .or_default() += 1;
                    let wait = match &error {
                        EngineStartError::Metadata(MetadataError::LeaseHeld {
                            expires_at_unix_micros,
                            ..
                        }) => {
                            let available_at = expires_at_unix_micros
                                + duration_micros(LEASE_TAKEOVER_GRACE)
                                + 1_000;
                            let micros = (available_at - clock.now_unix_micros()).max(1_000);
                            Duration::from_micros(u64::try_from(micros).unwrap_or(1_000))
                        }
                        _ => Duration::from_millis(self.rng.random_range(200..3_000)),
                    };
                    last_error = error.to_string();
                    tokio::time::sleep(wait).await;
                }
            }
        }
        Err(InvariantViolation::new(
            "node-cannot-recover",
            format!("{MAX_START_ATTEMPTS} starts failed; the last failed with: {last_error}"),
        ))
    }

    fn serve(&mut self, process: NodeProcess) {
        self.serving_node.serve(
            &process.process_label,
            process.registry.clone(),
            Arc::clone(&process.local_store),
        );
        self.serving = Some(process);
    }

    fn crash(&mut self, process: NodeProcess) {
        self.trace(&format!("{} crashes", process.process_label));
        process.crash(&mut self.crashed_process_tasks);
    }

    fn copy_serving_store(&mut self) {
        let Some(serving) = &self.serving else {
            return;
        };
        if self.store_copies.len() == KEPT_STORE_COPIES {
            self.store_copies.pop_front();
        }
        self.store_copies
            .push_back(copy_local_store(&serving.local_store));
    }

    fn jump_serving_clock(&mut self) {
        let Some(serving) = &self.serving else {
            return;
        };
        let jump_micros = if self.rng.random_bool(0.5) {
            self.rng.random_range(100_000..15_000_000)
        } else {
            -self.rng.random_range(100_000..5_000_000)
        };
        serving.clock.jump(jump_micros);
        self.summary.clock_jumps += 1;
        self.trace(&format!(
            "{} wall clock jumps by {jump_micros} µs",
            serving.process_label
        ));
    }

    fn later(&mut self, millis: std::ops::Range<u64>) -> Instant {
        Instant::now() + Duration::from_millis(self.rng.random_range(millis))
    }

    fn serving_label(&self) -> String {
        self.serving
            .as_ref()
            .map_or_else(String::new, |serving| serving.process_label.clone())
    }

    /// Returns the trace, the serving process's local store, and every stored object.
    async fn final_state(&self) -> EngineSimulationFinalState {
        let shared_store = &self.environment.shared_store;
        let mut objects = BTreeMap::new();
        let mut start_after = None;
        loop {
            let listing = shared_store
                .list("", start_after.as_deref(), 1_000)
                .await
                .expect("the shared in-memory store lists without faults");
            for object in listing.objects {
                let content = shared_store
                    .get(&object.key)
                    .await
                    .expect("the shared in-memory store reads without faults");
                if let Some(content) = content {
                    objects.insert(object.key, content.bytes);
                }
            }
            match listing.next_start_after {
                Some(next) => start_after = Some(next),
                None => break,
            }
        }
        EngineSimulationFinalState {
            trace_events: self.environment.trace.all_events(),
            local_store: self
                .serving
                .as_ref()
                .map(|serving| copy_local_store(&serving.local_store)),
            objects,
        }
    }

    fn finish_summary(&mut self) -> EngineSimulationSummary {
        let mut summary = std::mem::take(&mut self.summary);
        let environment = &self.environment;
        summary.trace_hash = environment.trace.digest();
        summary.trace_events = environment.trace.event_count();
        summary.recent_events = environment.trace.recent_events();
        {
            let observations = environment
                .storage_observations
                .lock()
                .expect("storage observation lock is not poisoned");
            summary.injected_faults = observations
                .injected_faults
                .iter()
                .map(|((fault, effect), count)| (format!("{fault:?}/{effect:?}"), *count))
                .collect();
            summary.precondition_failed_puts = name_counts(&observations.precondition_failed_puts);
            summary.deleted_objects = name_counts(&observations.deleted_objects);
            summary
                .snapshot_reads_during_start
                .clone_from(&observations.snapshot_reads_during_start);
            summary.old_epoch_overlaps = observations.old_epoch.overlaps;
            summary.old_epoch_frames = observations.old_epoch.frames;
            summary.old_epoch_write_attempts = name_counts(&observations.old_epoch.write_attempts);
        }
        summary.rejected_by_kind = name_counts(&environment.committed_log.rejected_by_kind());
        summary.worker_behaviors = environment
            .workers
            .behavior_counts()
            .into_iter()
            .map(|(behavior, count)| (format!("{behavior:?}"), count))
            .collect();
        summary.applied_by_kind = environment
            .committed_log
            .applied_by_kind()
            .into_iter()
            .map(|(kind, count)| (kind.to_owned(), count))
            .collect();
        summary.replayed_frames = environment.committed_log.replayed_frames();
        summary.final_runs = environment.committed_log.with_shadow(run_count);
        let records = self
            .client_records
            .lock()
            .expect("client records lock is not poisoned");
        summary.acknowledged_writes = records.acknowledged_writes.len() as u64;
        summary.checked_reads = records.checked_reads;
        summary.checked_read_values = records.checked_read_values;
        summary.client_calls = records
            .outcomes
            .iter()
            .map(|((operation, outcome), count)| (format!("{operation} {outcome}"), *count))
            .collect();
        summary
    }
}

fn name_counts(counts: &BTreeMap<&'static str, u64>) -> BTreeMap<String, u64> {
    counts
        .iter()
        .map(|(name, count)| ((*name).to_owned(), *count))
        .collect()
}

fn start_error_kind(error: &EngineStartError) -> &'static str {
    match error {
        EngineStartError::Metadata(MetadataError::LeaseHeld { .. }) => "lease-held",
        EngineStartError::Metadata(_) => "metadata",
        EngineStartError::Storage(_) => "local-store",
        EngineStartError::Log(_) => "log",
        EngineStartError::Stopped { .. } => "stopped",
        EngineStartError::RuntimeRebuild(_) => "runtime-rebuild",
        _ => "other",
    }
}
