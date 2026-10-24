// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! A chain and counter workload whose client ledger and Worker journal survive server failures.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::net::SocketAddr;
use std::process::ExitStatus;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dexzd_engine::WAIT_FOR_FLOW_TIMEOUT_MESSAGE;
use dexzd_protocol::dex::flow_service_client::FlowServiceClient;
use dexzd_protocol::dex::worker_task_service_client::WorkerTaskServiceClient;
use dexzd_protocol::dex::{
    AttributeWrite, CloseDecision, CloseDecisionType, FlowAlreadyStartedOptions, FlowConfig,
    FlowStartOptions, FlowStatus, GetAttributesRequest, InvokeExecuteMethodOutput,
    InvokeExecuteMethodRequest, InvokeExecuteMethodResponse, InvokeRpcRequest,
    InvokeWaitForMethodOutput, InvokeWaitForMethodResponse, InvokeWorkerRpcRequest,
    InvokeWorkerRpcResponse, PollWorkerTask, PollWorkerTaskFrame, PollWorkerTaskRegister,
    PollWorkerTaskRequest, StartFlowRequest, StepDecision, StepMovement, StepOptions, Value,
    WaitForFlowRequest, WaitingCondition, WorkerTarget, invoke_execute_method_output,
    invoke_wait_for_method_output, poll_worker_task, poll_worker_task_frame,
    poll_worker_task_request, poll_worker_task_response, value,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Channel;

use super::processes::RobustnessProcess;

const CHAIN_FLOW_TYPE: &str = "RobustnessChainFlow";
const COUNTER_FLOW_TYPE: &str = "RobustnessCounterFlow";
const CHAIN_STEP_TYPE: &str = "ChainStep";
const COUNTER_START_STEP_TYPE: &str = "CounterStartStep";
const INCREMENT_COUNTER_RPC: &str = "IncrementCounter";
const FINISH_COUNTER_RPC: &str = "FinishCounter";
const FINISH_COUNTER_STEP_TYPE: &str = "FinishCounterStep";
const COUNTER_ATTRIBUTE: &str = "counter-value";
/// A chain Flow runs ChainStep for indexes 0 through this one, then completes with it.
const LAST_CHAIN_INDEX: i64 = 9;
/// Every Worker incarnation reuses this ID, so sticky dispatch finds the reconnected streams.
const WORKER_ID: &str = "robustness-worker";
/// Each stream holds one task at a time, so concurrent Flows need several per queue.
const STREAMS_PER_QUEUE: usize = 4;
/// Lets the server release a counter lock soon after a client gave up on the call.
const RPC_TIMEOUT_SECONDS: i32 = 2;
const RETRY_PAUSE: Duration = Duration::from_millis(50);
const PROGRESS_POLL_INTERVAL: Duration = Duration::from_millis(20);
const PROGRESS_BUDGET: Duration = Duration::from_secs(30);
const SETUP_BUDGET: Duration = Duration::from_secs(20);
const STREAM_OPEN_TIMEOUT: Duration = Duration::from_secs(5);
/// Covers chain Flows that resume only after the last restart.
/// Also fits two 10 s dispatch waits and their retry backoff after a takeover.
const VERIFICATION_BUDGET: Duration = Duration::from_secs(60);
const VERIFICATION_CALL_TIMEOUT: Duration = Duration::from_secs(10);
const RECENT_FAILURE_COUNT: usize = 20;

/// One Execute call that a Worker handled.
#[derive(Clone, Debug)]
struct ChainStepExecution {
    flow_id: String,
    run_id: String,
    step_execution_id: String,
    attempt: i32,
    index: i64,
}

/// One IncrementCounter call that a Worker handled.
#[derive(Clone, Debug)]
struct CounterIncrement {
    flow_id: String,
    run_id: String,
    value: i64,
}

/// What every Worker incarnation observed, in handling order.
#[derive(Default)]
struct WorkerJournal {
    chain_steps: Mutex<Vec<ChainStepExecution>>,
    counter_increments: Mutex<Vec<CounterIncrement>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WorkloadFlowKind {
    Chain,
    Counter,
}

/// A StartFlow call that returned success.
#[derive(Clone, Debug)]
struct AcknowledgedStart {
    flow_id: String,
    run_id: String,
    kind: WorkloadFlowKind,
}

/// An acknowledged chain Flow that a test saw complete.
pub(crate) struct CompletedChain(AcknowledgedStart);

impl CompletedChain {
    pub(crate) fn flow_id(&self) -> &str {
        &self.0.flow_id
    }
}

/// The client view of one counter Flow.
#[derive(Clone, Debug, Default)]
struct CounterLedger {
    acknowledged_values: Vec<i64>,
    unknown_outcomes: u64,
}

/// Every client call outcome: only calls that returned success are acknowledged.
#[derive(Default)]
struct ClientLedger {
    starts: Mutex<Vec<AcknowledgedStart>>,
    counters: Mutex<BTreeMap<String, CounterLedger>>,
    acknowledged_count: AtomicU64,
    failed_start_attempts: AtomicU64,
    next_request_number: AtomicU64,
    recent_failures: Mutex<VecDeque<String>>,
}

impl ClientLedger {
    fn record_start(&self, flow_id: &str, run_id: &str, kind: WorkloadFlowKind) {
        self.starts
            .lock()
            .expect("the ledger lock is not poisoned")
            .push(AcknowledgedStart {
                flow_id: flow_id.to_owned(),
                run_id: run_id.to_owned(),
                kind,
            });
        if kind == WorkloadFlowKind::Counter {
            self.counters
                .lock()
                .expect("the ledger lock is not poisoned")
                .entry(flow_id.to_owned())
                .or_default();
        }
        self.acknowledged_count.fetch_add(1, Ordering::SeqCst);
    }

    fn record_failed_start(&self, failure: String) {
        self.failed_start_attempts.fetch_add(1, Ordering::SeqCst);
        self.record_failure(failure);
    }

    fn record_increment(&self, flow_id: &str, value: i64) {
        self.counters
            .lock()
            .expect("the ledger lock is not poisoned")
            .get_mut(flow_id)
            .expect("increments target an acknowledged counter")
            .acknowledged_values
            .push(value);
        self.acknowledged_count.fetch_add(1, Ordering::SeqCst);
    }

    fn record_unknown_increment(&self, flow_id: &str, failure: String) {
        self.counters
            .lock()
            .expect("the ledger lock is not poisoned")
            .get_mut(flow_id)
            .expect("increments target an acknowledged counter")
            .unknown_outcomes += 1;
        self.record_failure(failure);
    }

    fn record_failure(&self, failure: String) {
        let mut recent = self
            .recent_failures
            .lock()
            .expect("the ledger lock is not poisoned");
        if recent.len() == RECENT_FAILURE_COUNT {
            recent.pop_front();
        }
        recent.push_back(failure);
    }

    /// Returns a request ID that no earlier call of this test used.
    fn new_request_id(&self, purpose: &str) -> String {
        let number = self.next_request_number.fetch_add(1, Ordering::SeqCst);
        format!("{purpose}-{number}")
    }

    fn starts(&self) -> Vec<AcknowledgedStart> {
        self.starts
            .lock()
            .expect("the ledger lock is not poisoned")
            .clone()
    }

    fn counters(&self) -> BTreeMap<String, CounterLedger> {
        self.counters
            .lock()
            .expect("the ledger lock is not poisoned")
            .clone()
    }
}

/// Ledger counts a load phase must reach, so a phase cannot pass on chain starts alone.
#[derive(Debug)]
pub(crate) struct ProgressTarget {
    acknowledged_count: u64,
    counter_increment_counts: BTreeMap<String, usize>,
}

/// Counts from a passing verification.
#[derive(Debug)]
pub(crate) struct VerificationReport {
    chain_flows: usize,
    counter_flows: usize,
    acknowledged_increments: usize,
    unknown_increments: u64,
    chain_step_executions: usize,
    re_executed_step_executions: usize,
}

impl fmt::Display for VerificationReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} chain Flows completed; {} counters completed with {} acknowledged and {} unknown increments; {} chain Step executions, {} re-executed",
            self.chain_flows,
            self.counter_flows,
            self.acknowledged_increments,
            self.unknown_increments,
            self.chain_step_executions,
            self.re_executed_step_executions
        )
    }
}

/// Chain Flows and locked counters driven against one fixed server address.
pub(crate) struct Workload {
    ledger: Arc<ClientLedger>,
    journal: Arc<WorkerJournal>,
}

impl Workload {
    pub(crate) fn new() -> Self {
        Self {
            ledger: Arc::new(ClientLedger::default()),
            journal: Arc::new(WorkerJournal::default()),
        }
    }

    /// Returns a callback that describes the ledger and journal for failure output.
    pub(crate) fn summary_source(&self) -> Box<dyn Fn() -> String + Send + Sync> {
        let ledger = self.ledger.clone();
        let journal = self.journal.clone();
        Box::new(move || summarize(&ledger, &journal))
    }

    pub(crate) fn acknowledged_count(&self) -> u64 {
        self.ledger.acknowledged_count.load(Ordering::SeqCst)
    }

    /// Connects a Worker that records every handler call in the shared journal.
    pub(crate) async fn start_worker(&self, channel: Channel) -> WorkloadWorker {
        self.try_start_worker(channel)
            .await
            .unwrap_or_else(|error| panic!("the Worker did not register: {error}"))
    }

    /// Like [`Self::start_worker`], but returns why a stream did not register.
    pub(crate) async fn try_start_worker(
        &self,
        channel: Channel,
    ) -> Result<WorkloadWorker, String> {
        WorkloadWorker::start(channel, self.journal.clone()).await
    }

    /// Starts `count` counter Flows; each must be acknowledged within the setup budget.
    pub(crate) async fn start_counters(&self, channel: Channel, count: usize) {
        let mut client = FlowServiceClient::new(channel);
        let is_stopping = AtomicBool::new(false);
        for counter in 0..count {
            let flow_id = format!("counter-{counter}");
            let request = start_request(
                &flow_id,
                COUNTER_FLOW_TYPE,
                COUNTER_START_STEP_TYPE,
                int_value(0),
                &self.ledger,
            );
            let run_id = tokio::time::timeout(
                SETUP_BUDGET,
                start_until_acknowledged(
                    &mut client,
                    &request,
                    &self.ledger,
                    &is_stopping,
                    SETUP_BUDGET,
                ),
            )
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "{flow_id} was not acknowledged within {SETUP_BUDGET:?}:\n{}",
                    summarize(&self.ledger, &self.journal)
                )
            })
            .expect("setup never stops");
            self.ledger
                .record_start(&flow_id, &run_id, WorkloadFlowKind::Counter);
        }
    }

    /// Starts `chain_starters` sequential chain starters and one writer per counter.
    ///
    /// Flow IDs include `phase`, so each phase starts new chain Flows.
    pub(crate) fn start_load(
        &self,
        address: SocketAddr,
        phase: &str,
        chain_starters: usize,
        call_timeout: Duration,
    ) -> RunningLoad {
        let is_stopping = Arc::new(AtomicBool::new(false));
        let mut tasks = Vec::new();
        for starter in 0..chain_starters {
            let client = FlowServiceClient::new(lazy_channel(address));
            let ledger = self.ledger.clone();
            let is_stopping = is_stopping.clone();
            let flow_id_prefix = format!("chain-{phase}-{starter}");
            let starter_task = tokio::spawn(run_chain_starter(
                client,
                ledger,
                is_stopping,
                flow_id_prefix,
                call_timeout,
            ));
            tasks.push(starter_task);
        }
        for flow_id in self.ledger.counters().into_keys() {
            let client = FlowServiceClient::new(lazy_channel(address));
            let ledger = self.ledger.clone();
            let is_stopping = is_stopping.clone();
            let writer_task = tokio::spawn(run_counter_writer(
                client,
                ledger,
                is_stopping,
                flow_id,
                call_timeout,
            ));
            tasks.push(writer_task);
        }
        RunningLoad { is_stopping, tasks }
    }

    /// Waits for `count` more acknowledged calls; fails if `process` exits meanwhile.
    ///
    /// Every counter must also gain an acknowledged increment.
    pub(crate) async fn wait_for_new_acknowledgements(
        &self,
        count: u64,
        process: &mut RobustnessProcess,
    ) {
        let target = self.progress_target(count);
        if let Err(status) = self.wait_for_progress_or_exit(&target, process).await {
            panic!(
                "dex-server-zd exited with {status} under load:\n{}",
                process.log()
            );
        }
    }

    /// Returns a target of `count` more acknowledged calls and one more increment per counter.
    pub(crate) fn progress_target(&self, count: u64) -> ProgressTarget {
        ProgressTarget {
            acknowledged_count: self.acknowledged_count() + count,
            counter_increment_counts: self
                .ledger
                .counters()
                .into_iter()
                .map(|(flow_id, counter)| (flow_id, counter.acknowledged_values.len() + 1))
                .collect(),
        }
    }

    /// Waits until the load reaches `target`, or returns the exit status if `process` exits.
    pub(crate) async fn wait_for_progress_or_exit(
        &self,
        target: &ProgressTarget,
        process: &mut RobustnessProcess,
    ) -> Result<(), ExitStatus> {
        let deadline = Instant::now() + PROGRESS_BUDGET;
        while !self.has_reached(target) {
            if let Some(status) = process.exit_status() {
                return Err(status);
            }
            assert!(
                Instant::now() < deadline,
                "the load did not reach {target:?} within {PROGRESS_BUDGET:?}:\n{}",
                summarize(&self.ledger, &self.journal)
            );
            tokio::time::sleep(PROGRESS_POLL_INTERVAL).await;
        }
        Ok(())
    }

    fn has_reached(&self, target: &ProgressTarget) -> bool {
        if self.acknowledged_count() < target.acknowledged_count {
            return false;
        }
        let counters = self.ledger.counters();
        target
            .counter_increment_counts
            .iter()
            .all(|(flow_id, increment_count)| {
                counters
                    .get(flow_id)
                    .is_some_and(|counter| counter.acknowledged_values.len() >= *increment_count)
            })
    }

    /// Panics unless every acknowledged write is on the server behind `channel` and matches the journal.
    ///
    /// Panics with the server log as soon as `server` exits.
    pub(crate) async fn verify(
        &self,
        channel: Channel,
        server: &mut RobustnessProcess,
    ) -> VerificationReport {
        let mut client = FlowServiceClient::new(channel);
        let deadline = Instant::now() + VERIFICATION_BUDGET;
        let starts = self.ledger.starts();
        let mut chain_flows = 0;
        let mut final_counter_values = BTreeMap::new();
        for start in &starts {
            match start.kind {
                WorkloadFlowKind::Chain => {
                    self.wait_for_completion(
                        &mut client,
                        server,
                        start,
                        LAST_CHAIN_INDEX,
                        deadline,
                    )
                    .await;
                    chain_flows += 1;
                }
                WorkloadFlowKind::Counter => {
                    let final_value = self
                        .finish_counter(&mut client, server, start, deadline)
                        .await;
                    final_counter_values.insert(start.flow_id.clone(), final_value);
                }
            }
        }
        let (chain_step_executions, re_executed_step_executions) =
            self.verify_chain_journal(&starts);
        let (acknowledged_increments, unknown_increments) =
            self.verify_counters(&starts, &final_counter_values);
        VerificationReport {
            chain_flows,
            counter_flows: final_counter_values.len(),
            acknowledged_increments,
            unknown_increments,
            chain_step_executions,
            re_executed_step_executions,
        }
    }

    /// Waits until the first acknowledged chain Flow completes on `server` and returns it.
    pub(crate) async fn wait_for_first_completed_chain(
        &self,
        channel: Channel,
        server: &mut RobustnessProcess,
    ) -> CompletedChain {
        let start = self
            .ledger
            .starts()
            .into_iter()
            .find(|start| start.kind == WorkloadFlowKind::Chain)
            .expect("the load acknowledged a chain start");
        self.verify_completed_chain(channel, server, &CompletedChain(start.clone()))
            .await;
        CompletedChain(start)
    }

    /// Panics unless `chain` is Completed with its last index on its acknowledged run.
    pub(crate) async fn verify_completed_chain(
        &self,
        channel: Channel,
        server: &mut RobustnessProcess,
        chain: &CompletedChain,
    ) {
        let mut client = FlowServiceClient::new(channel);
        let deadline = Instant::now() + VERIFICATION_BUDGET;
        self.wait_for_completion(&mut client, server, &chain.0, LAST_CHAIN_INDEX, deadline)
            .await;
    }

    /// Completes a counter Flow with its locked value through FinishCounter and returns that value.
    async fn finish_counter(
        &self,
        client: &mut FlowServiceClient<Channel>,
        server: &mut RobustnessProcess,
        start: &AcknowledgedStart,
        deadline: Instant,
    ) -> i64 {
        // The reused request ID replays the reply of an attempt whose response was lost.
        let request_id = self
            .ledger
            .new_request_id(&format!("{}-finish", start.flow_id));
        let final_value = loop {
            assert_still_serving(server);
            let call = client.invoke_rpc(InvokeRpcRequest {
                flow_id: start.flow_id.clone(),
                run_id: start.run_id.clone(),
                rpc_name: FINISH_COUNTER_RPC.to_owned(),
                timeout_seconds: RPC_TIMEOUT_SECONDS,
                lock_attribute_keys: vec![COUNTER_ATTRIBUTE.to_owned()],
                request_id: request_id.clone(),
                ..InvokeRpcRequest::default()
            });
            let failure = match tokio::time::timeout(VERIFICATION_CALL_TIMEOUT, call).await {
                Ok(Ok(response)) => break integer_of(response.into_inner().output.as_ref()),
                Ok(Err(status)) => status.to_string(),
                Err(_) => format!("no reply within {VERIFICATION_CALL_TIMEOUT:?}"),
            };
            assert!(
                Instant::now() < deadline,
                "FinishCounter for acknowledged {} run {} failed until {VERIFICATION_BUDGET:?} passed: {failure}",
                start.flow_id,
                start.run_id
            );
            tokio::time::sleep(RETRY_PAUSE).await;
        };
        self.wait_for_completion(client, server, start, final_value, deadline)
            .await;
        let stored_value = self.read_counter(client, server, start).await;
        assert_eq!(
            stored_value, final_value,
            "{} completed with {final_value} but stores {stored_value}",
            start.flow_id
        );
        final_value
    }

    /// Panics unless the acknowledged run completes with `expected_output` before `deadline`.
    async fn wait_for_completion(
        &self,
        client: &mut FlowServiceClient<Channel>,
        server: &mut RobustnessProcess,
        start: &AcknowledgedStart,
        expected_output: i64,
        deadline: Instant,
    ) {
        loop {
            assert_still_serving(server);
            let wait = client.wait_for_flow(WaitForFlowRequest {
                flow_id: start.flow_id.clone(),
                run_id: start.run_id.clone(),
                needs_results: true,
                wait_time_seconds: 2,
            });
            let outcome = tokio::time::timeout(VERIFICATION_CALL_TIMEOUT, wait)
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "WaitForFlow for {} did not return within {VERIFICATION_CALL_TIMEOUT:?}",
                        start.flow_id
                    )
                });
            match outcome {
                Ok(response) => {
                    let result = response.into_inner();
                    if result.flow_status == FlowStatus::Completed as i32 {
                        assert!(
                            result
                                .results
                                .iter()
                                .any(|output| output.completed_step_output
                                    == Some(int_value(expected_output))),
                            "{} completed without output {expected_output}: {result:?}",
                            start.flow_id
                        );
                        return;
                    }
                    assert_eq!(
                        result.flow_status,
                        FlowStatus::Running as i32,
                        "acknowledged {} closed without completing: {result:?}",
                        start.flow_id
                    );
                }
                // The server answers a Flow that is still running when the wait time ends this way.
                Err(status)
                    if status.code() == tonic::Code::DeadlineExceeded
                        && status.message() == WAIT_FOR_FLOW_TIMEOUT_MESSAGE => {}
                Err(status) => {
                    assert_still_serving(server);
                    panic!(
                        "WaitForFlow for acknowledged {} run {} failed: {status}\n{}",
                        start.flow_id,
                        start.run_id,
                        summarize(&self.ledger, &self.journal)
                    );
                }
            }
            assert!(
                Instant::now() < deadline,
                "acknowledged {} was still running after {VERIFICATION_BUDGET:?}:\n{}",
                start.flow_id,
                summarize(&self.ledger, &self.journal)
            );
        }
    }

    async fn read_counter(
        &self,
        client: &mut FlowServiceClient<Channel>,
        server: &mut RobustnessProcess,
        start: &AcknowledgedStart,
    ) -> i64 {
        assert_still_serving(server);
        let read = client.get_attributes(GetAttributesRequest {
            flow_id: start.flow_id.clone(),
            run_id: start.run_id.clone(),
            keys: vec![COUNTER_ATTRIBUTE.to_owned()],
            all_keys: false,
        });
        let attributes = tokio::time::timeout(VERIFICATION_CALL_TIMEOUT, read)
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "GetAttributes for {} did not return within {VERIFICATION_CALL_TIMEOUT:?}",
                    start.flow_id
                )
            })
            .unwrap_or_else(|status| {
                assert_still_serving(server);
                panic!(
                    "GetAttributes for acknowledged {} run {} failed: {status}",
                    start.flow_id, start.run_id
                )
            })
            .into_inner()
            .attributes;
        attributes
            .iter()
            .find(|attribute| attribute.key == COUNTER_ATTRIBUTE)
            .map_or(0, |attribute| integer_of(attribute.value.as_ref()))
    }

    /// Returns the number of chain Step executions and of those that ran more than once.
    fn verify_chain_journal(&self, starts: &[AcknowledgedStart]) -> (usize, usize) {
        let acknowledged_runs: BTreeMap<&str, &str> = starts
            .iter()
            .filter(|start| start.kind == WorkloadFlowKind::Chain)
            .map(|start| (start.flow_id.as_str(), start.run_id.as_str()))
            .collect();
        let executions = self
            .journal
            .chain_steps
            .lock()
            .expect("the journal lock is not poisoned")
            .clone();
        let mut step_execution_ids: BTreeMap<(&str, i64), BTreeSet<&str>> = BTreeMap::new();
        let mut handler_calls: BTreeMap<(&str, &str), usize> = BTreeMap::new();
        for execution in &executions {
            let Some(run_id) = acknowledged_runs.get(execution.flow_id.as_str()) else {
                continue;
            };
            assert_eq!(
                execution.run_id, *run_id,
                "a handler saw a run other than the acknowledged one: {execution:?}"
            );
            assert!(
                (0..=LAST_CHAIN_INDEX).contains(&execution.index),
                "unexpected chain index: {execution:?}"
            );
            step_execution_ids
                .entry((execution.flow_id.as_str(), execution.index))
                .or_default()
                .insert(execution.step_execution_id.as_str());
            *handler_calls
                .entry((
                    execution.flow_id.as_str(),
                    execution.step_execution_id.as_str(),
                ))
                .or_default() += 1;
        }
        for flow_id in acknowledged_runs.keys() {
            for index in 0..=LAST_CHAIN_INDEX {
                let ids = step_execution_ids.get(&(*flow_id, index));
                assert_eq!(
                    ids.map_or(0, BTreeSet::len),
                    1,
                    "{flow_id} index {index} must run under exactly one StepExecutionID: {ids:?}"
                );
            }
        }
        let re_executed = handler_calls.values().filter(|calls| **calls > 1).count();
        (handler_calls.len(), re_executed)
    }

    /// Returns the number of acknowledged and unknown-outcome increments.
    fn verify_counters(
        &self,
        starts: &[AcknowledgedStart],
        final_values: &BTreeMap<String, i64>,
    ) -> (usize, u64) {
        let increments = self
            .journal
            .counter_increments
            .lock()
            .expect("the journal lock is not poisoned")
            .clone();
        let mut acknowledged_total = 0;
        let mut unknown_total = 0;
        for (flow_id, counter) in self.ledger.counters() {
            let run_id = &starts
                .iter()
                .find(|start| start.flow_id == flow_id)
                .expect("every counter was acknowledged")
                .run_id;
            let final_value = final_values[&flow_id];
            let handled: BTreeSet<i64> = increments
                .iter()
                .filter(|increment| increment.flow_id == flow_id)
                .map(|increment| {
                    assert_eq!(
                        &increment.run_id, run_id,
                        "a handler saw a run other than the acknowledged one: {increment:?}"
                    );
                    increment.value
                })
                .collect();
            let acknowledged: BTreeSet<i64> = counter.acknowledged_values.iter().copied().collect();
            assert!(
                !acknowledged.is_empty(),
                "{flow_id} has no acknowledged increment"
            );
            assert_eq!(
                acknowledged.len(),
                counter.acknowledged_values.len(),
                "{flow_id} acknowledged a value twice, so an update was lost: {:?}",
                counter.acknowledged_values
            );
            let largest = acknowledged.last().copied().unwrap_or(0);
            assert!(
                final_value >= largest,
                "{flow_id} ended at {final_value}, below the acknowledged {largest}"
            );
            let excess = u64::try_from(final_value - counter.acknowledged_values.len() as i64)
                .expect("unique positive values cannot outnumber the final value");
            assert!(
                excess <= counter.unknown_outcomes,
                "{flow_id} ended at {final_value} after {} acknowledged and {} unknown increments",
                counter.acknowledged_values.len(),
                counter.unknown_outcomes
            );
            assert!(
                acknowledged.is_subset(&handled),
                "{flow_id} acknowledged values no Worker returned: {:?}",
                acknowledged.difference(&handled).collect::<Vec<_>>()
            );
            acknowledged_total += counter.acknowledged_values.len();
            unknown_total += counter.unknown_outcomes;
        }
        (acknowledged_total, unknown_total)
    }
}

/// Load tasks that run until [`RunningLoad::stop`].
pub(crate) struct RunningLoad {
    is_stopping: Arc<AtomicBool>,
    tasks: Vec<JoinHandle<()>>,
}

impl RunningLoad {
    /// Stops issuing calls and waits for each in-flight call to finish or time out.
    pub(crate) async fn stop(self) {
        self.is_stopping.store(true, Ordering::SeqCst);
        for task in self.tasks {
            task.await.expect("a load task does not panic");
        }
    }
}

/// Starts one chain Flow at a time and waits for it to close before starting the next.
async fn run_chain_starter(
    mut client: FlowServiceClient<Channel>,
    ledger: Arc<ClientLedger>,
    is_stopping: Arc<AtomicBool>,
    flow_id_prefix: String,
    call_timeout: Duration,
) {
    let mut sequence = 0;
    while !is_stopping.load(Ordering::SeqCst) {
        let flow_id = format!("{flow_id_prefix}-{sequence}");
        sequence += 1;
        let request = start_request(
            &flow_id,
            CHAIN_FLOW_TYPE,
            CHAIN_STEP_TYPE,
            int_value(0),
            &ledger,
        );
        let Some(run_id) =
            start_until_acknowledged(&mut client, &request, &ledger, &is_stopping, call_timeout)
                .await
        else {
            return;
        };
        ledger.record_start(&flow_id, &run_id, WorkloadFlowKind::Chain);
        while !is_stopping.load(Ordering::SeqCst) {
            let wait = client.wait_for_flow(WaitForFlowRequest {
                flow_id: flow_id.clone(),
                run_id: run_id.clone(),
                needs_results: false,
                wait_time_seconds: 1,
            });
            match tokio::time::timeout(call_timeout, wait).await {
                Ok(Ok(result)) if result.get_ref().flow_status != FlowStatus::Running as i32 => {
                    break;
                }
                Ok(Ok(_)) => {}
                Ok(Err(_)) | Err(_) => tokio::time::sleep(RETRY_PAUSE).await,
            }
        }
    }
}

/// Increments one counter sequentially; a failed call is an unknown outcome and is not retried.
async fn run_counter_writer(
    mut client: FlowServiceClient<Channel>,
    ledger: Arc<ClientLedger>,
    is_stopping: Arc<AtomicBool>,
    flow_id: String,
    call_timeout: Duration,
) {
    while !is_stopping.load(Ordering::SeqCst) {
        let call = client.invoke_rpc(InvokeRpcRequest {
            flow_id: flow_id.clone(),
            rpc_name: INCREMENT_COUNTER_RPC.to_owned(),
            timeout_seconds: RPC_TIMEOUT_SECONDS,
            lock_attribute_keys: vec![COUNTER_ATTRIBUTE.to_owned()],
            request_id: ledger.new_request_id(&flow_id),
            ..InvokeRpcRequest::default()
        });
        match tokio::time::timeout(call_timeout, call).await {
            Ok(Ok(response)) => {
                let value = integer_of(response.into_inner().output.as_ref());
                ledger.record_increment(&flow_id, value);
            }
            Ok(Err(status)) => {
                ledger.record_unknown_increment(&flow_id, format!("{flow_id} increment: {status}"));
                tokio::time::sleep(RETRY_PAUSE).await;
            }
            Err(_) => {
                ledger.record_unknown_increment(&flow_id, format!("{flow_id} increment timed out"));
            }
        }
    }
}

/// Repeats `request` until acknowledged and returns its run ID, or `None` on stop.
///
/// The reused request ID returns the run of an attempt whose response was lost.
async fn start_until_acknowledged(
    client: &mut FlowServiceClient<Channel>,
    request: &StartFlowRequest,
    ledger: &ClientLedger,
    is_stopping: &AtomicBool,
    call_timeout: Duration,
) -> Option<String> {
    while !is_stopping.load(Ordering::SeqCst) {
        match tokio::time::timeout(call_timeout, client.start_flow(request.clone())).await {
            Ok(Ok(response)) => return Some(response.into_inner().run_id),
            Ok(Err(status)) => {
                ledger.record_failed_start(format!("StartFlow {}: {status}", request.flow_id));
            }
            Err(_) => {
                ledger.record_failed_start(format!("StartFlow {} timed out", request.flow_id));
            }
        }
        tokio::time::sleep(RETRY_PAUSE).await;
    }
    None
}

fn start_request(
    flow_id: &str,
    flow_type: &str,
    start_step_type: &str,
    step_input: Value,
    ledger: &ClientLedger,
) -> StartFlowRequest {
    StartFlowRequest {
        flow_id: flow_id.to_owned(),
        flow_type: flow_type.to_owned(),
        start_step_type: start_step_type.to_owned(),
        step_input: Some(step_input),
        step_options: Some(skipped_wait_for()),
        flow_start_options: Some(FlowStartOptions {
            flow_config_override: Some(FlowConfig {
                worker_target: Some(WorkerTarget {
                    is_pull_mode: true,
                    ..WorkerTarget::default()
                }),
                ..FlowConfig::default()
            }),
            flow_already_started_options: Some(FlowAlreadyStartedOptions {
                ignore_already_started_error: true,
            }),
            ..FlowStartOptions::default()
        }),
        request_id: ledger.new_request_id(flow_id),
        ..StartFlowRequest::default()
    }
}

fn lazy_channel(address: SocketAddr) -> Channel {
    Channel::from_shared(format!("http://{address}"))
        .expect("the server address is a valid URI")
        .connect_lazy()
}

/// A pull Worker for both workload Flow types; it ends when its server stops.
pub(crate) struct WorkloadWorker {
    streams: Vec<WorkerStream>,
}

struct WorkerStream {
    frames: mpsc::UnboundedSender<PollWorkerTaskRequest>,
    task: JoinHandle<()>,
}

impl WorkloadWorker {
    /// Registers on the Worker ID queue for sticky tasks and on each Flow type queue.
    async fn start(channel: Channel, journal: Arc<WorkerJournal>) -> Result<Self, String> {
        let mut worker = Self {
            streams: Vec::new(),
        };
        for queue in [WORKER_ID, CHAIN_FLOW_TYPE, COUNTER_FLOW_TYPE] {
            for _ in 0..STREAMS_PER_QUEUE {
                match WorkerStream::open(channel.clone(), queue, journal.clone()).await {
                    Ok(stream) => worker.streams.push(stream),
                    Err(error) => {
                        worker.stop().await;
                        return Err(format!("{queue}: {error}"));
                    }
                }
            }
        }
        Ok(worker)
    }

    /// Closes every stream; fails if a handler panicked.
    pub(crate) async fn stop(self) {
        for stream in self.streams {
            drop(stream.frames);
            stream.task.abort();
            if let Err(error) = stream.task.await {
                assert!(error.is_cancelled(), "a Worker stream panicked: {error}");
            }
        }
    }
}

impl WorkerStream {
    async fn open(
        channel: Channel,
        queue: &str,
        journal: Arc<WorkerJournal>,
    ) -> Result<Self, String> {
        let (frames, outgoing) = mpsc::unbounded_channel();
        frames
            .send(PollWorkerTaskRequest {
                frame: Some(poll_worker_task_request::Frame::Register(
                    PollWorkerTaskRegister {
                        queue: queue.to_owned(),
                        app_worker_id: WORKER_ID.to_owned(),
                        flow_types: vec![CHAIN_FLOW_TYPE.to_owned(), COUNTER_FLOW_TYPE.to_owned()],
                        sdk_name: "dexzd-server-tests".to_owned(),
                        sdk_version: "0".to_owned(),
                    },
                )),
            })
            .expect("the outgoing stream is open");
        let mut client = WorkerTaskServiceClient::new(channel);
        let mut incoming = tokio::time::timeout(
            STREAM_OPEN_TIMEOUT,
            client.poll_worker_task(UnboundedReceiverStream::new(outgoing)),
        )
        .await
        .map_err(|_| format!("PollWorkerTask did not open within {STREAM_OPEN_TIMEOUT:?}"))?
        .map_err(|status| format!("PollWorkerTask failed: {status}"))?
        .into_inner();
        let first = tokio::time::timeout(STREAM_OPEN_TIMEOUT, incoming.next())
            .await
            .map_err(|_| format!("no registration answer within {STREAM_OPEN_TIMEOUT:?}"))?
            .ok_or("the stream ended before the registration answer")?
            .map_err(|status| format!("the registration failed: {status}"))?;
        assert!(
            matches!(
                first.frame,
                Some(poll_worker_task_response::Frame::Registered(_))
            ),
            "the first frame is registered, got {first:?}"
        );
        let replies = frames.clone();
        let task = tokio::spawn(async move {
            while let Some(Ok(response)) = incoming.next().await {
                let reply = match response.frame {
                    Some(poll_worker_task_response::Frame::Task(task)) => {
                        task_reply(task, &journal)
                    }
                    Some(poll_worker_task_response::Frame::Ping(_)) => PollWorkerTaskRequest {
                        frame: Some(poll_worker_task_request::Frame::Pong(Default::default())),
                    },
                    _ => continue,
                };
                if replies.send(reply).is_err() {
                    return;
                }
            }
        });
        Ok(Self { frames, task })
    }
}

fn task_reply(task: PollWorkerTask, journal: &WorkerJournal) -> PollWorkerTaskRequest {
    let output = match task.request.expect("a task carries a request") {
        poll_worker_task::Request::WaitFor(_) => {
            poll_worker_task_frame::Output::WaitFor(InvokeWaitForMethodOutput {
                output: Some(invoke_wait_for_method_output::Output::Result(
                    InvokeWaitForMethodResponse {
                        waiting_condition: Some(WaitingCondition::default()),
                        ..InvokeWaitForMethodResponse::default()
                    },
                )),
            })
        }
        poll_worker_task::Request::Execute(request) => {
            poll_worker_task_frame::Output::Execute(InvokeExecuteMethodOutput {
                output: Some(invoke_execute_method_output::Output::Result(execute(
                    &request, journal,
                ))),
            })
        }
        poll_worker_task::Request::Rpc(request) => {
            poll_worker_task_frame::Output::Rpc(match request.rpc_name.as_str() {
                INCREMENT_COUNTER_RPC => increment_counter(&request, journal),
                FINISH_COUNTER_RPC => schedule_counter_finish(&request),
                other => panic!("unexpected RPC {other}"),
            })
        }
    };
    PollWorkerTaskRequest {
        frame: Some(poll_worker_task_request::Frame::TaskFrame(
            PollWorkerTaskFrame {
                task_id: task.task_id,
                output: Some(output),
            },
        )),
    }
}

/// ChainStep moves to the next index or completes with the last; CounterStartStep ends its thread.
fn execute(
    request: &InvokeExecuteMethodRequest,
    journal: &WorkerJournal,
) -> InvokeExecuteMethodResponse {
    let context = request
        .context
        .as_ref()
        .expect("an Execute request carries its context");
    let step_decision = match request.step_type.as_str() {
        CHAIN_STEP_TYPE => {
            let index = integer_of(request.step_input.as_ref());
            journal
                .chain_steps
                .lock()
                .expect("the journal lock is not poisoned")
                .push(ChainStepExecution {
                    flow_id: context.flow_id.clone(),
                    run_id: context.run_id.clone(),
                    step_execution_id: context.step_execution_id.clone(),
                    attempt: context.attempt,
                    index,
                });
            if index >= LAST_CHAIN_INDEX {
                close_decision(CloseDecisionType::GracefulComplete, Some(int_value(index)))
            } else {
                StepDecision {
                    next_steps: vec![StepMovement {
                        step_type: CHAIN_STEP_TYPE.to_owned(),
                        step_input: Some(int_value(index + 1)),
                        step_options: Some(skipped_wait_for()),
                        ..StepMovement::default()
                    }],
                    ..StepDecision::default()
                }
            }
        }
        COUNTER_START_STEP_TYPE => close_decision(CloseDecisionType::DeadEnd, None),
        FINISH_COUNTER_STEP_TYPE => close_decision(
            CloseDecisionType::GracefulComplete,
            Some(int_value(integer_of(request.step_input.as_ref()))),
        ),
        other => panic!("unexpected Step type {other}"),
    };
    InvokeExecuteMethodResponse {
        step_decision: Some(step_decision),
        ..InvokeExecuteMethodResponse::default()
    }
}

/// Writes and returns the locked counter plus one.
fn increment_counter(
    request: &InvokeWorkerRpcRequest,
    journal: &WorkerJournal,
) -> InvokeWorkerRpcResponse {
    assert_eq!(request.rpc_name, INCREMENT_COUNTER_RPC);
    let context = request
        .context
        .as_ref()
        .expect("an RPC request carries its context");
    let current = locked_counter_value(request);
    let next = current + 1;
    journal
        .counter_increments
        .lock()
        .expect("the journal lock is not poisoned")
        .push(CounterIncrement {
            flow_id: context.flow_id.clone(),
            run_id: context.run_id.clone(),
            value: next,
        });
    InvokeWorkerRpcResponse {
        output: Some(int_value(next)),
        upsert_attributes: vec![AttributeWrite {
            key: COUNTER_ATTRIBUTE.to_owned(),
            value: Some(int_value(next)),
            ..AttributeWrite::default()
        }],
        ..InvokeWorkerRpcResponse::default()
    }
}

/// Returns the locked counter and starts FinishCounterStep, which completes the Flow with it.
fn schedule_counter_finish(request: &InvokeWorkerRpcRequest) -> InvokeWorkerRpcResponse {
    let value = locked_counter_value(request);
    InvokeWorkerRpcResponse {
        output: Some(int_value(value)),
        step_decision: Some(StepDecision {
            next_steps: vec![StepMovement {
                step_type: FINISH_COUNTER_STEP_TYPE.to_owned(),
                step_input: Some(int_value(value)),
                step_options: Some(skipped_wait_for()),
                ..StepMovement::default()
            }],
            ..StepDecision::default()
        }),
        ..InvokeWorkerRpcResponse::default()
    }
}

/// Returns the counter value an RPC request loaded, or 0 before the first increment.
fn locked_counter_value(request: &InvokeWorkerRpcRequest) -> i64 {
    request
        .attributes
        .iter()
        .find(|attribute| attribute.key == COUNTER_ATTRIBUTE)
        .map_or(0, |attribute| integer_of(attribute.value.as_ref()))
}

fn close_decision(
    close_decision_type: CloseDecisionType,
    close_input: Option<Value>,
) -> StepDecision {
    StepDecision {
        close_decision: Some(CloseDecision {
            close_decision_type: close_decision_type.into(),
            close_input,
            ..CloseDecision::default()
        }),
        ..StepDecision::default()
    }
}

fn skipped_wait_for() -> StepOptions {
    StepOptions {
        skip_wait_for: true,
        ..StepOptions::default()
    }
}

fn int_value(number: i64) -> Value {
    Value {
        kind: Some(value::Kind::IntValue(number)),
    }
}

fn integer_of(value: Option<&Value>) -> i64 {
    match value.and_then(|value| value.kind.as_ref()) {
        Some(value::Kind::IntValue(number)) => *number,
        other => panic!("expected an integer value, got {other:?}"),
    }
}

/// Panics with the server log if `server` has exited.
fn assert_still_serving(server: &mut RobustnessProcess) {
    if let Some(status) = server.exit_status() {
        panic!(
            "dex-server-zd exited with {status} during verification:\n{}",
            server.log()
        );
    }
}

fn summarize(ledger: &ClientLedger, journal: &WorkerJournal) -> String {
    let starts = ledger.starts();
    let chain_starts = starts
        .iter()
        .filter(|start| start.kind == WorkloadFlowKind::Chain)
        .count();
    let mut summary = format!(
        "acknowledged calls: {}; acknowledged chain starts: {chain_starts}; failed start attempts: {}\n",
        ledger.acknowledged_count.load(Ordering::SeqCst),
        ledger.failed_start_attempts.load(Ordering::SeqCst)
    );
    for (flow_id, counter) in ledger.counters() {
        summary.push_str(&format!(
            "{flow_id}: {} acknowledged (largest {:?}), {} unknown\n",
            counter.acknowledged_values.len(),
            counter.acknowledged_values.iter().max(),
            counter.unknown_outcomes
        ));
    }
    let chain_steps = journal
        .chain_steps
        .lock()
        .expect("the journal lock is not poisoned");
    let counter_increments = journal
        .counter_increments
        .lock()
        .expect("the journal lock is not poisoned");
    summary.push_str(&format!(
        "journal: {} chain Execute calls (repeated attempts: {}), {} increment calls\n",
        chain_steps.len(),
        chain_steps
            .iter()
            .filter(|execution| execution.attempt > 1)
            .count(),
        counter_increments.len()
    ));
    summary.push_str("recent failures:\n");
    for failure in ledger
        .recent_failures
        .lock()
        .expect("the ledger lock is not poisoned")
        .iter()
    {
        summary.push_str(&format!("  {failure}\n"));
    }
    summary
}
