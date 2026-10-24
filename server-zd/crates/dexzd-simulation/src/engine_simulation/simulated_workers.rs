// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Seeded pull Workers behind the engine's dispatch ports, without the matching layer.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dexzd_engine::{
    MethodDispatchRequest, MethodDispatchSession, MethodDispatchStartError, MethodDispatcher,
    MethodRequest, RpcInvokeError, RpcWorkerInvoker, WorkerRpcInvocation, WorkerRpcResult,
};
use dexzd_protocol::dex::{
    Context, DispatchTaskErrorReason, InvokeExecuteMethodOutput, InvokeWaitForMethodOutput,
    PollWorkerTaskFrame, StepMethodHeartbeat, invoke_execute_method_output,
    invoke_wait_for_method_output, poll_worker_task_frame,
};
use dexzd_worker_dispatch::{DispatchEvent, DispatchOutcome};
use futures::future::BoxFuture;
use rand::{RngExt, SeedableRng};
use rand_chacha::ChaCha8Rng;
use tokio::sync::{mpsc, watch};

use crate::engine_simulation::event_trace::EventTrace;
use crate::engine_simulation::workload::{
    MethodCallView, execute_response, heartbeat_value, rpc_response, wait_for_response,
    worker_error,
};

/// New Steps each run's Workers may create, so every run eventually closes.
const STEPS_PER_RUN: u32 = 8;

/// One method attempt as the Worker sees it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct AttemptIdentity {
    pub(crate) flow_id: String,
    pub(crate) run_id: String,
    pub(crate) run_attempt: i32,
    pub(crate) step_execution_id: String,
    pub(crate) is_execute: bool,
    pub(crate) attempt: i32,
}

impl AttemptIdentity {
    fn of(request: &MethodRequest) -> Self {
        let (context, is_execute) = match request {
            MethodRequest::WaitFor(request) => (request.context.as_ref(), false),
            MethodRequest::Execute(request) => (request.context.as_ref(), true),
        };
        let context = context.cloned().unwrap_or_else(Context::default);
        Self {
            flow_id: context.flow_id,
            run_id: context.run_id,
            run_attempt: context.run_attempt,
            step_execution_id: context.step_execution_id,
            is_execute,
            attempt: context.attempt,
        }
    }
}

/// How a Worker handles one dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum WorkerBehavior {
    Succeed,
    Fail,
    /// Never answers; the method or heartbeat timeout ends the attempt.
    Hang,
    /// Never answers and sends no heartbeat, while the simulator compares dispatches.
    Hold,
    Slow,
    /// Returns the result it already returned for the same attempt.
    RepeatResult,
    DisconnectBeforeOutput,
    DisconnectAfterOutput,
    NoWorker,
    RpcSucceed,
    RpcFail,
    RpcTimeout,
    /// A heartbeat frame that carries a checkpoint value.
    HeartbeatWithValue,
    /// A heartbeat frame without a value, which clears the checkpoint.
    HeartbeatWithoutValue,
}

/// One timed event of a scripted dispatch; `None` waits until the dispatch is canceled.
struct ScriptedEvent {
    /// Whether the delay starts only once the pool stops holding.
    waits_for_release: bool,
    delay: Duration,
    event: Option<DispatchEvent>,
}

struct WorkerPoolState {
    rng: ChaCha8Rng,
    is_calm: bool,
    is_holding: bool,
    dispatch_history: BTreeMap<String, Vec<AttemptIdentity>>,
    returned_results: BTreeMap<AttemptIdentity, PollWorkerTaskFrame>,
    steps_left: BTreeMap<(String, String), u32>,
    behaviors: BTreeMap<WorkerBehavior, u64>,
}

/// The Worker pool every node process dispatches to.
pub(crate) struct SimulatedWorkers {
    state: Mutex<WorkerPoolState>,
    /// True while held Workers keep their tasks; they answer once it turns false.
    hold: watch::Sender<bool>,
    trace: Arc<EventTrace>,
}

impl SimulatedWorkers {
    pub(crate) fn new(seed: u64, trace: Arc<EventTrace>) -> Self {
        Self {
            state: Mutex::new(WorkerPoolState {
                rng: ChaCha8Rng::seed_from_u64(seed),
                is_calm: false,
                is_holding: false,
                dispatch_history: BTreeMap::new(),
                returned_results: BTreeMap::new(),
                steps_left: BTreeMap::new(),
                behaviors: BTreeMap::new(),
            }),
            hold: watch::Sender::new(false),
            trace,
        }
    }

    /// In calm mode every Worker answers promptly and successfully.
    pub(crate) fn set_calm(&self, is_calm: bool) {
        self.lock().is_calm = is_calm;
    }

    /// While holding, new tasks get Workers that answer only after the hold ends.
    pub(crate) fn set_holding(&self, is_holding: bool) {
        self.lock().is_holding = is_holding;
        self.hold.send_replace(is_holding);
    }

    /// Returns every attempt `process_label` dispatched, in dispatch order.
    pub(crate) fn dispatch_history(&self, process_label: &str) -> Vec<AttemptIdentity> {
        self.lock()
            .dispatch_history
            .get(process_label)
            .cloned()
            .unwrap_or_default()
    }

    pub(crate) fn behavior_counts(&self) -> BTreeMap<WorkerBehavior, u64> {
        self.lock().behaviors.clone()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, WorkerPoolState> {
        self.state.lock().expect("worker pool lock is not poisoned")
    }

    /// Returns the dispatch port of one node process.
    pub(crate) fn dispatcher_for(self: &Arc<Self>, process_label: &str) -> Arc<NodeWorkerPort> {
        Arc::new(NodeWorkerPort {
            workers: Arc::clone(self),
            process_label: process_label.to_owned(),
        })
    }

    fn start_dispatch(
        self: &Arc<Self>,
        process_label: &str,
        request: &MethodRequest,
    ) -> Result<Box<dyn MethodDispatchSession>, MethodDispatchStartError> {
        let identity = AttemptIdentity::of(request);
        let mut state = self.lock();
        let behavior = state.draw_behavior(&identity);
        *state.behaviors.entry(behavior).or_default() += 1;
        state
            .dispatch_history
            .entry(process_label.to_owned())
            .or_default()
            .push(identity.clone());
        self.trace.record(&format!(
            "{process_label} dispatch {} run {} step {} execute={} attempt {} {behavior:?}",
            identity.flow_id,
            identity.run_id,
            identity.step_execution_id,
            identity.is_execute,
            identity.attempt
        ));
        if behavior == WorkerBehavior::NoWorker && state.rng.random_bool(0.5) {
            return Err(MethodDispatchStartError {
                detail: "no simulated Worker polls this Flow type".to_owned(),
            });
        }
        let script = state.script(behavior, &identity, request);
        drop(state);
        let (event_sender, event_receiver) = mpsc::channel(script.len().max(1));
        let (cancel_sender, cancel_receiver) = watch::channel(false);
        let worker_task = tokio::spawn(play_script(
            script,
            event_sender,
            cancel_receiver,
            self.hold.subscribe(),
        ));
        // The session owns the task: dropping the session ends the script.
        Ok(Box::new(SimulatedDispatchSession {
            events: event_receiver,
            cancel: cancel_sender,
            worker_task,
        }))
    }

    async fn invoke_rpc(
        &self,
        process_label: &str,
        invocation: WorkerRpcInvocation,
    ) -> Result<WorkerRpcResult, RpcInvokeError> {
        let (behavior, delay, outcome) = {
            let mut state = self.lock();
            let rng = &mut state.rng;
            let behavior = match rng.random_range(0..20) {
                _ if state.is_calm => WorkerBehavior::RpcSucceed,
                0..=1 => WorkerBehavior::RpcFail,
                2 => WorkerBehavior::RpcTimeout,
                _ => WorkerBehavior::RpcSucceed,
            };
            let rng = &mut state.rng;
            let delay = Duration::from_millis(rng.random_range(1..500));
            let outcome = match behavior {
                WorkerBehavior::RpcFail => Err(RpcInvokeError::WorkerError {
                    response: worker_error(rng),
                }),
                WorkerBehavior::RpcTimeout => Err(RpcInvokeError::DeadlineExceeded),
                _ => Ok(WorkerRpcResult {
                    response: rpc_response(rng),
                    app_worker_id: format!("worker-{}", rng.random_range(0..3)),
                    pull_routing: None,
                }),
            };
            *state.behaviors.entry(behavior).or_default() += 1;
            (behavior, delay, outcome)
        };
        self.trace.record(&format!(
            "{process_label} rpc {} run {} {behavior:?}",
            invocation.flow.flow_id, invocation.run_id
        ));
        if behavior == WorkerBehavior::RpcTimeout {
            tokio::time::sleep_until(invocation.deadline).await;
            return outcome;
        }
        tokio::time::sleep(delay).await;
        outcome
    }
}

impl WorkerPoolState {
    fn draw_behavior(&mut self, identity: &AttemptIdentity) -> WorkerBehavior {
        if self.is_holding {
            return WorkerBehavior::Hold;
        }
        if self.is_calm {
            return WorkerBehavior::Succeed;
        }
        let has_result = self.returned_results.contains_key(identity);
        match self.rng.random_range(0..100) {
            0..=54 => WorkerBehavior::Succeed,
            55..=64 => WorkerBehavior::Fail,
            65..=69 => WorkerBehavior::Hang,
            70..=77 => WorkerBehavior::Slow,
            78..=84 if has_result => WorkerBehavior::RepeatResult,
            78..=84 => WorkerBehavior::Succeed,
            85..=90 => WorkerBehavior::DisconnectBeforeOutput,
            91..=96 => WorkerBehavior::DisconnectAfterOutput,
            _ => WorkerBehavior::NoWorker,
        }
    }

    fn script(
        &mut self,
        behavior: WorkerBehavior,
        identity: &AttemptIdentity,
        request: &MethodRequest,
    ) -> Vec<ScriptedEvent> {
        let worker_id = format!("worker-{}", self.rng.random_range(0..3));
        let assigned = ScriptedEvent {
            waits_for_release: false,
            delay: Duration::from_millis(self.rng.random_range(1..200)),
            event: Some(DispatchEvent::Assigned {
                app_worker_id: worker_id.clone(),
            }),
        };
        let mut script = Vec::new();
        match behavior {
            WorkerBehavior::Succeed | WorkerBehavior::Slow => {
                script.push(assigned);
                let is_slow = behavior == WorkerBehavior::Slow;
                self.push_heartbeats(&mut script, request, is_slow);
                let delay = if is_slow {
                    Duration::from_millis(self.rng.random_range(2_000..25_000))
                } else {
                    Duration::from_millis(self.rng.random_range(10..1_500))
                };
                script.push(self.result_event(delay, identity, request, worker_id));
            }
            WorkerBehavior::RepeatResult => {
                script.push(assigned);
                let frame = self
                    .returned_results
                    .get(identity)
                    .cloned()
                    .expect("RepeatResult is drawn only for attempts with a returned result");
                script.push(ScriptedEvent {
                    waits_for_release: false,
                    delay: Duration::from_millis(self.rng.random_range(1..300)),
                    event: Some(DispatchEvent::Completed(DispatchOutcome::WorkerResult {
                        frame,
                        app_worker_id: worker_id,
                    })),
                });
            }
            WorkerBehavior::Fail => {
                script.push(assigned);
                script.push(ScriptedEvent {
                    waits_for_release: false,
                    delay: Duration::from_millis(self.rng.random_range(10..1_000)),
                    event: Some(DispatchEvent::Completed(DispatchOutcome::WorkerError {
                        frame: PollWorkerTaskFrame {
                            task_id: String::new(),
                            output: Some(poll_worker_task_frame::Output::WorkerError(
                                worker_error(&mut self.rng),
                            )),
                        },
                        app_worker_id: worker_id,
                    })),
                });
            }
            WorkerBehavior::Hang => {
                script.push(assigned);
                self.push_heartbeats(&mut script, request, false);
                script.push(ScriptedEvent {
                    waits_for_release: false,
                    delay: Duration::ZERO,
                    event: None,
                });
            }
            WorkerBehavior::Hold => {
                script.push(assigned);
                let delay = Duration::from_millis(self.rng.random_range(10..500));
                let mut result = self.result_event(delay, identity, request, worker_id);
                result.waits_for_release = true;
                script.push(result);
            }
            WorkerBehavior::DisconnectBeforeOutput => {
                script.push(assigned);
                script.push(ScriptedEvent {
                    waits_for_release: false,
                    delay: Duration::from_millis(self.rng.random_range(10..2_000)),
                    event: Some(DispatchEvent::Unassigned),
                });
                let other_worker = format!("worker-{}", self.rng.random_range(3..6));
                script.push(ScriptedEvent {
                    waits_for_release: false,
                    delay: Duration::from_millis(self.rng.random_range(10..500)),
                    event: Some(DispatchEvent::Assigned {
                        app_worker_id: other_worker.clone(),
                    }),
                });
                let delay = Duration::from_millis(self.rng.random_range(10..1_500));
                script.push(self.result_event(delay, identity, request, other_worker));
            }
            WorkerBehavior::DisconnectAfterOutput => {
                script.push(assigned);
                self.push_heartbeats(&mut script, request, false);
                script.push(ScriptedEvent {
                    waits_for_release: false,
                    delay: Duration::from_millis(self.rng.random_range(10..1_000)),
                    event: Some(DispatchEvent::Completed(DispatchOutcome::DispatchError {
                        reason: DispatchTaskErrorReason::WorkerDisconnected,
                        detail: "simulated Worker disconnected".to_owned(),
                    })),
                });
            }
            WorkerBehavior::NoWorker
            | WorkerBehavior::RpcSucceed
            | WorkerBehavior::RpcFail
            | WorkerBehavior::RpcTimeout
            | WorkerBehavior::HeartbeatWithValue
            | WorkerBehavior::HeartbeatWithoutValue => {
                script.push(ScriptedEvent {
                    waits_for_release: false,
                    delay: Duration::from_millis(self.rng.random_range(10..1_000)),
                    event: Some(DispatchEvent::Completed(DispatchOutcome::DispatchError {
                        reason: DispatchTaskErrorReason::NoWorkerAvailable,
                        detail: "no simulated Worker is available".to_owned(),
                    })),
                });
            }
        }
        script
    }

    fn push_heartbeats(
        &mut self,
        script: &mut Vec<ScriptedEvent>,
        request: &MethodRequest,
        is_slow: bool,
    ) {
        if is_slow {
            return;
        }
        for _ in 0..self.rng.random_range(0..3) {
            let value = heartbeat_value(&mut self.rng);
            let kind = if value.is_some() {
                WorkerBehavior::HeartbeatWithValue
            } else {
                WorkerBehavior::HeartbeatWithoutValue
            };
            *self.behaviors.entry(kind).or_default() += 1;
            let output = match request {
                MethodRequest::WaitFor(_) => {
                    poll_worker_task_frame::Output::WaitFor(InvokeWaitForMethodOutput {
                        output: Some(invoke_wait_for_method_output::Output::Heartbeat(
                            StepMethodHeartbeat { value },
                        )),
                    })
                }
                MethodRequest::Execute(_) => {
                    poll_worker_task_frame::Output::Execute(InvokeExecuteMethodOutput {
                        output: Some(invoke_execute_method_output::Output::Heartbeat(
                            StepMethodHeartbeat { value },
                        )),
                    })
                }
            };
            script.push(ScriptedEvent {
                waits_for_release: false,
                delay: Duration::from_millis(self.rng.random_range(100..2_000)),
                event: Some(DispatchEvent::Output(PollWorkerTaskFrame {
                    task_id: String::new(),
                    output: Some(output),
                })),
            });
        }
    }

    /// Builds the final result now, so the draws do not depend on timing, and remembers it.
    fn result_event(
        &mut self,
        delay: Duration,
        identity: &AttemptIdentity,
        request: &MethodRequest,
        worker_id: String,
    ) -> ScriptedEvent {
        let output = match request {
            MethodRequest::WaitFor(request) => {
                let call = MethodCallView {
                    step_type: &request.step_type,
                    is_execute: false,
                    attributes: &request.attributes,
                    loaded_channel_messages: &request.loaded_channel_messages,
                };
                poll_worker_task_frame::Output::WaitFor(InvokeWaitForMethodOutput {
                    output: Some(invoke_wait_for_method_output::Output::Result(
                        wait_for_response(&mut self.rng, &call),
                    )),
                })
            }
            MethodRequest::Execute(request) => {
                let call = MethodCallView {
                    step_type: &request.step_type,
                    is_execute: true,
                    attributes: &request.attributes,
                    loaded_channel_messages: &request.loaded_channel_messages,
                };
                let steps_left = self
                    .steps_left
                    .entry((identity.flow_id.clone(), identity.run_id.clone()))
                    .or_insert(STEPS_PER_RUN);
                poll_worker_task_frame::Output::Execute(InvokeExecuteMethodOutput {
                    output: Some(invoke_execute_method_output::Output::Result(
                        execute_response(&mut self.rng, &call, steps_left),
                    )),
                })
            }
        };
        let frame = PollWorkerTaskFrame {
            task_id: String::new(),
            output: Some(output),
        };
        self.returned_results
            .insert(identity.clone(), frame.clone());
        ScriptedEvent {
            waits_for_release: false,
            delay,
            event: Some(DispatchEvent::Completed(DispatchOutcome::WorkerResult {
                frame,
                app_worker_id: worker_id,
            })),
        }
    }
}

/// Plays `script` until it ends, the session goes away, or the engine cancels the dispatch.
async fn play_script(
    script: Vec<ScriptedEvent>,
    events: mpsc::Sender<DispatchEvent>,
    mut cancel: watch::Receiver<bool>,
    mut hold: watch::Receiver<bool>,
) {
    for scripted in script {
        let is_canceled_while_held = scripted.waits_for_release
            && tokio::select! {
                biased;
                changed = cancel.wait_for(|is_canceled| *is_canceled) => changed.is_ok(),
                released = hold.wait_for(|is_holding| !*is_holding) => released.is_err(),
            };
        let is_canceled = is_canceled_while_held
            || tokio::select! {
                biased;
                changed = cancel.wait_for(|is_canceled| *is_canceled) => changed.is_ok(),
                () = tokio::time::sleep(scripted.delay) => false,
            };
        let event = match scripted.event {
            _ if is_canceled => DispatchEvent::Completed(DispatchOutcome::Cancelled),
            Some(event) => event,
            None => {
                if cancel.wait_for(|is_canceled| *is_canceled).await.is_err() {
                    return;
                }
                DispatchEvent::Completed(DispatchOutcome::Cancelled)
            }
        };
        let is_final = matches!(event, DispatchEvent::Completed(_));
        // A failed send means the engine dropped the session; the script ends either way.
        if events.send(event).await.is_err() || is_final {
            return;
        }
    }
}

struct SimulatedDispatchSession {
    events: mpsc::Receiver<DispatchEvent>,
    cancel: watch::Sender<bool>,
    worker_task: tokio::task::JoinHandle<()>,
}

impl MethodDispatchSession for SimulatedDispatchSession {
    fn next_event(&mut self) -> BoxFuture<'_, Option<DispatchEvent>> {
        Box::pin(self.events.recv())
    }

    fn cancel(&self) {
        self.cancel.send_replace(true);
    }
}

impl Drop for SimulatedDispatchSession {
    fn drop(&mut self) {
        self.worker_task.abort();
    }
}

/// The Worker ports of one node process.
pub(crate) struct NodeWorkerPort {
    workers: Arc<SimulatedWorkers>,
    process_label: String,
}

impl MethodDispatcher for NodeWorkerPort {
    fn dispatch(
        &self,
        request: MethodDispatchRequest,
    ) -> Result<Box<dyn MethodDispatchSession>, MethodDispatchStartError> {
        self.workers
            .start_dispatch(&self.process_label, &request.request)
    }
}

impl RpcWorkerInvoker for NodeWorkerPort {
    fn invoke_worker_rpc(
        &self,
        invocation: WorkerRpcInvocation,
    ) -> BoxFuture<'_, Result<WorkerRpcResult, RpcInvokeError>> {
        Box::pin(self.workers.invoke_rpc(&self.process_label, invocation))
    }
}
