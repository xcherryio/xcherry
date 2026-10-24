// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! In-memory, tenant-scoped pull Worker streams and task matching.

use std::collections::{HashMap, HashSet, VecDeque};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dexzd_clock::WallClock;
use dexzd_metrics::{NodeMetrics, WorkerStreamEvent};
use dexzd_protocol::dex::{
    self, DispatchTaskErrorReason, PollWorkerTask, PollWorkerTaskCancelTask, PollWorkerTaskFrame,
    PollWorkerTaskPing, PollWorkerTaskReconnect, PollWorkerTaskRegister, PollWorkerTaskRegistered,
    PollWorkerTaskRejectReason, PollWorkerTaskRequest, PollWorkerTaskResponse, poll_worker_task,
    poll_worker_task_frame, poll_worker_task_request, poll_worker_task_response,
    worker_task_service_server,
};
use futures::Stream;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

use crate::MatchingConfig;
mod dispatch_request;
mod queue_state;
mod worker_protocol;
pub use dispatch_request::{DispatchRequest, DispatchTaskPayload};
use queue_state::QueuedTaskMetric;
pub use worker_protocol::WorkerTaskService;

/// One ordered frame from the Worker, an assignment change, or the terminal dispatch outcome.
#[derive(Debug)]
pub enum DispatchEvent {
    /// The task was sent to this Worker, which may still reject it before any output.
    Assigned {
        /// Assigned Worker process ID.
        app_worker_id: String,
    },
    /// The assigned Worker rejected the task or disconnected before any output; matching
    /// continues with another Worker.
    Unassigned,
    /// A non-final heartbeat or stream write from the assigned Worker.
    Output(PollWorkerTaskFrame),
    /// The unique final outcome of the dispatch.
    Completed(DispatchOutcome),
}

/// The terminal result of one dispatch.
#[derive(Debug)]
pub enum DispatchOutcome {
    /// A final WaitFor, Execute, or RPC result and the Worker that served it.
    WorkerResult {
        /// Final Worker frame.
        frame: PollWorkerTaskFrame,
        /// Serving Worker process ID.
        app_worker_id: String,
    },
    /// A Worker error response and the Worker that returned it.
    WorkerError {
        /// Final Worker error frame.
        frame: PollWorkerTaskFrame,
        /// Serving Worker process ID.
        app_worker_id: String,
    },
    /// A matching failure with the same reason values as DispatchTaskErrorReason.
    DispatchError {
        /// Matching failure reason.
        reason: DispatchTaskErrorReason,
        /// Human-readable diagnostic.
        detail: String,
    },
    /// The caller cancelled this dispatch.
    Cancelled,
    /// The method deadline elapsed before a final Worker frame.
    DeadlineExceeded,
    /// The matching owner shut down while waiting for a Worker.
    ServerClosing,
}

/// Receives ordered events and can request cancellation of an active Worker task.
pub struct DispatchHandle {
    events: mpsc::Receiver<DispatchEvent>,
    cancel: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}

impl DispatchHandle {
    /// Returns the next Worker output, assignment change, or the single final outcome, then
    /// returns `None`.
    pub async fn next(&mut self) -> Option<DispatchEvent> {
        self.events.recv().await
    }

    /// Sends cancel_task for an assigned task, then allows one second for its final frame.
    pub fn cancel(&self) {
        self.cancel.send_replace(true);
    }
}

impl Drop for DispatchHandle {
    fn drop(&mut self) {
        self.cancel();
        // The dispatch task owns its one-second cancellation grace period.
        drop(self.task.take());
    }
}

/// Shared in-memory matching owner. Clone it to give a tenant-scoped gRPC service access.
#[derive(Clone)]
pub struct WorkerDispatch {
    inner: Arc<DispatchInner>,
}

struct DispatchInner {
    metrics: NodeMetrics,
    config: MatchingConfig,
    clock: Arc<dyn WallClock>,
    registry: Mutex<Registry>,
    shutdown: watch::Sender<bool>,
}

#[derive(Default)]
struct Registry {
    is_closing: bool,
    streams: HashMap<Uuid, Arc<WorkerStream>>,
    worker_counts: HashMap<(u64, String), usize>,
    queues: HashMap<(u64, String), QueueState>,
}

struct QueueState {
    stream_ids: HashSet<Uuid>,
    idle: VecDeque<Uuid>,
    affinity: HashMap<String, (String, Instant)>,
    affinity_order: VecDeque<(String, Instant)>,
    changes: watch::Sender<u64>,
    users: usize,
}

struct QueueLease {
    dispatch: WorkerDispatch,
    key: (u64, String),
}

struct WorkerStream {
    id: Uuid,
    tenant_id: u64,
    queue: String,
    app_worker_id: String,
    output: mpsc::UnboundedSender<Result<PollWorkerTaskResponse, Status>>,
    close: watch::Sender<bool>,
    state: Mutex<WorkerState>,
}

#[derive(Default)]
struct WorkerState {
    is_busy: bool,
    is_closing: bool,
    current: Option<Assignment>,
}

struct Assignment {
    task_id: String,
    frames: mpsc::Sender<AssignmentFrame>,
}

enum AssignmentFrame {
    Output(Box<PollWorkerTaskFrame>),
    Reject(PollWorkerTaskRejectReason),
}

struct QueuePick {
    worker: Option<Arc<WorkerStream>>,
    assignment_frames: Option<mpsc::Receiver<AssignmentFrame>>,
    changes: watch::Receiver<u64>,
    has_streams: bool,
}

impl WorkerDispatch {
    /// Creates a matching owner using the supplied Go-compatible defaults and wall clock.
    pub fn new(config: MatchingConfig, clock: Arc<dyn WallClock>) -> Self {
        Self::new_with_metrics(config, clock, NodeMetrics::unregistered())
    }

    /// Creates a matching owner with the process-local metric handles.
    pub fn new_with_metrics(
        config: MatchingConfig,
        clock: Arc<dyn WallClock>,
        metrics: NodeMetrics,
    ) -> Self {
        assert!(config.max_streams > 0 && config.max_concurrent_tasks_cap > 0);
        assert!(
            config.max_queue_length > 0
                && config.max_flow_types > 0
                && config.max_flow_type_length > 0
        );
        assert!(config.dispatch_wait > Duration::ZERO && config.sticky_wait > Duration::ZERO);
        assert!(config.affinity_duration > Duration::ZERO && config.ping_interval > Duration::ZERO);
        let (shutdown, _) = watch::channel(false);
        Self {
            inner: Arc::new(DispatchInner {
                metrics,
                config,
                clock,
                registry: Mutex::new(Registry::default()),
                shutdown,
            }),
        }
    }

    /// Returns a gRPC WorkerTaskService bound to one trusted tenant ID.
    pub fn service_for_tenant(&self, tenant_id: u64) -> WorkerTaskService {
        WorkerTaskService {
            dispatch: self.clone(),
            tenant_id,
        }
    }

    /// Starts one dispatch; its handle emits Worker frames in order and exactly one outcome.
    pub fn dispatch(&self, request: DispatchRequest) -> Result<DispatchHandle, Status> {
        self.dispatch_with_schedule(request, None)
    }

    /// Starts one dispatch and records latency from a committed Step schedule time.
    pub fn dispatch_with_schedule(
        &self,
        request: DispatchRequest,
        scheduled_at: Option<Instant>,
    ) -> Result<DispatchHandle, Status> {
        validate_dispatch(&request, &self.inner.config, self.inner.clock.as_ref())?;
        if self
            .inner
            .registry
            .lock()
            .expect("matching registry lock poisoned")
            .is_closing
        {
            return Err(Status::unavailable("matching server is closing"));
        }
        let (events_tx, events) = mpsc::channel(32);
        let (cancel, cancel_rx) = watch::channel(false);
        let dispatch = self.clone();
        let task = tokio::spawn(async move {
            let outcome = dispatch
                .run_dispatch(request, scheduled_at, &events_tx, cancel_rx)
                .await;
            if events_tx
                .send(DispatchEvent::Completed(outcome))
                .await
                .is_err()
            {
                tracing::debug!("dispatch result receiver closed");
            }
        });
        Ok(DispatchHandle {
            events,
            cancel,
            task: Some(task),
        })
    }

    /// Drains streams: idle Workers reconnect now; busy Workers reconnect after their task.
    pub fn shutdown(&self) {
        let mut registry = self
            .inner
            .registry
            .lock()
            .expect("matching registry lock poisoned");
        registry.is_closing = true;
        self.inner.shutdown.send_replace(true);
    }

    async fn run_dispatch(
        &self,
        request: DispatchRequest,
        mut scheduled_at: Option<Instant>,
        events: &mpsc::Sender<DispatchEvent>,
        mut cancel: watch::Receiver<bool>,
    ) -> DispatchOutcome {
        let task_id = Uuid::now_v7().to_string();
        let now = self.inner.clock.now_unix_micros();
        let deadline_micros = system_time_micros(request.deadline);
        let deadline_wait =
            Duration::from_micros(deadline_micros.saturating_sub(now).max(0) as u64);
        let total_wait = request
            .dispatch_wait
            .unwrap_or(self.inner.config.dispatch_wait);
        let dispatch_end = Instant::now() + total_wait.min(deadline_wait);
        let sticky_wait = request.sticky_wait.unwrap_or(self.inner.config.sticky_wait);
        let sticky_queue = request
            .sticky_app_worker_id
            .as_ref()
            .filter(|id| *id != &request.flow_type);
        let sticky_end = Instant::now() + sticky_wait.min(total_wait).min(deadline_wait);
        let mut excluded_worker_ids = HashSet::new();
        let mut unavailable = false;
        let mut unknown = false;
        let mut excluded_streams = HashMap::new();
        let mut capacity_backoffs = HashMap::new();
        if let Some(queue) = sticky_queue
            && let Some(outcome) = self
                .search_queue(
                    &request,
                    queue,
                    &task_id,
                    sticky_end,
                    dispatch_end,
                    true,
                    events,
                    &mut cancel,
                    &mut excluded_worker_ids,
                    &mut excluded_streams,
                    &mut capacity_backoffs,
                    &mut unavailable,
                    &mut unknown,
                    &mut scheduled_at,
                )
                .await
        {
            return outcome;
        }
        if let Some(outcome) = self
            .search_queue(
                &request,
                &request.flow_type,
                &task_id,
                dispatch_end,
                dispatch_end,
                false,
                events,
                &mut cancel,
                &mut excluded_worker_ids,
                &mut excluded_streams,
                &mut capacity_backoffs,
                &mut unavailable,
                &mut unknown,
                &mut scheduled_at,
            )
            .await
        {
            return outcome;
        }
        let reason = if unknown && !unavailable {
            DispatchTaskErrorReason::UnknownName
        } else {
            DispatchTaskErrorReason::NoWorkerAvailable
        };
        DispatchOutcome::DispatchError {
            reason,
            detail: if reason == DispatchTaskErrorReason::UnknownName {
                unknown_name_detail(&request)
            } else {
                "no Worker accepted the task".to_owned()
            },
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Queue search carries one dispatch's retry state across sticky and fallback queues."
    )]
    async fn search_queue(
        &self,
        request: &DispatchRequest,
        queue_name: &str,
        task_id: &str,
        queue_end: Instant,
        dispatch_end: Instant,
        is_sticky: bool,
        events: &mpsc::Sender<DispatchEvent>,
        cancel: &mut watch::Receiver<bool>,
        excluded_worker_ids: &mut HashSet<String>,
        excluded_streams: &mut HashMap<Uuid, Instant>,
        capacity_backoffs: &mut HashMap<Uuid, Duration>,
        unavailable: &mut bool,
        unknown: &mut bool,
        scheduled_at: &mut Option<Instant>,
    ) -> Option<DispatchOutcome> {
        let _queue_lease = self.acquire_queue(request.tenant_id, queue_name);
        let mut queued_task =
            QueuedTaskMetric::new(self.inner.metrics.matching_queued_tasks.clone());
        let mut shutdown = self.inner.shutdown.subscribe();
        while Instant::now() < queue_end {
            if *shutdown.borrow() {
                return Some(DispatchOutcome::ServerClosing);
            }
            if *cancel.borrow() {
                return Some(DispatchOutcome::Cancelled);
            }
            let now = Instant::now();
            excluded_streams.retain(|_, until| *until > now);
            let picked = self.pick_worker(
                request.tenant_id,
                queue_name,
                &request.flow_id,
                task_id,
                excluded_worker_ids,
                excluded_streams,
            );
            if let (Some(worker), Some(mut frames)) = (picked.worker, picked.assignment_frames) {
                let task = build_task(task_id, request);
                if worker
                    .output
                    .send(Ok(PollWorkerTaskResponse {
                        frame: Some(poll_worker_task_response::Frame::Task(task)),
                    }))
                    .is_err()
                {
                    self.disconnect(worker.id);
                    *unavailable = true;
                    if Instant::now() >= dispatch_end {
                        return Some(disconnected_outcome("Worker stream disconnected"));
                    }
                    continue;
                }
                queued_task.set_waiting(false);
                self.inner
                    .metrics
                    .worker_stream_events
                    .record(WorkerStreamEvent::Delivered);
                if let Some(when_scheduled) = scheduled_at.take() {
                    self.inner
                        .metrics
                        .step_schedule_to_start_latency
                        .observe(when_scheduled.elapsed().as_secs_f64());
                }
                let assigned = DispatchEvent::Assigned {
                    app_worker_id: worker.app_worker_id.clone(),
                };
                if let Some(outcome) = self
                    .forward_event(assigned, request.deadline, events, cancel)
                    .await
                {
                    self.cancel_assignment(&worker, task_id, &mut frames).await;
                    return Some(outcome);
                }
                let attempt = self
                    .await_assignment(
                        &worker,
                        task_id,
                        request.deadline,
                        dispatch_end,
                        &request.payload,
                        events,
                        cancel,
                        &mut frames,
                    )
                    .await;
                if !matches!(attempt, AttemptOutcome::Final(_))
                    && let Some(outcome) = self
                        .forward_event(DispatchEvent::Unassigned, request.deadline, events, cancel)
                        .await
                {
                    return Some(outcome);
                }
                match attempt {
                    AttemptOutcome::Final(outcome) => return Some(*outcome),
                    AttemptOutcome::Capacity => {
                        queued_task.set_waiting(true);
                        *unavailable = true;
                        let backoff = capacity_backoffs
                            .entry(worker.id)
                            .and_modify(|duration| {
                                *duration = (*duration * 2).min(Duration::from_secs(2));
                            })
                            .or_insert(Duration::from_millis(300));
                        excluded_streams.insert(worker.id, Instant::now() + *backoff);
                    }
                    AttemptOutcome::UnknownName => {
                        queued_task.set_waiting(true);
                        *unknown = true;
                        excluded_worker_ids.insert(worker.app_worker_id.clone());
                        if is_sticky {
                            return None;
                        }
                    }
                    AttemptOutcome::Disconnected => {
                        queued_task.set_waiting(true);
                        *unavailable = true;
                        excluded_streams.insert(worker.id, dispatch_end);
                    }
                }
                continue;
            }
            if is_sticky && !picked.has_streams {
                return None;
            }
            let next_backoff = excluded_streams
                .values()
                .copied()
                .min()
                .unwrap_or(queue_end);
            let wake = next_backoff.min(queue_end);
            let mut changes = picked.changes;
            tokio::select! {
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(wake)) => {},
                changed = changes.changed() => {
                    if changed.is_err() && *shutdown.borrow() {
                        return Some(DispatchOutcome::ServerClosing);
                    }
                },
                changed = cancel.changed() => { if changed.is_err() || *cancel.borrow() { return Some(DispatchOutcome::Cancelled); } },
                changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { return Some(DispatchOutcome::ServerClosing); } },
            }
        }
        None
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Assignment processing needs the task's deadline, output, and cancellation controls."
    )]
    async fn await_assignment(
        &self,
        worker: &WorkerStream,
        task_id: &str,
        deadline: SystemTime,
        dispatch_end: Instant,
        payload: &DispatchTaskPayload,
        events: &mpsc::Sender<DispatchEvent>,
        cancel: &mut watch::Receiver<bool>,
        frames: &mut mpsc::Receiver<AssignmentFrame>,
    ) -> AttemptOutcome {
        let mut has_output = false;
        loop {
            let deadline_wait = Duration::from_micros(
                system_time_micros(deadline)
                    .saturating_sub(self.inner.clock.now_unix_micros())
                    .max(0) as u64,
            );
            tokio::select! {
                frame = frames.recv() => match frame {
                    Some(AssignmentFrame::Output(frame)) => {
                        let frame = *frame;
                        if is_final(&frame) {
                            return AttemptOutcome::Final(Box::new(if matches!(frame.output, Some(poll_worker_task_frame::Output::WorkerError(_))) {
                                DispatchOutcome::WorkerError { frame, app_worker_id: worker.app_worker_id.clone() }
                            } else {
                                DispatchOutcome::WorkerResult { frame, app_worker_id: worker.app_worker_id.clone() }
                            }));
                        }
                        has_output = true;
                        tokio::select! {
                            sent = events.send(DispatchEvent::Output(frame)) => {
                                if sent.is_err() {
                                    self.cancel_assignment(worker, task_id, frames).await;
                                    return AttemptOutcome::Final(Box::new(DispatchOutcome::Cancelled));
                                }
                            },
                            changed = cancel.changed() => {
                                if changed.is_err() || *cancel.borrow() {
                                    self.cancel_assignment(worker, task_id, frames).await;
                                    return AttemptOutcome::Final(Box::new(DispatchOutcome::Cancelled));
                                }
                            },
                            _ = tokio::time::sleep(deadline_wait) => {
                                self.cancel_assignment(worker, task_id, frames).await;
                                return AttemptOutcome::Final(Box::new(DispatchOutcome::DeadlineExceeded));
                            },
                        }
                    }
                    Some(AssignmentFrame::Reject(reason)) => {
                        if has_output {
                            return AttemptOutcome::Final(Box::new(disconnected_outcome("Worker rejected task after output")));
                        }
                        return match reason {
                            PollWorkerTaskRejectReason::NoCapacity => AttemptOutcome::Capacity,
                            PollWorkerTaskRejectReason::UnknownStep if !matches!(payload, DispatchTaskPayload::Rpc(_)) => AttemptOutcome::UnknownName,
                            PollWorkerTaskRejectReason::UnknownRpc if matches!(payload, DispatchTaskPayload::Rpc(_)) => AttemptOutcome::UnknownName,
                            _ => AttemptOutcome::Disconnected,
                        };
                    }
                    None => {
                        if !has_output && Instant::now() < dispatch_end {
                            return AttemptOutcome::Disconnected;
                        }
                        return AttemptOutcome::Final(Box::new(disconnected_outcome("Worker stream disconnected")));
                    }
                },
                changed = cancel.changed() => {
                    if changed.is_err() || *cancel.borrow() {
                        self.cancel_assignment(worker, task_id, frames).await;
                        return AttemptOutcome::Final(Box::new(DispatchOutcome::Cancelled));
                    }
                },
                _ = tokio::time::sleep(deadline_wait) => {
                    self.cancel_assignment(worker, task_id, frames).await;
                    return AttemptOutcome::Final(Box::new(DispatchOutcome::DeadlineExceeded));
                },
            }
        }
    }

    /// Sends `event` to the dispatch handle, waiting for room in its channel.
    ///
    /// Returns `None` once sent, or the dispatch's final outcome when it is cancelled or its
    /// deadline passes first.
    async fn forward_event(
        &self,
        event: DispatchEvent,
        deadline: SystemTime,
        events: &mpsc::Sender<DispatchEvent>,
        cancel: &mut watch::Receiver<bool>,
    ) -> Option<DispatchOutcome> {
        let deadline_wait = Duration::from_micros(
            system_time_micros(deadline)
                .saturating_sub(self.inner.clock.now_unix_micros())
                .max(0) as u64,
        );
        let send = events.send(event);
        tokio::pin!(send);
        let deadline_sleep = tokio::time::sleep(deadline_wait);
        tokio::pin!(deadline_sleep);
        loop {
            tokio::select! {
                sent = &mut send => {
                    return sent.is_err().then_some(DispatchOutcome::Cancelled);
                },
                changed = cancel.changed() => {
                    if changed.is_err() || *cancel.borrow() {
                        return Some(DispatchOutcome::Cancelled);
                    }
                },
                () = &mut deadline_sleep => return Some(DispatchOutcome::DeadlineExceeded),
            }
        }
    }

    async fn cancel_assignment(
        &self,
        worker: &WorkerStream,
        task_id: &str,
        frames: &mut mpsc::Receiver<AssignmentFrame>,
    ) {
        if worker
            .output
            .send(Ok(PollWorkerTaskResponse {
                frame: Some(poll_worker_task_response::Frame::CancelTask(
                    PollWorkerTaskCancelTask {
                        task_id: task_id.to_owned(),
                    },
                )),
            }))
            .is_err()
        {
            self.disconnect(worker.id);
            return;
        }
        let grace_end = Instant::now() + Duration::from_secs(1);
        loop {
            match tokio::time::timeout_at(tokio::time::Instant::from_std(grace_end), frames.recv())
                .await
            {
                Ok(Some(AssignmentFrame::Output(frame))) if is_final(&frame) => return,
                Ok(Some(AssignmentFrame::Reject(_))) => return,
                Ok(Some(AssignmentFrame::Output(_))) => {}
                Ok(None) | Err(_) => {
                    self.disconnect(worker.id);
                    return;
                }
            }
        }
    }

    fn acquire_queue(&self, tenant_id: u64, queue_name: &str) -> QueueLease {
        let key = (tenant_id, queue_name.to_owned());
        let mut registry = self
            .inner
            .registry
            .lock()
            .expect("matching registry lock poisoned");
        registry
            .queues
            .entry(key.clone())
            .or_insert_with(QueueState::new)
            .users += 1;
        QueueLease {
            dispatch: self.clone(),
            key,
        }
    }

    fn release_queue(&self, key: &(u64, String)) {
        let mut registry = self
            .inner
            .registry
            .lock()
            .expect("matching registry lock poisoned");
        let queue = registry.queues.get_mut(key).expect("leased queue exists");
        queue.users -= 1;
        if queue.users == 0 {
            registry.queues.remove(key);
        }
    }

    fn pick_worker(
        &self,
        tenant_id: u64,
        queue_name: &str,
        flow_id: &str,
        task_id: &str,
        excluded_worker_ids: &HashSet<String>,
        excluded_streams: &HashMap<Uuid, Instant>,
    ) -> QueuePick {
        let mut registry = self
            .inner
            .registry
            .lock()
            .expect("matching registry lock poisoned");
        let key = (tenant_id, queue_name.to_owned());
        let queue = registry.queues.get(&key).expect("queue lease exists");
        let changes = queue.changes.subscribe();
        let has_streams = !queue.stream_ids.is_empty();
        if registry.is_closing {
            return QueuePick {
                worker: None,
                assignment_frames: None,
                changes,
                has_streams,
            };
        }
        let preferred = queue
            .affinity
            .get(flow_id)
            .filter(|(_, expiry)| *expiry > Instant::now())
            .map(|(worker_id, _)| worker_id.clone());
        let mut selected = None;
        for (index, id) in queue.idle.iter().enumerate() {
            let worker = registry.streams.get(id).expect("idle stream is registered");
            if excluded_worker_ids.contains(&worker.app_worker_id)
                || excluded_streams.contains_key(id)
            {
                continue;
            }
            if selected.is_none() || preferred.as_ref() == Some(&worker.app_worker_id) {
                selected = Some((index, worker.clone()));
            }
            if preferred.as_ref() == Some(&worker.app_worker_id) {
                break;
            }
        }
        let Some((index, worker)) = selected else {
            return QueuePick {
                worker: None,
                assignment_frames: None,
                changes,
                has_streams,
            };
        };
        registry
            .queues
            .get_mut(&key)
            .expect("queue lease exists")
            .idle
            .remove(index)
            .expect("selected idle stream exists");
        let (sender, receiver) = mpsc::channel(32);
        let mut state = worker.state.lock().expect("Worker state lock poisoned");
        assert!(
            !state.is_busy && !state.is_closing,
            "idle stream must be available"
        );
        state.is_busy = true;
        state.current = Some(Assignment {
            task_id: task_id.to_owned(),
            frames: sender,
        });
        drop(state);
        registry
            .queues
            .get_mut(&key)
            .expect("queue lease exists")
            .remember(
                flow_id,
                &worker.app_worker_id,
                self.inner.config.affinity_duration,
            );
        QueuePick {
            worker: Some(worker),
            assignment_frames: Some(receiver),
            changes,
            has_streams,
        }
    }

    fn make_idle(&self, worker: &WorkerStream) {
        let mut registry = self
            .inner
            .registry
            .lock()
            .expect("matching registry lock poisoned");
        if !registry.streams.contains_key(&worker.id) {
            return;
        }
        let mut state = worker.state.lock().expect("Worker state lock poisoned");
        if state.is_closing {
            return;
        }
        state.is_busy = false;
        state.current = None;
        drop(state);
        let queue = registry
            .queues
            .get_mut(&(worker.tenant_id, worker.queue.clone()))
            .expect("registered queue exists");
        queue.idle.push_back(worker.id);
        queue.signal();
    }

    fn disconnect(&self, id: Uuid) {
        let mut registry = self
            .inner
            .registry
            .lock()
            .expect("matching registry lock poisoned");
        let Some(worker) = registry.streams.remove(&id) else {
            return;
        };
        self.inner
            .metrics
            .worker_stream_events
            .record(WorkerStreamEvent::Closed);
        self.inner.metrics.matching_worker_streams.dec();
        let worker_key = (worker.tenant_id, worker.app_worker_id.clone());
        if let Some(count) = registry.worker_counts.get_mut(&worker_key) {
            *count -= 1;
            if *count == 0 {
                registry.worker_counts.remove(&worker_key);
            }
        }
        let queue_key = (worker.tenant_id, worker.queue.clone());
        if let Some(queue) = registry.queues.get_mut(&queue_key) {
            queue.stream_ids.remove(&id);
            queue.idle.retain(|stream_id| *stream_id != id);
            queue.users -= 1;
            queue.signal();
            if queue.users == 0 {
                registry.queues.remove(&queue_key);
            }
        }
        let mut state = worker.state.lock().expect("Worker state lock poisoned");
        state.is_closing = true;
        state.current = None;
        worker.close.send_replace(true);
    }
}

enum AttemptOutcome {
    Final(Box<DispatchOutcome>),
    Capacity,
    UnknownName,
    Disconnected,
}

fn build_task(task_id: &str, request: &DispatchRequest) -> PollWorkerTask {
    let mut task = PollWorkerTask {
        task_id: task_id.to_owned(),
        deadline: Some(Default::default()),
        request: None,
    };
    let deadline = request
        .deadline
        .duration_since(UNIX_EPOCH)
        .expect("validated deadline is after Unix epoch");
    let timestamp = task
        .deadline
        .as_mut()
        .expect("task deadline was initialized");
    timestamp.seconds = i64::try_from(deadline.as_secs()).unwrap_or(i64::MAX);
    timestamp.nanos = i32::try_from(deadline.subsec_nanos()).expect("nanoseconds fit i32");
    task.request = Some(match &request.payload {
        DispatchTaskPayload::WaitFor(value) => poll_worker_task::Request::WaitFor(value.clone()),
        DispatchTaskPayload::Execute(value) => poll_worker_task::Request::Execute(value.clone()),
        DispatchTaskPayload::Rpc(value) => poll_worker_task::Request::Rpc(value.clone()),
    });
    task
}

fn validate_dispatch(
    request: &DispatchRequest,
    config: &MatchingConfig,
    clock: &dyn WallClock,
) -> Result<(), Status> {
    if request.flow_type.is_empty() || request.flow_type.len() > config.max_queue_length {
        return Err(Status::invalid_argument(
            "Flow type queue must have a valid length",
        ));
    }
    if request
        .sticky_app_worker_id
        .as_ref()
        .is_some_and(|id| id.is_empty() || id.len() > config.max_queue_length)
    {
        return Err(Status::invalid_argument(
            "sticky queue must have a valid length",
        ));
    }
    if system_time_micros(request.deadline) <= clock.now_unix_micros() {
        return Err(Status::invalid_argument("future task deadline is required"));
    }
    if request.dispatch_wait == Some(Duration::ZERO) {
        return Err(Status::invalid_argument(
            "positive dispatch_wait is required",
        ));
    }
    Ok(())
}

fn validate_registration(
    registration: &PollWorkerTaskRegister,
    config: &MatchingConfig,
) -> Result<(), Status> {
    let worker_id = registration.app_worker_id.as_bytes();
    if worker_id.is_empty() || worker_id.len() > 128 {
        return Err(Status::invalid_argument(
            "app_worker_id must have 1 to 128 characters",
        ));
    }
    if !worker_id
        .iter()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(Status::invalid_argument(
            "app_worker_id contains an invalid character",
        ));
    }
    if registration.queue.is_empty() || registration.queue.len() > config.max_queue_length {
        return Err(Status::invalid_argument("queue must have a valid length"));
    }
    if registration.flow_types.len() > config.max_flow_types {
        return Err(Status::invalid_argument("too many flow_types"));
    }
    if registration
        .flow_types
        .iter()
        .any(|flow_type| flow_type.len() > config.max_flow_type_length)
    {
        return Err(Status::invalid_argument(
            "flow_type exceeds the length limit",
        ));
    }
    if registration.queue != registration.app_worker_id
        && !registration.flow_types.contains(&registration.queue)
    {
        return Err(Status::invalid_argument(
            "queue must be app_worker_id or a declared flow_type",
        ));
    }
    Ok(())
}

fn is_final(frame: &PollWorkerTaskFrame) -> bool {
    match &frame.output {
        Some(
            poll_worker_task_frame::Output::WorkerError(_) | poll_worker_task_frame::Output::Rpc(_),
        ) => true,
        Some(poll_worker_task_frame::Output::WaitFor(output)) => matches!(
            output.output,
            Some(dex::invoke_wait_for_method_output::Output::Result(_))
        ),
        Some(poll_worker_task_frame::Output::Execute(output)) => matches!(
            output.output,
            Some(dex::invoke_execute_method_output::Output::Result(_))
        ),
        None => false,
    }
}

fn disconnected_outcome(detail: &str) -> DispatchOutcome {
    DispatchOutcome::DispatchError {
        reason: DispatchTaskErrorReason::WorkerDisconnected,
        detail: detail.to_owned(),
    }
}

fn unknown_name_detail(request: &DispatchRequest) -> String {
    match &request.payload {
        DispatchTaskPayload::WaitFor(value) => format!(
            "no pull Worker registered Step {:?} for Flow type {:?}",
            value.step_type, value.flow_type
        ),
        DispatchTaskPayload::Execute(value) => format!(
            "no pull Worker registered Step {:?} for Flow type {:?}",
            value.step_type, value.flow_type
        ),
        DispatchTaskPayload::Rpc(value) => format!(
            "no pull Worker registered RPC {:?} for Flow type {:?}",
            value.rpc_name, value.flow_type
        ),
    }
}

fn system_time_micros(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_micros()).unwrap_or(i64::MAX))
        .unwrap_or(i64::MIN)
}
