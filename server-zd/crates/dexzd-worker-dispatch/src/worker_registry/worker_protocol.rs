// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Tonic pull Worker stream handling.

use super::*;

/// Tonic WorkerTaskService implementation for one trusted tenant.
#[derive(Clone)]
pub struct WorkerTaskService {
    pub(super) dispatch: WorkerDispatch,
    pub(super) tenant_id: u64,
}

/// Owns the Worker reader task and closes its registration when the gRPC stream ends.
pub struct PollResponseStream {
    receiver: UnboundedReceiverStream<Result<PollWorkerTaskResponse, Status>>,
    reader: JoinHandle<()>,
    dispatch: WorkerDispatch,
    worker_id: Uuid,
}

impl Stream for PollResponseStream {
    type Item = Result<PollWorkerTaskResponse, Status>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.receiver).poll_next(context)
    }
}

impl Drop for PollResponseStream {
    fn drop(&mut self) {
        self.dispatch.disconnect(self.worker_id);
        self.reader.abort();
    }
}

#[tonic::async_trait]
impl worker_task_service_server::WorkerTaskService for WorkerTaskService {
    type PollWorkerTaskStream = PollResponseStream;

    async fn poll_worker_task(
        &self,
        request: Request<Streaming<PollWorkerTaskRequest>>,
    ) -> Result<Response<Self::PollWorkerTaskStream>, Status> {
        let mut incoming = request.into_inner();
        let first = incoming
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("first frame must register a Worker"))?;
        let registration = match first.frame {
            Some(poll_worker_task_request::Frame::Register(registration)) => registration,
            _ => {
                return Err(Status::invalid_argument(
                    "first frame must register a Worker",
                ));
            }
        };
        validate_registration(&registration, &self.dispatch.inner.config)?;
        let (output, receiver) = mpsc::unbounded_channel();
        let (close, close_receiver) = watch::channel(false);
        let worker = Arc::new(WorkerStream {
            id: Uuid::now_v7(),
            tenant_id: self.tenant_id,
            queue: registration.queue.clone(),
            app_worker_id: registration.app_worker_id.clone(),
            output,
            close,
            state: Mutex::new(WorkerState {
                is_busy: true,
                ..WorkerState::default()
            }),
        });
        let shutdown = {
            let mut registry = self
                .dispatch
                .inner
                .registry
                .lock()
                .expect("matching registry lock poisoned");
            if registry.is_closing {
                return Err(Status::unavailable("matching server is closing"));
            }
            let shutdown = self.dispatch.inner.shutdown.subscribe();
            if registry.streams.len() >= self.dispatch.inner.config.max_streams {
                return Err(Status::resource_exhausted("matching stream limit reached"));
            }
            if registry
                .worker_counts
                .get(&(worker.tenant_id, worker.app_worker_id.clone()))
                .copied()
                .unwrap_or(0)
                >= self.dispatch.inner.config.max_concurrent_tasks_cap
            {
                return Err(Status::resource_exhausted("Worker stream limit reached"));
            }
            *registry
                .worker_counts
                .entry((worker.tenant_id, worker.app_worker_id.clone()))
                .or_default() += 1;
            let queue = registry
                .queues
                .entry((worker.tenant_id, worker.queue.clone()))
                .or_insert_with(QueueState::new);
            queue.stream_ids.insert(worker.id);
            queue.users += 1;
            registry.streams.insert(worker.id, worker.clone());
            self.dispatch.inner.metrics.matching_worker_streams.inc();
            shutdown
        };
        if worker
            .output
            .send(Ok(PollWorkerTaskResponse {
                frame: Some(poll_worker_task_response::Frame::Registered(
                    PollWorkerTaskRegistered {
                        supports_unknown_name_rejection: true,
                    },
                )),
            }))
            .is_err()
        {
            self.dispatch.disconnect(worker.id);
            return Err(Status::unavailable(
                "Worker stream closed during registration",
            ));
        }
        self.dispatch.make_idle(&worker);
        let dispatch = self.dispatch.clone();
        let worker_for_reader = worker.clone();
        let reader = tokio::spawn(async move {
            read_worker_stream(
                dispatch,
                worker_for_reader,
                incoming,
                shutdown,
                close_receiver,
            )
            .await;
        });
        Ok(Response::new(PollResponseStream {
            receiver: UnboundedReceiverStream::new(receiver),
            reader,
            dispatch: self.dispatch.clone(),
            worker_id: worker.id,
        }))
    }
}

async fn read_worker_stream(
    dispatch: WorkerDispatch,
    worker: Arc<WorkerStream>,
    mut incoming: Streaming<PollWorkerTaskRequest>,
    mut shutdown: watch::Receiver<bool>,
    mut close: watch::Receiver<bool>,
) {
    if *shutdown.borrow() && begin_shutdown(&worker) {
        dispatch.disconnect(worker.id);
        return;
    }
    let mut ping = tokio::time::interval(dispatch.inner.config.ping_interval);
    ping.tick().await;
    loop {
        tokio::select! {
            message = incoming.message() => match message {
                Ok(Some(frame)) => match process_worker_frame(&dispatch, &worker, frame).await {
                    Ok(should_continue) => if !should_continue { break },
                    Err(status) => {
                        if worker.output.send(Err(status)).is_err() { tracing::debug!("Worker response stream closed"); }
                        break;
                    }
                },
                Ok(None) => break,
                Err(status) => { tracing::debug!(code = ?status.code(), "Worker stream receive failed"); break; }
            },
            _ = ping.tick() => {
                let state = worker.state.lock().expect("Worker state lock poisoned");
                if !state.is_busy && !state.is_closing && worker.output.send(Ok(PollWorkerTaskResponse {
                    frame: Some(poll_worker_task_response::Frame::Ping(PollWorkerTaskPing {})),
                })).is_err() { break; }
            },
            changed = shutdown.changed() => {
                if (changed.is_err() || *shutdown.borrow()) && begin_shutdown(&worker) { break; }
            },
            _ = worker.output.closed() => break,
            changed = close.changed() => { if changed.is_err() || *close.borrow() { break; } },
        }
    }
    dispatch.disconnect(worker.id);
}

fn begin_shutdown(worker: &WorkerStream) -> bool {
    let mut state = worker.state.lock().expect("Worker state lock poisoned");
    state.is_closing = true;
    if state.is_busy {
        return false;
    }
    send_reconnect(worker, "matching server is closing");
    true
}

async fn process_worker_frame(
    dispatch: &WorkerDispatch,
    worker: &WorkerStream,
    frame: PollWorkerTaskRequest,
) -> Result<bool, Status> {
    match frame.frame {
        Some(poll_worker_task_request::Frame::Register(_)) => {
            Err(Status::invalid_argument("duplicate Worker registration"))
        }
        Some(poll_worker_task_request::Frame::Pong(_)) => Ok(true),
        Some(poll_worker_task_request::Frame::TaskFrame(frame)) => {
            let assignment_sender = {
                let state = worker.state.lock().expect("Worker state lock poisoned");
                let Some(assignment) = state.current.as_ref() else {
                    return Ok(true);
                };
                if frame.task_id != assignment.task_id {
                    return Ok(true);
                }
                if frame.output.is_none() {
                    return Err(Status::invalid_argument("task output is required"));
                }
                assignment.frames.clone()
            };
            let is_final = is_final(&frame);
            let task_id = frame.task_id.clone();
            if assignment_sender
                .send(AssignmentFrame::Output(Box::new(frame)))
                .await
                .is_err()
            {
                tracing::debug!("abandoned task output received");
            }
            if is_final {
                return Ok(finish_assignment(dispatch, worker, &task_id));
            }
            Ok(true)
        }
        Some(poll_worker_task_request::Frame::Reject(rejection)) => {
            let reason = PollWorkerTaskRejectReason::try_from(rejection.reason)
                .map_err(|_| Status::invalid_argument("invalid task rejection"))?;
            if !matches!(
                reason,
                PollWorkerTaskRejectReason::NoCapacity
                    | PollWorkerTaskRejectReason::UnknownStep
                    | PollWorkerTaskRejectReason::UnknownRpc
            ) {
                return Err(Status::invalid_argument("invalid task rejection"));
            }
            let assignment_sender = {
                let state = worker.state.lock().expect("Worker state lock poisoned");
                let Some(assignment) = state.current.as_ref() else {
                    return Ok(true);
                };
                if rejection.task_id != assignment.task_id {
                    return Ok(true);
                }
                assignment.frames.clone()
            };
            if assignment_sender
                .send(AssignmentFrame::Reject(reason))
                .await
                .is_err()
            {
                tracing::debug!("abandoned task rejection received");
            }
            Ok(finish_assignment(dispatch, worker, &rejection.task_id))
        }
        None => Err(Status::invalid_argument("unknown Worker frame")),
    }
}

fn finish_assignment(dispatch: &WorkerDispatch, worker: &WorkerStream, task_id: &str) -> bool {
    let mut state = worker.state.lock().expect("Worker state lock poisoned");
    if !state
        .current
        .as_ref()
        .is_some_and(|assignment| assignment.task_id == task_id)
    {
        return true;
    }
    state.current = None;
    state.is_busy = false;
    if state.is_closing {
        send_reconnect(worker, "matching server is closing");
        return false;
    }
    drop(state);
    dispatch.make_idle(worker);
    true
}

fn send_reconnect(worker: &WorkerStream, reason: &str) {
    if worker
        .output
        .send(Ok(PollWorkerTaskResponse {
            frame: Some(poll_worker_task_response::Frame::Reconnect(
                PollWorkerTaskReconnect {
                    reason: reason.to_owned(),
                },
            )),
        }))
        .is_err()
    {
        tracing::debug!("Worker response stream closed before reconnect");
    }
}
