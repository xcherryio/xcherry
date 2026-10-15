// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use dex_protocol::dex::poll_worker_task::Request as TaskRequest;
use dex_protocol::dex::poll_worker_task_frame::Output;
use dex_protocol::dex::poll_worker_task_request::Frame as RequestFrame;
use dex_protocol::dex::poll_worker_task_response::Frame as ResponseFrame;
use dex_protocol::dex::worker_task_service_client::WorkerTaskServiceClient;
use dex_protocol::dex::{
    PollWorkerTask, PollWorkerTaskFrame, PollWorkerTaskPong, PollWorkerTaskRegister,
    PollWorkerTaskReject, PollWorkerTaskRejectReason, PollWorkerTaskRequest, WorkerErrorResponse,
};
use tokio::sync::{Notify, mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Instant, sleep, sleep_until, timeout};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Status};

use crate::context::InvocationCancellation;
use crate::worker::worker_error_response;
use crate::worker_dispatcher::WorkerDispatcher;
use crate::{PullWorkerOptions, SdkError, SdkResult};

const DRAINING: u8 = 1;
const FORCED: u8 = 2;

pub(crate) fn outbound_task_channel(endpoint: Endpoint) -> Channel {
    endpoint.connect_lazy()
}

pub(crate) struct PullWorker {
    dispatcher: WorkerDispatcher,
    client: WorkerTaskServiceClient<Channel>,
    app_worker_id: String,
    flow_types: Vec<String>,
    queues: Vec<String>,
    max_concurrent_tasks: usize,
    idle_streams_per_queue: usize,
    stop_drain_timeout: Duration,
}

struct PullState {
    streams: HashMap<u64, StreamSlot>,
    busy_tasks: usize,
    next_stream_id: u64,
    is_stopping: bool,
    logged_unsupported: bool,
}

struct StreamSlot {
    queue: String,
    is_busy: bool,
}

struct PullShared {
    state: Mutex<PullState>,
    notify: Notify,
    max_concurrent_tasks: usize,
}

struct ActiveTask {
    task_id: String,
    cancellation: InvocationCancellation,
    transport_cancellation: InvocationCancellation,
    cancel_code: Arc<AtomicI32>,
    handler: JoinHandle<()>,
    deadline: Option<Instant>,
}

struct StreamRegistration {
    shared: Arc<PullShared>,
    stream_id: u64,
}

impl Drop for StreamRegistration {
    fn drop(&mut self) {
        self.shared
            .state
            .lock()
            .expect("pull state lock")
            .streams
            .remove(&self.stream_id);
        self.shared.notify.notify_one();
    }
}

impl PullWorker {
    pub(crate) fn new(
        options: &PullWorkerOptions,
        dispatcher: &WorkerDispatcher,
        channel: Channel,
    ) -> SdkResult<Self> {
        let max_concurrent_tasks = match options.max_concurrent_tasks {
            Some(value) => value,
            None => std::thread::available_parallelism()
                .map(std::num::NonZeroUsize::get)
                .map_err(|error| SdkError::Service {
                    service: crate::ServiceError::local(
                        "worker",
                        format!("read CPU count: {error}"),
                    ),
                })?,
        };
        let idle_streams_per_queue = options.idle_streams_per_queue.unwrap_or(1);
        let stop_drain_timeout = options
            .stop_drain_timeout
            .unwrap_or(Duration::from_secs(30));
        if max_concurrent_tasks == 0 || idle_streams_per_queue == 0 || stop_drain_timeout.is_zero()
        {
            return Err(invalid(
                "pull Worker task limit, idle stream count, and drain timeout must be positive",
            ));
        }
        let app_worker_id = match &options.app_worker_id {
            Some(value) => value.clone(),
            None => generate_app_worker_id(),
        };
        validate_app_worker_id(&app_worker_id)?;
        let flow_types = dispatcher.flow_types();
        if flow_types
            .iter()
            .any(|flow_type| flow_type == &app_worker_id)
        {
            return Err(invalid(format!(
                "app Worker ID conflicts with Flow type {app_worker_id:?}"
            )));
        }
        let queues = std::iter::once(app_worker_id.clone())
            .chain(flow_types.iter().cloned())
            .collect();
        Ok(Self {
            dispatcher: dispatcher.clone(),
            client: WorkerTaskServiceClient::new(channel),
            app_worker_id,
            flow_types,
            queues,
            max_concurrent_tasks,
            idle_streams_per_queue,
            stop_drain_timeout,
        })
    }

    pub(crate) async fn run(&self, mut shutdown: watch::Receiver<bool>) -> SdkResult<()> {
        let shared = Arc::new(PullShared {
            state: Mutex::new(PullState {
                streams: HashMap::new(),
                busy_tasks: 0,
                next_stream_id: 0,
                is_stopping: false,
                logged_unsupported: false,
            }),
            notify: Notify::new(),
            max_concurrent_tasks: self.max_concurrent_tasks,
        });
        let (stop, stop_receiver) = watch::channel(0u8);
        let mut streams = JoinSet::new();
        loop {
            if *shutdown.borrow() {
                break;
            }
            self.reconcile(&shared, &stop_receiver, &mut streams);
            tokio::select! {
                change = shutdown.changed() => {
                    if change.is_err() || *shutdown.borrow() { break; }
                }
                () = shared.notify.notified() => {}
                result = streams.join_next(), if !streams.is_empty() => {
                    if let Some(Err(error)) = result { eprintln!("ERROR: pull Worker stream failed: {error}"); }
                }
            }
        }
        shared.state.lock().expect("pull state lock").is_stopping = true;
        stop.send_replace(DRAINING);
        if timeout(self.stop_drain_timeout, async {
            while let Some(result) = streams.join_next().await {
                if let Err(error) = result {
                    eprintln!("ERROR: pull Worker stream failed: {error}");
                }
            }
        })
        .await
        .is_err()
        {
            stop.send_replace(FORCED);
            while let Some(result) = streams.join_next().await {
                if let Err(error) = result {
                    eprintln!("ERROR: pull Worker stream failed: {error}");
                }
            }
        }
        Ok(())
    }

    fn reconcile(
        &self,
        shared: &Arc<PullShared>,
        stop: &watch::Receiver<u8>,
        streams: &mut JoinSet<()>,
    ) {
        let mut state = shared.state.lock().expect("pull state lock");
        if state.is_stopping {
            return;
        }
        for queue in &self.queues {
            let idle = state
                .streams
                .values()
                .filter(|slot| slot.queue == *queue && !slot.is_busy)
                .count();
            for _ in idle..self.idle_streams_per_queue {
                let stream_id = state.next_stream_id;
                state.next_stream_id += 1;
                state.streams.insert(
                    stream_id,
                    StreamSlot {
                        queue: queue.clone(),
                        is_busy: false,
                    },
                );
                let worker = PullStream {
                    client: self.client.clone(),
                    dispatcher: self.dispatcher.clone(),
                    app_worker_id: self.app_worker_id.clone(),
                    flow_types: self.flow_types.clone(),
                    queue: queue.clone(),
                    stream_id,
                    shared: Arc::clone(shared),
                    stop: stop.clone(),
                };
                streams.spawn(async move { worker.run().await });
            }
        }
    }
}

struct PullStream {
    client: WorkerTaskServiceClient<Channel>,
    dispatcher: WorkerDispatcher,
    app_worker_id: String,
    flow_types: Vec<String>,
    queue: String,
    stream_id: u64,
    shared: Arc<PullShared>,
    stop: watch::Receiver<u8>,
}

impl PullStream {
    async fn run(mut self) {
        let _registration = StreamRegistration {
            shared: Arc::clone(&self.shared),
            stream_id: self.stream_id,
        };
        let mut backoff = Duration::from_millis(100);
        loop {
            if *self.stop.borrow() != 0 {
                break;
            }
            let (was_registered, is_reconnecting, result) = self.poll().await;
            if *self.stop.borrow() != 0 {
                break;
            }
            if is_reconnecting && result.is_ok() {
                backoff = Duration::from_millis(100);
                continue;
            }
            if was_registered || is_reconnecting {
                backoff = Duration::from_millis(100);
            }
            let delay = match &result {
                Err(status) if status.code() == Code::Unimplemented => {
                    let mut state = self.shared.state.lock().expect("pull state lock");
                    if !state.logged_unsupported {
                        state.logged_unsupported = true;
                        eprintln!(
                            "ERROR: pull Worker polling is unsupported by this Dex server: {status}"
                        );
                    }
                    Duration::from_secs(60)
                }
                Err(status) if is_permanent(status.code()) => {
                    eprintln!(
                        "ERROR: Worker task stream rejected for {}: {status}",
                        self.queue
                    );
                    backoff = Duration::from_millis(100);
                    Duration::from_secs(60)
                }
                Err(status) => {
                    eprintln!(
                        "WARN: Worker task stream disconnected for {}: {status}",
                        self.queue
                    );
                    backoff
                }
                Ok(()) => backoff,
            };
            let jitter = Duration::from_millis(
                (uuid::Uuid::new_v4().as_u128() as u64) % (delay.as_millis() as u64 / 2).max(1),
            );
            tokio::select! {
                () = sleep(delay + jitter) => {}
                change = self.stop.changed() => { if change.is_err() || *self.stop.borrow() != 0 { break; } }
            }
            if delay < Duration::from_secs(60) {
                backoff = (backoff * 2).min(Duration::from_secs(5));
            }
        }
    }

    async fn poll(&mut self) -> (bool, bool, Result<(), Status>) {
        let (outbound_sender, receiver) = mpsc::channel(16);
        let mut sender = Some(outbound_sender);
        let register = PollWorkerTaskRegister {
            queue: self.queue.clone(),
            app_worker_id: self.app_worker_id.clone(),
            flow_types: self.flow_types.clone(),
            sdk_name: "rust".to_string(),
            sdk_version: env!("CARGO_PKG_VERSION").to_string(),
        };
        if sender
            .as_ref()
            .expect("open Worker stream")
            .send(PollWorkerTaskRequest {
                frame: Some(RequestFrame::Register(register)),
            })
            .await
            .is_err()
        {
            return (
                false,
                false,
                Err(Status::internal("Worker register channel closed")),
            );
        }
        let opening = tokio::select! {
            result = self.client.poll_worker_task(ReceiverStream::new(receiver)) => result,
            change = self.stop.changed() => {
                if change.is_err() || *self.stop.borrow() != 0 {
                    return (false, false, Ok(()));
                }
                return (false, false, Err(Status::unavailable("Worker poll interrupted")));
            }
        };
        let mut response_stream = match opening {
            Ok(response) => response.into_inner(),
            Err(status) => return (false, false, Err(status)),
        };
        let mut was_registered = false;
        let mut is_draining = false;
        let mut active: Option<ActiveTask> = None;
        loop {
            let deadline = active.as_ref().and_then(|task| task.deadline);
            tokio::select! {
                response = response_stream.message() => {
                    match response {
                        Ok(Some(response)) => {
                            if !was_registered {
                                match response.frame {
                                    Some(ResponseFrame::Registered(_)) => { was_registered = true; self.shared.notify.notify_one(); }
                                    Some(ResponseFrame::Reconnect(_)) => return (false, true, Ok(())),
                                    _ => return (false, false, Err(Status::internal("Worker task stream did not register"))),
                                }
                                continue;
                            }
                            match response.frame {
                                Some(ResponseFrame::Task(task)) => {
                                    if active.is_some() || is_draining || !self.shared.try_accept(self.stream_id) {
                                        let reject = PollWorkerTaskReject { task_id: task.task_id, reason: PollWorkerTaskRejectReason::NoCapacity as i32 };
                                        if let Err(status) = send_control_frame(sender.as_ref().expect("open Worker stream"), RequestFrame::Reject(reject)).await {
                                            self.cancel_and_wait(&mut active).await;
                                            return (was_registered, false, Err(status));
                                        }
                                    } else {
                                        let cancellation = InvocationCancellation::new();
                                        let transport_cancellation = InvocationCancellation::new();
                                        let cancel_code = Arc::new(AtomicI32::new(0));
                                        let task_id = task.task_id.clone();
                                        let deadline = task.deadline.as_ref().map(task_deadline);
                                        let handler = tokio::spawn(execute_task(self.dispatcher.clone(), task, cancellation.clone(), transport_cancellation.clone(), Arc::clone(&cancel_code), sender.as_ref().expect("open Worker stream").clone()));
                                        active = Some(ActiveTask { task_id, cancellation, transport_cancellation, cancel_code, handler, deadline });
                                        self.shared.notify.notify_one();
                                    }
                                }
                                Some(ResponseFrame::CancelTask(cancel)) => {
                                    if let Some(task) = &active && task.task_id == cancel.task_id {
                                        task.cancel_code.store(Code::Cancelled as i32, Ordering::Release);
                                        task.cancellation.cancel();
                                    }
                                }
                                Some(ResponseFrame::Ping(_)) => {
                                    if let Err(status) = send_control_frame(sender.as_ref().expect("open Worker stream"), RequestFrame::Pong(PollWorkerTaskPong {})).await {
                                        self.cancel_and_wait(&mut active).await;
                                        return (was_registered, false, Err(status));
                                    }
                                }
                                Some(ResponseFrame::Reconnect(_)) => {
                                    is_draining = true;
                                    if active.is_none() { return (was_registered, true, Ok(())); }
                                    self.shared.notify.notify_one();
                                }
                                _ => {}
                            }
                        }
                        Ok(None) => {
                            self.cancel_and_wait(&mut active).await;
                            return (was_registered, false, Ok(()));
                        }
                        Err(status) => {
                            self.cancel_and_wait(&mut active).await;
                            return (was_registered, false, Err(status));
                        }
                    }
                }
                result = async { (&mut active.as_mut().expect("active task").handler).await }, if active.is_some() => {
                    if let Err(error) = result { eprintln!("ERROR: pull Worker handler failed: {error}"); }
                    active = None;
                    self.shared.release(self.stream_id);
                    if is_draining || *self.stop.borrow() != 0 {
                        let is_reconnecting = is_draining && *self.stop.borrow() == 0;
                        let result = close_poll_send_and_wait(&mut sender, &mut response_stream).await;
                        return (was_registered, is_reconnecting, result);
                    }
                }
                () = async { sleep_until(deadline.expect("task deadline")).await }, if deadline.is_some() => {
                    if let Some(task) = &mut active {
                        task.cancel_code.store(Code::DeadlineExceeded as i32, Ordering::Release);
                        task.cancellation.cancel();
                        task.deadline = None;
                    }
                }
                change = self.stop.changed() => {
                    if change.is_err() || *self.stop.borrow() != 0 {
                        is_draining = true;
                        if *self.stop.borrow() >= FORCED && let Some(task) = &active {
                            task.cancel_code.store(Code::Cancelled as i32, Ordering::Release);
                            task.cancellation.cancel();
                            task.transport_cancellation.cancel();
                        }
                        if active.is_none() { return (was_registered, false, Ok(())); }
                    }
                }
            }
        }
    }

    async fn cancel_and_wait(&self, active: &mut Option<ActiveTask>) {
        if let Some(task) = active.take() {
            task.cancel_code
                .store(Code::Cancelled as i32, Ordering::Release);
            task.cancellation.cancel();
            task.transport_cancellation.cancel();
            if let Err(error) = task.handler.await {
                eprintln!("ERROR: pull Worker handler failed: {error}");
            }
            self.shared.release(self.stream_id);
        }
    }
}

impl PullShared {
    fn try_accept(&self, stream_id: u64) -> bool {
        let mut state = self.state.lock().expect("pull state lock");
        if state.is_stopping || state.busy_tasks >= self.max_concurrent_tasks {
            return false;
        }
        let Some(slot) = state.streams.get_mut(&stream_id) else {
            return false;
        };
        if slot.is_busy {
            return false;
        }
        slot.is_busy = true;
        state.busy_tasks += 1;
        true
    }

    fn release(&self, stream_id: u64) {
        let mut state = self.state.lock().expect("pull state lock");
        let slot = state
            .streams
            .get_mut(&stream_id)
            .expect("registered pull stream");
        assert!(slot.is_busy, "pull stream must have an active task");
        slot.is_busy = false;
        state.busy_tasks -= 1;
        self.notify.notify_one();
    }
}

async fn execute_task(
    dispatcher: WorkerDispatcher,
    task: PollWorkerTask,
    cancellation: InvocationCancellation,
    transport_cancellation: InvocationCancellation,
    cancel_code: Arc<AtomicI32>,
    sender: mpsc::Sender<PollWorkerTaskRequest>,
) {
    let task_id = task.task_id;
    match task.request {
        Some(TaskRequest::WaitFor(request)) => {
            let mut output = dispatcher
                .invoke_wait_for_with_cancellation(request, cancellation.clone())
                .into_stream(crate::worker::worker_status);
            while let Some(result) = output.next().await {
                let frame = match result {
                    Ok(value) => Output::WaitFor(value),
                    Err(status) => Output::WorkerError(with_cancel_status(
                        worker_error_from_status(status),
                        &cancel_code,
                    )),
                };
                if send_task_frame(&sender, &task_id, frame, &transport_cancellation)
                    .await
                    .is_err()
                {
                    cancellation.cancel();
                    transport_cancellation.cancel();
                }
            }
        }
        Some(TaskRequest::Execute(request)) => {
            let mut output = dispatcher
                .invoke_execute_with_cancellation(request, cancellation.clone())
                .into_stream(crate::worker::worker_status);
            while let Some(result) = output.next().await {
                let frame = match result {
                    Ok(value) => Output::Execute(value),
                    Err(status) => Output::WorkerError(with_cancel_status(
                        worker_error_from_status(status),
                        &cancel_code,
                    )),
                };
                if send_task_frame(&sender, &task_id, frame, &transport_cancellation)
                    .await
                    .is_err()
                {
                    cancellation.cancel();
                    transport_cancellation.cancel();
                }
            }
        }
        Some(TaskRequest::Rpc(request)) => {
            let frame = match dispatcher
                .invoke_rpc_with_cancellation(request, cancellation)
                .await
            {
                Ok(value) => Output::Rpc(value),
                Err(error) => Output::WorkerError(with_cancel_status(
                    worker_error_response(error),
                    &cancel_code,
                )),
            };
            if let Err(error) =
                send_task_frame(&sender, &task_id, frame, &transport_cancellation).await
            {
                eprintln!("WARN: send Worker RPC result: {error}");
            }
        }
        None => {
            let error = WorkerErrorResponse {
                detail: "Worker task request is missing".to_string(),
                grpc_status: Code::InvalidArgument as i32,
                ..Default::default()
            };
            if let Err(error) = send_task_frame(
                &sender,
                &task_id,
                Output::WorkerError(error),
                &transport_cancellation,
            )
            .await
            {
                eprintln!("WARN: send Worker task error: {error}");
            }
        }
    }
}

async fn send_control_frame(
    sender: &mpsc::Sender<PollWorkerTaskRequest>,
    frame: RequestFrame,
) -> Result<(), Status> {
    sender
        .send(PollWorkerTaskRequest { frame: Some(frame) })
        .await
        .map_err(|error| {
            Status::unavailable(format!(
                "Worker task stream cannot send control frame: {error}"
            ))
        })
}

async fn close_poll_send_and_wait(
    sender: &mut Option<mpsc::Sender<PollWorkerTaskRequest>>,
    responses: &mut tonic::Streaming<dex_protocol::dex::PollWorkerTaskResponse>,
) -> Result<(), Status> {
    sender.take();
    timeout(Duration::from_secs(5), async {
        while responses.message().await?.is_some() {}
        Ok(())
    })
    .await
    .map_err(|_| Status::deadline_exceeded("Worker task stream did not close after drain"))?
}

fn with_cancel_status(
    mut error: WorkerErrorResponse,
    cancel_code: &AtomicI32,
) -> WorkerErrorResponse {
    let code = cancel_code.load(Ordering::Acquire);
    if code != 0 {
        error.grpc_status = code;
    }
    error
}

async fn send_task_frame(
    sender: &mpsc::Sender<PollWorkerTaskRequest>,
    task_id: &str,
    output: Output,
    cancellation: &InvocationCancellation,
) -> Result<(), Status> {
    let request = PollWorkerTaskRequest {
        frame: Some(RequestFrame::TaskFrame(PollWorkerTaskFrame {
            task_id: task_id.to_string(),
            output: Some(output),
        })),
    };
    tokio::select! {
        biased;
        result = sender.send(request) => result.map_err(|error| Status::unavailable(format!("Worker task stream cannot send task frame: {error}"))),
        () = cancellation.cancelled() => Err(Status::cancelled("Worker task was cancelled")),
    }
}

fn worker_error_from_status(status: Status) -> WorkerErrorResponse {
    #[derive(prost::Message)]
    struct RpcStatus {
        #[prost(int32, tag = "1")]
        code: i32,
        #[prost(string, tag = "2")]
        message: String,
        #[prost(message, repeated, tag = "3")]
        details: Vec<prost_types::Any>,
    }
    use prost::Message;
    if let Ok(details) = RpcStatus::decode(status.details())
        && let Some(worker) = details
            .details
            .iter()
            .find(|detail| detail.type_url.ends_with("/dex.WorkerErrorResponse"))
        && let Ok(mut error) = WorkerErrorResponse::decode(worker.value.as_slice())
    {
        error.grpc_status = status.code() as i32;
        return error;
    }
    WorkerErrorResponse {
        detail: status.message().to_string(),
        grpc_status: status.code() as i32,
        ..Default::default()
    }
}

fn task_deadline(timestamp: &prost_types::Timestamp) -> Instant {
    let current = Instant::now();
    if timestamp.seconds < 0 || !(0..1_000_000_000).contains(&timestamp.nanos) {
        return current;
    }
    let Some(deadline) = SystemTime::UNIX_EPOCH
        .checked_add(Duration::from_secs(timestamp.seconds as u64))
        .and_then(|time| time.checked_add(Duration::from_nanos(timestamp.nanos as u64)))
    else {
        return current;
    };
    current
        .checked_add(
            deadline
                .duration_since(SystemTime::now())
                .unwrap_or_default(),
        )
        .unwrap_or(current)
}

fn is_permanent(code: Code) -> bool {
    matches!(
        code,
        Code::Unauthenticated
            | Code::PermissionDenied
            | Code::InvalidArgument
            | Code::Unimplemented
            | Code::ResourceExhausted
    )
}

fn generate_app_worker_id() -> String {
    let identity = ["DEX_WORKER_INSTANCE", "POD_NAME", "HOSTNAME"]
        .into_iter()
        .filter_map(std::env::var_os)
        .map(|value| value.to_string_lossy().into_owned())
        .find(|value| !value.trim().is_empty())
        .unwrap_or_else(|| gethostname::gethostname().to_string_lossy().into_owned());
    let mut cleaned = String::new();
    for character in identity.to_ascii_lowercase().chars() {
        if cleaned.len() >= 54 {
            break;
        }
        if character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-' {
            cleaned.push(character);
        } else if !cleaned.is_empty() {
            cleaned.push('-');
        }
    }
    let cleaned = cleaned.trim_matches('-');
    let identity = if cleaned.is_empty() {
        "worker"
    } else {
        cleaned
    };
    let random = uuid::Uuid::new_v4();
    let mut bits = 0u64;
    for byte in &random.as_bytes()[..5] {
        bits = (bits << 8) | u64::from(*byte);
    }
    let alphabet = b"abcdefghijklmnopqrstuvwxyz234567";
    let suffix: String = (0..8)
        .rev()
        .map(|index| alphabet[((bits >> (index * 5)) & 31) as usize] as char)
        .collect();
    format!("{identity}-{suffix}")
}

fn validate_app_worker_id(value: &str) -> SdkResult<()> {
    if value.is_empty() || value.len() > 63 {
        return Err(invalid("app Worker ID must contain 1-63 characters"));
    }
    if let Some(character) = value.chars().find(|character| {
        !character.is_ascii_alphanumeric() && !matches!(character, '.' | '_' | '-')
    }) {
        return Err(invalid(format!(
            "app Worker ID contains invalid character {character:?}"
        )));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> SdkError {
    SdkError::FlowDefinition {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value_hydrator::ValueHydrator;
    use crate::{
        BlobCache, BlobCacheConfig, Context, Flow, HandlerError, HandlerResult, Registry, Rpc,
        RpcList, RpcResult, Worker, WorkerOptions, WorkerTarget,
    };
    use dex_protocol::dex::flow_service_client::FlowServiceClient;
    use dex_protocol::dex::worker_task_service_server::{
        WorkerTaskService, WorkerTaskServiceServer,
    };
    use dex_protocol::dex::{
        Context as ProtoContext, InvokeWorkerRpcRequest, PollWorkerTaskCancelTask,
        PollWorkerTaskPing, PollWorkerTaskReconnect, PollWorkerTaskRegistered,
        PollWorkerTaskResponse,
    };
    use std::sync::Condvar;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
    use tonic::transport::Server;
    use tonic::{Request, Response};

    struct RegisteredFlow;

    impl Flow for RegisteredFlow {
        type StartInput = ();
    }

    fn test_cache() -> (Arc<BlobCache>, tempfile::TempDir) {
        let directory = tempfile::tempdir().expect("cache directory");
        let cache = Arc::new(
            BlobCache::open(
                BlobCacheConfig::new(directory.path(), 1024 * 1024, 0).expect("cache config"),
            )
            .expect("open cache"),
        );
        (cache, directory)
    }

    #[test]
    fn validates_pull_options_and_target_conflicts() {
        let (cache, _directory) = test_cache();
        let registry = Registry::new()
            .register(RegisteredFlow)
            .expect("register Flow");
        for options in [
            WorkerOptions::new().pull(PullWorkerOptions::new().max_concurrent_tasks(0)),
            WorkerOptions::new().pull(PullWorkerOptions::new().idle_streams_per_queue(0)),
            WorkerOptions::new().pull(PullWorkerOptions::new().stop_drain_timeout(Duration::ZERO)),
            WorkerOptions::new().pull(PullWorkerOptions::new().app_worker_id("bad/id")),
            WorkerOptions::new().pull(PullWorkerOptions::new().app_worker_id("RegisteredFlow")),
            WorkerOptions::new()
                .pull(PullWorkerOptions::new())
                .bind_address("127.0.0.1:8803"),
            WorkerOptions::new()
                .pull(PullWorkerOptions::new())
                .worker_target(WorkerTarget::pull()),
            WorkerOptions::new().worker_target(WorkerTarget::pull()),
        ] {
            assert!(matches!(
                Worker::try_new(registry.clone(), Arc::clone(&cache), options),
                Err(SdkError::FlowDefinition { .. })
            ));
        }
        assert!(
            Worker::try_new(
                registry,
                cache,
                WorkerOptions::new().pull(PullWorkerOptions::new())
            )
            .is_ok()
        );
        assert!(WorkerTarget::pull().headless(true).validate().is_err());
    }

    #[test]
    fn generates_distinct_valid_app_worker_ids() {
        let first = generate_app_worker_id();
        let second = generate_app_worker_id();
        assert_ne!(first, second);
        validate_app_worker_id(&first).expect("valid first ID");
        assert!(first.len() <= 63);
        let suffix = first.rsplit('-').next().expect("ID suffix");
        assert_eq!(8, suffix.len());
        assert!(
            suffix
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || (b'2'..=b'7').contains(&byte))
        );
        assert!(validate_app_worker_id("").is_err());
        assert!(validate_app_worker_id(&"a".repeat(64)).is_err());
        assert!(validate_app_worker_id("has space").is_err());
    }

    #[test]
    fn rejects_an_assignment_when_global_capacity_is_full() {
        let shared = PullShared {
            state: Mutex::new(PullState {
                streams: HashMap::from([
                    (
                        1,
                        StreamSlot {
                            queue: "queue".to_string(),
                            is_busy: false,
                        },
                    ),
                    (
                        2,
                        StreamSlot {
                            queue: "queue".to_string(),
                            is_busy: false,
                        },
                    ),
                ]),
                busy_tasks: 0,
                next_stream_id: 3,
                is_stopping: false,
                logged_unsupported: false,
            }),
            notify: Notify::new(),
            max_concurrent_tasks: 1,
        };
        assert!(shared.try_accept(1));
        assert!(!shared.try_accept(2));
        assert_eq!(PollWorkerTaskRejectReason::NoCapacity as i32, 1);
        shared.release(1);
        assert!(shared.try_accept(2));
    }

    #[test]
    fn maps_push_worker_failure_fields_into_pull_frame() {
        let status = crate::worker::worker_status(HandlerError::retry_after(
            7,
            "PaymentDeclined",
            "card declined",
        ));
        let error = worker_error_from_status(status);
        assert_eq!(Code::Unknown as i32, error.grpc_status);
        assert_eq!("card declined", error.detail);
        assert_eq!("PaymentDeclined", error.error_type);
        assert_eq!(7, error.retry_after_seconds);
        assert!(
            error
                .stack_trace
                .contains("maps_push_worker_failure_fields")
        );
        let cancelled_reason = AtomicI32::new(Code::Cancelled as i32);
        let cancelled_error = with_cancel_status(error.clone(), &cancelled_reason);
        assert_eq!(Code::Cancelled as i32, cancelled_error.grpc_status);
        assert_eq!(error.detail, cancelled_error.detail);
        assert_eq!(error.error_type, cancelled_error.error_type);
        assert_eq!(error.stack_trace, cancelled_error.stack_trace);
        assert_eq!(
            error.retry_after_seconds,
            cancelled_error.retry_after_seconds
        );
        let deadline_reason = AtomicI32::new(Code::DeadlineExceeded as i32);
        assert_eq!(
            Code::DeadlineExceeded as i32,
            with_cancel_status(error, &deadline_reason).grpc_status
        );
        let cancelled = worker_error_from_status(Status::cancelled("cancelled"));
        assert_eq!(Code::Cancelled as i32, cancelled.grpc_status);
    }

    #[test]
    fn expired_task_deadline_is_immediate() {
        let deadline = task_deadline(&prost_types::Timestamp {
            seconds: 0,
            nanos: 0,
        });
        assert!(deadline <= Instant::now());
        let malformed = task_deadline(&prost_types::Timestamp {
            seconds: i64::MAX,
            nanos: -1,
        });
        assert!(malformed <= Instant::now());
    }

    #[tokio::test]
    async fn cancellation_waits_for_the_handler_to_return() {
        let cancellation = InvocationCancellation::new();
        let waiting = cancellation.clone();
        let handler = tokio::spawn(async move { waiting.cancelled().await });
        cancellation.cancel();
        timeout(Duration::from_secs(1), handler)
            .await
            .expect("handler returned")
            .expect("join handler");
    }

    #[tokio::test]
    async fn cancelled_task_does_not_block_on_a_full_outbound_stream() {
        let (sender, _receiver) = mpsc::channel(1);
        sender
            .try_send(PollWorkerTaskRequest {
                frame: Some(RequestFrame::Pong(PollWorkerTaskPong {})),
            })
            .expect("fill outbound stream");
        let cancellation = InvocationCancellation::new();
        cancellation.cancel();
        let result = timeout(
            Duration::from_secs(1),
            send_task_frame(
                &sender,
                "task-1",
                Output::WorkerError(WorkerErrorResponse::default()),
                &cancellation,
            ),
        )
        .await
        .expect("send stopped");
        assert_eq!(Code::Cancelled, result.expect_err("cancelled send").code());
    }

    #[derive(Clone)]
    struct ReconnectService {
        registrations: Arc<AtomicUsize>,
        pongs: Arc<AtomicUsize>,
        notify: Arc<Notify>,
    }

    #[tonic::async_trait]
    impl WorkerTaskService for ReconnectService {
        type PollWorkerTaskStream = ReceiverStream<Result<PollWorkerTaskResponse, Status>>;

        async fn poll_worker_task(
            &self,
            request: Request<tonic::Streaming<PollWorkerTaskRequest>>,
        ) -> Result<Response<Self::PollWorkerTaskStream>, Status> {
            let mut incoming = request.into_inner();
            let first = incoming
                .message()
                .await?
                .ok_or_else(|| Status::invalid_argument("missing Register"))?;
            let Some(RequestFrame::Register(register)) = first.frame else {
                return Err(Status::invalid_argument("Register must be first"));
            };
            assert_eq!("rust", register.sdk_name);
            assert_eq!(register.queue, register.app_worker_id);
            self.registrations.fetch_add(1, Ordering::SeqCst);
            self.notify.notify_one();
            let (sender, receiver) = mpsc::channel(4);
            let pongs = Arc::clone(&self.pongs);
            tokio::spawn(async move {
                let registered = PollWorkerTaskResponse {
                    frame: Some(ResponseFrame::Registered(PollWorkerTaskRegistered {})),
                };
                if sender.send(Ok(registered)).await.is_err() {
                    return;
                }
                let ping = PollWorkerTaskResponse {
                    frame: Some(ResponseFrame::Ping(PollWorkerTaskPing {})),
                };
                if sender.send(Ok(ping)).await.is_err() {
                    return;
                }
                match incoming.message().await {
                    Ok(Some(PollWorkerTaskRequest {
                        frame: Some(RequestFrame::Pong(_)),
                    })) => {
                        pongs.fetch_add(1, Ordering::SeqCst);
                    }
                    Ok(other) => {
                        eprintln!("expected Pong frame, received {other:?}");
                        return;
                    }
                    Err(error) => {
                        eprintln!("read Pong frame: {error}");
                        return;
                    }
                }
                let reconnect = PollWorkerTaskResponse {
                    frame: Some(ResponseFrame::Reconnect(PollWorkerTaskReconnect {
                        reason: "test".to_string(),
                    })),
                };
                if let Err(error) = sender.send(Ok(reconnect)).await {
                    eprintln!("send reconnect frame: {error}");
                }
            });
            Ok(Response::new(ReceiverStream::new(receiver)))
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reconnects_after_ping_and_drains_idle_stream_on_stop() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake task service");
        let address = listener.local_addr().expect("fake service address");
        let service = ReconnectService {
            registrations: Arc::new(AtomicUsize::new(0)),
            pongs: Arc::new(AtomicUsize::new(0)),
            notify: Arc::new(Notify::new()),
        };
        let (server_stop, mut server_shutdown) = watch::channel(false);
        let server = tokio::spawn(
            Server::builder()
                .add_service(WorkerTaskServiceServer::new(service.clone()))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    while !*server_shutdown.borrow() && server_shutdown.changed().await.is_ok() {}
                }),
        );
        let channel = Endpoint::from_shared(format!("http://{address}"))
            .expect("endpoint")
            .connect_lazy();
        let (cache, _directory) = test_cache();
        let dispatcher = WorkerDispatcher::new(
            Registry::new(),
            ValueHydrator::new(FlowServiceClient::new(channel.clone()), cache),
        );
        let worker =
            PullWorker::new(&PullWorkerOptions::new(), &dispatcher, channel).expect("pull Worker");
        let (stop, shutdown) = watch::channel(false);
        let running = tokio::spawn(async move { worker.run(shutdown).await });
        timeout(Duration::from_secs(5), async {
            while service.registrations.load(Ordering::SeqCst) < 2 {
                service.notify.notified().await;
            }
        })
        .await
        .expect("second registration");
        assert!(service.pongs.load(Ordering::SeqCst) >= 1);
        stop.send_replace(true);
        timeout(Duration::from_secs(5), running)
            .await
            .expect("pull Worker stopped")
            .expect("join Worker")
            .expect("Worker result");
        server_stop.send_replace(true);
        timeout(Duration::from_secs(5), server)
            .await
            .expect("fake server stopped")
            .expect("join server")
            .expect("server result");
    }

    #[derive(Clone)]
    struct RejectService {
        rejected: mpsc::Sender<PollWorkerTaskReject>,
    }

    #[tonic::async_trait]
    impl WorkerTaskService for RejectService {
        type PollWorkerTaskStream = ReceiverStream<Result<PollWorkerTaskResponse, Status>>;

        async fn poll_worker_task(
            &self,
            request: Request<tonic::Streaming<PollWorkerTaskRequest>>,
        ) -> Result<Response<Self::PollWorkerTaskStream>, Status> {
            let mut incoming = request.into_inner();
            let first = incoming
                .message()
                .await?
                .ok_or_else(|| Status::invalid_argument("missing Register"))?;
            if !matches!(first.frame, Some(RequestFrame::Register(_))) {
                return Err(Status::invalid_argument("Register must be first"));
            }
            let (sender, receiver) = mpsc::channel(2);
            let rejected = self.rejected.clone();
            tokio::spawn(async move {
                let registered = PollWorkerTaskResponse {
                    frame: Some(ResponseFrame::Registered(PollWorkerTaskRegistered {})),
                };
                if sender.send(Ok(registered)).await.is_err() {
                    return;
                }
                let assigned = PollWorkerTaskResponse {
                    frame: Some(ResponseFrame::Task(PollWorkerTask {
                        task_id: "assigned-1".to_string(),
                        deadline: None,
                        request: None,
                    })),
                };
                if sender.send(Ok(assigned)).await.is_err() {
                    return;
                }
                match incoming.message().await {
                    Ok(Some(PollWorkerTaskRequest {
                        frame: Some(RequestFrame::Reject(frame)),
                    })) => {
                        if let Err(error) = rejected.send(frame).await {
                            eprintln!("record Reject frame: {error}");
                        }
                    }
                    Ok(other) => eprintln!("expected Reject frame, received {other:?}"),
                    Err(error) => eprintln!("read Reject frame: {error}"),
                }
            });
            Ok(Response::new(ReceiverStream::new(receiver)))
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sends_no_capacity_reject_frame() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake task service");
        let address = listener.local_addr().expect("fake service address");
        let (rejected, mut rejected_frames) = mpsc::channel(1);
        let (server_stop, mut server_shutdown) = watch::channel(false);
        let server = tokio::spawn(
            Server::builder()
                .add_service(WorkerTaskServiceServer::new(RejectService { rejected }))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    while !*server_shutdown.borrow() && server_shutdown.changed().await.is_ok() {}
                }),
        );
        let channel = Endpoint::from_shared(format!("http://{address}"))
            .expect("endpoint")
            .connect_lazy();
        let (cache, _directory) = test_cache();
        let dispatcher = WorkerDispatcher::new(
            Registry::new(),
            ValueHydrator::new(FlowServiceClient::new(channel.clone()), cache),
        );
        let shared = Arc::new(PullShared {
            state: Mutex::new(PullState {
                streams: HashMap::from([(
                    1,
                    StreamSlot {
                        queue: "worker-1".to_string(),
                        is_busy: false,
                    },
                )]),
                busy_tasks: 1,
                next_stream_id: 2,
                is_stopping: false,
                logged_unsupported: false,
            }),
            notify: Notify::new(),
            max_concurrent_tasks: 1,
        });
        let (_stop, stop_receiver) = watch::channel(0u8);
        let mut stream = PullStream {
            client: WorkerTaskServiceClient::new(channel),
            dispatcher,
            app_worker_id: "worker-1".to_string(),
            flow_types: Vec::new(),
            queue: "worker-1".to_string(),
            stream_id: 1,
            shared,
            stop: stop_receiver,
        };
        let (was_registered, _, result) = timeout(Duration::from_secs(5), stream.poll())
            .await
            .expect("poll returned");
        assert!(was_registered);
        result.expect("poll result");
        let reject = rejected_frames.recv().await.expect("Reject frame");
        assert_eq!("assigned-1", reject.task_id);
        assert_eq!(PollWorkerTaskRejectReason::NoCapacity as i32, reject.reason);
        server_stop.send_replace(true);
        timeout(Duration::from_secs(5), server)
            .await
            .expect("fake server stopped")
            .expect("join server")
            .expect("server result");
    }

    const GET_DRAIN_RESULT: Rpc<(), String> = Rpc::new("get_drain_result");

    struct DrainFlow {
        started: Arc<Notify>,
        release: Arc<(Mutex<bool>, Condvar)>,
        wait_for_cancel: bool,
    }

    impl DrainFlow {
        fn get_drain_result(&self, context: &mut Context) -> HandlerResult<RpcResult<String>> {
            self.started.notify_one();
            if self.wait_for_cancel {
                context.wait_for_cancellation();
                return Err(HandlerError::new("Cancelled", "handler was cancelled"));
            }
            let (lock, changed) = &*self.release;
            let mut is_released = lock.lock().expect("drain gate lock");
            while !*is_released {
                is_released = changed.wait(is_released).expect("drain gate lock");
            }
            Ok(RpcResult::new("done".to_string()))
        }
    }

    impl Flow for DrainFlow {
        type StartInput = ();

        fn rpcs(&self) -> RpcList<Self> {
            RpcList::new().function_without_input(GET_DRAIN_RESULT, Self::get_drain_result)
        }
    }

    enum DrainEvent {
        FinalFrame(PollWorkerTaskFrame),
        HalfClosed,
        ReadError(Status),
    }

    #[derive(Clone)]
    struct DrainService {
        events: mpsc::Sender<DrainEvent>,
        assigned: Arc<AtomicBool>,
        cancel_kind: Option<Code>,
    }

    #[tonic::async_trait]
    impl WorkerTaskService for DrainService {
        type PollWorkerTaskStream = ReceiverStream<Result<PollWorkerTaskResponse, Status>>;

        async fn poll_worker_task(
            &self,
            request: Request<tonic::Streaming<PollWorkerTaskRequest>>,
        ) -> Result<Response<Self::PollWorkerTaskStream>, Status> {
            let mut incoming = request.into_inner();
            let first = incoming
                .message()
                .await?
                .ok_or_else(|| Status::invalid_argument("missing Register"))?;
            let Some(RequestFrame::Register(register)) = first.frame else {
                return Err(Status::invalid_argument("Register must be first"));
            };
            let (sender, receiver) = mpsc::channel(4);
            let registered = PollWorkerTaskResponse {
                frame: Some(ResponseFrame::Registered(PollWorkerTaskRegistered {})),
            };
            sender
                .send(Ok(registered))
                .await
                .map_err(|_| Status::internal("registration response closed"))?;
            let is_task_stream =
                register.queue == "DrainFlow" && !self.assigned.swap(true, Ordering::AcqRel);
            if is_task_stream {
                let request = InvokeWorkerRpcRequest {
                    context: Some(ProtoContext {
                        flow_id: "flow-1".to_string(),
                        run_id: "run-1".to_string(),
                        ..Default::default()
                    }),
                    flow_type: "DrainFlow".to_string(),
                    rpc_name: "get_drain_result".to_string(),
                    input: Some(
                        crate::value_mapper::encode(&())
                            .map_err(|error| Status::internal(error.to_string()))?,
                    ),
                    ..Default::default()
                };
                let task = PollWorkerTaskResponse {
                    frame: Some(ResponseFrame::Task(PollWorkerTask {
                        task_id: "drain-task".to_string(),
                        deadline: (self.cancel_kind == Some(Code::DeadlineExceeded)).then_some(
                            prost_types::Timestamp {
                                seconds: 0,
                                nanos: 0,
                            },
                        ),
                        request: Some(TaskRequest::Rpc(request)),
                    })),
                };
                sender
                    .send(Ok(task))
                    .await
                    .map_err(|_| Status::internal("task response closed"))?;
                if self.cancel_kind == Some(Code::Cancelled) {
                    sender
                        .send(Ok(PollWorkerTaskResponse {
                            frame: Some(ResponseFrame::CancelTask(PollWorkerTaskCancelTask {
                                task_id: "drain-task".to_string(),
                            })),
                        }))
                        .await
                        .map_err(|_| Status::internal("cancel response closed"))?;
                }
            }
            let events = self.events.clone();
            tokio::spawn(async move {
                loop {
                    match incoming.message().await {
                        Ok(Some(PollWorkerTaskRequest {
                            frame: Some(RequestFrame::TaskFrame(frame)),
                        })) if is_task_stream => {
                            if let Err(error) = events.send(DrainEvent::FinalFrame(frame)).await {
                                eprintln!("record final frame: {error}");
                                break;
                            }
                        }
                        Ok(Some(_)) => {}
                        Ok(None) => {
                            if is_task_stream
                                && let Err(error) = events.send(DrainEvent::HalfClosed).await
                            {
                                eprintln!("record half-close: {error}");
                            }
                            break;
                        }
                        Err(status) => {
                            if is_task_stream
                                && let Err(error) = events.send(DrainEvent::ReadError(status)).await
                            {
                                eprintln!("record stream error: {error}");
                            }
                            break;
                        }
                    }
                }
                drop(sender);
            });
            Ok(Response::new(ReceiverStream::new(receiver)))
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stop_drain_delivers_final_frame_before_half_close() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake task service");
        let address = listener.local_addr().expect("fake service address");
        let (events, mut received) = mpsc::channel(4);
        let (server_stop, mut server_shutdown) = watch::channel(false);
        let server = tokio::spawn(
            Server::builder()
                .add_service(WorkerTaskServiceServer::new(DrainService {
                    events,
                    assigned: Arc::new(AtomicBool::new(false)),
                    cancel_kind: None,
                }))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    while !*server_shutdown.borrow() && server_shutdown.changed().await.is_ok() {}
                }),
        );
        let channel = Endpoint::from_shared(format!("http://{address}"))
            .expect("endpoint")
            .connect_lazy();
        let (cache, _directory) = test_cache();
        let started = Arc::new(Notify::new());
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let registry = Registry::new()
            .register(DrainFlow {
                started: Arc::clone(&started),
                release: Arc::clone(&release),
                wait_for_cancel: false,
            })
            .expect("register Drain Flow");
        let dispatcher = WorkerDispatcher::new(
            registry,
            ValueHydrator::new(FlowServiceClient::new(channel.clone()), cache),
        );
        let worker = PullWorker::new(
            &PullWorkerOptions::new().app_worker_id("worker-drain"),
            &dispatcher,
            channel,
        )
        .expect("pull Worker");
        let (stop, shutdown) = watch::channel(false);
        let running = tokio::spawn(async move { worker.run(shutdown).await });
        timeout(Duration::from_secs(5), started.notified())
            .await
            .expect("RPC started");
        stop.send_replace(true);
        assert!(!running.is_finished());
        let (lock, changed) = &*release;
        *lock.lock().expect("drain gate lock") = true;
        changed.notify_all();
        timeout(Duration::from_secs(5), running)
            .await
            .expect("Worker stopped")
            .expect("join Worker")
            .expect("Worker result");
        let final_frame = timeout(Duration::from_secs(5), received.recv())
            .await
            .expect("final frame received")
            .expect("final frame event");
        match final_frame {
            DrainEvent::FinalFrame(frame) => {
                assert_eq!("drain-task", frame.task_id);
                assert!(matches!(frame.output, Some(Output::Rpc(_))));
            }
            DrainEvent::HalfClosed => panic!("stream closed before final frame"),
            DrainEvent::ReadError(error) => panic!("stream failed before final frame: {error}"),
        }
        let closed = timeout(Duration::from_secs(5), received.recv())
            .await
            .expect("half-close received")
            .expect("half-close event");
        assert!(matches!(closed, DrainEvent::HalfClosed));
        server_stop.send_replace(true);
        timeout(Duration::from_secs(5), server)
            .await
            .expect("fake server stopped")
            .expect("join server")
            .expect("server result");
    }

    async fn cancellation_frame_has_status(cancel_kind: Code) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake task service");
        let address = listener.local_addr().expect("fake service address");
        let (events, mut received) = mpsc::channel(4);
        let (server_stop, mut server_shutdown) = watch::channel(false);
        let server = tokio::spawn(
            Server::builder()
                .add_service(WorkerTaskServiceServer::new(DrainService {
                    events,
                    assigned: Arc::new(AtomicBool::new(false)),
                    cancel_kind: Some(cancel_kind),
                }))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    while !*server_shutdown.borrow() && server_shutdown.changed().await.is_ok() {}
                }),
        );
        let channel = Endpoint::from_shared(format!("http://{address}"))
            .expect("endpoint")
            .connect_lazy();
        let (cache, _directory) = test_cache();
        let started = Arc::new(Notify::new());
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let registry = Registry::new()
            .register(DrainFlow {
                started,
                release,
                wait_for_cancel: true,
            })
            .expect("register cancellation Flow");
        let dispatcher = WorkerDispatcher::new(
            registry,
            ValueHydrator::new(FlowServiceClient::new(channel.clone()), cache),
        );
        let worker = PullWorker::new(
            &PullWorkerOptions::new().app_worker_id("worker-cancel"),
            &dispatcher,
            channel,
        )
        .expect("pull Worker");
        let (stop, shutdown) = watch::channel(false);
        let running = tokio::spawn(async move { worker.run(shutdown).await });
        let event = timeout(Duration::from_secs(5), received.recv())
            .await
            .expect("error frame received")
            .expect("error frame event");
        match event {
            DrainEvent::FinalFrame(frame) => {
                let Some(Output::WorkerError(error)) = frame.output else {
                    panic!("expected Worker error frame");
                };
                assert_eq!(cancel_kind as i32, error.grpc_status);
                assert_eq!("Cancelled", error.error_type);
            }
            DrainEvent::HalfClosed => panic!("stream closed before error frame"),
            DrainEvent::ReadError(error) => panic!("stream failed before error frame: {error}"),
        }
        stop.send_replace(true);
        timeout(Duration::from_secs(5), running)
            .await
            .expect("Worker stopped")
            .expect("join Worker")
            .expect("Worker result");
        server_stop.send_replace(true);
        timeout(Duration::from_secs(5), server)
            .await
            .expect("fake server stopped")
            .expect("join server")
            .expect("server result");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cancel_task_reports_cancelled_status() {
        cancellation_frame_has_status(Code::Cancelled).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deadline_reports_deadline_exceeded_status() {
        cancellation_frame_has_status(Code::DeadlineExceeded).await;
    }
}
