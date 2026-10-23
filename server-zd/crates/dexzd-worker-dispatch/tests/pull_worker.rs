// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use dexzd_clock::SystemWallClock;
use dexzd_protocol::dex::{
    self, DispatchTaskErrorReason, InvokeWaitForMethodOutput, InvokeWaitForMethodRequest,
    InvokeWaitForMethodResponse, InvokeWorkerRpcRequest, PollWorkerTaskFrame,
    PollWorkerTaskRegister, PollWorkerTaskReject, PollWorkerTaskRejectReason,
    PollWorkerTaskRequest, PollWorkerTaskResponse, StepMethodHeartbeat, StepStreamWrite, Value,
    WorkerErrorResponse, poll_worker_task_frame, poll_worker_task_request,
    poll_worker_task_response, worker_task_service_client::WorkerTaskServiceClient,
    worker_task_service_server::WorkerTaskServiceServer,
};
use dexzd_worker_dispatch::{
    DispatchEvent, DispatchOutcome, DispatchRequest, DispatchTaskPayload, MatchingConfig,
    WorkerDispatch,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::{TcpListenerStream, UnboundedReceiverStream};
use tonic::transport::{Channel, Server};
use tonic::{Code, Request, Streaming};

struct TestServer {
    channel: Channel,
    task: JoinHandle<()>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct TestWorker {
    sender: mpsc::UnboundedSender<PollWorkerTaskRequest>,
    stream: Streaming<PollWorkerTaskResponse>,
}

impl TestWorker {
    async fn next(&mut self) -> PollWorkerTaskResponse {
        tokio::time::timeout(Duration::from_secs(3), self.stream.message())
            .await
            .expect("test operation succeeds")
            .expect("test operation succeeds")
            .expect("test operation succeeds")
    }

    fn send(&self, frame: poll_worker_task_request::Frame) {
        self.sender
            .send(PollWorkerTaskRequest { frame: Some(frame) })
            .expect("test operation succeeds");
    }
}

async fn start_server(dispatch: WorkerDispatch, tenant_id: u64) -> TestServer {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test operation succeeds");
    let address = listener.local_addr().expect("test operation succeeds");
    let task = tokio::spawn(async move {
        Server::builder()
            .add_service(WorkerTaskServiceServer::new(
                dispatch.service_for_tenant(tenant_id),
            ))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("test operation succeeds");
    });
    let channel = Channel::from_shared(format!("http://{address}"))
        .expect("test operation succeeds")
        .connect()
        .await
        .expect("test operation succeeds");
    TestServer { channel, task }
}

fn dispatch_with_config(config: MatchingConfig) -> WorkerDispatch {
    WorkerDispatch::new(config, Arc::new(SystemWallClock))
}

fn registration(queue: &str, worker_id: &str) -> PollWorkerTaskRegister {
    PollWorkerTaskRegister {
        queue: queue.to_owned(),
        app_worker_id: worker_id.to_owned(),
        flow_types: vec!["Flow".to_owned()],
        ..Default::default()
    }
}

async fn register_worker(channel: &Channel, registration: PollWorkerTaskRegister) -> TestWorker {
    let (sender, receiver) = mpsc::unbounded_channel();
    sender
        .send(PollWorkerTaskRequest {
            frame: Some(poll_worker_task_request::Frame::Register(registration)),
        })
        .expect("test operation succeeds");
    let mut client = WorkerTaskServiceClient::new(channel.clone());
    let response = client
        .poll_worker_task(Request::new(UnboundedReceiverStream::new(receiver)))
        .await
        .expect("test operation succeeds");
    let mut worker = TestWorker {
        sender,
        stream: response.into_inner(),
    };
    assert!(
        matches!(worker.next().await.frame, Some(poll_worker_task_response::Frame::Registered(registered)) if registered.supports_unknown_name_rejection)
    );
    worker
}

fn request(tenant_id: u64, flow_id: &str) -> DispatchRequest {
    DispatchRequest {
        tenant_id,
        flow_id: flow_id.to_owned(),
        flow_type: "Flow".to_owned(),
        sticky_app_worker_id: None,
        payload: DispatchTaskPayload::WaitFor(InvokeWaitForMethodRequest {
            flow_type: "Flow".to_owned(),
            step_type: "WorkStep".to_owned(),
            ..Default::default()
        }),
        deadline: SystemTime::now() + Duration::from_secs(10),
        sticky_wait: None,
        dispatch_wait: Some(Duration::from_millis(400)),
    }
}

fn heartbeat(task_id: &str) -> PollWorkerTaskRequest {
    PollWorkerTaskRequest {
        frame: Some(poll_worker_task_request::Frame::TaskFrame(
            PollWorkerTaskFrame {
                task_id: task_id.to_owned(),
                output: Some(poll_worker_task_frame::Output::WaitFor(
                    InvokeWaitForMethodOutput {
                        output: Some(dex::invoke_wait_for_method_output::Output::Heartbeat(
                            StepMethodHeartbeat::default(),
                        )),
                    },
                )),
            },
        )),
    }
}

fn result(task_id: &str) -> PollWorkerTaskRequest {
    PollWorkerTaskRequest {
        frame: Some(poll_worker_task_request::Frame::TaskFrame(
            PollWorkerTaskFrame {
                task_id: task_id.to_owned(),
                output: Some(poll_worker_task_frame::Output::WaitFor(
                    InvokeWaitForMethodOutput {
                        output: Some(dex::invoke_wait_for_method_output::Output::Result(
                            InvokeWaitForMethodResponse::default(),
                        )),
                    },
                )),
            },
        )),
    }
}

async fn task_id(worker: &mut TestWorker) -> String {
    match worker.next().await.frame.expect("test operation succeeds") {
        poll_worker_task_response::Frame::Task(task) => task.task_id,
        other => panic!("expected task, got {other:?}"),
    }
}

async fn completed(handle: &mut dexzd_worker_dispatch::DispatchHandle) -> DispatchOutcome {
    loop {
        match tokio::time::timeout(Duration::from_secs(3), handle.next())
            .await
            .expect("test operation succeeds")
            .expect("test operation succeeds")
        {
            DispatchEvent::Completed(outcome) => return outcome,
            DispatchEvent::Output(_) => {}
        }
    }
}

#[tokio::test]
async fn registration_validates_every_field_and_first_frame() {
    let dispatch = dispatch_with_config(MatchingConfig::default());
    let server = start_server(dispatch, 0).await;
    let mut invalid = vec![
        registration("Flow", ""),
        registration("Flow", &"w".repeat(129)),
        registration("Flow", "bad/id"),
        registration("", "worker"),
        registration(&"q".repeat(257), "worker"),
        registration("Other", "worker"),
    ];
    let mut too_many = registration("Flow", "worker");
    too_many.flow_types = vec!["Flow".to_owned(); 257];
    invalid.push(too_many);
    let mut too_long = registration("Flow", "worker");
    too_long.flow_types.push("F".repeat(257));
    invalid.push(too_long);
    for candidate in invalid {
        let (sender, receiver) = mpsc::unbounded_channel();
        sender
            .send(PollWorkerTaskRequest {
                frame: Some(poll_worker_task_request::Frame::Register(candidate)),
            })
            .expect("test operation succeeds");
        let mut client = WorkerTaskServiceClient::new(server.channel.clone());
        let error = client
            .poll_worker_task(Request::new(UnboundedReceiverStream::new(receiver)))
            .await
            .expect_err("registration must fail");
        assert_eq!(error.code(), Code::InvalidArgument);
    }
    let (sender, receiver) = mpsc::unbounded_channel();
    sender
        .send(PollWorkerTaskRequest {
            frame: Some(poll_worker_task_request::Frame::Pong(Default::default())),
        })
        .expect("test operation succeeds");
    let mut client = WorkerTaskServiceClient::new(server.channel.clone());
    let error = client
        .poll_worker_task(Request::new(UnboundedReceiverStream::new(receiver)))
        .await
        .expect_err("registration must fail");
    assert_eq!(error.code(), Code::InvalidArgument);
    let mut worker = register_worker(&server.channel, registration("Flow", "worker")).await;
    worker.send(poll_worker_task_request::Frame::Register(registration(
        "Flow", "worker",
    )));
    let error = worker
        .stream
        .message()
        .await
        .expect_err("registration must fail");
    assert_eq!(error.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn dispatch_relays_heartbeats_stream_writes_and_result_in_order() {
    let dispatch = dispatch_with_config(MatchingConfig::default());
    let server = start_server(dispatch.clone(), 0).await;
    let mut worker = register_worker(&server.channel, registration("Flow", "worker")).await;
    let mut handle = dispatch
        .dispatch(request(0, "flow-a"))
        .expect("test operation succeeds");
    let assigned = task_id(&mut worker).await;
    assert_eq!(
        uuid::Uuid::parse_str(&assigned)
            .expect("test operation succeeds")
            .get_version_num(),
        7
    );
    worker
        .sender
        .send(heartbeat("wrong-task"))
        .expect("test operation succeeds");
    worker
        .sender
        .send(heartbeat(&assigned))
        .expect("test operation succeeds");
    worker.send(poll_worker_task_request::Frame::TaskFrame(
        PollWorkerTaskFrame {
            task_id: assigned.clone(),
            output: Some(poll_worker_task_frame::Output::WaitFor(
                InvokeWaitForMethodOutput {
                    output: Some(dex::invoke_wait_for_method_output::Output::Heartbeat(
                        StepMethodHeartbeat {
                            value: Some(Value::default()),
                        },
                    )),
                },
            )),
        },
    ));
    worker.send(poll_worker_task_request::Frame::TaskFrame(
        PollWorkerTaskFrame {
            task_id: assigned.clone(),
            output: Some(poll_worker_task_frame::Output::WaitFor(
                InvokeWaitForMethodOutput {
                    output: Some(dex::invoke_wait_for_method_output::Output::StreamWrite(
                        StepStreamWrite {
                            stream_name: "events".to_owned(),
                            ..Default::default()
                        },
                    )),
                },
            )),
        },
    ));
    worker
        .sender
        .send(result(&assigned))
        .expect("test operation succeeds");
    assert!(matches!(
        handle.next().await,
        Some(DispatchEvent::Output(PollWorkerTaskFrame {
            output: Some(poll_worker_task_frame::Output::WaitFor(
                InvokeWaitForMethodOutput {
                    output: Some(dex::invoke_wait_for_method_output::Output::Heartbeat(_))
                }
            )),
            ..
        }))
    ));
    assert!(matches!(
        handle.next().await,
        Some(DispatchEvent::Output(PollWorkerTaskFrame {
            output: Some(poll_worker_task_frame::Output::WaitFor(
                InvokeWaitForMethodOutput {
                    output: Some(dex::invoke_wait_for_method_output::Output::Heartbeat(
                        StepMethodHeartbeat { value: Some(_) }
                    ))
                }
            )),
            ..
        }))
    ));
    assert!(matches!(
        handle.next().await,
        Some(DispatchEvent::Output(PollWorkerTaskFrame {
            output: Some(poll_worker_task_frame::Output::WaitFor(
                InvokeWaitForMethodOutput {
                    output: Some(dex::invoke_wait_for_method_output::Output::StreamWrite(_))
                }
            )),
            ..
        }))
    ));
    assert!(
        matches!(completed(&mut handle).await, DispatchOutcome::WorkerResult { app_worker_id, .. } if app_worker_id == "worker")
    );
    assert!(handle.next().await.is_none());
}

#[tokio::test]
async fn sticky_worker_is_preferred_and_missing_sticky_queue_falls_back() {
    let dispatch = dispatch_with_config(MatchingConfig::default());
    let server = start_server(dispatch.clone(), 0).await;
    let mut flow_worker =
        register_worker(&server.channel, registration("Flow", "flow-worker")).await;
    let mut sticky_worker = register_worker(
        &server.channel,
        registration("sticky-worker", "sticky-worker"),
    )
    .await;
    let mut first = request(0, "flow-sticky");
    first.sticky_app_worker_id = Some("sticky-worker".to_owned());
    let mut first_handle = dispatch.dispatch(first).expect("test operation succeeds");
    let sticky_task = task_id(&mut sticky_worker).await;
    sticky_worker
        .sender
        .send(result(&sticky_task))
        .expect("test operation succeeds");
    assert!(
        matches!(completed(&mut first_handle).await, DispatchOutcome::WorkerResult { app_worker_id, .. } if app_worker_id == "sticky-worker")
    );
    let mut second = request(0, "flow-fallback");
    second.sticky_app_worker_id = Some("absent-worker".to_owned());
    let started = Instant::now();
    let mut second_handle = dispatch.dispatch(second).expect("test operation succeeds");
    let fallback_task = task_id(&mut flow_worker).await;
    assert!(started.elapsed() < Duration::from_millis(250));
    flow_worker
        .sender
        .send(result(&fallback_task))
        .expect("test operation succeeds");
    assert!(
        matches!(completed(&mut second_handle).await, DispatchOutcome::WorkerResult { app_worker_id, .. } if app_worker_id == "flow-worker")
    );
}

#[tokio::test]
async fn capacity_rejection_backs_off_and_unknown_name_excludes_worker_id() {
    let dispatch = dispatch_with_config(MatchingConfig::default());
    let server = start_server(dispatch.clone(), 0).await;
    let mut worker = register_worker(&server.channel, registration("Flow", "worker")).await;
    let mut capacity_request = request(0, "capacity");
    capacity_request.dispatch_wait = Some(Duration::from_millis(1_400));
    let mut handle = dispatch
        .dispatch(capacity_request)
        .expect("test operation succeeds");
    let first = task_id(&mut worker).await;
    let rejected_at = Instant::now();
    worker.send(poll_worker_task_request::Frame::Reject(
        PollWorkerTaskReject {
            task_id: first,
            reason: PollWorkerTaskRejectReason::NoCapacity as i32,
        },
    ));
    let second = task_id(&mut worker).await;
    assert!(rejected_at.elapsed() >= Duration::from_millis(290));
    let rejected_again_at = Instant::now();
    worker.send(poll_worker_task_request::Frame::Reject(
        PollWorkerTaskReject {
            task_id: second.clone(),
            reason: PollWorkerTaskRejectReason::NoCapacity as i32,
        },
    ));
    let third = task_id(&mut worker).await;
    assert_eq!(second, third);
    assert!(rejected_again_at.elapsed() >= Duration::from_millis(590));
    worker
        .sender
        .send(result(&third))
        .expect("test operation succeeds");
    assert!(matches!(
        completed(&mut handle).await,
        DispatchOutcome::WorkerResult { .. }
    ));
    let mut same_worker_stream =
        register_worker(&server.channel, registration("Flow", "worker")).await;
    let mut missing = request(0, "missing");
    missing.dispatch_wait = Some(Duration::from_millis(50));
    let mut missing_handle = dispatch.dispatch(missing).expect("test operation succeeds");
    let missing_task = task_id(&mut worker).await;
    worker.send(poll_worker_task_request::Frame::Reject(
        PollWorkerTaskReject {
            task_id: missing_task,
            reason: PollWorkerTaskRejectReason::UnknownStep as i32,
        },
    ));
    assert!(matches!(
        completed(&mut missing_handle).await,
        DispatchOutcome::DispatchError {
            reason: DispatchTaskErrorReason::UnknownName,
            ..
        }
    ));
    assert!(
        tokio::time::timeout(
            Duration::from_millis(30),
            same_worker_stream.stream.message()
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn capacity_rejection_without_an_accepting_worker_returns_no_worker_available() {
    let dispatch = dispatch_with_config(MatchingConfig::default());
    let server = start_server(dispatch.clone(), 0).await;
    let mut worker = register_worker(&server.channel, registration("Flow", "worker")).await;
    let mut short = request(0, "full-worker");
    short.dispatch_wait = Some(Duration::from_millis(100));
    let mut handle = dispatch.dispatch(short).expect("dispatch starts");
    let assigned = task_id(&mut worker).await;
    worker.send(poll_worker_task_request::Frame::Reject(
        PollWorkerTaskReject {
            task_id: assigned,
            reason: PollWorkerTaskRejectReason::NoCapacity as i32,
        },
    ));
    assert!(matches!(
        completed(&mut handle).await,
        DispatchOutcome::DispatchError {
            reason: DispatchTaskErrorReason::NoWorkerAvailable,
            ..
        }
    ));
}

#[tokio::test]
async fn disconnect_replays_before_output_and_fails_after_output() {
    let dispatch = dispatch_with_config(MatchingConfig::default());
    let server = start_server(dispatch.clone(), 0).await;
    let mut first = register_worker(&server.channel, registration("Flow", "first")).await;
    let mut second = register_worker(&server.channel, registration("Flow", "second")).await;
    let mut handle = dispatch
        .dispatch(request(0, "replay"))
        .expect("test operation succeeds");
    let first_task = task_id(&mut first).await;
    drop(first.sender);
    let replayed = task_id(&mut second).await;
    assert_eq!(first_task, replayed);
    second
        .sender
        .send(result(&replayed))
        .expect("test operation succeeds");
    assert!(
        matches!(completed(&mut handle).await, DispatchOutcome::WorkerResult { app_worker_id, .. } if app_worker_id == "second")
    );
    let mut after_output = dispatch
        .dispatch(request(0, "after-output"))
        .expect("test operation succeeds");
    let assigned = task_id(&mut second).await;
    second
        .sender
        .send(heartbeat(&assigned))
        .expect("test operation succeeds");
    assert!(matches!(
        after_output.next().await,
        Some(DispatchEvent::Output(_))
    ));
    drop(second.sender);
    assert!(matches!(
        completed(&mut after_output).await,
        DispatchOutcome::DispatchError {
            reason: DispatchTaskErrorReason::WorkerDisconnected,
            ..
        }
    ));
}

#[tokio::test]
async fn cancellation_waits_for_final_frame_or_drops_stream_after_one_second() {
    let dispatch = dispatch_with_config(MatchingConfig::default());
    let server = start_server(dispatch.clone(), 0).await;
    let mut worker = register_worker(&server.channel, registration("Flow", "worker")).await;
    let mut first = dispatch
        .dispatch(request(0, "cancel-final"))
        .expect("test operation succeeds");
    let first_task = task_id(&mut worker).await;
    first.cancel();
    assert!(
        matches!(worker.next().await.frame, Some(poll_worker_task_response::Frame::CancelTask(cancel)) if cancel.task_id == first_task)
    );
    worker
        .sender
        .send(result(&first_task))
        .expect("test operation succeeds");
    assert!(matches!(
        completed(&mut first).await,
        DispatchOutcome::Cancelled
    ));
    let mut second = dispatch
        .dispatch(request(0, "cancel-no-final"))
        .expect("test operation succeeds");
    let second_task = task_id(&mut worker).await;
    second.cancel();
    assert!(
        matches!(worker.next().await.frame, Some(poll_worker_task_response::Frame::CancelTask(cancel)) if cancel.task_id == second_task)
    );
    assert!(matches!(
        completed(&mut second).await,
        DispatchOutcome::Cancelled
    ));
    assert!(
        tokio::time::timeout(Duration::from_secs(2), worker.stream.message())
            .await
            .expect("test operation succeeds")
            .expect("test operation succeeds")
            .is_none()
    );
}

#[tokio::test]
async fn idle_ping_shutdown_reconnect_and_tenant_queues_are_isolated() {
    let config = MatchingConfig {
        ping_interval: Duration::from_millis(20),
        ..Default::default()
    };
    let dispatch = dispatch_with_config(config);
    let _tenant_zero = start_server(dispatch.clone(), 0).await;
    let tenant_one = start_server(dispatch.clone(), 1).await;
    let mut worker = register_worker(&tenant_one.channel, registration("Flow", "worker")).await;
    assert!(matches!(
        worker.next().await.frame,
        Some(poll_worker_task_response::Frame::Ping(_))
    ));
    let mut missing = request(0, "tenant-zero");
    missing.dispatch_wait = Some(Duration::from_millis(30));
    let mut missing_handle = dispatch.dispatch(missing).expect("test operation succeeds");
    assert!(matches!(
        completed(&mut missing_handle).await,
        DispatchOutcome::DispatchError {
            reason: DispatchTaskErrorReason::NoWorkerAvailable,
            ..
        }
    ));
    let mut tenant_one_handle = dispatch
        .dispatch(request(1, "tenant-one"))
        .expect("test operation succeeds");
    let assigned = loop {
        match worker.next().await.frame.expect("test operation succeeds") {
            poll_worker_task_response::Frame::Task(task) => break task.task_id,
            poll_worker_task_response::Frame::Ping(_) => {}
            other => panic!("unexpected {other:?}"),
        }
    };
    worker
        .sender
        .send(result(&assigned))
        .expect("test operation succeeds");
    assert!(matches!(
        completed(&mut tenant_one_handle).await,
        DispatchOutcome::WorkerResult { .. }
    ));
    dispatch.shutdown();
    assert!(matches!(
        worker.next().await.frame,
        Some(poll_worker_task_response::Frame::Reconnect(_))
    ));
}

#[tokio::test]
async fn node_and_worker_stream_limits_use_resource_exhausted() {
    let config = MatchingConfig {
        max_streams: 2,
        max_concurrent_tasks_cap: 1,
        ..Default::default()
    };
    let dispatch = dispatch_with_config(config);
    let server = start_server(dispatch, 0).await;
    let _first = register_worker(&server.channel, registration("Flow", "same-worker")).await;
    let (sender, receiver) = mpsc::unbounded_channel();
    sender
        .send(PollWorkerTaskRequest {
            frame: Some(poll_worker_task_request::Frame::Register(registration(
                "Flow",
                "same-worker",
            ))),
        })
        .expect("test operation succeeds");
    let mut client = WorkerTaskServiceClient::new(server.channel.clone());
    let error = client
        .poll_worker_task(Request::new(UnboundedReceiverStream::new(receiver)))
        .await
        .expect_err("Worker stream limit must fail");
    assert_eq!(error.code(), Code::ResourceExhausted);
    let _second = register_worker(&server.channel, registration("Flow", "other-worker")).await;
    {
        let worker_id = "third-worker";
        let (sender, receiver) = mpsc::unbounded_channel();
        sender
            .send(PollWorkerTaskRequest {
                frame: Some(poll_worker_task_request::Frame::Register(registration(
                    "Flow", worker_id,
                ))),
            })
            .expect("test operation succeeds");
        let mut client = WorkerTaskServiceClient::new(server.channel.clone());
        let error = client
            .poll_worker_task(Request::new(UnboundedReceiverStream::new(receiver)))
            .await
            .expect_err("registration must fail");
        assert_eq!(error.code(), Code::ResourceExhausted);
    }
}

#[tokio::test]
async fn affinity_prefers_the_same_worker_when_an_earlier_stream_is_idle() {
    let dispatch = dispatch_with_config(MatchingConfig::default());
    let server = start_server(dispatch.clone(), 0).await;
    let mut first_worker = register_worker(&server.channel, registration("Flow", "first")).await;
    let mut second_worker = register_worker(&server.channel, registration("Flow", "second")).await;
    let mut blocking = dispatch
        .dispatch(request(0, "different-flow"))
        .expect("dispatch starts");
    let blocking_task = task_id(&mut first_worker).await;
    let mut initial = dispatch
        .dispatch(request(0, "affinity-flow"))
        .expect("dispatch starts");
    let initial_task = task_id(&mut second_worker).await;
    second_worker
        .sender
        .send(result(&initial_task))
        .expect("result sends");
    assert!(matches!(
        completed(&mut initial).await,
        DispatchOutcome::WorkerResult { .. }
    ));
    first_worker
        .sender
        .send(result(&blocking_task))
        .expect("result sends");
    assert!(matches!(
        completed(&mut blocking).await,
        DispatchOutcome::WorkerResult { .. }
    ));
    let mut repeat = dispatch
        .dispatch(request(0, "affinity-flow"))
        .expect("dispatch starts");
    let repeat_task = task_id(&mut second_worker).await;
    second_worker
        .sender
        .send(result(&repeat_task))
        .expect("result sends");
    assert!(
        matches!(completed(&mut repeat).await, DispatchOutcome::WorkerResult { app_worker_id, .. } if app_worker_id == "second")
    );
}

#[tokio::test]
async fn unknown_rpc_and_worker_error_are_terminal_outcomes() {
    let dispatch = dispatch_with_config(MatchingConfig::default());
    let server = start_server(dispatch.clone(), 0).await;
    let mut worker = register_worker(&server.channel, registration("Flow", "worker")).await;
    let mut rpc_request = request(0, "rpc-flow");
    rpc_request.payload = DispatchTaskPayload::Rpc(InvokeWorkerRpcRequest {
        flow_type: "Flow".to_owned(),
        rpc_name: "getStatus".to_owned(),
        ..Default::default()
    });
    rpc_request.dispatch_wait = Some(Duration::from_millis(50));
    let mut rpc = dispatch.dispatch(rpc_request).expect("dispatch starts");
    let rpc_task = task_id(&mut worker).await;
    worker.send(poll_worker_task_request::Frame::Reject(
        PollWorkerTaskReject {
            task_id: rpc_task,
            reason: PollWorkerTaskRejectReason::UnknownRpc as i32,
        },
    ));
    assert!(matches!(
        completed(&mut rpc).await,
        DispatchOutcome::DispatchError {
            reason: DispatchTaskErrorReason::UnknownName,
            ..
        }
    ));
    let mut error_dispatch = dispatch
        .dispatch(request(0, "error-flow"))
        .expect("dispatch starts");
    let error_task = task_id(&mut worker).await;
    worker.send(poll_worker_task_request::Frame::TaskFrame(
        PollWorkerTaskFrame {
            task_id: error_task,
            output: Some(poll_worker_task_frame::Output::WorkerError(
                WorkerErrorResponse {
                    detail: "failed".to_owned(),
                    ..Default::default()
                },
            )),
        },
    ));
    assert!(
        matches!(completed(&mut error_dispatch).await, DispatchOutcome::WorkerError { app_worker_id, .. } if app_worker_id == "worker")
    );
}

#[tokio::test]
async fn busy_worker_reconnects_after_finishing_its_task() {
    let dispatch = dispatch_with_config(MatchingConfig::default());
    let server = start_server(dispatch.clone(), 0).await;
    let mut worker = register_worker(&server.channel, registration("Flow", "worker")).await;
    let mut handle = dispatch
        .dispatch(request(0, "shutdown-flow"))
        .expect("dispatch starts");
    let assigned = task_id(&mut worker).await;
    dispatch.shutdown();
    assert!(
        tokio::time::timeout(Duration::from_millis(40), worker.stream.message())
            .await
            .is_err()
    );
    worker.sender.send(result(&assigned)).expect("result sends");
    assert!(matches!(
        completed(&mut handle).await,
        DispatchOutcome::WorkerResult { .. }
    ));
    assert!(matches!(
        worker.next().await.frame,
        Some(poll_worker_task_response::Frame::Reconnect(_))
    ));
    match dispatch.dispatch(request(0, "new-flow")) {
        Err(status) => assert_eq!(status.code(), Code::Unavailable),
        Ok(_) => panic!("dispatch must stop"),
    }
}
