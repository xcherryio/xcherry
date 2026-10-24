// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::net::SocketAddr;
use std::time::Duration;

use dexzd_protocol::dex::flow_service_client::FlowServiceClient;
use dexzd_protocol::dex::worker_task_service_client::WorkerTaskServiceClient;
use dexzd_protocol::dex::{
    InvokeExecuteMethodOutput, InvokeExecuteMethodRequest, InvokeExecuteMethodResponse,
    InvokeWaitForMethodOutput, InvokeWaitForMethodResponse, PollWorkerTask, PollWorkerTaskFrame,
    PollWorkerTaskRegister, PollWorkerTaskRequest, PollWorkerTaskResponse, StepDecision,
    WaitingCondition, invoke_execute_method_output, invoke_wait_for_method_output,
    poll_worker_task, poll_worker_task_frame, poll_worker_task_request, poll_worker_task_response,
};
use dexzd_server::{DevCommandOptions, RunningServer};
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Channel;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

/// Service names that `dexcli dev` checks before it reports readiness.
const READINESS_SERVICE_NAMES: [&str; 3] = ["", "dex.FlowService", "dex.WorkerTaskService"];

/// A dev server started in this process on a free loopback port, with its data in a temporary directory.
pub(crate) struct DevServer {
    server: Option<RunningServer>,
    channel: Channel,
    _data_directory: TempDir,
}

impl DevServer {
    /// Starts `dex-server-zd dev` in process and waits until every health name is SERVING.
    pub(crate) async fn start() -> Self {
        Self::start_with_config_overrides(None).await
    }

    /// Starts the dev server like [`DevServer::start`], with `yaml` as the `--config` override file.
    pub(crate) async fn start_with_config_overrides(yaml: Option<&str>) -> Self {
        let data_directory = TempDir::new().expect("a temporary data directory can be created");
        let config = yaml.map(|yaml| {
            let path = data_directory.path().join("overrides.yaml");
            std::fs::write(&path, yaml).expect("the override file can be written");
            path
        });
        let server = dexzd_server::start_dev_server(&DevCommandOptions {
            data_dir: data_directory.path().to_path_buf(),
            listen: free_loopback_address(),
            log_dir: None,
            config,
        })
        .await
        .expect("the dev server starts");
        let channel = Channel::from_shared(format!("http://{}", server.local_address()))
            .expect("the server address is a valid URI")
            .connect()
            .await
            .expect("the gRPC channel connects");
        wait_until_serving(channel.clone()).await;
        Self {
            server: Some(server),
            channel,
            _data_directory: data_directory,
        }
    }

    /// Returns the connected channel, for calls that set their own gRPC timeout.
    pub(crate) fn channel(&self) -> Channel {
        self.channel.clone()
    }

    pub(crate) fn flow_client(&self) -> FlowServiceClient<Channel> {
        FlowServiceClient::new(self.channel.clone())
    }

    pub(crate) fn worker_client(&self) -> WorkerTaskServiceClient<Channel> {
        WorkerTaskServiceClient::new(self.channel.clone())
    }

    /// Stops the server gracefully and returns how long the shutdown took.
    pub(crate) async fn shutdown(mut self) -> Duration {
        let server = self.server.take().expect("the server is shut down once");
        let started = tokio::time::Instant::now();
        server.shutdown().await.expect("the server stops cleanly");
        started.elapsed()
    }
}

/// Returns a loopback address whose port was free a moment ago; dev mode rejects port 0.
fn free_loopback_address() -> SocketAddr {
    std::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .and_then(|listener| listener.local_addr())
        .expect("a free loopback port exists")
}

async fn wait_until_serving(channel: Channel) {
    let mut health = HealthClient::new(channel);
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let mut is_serving = true;
            for service in READINESS_SERVICE_NAMES {
                let response = health
                    .check(HealthCheckRequest {
                        service: service.to_owned(),
                    })
                    .await;
                if !matches!(&response, Ok(response) if response.get_ref().status == ServingStatus::Serving as i32) {
                    is_serving = false;
                }
            }
            if is_serving {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("every health name becomes SERVING");
}

/// Decides the result of one Execute call.
pub(crate) type ExecuteHandler =
    Box<dyn Fn(&InvokeExecuteMethodRequest) -> InvokeExecuteMethodResponse + Send + Sync>;

/// A minimal pull Worker for one Flow type: WaitFor proceeds at once; Execute calls `handler`.
pub(crate) struct PullWorker {
    frames: mpsc::UnboundedSender<PollWorkerTaskRequest>,
    task: JoinHandle<()>,
}

impl PullWorker {
    /// Registers on the Flow type queue and waits for `registered`.
    pub(crate) async fn start(
        client: WorkerTaskServiceClient<Channel>,
        flow_type: &str,
        handler: ExecuteHandler,
    ) -> Self {
        let (frames, mut incoming) = register_pull_stream(client, flow_type, "smoke-worker").await;
        let replies = frames.clone();
        let task = tokio::spawn(async move {
            while let Some(Ok(response)) = incoming.next().await {
                if let Some(reply) = reply_to(response, &handler)
                    && replies.send(reply).is_err()
                {
                    return;
                }
            }
        });
        Self { frames, task }
    }

    /// Closes the Worker's stream.
    pub(crate) async fn stop(self) {
        drop(self.frames);
        self.task.abort();
        if let Err(error) = self.task.await {
            assert!(error.is_cancelled(), "the Worker task panicked: {error}");
        }
    }
}

/// A pull Worker whose tasks the test receives and answers one at a time; pings are answered.
pub(crate) struct ScriptedWorker {
    frames: mpsc::UnboundedSender<PollWorkerTaskRequest>,
    tasks: mpsc::UnboundedReceiver<PollWorkerTask>,
    task: JoinHandle<()>,
}

impl ScriptedWorker {
    /// Registers on the Flow type queue and waits for `registered`.
    pub(crate) async fn start(client: WorkerTaskServiceClient<Channel>, flow_type: &str) -> Self {
        let (frames, mut incoming) =
            register_pull_stream(client, flow_type, "scripted-worker").await;
        let (task_sender, tasks) = mpsc::unbounded_channel();
        let pongs = frames.clone();
        let task = tokio::spawn(async move {
            while let Some(Ok(response)) = incoming.next().await {
                let is_open = match response.frame {
                    Some(poll_worker_task_response::Frame::Task(task)) => {
                        task_sender.send(task).is_ok()
                    }
                    Some(poll_worker_task_response::Frame::Ping(_)) => pongs
                        .send(PollWorkerTaskRequest {
                            frame: Some(poll_worker_task_request::Frame::Pong(Default::default())),
                        })
                        .is_ok(),
                    _ => true,
                };
                if !is_open {
                    return;
                }
            }
        });
        Self {
            frames,
            tasks,
            task,
        }
    }

    /// Returns the next task the server assigns to this Worker.
    pub(crate) async fn next_task(&mut self) -> PollWorkerTask {
        tokio::time::timeout(Duration::from_secs(30), self.tasks.recv())
            .await
            .expect("the server assigns a task within 30 seconds")
            .expect("the Worker stream stays open")
    }

    /// Ends the task `task_id` with `output`.
    pub(crate) fn reply(&self, task_id: String, output: poll_worker_task_frame::Output) {
        self.frames
            .send(PollWorkerTaskRequest {
                frame: Some(poll_worker_task_request::Frame::TaskFrame(
                    PollWorkerTaskFrame {
                        task_id,
                        output: Some(output),
                    },
                )),
            })
            .expect("the outgoing stream is open");
    }

    /// Closes the Worker's stream.
    pub(crate) async fn stop(self) {
        drop(self.frames);
        self.task.abort();
        if let Err(error) = self.task.await {
            assert!(error.is_cancelled(), "the Worker task panicked: {error}");
        }
    }
}

/// Opens PollWorkerTask on the Flow type queue and waits for `registered`.
///
/// Returns the sender of outgoing frames and the incoming frames after the registration.
async fn register_pull_stream(
    mut client: WorkerTaskServiceClient<Channel>,
    flow_type: &str,
    app_worker_id: &str,
) -> (
    mpsc::UnboundedSender<PollWorkerTaskRequest>,
    tonic::Streaming<PollWorkerTaskResponse>,
) {
    let (frames, outgoing) = mpsc::unbounded_channel();
    frames
        .send(PollWorkerTaskRequest {
            frame: Some(poll_worker_task_request::Frame::Register(
                PollWorkerTaskRegister {
                    queue: flow_type.to_owned(),
                    app_worker_id: app_worker_id.to_owned(),
                    flow_types: vec![flow_type.to_owned()],
                    sdk_name: "dexzd-e2e-tests".to_owned(),
                    sdk_version: "0".to_owned(),
                },
            )),
        })
        .expect("the outgoing stream is open");
    let mut incoming = client
        .poll_worker_task(UnboundedReceiverStream::new(outgoing))
        .await
        .expect("PollWorkerTask opens")
        .into_inner();
    let first = incoming
        .next()
        .await
        .expect("the server answers the registration")
        .expect("the registration succeeds");
    assert!(
        matches!(
            first.frame,
            Some(poll_worker_task_response::Frame::Registered(_))
        ),
        "the first frame is registered, got {first:?}"
    );
    (frames, incoming)
}

fn reply_to(
    response: PollWorkerTaskResponse,
    handler: &ExecuteHandler,
) -> Option<PollWorkerTaskRequest> {
    let task = match response.frame? {
        poll_worker_task_response::Frame::Task(task) => task,
        poll_worker_task_response::Frame::Ping(_) => {
            return Some(PollWorkerTaskRequest {
                frame: Some(poll_worker_task_request::Frame::Pong(Default::default())),
            });
        }
        _ => return None,
    };
    let output = match task.request? {
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
                output: Some(invoke_execute_method_output::Output::Result(handler(
                    &request,
                ))),
            })
        }
        poll_worker_task::Request::Rpc(_) => return None,
    };
    Some(PollWorkerTaskRequest {
        frame: Some(poll_worker_task_request::Frame::TaskFrame(
            PollWorkerTaskFrame {
                task_id: task.task_id,
                output: Some(output),
            },
        )),
    })
}

/// A Step decision that completes the Flow with `output` as its result.
pub(crate) fn force_complete(output: dexzd_protocol::dex::Value) -> StepDecision {
    StepDecision {
        close_decision: Some(dexzd_protocol::dex::CloseDecision {
            close_decision_type: dexzd_protocol::dex::CloseDecisionType::ForceComplete.into(),
            close_input: Some(output),
            ..Default::default()
        }),
        ..StepDecision::default()
    }
}
