// Portions of this file are derived from indeedeng/iwf-java-sdk.
// Those portions are licensed under the Apache License, Version 2.0.
// See LICENSES/Apache-2.0.txt and LEGACY_NOTICES.md.
//
// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications are licensed under the Sustainable Use License 1.0.
// Third-Party Materials remain under the Apache License, Version 2.0.
// See LICENSE and LEGACY_NOTICES.md.

use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use dex_sdk::{
    BlobCache, BlobCacheConfig, Client, ClientOptions, Context, Flow, HandlerError, HandlerResult,
    PullWorkerOptions, Registry, Rpc, RpcList, RpcResult, Step, StepDecision, StepList,
    StepOptions, StopFlowOptions, Wait, Worker, WorkerOptions,
};

use crate::basic_workflow::BasicWorkflow;
use crate::support::{DexDevTestEnvironment, flow_id, skip_if_server_zd};

const GET_WORKER: Rpc<(), String> = Rpc::new("get_worker");

struct Gate {
    released: Mutex<bool>,
    changed: Condvar,
}

impl Gate {
    fn new() -> Self {
        Self {
            released: Mutex::new(false),
            changed: Condvar::new(),
        }
    }

    fn release(&self) {
        *self.released.lock().expect("gate lock") = true;
        self.changed.notify_all();
    }

    fn wait(&self, context: &Context) -> HandlerResult<()> {
        let mut released = self.released.lock().expect("gate lock");
        while !*released {
            if context.is_cancelled() {
                return Err(HandlerError::new("Cancelled", "Step handler was cancelled"));
            }
            (released, _) = self
                .changed
                .wait_timeout(released, Duration::from_millis(50))
                .expect("gate lock");
        }
        Ok(())
    }
}

#[derive(Clone)]
struct PullFlow {
    worker_id: String,
    observations: mpsc::Sender<(String, &'static str)>,
    first: PullFirstStep,
    second: PullSecondStep,
}

impl PullFlow {
    fn new(
        worker_id: &str,
        observations: mpsc::Sender<(String, &'static str)>,
        first_gate: Arc<Gate>,
        second_gate: Arc<Gate>,
    ) -> Self {
        let first = PullFirstStep {
            worker_id: worker_id.to_string(),
            observations: observations.clone(),
            gate: Arc::clone(&first_gate),
        };
        let second = PullSecondStep {
            worker_id: worker_id.to_string(),
            observations: observations.clone(),
            gate: Arc::clone(&second_gate),
        };
        Self {
            worker_id: worker_id.to_string(),
            observations,
            first,
            second,
        }
    }

    fn get_worker(&self, _context: &mut Context) -> HandlerResult<RpcResult<String>> {
        observe(&self.observations, &self.worker_id, "RPC");
        Ok(RpcResult::new(self.worker_id.clone()))
    }
}

impl Flow for PullFlow {
    type StartInput = String;

    fn steps(&self) -> StepList<'_, Self::StartInput> {
        StepList::start(&self.first).and(&self.second)
    }

    fn rpcs(&self) -> RpcList<Self> {
        RpcList::new().function_without_input(GET_WORKER, Self::get_worker)
    }
}

struct OldPullFlow(PullFlow);

impl Flow for OldPullFlow {
    type StartInput = String;

    fn flow_type(&self) -> &'static str {
        "PullFlow"
    }

    fn steps(&self) -> StepList<'_, Self::StartInput> {
        StepList::start(&self.0.first)
    }
}

struct NewStartPullFlow {
    flow: PullFlow,
    second: StartingPullSecondStep,
}

impl NewStartPullFlow {
    fn new(flow: PullFlow, starting_step_attempts: mpsc::Sender<u32>) -> Self {
        Self {
            second: StartingPullSecondStep {
                step: flow.second.clone(),
                starting_step_attempts,
            },
            flow,
        }
    }
}

impl Flow for NewStartPullFlow {
    type StartInput = String;

    fn flow_type(&self) -> &'static str {
        "PullFlow"
    }

    fn steps(&self) -> StepList<'_, Self::StartInput> {
        StepList::start(&self.second)
    }
}

#[derive(Clone)]
struct PullFirstStep {
    worker_id: String,
    observations: mpsc::Sender<(String, &'static str)>,
    gate: Arc<Gate>,
}

impl Step for PullFirstStep {
    type Input = String;

    fn wait_for(&self, _context: &mut Context, _input: String) -> HandlerResult<Wait> {
        observe(&self.observations, &self.worker_id, "WaitFor");
        Ok(Wait::skip_immediately())
    }

    fn execute(&self, context: &mut Context, input: String) -> HandlerResult<StepDecision> {
        observe(&self.observations, &self.worker_id, "Execute");
        self.gate.wait(context)?;
        Ok(StepDecision::go_to(
            &PullSecondStep {
                worker_id: self.worker_id.clone(),
                observations: self.observations.clone(),
                gate: Arc::clone(&self.gate),
            },
            input,
        ))
    }
}

#[derive(Clone)]
struct PullSecondStep {
    worker_id: String,
    observations: mpsc::Sender<(String, &'static str)>,
    gate: Arc<Gate>,
}

struct StartingPullSecondStep {
    step: PullSecondStep,
    starting_step_attempts: mpsc::Sender<u32>,
}

impl Step for StartingPullSecondStep {
    type Input = String;

    fn wait_for(&self, context: &mut Context, input: String) -> HandlerResult<Wait> {
        self.starting_step_attempts
            .send(context.attempt())
            .expect("starting Step attempt receiver");
        self.step.wait_for(context, input)
    }

    fn execute(&self, context: &mut Context, input: String) -> HandlerResult<StepDecision> {
        self.step.execute(context, input)
    }
}

impl Step for PullSecondStep {
    type Input = String;

    fn wait_for(&self, _context: &mut Context, _input: String) -> HandlerResult<Wait> {
        observe(&self.observations, &self.worker_id, "SecondWaitFor");
        Ok(Wait::skip_immediately())
    }

    fn execute(&self, context: &mut Context, input: String) -> HandlerResult<StepDecision> {
        observe(&self.observations, &self.worker_id, "SecondExecute");
        self.gate.wait(context)?;
        Ok(StepDecision::graceful_complete(input))
    }
}

struct PullProbeFlow {
    step: PullProbeStep,
}

impl PullProbeFlow {
    fn new(worker_id: &str) -> Self {
        Self {
            step: PullProbeStep {
                worker_id: worker_id.to_string(),
            },
        }
    }
}

impl Flow for PullProbeFlow {
    type StartInput = ();

    fn steps(&self) -> StepList<'_, Self::StartInput> {
        StepList::start(&self.step)
    }
}

struct PullProbeStep {
    worker_id: String,
}

impl Step for PullProbeStep {
    type Input = ();

    fn execute(&self, _context: &mut Context, _input: ()) -> HandlerResult<StepDecision> {
        Ok(StepDecision::graceful_complete(self.worker_id.clone()))
    }
}

struct PullTestWorker {
    worker: Arc<Worker>,
    thread: Option<JoinHandle<dex_sdk::SdkResult<()>>>,
}

impl PullTestWorker {
    fn start(registry: Registry, cache: Arc<BlobCache>, address: &str, worker_id: &str) -> Self {
        Self::start_with_drain_timeout(registry, cache, address, worker_id, Duration::from_secs(10))
    }

    fn start_with_drain_timeout(
        registry: Registry,
        cache: Arc<BlobCache>,
        address: &str,
        worker_id: &str,
        drain_timeout: Duration,
    ) -> Self {
        let worker = Arc::new(
            Worker::try_new(
                registry,
                cache,
                WorkerOptions::new().server_address(address).pull(
                    PullWorkerOptions::new()
                        .app_worker_id(worker_id)
                        .max_concurrent_tasks(2)
                        .stop_drain_timeout(drain_timeout),
                ),
            )
            .expect("create pull Worker"),
        );
        let running = Arc::clone(&worker);
        let thread = thread::spawn(move || running.start());
        Self {
            worker,
            thread: Some(thread),
        }
    }

    fn join(&mut self) {
        self.thread
            .take()
            .expect("running Worker")
            .join()
            .expect("join pull Worker")
            .expect("pull Worker finished");
    }
}

struct CancelFlow {
    observed: mpsc::Sender<&'static str>,
    finish: Arc<Mutex<mpsc::Receiver<()>>>,
    step: CancelStep,
}

impl CancelFlow {
    fn new(observed: mpsc::Sender<&'static str>, finish: Arc<Mutex<mpsc::Receiver<()>>>) -> Self {
        Self {
            observed: observed.clone(),
            finish: Arc::clone(&finish),
            step: CancelStep { observed, finish },
        }
    }
}

impl Flow for CancelFlow {
    type StartInput = ();

    fn steps(&self) -> StepList<'_, Self::StartInput> {
        StepList::start(&self.step)
    }
}

struct CancelStep {
    observed: mpsc::Sender<&'static str>,
    finish: Arc<Mutex<mpsc::Receiver<()>>>,
}

impl Step for CancelStep {
    type Input = ();

    fn wait_for(&self, _context: &mut Context, _input: ()) -> HandlerResult<Wait> {
        Ok(Wait::skip_immediately())
    }

    fn execute(&self, context: &mut Context, _input: ()) -> HandlerResult<StepDecision> {
        self.observed.send("started").expect("observe started Step");
        while !context.is_cancelled() {
            if let Err(error) = context.record_heartbeat() {
                if !context.is_cancelled() {
                    return Err(error);
                }
                break;
            }
            thread::sleep(Duration::from_millis(100));
        }
        self.observed
            .send("cancelled")
            .expect("observe cancelled Step");
        self.finish
            .lock()
            .expect("finish gate lock")
            .recv()
            .expect("release cancelled Step");
        self.observed
            .send("returned")
            .expect("observe returned Step");
        Err(HandlerError::new("Cancelled", "Step handler was cancelled"))
    }

    fn options(&self) -> StepOptions<Self::Input> {
        StepOptions::new().heartbeat_timeout(Duration::from_secs(10))
    }
}

impl Drop for PullTestWorker {
    fn drop(&mut self) {
        self.worker.stop();
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .expect("join pull Worker")
                .expect("pull Worker finished");
        }
    }
}

fn observe(sender: &mpsc::Sender<(String, &'static str)>, worker_id: &str, method: &'static str) {
    sender
        .send((worker_id.to_string(), method))
        .expect("send pull observation");
}

fn await_observation(
    receiver: &mpsc::Receiver<(String, &'static str)>,
    worker_id: &str,
    method: &'static str,
) {
    assert_eq!(
        (worker_id.to_string(), method),
        receiver
            .recv_timeout(Duration::from_secs(30))
            .expect("receive pull observation")
    );
}

fn run_probe(client: &Client) -> String {
    let id = flow_id("rust-pull-probe");
    client
        .start_flow(&PullProbeFlow::new("client"), &id, ())
        .expect("start probe Flow");
    client
        .wait_for_flow_with_timeout(&id, Duration::from_secs(30))
        .expect("complete probe Flow")
        .single_output::<String>()
        .expect("probe result")
}

#[test]
#[ignore = "requires dexcli dev"]
fn pull_worker_test_end_to_end() {
    let server_address =
        std::env::var("DEX_SERVER_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8801".to_string());
    let cache_directory = tempfile::tempdir().expect("create pull cache directory");
    let cache = Arc::new(
        BlobCache::open(
            BlobCacheConfig::new(cache_directory.path(), 64 * 1024 * 1024, 10_000)
                .expect("cache config"),
        )
        .expect("open cache"),
    );
    let (observed, observations) = mpsc::channel();
    let first_gate = Arc::new(Gate::new());
    let second_gate = Arc::new(Gate::new());
    let worker_a_id = format!("pull-rust-a-{}", uuid::Uuid::new_v4());
    let worker_b_id = format!("pull-rust-b-{}", uuid::Uuid::new_v4());
    let flow_a = PullFlow::new(
        &worker_a_id,
        observed.clone(),
        Arc::clone(&first_gate),
        Arc::clone(&second_gate),
    );
    let flow_b = PullFlow::new(
        &worker_b_id,
        observed.clone(),
        Arc::clone(&first_gate),
        Arc::clone(&second_gate),
    );
    let mut worker_a = PullTestWorker::start(
        Registry::new()
            .register(flow_a.clone())
            .expect("register A"),
        Arc::clone(&cache),
        &server_address,
        &worker_a_id,
    );
    let client = Client::new(
        Registry::new()
            .register(flow_a.clone())
            .expect("register client Flow")
            .register(PullProbeFlow::new("client"))
            .expect("register client probe"),
        Arc::clone(&cache),
        ClientOptions::new()
            .server_address(&server_address)
            .worker_target(worker_a.worker.worker_target().clone()),
    );
    client.health_check().expect("Dex health check");

    let id = flow_id("rust-pull");
    client
        .start_flow(&flow_a, &id, "completed".to_string())
        .expect("start pull Flow");
    await_observation(&observations, &worker_a_id, "WaitFor");
    await_observation(&observations, &worker_a_id, "Execute");

    let worker_b = PullTestWorker::start(
        Registry::new()
            .register(flow_b)
            .expect("register B Flow")
            .register(PullProbeFlow::new(&worker_b_id))
            .expect("register B probe"),
        Arc::clone(&cache),
        &server_address,
        &worker_b_id,
    );
    assert_eq!(worker_b_id, run_probe(&client));
    first_gate.release();
    await_observation(&observations, &worker_a_id, "SecondWaitFor");
    await_observation(&observations, &worker_a_id, "SecondExecute");
    assert_eq!(
        worker_a_id,
        client
            .invoke_rpc_without_input(&id, GET_WORKER)
            .expect("invoke pull RPC")
    );
    await_observation(&observations, &worker_a_id, "RPC");

    worker_a.worker.stop();
    assert_eq!(worker_b_id, run_probe(&client));
    assert!(
        !worker_a
            .thread
            .as_ref()
            .expect("running Worker")
            .is_finished()
    );
    second_gate.release();
    worker_a.join();
    assert_eq!(
        "completed",
        client
            .wait_for_flow_with_timeout(&id, Duration::from_secs(30))
            .expect("complete pull Flow")
            .single_output::<String>()
            .expect("pull result")
    );
    assert!(matches!(
        observations.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));

    // C18: server-zd rejects push Worker targets.
    if !skip_if_server_zd("push Workers") {
        let push =
            DexDevTestEnvironment::start_push(Registry::new().register(BasicWorkflow::new()));
        let push_id = flow_id("rust-push-alongside-pull");
        push.client
            .start_flow(&BasicWorkflow::new(), &push_id, 0)
            .expect("start push Flow");
        assert_eq!(
            2,
            push.client
                .wait_for_flow_with_timeout(&push_id, Duration::from_secs(30))
                .expect("complete push Flow")
                .single_output::<i32>()
                .expect("push result")
        );
    }
    drop(worker_b);
    drop(client);
    cache.close().expect("close pull cache");
}

#[test]
#[ignore = "requires dexcli dev"]
fn pull_worker_test_reassigns_unregistered_rpc() {
    let server_address =
        std::env::var("DEX_SERVER_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8801".to_string());
    let cache_directory = tempfile::tempdir().expect("create pull cache directory");
    let cache = Arc::new(
        BlobCache::open(
            BlobCacheConfig::new(cache_directory.path(), 64 * 1024 * 1024, 10_000)
                .expect("cache config"),
        )
        .expect("open cache"),
    );
    let (observed, observations) = mpsc::channel();
    let first_gate = Arc::new(Gate::new());
    let second_gate = Arc::new(Gate::new());
    second_gate.release();
    let old_flow = OldPullFlow(PullFlow::new(
        "old",
        observed.clone(),
        Arc::clone(&first_gate),
        Arc::clone(&second_gate),
    ));
    let new_flow = PullFlow::new(
        "new",
        observed,
        Arc::clone(&first_gate),
        Arc::clone(&second_gate),
    );
    let old_worker_id = format!("pull-rust-old-{}", uuid::Uuid::new_v4());
    let new_worker_id = format!("pull-rust-new-{}", uuid::Uuid::new_v4());
    let old_worker = PullTestWorker::start(
        Registry::new()
            .register(old_flow)
            .expect("register old Flow"),
        Arc::clone(&cache),
        &server_address,
        &old_worker_id,
    );
    let client = Client::new(
        Registry::new()
            .register(new_flow.clone())
            .expect("register client Flow"),
        Arc::clone(&cache),
        ClientOptions::new()
            .server_address(&server_address)
            .worker_target(old_worker.worker.worker_target().clone()),
    );
    client.health_check().expect("Dex health check");
    let id = flow_id("rust-pull-unknown");
    client
        .start_flow(&new_flow, &id, "complete".to_string())
        .expect("start pull Flow");
    await_observation(&observations, "old", "WaitFor");
    await_observation(&observations, "old", "Execute");
    let new_worker = PullTestWorker::start(
        Registry::new()
            .register(new_flow)
            .expect("register new Flow"),
        Arc::clone(&cache),
        &server_address,
        &new_worker_id,
    );
    assert_eq!(
        "new",
        client
            .invoke_rpc_without_input(&id, GET_WORKER)
            .expect("reassign pull RPC")
    );
    await_observation(&observations, "new", "RPC");
    client
        .stop_flow(&id, StopFlowOptions::cancel())
        .expect("stop pull Flow");
    first_gate.release();
    drop(new_worker);
    drop(old_worker);
    drop(client);
    cache.close().expect("close pull cache");
}

#[test]
#[ignore = "requires dexcli dev"]
fn pull_worker_test_reassigns_unregistered_start_step() {
    let server_address =
        std::env::var("DEX_SERVER_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8801".to_string());
    let cache_directory = tempfile::tempdir().expect("create pull cache directory");
    let cache = Arc::new(
        BlobCache::open(
            BlobCacheConfig::new(cache_directory.path(), 64 * 1024 * 1024, 10_000)
                .expect("cache config"),
        )
        .expect("open cache"),
    );
    let (observed, observations) = mpsc::channel();
    let first_gate = Arc::new(Gate::new());
    let second_gate = Arc::new(Gate::new());
    second_gate.release();
    let old_flow = OldPullFlow(PullFlow::new(
        "old",
        observed.clone(),
        Arc::clone(&first_gate),
        Arc::clone(&second_gate),
    ));
    let (starting_step_attempts, starting_step_attempt_receiver) = mpsc::channel();
    let new_flow = NewStartPullFlow::new(
        PullFlow::new("new", observed, first_gate, second_gate),
        starting_step_attempts.clone(),
    );
    let old_worker_id = format!("pull-rust-old-step-{}", uuid::Uuid::new_v4());
    let new_worker_id = format!("pull-rust-new-step-{}", uuid::Uuid::new_v4());
    let old_worker = PullTestWorker::start(
        Registry::new()
            .register(old_flow)
            .expect("register old Flow"),
        Arc::clone(&cache),
        &server_address,
        &old_worker_id,
    );
    let client = Client::new(
        Registry::new()
            .register(NewStartPullFlow::new(
                new_flow.flow.clone(),
                starting_step_attempts,
            ))
            .expect("register client Flow"),
        Arc::clone(&cache),
        ClientOptions::new()
            .server_address(&server_address)
            .worker_target(old_worker.worker.worker_target().clone()),
    );
    client.health_check().expect("Dex health check");
    let id = flow_id("rust-pull-unknown-step");
    client
        .start_flow(&new_flow, &id, "complete".to_string())
        .expect("start pull Flow");
    let new_worker = PullTestWorker::start(
        Registry::new()
            .register(new_flow)
            .expect("register new Flow"),
        Arc::clone(&cache),
        &server_address,
        &new_worker_id,
    );
    assert_eq!(
        "complete",
        client
            .wait_for_flow_with_timeout(&id, Duration::from_secs(30))
            .expect("complete pull Flow")
            .single_output::<String>()
            .expect("pull result")
    );
    await_observation(&observations, "new", "SecondWaitFor");
    await_observation(&observations, "new", "SecondExecute");
    assert_eq!(
        1,
        starting_step_attempt_receiver
            .recv_timeout(Duration::from_secs(20))
            .expect("starting Step attempt")
    );
    drop(new_worker);
    drop(old_worker);
    drop(client);
    cache.close().expect("close pull cache");
}

#[test]
#[ignore = "requires dexcli dev"]
fn pull_worker_test_cancel_and_forced_stop() {
    let server_address =
        std::env::var("DEX_SERVER_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8801".to_string());
    let cache_directory = tempfile::tempdir().expect("create pull cache directory");
    let cache = Arc::new(
        BlobCache::open(
            BlobCacheConfig::new(cache_directory.path(), 64 * 1024 * 1024, 10_000)
                .expect("cache config"),
        )
        .expect("open cache"),
    );
    let (observed, observations) = mpsc::channel();
    let (release, finished) = mpsc::channel();
    let finish = Arc::new(Mutex::new(finished));
    let flow = CancelFlow::new(observed, Arc::clone(&finish));
    let worker_id = format!("pull-rust-cancel-{}", uuid::Uuid::new_v4());
    let mut worker = PullTestWorker::start_with_drain_timeout(
        Registry::new()
            .register(CancelFlow::new(
                flow.observed.clone(),
                Arc::clone(&flow.finish),
            ))
            .expect("register cancel Flow"),
        Arc::clone(&cache),
        &server_address,
        &worker_id,
        Duration::from_millis(100),
    );
    let client = Client::new(
        Registry::new()
            .register(CancelFlow::new(
                flow.observed.clone(),
                Arc::clone(&flow.finish),
            ))
            .expect("register client Flow"),
        Arc::clone(&cache),
        ClientOptions::new()
            .server_address(&server_address)
            .worker_target(worker.worker.worker_target().clone()),
    );
    client.health_check().expect("Dex health check");

    let cancelled_flow_id = flow_id("rust-pull-cancel");
    client
        .start_flow(&flow, &cancelled_flow_id, ())
        .expect("start cancel Flow");
    assert_eq!(
        "started",
        observations
            .recv_timeout(Duration::from_secs(30))
            .expect("first Execute started")
    );
    client
        .stop_flow(&cancelled_flow_id, StopFlowOptions::cancel())
        .expect("cancel Flow");
    assert_eq!(
        "cancelled",
        observations
            .recv_timeout(Duration::from_secs(30))
            .expect("CancelTask reached handler")
    );
    release.send(()).expect("release cancelled Step");
    assert_eq!(
        "returned",
        observations
            .recv_timeout(Duration::from_secs(30))
            .expect("first handler returned")
    );

    let forced_flow_id = flow_id("rust-pull-force");
    client
        .start_flow(&flow, &forced_flow_id, ())
        .expect("start force Flow");
    assert_eq!(
        "started",
        observations
            .recv_timeout(Duration::from_secs(30))
            .expect("second Execute started")
    );
    worker.worker.stop();
    assert_eq!(
        "cancelled",
        observations
            .recv_timeout(Duration::from_secs(30))
            .expect("forced stop cancelled handler")
    );
    assert!(
        !worker
            .thread
            .as_ref()
            .expect("running Worker")
            .is_finished()
    );
    release.send(()).expect("release forced Step");
    assert_eq!(
        "returned",
        observations
            .recv_timeout(Duration::from_secs(30))
            .expect("forced handler returned")
    );
    worker.join();
    client
        .stop_flow(&forced_flow_id, StopFlowOptions::cancel())
        .expect("cancel forced-stop Flow");
    drop(client);
    cache.close().expect("close pull cache");
}

struct RegistrationProbeFlow {
    is_invalid: bool,
}

impl Flow for RegistrationProbeFlow {
    type StartInput = ();

    fn flow_type(&self) -> &'static str {
        if self.is_invalid {
            concat!(
                "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            )
        } else {
            "RegistrationProbeFlow"
        }
    }
}

#[test]
#[ignore = "requires dexcli dev"]
fn pull_registration_readiness_and_invalid_argument() {
    let directory = tempfile::tempdir().expect("cache directory");
    let cache = Arc::new(
        BlobCache::open(
            BlobCacheConfig::new(directory.path(), 64 * 1024 * 1024, 0).expect("cache config"),
        )
        .expect("cache"),
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("wait runtime");
    for is_invalid in [false, true] {
        let registry = Registry::new()
            .register(RegistrationProbeFlow { is_invalid })
            .expect("register Flow");
        let worker = Arc::new(
            Worker::try_new(
                registry,
                Arc::clone(&cache),
                WorkerOptions::new()
                    .server_address(
                        std::env::var("DEX_SERVER_ADDRESS")
                            .unwrap_or_else(|_| "127.0.0.1:8801".to_string()),
                    )
                    .pull(PullWorkerOptions::new().stop_on_permanent_stream_rejection(is_invalid)),
            )
            .expect("pull Worker"),
        );
        let running = Arc::clone(&worker);
        let worker_thread = thread::spawn(move || running.start());
        let result = runtime
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(10), worker.wait_pull_registered()).await
            })
            .expect("registration deadline");
        if is_invalid {
            assert!(
                result
                    .expect_err("registration rejected")
                    .pull_registration_error()
                    .is_some_and(
                        |rejection| rejection.status.code() == tonic::Code::InvalidArgument
                    )
            );
            assert!(!worker.pull_registration_state().is_ready);
        } else {
            result.expect("all queues registered");
            assert!(worker.pull_registration_state().is_ready);
            assert_eq!(worker.pull_registration_state().queues.len(), 2);
        }
        worker.stop();
        let result = worker_thread.join().expect("join Worker");
        if is_invalid {
            assert!(
                result
                    .expect_err("fatal rejection")
                    .pull_registration_error()
                    .is_some()
            );
        } else {
            result.expect("Worker stopped");
        }
        worker.stop();
    }
    cache.close().expect("close cache");
}
