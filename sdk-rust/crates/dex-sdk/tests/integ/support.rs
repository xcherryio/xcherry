// Portions of this file are derived from indeedeng/iwf-java-sdk.
// Those portions are licensed under the Apache License, Version 2.0.
// See LICENSES/Apache-2.0.txt and LEGACY_NOTICES.md.
//
// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications are licensed under the Sustainable Use License 1.0.
// Third-Party Materials remain under the Apache License, Version 2.0.
// See LICENSE and LEGACY_NOTICES.md.

use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use dex_protocol::dex::{ServerEngine, flow_service_client::FlowServiceClient};
use dex_sdk::{
    BlobCache, BlobCacheConfig, Client, ClientOptions, Context, Flow, GrpcCode, HandlerResult,
    PullWorkerOptions, Registry, SdkResult, Step, StepDecision, StepExecutionId, StepList, TimerId,
    Worker, WorkerOptions,
};
use tempfile::TempDir;
use tokio::runtime::Runtime;

static SERVER_ENGINE: OnceLock<ServerEngine> = OnceLock::new();

#[expect(
    clippy::print_stdout,
    reason = "Rust integration tests have no skip API"
)]
pub(crate) fn skip_if_server_zd(feature: &str) -> bool {
    let server_engine = SERVER_ENGINE.get_or_init(|| {
        let server_address =
            std::env::var("DEX_SERVER_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8801".to_string());
        let runtime = Runtime::new().expect("create server engine check runtime");
        runtime.block_on(async {
            FlowServiceClient::connect(format!("http://{server_address}"))
                .await
                .expect("connect FlowService for server engine check")
                .get_server_info(())
                .await
                .expect("GetServerInfo for server engine check")
                .into_inner()
                .server_engine()
        })
    });
    if *server_engine != ServerEngine::Zd {
        return false;
    }
    println!("server-zd does not support {feature}");
    true
}

pub(crate) struct DexDevTestEnvironment {
    pub(crate) client: Client,
    worker: Arc<Worker>,
    worker_thread: Option<JoinHandle<()>>,
    worker_failure: Arc<Mutex<Option<String>>>,
    cache: Arc<BlobCache>,
    _cache_directory: TempDir,
}

impl DexDevTestEnvironment {
    pub(crate) fn start(registry: SdkResult<Registry>) -> Self {
        Self::start_with_mode(registry, is_pull_mode())
    }
    pub(crate) fn start_push(registry: SdkResult<Registry>) -> Self {
        Self::start_with_mode(registry, false)
    }

    fn start_with_mode(registry: SdkResult<Registry>, is_pull_mode: bool) -> Self {
        let registry = registry.expect("register integration test Flow");
        let registry = if is_pull_mode {
            registry
                .register(PullWorkerReadyFlow {
                    step: PullWorkerReadyStep,
                })
                .expect("register pull Worker readiness Flow")
        } else {
            registry
        };
        let server_address =
            std::env::var("DEX_SERVER_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8801".to_string());
        let worker_port = (!is_pull_mode).then(available_port);
        let cache_directory = tempfile::tempdir().expect("create Rust SDK test cache directory");
        let cache = Arc::new(
            BlobCache::open(
                BlobCacheConfig::new(cache_directory.path(), 64 * 1024 * 1024, 10_000)
                    .expect("valid Rust SDK test cache config"),
            )
            .expect("open Rust SDK test cache"),
        );
        let mut worker_options = WorkerOptions::new().server_address(&server_address);
        if is_pull_mode {
            worker_options = worker_options.pull(PullWorkerOptions::new());
        } else {
            let worker_address = format!("127.0.0.1:{}", worker_port.expect("push Worker port"));
            worker_options = worker_options.bind_address(worker_address);
        }
        let worker = Arc::new(Worker::new(
            registry.clone(),
            Arc::clone(&cache),
            worker_options,
        ));
        let worker_failure = Arc::new(Mutex::new(None));
        let thread_worker = Arc::clone(&worker);
        let thread_failure = Arc::clone(&worker_failure);
        let worker_thread = thread::Builder::new()
            .name("dex-rust-integration-worker".to_string())
            .spawn(move || {
                if let Err(error) = thread_worker.start() {
                    *thread_failure.lock().expect("worker failure lock") = Some(error.to_string());
                }
            })
            .expect("start Rust integration Worker thread");
        if let Some(worker_port) = worker_port {
            await_worker(worker_port, &worker_failure);
        }
        let client = Client::new(
            registry,
            Arc::clone(&cache),
            ClientOptions::new()
                .server_address(server_address)
                .worker_target(worker.worker_target().clone()),
        );
        client.health_check().expect("Dex server health check");
        if is_pull_mode {
            let readiness_flow_id = flow_id("rust-pull-worker-ready");
            client
                .start_flow(
                    &PullWorkerReadyFlow {
                        step: PullWorkerReadyStep,
                    },
                    &readiness_flow_id,
                    (),
                )
                .expect("start pull Worker readiness Flow");
            let readiness_result: String = client
                .wait_for_flow_with_timeout(&readiness_flow_id, Duration::from_secs(30))
                .and_then(|result| result.single_output())
                .expect("pull Worker became ready");
            assert_eq!("ready", readiness_result);
        }
        Self {
            client,
            worker,
            worker_thread: Some(worker_thread),
            worker_failure,
            cache,
            _cache_directory: cache_directory,
        }
    }
}

impl Drop for DexDevTestEnvironment {
    fn drop(&mut self) {
        self.worker.stop();
        if let Some(worker_thread) = self.worker_thread.take() {
            worker_thread.join().expect("join Rust integration Worker");
        }
        self.cache.close().expect("close Rust SDK test cache");
        if let Some(failure) = self
            .worker_failure
            .lock()
            .expect("worker failure lock")
            .take()
        {
            panic!("Rust integration Worker failed: {failure}");
        }
    }
}

pub(crate) fn flow_id(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4())
}

pub(crate) fn is_pull_mode() -> bool {
    match std::env::var("DEX_INTEG_WORKER_MODE").as_deref() {
        Ok("pull") => true,
        Ok("push") | Err(std::env::VarError::NotPresent) => false,
        Ok(mode) => panic!("invalid DEX_INTEG_WORKER_MODE: {mode}"),
        Err(error) => panic!("cannot read DEX_INTEG_WORKER_MODE: {error}"),
    }
}
pub(crate) fn skip_timer_when_pending(
    client: &Client,
    flow_id: &str,
    step_execution: StepExecutionId,
    timer: TimerId,
) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match client.skip_timer(flow_id, step_execution.clone(), timer.clone()) {
            Ok(()) => return,
            Err(error)
                if Instant::now() < deadline
                    && error.service_error().is_some_and(|service| {
                        service.code() == GrpcCode::InvalidArgument
                            && service.detail()
                                == "requested timer condition does not exist or is not pending"
                    }) =>
            {
                thread::yield_now();
            }
            Err(error) => panic!("timer did not become pending: {error}"),
        }
    }
}

fn available_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind an available Worker port")
        .local_addr()
        .expect("read available Worker port")
        .port()
}

fn await_worker(worker_port: u16, failure: &Mutex<Option<String>>) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Some(failure) = failure.lock().expect("worker failure lock").as_ref() {
            panic!("Rust integration Worker failed: {failure}");
        }
        if TcpStream::connect(("127.0.0.1", worker_port)).is_ok() {
            return;
        }
        thread::yield_now();
    }
    panic!("Rust integration Worker did not become ready");
}

struct PullWorkerReadyFlow {
    step: PullWorkerReadyStep,
}

impl Flow for PullWorkerReadyFlow {
    type StartInput = ();

    fn steps(&self) -> StepList<'_, Self::StartInput> {
        StepList::start(&self.step)
    }
}

struct PullWorkerReadyStep;

impl Step for PullWorkerReadyStep {
    type Input = ();

    fn execute(&self, _context: &mut Context, _input: ()) -> HandlerResult<StepDecision> {
        Ok(StepDecision::graceful_complete("ready".to_string()))
    }
}
