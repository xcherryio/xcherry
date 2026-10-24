// Copyright (c) 2022-2026 Super Durable, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod common;
mod worker_harness;

use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use common::{
    FlowSmokeEntry, FlowSmokeFlags, FlowSmokeHttpClient, assert_flow_smoke_no_unexpected_failures,
    assert_flow_smoke_start_step, query, query_with,
};
use dex_examples_rust::patterns::polling::FakeJobService;
use dex_examples_rust::server::build_router;
use dex_examples_rust::shared::{WorkerMode, example_pull_worker_options};
use dex_examples_rust::{create_example_registry, create_worker_registry};
use dex_sdk::{
    BlobCache, BlobCacheConfig, Client, ClientOptions, SdkResult, Worker, WorkerOptions,
    WorkerTarget,
};
use tempfile::TempDir;
use worker_harness::{await_pull_registration, read_worker_mode};

struct FlowSmokeEnvironment {
    _client: Arc<Client>,
    _worker: Arc<Worker>,
    _worker_thread: Option<JoinHandle<SdkResult<()>>>,
    _cache: Arc<BlobCache>,
    http_base_url: String,
    _server_thread: Option<JoinHandle<()>>,
    _cache_directory: TempDir,
}

impl FlowSmokeEnvironment {
    fn start() -> Self {
        let server_address =
            std::env::var("DEX_SERVER_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8801".to_string());
        let worker_port = available_worker_port();
        let worker_address = format!("127.0.0.1:{worker_port}");
        let cache_directory = tempfile::tempdir().expect("create Rust flow smoke cache directory");
        let cache = Arc::new(
            BlobCache::open(
                BlobCacheConfig::new(cache_directory.path(), 64 * 1024 * 1024, 10_000)
                    .expect("valid Rust flow smoke cache config"),
            )
            .expect("open Rust flow smoke cache"),
        );
        let polling_jobs = Arc::new(FakeJobService::default());
        let worker_mode = read_worker_mode();
        let worker_target = match worker_mode {
            WorkerMode::Push => WorkerTarget::new(worker_address.clone()),
            WorkerMode::Pull => WorkerTarget::pull(),
        };
        let client = Arc::new(
            Client::try_new(
                create_example_registry(Arc::clone(&polling_jobs))
                    .expect("register Rust client examples"),
                Arc::clone(&cache),
                ClientOptions::new()
                    .server_address(&server_address)
                    .worker_target(worker_target.clone()),
            )
            .expect("create Rust flow smoke Client"),
        );
        let worker_options = match worker_mode {
            WorkerMode::Push => WorkerOptions::new()
                .server_address(&server_address)
                .bind_address(&worker_address)
                .worker_target(worker_target),
            WorkerMode::Pull => WorkerOptions::new()
                .server_address(&server_address)
                .pull(example_pull_worker_options()),
        };
        let worker = Arc::new(
            Worker::try_new(
                create_worker_registry(Arc::clone(&client), Arc::clone(&polling_jobs))
                    .expect("register Rust Worker examples"),
                Arc::clone(&cache),
                worker_options,
            )
            .expect("create Rust flow smoke Worker"),
        );
        let thread_worker = Arc::clone(&worker);
        let worker_thread = thread::Builder::new()
            .name("dex-rust-flow-smoke-worker".to_string())
            .spawn(move || thread_worker.start())
            .expect("start Rust flow smoke Worker thread");
        match worker_mode {
            WorkerMode::Push => await_worker(worker_port, &worker_thread),
            WorkerMode::Pull => await_pull_registration(&worker),
        }
        client.health_check().expect("Dex health check");

        let http_port = available_worker_port();
        let router = build_router(Arc::clone(&client), polling_jobs);
        let server_thread = thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("create flow smoke HTTP runtime");
            runtime.block_on(async move {
                let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{http_port}"))
                    .await
                    .expect("bind flow smoke HTTP server");
                axum::serve(listener, router)
                    .await
                    .expect("serve flow smoke HTTP");
            });
        });
        await_http(http_port);

        Self {
            _client: client,
            _worker: worker,
            _worker_thread: Some(worker_thread),
            _cache: cache,
            http_base_url: format!("http://127.0.0.1:{http_port}"),
            _server_thread: Some(server_thread),
            _cache_directory: cache_directory,
        }
    }
}

fn flow_smoke_catalog(client: &mut FlowSmokeHttpClient) -> Vec<FlowSmokeEntry> {
    vec![
        FlowSmokeEntry {
            name: "products/engagement",
            path: "/products/engagement/start",
            query: HashMap::new(),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "products/microservices",
            path: "/products/microservices/start",
            query: query(&client.new_flow_id("microservices")),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "products/money-transfer",
            path: "/products/money-transfer/start",
            query: HashMap::from([
                ("amount".into(), "100".into()),
                ("fromAccount".into(), "from-smoke".into()),
                ("toAccount".into(), "to-smoke".into()),
                ("notes".into(), "smoke".into()),
            ]),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "products/order-processing",
            path: "/products/order-processing/start",
            query: HashMap::new(),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "products/subscription",
            path: "/products/subscription/start",
            query: HashMap::new(),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "products/signup",
            path: "/products/signup/start",
            query: HashMap::new(),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "products/job-post",
            path: "/products/job-post/start",
            query: HashMap::new(),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "patterns/polling",
            path: "/patterns/polling/start",
            query: query(&client.new_flow_id("pattern-polling")),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "patterns/interruptible",
            path: "/patterns/interruptible/start",
            query: query(&client.new_flow_id("interruptible")),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "patterns/reminders",
            path: "/patterns/reminders/start",
            query: HashMap::new(),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "patterns/entity-store",
            path: "/patterns/entity-store/start",
            query: HashMap::new(),
            flags: FlowSmokeFlags::NO_START_STEP,
        },
        FlowSmokeEntry {
            name: "patterns/manual-recovery",
            path: "/patterns/manual-recovery/start",
            query: query(&client.new_flow_id("manual-recovery")),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "patterns/inactiveness-tracker-timer",
            path: "/patterns/inactiveness-tracker-timer/start",
            query: query(&client.new_flow_id("inactiveness-tracker-timer")),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "patterns/parallel/static",
            path: "/patterns/parallel/start/static",
            query: query(&client.new_flow_id("parallel-static")),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "patterns/parallel/dynamic",
            path: "/patterns/parallel/start/dynamic",
            query: query(&client.new_flow_id("parallel-dynamic")),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "patterns/parallel/await",
            path: "/patterns/parallel/start/await",
            query: query(&client.new_flow_id("parallel-await")),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "patterns/parallel/first-win",
            path: "/patterns/parallel/start/first-win",
            query: query(&client.new_flow_id("parallel-first-win")),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "patterns/recovery",
            path: "/patterns/recovery/start",
            query: query_with(&client.new_flow_id("recovery"), &[("itemName", "widget")]),
            flags: FlowSmokeFlags::STEP_START_MAY_FAIL,
        },
        parallel_subflows_entry(client, "basic", "/patterns/parallel-subflows/start/basic"),
        parallel_subflows_entry(
            client,
            "wait-for-half",
            "/patterns/parallel-subflows/start/wait-for-half",
        ),
        parallel_subflows_entry(
            client,
            "long-lived-parent",
            "/patterns/parallel-subflows/start/long-lived-parent",
        ),
        parallel_subflows_entry(
            client,
            "short-lived-parent",
            "/patterns/parallel-subflows/start/short-lived-parent",
        ),
        FlowSmokeEntry {
            name: "patterns/drain-channels/internal",
            path: "/patterns/drain-channels/internal/start",
            query: query(&client.new_flow_id("drain-internal")),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "patterns/drain-channels/external-publishing",
            path: "/patterns/drain-channels/external-publishing/start-or-publish",
            query: query(&client.new_flow_id("drain-external")),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "patterns/wait-for-step-completion",
            path: "/patterns/wait-for-step-completion/start",
            query: query(&client.new_flow_id("wait-for-state")),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "patterns/timeout",
            path: "/patterns/timeout/start",
            query: query_with(
                &client.new_flow_id("timeout"),
                &[("successfulWorkflow", "true")],
            ),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "primitives/step",
            path: "/primitives/step/start",
            query: query_with(&client.new_flow_id("primitive-step"), &[("inputNum", "1")]),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "primitives/step/retry",
            path: "/primitives/step/retry/start",
            query: query_with(
                &client.new_flow_id("primitive-step-retry"),
                &[("readyAfterAttempt", "2")],
            ),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "primitives/step/custom-retry",
            path: "/primitives/step/custom-retry/start",
            query: query_with(
                &client.new_flow_id("primitive-step-custom-retry"),
                &[("readyAfterAttempt", "1")],
            ),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "primitives/step/durability",
            path: "/primitives/step/durability/start",
            query: query_with(
                &client.new_flow_id("primitive-step-durability"),
                &[("mode", "sync")],
            ),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "primitives/step/heartbeat",
            path: "/primitives/step/heartbeat/start",
            query: query_with(
                &client.new_flow_id("primitive-step-heartbeat"),
                &[("batches", "0")],
            ),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "primitives/step/options-override",
            path: "/primitives/step/options-override/start",
            query: query_with(
                &client.new_flow_id("primitive-step-options-override"),
                &[("input", "smoke")],
            ),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "primitives/step/step-decision",
            path: "/primitives/step/step-decision/start",
            query: query_with(
                &client.new_flow_id("primitive-step-decision"),
                &[("mode", "graceful")],
            ),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "primitives/step/wait-types",
            path: "/primitives/step/wait-types/start",
            query: query_with(
                &client.new_flow_id("primitive-step-wait-types"),
                &[("mode", "any"), ("timeoutSeconds", "1")],
            ),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "primitives/attribute",
            path: "/primitives/attribute/start",
            query: query_with(
                &client.new_flow_id("primitive-attribute"),
                &[("message", "smoke")],
            ),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "primitives/channel",
            path: "/primitives/channel/start",
            query: query_with(
                &client.new_flow_id("primitive-channel"),
                &[("inputNum", "1")],
            ),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "primitives/stream",
            path: "/primitives/stream/start",
            query: query_with(
                &client.new_flow_id("primitive-stream"),
                &[("input", "smoke")],
            ),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "primitives/timer",
            path: "/primitives/timer/start",
            query: query_with(&client.new_flow_id("primitive-timer"), &[("seconds", "1")]),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "primitives/rpc",
            path: "/primitives/rpc/start",
            query: query(&client.new_flow_id("primitive-rpc")),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "primitives/subflow",
            path: "/primitives/subflow/start",
            query: query_with(
                &client.new_flow_id("primitive-subflow"),
                &[("inputNum", "1")],
            ),
            flags: FlowSmokeFlags::NONE,
        },
        FlowSmokeEntry {
            name: "primitives/client-apis",
            path: "/primitives/client-apis/start",
            query: query_with(
                &client.new_flow_id("primitive-client-apis"),
                &[("keyword", "smoke")],
            ),
            flags: FlowSmokeFlags::NONE,
        },
    ]
}

fn parallel_subflows_entry(
    client: &mut FlowSmokeHttpClient,
    kind: &'static str,
    path: &'static str,
) -> FlowSmokeEntry {
    let name = match kind {
        "basic" => "patterns/parallel-subflows/basic",
        "wait-for-half" => "patterns/parallel-subflows/wait-for-half",
        "long-lived-parent" => "patterns/parallel-subflows/long-lived-parent",
        "short-lived-parent" => "patterns/parallel-subflows/short-lived-parent",
        _ => panic!("unknown Parallel SubFlows kind: {kind}"),
    };
    FlowSmokeEntry {
        name,
        path,
        query: query(&client.new_flow_id(&format!("parallel-subflows-{kind}"))),
        flags: FlowSmokeFlags::NONE,
    }
}

/// Starts the shared environment once so every flow smoke test reuses one Worker.
fn shared_flow_smoke_environment() -> &'static FlowSmokeEnvironment {
    static ENVIRONMENT: OnceLock<FlowSmokeEnvironment> = OnceLock::new();
    ENVIRONMENT.get_or_init(FlowSmokeEnvironment::start)
}

/// Declares one filterable test per catalog entry that some engines reject.
macro_rules! dedicated_flow_smoke_entry_tests {
    ($($test_name:ident => $entry_name:literal,)*) => {
        const DEDICATED_FLOW_SMOKE_ENTRY_NAMES: &[&str] = &[$($entry_name),*];
        $(
            #[test]
            #[ignore = "requires dexcli dev"]
            fn $test_name() {
                run_dedicated_flow_smoke_entry($entry_name);
            }
        )*
    };
}

dedicated_flow_smoke_entry_tests! {
    flow_smoke_products_money_transfer => "products/money-transfer",
    flow_smoke_products_order_processing => "products/order-processing",
    flow_smoke_patterns_polling => "patterns/polling",
    flow_smoke_patterns_entity_store => "patterns/entity-store",
    flow_smoke_patterns_manual_recovery => "patterns/manual-recovery",
    flow_smoke_patterns_recovery => "patterns/recovery",
    flow_smoke_primitives_step_options_override => "primitives/step/options-override",
}

fn run_dedicated_flow_smoke_entry(entry_name: &str) {
    let environment = shared_flow_smoke_environment();
    let mut http_client = FlowSmokeHttpClient::new(environment.http_base_url.clone());
    let entry = flow_smoke_catalog(&mut http_client)
        .into_iter()
        .find(|entry| entry.name == entry_name)
        .unwrap_or_else(|| panic!("flow smoke entry {entry_name} is not in the catalog"));
    run_flow_smoke_entry(&http_client, &entry);
}

#[test]
#[ignore = "requires dexcli dev"]
fn flow_smoke_all_registered_flows_via_controller() {
    let environment = shared_flow_smoke_environment();
    let mut http_client = FlowSmokeHttpClient::new(environment.http_base_url.clone());
    let catalog = flow_smoke_catalog(&mut http_client);
    assert!(!catalog.is_empty(), "flow smoke catalog is empty");
    for entry in catalog
        .iter()
        .filter(|entry| !DEDICATED_FLOW_SMOKE_ENTRY_NAMES.contains(&entry.name))
    {
        run_flow_smoke_entry(&http_client, entry);
    }
    for (name, path) in [
        (
            "patterns/sequentially-chunked-attribute-map",
            "/patterns/sequentially-chunked-attribute-map/start",
        ),
        (
            "patterns/hash-partitioned-attribute-map",
            "/patterns/hash-partitioned-attribute-map/start",
        ),
    ] {
        let entry = FlowSmokeEntry {
            name,
            path,
            query: HashMap::new(),
            flags: FlowSmokeFlags::NO_START_STEP,
        };
        let result = http_client.trigger_post(entry.path);
        assert!(
            !result.flow_id.is_empty(),
            "{}: controller response did not include flowID",
            entry.name
        );
        assert_flow_smoke_no_unexpected_failures(&entry, &result.flow_id, &result.run_id);
    }
}

fn run_flow_smoke_entry(http_client: &FlowSmokeHttpClient, entry: &FlowSmokeEntry) {
    let result = http_client.trigger_get(entry.path, &entry.query);
    assert!(
        !result.flow_id.is_empty(),
        "{}: controller response did not include flowID",
        entry.name
    );
    assert_flow_smoke_start_step(entry, &result.flow_id, &result.run_id);
    assert_flow_smoke_no_unexpected_failures(entry, &result.flow_id, &result.run_id);
}

fn available_worker_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("read ephemeral port")
        .port()
}

fn await_worker(worker_port: u16, worker_thread: &JoinHandle<SdkResult<()>>) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if worker_thread.is_finished() {
            panic!("Worker exited before becoming ready");
        }
        if TcpStream::connect(format!("127.0.0.1:{worker_port}")).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("Worker did not become ready");
}

fn await_http(http_port: u16) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if TcpStream::connect(format!("127.0.0.1:{http_port}")).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("HTTP server did not become ready");
}
