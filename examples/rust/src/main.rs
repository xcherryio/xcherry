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

//! Runnable Rust examples for Dex Flows, Steps, and application patterns.

mod config;

use std::sync::Arc;
use std::thread;

use dex_examples_rust::patterns::polling::FakeJobService;
use dex_examples_rust::server::build_router;
use dex_examples_rust::shared::{
    PULL_REGISTRATION_TIMEOUT, WorkerMode, example_pull_worker_options,
};
use dex_examples_rust::{create_example_registry, create_worker_registry};
use dex_sdk::{
    BlobCache, BlobCacheConfig, Client, ClientOptions, Worker, WorkerOptions, WorkerTarget,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = config::ExamplesConfig::from_environment()?;

    let cache = Arc::new(BlobCache::open(BlobCacheConfig::new(
        config.cache_directory,
        64 * 1024 * 1024,
        10_000,
    )?)?);
    let polling_jobs = Arc::new(FakeJobService::default());
    let worker_target = match config.worker_mode {
        WorkerMode::Push => WorkerTarget::new(config.worker_bind_address.clone()),
        WorkerMode::Pull => WorkerTarget::pull(),
    };
    let client = Arc::new(Client::try_new(
        create_example_registry(Arc::clone(&polling_jobs))?,
        Arc::clone(&cache),
        ClientOptions::new()
            .server_address(config.server_address.clone())
            .worker_target(worker_target.clone()),
    )?);
    let worker_options = match config.worker_mode {
        WorkerMode::Push => WorkerOptions::new()
            .server_address(config.server_address.clone())
            .bind_address(config.worker_bind_address.clone())
            .worker_target(worker_target),
        WorkerMode::Pull => WorkerOptions::new()
            .server_address(config.server_address.clone())
            .pull(example_pull_worker_options()),
    };
    let worker = Arc::new(Worker::try_new(
        create_worker_registry(Arc::clone(&client), Arc::clone(&polling_jobs))?,
        Arc::clone(&cache),
        worker_options,
    )?);
    let running_worker = Arc::clone(&worker);
    let worker_handle = thread::spawn(move || running_worker.start());
    let worker_description = match config.worker_mode {
        WorkerMode::Push => format!("worker {}", config.worker_bind_address),
        WorkerMode::Pull => {
            if let Err(error) = wait_for_pull_registration(&worker).await {
                worker.stop();
                if let Err(stop_error) = worker_handle.join().expect("worker thread panicked") {
                    eprintln!("pull Worker stopped with an error: {stop_error}");
                }
                return Err(error);
            }
            "pull worker".to_string()
        }
    };

    let router = build_router(client, polling_jobs);
    let listener = tokio::net::TcpListener::bind(&config.http_address).await?;
    println!(
        "Dex Rust examples listening on http://{} ({worker_description})",
        config.http_address
    );
    axum::serve(listener, router).await?;

    if let Err(error) = worker_handle.join().expect("worker thread panicked") {
        return Err(error.into());
    }
    cache.close()?;
    Ok(())
}

async fn wait_for_pull_registration(worker: &Worker) -> Result<(), Box<dyn std::error::Error>> {
    tokio::time::timeout(PULL_REGISTRATION_TIMEOUT, worker.wait_pull_registered()).await??;
    Ok(())
}
