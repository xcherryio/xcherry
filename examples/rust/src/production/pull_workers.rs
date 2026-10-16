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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, EXPRESS OR IMPLIED.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::{Arc, LazyLock};
use std::thread::{self, JoinHandle};

use dex_sdk::{
    Attribute, BlobCache, Client, ClientOptions, Context, Flow, HandlerResult, PersistenceSchema,
    PullWorkerOptions, Registry, Rpc, RpcList, RpcResult, SdkResult, Step, StepDecision, StepList,
    Worker, WorkerOptions,
};

pub const GET_GREETING: Rpc<(), String> = Rpc::new("GetGreeting");

static GREETING_MESSAGE: LazyLock<Attribute<String>> =
    LazyLock::new(|| Attribute::new("pull-greeting-message"));

#[derive(Default)]
pub struct GreetingFlow {
    greet_step: GreetStep,
}

impl GreetingFlow {
    fn get_greeting(&self, context: &mut Context) -> HandlerResult<RpcResult<String>> {
        Ok(RpcResult::new(GREETING_MESSAGE.get_required(context)?))
    }
}

impl Flow for GreetingFlow {
    type StartInput = String;

    fn steps(&self) -> StepList<'_, Self::StartInput> {
        StepList::start(&self.greet_step)
    }

    fn persistence(&self) -> PersistenceSchema {
        PersistenceSchema::new().attribute(&GREETING_MESSAGE)
    }

    fn rpcs(&self) -> RpcList<Self> {
        RpcList::new().function_without_input(GET_GREETING, Self::get_greeting)
    }
}

#[derive(Default)]
struct GreetStep;

impl Step for GreetStep {
    type Input = String;

    fn execute(&self, context: &mut Context, name: Self::Input) -> HandlerResult<StepDecision> {
        let message = format!("Hello, {name}!");
        GREETING_MESSAGE.set(context, message.clone())?;
        Ok(StepDecision::graceful_complete(message))
    }
}

pub fn start_pull_greeting(
    cache: Arc<BlobCache>,
    server_address: &str,
) -> SdkResult<(Arc<Worker>, Client, JoinHandle<SdkResult<()>>)> {
    let registry = Registry::new().register(GreetingFlow::default())?;
    let worker = Arc::new(Worker::try_new(
        registry.clone(),
        Arc::clone(&cache),
        WorkerOptions::new()
            .server_address(server_address)
            .pull(PullWorkerOptions::new().max_concurrent_tasks(8)),
    )?);
    let client = Client::try_new(
        registry,
        cache,
        ClientOptions::new()
            .server_address(server_address)
            .worker_target(worker.worker_target().clone()),
    )?;
    let running_worker = Arc::clone(&worker);
    let worker_thread = thread::spawn(move || running_worker.start());
    Ok((worker, client, worker_thread))
}
