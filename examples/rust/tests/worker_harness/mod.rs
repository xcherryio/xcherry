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

use dex_examples_rust::shared::{
    PULL_REGISTRATION_TIMEOUT, WORKER_MODE_ENVIRONMENT_VARIABLE, WorkerMode,
};
use dex_sdk::Worker;

pub(crate) fn read_worker_mode() -> WorkerMode {
    let value = std::env::var(WORKER_MODE_ENVIRONMENT_VARIABLE).ok();
    WorkerMode::parse(value.as_deref()).expect("valid DEX_EXAMPLES_WORKER_MODE")
}

/// Returns true and reports the reason when server-zd rejects a sample feature.
pub(crate) fn should_skip_on_server_zd(feature: &str) -> bool {
    let is_server_zd = std::env::var("DEX_INTEG_ENGINE").is_ok_and(|engine| engine == "zd");
    if is_server_zd {
        eprintln!("server-zd does not support {feature}");
    }
    is_server_zd
}

pub(crate) fn await_pull_registration(worker: &Worker) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create pull registration runtime");
    runtime
        .block_on(async {
            tokio::time::timeout(PULL_REGISTRATION_TIMEOUT, worker.wait_pull_registered()).await
        })
        .expect("pull Worker registered before the timeout")
        .expect("pull Worker registration");
}
