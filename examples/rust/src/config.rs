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

pub(crate) struct ExamplesConfig {
    pub(crate) server_address: String,
    pub(crate) worker_bind_address: String,
    pub(crate) http_address: String,
    pub(crate) cache_directory: String,
}

impl ExamplesConfig {
    pub(crate) fn from_environment() -> Self {
        let server_address =
            std::env::var("DEX_SERVER_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8801".to_string());
        let worker_bind_address = std::env::var("DEX_WORKER_BIND_ADDRESS")
            .unwrap_or_else(|_| "127.0.0.1:8803".to_string());
        let http_address = std::env::var("DEX_EXAMPLES_HTTP_ADDRESS")
            .unwrap_or_else(|_| "127.0.0.1:8080".to_string());
        let cache_directory = std::env::var("DEX_BLOB_CACHE_DIR")
            .unwrap_or_else(|_| "/tmp/dex-rust-examples-blobs".to_string());

        Self {
            server_address,
            worker_bind_address,
            http_address,
            cache_directory,
        }
    }
}
