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

use std::sync::Arc;

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use dex_sdk::{SdkError, SdkResult};
use serde::Serialize;

pub type SharedClient = Arc<dex_sdk::Client>;

#[derive(Serialize)]
pub struct ErrorBody {
    pub error: String,
}

#[derive(Serialize)]
pub struct StartResponse {
    #[serde(rename = "flowID")]
    pub flow_id: String,
    #[serde(rename = "runID")]
    pub run_id: String,
}

pub fn new_flow_id(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0)
    )
}

pub fn map_sdk_error(error: SdkError) -> (StatusCode, Json<ErrorBody>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorBody {
            error: error.to_string(),
        }),
    )
}

pub fn is_already_started(error: &SdkError) -> bool {
    matches!(error, SdkError::FlowAlreadyStarted { .. })
}

pub fn is_missing_or_inactive(error: &SdkError) -> bool {
    matches!(
        error,
        SdkError::FlowNotFound { .. } | SdkError::FlowNotActiveOrNotFound { .. }
    )
}

pub fn ok_json<T: Serialize>(value: T) -> Response {
    Json(value).into_response()
}

pub fn ok_text(value: impl Into<String>) -> Response {
    value.into().into_response()
}

pub fn run_blocking<F, T>(function: F) -> SdkResult<T>
where
    F: FnOnce() -> SdkResult<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread => {
            std::thread::spawn(function)
                .join()
                .expect("blocking Dex client call panicked")
        }
        Ok(_) => tokio::task::block_in_place(function),
        Err(_) => function(),
    }
}
