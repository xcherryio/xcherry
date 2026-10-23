// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! JSON admin dispatch, independent of S3 routing and authentication.

use std::sync::Arc;

use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Limited};
use s3mock::Store;
use serde::Serialize;
use serde_json::json;

use super::{Controls, Counters, FaultAction, FaultRule, S3OperationName};
use crate::server::{RequestBody, S3Response, full_body, run_store_operation};

impl Controls {
    pub(crate) async fn admin(
        &self,
        store: Arc<Store>,
        request: Request<RequestBody>,
    ) -> S3Response {
        match self.dispatch_admin(store, request).await {
            Ok(value) => json_response(StatusCode::OK, &value),
            Err((status, message)) => json_response(status, &json!({"error": message})),
        }
    }

    async fn dispatch_admin(
        &self,
        store: Arc<Store>,
        request: Request<RequestBody>,
    ) -> Result<serde_json::Value, (StatusCode, String)> {
        let path = request.uri().path();
        match (request.method(), path) {
            (&Method::GET, "/_s3mock/faults") => {
                let state = self.state.lock().expect("control operations cannot panic");
                Ok(json!({"rules": state.rules, "seed": self.config.fault_seed}))
            }
            (&Method::PUT, "/_s3mock/faults") => {
                #[derive(serde::Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Replacement {
                    rules: Vec<FaultRule>,
                }
                let body = Limited::new(request.into_body(), 1024 * 1024)
                    .collect()
                    .await
                    .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?
                    .to_bytes();
                let replacement: Replacement = serde_json::from_slice(&body)
                    .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
                for rule in &replacement.rules {
                    if !rule.probability.is_finite() || !(0.0..=1.0).contains(&rule.probability) {
                        return Err((
                            StatusCode::BAD_REQUEST,
                            "probability must be between zero and one".into(),
                        ));
                    }
                    if matches!(rule.action, FaultAction::Error { status } if ![409, 500, 503].contains(&status))
                    {
                        return Err((
                            StatusCode::BAD_REQUEST,
                            "error status must be 409, 500, or 503".into(),
                        ));
                    }
                    if matches!(rule.action, FaultAction::CorruptBody {})
                        && rule.operation != Some(S3OperationName::GetObject)
                    {
                        return Err((
                            StatusCode::BAD_REQUEST,
                            "corrupt_body requires operation GetObject".into(),
                        ));
                    }
                    if matches!(rule.action, FaultAction::CommitThenDrop {})
                        && !matches!(
                            rule.operation,
                            Some(
                                S3OperationName::PutObject
                                    | S3OperationName::UploadPart
                                    | S3OperationName::CompleteMultipartUpload
                            )
                        )
                    {
                        return Err((
                            StatusCode::BAD_REQUEST,
                            "commit_then_drop requires operation PutObject, UploadPart, or CompleteMultipartUpload".into(),
                        ));
                    }
                }
                for rule in &replacement.rules {
                    if matches!(rule.action, FaultAction::Delay { milliseconds } if milliseconds > 600_000)
                    {
                        return Err((
                            StatusCode::BAD_REQUEST,
                            "delay must not exceed 600000 milliseconds".into(),
                        ));
                    }
                    if matches!(rule.action, FaultAction::TruncateResponse {})
                        && !matches!(
                            rule.operation,
                            Some(
                                S3OperationName::GetObject
                                    | S3OperationName::ListBuckets
                                    | S3OperationName::ListObjectsV2
                                    | S3OperationName::ListObjectVersions
                                    | S3OperationName::ListParts
                                    | S3OperationName::ListMultipartUploads
                            )
                        )
                    {
                        return Err((
                            StatusCode::BAD_REQUEST,
                            "truncate_response requires GetObject or a List operation".into(),
                        ));
                    }
                }
                let mut state = self.state.lock().expect("control operations cannot panic");
                state.rules = replacement.rules;
                state.random_state = self.config.fault_seed;
                Ok(json!({"rules": state.rules, "seed": self.config.fault_seed}))
            }
            (&Method::DELETE, "/_s3mock/faults") => {
                let mut state = self.state.lock().expect("control operations cannot panic");
                state.rules.clear();
                self.fault_reset
                    .send_modify(|generation| *generation = generation.wrapping_add(1));
                state.random_state = self.config.fault_seed;
                Ok(json!({"rules": []}))
            }
            (&Method::GET, "/_s3mock/counters") => {
                let state = self.state.lock().expect("control operations cannot panic");
                Ok(
                    json!({"prefix_depth": self.config.counter_prefix_depth, "counters": state.counters}),
                )
            }
            (&Method::DELETE, "/_s3mock/counters") => {
                self.state
                    .lock()
                    .expect("control operations cannot panic")
                    .counters = Counters::default();
                Ok(json!({"reset": true}))
            }
            (&Method::GET, "/_s3mock/requests") => {
                let state = self.state.lock().expect("control operations cannot panic");
                Ok(
                    json!({"capacity": self.config.request_log_capacity, "requests": state.requests}),
                )
            }
            (&Method::POST, "/_s3mock/gc") => {
                let result = run_store_operation(&store, Store::collect_orphan_data_files)
                    .await
                    .map_err(|error| (error.status, error.message))?;
                Ok(
                    json!({"examined_files": result.examined_files, "removed_files": result.removed_files}),
                )
            }
            (&Method::GET, "/_s3mock/lifecycle/clock") => Ok(lifecycle_clock(&store)),
            (&Method::POST, "/_s3mock/lifecycle/advance") => {
                #[derive(serde::Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Advance {
                    milliseconds: i64,
                }
                let body = Limited::new(request.into_body(), 64 * 1024)
                    .collect()
                    .await
                    .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?
                    .to_bytes();
                let advance: Advance = serde_json::from_slice(&body)
                    .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
                // The clock only moves forward, so a pass can never undo an earlier expiration decision.
                if advance.milliseconds < 0 {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        "milliseconds must not be negative".into(),
                    ));
                }
                store.advance_lifecycle_clock(advance.milliseconds);
                Ok(lifecycle_clock(&store))
            }
            (&Method::POST, "/_s3mock/lifecycle/run") => {
                let run = run_store_operation(&store, Store::run_lifecycle)
                    .await
                    .map_err(|error| (error.status, error.message))?;
                Ok(json!({
                    "expired_current_versions": run.expired_current_versions,
                    "removed_delete_markers": run.removed_delete_markers,
                    "expired_noncurrent_versions": run.expired_noncurrent_versions,
                    "aborted_uploads": run.aborted_uploads,
                }))
            }
            _ => Err((
                StatusCode::NOT_FOUND,
                "unknown admin endpoint or method".into(),
            )),
        }
    }
}

fn lifecycle_clock(store: &Store) -> serde_json::Value {
    json!({
        "now_unix_ms": store.lifecycle_now_unix_ms(),
        "offset_ms": store.lifecycle_clock_offset_ms(),
    })
}

fn json_response(status: StatusCode, value: &impl Serialize) -> S3Response {
    let bytes = serde_json::to_vec(value).expect("admin responses contain JSON-compatible values");
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(full_body(bytes))
        .expect("admin response headers are static")
}
