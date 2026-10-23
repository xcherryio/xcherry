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

use std::collections::HashMap;
use std::process::Command;
use std::time::{Duration, Instant};

use reqwest::blocking::Client as HttpClient;
use serde_json::Value;

#[derive(Clone, Copy)]
pub(crate) struct FlowSmokeFlags {
    pub step_start_may_fail: bool,
    pub no_start_step: bool,
}

impl FlowSmokeFlags {
    pub(crate) const NONE: Self = Self {
        step_start_may_fail: false,
        no_start_step: false,
    };
    pub(crate) const STEP_START_MAY_FAIL: Self = Self {
        step_start_may_fail: true,
        no_start_step: false,
    };
    pub(crate) const NO_START_STEP: Self = Self {
        step_start_may_fail: false,
        no_start_step: true,
    };
}

pub(crate) struct FlowSmokeEntry {
    pub name: &'static str,
    pub path: &'static str,
    pub query: HashMap<String, String>,
    pub flags: FlowSmokeFlags,
    pub server_zd_unsupported_feature: Option<&'static str>,
}

pub(crate) struct FlowSmokeHttpClient {
    pub base_url: String,
    http: HttpClient,
    flow_counter: u64,
}

pub(crate) struct FlowSmokeTriggerResult {
    pub flow_id: String,
    pub run_id: String,
}

impl FlowSmokeHttpClient {
    pub(crate) fn new(base_url: String) -> Self {
        Self {
            base_url,
            http: HttpClient::new(),
            flow_counter: 0,
        }
    }

    pub(crate) fn new_flow_id(&mut self, prefix: &str) -> String {
        self.flow_counter += 1;
        format!(
            "{prefix}-{}-{flow_counter}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock before epoch")
                .as_nanos(),
            flow_counter = self.flow_counter
        )
    }

    pub(crate) fn trigger_get(
        &self,
        path: &str,
        query: &HashMap<String, String>,
    ) -> FlowSmokeTriggerResult {
        let mut request = self.http.get(format!("{}{path}", self.base_url));
        for (key, value) in query {
            request = request.query(&[(key, value)]);
        }
        let response = request.send().expect("flow smoke HTTP GET");
        let status = response.status();
        let body = response.text().expect("read flow smoke response");
        assert!(status.is_success(), "GET {path} returned {status}: {body}");
        parse_flow_trigger_response(
            &body,
            query.get("workflowId").map(String::as_str).unwrap_or(""),
        )
    }

    pub(crate) fn trigger_post(&self, path: &str) -> FlowSmokeTriggerResult {
        let response = self
            .http
            .post(format!("{}{path}", self.base_url))
            .send()
            .expect("flow smoke HTTP POST");
        let status = response.status();
        let body = response.text().expect("read flow smoke response");
        assert!(status.is_success(), "POST {path} returned {status}: {body}");
        parse_flow_trigger_response(&body, "")
    }
}

pub(crate) fn parse_flow_trigger_response(
    body: &str,
    workflow_id_from_query: &str,
) -> FlowSmokeTriggerResult {
    let trimmed = body.trim();
    if let Ok(json) = serde_json::from_str::<Value>(trimmed)
        && let Some(flow_id) = json
            .get("flowID")
            .or_else(|| json.get("flowId"))
            .and_then(Value::as_str)
    {
        let run_id = json
            .get("runID")
            .or_else(|| json.get("runId"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        return FlowSmokeTriggerResult {
            flow_id: flow_id.to_string(),
            run_id,
        };
    }
    if let Some(run_id) = regex_run_id(trimmed) {
        return FlowSmokeTriggerResult {
            flow_id: workflow_id_from_query.to_string(),
            run_id,
        };
    }
    if let Some(flow_id) = trimmed.strip_prefix("Started workflowId: ") {
        return FlowSmokeTriggerResult {
            flow_id: flow_id.to_string(),
            run_id: String::new(),
        };
    }
    if let Some(flow_id) = trimmed.strip_prefix("started workflowId: ") {
        return FlowSmokeTriggerResult {
            flow_id: flow_id.to_string(),
            run_id: String::new(),
        };
    }
    if !workflow_id_from_query.is_empty() {
        return FlowSmokeTriggerResult {
            flow_id: workflow_id_from_query.to_string(),
            run_id: String::new(),
        };
    }
    FlowSmokeTriggerResult {
        flow_id: String::new(),
        run_id: trimmed.to_string(),
    }
}

fn regex_run_id(body: &str) -> Option<String> {
    body.split("runId")
        .nth(1)
        .and_then(|rest| rest.split_whitespace().next().map(str::to_string))
}

pub(crate) fn assert_flow_smoke_start_step(entry: &FlowSmokeEntry, flow_id: &str, run_id: &str) {
    if entry.flags.no_start_step {
        return;
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        let history = run_dexcli_flow_history(flow_id, run_id);
        let start_step_type = flow_started_start_step_type(&history);
        if !start_step_type.is_empty() {
            if entry.flags.step_start_may_fail {
                return;
            }
            if has_start_step_progress(&history, &start_step_type) {
                return;
            }
            let state = run_dexcli_flow_state(flow_id, run_id);
            if state.get("flowStatus").and_then(Value::as_str) == Some("FLOW_STATUS_RUNNING")
                && history
                    .get("events")
                    .and_then(Value::as_array)
                    .is_some_and(|events| events.len() > 1)
            {
                return;
            }
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    panic!("start step did not succeed for {}", entry.name);
}

pub(crate) fn assert_flow_smoke_no_unexpected_failures(
    entry: &FlowSmokeEntry,
    flow_id: &str,
    run_id: &str,
) {
    let history = run_dexcli_flow_history(flow_id, run_id);
    let events = history
        .get("events")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for event in &events {
        let event_type = event.get("type").and_then(Value::as_str).unwrap_or("");
        match event_type {
            "StepExecuteFailed" | "StepWaitForFailed" if !entry.flags.step_start_may_fail => {
                panic!("unexpected failure event for {}: {event_type}", entry.name);
            }
            "FlowClosed" => {
                let payload = event.get("payload").cloned().unwrap_or(Value::Null);
                if is_terminal_flow_closed_failure(&payload) && !entry.flags.step_start_may_fail {
                    panic!(
                        "unexpected terminal flow closure for {}: {payload}",
                        entry.name
                    );
                }
            }
            _ => {}
        }
    }
}

fn flow_started_start_step_type(history: &Value) -> String {
    let Some(events) = history.get("events").and_then(Value::as_array) else {
        return String::new();
    };
    for event in events {
        if event.get("type").and_then(Value::as_str) != Some("FlowStartedOrContinued") {
            continue;
        }
        if let Some(start_step_type) = event
            .pointer("/payload/initialStart/startStepType")
            .and_then(Value::as_str)
        {
            return start_step_type.to_string();
        }
    }
    String::new()
}

fn has_start_step_progress(history: &Value, start_step_type: &str) -> bool {
    let Some(events) = history.get("events").and_then(Value::as_array) else {
        return false;
    };
    for event in events {
        let event_type = event.get("type").and_then(Value::as_str).unwrap_or("");
        if event_type != "StepWaitForCompleted" && event_type != "StepExecuteCompleted" {
            continue;
        }
        if history_event_step_type(event.get("payload").unwrap_or(&Value::Null)) == start_step_type
        {
            return true;
        }
    }
    false
}

fn history_event_step_type(payload: &Value) -> String {
    if let Some(step_type) = payload.get("stepType").and_then(Value::as_str) {
        return step_type.to_string();
    }
    if let Some(step_type) = payload.pointer("/context/stepType").and_then(Value::as_str) {
        return step_type.to_string();
    }
    payload
        .pointer("/input/stepType")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

fn is_terminal_flow_closed_failure(payload: &Value) -> bool {
    let status_failed = match payload.get("flowStatus") {
        Some(Value::String(status)) => !matches!(
            status.as_str(),
            "FLOW_STATUS_COMPLETED"
                | "FLOW_STATUS_CONTINUED_AS_NEW"
                | "FLOW_STATUS_RUNNING"
                | "FLOW_STATUS_UNSPECIFIED"
                | ""
        ),
        Some(Value::Number(status)) => {
            let numeric = status.as_i64().unwrap_or(-1);
            numeric != 0 && numeric != 2 && numeric != 7
        }
        _ => false,
    };
    status_failed
        || payload
            .get("errorType")
            .and_then(Value::as_str)
            .is_some_and(|error_type| {
                !error_type.is_empty() && error_type != "FLOW_ERROR_TYPE_UNSPECIFIED"
            })
}

fn run_dexcli_flow_history(flow_id: &str, run_id: &str) -> Value {
    let mut args = vec![
        "flow".to_string(),
        "history".to_string(),
        flow_id.to_string(),
        "--server".to_string(),
        flow_service_address(),
        "--output".to_string(),
        "json".to_string(),
        "--page-size".to_string(),
        "50".to_string(),
    ];
    if !run_id.is_empty() {
        args.push("--run-id".to_string());
        args.push(run_id.to_string());
    }
    run_dexcli_json(args)
}

fn run_dexcli_flow_state(flow_id: &str, run_id: &str) -> Value {
    let mut args = vec![
        "flow".to_string(),
        "state".to_string(),
        flow_id.to_string(),
        "--server".to_string(),
        flow_service_address(),
        "--output".to_string(),
        "json".to_string(),
    ];
    if !run_id.is_empty() {
        args.push("--run-id".to_string());
        args.push(run_id.to_string());
    }
    run_dexcli_json(args)
}

fn run_dexcli_json(args: Vec<String>) -> Value {
    let output = Command::new(dexcli_path())
        .args(args)
        .output()
        .expect("run dexcli");
    assert!(
        output.status.success(),
        "dexcli failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("decode dexcli json")
}

fn flow_service_address() -> String {
    std::env::var("DEX_SERVER_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8801".to_string())
}

fn dexcli_path() -> String {
    if let Ok(path) = std::env::var("DEXCLI_PATH")
        && !path.trim().is_empty()
    {
        return path;
    }
    let repo_root = find_repo_root();
    let output_path =
        std::env::temp_dir().join(format!("dexcli-rust-smoke-{}", std::process::id()));
    let status = Command::new("go")
        .args(["build", "-trimpath", "-o"])
        .arg(&output_path)
        .arg("./cmd/dexcli")
        .current_dir(repo_root.join("cli"))
        .env("GOWORK", "off")
        .status()
        .expect("build dexcli");
    assert!(status.success(), "build dexcli");
    output_path.to_string_lossy().into_owned()
}

fn find_repo_root() -> std::path::PathBuf {
    let mut directory = std::env::current_dir().expect("current dir");
    loop {
        if directory.join("cli/cmd/dexcli/main.go").is_file() {
            return directory;
        }
        if !directory.pop() {
            panic!("find repository root");
        }
    }
}

pub(crate) fn query(workflow_id: &str) -> HashMap<String, String> {
    HashMap::from([(String::from("workflowId"), workflow_id.to_string())])
}

pub(crate) fn query_with(workflow_id: &str, extra: &[(&str, &str)]) -> HashMap<String, String> {
    let mut values = query(workflow_id);
    for (key, value) in extra {
        values.insert((*key).to_string(), (*value).to_string());
    }
    values
}
