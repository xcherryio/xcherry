// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::fs;
use std::net::SocketAddr;

use dexzd_protocol::dex::RetryPolicy;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metrics_endpoint_tracks_one_completed_flow_and_worker_poll() {
    let primary = S3Emulator::start(PRIMARY_BUCKET, SignatureVerification::Strict).await;
    let directory = TempDir::new().expect("temporary directory exists");
    let api_address = free_loopback_address();
    let metrics_address = free_loopback_address();
    let configuration_path = directory.path().join("metrics.yaml");
    ServeConfig {
        primary_endpoint: &primary.endpoint(),
        hard_delete_endpoint: None,
        listen: api_address,
        local_directory: &directory.path().join("store"),
        lease_duration: HELD_LEASE_DURATION,
        fail_if_held: true,
        parade_db: None,
    }
    .write(&configuration_path);
    let mut configuration =
        fs::read_to_string(&configuration_path).expect("configuration is readable");
    configuration.push_str(&format!(
        "metrics:\n  listen: '{metrics_address}'\nmatching:\n  dispatchWait: 1s\n  stickyWait: 1s\n"
    ));
    fs::write(&configuration_path, configuration).expect("configuration is writable");
    let mut process = ServeProcess::spawn(
        &configuration_path,
        api_address,
        directory.path().join("metrics.log"),
    );
    let channel = process.wait_until_serving().await;
    let before = scrape(metrics_address, "/metrics").await;
    assert!(before.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(
        before.contains("Content-Type: application/openmetrics-text; version=1.0.0; charset=utf-8")
    );
    assert!(
        scrape(metrics_address, "/other")
            .await
            .starts_with("HTTP/1.1 404")
    );

    let worker = PullWorker::start(
        WorkerTaskServiceClient::new(channel.clone()),
        FLOW_TYPE,
        Box::new(|request| {
            assert_eq!(request.step_type, "GreetStep");
            InvokeExecuteMethodResponse {
                step_decision: Some(StepDecision {
                    close_decision: Some(CloseDecision {
                        close_decision_type: CloseDecisionType::ForceComplete.into(),
                        close_input: Some(string_value("completed")),
                        ..CloseDecision::default()
                    }),
                    ..StepDecision::default()
                }),
                ..InvokeExecuteMethodResponse::default()
            }
        }),
    )
    .await;
    let mut client = FlowServiceClient::new(channel);
    client
        .start_flow(StartFlowRequest {
            flow_id: FLOW_ID.to_owned(),
            flow_type: FLOW_TYPE.to_owned(),
            start_step_type: "GreetStep".to_owned(),
            step_input: Some(string_value("start")),
            step_options: Some(StepOptions {
                skip_wait_for: true,
                ..StepOptions::default()
            }),
            flow_start_options: Some(FlowStartOptions {
                flow_config_override: Some(FlowConfig {
                    worker_target: Some(WorkerTarget {
                        is_pull_mode: true,
                        ..WorkerTarget::default()
                    }),
                    ..FlowConfig::default()
                }),
                ..FlowStartOptions::default()
            }),
            request_id: "metrics-request".to_owned(),
            ..StartFlowRequest::default()
        })
        .await
        .expect("Flow starts");
    let result = client
        .wait_for_flow(WaitForFlowRequest {
            flow_id: FLOW_ID.to_owned(),
            needs_results: true,
            wait_time_seconds: 30,
            ..WaitForFlowRequest::default()
        })
        .await
        .expect("Flow completes")
        .into_inner();
    assert_eq!(result.flow_status, FlowStatus::Completed as i32);
    worker.stop().await;

    client
        .start_flow(StartFlowRequest {
            flow_id: "metrics-failed-flow".to_owned(),
            flow_type: FLOW_TYPE.to_owned(),
            start_step_type: "GreetStep".to_owned(),
            step_input: Some(string_value("start")),
            step_options: Some(StepOptions {
                skip_wait_for: true,
                execute_timeout_seconds: 2,
                execute_retry_policy: Some(RetryPolicy {
                    maximum_attempts: 1,
                    ..RetryPolicy::default()
                }),
                ..StepOptions::default()
            }),
            flow_start_options: Some(FlowStartOptions {
                flow_config_override: Some(FlowConfig {
                    worker_target: Some(WorkerTarget {
                        is_pull_mode: true,
                        ..WorkerTarget::default()
                    }),
                    ..FlowConfig::default()
                }),
                ..FlowStartOptions::default()
            }),
            request_id: "metrics-failed-request".to_owned(),
            ..StartFlowRequest::default()
        })
        .await
        .expect("second Flow starts");
    let failed = client
        .wait_for_flow(WaitForFlowRequest {
            flow_id: "metrics-failed-flow".to_owned(),
            wait_time_seconds: 30,
            ..WaitForFlowRequest::default()
        })
        .await
        .expect("second Flow closes")
        .into_inner();
    assert_eq!(failed.flow_status, FlowStatus::Failed as i32);

    let after = scrape(metrics_address, "/metrics").await;
    let body = after.split_once("\r\n\r\n").expect("HTTP body exists").1;
    let before_body = before.split_once("\r\n\r\n").expect("HTTP body exists").1;
    for name in [
        "dexzd_build_info",
        "dexzd_behavior_version",
        "dexzd_node_role",
        "dexzd_lease_epoch",
        "dexzd_lease_renewals_total",
        "dexzd_api_requests_total",
        "dexzd_api_request_latency_seconds",
        "dexzd_commit_batches_total",
        "dexzd_commit_records_total",
        "dexzd_commit_bytes_total",
        "dexzd_commit_latency_seconds",
        "dexzd_object_store_requests_total",
        "dexzd_object_store_request_errors_total",
        "dexzd_object_store_put_bytes_total",
        "dexzd_snapshots_total",
        "dexzd_snapshot_last_duration_seconds",
        "dexzd_snapshot_last_bytes",
        "dexzd_recovery_last_duration_seconds",
        "dexzd_recovery_last_replayed_bytes",
        "dexzd_local_storage_bytes",
        "dexzd_timer_lag_seconds",
        "dexzd_matching_queued_tasks",
        "dexzd_matching_worker_streams",
        "dexzd_step_attempt_failures_total",
        "dexzd_step_retries_exhausted_total",
        "dexzd_flow_closes_total",
        "dexzd_flow_end_to_end_latency_seconds",
        "dexzd_step_schedule_to_start_latency_seconds",
        "dexzd_worker_stream_events_total",
    ] {
        let family = name.strip_suffix("_total").unwrap_or(name);
        assert!(
            body.contains(&format!("# TYPE {family} ")),
            "missing {name}: {body}"
        );
        if name.ends_with("_latency_seconds") {
            assert!(
                body.lines()
                    .any(|line| line.starts_with(&format!("{name}_count")))
            );
        } else {
            assert!(
                body.lines().any(|line| {
                    line.strip_prefix(name)
                        .is_some_and(|tail| tail.starts_with(' ') || tail.starts_with('{'))
                }),
                "missing exposed sample {name}"
            );
        }
    }
    #[cfg(target_os = "linux")]
    for name in [
        "process_resident_memory_bytes",
        "process_cpu_seconds_total",
        "process_open_fds",
        "process_start_time_seconds",
    ] {
        assert!(
            body.lines().any(|line| line.starts_with(name)),
            "missing {name}"
        );
    }
    assert!(
        sample_sum(body, "dexzd_commit_batches_total")
            > sample_sum(before_body, "dexzd_commit_batches_total")
    );
    assert!(
        sample_sum(body, "dexzd_commit_records_total")
            > sample_sum(before_body, "dexzd_commit_records_total")
    );
    assert!(
        sample_sum(body, "dexzd_object_store_put_bytes_total")
            > sample_sum(before_body, "dexzd_object_store_put_bytes_total")
    );
    assert!(
        sample_sum(body, "dexzd_api_requests_total")
            > sample_sum(before_body, "dexzd_api_requests_total")
    );
    assert!(body.contains("rpc_class=\"worker_poll\""));
    assert!(!body.contains("dexzd_api_request_latency_seconds_count{rpc_class=\"worker_poll\""));
    for close_status in ["completed", "failed"] {
        assert!(
            sample_with_label(
                body,
                "dexzd_flow_closes_total",
                "close_status",
                close_status
            ) > 0.0,
            "{close_status} Flow close did not move"
        );
    }
    for event in ["delivered", "closed"] {
        assert!(
            sample_with_label(body, "dexzd_worker_stream_events_total", "event", event) > 0.0,
            "{event} did not move"
        );
    }
    for line in body
        .lines()
        .filter(|line| line.starts_with("dexzd_step_attempt_failures_total{"))
    {
        let failure_kind = line
            .split("failure_kind=\"")
            .nth(1)
            .and_then(|suffix| suffix.split('"').next())
            .expect("Step failure kind label exists");
        assert!(matches!(
            failure_kind,
            "worker_error"
                | "method_timeout"
                | "heartbeat_timeout"
                | "dispatch_error"
                | "invalid_response"
                | "unsupported_feature"
                | "value_too_large"
                | "request_too_large"
        ));
    }
    for name in [
        "dexzd_api_request_latency_seconds_count",
        "dexzd_step_attempt_failures_total",
        "dexzd_step_retries_exhausted_total",
        "dexzd_flow_closes_total",
        "dexzd_flow_end_to_end_latency_seconds_count",
        "dexzd_step_schedule_to_start_latency_seconds_count",
        "dexzd_worker_stream_events_total",
    ] {
        assert!(
            sample_sum(body, name) > sample_sum(before_body, name),
            "{name} did not move"
        );
    }
    assert!(!body.contains("_total_total"));
    let series_count = body
        .lines()
        .filter(|line| !line.starts_with('#') && !line.is_empty())
        .count();
    assert!(series_count <= 160, "scrape has {series_count} series");
    eprintln!("metrics scenario series count: {series_count}");
    let status = process.terminate(SHUTDOWN_BUDGET).await;
    assert_eq!(status.code(), Some(0), "{}", process.log());

    let recovered_address = free_loopback_address();
    let recovered_metrics_address = free_loopback_address();
    let recovered_config_path = directory.path().join("recovered-metrics.yaml");
    ServeConfig {
        primary_endpoint: &primary.endpoint(),
        hard_delete_endpoint: None,
        listen: recovered_address,
        local_directory: &directory.path().join("recovered-store"),
        lease_duration: HELD_LEASE_DURATION,
        fail_if_held: true,
        parade_db: None,
    }
    .write(&recovered_config_path);
    let mut recovered_configuration =
        fs::read_to_string(&recovered_config_path).expect("recovery configuration is readable");
    recovered_configuration.push_str(&format!(
        "metrics:\n  listen: '{recovered_metrics_address}'\n"
    ));
    fs::write(&recovered_config_path, recovered_configuration)
        .expect("recovery configuration is writable");
    let mut recovered = ServeProcess::spawn(
        &recovered_config_path,
        recovered_address,
        directory.path().join("recovered-metrics.log"),
    );
    recovered.wait_until_serving().await;
    let replay_scrape = scrape(recovered_metrics_address, "/metrics").await;
    let replay_body = replay_scrape
        .split_once("\r\n\r\n")
        .expect("recovery scrape has a body")
        .1;
    for name in [
        "dexzd_flow_closes_total",
        "dexzd_flow_end_to_end_latency_seconds_count",
        "dexzd_step_attempt_failures_total",
        "dexzd_step_retries_exhausted_total",
    ] {
        assert_eq!(sample_sum(replay_body, name), 0.0, "replay counted {name}");
    }
    let status = recovered.terminate(SHUTDOWN_BUDGET).await;
    assert_eq!(status.code(), Some(0), "{}", recovered.log());
}

async fn scrape(address: SocketAddr, path: &str) -> String {
    let mut stream = tokio::net::TcpStream::connect(address)
        .await
        .expect("metrics listener is reachable");
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .expect("request is sent");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .expect("response is received");
    String::from_utf8(response).expect("metrics text is UTF-8")
}

fn sample_sum(body: &str, name: &str) -> f64 {
    body.lines()
        .filter(|line| line.starts_with(name))
        .map(|line| {
            line.split_whitespace()
                .last()
                .expect("sample has value")
                .parse::<f64>()
                .expect("numeric sample")
        })
        .sum()
}

fn sample_with_label(body: &str, name: &str, label: &str, value: &str) -> f64 {
    let prefix = format!("{name}{{{label}=\"{value}\"}}");
    body.lines()
        .find(|line| line.starts_with(&prefix))
        .expect("labelled sample exists")
        .split_whitespace()
        .last()
        .expect("sample has value")
        .parse::<f64>()
        .expect("numeric sample")
}
