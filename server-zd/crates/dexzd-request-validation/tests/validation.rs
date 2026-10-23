// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::time::Duration;

use dexzd_protocol::dex::{
    self, AttributeMatch, AttributeWrite, ChannelCondition, ChannelMessage, CloseDecision,
    ConditionCombination, FlowConfig, FlowStartOptions, FlowTimeoutHandlerOptions,
    InvokeExecuteMethodResponse, InvokeWaitForMethodResponse, InvokeWorkerRpcResponse,
    StepDecision, StepMovement, StepOptions, SubFlowCondition, SubFlowOptions, TimerCondition,
    Value, WaitingCondition, WorkerTarget, value,
};
use dexzd_request_validation::{
    ClientRequest, ValidationCategory, ValidationConfig, ValidationError, WorkerResponse,
    validate_client_request, validate_waiting_condition, validate_worker_response,
};

fn config() -> ValidationConfig {
    let mut config = ValidationConfig::default();
    config.default_flow_config.worker_target = Some(WorkerTarget {
        is_pull_mode: true,
        ..WorkerTarget::default()
    });
    config
}

fn start_request() -> dex::StartFlowRequest {
    dex::StartFlowRequest {
        flow_id: "flow-1".into(),
        flow_type: "ExampleFlow".into(),
        start_step_type: "StartStep".into(),
        request_id: "request-1".into(),
        ..dex::StartFlowRequest::default()
    }
}

fn scalar_string(value: &str) -> Value {
    Value {
        kind: Some(value::Kind::StringValue(value.into())),
    }
}

fn internal_blob() -> Value {
    Value {
        kind: Some(value::Kind::InternalBlobIdForStringValue(
            "server-id".into(),
        )),
    }
}

fn assert_invalid(result: Result<(), ValidationError>, expected: &str) {
    let error = result.expect_err("must reject invalid input");
    assert_eq!(error.category, ValidationCategory::InvalidArgument);
    assert!(
        error.message.contains(expected),
        "expected {expected:?}, got {error:?}"
    );
    assert!(!error.field_path.is_empty());
}

fn assert_worker_invalid(
    result: Result<(), ValidationError>,
    expected: &str,
    category: ValidationCategory,
) {
    let error = result.expect_err("must reject invalid Worker response");
    assert_eq!(error.category, category);
    assert!(
        error.message.contains(expected),
        "expected {expected:?}, got {error:?}"
    );
    assert!(!error.field_path.is_empty());
}

fn assert_waiting_invalid(result: Result<(), ValidationError>, expected: &str) {
    assert_worker_invalid(
        result,
        expected,
        ValidationCategory::RetryableWorkerResponse,
    );
}

#[test]
fn start_flow_ports_go_request_checks() {
    let config = config();
    assert!(validate_client_request(ClientRequest::StartFlow(&start_request()), &config).is_ok());

    let mut cases: Vec<(&str, dex::StartFlowRequest, &str)> = Vec::new();
    let mut request = start_request();
    request.flow_id.clear();
    cases.push(("flow ID", request, "flow ID and flow type are required"));
    let mut request = start_request();
    request.flow_type.clear();
    cases.push(("flow type", request, "flow ID and flow type are required"));
    let mut request = start_request();
    request.request_id.clear();
    cases.push(("request ID", request, "request ID is required"));
    let mut request = start_request();
    request.flow_timeout_seconds = -1;
    cases.push(("timeout", request, "flow timeout must be non-negative"));
    let mut request = start_request();
    request.flow_timeout_policy = 1;
    cases.push((
        "policy without timeout",
        request,
        "flow timeout policy requires a positive timeout",
    ));
    let mut request = start_request();
    request.flow_timeout_seconds = 1;
    request.flow_timeout_policy = 99;
    cases.push(("unknown policy", request, "unknown flow timeout policy"));
    let mut request = start_request();
    request.step_input = Some(internal_blob());
    cases.push(("blob input", request, "internal_blob_id"));
    let mut request = start_request();
    request.flow_id = "invalid/id".into();
    cases.push(("slash Flow ID", request, "reserved character"));
    let mut request = start_request();
    request.start_step_type = "invalid$step".into();
    cases.push(("dollar Step type", request, "reserved character"));
    let mut request = start_request();
    request.flow_id = "é".repeat(501);
    cases.push(("UTF-8 byte limit", request, "1000 bytes"));
    let mut request = start_request();
    request.step_options = Some(StepOptions {
        heartbeat_timeout_seconds: 9,
        ..StepOptions::default()
    });
    cases.push(("heartbeat minimum", request, "heartbeat timeout"));
    let mut request = start_request();
    request.step_options = Some(StepOptions {
        wait_for_load_channel_names: vec!["events".into(), "events".into()],
        ..StepOptions::default()
    });
    cases.push(("duplicate state selection", request, "duplicated"));
    let mut request = start_request();
    request.flow_start_options = Some(FlowStartOptions {
        id_reuse_policy: 99,
        ..FlowStartOptions::default()
    });
    cases.push(("unknown reuse", request, "unknown ID reuse policy"));
    let mut request = start_request();
    request.flow_start_options = Some(FlowStartOptions {
        flow_config_override: Some(FlowConfig {
            continue_as_new_threshold: Some(-1),
            ..FlowConfig::default()
        }),
        ..FlowStartOptions::default()
    });
    cases.push(("negative config", request, "continue_as_new_threshold"));
    let mut request = start_request();
    request.flow_start_options = Some(FlowStartOptions {
        attributes: vec![
            AttributeWrite {
                key: "a".into(),
                value: Some(scalar_string("one")),
                ..AttributeWrite::default()
            },
            AttributeWrite {
                key: "a".into(),
                value: Some(scalar_string("two")),
                ..AttributeWrite::default()
            },
        ],
        ..FlowStartOptions::default()
    });
    cases.push((
        "duplicate Attribute",
        request,
        "attribute keys must be unique",
    ));
    for (name, request, expected) in cases {
        assert_invalid(
            validate_client_request(ClientRequest::StartFlow(&request), &config),
            expected,
        );
        assert!(!name.is_empty());
    }
}

#[test]
fn start_flow_rejects_server_zd_unsupported_options() {
    let config = config();
    let mut cases: Vec<dex::StartFlowRequest> = Vec::new();
    let mut request = start_request();
    request.flow_start_options = Some(FlowStartOptions {
        flow_start_delay_seconds: 1,
        ..FlowStartOptions::default()
    });
    cases.push(request);
    let mut request = start_request();
    request.flow_start_options = Some(FlowStartOptions {
        retry_policy: Some(dex::FlowRetryPolicy::default()),
        ..FlowStartOptions::default()
    });
    cases.push(request);
    let mut request = start_request();
    request.flow_start_options = Some(FlowStartOptions {
        flow_config_override: Some(FlowConfig {
            worker_target: Some(WorkerTarget {
                address: "localhost:9000".into(),
                ..WorkerTarget::default()
            }),
            ..FlowConfig::default()
        }),
        ..FlowStartOptions::default()
    });
    cases.push(request);
    let mut request = start_request();
    request.step_options = Some(StepOptions {
        wait_for_failure_policy: 2,
        ..StepOptions::default()
    });
    cases.push(request);
    let mut request = start_request();
    request.step_options = Some(StepOptions {
        execute_failure_policy: 2,
        ..StepOptions::default()
    });
    cases.push(request);
    let mut request = start_request();
    request.step_options = Some(StepOptions {
        execute_failure_proceed_step_type: "recovery".into(),
        ..StepOptions::default()
    });
    cases.push(request);
    let mut request = start_request();
    request.flow_timeout_seconds = 10;
    request.flow_timeout_policy = 3;
    request.flow_start_options = Some(FlowStartOptions {
        timeout_handler_options: Some(FlowTimeoutHandlerOptions {
            failure_proceed_step_type: "recovery".into(),
            ..FlowTimeoutHandlerOptions::default()
        }),
        ..FlowStartOptions::default()
    });
    cases.push(request);
    for request in cases {
        let error = validate_client_request(ClientRequest::StartFlow(&request), &config)
            .expect_err("unsupported option");
        assert_eq!(error.category, ValidationCategory::InvalidArgument);
        assert!(
            error.message.starts_with("server-zd does not support"),
            "{error:?}"
        );
    }
}

#[test]
fn client_request_variants_preserve_go_messages() {
    let config = config();
    let attributes = dex::SetAttributesRequest {
        flow_id: "flow".into(),
        ..dex::SetAttributesRequest::default()
    };
    assert_invalid(
        validate_client_request(ClientRequest::SetAttributes(&attributes), &config),
        "request ID is required",
    );

    let publication = dex::PublishToChannelRequest {
        flow_id: "flow".into(),
        messages: vec![ChannelMessage::default()],
        ..dex::PublishToChannelRequest::default()
    };
    assert_invalid(
        validate_client_request(ClientRequest::PublishToChannel(&publication), &config),
        "channel name is required",
    );

    let deletion = dex::DeleteChannelMessageRequest {
        flow_id: "flow".into(),
        channel_name: "channel".into(),
        message_id: "message".into(),
        ..dex::DeleteChannelMessageRequest::default()
    };
    assert_invalid(
        validate_client_request(ClientRequest::DeleteChannelMessage(&deletion), &config),
        "request ID is required",
    );

    let invocation = dex::InvokeRpcRequest {
        flow_id: "flow".into(),
        rpc_name: "getStatus".into(),
        request_id: "request".into(),
        timeout_seconds: -1,
        ..dex::InvokeRpcRequest::default()
    };
    assert_invalid(
        validate_client_request(ClientRequest::InvokeRpc(&invocation), &config),
        "RPC timeout must be non-negative",
    );

    let update = dex::UpdateFlowConfigRequest {
        flow_id: "flow".into(),
        flow_config: Some(FlowConfig {
            worker_target: Some(WorkerTarget {
                address: "host:9000".into(),
                ..WorkerTarget::default()
            }),
            ..FlowConfig::default()
        }),
        ..dex::UpdateFlowConfigRequest::default()
    };
    assert_invalid(
        validate_client_request(ClientRequest::UpdateFlowConfig(&update), &config),
        "server-zd does not support",
    );

    let skip = dex::SkipTimerRequest {
        flow_id: "flow".into(),
        step_execution_id: "step:1".into(),
        ..dex::SkipTimerRequest::default()
    };
    assert_invalid(
        validate_client_request(ClientRequest::SkipTimer(&skip), &config),
        "timer condition ID or index is required",
    );

    let reset = dex::ResetFlowRequest {
        flow_id: "flow".into(),
        reset_type: 4,
        ..dex::ResetFlowRequest::default()
    };
    assert_invalid(
        validate_client_request(ClientRequest::ResetFlow(&reset), &config),
        "step execution ID is required",
    );

    let wait_attribute = dex::WaitForAttributeRequest {
        flow_id: "flow".into(),
        r#match: Some(AttributeMatch {
            key: "status".into(),
            operator: 3,
            operand: Some(scalar_string("ready")),
        }),
        ..dex::WaitForAttributeRequest::default()
    };
    assert_invalid(
        validate_client_request(ClientRequest::WaitForAttribute(&wait_attribute), &config),
        "ordering requires an integer or double operand",
    );

    let wait_step = dex::WaitForStepCompletionRequest {
        flow_id: "flow".into(),
        step_type: "Step".into(),
        step_execution_number: "0".into(),
        ..dex::WaitForStepCompletionRequest::default()
    };
    assert_invalid(
        validate_client_request(ClientRequest::WaitForStepCompletion(&wait_step), &config),
        "positive integer",
    );
}

#[test]
fn worker_responses_reject_blob_values_and_bad_decisions() {
    let config = config();
    let bad_write = AttributeWrite {
        key: "key".into(),
        value: Some(internal_blob()),
        ..AttributeWrite::default()
    };
    let wait_for = InvokeWaitForMethodResponse {
        upsert_attributes: vec![bad_write],
        ..InvokeWaitForMethodResponse::default()
    };
    assert_worker_invalid(
        validate_worker_response(
            WorkerResponse::WaitFor {
                response: Some(&wait_for),
                parent_flow_config: &config.default_flow_config,
            },
            &config,
        ),
        "internal_blob_id",
        ValidationCategory::RetryableWorkerResponse,
    );

    let execute = InvokeExecuteMethodResponse {
        step_decision: Some(StepDecision::default()),
        ..InvokeExecuteMethodResponse::default()
    };
    assert_worker_invalid(
        validate_worker_response(WorkerResponse::Execute(Some(&execute)), &config),
        "empty step decision",
        ValidationCategory::RetryableWorkerResponse,
    );

    let rpc = InvokeWorkerRpcResponse {
        output: Some(internal_blob()),
        ..InvokeWorkerRpcResponse::default()
    };
    assert_worker_invalid(
        validate_worker_response(WorkerResponse::Rpc(Some(&rpc)), &config),
        "internal_blob_id",
        ValidationCategory::RetryableWorkerResponse,
    );
}

#[test]
fn nested_worker_step_options_are_non_retryable_for_c21() {
    let config = config();
    let nested = StepOptions {
        wait_for_failure_policy: 2,
        ..StepOptions::default()
    };
    let movement = StepMovement {
        step_type: "NextStep".into(),
        step_options: Some(StepOptions {
            execute_failure_proceed_step_options: Some(Box::new(nested)),
            ..StepOptions::default()
        }),
        ..StepMovement::default()
    };
    let decision = StepDecision {
        next_steps: vec![movement],
        ..StepDecision::default()
    };
    let execute = InvokeExecuteMethodResponse {
        step_decision: Some(decision.clone()),
        ..InvokeExecuteMethodResponse::default()
    };
    assert_worker_invalid(
        validate_worker_response(WorkerResponse::Execute(Some(&execute)), &config),
        "server-zd does not support",
        ValidationCategory::NonRetryableWorkerResponse,
    );
    let rpc = InvokeWorkerRpcResponse {
        step_decision: Some(decision),
        ..InvokeWorkerRpcResponse::default()
    };
    assert_worker_invalid(
        validate_worker_response(WorkerResponse::Rpc(Some(&rpc)), &config),
        "server-zd does not support",
        ValidationCategory::NonRetryableWorkerResponse,
    );
}

#[test]
fn waiting_conditions_port_go_cases_and_subflow_rejections() {
    let config = config();
    let mut waiting = WaitingCondition {
        waiting_condition_type: 1,
        timer_conditions: vec![TimerCondition {
            duration_seconds: -1,
            ..TimerCondition::default()
        }],
        ..WaitingCondition::default()
    };
    assert_waiting_invalid(
        validate_waiting_condition(Some(&waiting), &config.default_flow_config, &config),
        "negative duration_seconds",
    );
    waiting.timer_conditions.clear();
    waiting.channel_conditions.push(ChannelCondition {
        channel_name: "events".into(),
        at_least: Some(2),
        at_most: Some(1),
        ..ChannelCondition::default()
    });
    assert_waiting_invalid(
        validate_waiting_condition(Some(&waiting), &config.default_flow_config, &config),
        "at_most 1 < at_least 2",
    );
    waiting.channel_conditions.clear();
    waiting.waiting_condition_type = 3;
    waiting.condition_combinations.push(ConditionCombination {
        condition_ids: vec!["missing".into()],
    });
    assert_waiting_invalid(
        validate_waiting_condition(Some(&waiting), &config.default_flow_config, &config),
        "undeclared condition_id",
    );

    let subflow = SubFlowCondition {
        sub_flow_type: "ChildFlow".into(),
        start_step_type: "StartStep".into(),
        options: Some(SubFlowOptions {
            flow_start_delay_seconds: 1,
            ..SubFlowOptions::default()
        }),
        ..SubFlowCondition::default()
    };
    let waiting = WaitingCondition {
        waiting_condition_type: 1,
        sub_flow_conditions: vec![subflow],
        ..WaitingCondition::default()
    };
    let error = validate_waiting_condition(Some(&waiting), &config.default_flow_config, &config)
        .expect_err("delay unsupported");
    assert_eq!(error.category, ValidationCategory::RetryableWorkerResponse);
    assert!(error.message.starts_with("server-zd does not support"));

    let subflow = SubFlowCondition {
        sub_flow_type: "ChildFlow".into(),
        start_step_type: "StartStep".into(),
        options: Some(SubFlowOptions {
            retry_policy: Some(dex::FlowRetryPolicy::default()),
            ..SubFlowOptions::default()
        }),
        ..SubFlowCondition::default()
    };
    let waiting = WaitingCondition {
        waiting_condition_type: 1,
        sub_flow_conditions: vec![subflow],
        ..WaitingCondition::default()
    };
    let error = validate_worker_response(
        WorkerResponse::WaitFor {
            response: Some(&InvokeWaitForMethodResponse {
                waiting_condition: Some(waiting),
                ..InvokeWaitForMethodResponse::default()
            }),
            parent_flow_config: &config.default_flow_config,
        },
        &config,
    )
    .expect_err("retry unsupported");
    assert_eq!(error.category, ValidationCategory::RetryableWorkerResponse);
    assert!(error.message.starts_with("server-zd does not support"));
}

#[test]
fn close_decision_and_timeout_handler_rules() {
    let config = config();
    let execute = InvokeExecuteMethodResponse {
        step_decision: Some(StepDecision {
            close_decision: Some(CloseDecision {
                close_decision_type: 4,
                close_input: Some(Value {
                    kind: Some(value::Kind::IntValue(1)),
                }),
                ..CloseDecision::default()
            }),
            ..StepDecision::default()
        }),
        ..InvokeExecuteMethodResponse::default()
    };
    assert_worker_invalid(
        validate_worker_response(WorkerResponse::Execute(Some(&execute)), &config),
        "force fail close input must be a string",
        ValidationCategory::RetryableWorkerResponse,
    );

    let mut request = start_request();
    request.flow_timeout_seconds = 10;
    request.flow_timeout_policy = 3;
    request.flow_start_options = Some(FlowStartOptions {
        timeout_handler_options: Some(FlowTimeoutHandlerOptions {
            failure_policy: 2,
            ..FlowTimeoutHandlerOptions::default()
        }),
        ..FlowStartOptions::default()
    });
    assert_invalid(
        validate_client_request(ClientRequest::StartFlow(&request), &config),
        "server-zd does not support",
    );
}

#[test]
fn attributes_targets_and_stores_reject_invalid_client_inputs() {
    let config = config();
    let mut request = start_request();
    request.flow_start_options = Some(FlowStartOptions {
        attributes: vec![AttributeWrite {
            key: "empty".into(),
            ..AttributeWrite::default()
        }],
        ..FlowStartOptions::default()
    });
    assert_invalid(
        validate_client_request(ClientRequest::StartFlow(&request), &config),
        "attribute 0 is invalid",
    );

    let mut request = start_request();
    request.flow_start_options = Some(FlowStartOptions {
        attributes: vec![AttributeWrite {
            key: "secret".into(),
            value: Some(scalar_string("value")),
            is_hard_delete_on_null: true,
            ..AttributeWrite::default()
        }],
        ..FlowStartOptions::default()
    });
    assert_invalid(
        validate_client_request(ClientRequest::StartFlow(&request), &config),
        "hard-delete Attributes require an enabled blob store",
    );

    let mut request = start_request();
    request.flow_start_options = Some(FlowStartOptions {
        flow_config_override: Some(FlowConfig {
            attribute_store_names: Some(dex::AttributeStoreNames {
                names: vec!["missing".into()],
            }),
            ..FlowConfig::default()
        }),
        ..FlowStartOptions::default()
    });
    assert_invalid(
        validate_client_request(ClientRequest::StartFlow(&request), &config),
        "unknown Attribute Store",
    );

    let mut no_target = config.clone();
    no_target.default_flow_config.worker_target = None;
    assert_invalid(
        validate_client_request(ClientRequest::StartFlow(&start_request()), &no_target),
        "worker_target is required",
    );

    let write = dex::SetAttributesRequest {
        flow_id: "flow".into(),
        request_id: "request".into(),
        attributes: vec![AttributeWrite {
            key: "DexWorkQueuePermissions".into(),
            value: Some(scalar_string("value")),
            ..AttributeWrite::default()
        }],
        ..dex::SetAttributesRequest::default()
    };
    assert_invalid(
        validate_client_request(ClientRequest::SetAttributes(&write), &config),
        "managed by the Server",
    );
}

#[test]
fn worker_blob_and_channel_validation_covers_all_written_values() {
    let config = config();
    let cases = vec![
        InvokeWaitForMethodResponse {
            record_events: vec![dex::Kv {
                key: "event".into(),
                value: Some(internal_blob()),
            }],
            ..InvokeWaitForMethodResponse::default()
        },
        InvokeWaitForMethodResponse {
            upsert_step_exe_locals: vec![dex::Kv {
                key: "local".into(),
                value: Some(internal_blob()),
            }],
            ..InvokeWaitForMethodResponse::default()
        },
        InvokeWaitForMethodResponse {
            publish_to_channel: vec![ChannelMessage {
                channel_name: "messages".into(),
                value: Some(internal_blob()),
                ..ChannelMessage::default()
            }],
            ..InvokeWaitForMethodResponse::default()
        },
    ];
    for response in cases {
        assert_worker_invalid(
            validate_worker_response(
                WorkerResponse::WaitFor {
                    response: Some(&response),
                    parent_flow_config: &config.default_flow_config,
                },
                &config,
            ),
            "internal_blob_id",
            ValidationCategory::RetryableWorkerResponse,
        );
    }
    let response = InvokeWaitForMethodResponse {
        publish_to_channel: vec![ChannelMessage::default()],
        ..InvokeWaitForMethodResponse::default()
    };
    assert_worker_invalid(
        validate_worker_response(
            WorkerResponse::WaitFor {
                response: Some(&response),
                parent_flow_config: &config.default_flow_config,
            },
            &config,
        ),
        "Channel publication at index 0 is invalid",
        ValidationCategory::RetryableWorkerResponse,
    );
    let response = InvokeWaitForMethodResponse {
        delete_from_channel: vec![dex::ChannelMessageDeletion::default()],
        ..InvokeWaitForMethodResponse::default()
    };
    assert_worker_invalid(
        validate_worker_response(
            WorkerResponse::WaitFor {
                response: Some(&response),
                parent_flow_config: &config.default_flow_config,
            },
            &config,
        ),
        "Channel deletion at index 0 is invalid",
        ValidationCategory::RetryableWorkerResponse,
    );
}

#[test]
fn subflow_target_and_nested_failure_policy_are_rejected() {
    let config = config();
    let options = SubFlowOptions {
        flow_config_override: Some(FlowConfig {
            worker_target: Some(WorkerTarget {
                address: "host:9000".into(),
                ..WorkerTarget::default()
            }),
            ..FlowConfig::default()
        }),
        ..SubFlowOptions::default()
    };
    let condition = SubFlowCondition {
        sub_flow_type: "ChildFlow".into(),
        start_step_type: "StartStep".into(),
        options: Some(options),
        ..SubFlowCondition::default()
    };
    let waiting = WaitingCondition {
        waiting_condition_type: 1,
        sub_flow_conditions: vec![condition],
        ..WaitingCondition::default()
    };
    let response = InvokeWaitForMethodResponse {
        waiting_condition: Some(waiting),
        ..InvokeWaitForMethodResponse::default()
    };
    assert_worker_invalid(
        validate_worker_response(
            WorkerResponse::WaitFor {
                response: Some(&response),
                parent_flow_config: &config.default_flow_config,
            },
            &config,
        ),
        "server-zd does not support Worker target addresses",
        ValidationCategory::RetryableWorkerResponse,
    );

    let condition = SubFlowCondition {
        sub_flow_type: "ChildFlow".into(),
        start_step_type: "StartStep".into(),
        step_options: Some(StepOptions {
            execute_failure_policy: 2,
            ..StepOptions::default()
        }),
        ..SubFlowCondition::default()
    };
    let waiting = WaitingCondition {
        waiting_condition_type: 1,
        sub_flow_conditions: vec![condition],
        ..WaitingCondition::default()
    };
    let response = InvokeWaitForMethodResponse {
        waiting_condition: Some(waiting),
        ..InvokeWaitForMethodResponse::default()
    };
    assert_worker_invalid(
        validate_worker_response(
            WorkerResponse::WaitFor {
                response: Some(&response),
                parent_flow_config: &config.default_flow_config,
            },
            &config,
        ),
        "server-zd does not support",
        ValidationCategory::NonRetryableWorkerResponse,
    );
}

#[test]
fn step_options_port_go_heartbeat_and_state_selection_table() {
    let config = config();
    for heartbeat in [0, 10, 60] {
        let mut request = start_request();
        request.step_options = Some(StepOptions {
            heartbeat_timeout_seconds: heartbeat,
            ..StepOptions::default()
        });
        assert!(validate_client_request(ClientRequest::StartFlow(&request), &config).is_ok());
    }
    for heartbeat in [-1, 1, 9] {
        let mut request = start_request();
        request.step_options = Some(StepOptions {
            heartbeat_timeout_seconds: heartbeat,
            ..StepOptions::default()
        });
        assert_invalid(
            validate_client_request(ClientRequest::StartFlow(&request), &config),
            "heartbeat timeout",
        );
    }
    let mut request = start_request();
    request.step_options = Some(StepOptions {
        wait_for_load_attribute_map_instances: vec!["items/".into(), "items/tenant-a".into()],
        wait_for_load_channel_names: vec!["commands".into()],
        wait_for_load_channel_map_instances: vec!["commands-by-tenant/".into()],
        execute_load_attribute_map_instances: vec!["items/tenant-b".into()],
        execute_load_channel_names: vec!["events".into()],
        execute_load_channel_map_instances: vec!["events-by-tenant/tenant-b".into()],
        ..StepOptions::default()
    });
    assert!(validate_client_request(ClientRequest::StartFlow(&request), &config).is_ok());
}

#[test]
fn flow_config_and_wait_match_port_go_invalid_value_table() {
    let config = config();
    let invalid_configs = [
        (
            FlowConfig {
                continue_as_new_page_size_in_bytes: Some(-1),
                ..FlowConfig::default()
            },
            "continue_as_new_page_size_in_bytes",
        ),
        (
            FlowConfig {
                step_durability: Some(99),
                ..FlowConfig::default()
            },
            "unknown step_durability",
        ),
        (
            FlowConfig {
                active_step_search_mode: Some(99),
                ..FlowConfig::default()
            },
            "unknown active_step_search_mode",
        ),
        (
            FlowConfig {
                attribute_store_names: Some(dex::AttributeStoreNames {
                    names: vec![String::new()],
                }),
                ..FlowConfig::default()
            },
            "Attribute Store name must not be empty",
        ),
    ];
    for (flow_config, expected) in invalid_configs {
        let request = dex::UpdateFlowConfigRequest {
            flow_id: "flow".into(),
            flow_config: Some(flow_config),
            ..dex::UpdateFlowConfigRequest::default()
        };
        assert_invalid(
            validate_client_request(ClientRequest::UpdateFlowConfig(&request), &config),
            expected,
        );
    }
    let operands = [
        (0, scalar_string("value"), "operator is invalid"),
        (
            1,
            Value {
                kind: Some(value::Kind::DoubleValue(f64::NAN)),
            },
            "double operand must be finite",
        ),
        (
            1,
            Value {
                kind: Some(value::Kind::ObjValue(dex::EncodedObject::default())),
            },
            "supports only string",
        ),
    ];
    for (operator, operand, expected) in operands {
        let request = dex::WaitForAttributeRequest {
            flow_id: "flow".into(),
            r#match: Some(AttributeMatch {
                key: "status".into(),
                operator,
                operand: Some(operand),
            }),
            ..dex::WaitForAttributeRequest::default()
        };
        assert_invalid(
            validate_client_request(ClientRequest::WaitForAttribute(&request), &config),
            expected,
        );
    }
}

#[test]
fn waiting_condition_ids_and_server_owned_timer_state_are_rejected() {
    let config = config();
    let conditions = [
        (
            WaitingCondition {
                waiting_condition_type: 1,
                timer_conditions: vec![TimerCondition {
                    firing_unix_timestamp_seconds: 1,
                    ..TimerCondition::default()
                }],
                ..WaitingCondition::default()
            },
            "server-owned firing_unix_timestamp_seconds",
        ),
        (
            WaitingCondition {
                waiting_condition_type: 1,
                timer_conditions: vec![
                    TimerCondition {
                        condition_id: "same".into(),
                        ..TimerCondition::default()
                    },
                    TimerCondition {
                        condition_id: "same".into(),
                        ..TimerCondition::default()
                    },
                ],
                ..WaitingCondition::default()
            },
            "duplicate condition_id",
        ),
        (
            WaitingCondition {
                waiting_condition_type: 3,
                timer_conditions: vec![TimerCondition::default()],
                condition_combinations: vec![ConditionCombination {
                    condition_ids: vec!["id".into()],
                }],
                ..WaitingCondition::default()
            },
            "empty condition_id",
        ),
        (
            WaitingCondition {
                waiting_condition_type: 3,
                ..WaitingCondition::default()
            },
            "requires at least one condition_combination",
        ),
    ];
    for (waiting, expected) in conditions {
        assert_waiting_invalid(
            validate_waiting_condition(Some(&waiting), &config.default_flow_config, &config),
            expected,
        );
    }
}

#[test]
fn subflow_inherits_parent_effective_flow_config() {
    let mut config = ValidationConfig::default();
    config.attribute_store_names.insert("parent-store".into());
    let parent_flow_config = FlowConfig {
        worker_target: Some(WorkerTarget {
            is_pull_mode: true,
            ..WorkerTarget::default()
        }),
        attribute_store_names: Some(dex::AttributeStoreNames {
            names: vec!["parent-store".into()],
        }),
        ..FlowConfig::default()
    };
    let waiting = WaitingCondition {
        waiting_condition_type: dex::WaitingConditionType::AllCompleted as i32,
        sub_flow_conditions: vec![SubFlowCondition {
            sub_flow_type: "ChildFlow".into(),
            start_step_type: "StartStep".into(),
            ..SubFlowCondition::default()
        }],
        ..WaitingCondition::default()
    };
    assert!(validate_waiting_condition(Some(&waiting), &parent_flow_config, &config).is_ok());
    let response = InvokeWaitForMethodResponse {
        waiting_condition: Some(waiting),
        ..InvokeWaitForMethodResponse::default()
    };
    assert!(
        validate_worker_response(
            WorkerResponse::WaitFor {
                response: Some(&response),
                parent_flow_config: &parent_flow_config,
            },
            &config,
        )
        .is_ok()
    );
}

#[test]
fn default_heartbeat_minimum_matches_go_server() {
    let config = ValidationConfig::default();
    assert_eq!(
        config.minimum_step_heartbeat_timeout,
        Duration::from_secs(10)
    );
    let response = InvokeExecuteMethodResponse {
        step_decision: Some(StepDecision {
            next_steps: vec![StepMovement {
                step_type: "NextStep".into(),
                step_options: Some(StepOptions {
                    heartbeat_timeout_seconds: 1,
                    ..StepOptions::default()
                }),
                ..StepMovement::default()
            }],
            ..StepDecision::default()
        }),
        ..InvokeExecuteMethodResponse::default()
    };
    assert_worker_invalid(
        validate_worker_response(WorkerResponse::Execute(Some(&response)), &config),
        "heartbeat timeout must be zero or at least 10s",
        ValidationCategory::RetryableWorkerResponse,
    );

    let mut zero_minimum = config;
    zero_minimum.minimum_step_heartbeat_timeout = Duration::ZERO;
    let mut request = start_request();
    request.step_options = Some(StepOptions {
        heartbeat_timeout_seconds: -1,
        ..StepOptions::default()
    });
    let mut request_config = zero_minimum;
    request_config.default_flow_config.worker_target = Some(WorkerTarget {
        is_pull_mode: true,
        ..WorkerTarget::default()
    });
    assert_invalid(
        validate_client_request(ClientRequest::StartFlow(&request), &request_config),
        "heartbeat timeout must be zero or at least 0s",
    );
}

#[test]
fn timeout_handler_failure_target_requires_proceed_policy() {
    let config = config();
    for failure_policy in [
        dex::ExecuteMethodFailurePolicy::Unspecified as i32,
        dex::ExecuteMethodFailurePolicy::FailFlowOnExecuteMethodFailure as i32,
    ] {
        let handler = FlowTimeoutHandlerOptions {
            failure_policy,
            failure_proceed_step_options: Some(StepOptions::default()),
            ..FlowTimeoutHandlerOptions::default()
        };
        let mut request = start_request();
        request.flow_timeout_seconds = 10;
        request.flow_timeout_policy = dex::FlowTimeoutPolicy::Handler as i32;
        request.flow_start_options = Some(FlowStartOptions {
            timeout_handler_options: Some(handler.clone()),
            ..FlowStartOptions::default()
        });
        assert_invalid(
            validate_client_request(ClientRequest::StartFlow(&request), &config),
            "Flow timeout handler failure target requires the proceed policy",
        );

        let waiting = WaitingCondition {
            waiting_condition_type: dex::WaitingConditionType::AllCompleted as i32,
            sub_flow_conditions: vec![SubFlowCondition {
                sub_flow_type: "ChildFlow".into(),
                start_step_type: "StartStep".into(),
                options: Some(SubFlowOptions {
                    flow_timeout_seconds: 10,
                    flow_timeout_policy: dex::FlowTimeoutPolicy::Handler as i32,
                    timeout_handler_options: Some(handler),
                    ..SubFlowOptions::default()
                }),
                ..SubFlowCondition::default()
            }],
            ..WaitingCondition::default()
        };
        assert_waiting_invalid(
            validate_waiting_condition(Some(&waiting), &config.default_flow_config, &config),
            "Flow timeout handler failure target requires the proceed policy",
        );
    }
}

#[test]
fn unsupported_step_type_takes_priority_over_reserved_characters() {
    let config = config();
    let response = InvokeExecuteMethodResponse {
        step_decision: Some(StepDecision {
            next_steps: vec![StepMovement {
                step_type: "NextStep".into(),
                step_options: Some(StepOptions {
                    execute_failure_proceed_step_type: "a/b".into(),
                    ..StepOptions::default()
                }),
                ..StepMovement::default()
            }],
            ..StepDecision::default()
        }),
        ..InvokeExecuteMethodResponse::default()
    };
    assert_worker_invalid(
        validate_worker_response(WorkerResponse::Execute(Some(&response)), &config),
        "server-zd does not support execute_failure_proceed_step_type",
        ValidationCategory::NonRetryableWorkerResponse,
    );
}
