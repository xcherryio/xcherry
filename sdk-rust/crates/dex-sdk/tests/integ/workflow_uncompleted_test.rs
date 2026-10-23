// Portions of this file are derived from indeedeng/iwf-java-sdk.
// Those portions are licensed under the Apache License, Version 2.0.
// See LICENSES/Apache-2.0.txt and LEGACY_NOTICES.md.
//
// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications are licensed under the Sustainable Use License 1.0.
// Third-Party Materials remain under the Apache License, Version 2.0.
// See LICENSE and LEGACY_NOTICES.md.

use std::sync::LazyLock;
use std::time::Duration;

use dex_sdk::{
    Client, Context, Flow, FlowErrorType, FlowStatus, FlowTimeoutHandler, FlowTimeoutPolicy,
    HandlerResult, PersistenceSchema, Registry, SdkError, SdkResult, StartFlowOptions,
    StepDecision, StopFlowOptions, Stream,
};

use crate::signal_workflow::SignalWorkflow;
use crate::support::{DexDevTestEnvironment, flow_id};
use crate::workflow_uncompleted_empty_decision_workflow::WorkflowUncompletedEmptyDecisionWorkflow;
use crate::workflow_uncompleted_force_fail_workflow::WorkflowUncompletedForceFailWorkflow;
use crate::workflow_uncompleted_state_failure_workflow::WorkflowUncompletedStateFailureWorkflow;
use crate::workflow_uncompleted_state_timeout_workflow::WorkflowUncompletedStateTimeoutWorkflow;

#[test]
#[ignore = "requires dexcli dev"]
fn test_flow_wait_timeout() {
    let environment = DexDevTestEnvironment::start(Registry::new().register(SignalWorkflow::new()));
    let workflow = SignalWorkflow::new();
    let flow_id = flow_id("wait-timeout");
    environment
        .client
        .start_flow(&workflow, &flow_id, 1)
        .expect("start waiting Flow");
    match environment
        .client
        .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(1))
        .and_then(|result| result.single_output::<i32>())
        .expect_err("waitForFlow must time out")
    {
        SdkError::LongPollTimeout { service } => {
            assert_eq!(Some(flow_id.as_str()), service.flow_id())
        }
        error => panic!("expected LongPollTimeout, got {error:?}"),
    }
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_flow_timeout() {
    let environment = DexDevTestEnvironment::start(Registry::new().register(SignalWorkflow::new()));
    let workflow = SignalWorkflow::new();
    let flow_id = flow_id("flow-timeout");
    environment
        .client
        .start_flow_with_options(
            &workflow,
            &flow_id,
            1,
            StartFlowOptions::new().timeout(Duration::from_secs(1)),
        )
        .expect("start timed Flow");
    assert_failure(
        wait_for_failure(&environment, &flow_id),
        FlowStatus::Failed,
        Some(FlowErrorType::FlowTimeout),
        Some("Flow timed out after 1 seconds"),
        0,
    );
}

struct TimeoutHandlerFlow;

static TIMEOUT_PROGRESS: LazyLock<Stream<String>> =
    LazyLock::new(|| Stream::new("timeout-progress", 1 << 20));

impl TimeoutHandlerFlow {
    fn handle_timeout(&self, context: &mut Context) -> HandlerResult<StepDecision> {
        context.record_heartbeat()?;
        TIMEOUT_PROGRESS.write(context, "valid".to_string())?;
        Ok(StepDecision::force_complete("expired".to_string()))
    }
}

impl Flow for TimeoutHandlerFlow {
    type StartInput = ();

    fn persistence(&self) -> PersistenceSchema {
        PersistenceSchema::new().stream(&TIMEOUT_PROGRESS)
    }

    fn timeout_handler(&self) -> Option<FlowTimeoutHandler<Self>> {
        Some(Self::handle_timeout)
    }
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_flow_timeout_handler() {
    let environment = DexDevTestEnvironment::start(Registry::new().register(TimeoutHandlerFlow));
    let flow_id = flow_id("flow-timeout-handler");
    environment
        .client
        .start_flow_with_options(
            &TimeoutHandlerFlow,
            &flow_id,
            (),
            StartFlowOptions::new().timeout(Duration::from_secs(1)),
        )
        .expect("start timeout-handler Flow");
    let output: String = environment
        .client
        .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(15))
        .and_then(|result| result.single_output())
        .expect("timeout handler completes Flow");
    assert_eq!("expired", output);
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_flow_timeout_handler_cancel_override() {
    let environment = DexDevTestEnvironment::start(Registry::new().register(TimeoutHandlerFlow));
    let flow_id = flow_id("flow-timeout-handler-cancel");
    environment
        .client
        .start_flow_with_options(
            &TimeoutHandlerFlow,
            &flow_id,
            (),
            StartFlowOptions::new()
                .timeout(Duration::from_secs(1))
                .timeout_policy(FlowTimeoutPolicy::Cancel),
        )
        .expect("start canceled timeout Flow");
    assert_failure(
        wait_for_failure(&environment, &flow_id),
        FlowStatus::Canceled,
        None,
        None,
        0,
    );
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_flow_timeout_handler_requires_registration() {
    let environment = DexDevTestEnvironment::start(Registry::new().register(SignalWorkflow::new()));
    let workflow = SignalWorkflow::new();
    let error = environment
        .client
        .start_flow_with_options(
            &workflow,
            &flow_id("flow-timeout-policy-without-timeout"),
            1,
            StartFlowOptions::new().timeout_policy(FlowTimeoutPolicy::Cancel),
        )
        .expect_err("non-default policy must require a positive timeout");
    assert!(matches!(
        error,
        SdkError::InvalidArgument { message } if message.contains("requires a positive timeout")
    ));

    let error = environment
        .client
        .start_flow_with_options(
            &workflow,
            &flow_id("flow-timeout-handler-missing"),
            1,
            StartFlowOptions::new()
                .timeout(Duration::from_secs(1))
                .timeout_policy(FlowTimeoutPolicy::Handler),
        )
        .expect_err("handler policy must require a registered handler");
    assert!(matches!(
        error,
        SdkError::InvalidArgument { message } if message.contains("has no timeout handler")
    ));
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_flow_canceled() {
    assert_stopped_flow(StopFlowOptions::cancel(), FlowStatus::Canceled, None, None);
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_flow_terminated() {
    assert_stopped_flow(
        StopFlowOptions::terminate().reason("terminated"),
        FlowStatus::Terminated,
        None,
        None,
    );
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_flow_failed_by_api() {
    assert_stopped_flow(
        StopFlowOptions::fail().reason("fail by API"),
        FlowStatus::Failed,
        Some(FlowErrorType::ClientApiFailed),
        Some("fail by API"),
    );
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_force_fail_flow() {
    let environment = DexDevTestEnvironment::start(
        Registry::new().register(WorkflowUncompletedForceFailWorkflow::new()),
    );
    let workflow = WorkflowUncompletedForceFailWorkflow::new();
    let flow_id = flow_id("force-fail");
    environment
        .client
        .start_flow(&workflow, &flow_id, 5)
        .expect("start force-fail Flow");
    assert_failure(
        wait_for_failure(&environment, &flow_id),
        FlowStatus::Failed,
        Some(FlowErrorType::StepDecisionFailed),
        Some("a failing message"),
        0,
    );
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_worker_api_failure() {
    let environment = DexDevTestEnvironment::start(
        Registry::new().register(WorkflowUncompletedStateFailureWorkflow::new()),
    );
    let workflow = WorkflowUncompletedStateFailureWorkflow::new();
    let flow_id = flow_id("worker-api-failure");
    environment
        .client
        .start_flow(&workflow, &flow_id, 5)
        .expect("start worker-failure Flow");
    let result = wait_for_failure(&environment, &flow_id);
    assert_eq!(FlowStatus::Failed, result.status());
    assert_eq!(Some(FlowErrorType::WorkerApiFailed), result.error_type());
    assert!(
        result
            .error_message()
            .is_some_and(|value| value.contains("test api failing"))
    );
    assert_eq!(0, result.completions().len());
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_worker_api_timeout() {
    let environment = DexDevTestEnvironment::start(
        Registry::new().register(WorkflowUncompletedStateTimeoutWorkflow::new()),
    );
    let workflow = WorkflowUncompletedStateTimeoutWorkflow::new();
    let flow_id = flow_id("worker-api-timeout");
    environment
        .client
        .start_flow(&workflow, &flow_id, 5)
        .expect("start worker-timeout Flow");
    let result = wait_for_failure(&environment, &flow_id);
    assert_eq!(FlowStatus::Failed, result.status());
    assert_eq!(Some(FlowErrorType::WorkerApiFailed), result.error_type());
    assert!(
        result
            .error_message()
            .is_some_and(|value| value.to_lowercase().contains("timeout"))
    );
    assert_eq!(0, result.completions().len());
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_empty_decision_fails_flow() {
    let environment = DexDevTestEnvironment::start(
        Registry::new().register(WorkflowUncompletedEmptyDecisionWorkflow::new()),
    );
    let workflow = WorkflowUncompletedEmptyDecisionWorkflow::new();
    let flow_id = flow_id("empty-decision");
    environment
        .client
        .start_flow(&workflow, &flow_id, 5)
        .expect("start empty-decision Flow");
    let result = wait_for_failure(&environment, &flow_id);
    assert_eq!(FlowStatus::Failed, result.status());
    assert_eq!(Some(FlowErrorType::WorkerApiFailed), result.error_type());
    assert!(
        result
            .error_message()
            .is_some_and(|value| value.contains("go_to_many requires a movement"))
    );
    assert_eq!(0, result.completions().len());
}

fn assert_stopped_flow(
    options: StopFlowOptions,
    expected_status: FlowStatus,
    expected_error_type: Option<FlowErrorType>,
    expected_message: Option<&str>,
) {
    let environment = DexDevTestEnvironment::start(Registry::new().register(SignalWorkflow::new()));
    let workflow = SignalWorkflow::new();
    let flow_id = flow_id("stopped");
    environment
        .client
        .start_flow(&workflow, &flow_id, 1)
        .expect("start stoppable Flow");
    environment
        .client
        .stop_flow(&flow_id, options)
        .expect("stop Flow");
    assert_failure(
        wait_for_failure(&environment, &flow_id),
        expected_status,
        expected_error_type,
        expected_message,
        0,
    );
}

fn wait_for_failure(environment: &DexDevTestEnvironment, flow_id: &str) -> dex_sdk::FlowResult {
    environment
        .client
        .wait_for_flow_with_timeout(flow_id, Duration::from_secs(15))
        .expect("wait for terminal Flow result")
}

fn assert_failure(
    failure: dex_sdk::FlowResult,
    expected_status: FlowStatus,
    expected_error_type: Option<FlowErrorType>,
    expected_message: Option<&str>,
    expected_result_count: usize,
) {
    assert_eq!(expected_status, failure.status());
    assert_eq!(expected_error_type, failure.error_type());
    assert_eq!(expected_message, failure.error_message());
    assert_eq!(expected_result_count, failure.completions().len());
}

#[expect(
    dead_code,
    reason = "Integration fixtures implement schema variants exercised by selected test targets."
)]
fn compile_workflow_uncompleted(client: &Client) -> SdkResult<()> {
    client.start_flow(
        &WorkflowUncompletedForceFailWorkflow::new(),
        "force-fail",
        5,
    )?;
    client.start_flow(
        &WorkflowUncompletedStateFailureWorkflow::new(),
        "worker-failure",
        5,
    )?;
    client.start_flow(
        &WorkflowUncompletedStateTimeoutWorkflow::new(),
        "worker-timeout",
        5,
    )?;
    client.start_flow(
        &WorkflowUncompletedEmptyDecisionWorkflow::new(),
        "empty-decision",
        5,
    )?;
    Ok(())
}
