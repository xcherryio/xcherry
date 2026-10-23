// Portions of this file are derived from indeedeng/iwf-java-sdk.
// Those portions are licensed under the Apache License, Version 2.0.
// See LICENSES/Apache-2.0.txt and LEGACY_NOTICES.md.
//
// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications are licensed under the Sustainable Use License 1.0.
// Third-Party Materials remain under the Apache License, Version 2.0.
// See LICENSE and LEGACY_NOTICES.md.

use std::time::Duration;

use dex_sdk::{
    ActiveStepSearchMode, Client, Flow, FlowConfig, FlowErrorType, FlowStatus, GrpcCode,
    IdReusePolicy, Registry, SdkError, SdkResult, StartFlowOptions, StepExecutionId, StepList,
    WorkerTarget,
};

use crate::basic_abnormal_exit_workflow::BasicAbnormalExitWorkflow;
use crate::basic_empty_input_workflow::BasicEmptyInputWorkflow;
use crate::basic_immutable_step_options_workflow::BasicImmutableStepOptionsWorkflow;
use crate::basic_model_input_workflow::{BasicModelInput, BasicModelInputWorkflow};
use crate::basic_proceed_on_wait_failure_workflow::BasicProceedOnWaitFailureWorkflow;
use crate::basic_workflow::BasicWorkflow;
use crate::signal_workflow::SignalWorkflow;
use crate::skip_wait_until_mixed_wait_workflow::SkipWaitUntilMixedWaitWorkflow;
use crate::support::{DexDevTestEnvironment, flow_id};

#[test]
#[ignore = "requires dexcli dev"]
fn test_basic_workflow() {
    let environment = DexDevTestEnvironment::start(Registry::new().register(BasicWorkflow::new()));
    let workflow = BasicWorkflow::new();
    let flow_id = flow_id("basic");
    let options = StartFlowOptions::new().id_reuse_policy(IdReusePolicy::Disallow);
    environment
        .client
        .start_flow_with_options(&workflow, &flow_id, 0, options.clone())
        .expect("start basic Flow");
    assert_eq!(
        2,
        environment
            .client
            .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
            .and_then(|result| result.single_output::<i32>())
            .expect("complete basic Flow")
    );
    match environment
        .client
        .start_flow_with_options(&workflow, &flow_id, 0, options)
        .expect_err("duplicate Flow ID must fail")
    {
        SdkError::FlowAlreadyStarted { service } => {
            assert_eq!(GrpcCode::AlreadyExists, service.code());
        }
        error => panic!("expected FlowAlreadyStarted, got {error:?}"),
    }
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_basic_workflow_abnormal_exit_reuse() {
    let environment = DexDevTestEnvironment::start(
        Registry::new()
            .register(BasicAbnormalExitWorkflow::new())
            .expect("register abnormal-exit Flow")
            .register(BasicWorkflow::new()),
    );
    let abnormal = BasicAbnormalExitWorkflow::new();
    let workflow = BasicWorkflow::new();
    let flow_id = flow_id("abnormal-exit-reuse");
    let options = StartFlowOptions::new().id_reuse_policy(IdReusePolicy::AllowIfPreviousFailed);
    environment
        .client
        .start_flow_with_options(&abnormal, &flow_id, 0, options.clone())
        .expect("start abnormal-exit Flow");
    let failed = environment
        .client
        .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
        .expect("wait for failed Flow result");
    assert_eq!(FlowStatus::Failed, failed.status());
    environment
        .client
        .start_flow_with_options(&workflow, &flow_id, 0, options)
        .expect("reuse failed Flow ID");
    assert_eq!(
        2,
        environment
            .client
            .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
            .and_then(|result| result.single_output::<i32>())
            .expect("complete reused Flow ID")
    );
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_empty_input_workflow() {
    let environment =
        DexDevTestEnvironment::start(Registry::new().register(BasicEmptyInputWorkflow::new()));
    let workflow = BasicEmptyInputWorkflow::new();
    let missing_flow_id = flow_id("missing");
    let flow_id = flow_id("empty-input");
    environment
        .client
        .start_flow(&workflow, &flow_id, ())
        .expect("start empty-input Flow");
    environment
        .client
        .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
        .expect("complete empty-input Flow");
    match environment
        .client
        .wait_for_flow_with_timeout(&missing_flow_id, Duration::from_secs(1))
        .expect_err("missing Flow must fail")
    {
        SdkError::FlowNotFound { service } => {
            assert_eq!(GrpcCode::NotFound, service.code())
        }
        error => panic!("expected FlowNotFound, got {error:?}"),
    }
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_type_specified_workflow() {
    struct UnregisteredFlow;

    impl Flow for UnregisteredFlow {
        type StartInput = ();

        fn flow_type(&self) -> &'static str {
            "test-customized-flow-type"
        }

        fn steps(&self) -> StepList<'_, Self::StartInput> {
            StepList::empty()
        }
    }

    let environment =
        DexDevTestEnvironment::start(Registry::new().register(BasicEmptyInputWorkflow::new()));
    let workflow = BasicEmptyInputWorkflow::new();
    assert_eq!("test-customized-flow-type", workflow.flow_type());
    let unregistered_flow_id = flow_id("unregistered");
    let flow_id = flow_id("type-specified");
    environment
        .client
        .start_flow(&workflow, &flow_id, ())
        .expect("start custom-type Flow");
    environment
        .client
        .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
        .expect("complete custom-type Flow");
    assert!(matches!(
        environment
            .client
            .start_flow(&UnregisteredFlow, &unregistered_flow_id, ())
            .expect_err("unregistered Flow type must fail"),
        SdkError::FlowDefinition { .. }
    ));
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_model_input_workflow() {
    let environment =
        DexDevTestEnvironment::start(Registry::new().register(BasicModelInputWorkflow::new()));
    let workflow = BasicModelInputWorkflow::new();
    let flow_id = flow_id("model-input");
    environment
        .client
        .start_flow(&workflow, &flow_id, BasicModelInput { value: 10 })
        .expect("start model-input Flow");
    assert_eq!(
        10,
        environment
            .client
            .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
            .and_then(|result| result.single_output::<i32>())
            .expect("complete model-input Flow")
    );
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_workflow_config_override() {
    let environment = DexDevTestEnvironment::start(Registry::new().register(BasicWorkflow::new()));
    let workflow = BasicWorkflow::new();
    let flow_id = flow_id("config-override");
    let options =
        StartFlowOptions::new().config_override(FlowConfig::new().continue_as_new_threshold(1));
    environment
        .client
        .start_flow_with_options(&workflow, &flow_id, 0, options)
        .expect("start Flow with config override");
    assert_eq!(
        2,
        environment
            .client
            .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
            .and_then(|result| result.single_output::<i32>())
            .expect("complete Flow with config override")
    );
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_get_workflow_status_when_no_existing_workflow() {
    let environment = DexDevTestEnvironment::start(Registry::new().register(BasicWorkflow::new()));
    match environment
        .client
        .describe_flow(&flow_id("missing"))
        .expect_err("describing a missing Flow must fail")
    {
        SdkError::FlowNotFound { service } => {
            assert_eq!(GrpcCode::NotFound, service.code())
        }
        error => panic!("expected FlowNotFound, got {error:?}"),
    }
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_get_workflow_status_when_workflow_is_running() {
    let environment = DexDevTestEnvironment::start(Registry::new().register(SignalWorkflow::new()));
    let workflow = SignalWorkflow::new();
    let flow_id = flow_id("running");
    environment
        .client
        .start_flow(&workflow, &flow_id, 0)
        .expect("start waiting Flow");
    assert_eq!(
        FlowStatus::Running,
        environment
            .client
            .describe_flow(&flow_id)
            .expect("describe running Flow")
            .status
    );
    environment
        .client
        .stop_flow(&flow_id, dex_sdk::StopFlowOptions::cancel())
        .expect("cancel waiting Flow");
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_workflow_wait_for_step_completion() {
    let environment = DexDevTestEnvironment::start(Registry::new().register(BasicWorkflow::new()));
    let workflow = BasicWorkflow::new();
    let flow_id = flow_id("wait-step");
    environment
        .client
        .start_flow(&workflow, &flow_id, 5)
        .expect("start basic Flow");
    environment
        .client
        .wait_for_step_completion(
            &flow_id,
            StepExecutionId::of(&workflow.second),
            dex_sdk::WaitForStepCompletionOptions::new(),
        )
        .expect("wait for second Step");
    environment
        .client
        .wait_for_step_completion(
            &flow_id,
            StepExecutionId::of(&workflow.second),
            dex_sdk::WaitForStepCompletionOptions::new().request_timeout(Duration::from_secs(30)),
        )
        .expect("reuse server-derived Step wait ID");
    assert_eq!(
        7,
        environment
            .client
            .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
            .and_then(|result| result.single_output::<i32>())
            .expect("complete basic Flow")
    );
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_proceed_on_wait_failure_workflow() {
    let environment = DexDevTestEnvironment::start(
        Registry::new().register(BasicProceedOnWaitFailureWorkflow::new()),
    );
    let workflow = BasicProceedOnWaitFailureWorkflow::new();
    let flow_id = flow_id("proceed-on-wait-failure");
    environment
        .client
        .start_flow(&workflow, &flow_id, "input".to_string())
        .expect("start wait-failure Flow");
    assert_eq!(
        "input-recovered",
        environment
            .client
            .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
            .and_then(|result| result.single_output::<String>())
            .expect("recover from wait failure")
    );
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_mixed_wait_styles() {
    let environment = DexDevTestEnvironment::start(
        Registry::new().register(SkipWaitUntilMixedWaitWorkflow::new()),
    );
    let workflow = SkipWaitUntilMixedWaitWorkflow::new();
    let flow_id = flow_id("mixed-wait");
    environment
        .client
        .start_flow(&workflow, &flow_id, 0)
        .expect("start mixed-wait Flow");
    assert_eq!(
        2,
        environment
            .client
            .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
            .and_then(|result| result.single_output::<i32>())
            .expect("complete mixed-wait Flow")
    );
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_movement_options_do_not_mutate_step_defaults() {
    let environment = DexDevTestEnvironment::start(
        Registry::new().register(BasicImmutableStepOptionsWorkflow::new()),
    );
    let workflow = BasicImmutableStepOptionsWorkflow::new();
    let flow_id = flow_id("immutable-options");
    environment
        .client
        .start_flow(&workflow, &flow_id, 0)
        .expect("start immutable-options Flow");
    let failure = environment
        .client
        .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
        .expect("wait for failed Flow result");
    assert_eq!(FlowStatus::Failed, failure.status());
    assert_eq!(Some(FlowErrorType::WorkerApiFailed), failure.error_type());
    assert_eq!(Some("expected wait failure 2"), failure.error_message());
}

#[expect(
    dead_code,
    reason = "Integration fixtures implement schema variants exercised by selected test targets."
)]
fn compile_basic_and_reuse(client: &Client) -> SdkResult<()> {
    let workflow = BasicWorkflow::new();
    let options = StartFlowOptions::new()
        .timeout(Duration::from_secs(10))
        .id_reuse_policy(IdReusePolicy::AllowIfNotRunning);
    client.start_flow_with_options(&workflow, "basic", 10, options.clone())?;
    let output: i32 = client.wait_for_flow("basic")?.single_output()?;
    let abnormal = BasicAbnormalExitWorkflow::new();
    client.start_flow_with_options(&abnormal, "abnormal", 10, options.clone())?;
    client.start_flow_with_options(&workflow, "abnormal", output, options)?;
    Ok(())
}

#[expect(
    dead_code,
    reason = "Integration fixtures implement schema variants exercised by selected test targets."
)]
fn compile_empty_and_model_inputs(client: &Client) -> SdkResult<()> {
    client.start_flow(&BasicEmptyInputWorkflow::new(), "empty", ())?;
    client.start_flow(
        &BasicModelInputWorkflow::new(),
        "model",
        BasicModelInput { value: 10 },
    )?;
    Ok(())
}

#[expect(
    dead_code,
    reason = "Integration fixtures implement schema variants exercised by selected test targets."
)]
fn compile_failure_policy_and_config_override(client: &Client) -> SdkResult<()> {
    let config = FlowConfig::new()
        .active_step_search_mode(ActiveStepSearchMode::All)
        .worker_target(WorkerTarget::new("worker:8803"));
    let options = StartFlowOptions::new().config_override(config.clone());
    client.start_flow_with_options(
        &BasicProceedOnWaitFailureWorkflow::new(),
        "recover",
        "input".to_string(),
        options.clone(),
    )?;
    client.start_flow_with_options(&SkipWaitUntilMixedWaitWorkflow::new(), "mixed", 0, options)?;
    client.update_flow_config("mixed", config)
}

#[expect(
    dead_code,
    reason = "Integration fixtures implement schema variants exercised by selected test targets."
)]
fn compile_describe_and_step_wait(client: &Client) -> SdkResult<()> {
    let workflow = BasicWorkflow::new();
    let _info = client.describe_flow("basic")?;
    client.wait_for_step_completion(
        "basic",
        StepExecutionId::of(&workflow.second),
        dex_sdk::WaitForStepCompletionOptions::new()
            .request_id("wait-basic-second-step")
            .request_timeout(Duration::from_secs(5)),
    )
}
