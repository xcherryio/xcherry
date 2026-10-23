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
    Client, FlowStatus, Registry, SdkResult, StartFlowOptions, StepExecutionId, TimeTravelOptions,
    TimeTravelStepMethod,
};

use crate::reset_workflow::ResetWorkflow;
use crate::support::{DexDevTestEnvironment, flow_id};

#[test]
#[ignore = "requires dexcli dev"]
fn test_reset_with_locking_reapplies_rpc() {
    run_reset_scenario(true, false);
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_reset_with_locking_can_skip_rpc_reapply() {
    run_reset_scenario(true, true);
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_reset_without_locking_reapplies_channel_rpc() {
    run_reset_scenario(false, false);
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_reset_without_locking_can_skip_channel_reapply() {
    run_reset_scenario(false, true);
}

fn run_reset_scenario(locking: bool, skip_writes: bool) {
    let environment = DexDevTestEnvironment::start(Registry::new().register(ResetWorkflow::new()));
    let workflow = ResetWorkflow::new();
    let flow_id = flow_id("reset");
    environment
        .client
        .start_flow_with_options(
            &workflow,
            &flow_id,
            (),
            StartFlowOptions::new().timeout(Duration::from_secs(3)),
        )
        .expect("start reset Flow");
    if locking {
        environment
            .client
            .invoke_rpc_without_input(&flow_id, ResetWorkflow::WITH_ATTRIBUTE_MAP_LOCK)
            .expect("invoke AttributeMap-locking RPC");
        environment
            .client
            .invoke_rpc_without_input(&flow_id, ResetWorkflow::WITH_LOCKING)
            .expect("invoke locking RPC");
    } else {
        environment
            .client
            .invoke_rpc_without_input(&flow_id, ResetWorkflow::WITHOUT_LOCKING)
            .expect("invoke non-locking RPC");
    }
    assert_completed_with_attributes(&environment, &flow_id);
    let reset_run_id = environment
        .client
        .time_travel(
            &flow_id,
            TimeTravelOptions::from_beginning()
                .reason("testing reset")
                .skip_writes_reapply(skip_writes),
        )
        .expect("reset Flow");
    if skip_writes {
        assert_reset_times_out_without_attributes(&environment, &flow_id, &reset_run_id);
    } else {
        assert_completed_with_attributes(&environment, &flow_id);
        assert_eq!(
            reset_run_id,
            environment
                .client
                .describe_flow(&flow_id)
                .expect("describe reset Flow")
                .run_id
        );
    }
}

fn assert_completed_with_attributes(environment: &DexDevTestEnvironment, flow_id: &str) {
    let result = environment
        .client
        .wait_for_flow_with_timeout(flow_id, Duration::from_secs(10))
        .expect("complete reset Flow");
    assert_eq!(2, result.completions().len());
    assert_ne!(
        result.completions()[0].step_execution_id,
        result.completions()[1].step_execution_id
    );
    let mut outputs = result
        .completions()
        .iter()
        .map(|completion| completion.decode::<i32>().expect("decode reset output"))
        .collect::<Vec<_>>();
    outputs.sort_unstable();
    assert_eq!(vec![1, 2], outputs);
    assert_eq!(
        FlowStatus::Completed,
        environment
            .client
            .describe_flow(flow_id)
            .expect("describe completed reset Flow")
            .status
    );
}

fn assert_reset_times_out_without_attributes(
    environment: &DexDevTestEnvironment,
    flow_id: &str,
    _reset_run_id: &str,
) {
    let result = environment
        .client
        .wait_for_flow_with_timeout(flow_id, Duration::from_secs(10))
        .expect("wait for failed Flow result");
    assert_eq!(FlowStatus::Failed, result.status());
    assert_eq!(0, result.completions().len());
}

#[expect(
    dead_code,
    reason = "Integration fixtures implement schema variants exercised by selected test targets."
)]
fn compile_locking_rpc_reapply(client: &Client) -> SdkResult<()> {
    let workflow = ResetWorkflow::new();
    client.start_flow(&workflow, "reset-locking", ())?;
    client.invoke_rpc_without_input::<()>("reset-locking", ResetWorkflow::WITH_LOCKING)?;
    client
        .invoke_rpc_without_input::<()>("reset-locking", ResetWorkflow::WITH_ATTRIBUTE_MAP_LOCK)?;
    let options = TimeTravelOptions::from_beginning()
        .reason("replay locking RPC")
        .skip_writes_reapply(false);
    let _run_id = client.time_travel("reset-locking", options)?;
    Ok(())
}

#[expect(
    dead_code,
    reason = "Integration fixtures implement schema variants exercised by selected test targets."
)]
fn compile_skip_writes_reapply(client: &Client) -> SdkResult<()> {
    let workflow = ResetWorkflow::new();
    let options = TimeTravelOptions::from_step(&workflow.first).skip_writes_reapply(true);
    let _run_id = client.time_travel("reset-locking", options)?;
    let options = TimeTravelOptions::from_step_execution(
        StepExecutionId::of(&workflow.first),
        TimeTravelStepMethod::Execute,
    );
    let _run_id = client.time_travel("reset-locking", options)?;
    Ok(())
}
