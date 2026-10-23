// Portions of this file are derived from indeedeng/iwf-java-sdk.
// Those portions are licensed under the Apache License, Version 2.0.
// See LICENSES/Apache-2.0.txt and LEGACY_NOTICES.md.
//
// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications are licensed under the Sustainable Use License 1.0.
// Third-Party Materials remain under the Apache License, Version 2.0.
// See LICENSE and LEGACY_NOTICES.md.

use std::thread;
use std::time::{Duration, Instant};

use dex_sdk::{
    Client, Flow, FlowStatus, Registry, StartFlowOptions, Step, StepExecutionId, StopFlowOptions,
    SubFlowReusePolicy, TimeTravelOptions, TimerId,
};

use crate::basic_abnormal_exit_workflow::BasicAbnormalExitWorkflow;
use crate::basic_workflow::BasicWorkflow;
use crate::subflow_workflow::{
    AbnormalSubFlowParent, AllSubFlowParent, AnySubFlowParent, ContinueAsNewSubFlowParent,
    SingleSubFlowParent, TimerSubFlowParent,
};
use crate::support::{DexDevTestEnvironment, flow_id, skip_if_server_zd, skip_timer_when_pending};
use crate::timer_workflow::{TimerStep, TimerWorkflow};

#[test]
#[ignore = "requires dexcli dev"]
fn test_subflow_returns_identity_and_output() {
    let parent = SingleSubFlowParent::new(None);
    let id = flow_id("sub-flow-parent");
    let expected_child = sub_flow_id(&id, &parent.start, 0);
    let environment = environment(registry2(parent, BasicWorkflow::new()));
    let parent = SingleSubFlowParent::new(None);
    environment.client.start_flow(&parent, &id, 4).unwrap();
    let output: String = wait(&environment.client, &id).single_output().unwrap();
    assert_eq!(format!("{expected_child}|Completed|6"), output);
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_subflow_all_of_returns_stable_terminal_results() {
    let parent = AllSubFlowParent::new();
    let id = flow_id("sub-flow-all");
    let first = sub_flow_id(&id, &parent.start, 0);
    let second = sub_flow_id(&id, &parent.start, 1);
    let environment = environment(registry2(parent, BasicWorkflow::new()));
    let parent = AllSubFlowParent::new();
    environment.client.start_flow(&parent, &id, 4).unwrap();
    let output: String = wait(&environment.client, &id).single_output().unwrap();
    assert_eq!(
        vec![
            format!("{first}|Completed|6"),
            format!("{second}|Completed|16")
        ],
        output.split(';').collect::<Vec<_>>()
    );
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_subflow_any_of_running_snapshot_can_be_stopped() {
    let parent = AnySubFlowParent::new();
    let id = flow_id("sub-flow-any");
    let child_id = sub_flow_id(&id, &parent.start, 0);
    let environment = environment(registry2(parent, TimerWorkflow::new()));
    let parent = AnySubFlowParent::new();
    environment.client.start_flow(&parent, &id, 300).unwrap();
    let output: String = wait(&environment.client, &id).single_output().unwrap();
    assert_eq!(
        vec![child_id.as_str(), "Running", "false", "true"],
        output.split('|').collect::<Vec<_>>()
    );
    environment
        .client
        .stop_flow(&child_id, StopFlowOptions::cancel())
        .unwrap();
    assert_eq!(
        FlowStatus::Canceled,
        wait(&environment.client, &child_id).status()
    );
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_subflow_attach_keeps_running_execution_across_parent_reset() {
    assert_running_reuse(SubFlowReusePolicy::Attach, false);
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_subflow_always_restart_replaces_running_execution_across_parent_reset() {
    assert_running_reuse(SubFlowReusePolicy::AlwaysRestart, true);
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_subflow_default_reuse_restarts_failed_execution_across_parent_reset() {
    let parent = AbnormalSubFlowParent::new();
    let id = flow_id("sub-flow-abnormal");
    let child_id = sub_flow_id(&id, &parent.start, 0);
    let environment = environment(registry2(parent, BasicAbnormalExitWorkflow::new()));
    let parent = AbnormalSubFlowParent::new();
    environment.client.start_flow(&parent, &id, 1).unwrap();
    let first: String = wait(&environment.client, &id).single_output().unwrap();
    assert_eq!("Failed", first.split('|').nth(1).unwrap());
    let first_run_id = environment.client.describe_flow(&child_id).unwrap().run_id;
    environment
        .client
        .time_travel(
            &id,
            TimeTravelOptions::from_beginning().reason("verify SubFlow abnormal reuse"),
        )
        .unwrap();
    let second: String = wait(&environment.client, &id).single_output().unwrap();
    assert_eq!("Failed", second.split('|').nth(1).unwrap());
    assert_ne!(
        first_run_id,
        environment.client.describe_flow(&child_id).unwrap().run_id
    );
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_subflow_partial_results_survive_continue_as_new_without_restart() {
    if skip_if_server_zd("Continue-As-New") {
        return;
    }
    let parent = ContinueAsNewSubFlowParent::new();
    let id = flow_id("sub-flow-can");
    let completed_id = sub_flow_id(&id, &parent.start, 0);
    let delayed_id = sub_flow_id(&id, &parent.start, 1);
    let environment = environment(registry3(
        parent,
        BasicWorkflow::new(),
        TimerWorkflow::new(),
    ));
    let parent = ContinueAsNewSubFlowParent::new();
    let first_parent_run_id = environment
        .client
        .start_flow_with_options(
            &parent,
            &id,
            4,
            StartFlowOptions::new()
                .config_override(dex_sdk::FlowConfig::new().continue_as_new_threshold(1)),
        )
        .unwrap();
    await_different_run(&environment.client, &id, &first_parent_run_id);
    let completed_run_id = environment
        .client
        .describe_flow(&completed_id)
        .unwrap()
        .run_id;
    skip_timer_when_pending(
        &environment.client,
        &delayed_id,
        StepExecutionId::of(&TimerStep),
        TimerId::by_condition_id("test-timer-id"),
    );
    let output: String = wait(&environment.client, &id).single_output().unwrap();
    assert_eq!(
        vec![completed_id.as_str(), "6", delayed_id.as_str(), "Completed"],
        output.split('|').collect::<Vec<_>>()
    );
    assert_eq!(
        completed_run_id,
        environment
            .client
            .describe_flow(&completed_id)
            .unwrap()
            .run_id
    );
}

fn assert_running_reuse(reuse_policy: SubFlowReusePolicy, expects_restart: bool) {
    let parent = TimerSubFlowParent::new(reuse_policy);
    let id = flow_id("sub-flow-reuse");
    let child_id = sub_flow_id(&id, &parent.start, 0);
    let environment = environment(registry2(parent, TimerWorkflow::new()));
    let parent = TimerSubFlowParent::new(reuse_policy);
    environment
        .client
        .start_flow(&parent, &id, 300)
        .expect("the SubFlow integration fixture completes the requested operation");
    let first_run_id = await_running(&environment.client, &child_id, None);
    environment
        .client
        .time_travel(
            &id,
            TimeTravelOptions::from_beginning().reason("verify SubFlow running reuse"),
        )
        .expect("the SubFlow integration fixture completes the requested operation");
    let active_run_id = await_running(
        &environment.client,
        &child_id,
        expects_restart.then_some(first_run_id.as_str()),
    );
    assert_eq!(expects_restart, active_run_id != first_run_id);
    skip_timer_when_pending(
        &environment.client,
        &child_id,
        StepExecutionId::of(&TimerStep),
        TimerId::by_condition_id("test-timer-id"),
    );
    let output: String = wait(&environment.client, &id)
        .single_output()
        .expect("the SubFlow integration fixture completes the requested operation");
    assert_eq!(
        vec![child_id.as_str(), "Completed"],
        output.split('|').collect::<Vec<_>>()
    );
}

fn environment(registry: dex_sdk::SdkResult<Registry>) -> DexDevTestEnvironment {
    DexDevTestEnvironment::start(registry)
}

fn registry2<First: Flow, Second: Flow>(
    first: First,
    second: Second,
) -> dex_sdk::SdkResult<Registry> {
    Registry::new()
        .register(first)
        .and_then(|registry| registry.register(second))
}

fn registry3<First: Flow, Second: Flow, Third: Flow>(
    first: First,
    second: Second,
    third: Third,
) -> dex_sdk::SdkResult<Registry> {
    registry2(first, second).and_then(|registry| registry.register(third))
}

fn wait(client: &Client, flow_id: &str) -> dex_sdk::FlowResult {
    client
        .wait_for_flow_with_timeout(flow_id, Duration::from_secs(30))
        .expect("the SubFlow integration fixture completes the requested operation")
}

fn await_running(client: &Client, flow_id: &str, excluded_run_id: Option<&str>) -> String {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Ok(info) = client.describe_flow(flow_id)
            && info.status == FlowStatus::Running
            && Some(info.run_id.as_str()) != excluded_run_id
        {
            return info.run_id;
        }
        thread::yield_now();
    }
    panic!("SubFlow did not reach expected running execution: {flow_id}");
}

fn await_different_run(client: &Client, flow_id: &str, first_run_id: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if client
            .describe_flow(flow_id)
            .expect("the SubFlow integration fixture completes the requested operation")
            .run_id
            != first_run_id
        {
            return;
        }
        thread::yield_now();
    }
    panic!("Flow did not continue as new: {flow_id}");
}

fn sub_flow_id<SomeStep: Step>(parent_flow_id: &str, step: &SomeStep, index: usize) -> String {
    format!("SubFlow:{parent_flow_id}-{}-1-{index}", step.step_type())
}
