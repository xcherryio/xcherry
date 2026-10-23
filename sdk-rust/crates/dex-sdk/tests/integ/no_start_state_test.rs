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

use dex_sdk::{Client, Registry, SdkResult, StopFlowOptions};

use crate::no_start_state_dead_end_workflow::NoStartStateDeadEndWorkflow;
use crate::no_start_state_workflow::NoStartStateWorkflow;
use crate::rpc_no_state_workflow::RpcNoStateWorkflow;
use crate::support::{DexDevTestEnvironment, flow_id};

#[test]
#[ignore = "requires dexcli dev"]
fn test_no_start_state_workflow() {
    let environment =
        DexDevTestEnvironment::start(Registry::new().register(NoStartStateWorkflow::new()));
    let workflow = NoStartStateWorkflow::new();
    let flow_id = flow_id("no-start");
    environment
        .client
        .start_flow(&workflow, &flow_id, ())
        .expect("start Flow without a start Step");
    assert_eq!(
        NoStartStateWorkflow::RPC_OUTPUT,
        environment
            .client
            .invoke_rpc(
                &flow_id,
                NoStartStateWorkflow::INVOKE,
                "rpc-input".to_string(),
            )
            .expect("invoke no-start RPC")
    );
    assert_eq!(
        1,
        environment
            .client
            .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
            .and_then(|result| result.single_output::<i32>())
            .expect("complete no-start Flow")
    );
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_no_state_workflow() {
    let environment =
        DexDevTestEnvironment::start(Registry::new().register(RpcNoStateWorkflow::new()));
    let workflow = RpcNoStateWorkflow::new();
    let flow_id = flow_id("no-state");
    environment
        .client
        .start_flow(&workflow, &flow_id, ())
        .expect("start no-State Flow");
    assert_eq!(
        RpcNoStateWorkflow::RPC_OUTPUT,
        environment
            .client
            .invoke_rpc(
                &flow_id,
                RpcNoStateWorkflow::INVOKE,
                "rpc-input".to_string(),
            )
            .expect("invoke no-State RPC")
    );
    environment
        .client
        .stop_flow(&flow_id, StopFlowOptions::cancel())
        .expect("stop no-State Flow");
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_dead_end_workflow() {
    let environment =
        DexDevTestEnvironment::start(Registry::new().register(NoStartStateDeadEndWorkflow::new()));
    let workflow = NoStartStateDeadEndWorkflow::new();
    let flow_id = flow_id("dead-end");
    environment
        .client
        .start_flow(&workflow, &flow_id, ())
        .expect("start dead-end Flow");
    assert_eq!(
        100,
        environment
            .client
            .invoke_rpc(
                &flow_id,
                NoStartStateDeadEndWorkflow::INVOKE,
                "rpc-input".to_string(),
            )
            .expect("resume dead-end Flow")
    );
    environment
        .client
        .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
        .expect("complete resumed dead-end Flow");
}

#[expect(
    dead_code,
    reason = "Integration fixtures implement schema variants exercised by selected test targets."
)]
fn compile_no_start_step(client: &Client) -> SdkResult<()> {
    client.start_flow(&NoStartStateWorkflow::new(), "no-start", ())?;
    let _: i64 = client.invoke_rpc(
        "no-start",
        NoStartStateWorkflow::INVOKE,
        "input".to_string(),
    )?;
    Ok(())
}

#[expect(
    dead_code,
    reason = "Integration fixtures implement schema variants exercised by selected test targets."
)]
fn compile_no_step(client: &Client) -> SdkResult<()> {
    client.start_flow(&RpcNoStateWorkflow::new(), "no-step", ())?;
    let _: i32 =
        client.invoke_rpc_without_input("no-step", RpcNoStateWorkflow::INCREASE_COUNTER)?;
    client.stop_flow("no-step", StopFlowOptions::cancel())
}

#[expect(
    dead_code,
    reason = "Integration fixtures implement schema variants exercised by selected test targets."
)]
fn compile_dead_end(client: &Client) -> SdkResult<()> {
    client.start_flow(&NoStartStateDeadEndWorkflow::new(), "dead-end", ())?;
    let _: usize = client
        .invoke_rpc_without_input("dead-end", NoStartStateDeadEndWorkflow::PUBLISH_INTERNAL)?;
    Ok(())
}
