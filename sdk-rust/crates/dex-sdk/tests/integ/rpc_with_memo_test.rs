// Portions of this file are derived from indeedeng/iwf-java-sdk.
// Those portions are licensed under the Apache License, Version 2.0.
// See LICENSES/Apache-2.0.txt and LEGACY_NOTICES.md.
//
// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications are licensed under the Sustainable Use License 1.0.
// Third-Party Materials remain under the Apache License, Version 2.0.
// See LICENSE and LEGACY_NOTICES.md.

use dex_sdk::{Client, Registry, SdkResult, StopFlowOptions};

use crate::rpc_test::{assert_rpc_completion, verify_optional_rpc_attributes};
use crate::rpc_workflow::RpcWorkflow;
use crate::support::{DexDevTestEnvironment, flow_id};

#[test]
#[ignore = "requires dexcli dev"]
fn test_rpc_memo_workflow_func1() {
    let (environment, workflow, flow_id) = start("rpc-attribute-func-1");
    verify_optional_rpc_attributes(&environment, &flow_id);
    assert_eq!(
        RpcWorkflow::RPC_OUTPUT,
        environment
            .client
            .invoke_rpc(&flow_id, RpcWorkflow::FUNCTION_ONE, "rpc-input".to_string())
            .expect("invoke function one")
    );
    assert_rpc_completion(&environment, &workflow, &flow_id, "rpc-input");
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_rpc_memo_workflow_func0() {
    let (environment, workflow, flow_id) = start("rpc-attribute-func-0");
    assert_eq!(
        RpcWorkflow::RPC_OUTPUT,
        environment
            .client
            .invoke_rpc_without_input(&flow_id, RpcWorkflow::FUNCTION_ZERO)
            .expect("invoke function zero")
    );
    assert_rpc_completion(
        &environment,
        &workflow,
        &flow_id,
        RpcWorkflow::HARDCODED_VALUE,
    );
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_rpc_memo_workflow_proc1() {
    let (environment, workflow, flow_id) = start("rpc-attribute-proc-1");
    environment
        .client
        .invoke_rpc(
            &flow_id,
            RpcWorkflow::PROCEDURE_ONE,
            "rpc-input".to_string(),
        )
        .expect("invoke procedure one");
    assert_rpc_completion(&environment, &workflow, &flow_id, "rpc-input");
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_rpc_memo_workflow_proc0() {
    let (environment, workflow, flow_id) = start("rpc-attribute-proc-0");
    environment
        .client
        .invoke_rpc_without_input(&flow_id, RpcWorkflow::PROCEDURE_ZERO)
        .expect("invoke procedure zero");
    assert_rpc_completion(
        &environment,
        &workflow,
        &flow_id,
        RpcWorkflow::HARDCODED_VALUE,
    );
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_rpc_memo_workflow_func1_read_only() {
    let (environment, _workflow, flow_id) = start("rpc-attribute-read-only");
    assert_eq!(
        RpcWorkflow::RPC_OUTPUT,
        environment
            .client
            .invoke_rpc(&flow_id, RpcWorkflow::READ_ONLY, "rpc-input".to_string())
            .expect("invoke read-only function")
    );
    environment
        .client
        .stop_flow(
            &flow_id,
            StopFlowOptions::fail().reason(RpcWorkflow::HARDCODED_VALUE),
        )
        .expect("stop read-only Flow");
}

fn start(prefix: &str) -> (DexDevTestEnvironment, RpcWorkflow, String) {
    let environment = DexDevTestEnvironment::start(Registry::new().register(RpcWorkflow::new()));
    let workflow = RpcWorkflow::new();
    let flow_id = flow_id(prefix);
    environment
        .client
        .start_flow(&workflow, &flow_id, 999)
        .expect("start RPC Flow");
    (environment, workflow, flow_id)
}

#[expect(
    dead_code,
    reason = "Integration fixtures implement schema variants exercised by selected test targets."
)]
fn compile_memo_replacement(client: &Client) -> SdkResult<()> {
    client.start_flow(&RpcWorkflow::new(), "rpc-cache", 0)?;
    client.invoke_rpc(
        "rpc-cache",
        RpcWorkflow::SET_DATA,
        Some("value".to_string()),
    )?;
    let _data: Option<String> =
        client.invoke_rpc_without_input("rpc-cache", RpcWorkflow::GET_DATA)?;
    client.invoke_rpc(
        "rpc-cache",
        RpcWorkflow::SET_KEYWORD,
        Some("keyword".to_string()),
    )?;
    let _keyword: Option<String> =
        client.invoke_rpc_without_input("rpc-cache", RpcWorkflow::GET_KEYWORD)?;
    let _result: i64 =
        client.invoke_rpc("rpc-cache", RpcWorkflow::FUNCTION_ONE, "input".to_string())?;
    Ok(())
}
