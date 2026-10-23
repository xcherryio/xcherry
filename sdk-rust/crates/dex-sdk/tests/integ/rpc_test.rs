// Portions of this file are derived from indeedeng/iwf-java-sdk.
// Those portions are licensed under the Apache License, Version 2.0.
// See LICENSES/Apache-2.0.txt and LEGACY_NOTICES.md.
//
// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications are licensed under the Sustainable Use License 1.0.
// Third-Party Materials remain under the Apache License, Version 2.0.
// See LICENSE and LEGACY_NOTICES.md.

use std::sync::Arc;
use std::time::Duration;

use dex_sdk::{Client, GrpcCode, Registry, SdkError, SdkResult, StopFlowOptions};

use crate::no_start_state_dead_end_workflow::NoStartStateDeadEndWorkflow;
use crate::rpc_no_state_workflow::RpcNoStateWorkflow;
use crate::rpc_workflow::RpcWorkflow;
use crate::support::{DexDevTestEnvironment, flow_id};

#[test]
#[ignore = "requires dexcli dev"]
fn test_locking_rpc() {
    let environment = Arc::new(DexDevTestEnvironment::start(
        Registry::new().register(RpcNoStateWorkflow::new()),
    ));
    let workflow = RpcNoStateWorkflow::new();
    let flow_id = flow_id("rpc-lock");
    environment
        .client
        .start_flow(&workflow, &flow_id, ())
        .expect("start RPC locking Flow");

    let next_invocation = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut workers = Vec::new();
    for _ in 0..10 {
        let environment = Arc::clone(&environment);
        let flow_id = flow_id.clone();
        let next_invocation = Arc::clone(&next_invocation);
        workers.push(std::thread::spawn(move || {
            let mut succeeded = 0;
            loop {
                let invocation = next_invocation.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if invocation >= 100 {
                    return succeeded;
                }
                match environment
                    .client
                    .invoke_rpc_without_input::<i32>(&flow_id, RpcNoStateWorkflow::INCREASE_COUNTER)
                {
                    Ok(_) => succeeded += 1,
                    Err(SdkError::RpcLockConflict { .. }) => {}
                    Err(error) => panic!("increase counter RPC failed: {error:?}"),
                }
            }
        }));
    }
    let succeeded: usize = workers
        .into_iter()
        .map(|worker| worker.join().expect("join RPC worker"))
        .sum();
    assert!(succeeded > 0);
    assert_eq!(
        Some(succeeded as i32),
        environment
            .client
            .invoke_rpc_without_input::<Option<i32>>(&flow_id, RpcNoStateWorkflow::GET_COUNTER,)
            .expect("read locked counter")
    );
    environment
        .client
        .stop_flow(&flow_id, StopFlowOptions::cancel())
        .expect("stop RPC locking Flow");
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_rpc_procedure_without_attribute_access() {
    let (environment, _workflow, flow_id) = start_rpc_workflow("rpc-no-attributes");
    environment
        .client
        .invoke_rpc_without_input(&flow_id, RpcWorkflow::PUBLISH_WITHOUT_ATTRIBUTE_ACCESS)
        .expect("publish without Attribute access");
    assert_eq!(
        2,
        environment
            .client
            .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
            .and_then(|result| result.single_output::<i32>())
            .expect("complete RPC Flow")
    );
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_rpc_workflow_func1() {
    let (environment, workflow, flow_id) = start_rpc_workflow("rpc-func-1");
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
fn test_rpc_workflow_func0() {
    let (environment, workflow, flow_id) = start_rpc_workflow("rpc-func-0");
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
fn test_rpc_workflow_proc1() {
    let (environment, workflow, flow_id) = start_rpc_workflow("rpc-proc-1");
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
fn test_rpc_workflow_proc0() {
    let (environment, workflow, flow_id) = start_rpc_workflow("rpc-proc-0");
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
fn test_rpc_workflow_func1_read_only() {
    let (environment, _workflow, flow_id) = start_rpc_workflow("rpc-read-only");
    assert_eq!(
        RpcWorkflow::RPC_OUTPUT,
        environment
            .client
            .invoke_rpc(&flow_id, RpcWorkflow::READ_ONLY, "rpc-input".to_string())
            .expect("invoke read-only function")
    );
    environment
        .client
        .invoke_rpc(
            &flow_id,
            RpcWorkflow::SET_DATA,
            Some("retained-value".to_string()),
        )
        .expect("persist retained snapshot");
    environment
        .client
        .stop_flow(
            &flow_id,
            StopFlowOptions::fail().reason(RpcWorkflow::HARDCODED_VALUE),
        )
        .expect("stop read-only Flow");
    environment
        .client
        .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
        .expect("establish terminal status");
    assert_eq!(
        Some("retained-value".to_string()),
        environment
            .client
            .invoke_rpc_without_input(&flow_id, RpcWorkflow::GET_DATA)
            .expect("read retained snapshot")
    );
    assert!(matches!(
        environment
            .client
            .invoke_rpc(&flow_id, RpcWorkflow::SET_DATA, Some("value".to_string())),
        Err(SdkError::FlowNotActiveOrNotFound { .. })
    ));
    let missing_flow_id = crate::support::flow_id("rpc-missing");
    let missing_error = environment
        .client
        .invoke_rpc_without_input(&missing_flow_id, RpcWorkflow::GET_DATA)
        .expect_err("missing query target");
    assert!(matches!(
        &missing_error,
        SdkError::FlowNotActiveOrNotFound { .. }
    ));
    let service = missing_error.service_error().expect("service metadata");
    assert_eq!(service.flow_id(), Some(missing_flow_id.as_str()));
    assert!(std::error::Error::source(service).is_some());
    assert!(matches!(
        environment.client.invoke_rpc(
            &missing_flow_id,
            RpcWorkflow::SET_DATA,
            Some("value".to_string())
        ),
        Err(SdkError::FlowNotActiveOrNotFound { .. })
    ));
    assert!(matches!(
        environment.client.describe_flow(&missing_flow_id),
        Err(SdkError::FlowNotFound { .. })
    ));
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_rpc_error() {
    let environment =
        DexDevTestEnvironment::start(Registry::new().register(RpcNoStateWorkflow::new()));
    let workflow = RpcNoStateWorkflow::new();
    let flow_id = flow_id("rpc-error");
    environment
        .client
        .start_flow(&workflow, &flow_id, ())
        .expect("start RPC error Flow");
    match environment
        .client
        .invoke_rpc::<String, i64>(
            &flow_id,
            RpcNoStateWorkflow::FAIL,
            "this is an error".to_string(),
        )
        .expect_err("RPC must return user error")
    {
        SdkError::WorkerInvocation { service, worker } => {
            assert_eq!(GrpcCode::FailedPrecondition, service.code());
            assert_eq!("RpcNoStateFailure", worker.error_type());
            assert!(worker.detail().contains("this is an error"));
        }
        error => panic!("expected WorkerInvocation, got {error:?}"),
    }
    environment
        .client
        .stop_flow(&flow_id, StopFlowOptions::cancel())
        .expect("stop RPC error Flow");
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_signal_channel_size_info() {
    let environment =
        DexDevTestEnvironment::start(Registry::new().register(NoStartStateDeadEndWorkflow::new()));
    let workflow = NoStartStateDeadEndWorkflow::new();
    let flow_id = flow_id("channel-size");
    environment
        .client
        .start_flow(&workflow, &flow_id, ())
        .expect("start channel-size Flow");
    environment
        .client
        .invoke_rpc_without_input::<usize>(&flow_id, NoStartStateDeadEndWorkflow::PUBLISH_INTERNAL)
        .expect("publish first internal message");
    assert_eq!(
        2,
        environment
            .client
            .invoke_rpc_without_input::<usize>(
                &flow_id,
                NoStartStateDeadEndWorkflow::PUBLISH_INTERNAL,
            )
            .expect("publish second internal message")
    );
    environment
        .client
        .invoke_rpc(&flow_id, NoStartStateDeadEndWorkflow::PUBLISH_SIGNALS, 3)
        .expect("publish external messages");
    assert_eq!(
        3,
        environment
            .client
            .invoke_rpc_without_input::<usize>(&flow_id, NoStartStateDeadEndWorkflow::SIGNAL_SIZE,)
            .expect("read signal size")
    );
    environment
        .client
        .stop_flow(&flow_id, StopFlowOptions::cancel())
        .expect("stop channel-size Flow");
}

fn start_rpc_workflow(prefix: &str) -> (DexDevTestEnvironment, RpcWorkflow, String) {
    let environment = DexDevTestEnvironment::start(Registry::new().register(RpcWorkflow::new()));
    let workflow = RpcWorkflow::new();
    let flow_id = flow_id(prefix);
    environment
        .client
        .start_flow(&workflow, &flow_id, 999)
        .expect("start RPC Flow");
    (environment, workflow, flow_id)
}

pub(crate) fn verify_optional_rpc_attributes(environment: &DexDevTestEnvironment, flow_id: &str) {
    environment
        .client
        .invoke_rpc(
            flow_id,
            RpcWorkflow::SET_DATA,
            Some("test-value".to_string()),
        )
        .expect("set data Attribute");
    assert_eq!(
        Some("test-value".to_string()),
        environment
            .client
            .invoke_rpc_without_input(flow_id, RpcWorkflow::GET_DATA)
            .expect("get data Attribute")
    );
    environment
        .client
        .invoke_rpc(flow_id, RpcWorkflow::SET_DATA, None)
        .expect("delete data Attribute");
    assert_eq!(
        None,
        environment
            .client
            .invoke_rpc_without_input::<Option<String>>(flow_id, RpcWorkflow::GET_DATA)
            .expect("get deleted data Attribute")
    );
    environment
        .client
        .invoke_rpc(
            flow_id,
            RpcWorkflow::SET_KEYWORD,
            Some("test-value".to_string()),
        )
        .expect("set keyword Attribute");
    assert_eq!(
        Some("test-value".to_string()),
        environment
            .client
            .invoke_rpc_without_input(flow_id, RpcWorkflow::GET_KEYWORD)
            .expect("get keyword Attribute")
    );
    environment
        .client
        .invoke_rpc(flow_id, RpcWorkflow::SET_KEYWORD, None)
        .expect("delete keyword Attribute");
    assert_eq!(
        None,
        environment
            .client
            .invoke_rpc_without_input::<Option<String>>(flow_id, RpcWorkflow::GET_KEYWORD)
            .expect("get deleted keyword Attribute")
    );
}

pub(crate) fn assert_rpc_completion(
    environment: &DexDevTestEnvironment,
    _workflow: &RpcWorkflow,
    flow_id: &str,
    _expected_value: &str,
) {
    assert_eq!(
        2,
        environment
            .client
            .wait_for_flow_with_timeout(flow_id, Duration::from_secs(30))
            .and_then(|result| result.single_output::<i32>())
            .expect("complete RPC Flow")
    );
}

#[expect(
    dead_code,
    reason = "Integration fixtures implement schema variants exercised by selected test targets."
)]
fn compile_locking(client: &Client) -> SdkResult<()> {
    client.start_flow(&RpcNoStateWorkflow::new(), "rpc-lock", ())?;
    let _first: i32 =
        client.invoke_rpc_without_input("rpc-lock", RpcNoStateWorkflow::INCREASE_COUNTER)?;
    let _second: Option<i32> =
        client.invoke_rpc_without_input("rpc-lock", RpcNoStateWorkflow::GET_COUNTER)?;
    Ok(())
}

#[expect(
    dead_code,
    reason = "Integration fixtures implement schema variants exercised by selected test targets."
)]
fn compile_functions_and_procedures(client: &Client) -> SdkResult<()> {
    client.start_flow(&RpcWorkflow::new(), "rpc", 0)?;
    client.invoke_rpc_without_input::<()>("rpc", RpcWorkflow::PUBLISH_WITHOUT_ATTRIBUTE_ACCESS)?;
    let _one: i64 = client.invoke_rpc("rpc", RpcWorkflow::FUNCTION_ONE, "input".to_string())?;
    let _zero: i64 = client.invoke_rpc_without_input("rpc", RpcWorkflow::FUNCTION_ZERO)?;
    client.invoke_rpc("rpc", RpcWorkflow::PROCEDURE_ONE, "input".to_string())?;
    client.invoke_rpc_without_input::<()>("rpc", RpcWorkflow::PROCEDURE_ZERO)?;
    let _read_only: i64 = client.invoke_rpc("rpc", RpcWorkflow::READ_ONLY, "input".to_string())?;
    client.invoke_rpc("rpc", RpcWorkflow::SET_DATA, Some("value".to_string()))?;
    let _data: Option<String> = client.invoke_rpc_without_input("rpc", RpcWorkflow::GET_DATA)?;
    client.invoke_rpc("rpc", RpcWorkflow::SET_KEYWORD, Some("value".to_string()))?;
    let _keyword: Option<String> =
        client.invoke_rpc_without_input("rpc", RpcWorkflow::GET_KEYWORD)?;
    Ok(())
}

#[expect(
    dead_code,
    reason = "Integration fixtures implement schema variants exercised by selected test targets."
)]
fn compile_rpc_error_and_channel_size(client: &Client) -> SdkResult<()> {
    let _ignored: i64 =
        client.invoke_rpc("rpc-error", RpcNoStateWorkflow::FAIL, "error".to_string())?;
    let _published: usize = client.invoke_rpc_without_input(
        "channel-size",
        NoStartStateDeadEndWorkflow::PUBLISH_INTERNAL,
    )?;
    let _size: usize = client
        .invoke_rpc_without_input("channel-size", NoStartStateDeadEndWorkflow::SIGNAL_SIZE)?;
    Ok(())
}
