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

use dex_sdk::{Client, FlowErrorType, FlowStatus, Registry, SdkResult, StartFlowOptions};

use crate::any_command_combination_workflow::AnyCommandCombinationWorkflow;
use crate::support::{DexDevTestEnvironment, flow_id};

#[test]
#[ignore = "requires dexcli dev"]
fn test_state_api_fail_workflow() {
    let environment = DexDevTestEnvironment::start(
        Registry::new().register(AnyCommandCombinationWorkflow::new()),
    );
    let workflow = AnyCommandCombinationWorkflow::new();
    let flow_id = flow_id("any-combination-fail");
    let run_id = environment
        .client
        .start_flow(&workflow, &flow_id, 5)
        .expect("start invalid-combination Flow");
    let result = environment
        .client
        .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
        .expect("wait for failed Flow result");
    assert_eq!(FlowStatus::Failed, result.status());
    assert_eq!(Some(FlowErrorType::WorkerApiFailed), result.error_type());
    assert!(
        result
            .error_message()
            .is_some_and(|value| value.contains("unknown condition ID"))
    );
    assert_eq!(0, result.completions().len());
    let info = environment
        .client
        .describe_flow(&flow_id)
        .expect("describe failed Flow");
    assert_eq!(run_id, info.run_id);
    assert_eq!(FlowStatus::Failed, info.status);
}

#[expect(
    dead_code,
    reason = "Integration fixtures implement schema variants exercised by selected test targets."
)]
fn compile_state_api_failure(client: &Client) -> SdkResult<()> {
    client.start_flow_with_options(
        &AnyCommandCombinationWorkflow::new(),
        "any-combination",
        0,
        StartFlowOptions::new().timeout(Duration::from_secs(10)),
    )?;
    let _: i32 = client.wait_for_flow("any-combination")?.single_output()?;
    Ok(())
}
