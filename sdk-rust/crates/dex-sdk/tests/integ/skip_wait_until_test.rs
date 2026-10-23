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

use dex_sdk::{Client, FlowConfig, Registry, SdkResult, StartFlowOptions};

use crate::skip_wait_until_mixed_wait_workflow::SkipWaitUntilMixedWaitWorkflow;
use crate::skip_wait_until_workflow::SkipWaitUntilWorkflow;
use crate::support::{DexDevTestEnvironment, flow_id};

#[test]
#[ignore = "requires dexcli dev"]
fn test_skip_wait_until() {
    let environment =
        DexDevTestEnvironment::start(Registry::new().register(SkipWaitUntilWorkflow::new()));
    let workflow = SkipWaitUntilWorkflow::new();
    let flow_id = flow_id("skip-wait-until");
    let options =
        StartFlowOptions::new().config_override(FlowConfig::new().continue_as_new_threshold(1));
    environment
        .client
        .start_flow_with_options(&workflow, &flow_id, 0, options)
        .expect("start execute-only Flow");
    assert_eq!(
        2,
        environment
            .client
            .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
            .and_then(|result| result.single_output::<i32>())
            .expect("complete execute-only Flow")
    );
}

#[expect(
    dead_code,
    reason = "Integration fixtures implement schema variants exercised by selected test targets."
)]
fn compile_skip_wait_until_test(client: &Client) -> SdkResult<()> {
    client.start_flow(&SkipWaitUntilWorkflow::new(), "execute-only", 0)?;
    let _: i32 = client.wait_for_flow("execute-only")?.single_output()?;
    client.start_flow(&SkipWaitUntilMixedWaitWorkflow::new(), "mixed-wait", 0)?;
    let _: i32 = client.wait_for_flow("mixed-wait")?.single_output()?;
    Ok(())
}
