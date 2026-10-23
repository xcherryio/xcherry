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

use dex_sdk::{Client, Registry, SdkResult};

use crate::state_options_override_workflow::StateOptionsOverrideWorkflow;
use crate::support::{DexDevTestEnvironment, flow_id};

#[test]
#[ignore = "requires dexcli dev"]
fn test_state_options_override_workflow() {
    let environment =
        DexDevTestEnvironment::start(Registry::new().register(StateOptionsOverrideWorkflow::new()));
    let workflow = StateOptionsOverrideWorkflow::new();
    let flow_id = flow_id("state-options-override");
    environment
        .client
        .start_flow(&workflow, &flow_id, "input".to_string())
        .expect("start options-override Flow");
    assert_eq!(
        "input_state1_start_state1_decide_state2_start_state2_decide",
        environment
            .client
            .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
            .and_then(|result| result.single_output::<String>())
            .expect("complete options-override Flow")
    );
}

#[expect(
    dead_code,
    reason = "Integration fixtures implement schema variants exercised by selected test targets."
)]
fn compile_movement_options_override(client: &Client) -> SdkResult<()> {
    let workflow = StateOptionsOverrideWorkflow::new();
    client.start_flow(&workflow, "options-override", "input".to_string())?;
    let output: String = client.wait_for_flow("options-override")?.single_output()?;
    drop(output);
    Ok(())
}
