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

use crate::conditional_complete_workflow::ConditionalCompleteWorkflow;
use crate::support::{DexDevTestEnvironment, flow_id};

#[test]
#[ignore = "requires dexcli dev"]
fn test_signal_channel() {
    let environment =
        DexDevTestEnvironment::start(Registry::new().register(ConditionalCompleteWorkflow::new()));
    let workflow = ConditionalCompleteWorkflow::new();
    let flow_id = flow_id("conditional-signal");
    environment
        .client
        .start_flow(&workflow, &flow_id, true)
        .expect("start conditional signal Flow");
    environment
        .client
        .invoke_rpc(&flow_id, ConditionalCompleteWorkflow::PUBLISH_SIGNAL, 3)
        .expect("publish signal messages");
    assert_eq!(
        3,
        environment
            .client
            .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
            .and_then(|result| result.single_output::<i32>())
            .expect("drain signal channel")
    );
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_internal_channel() {
    let environment =
        DexDevTestEnvironment::start(Registry::new().register(ConditionalCompleteWorkflow::new()));
    let workflow = ConditionalCompleteWorkflow::new();
    let flow_id = flow_id("conditional-internal");
    environment
        .client
        .start_flow(&workflow, &flow_id, false)
        .expect("start conditional internal Flow");
    environment
        .client
        .invoke_rpc(
            &flow_id,
            ConditionalCompleteWorkflow::PUBLISH_TO_INTERNAL,
            3,
        )
        .expect("publish internal messages through RPC");
    assert_eq!(
        3,
        environment
            .client
            .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
            .and_then(|result| result.single_output::<i32>())
            .expect("drain internal channel")
    );
}

#[expect(
    dead_code,
    reason = "Integration fixtures implement schema variants exercised by selected test targets."
)]
fn compile_conditional_complete(client: &Client) -> SdkResult<()> {
    let workflow = ConditionalCompleteWorkflow::new();
    client.start_flow(&workflow, "conditional-signal", true)?;
    client.invoke_rpc(
        "conditional-signal",
        ConditionalCompleteWorkflow::PUBLISH_SIGNAL,
        3,
    )?;
    let _: i32 = client
        .wait_for_flow("conditional-signal")?
        .single_output()?;
    client.start_flow(&workflow, "conditional-internal", false)?;
    client.invoke_rpc(
        "conditional-internal",
        ConditionalCompleteWorkflow::PUBLISH_TO_INTERNAL,
        3,
    )
}
