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

use crate::internal_channel_waiting_workflow::InternalChannelWaitingWorkflow;
use crate::internal_channel_workflow::InternalChannelWorkflow;
use crate::support::{DexDevTestEnvironment, flow_id};

#[test]
#[ignore = "requires dexcli dev"]
fn test_basic_internal_channel() {
    let environment =
        DexDevTestEnvironment::start(Registry::new().register(InternalChannelWorkflow::new()));
    let workflow = InternalChannelWorkflow::new();
    let flow_id = flow_id("basic-internal");
    environment
        .client
        .start_flow(&workflow, &flow_id, 1)
        .expect("start internal-channel Flow");
    assert_eq!(
        3,
        environment
            .client
            .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
            .and_then(|result| result.single_output::<i32>())
            .expect("complete internal-channel Flow")
    );
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_waiting_internal_channel() {
    let environment = DexDevTestEnvironment::start(
        Registry::new().register(InternalChannelWaitingWorkflow::new()),
    );
    let workflow = InternalChannelWaitingWorkflow::new();
    let flow_id = flow_id("waiting-internal");
    environment
        .client
        .start_flow(&workflow, &flow_id, 1)
        .expect("start waiting-channel Flow");
    environment
        .client
        .invoke_rpc(&flow_id, InternalChannelWaitingWorkflow::PUBLISH, 2)
        .expect("publish first waiting-channel message");
    environment
        .client
        .invoke_rpc(&flow_id, InternalChannelWaitingWorkflow::PUBLISH, 3)
        .expect("publish second waiting-channel message");
    assert_eq!(
        6,
        environment
            .client
            .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
            .and_then(|result| result.single_output::<i32>())
            .expect("complete waiting-channel Flow")
    );
}

#[expect(
    dead_code,
    reason = "Integration fixtures implement schema variants exercised by selected test targets."
)]
fn compile_internal_channels(client: &Client) -> SdkResult<()> {
    client.start_flow(&InternalChannelWorkflow::new(), "basic-internal", 1)?;
    let _: i32 = client.wait_for_flow("basic-internal")?.single_output()?;
    let workflow = InternalChannelWaitingWorkflow::new();
    client.start_flow(&workflow, "waiting-internal", 1)?;
    client.invoke_rpc(
        "waiting-internal",
        InternalChannelWaitingWorkflow::PUBLISH,
        2,
    )?;
    client.invoke_rpc(
        "waiting-internal",
        InternalChannelWaitingWorkflow::PUBLISH,
        3,
    )?;
    let _: i32 = client.wait_for_flow("waiting-internal")?.single_output()?;
    Ok(())
}
