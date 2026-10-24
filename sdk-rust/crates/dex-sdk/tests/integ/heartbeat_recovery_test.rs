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

use dex_sdk::Registry;

use crate::heartbeat_recovery_workflow::{
    HEARTBEAT_PROGRESS, HeartbeatRecoveryWorkflow, NoOutputHeartbeatTimeoutWorkflow,
};
use crate::support::{DexDevTestEnvironment, flow_id, skip_if_server_zd};

#[test]
#[ignore = "requires dexcli dev"]
fn test_heartbeat_recovery_and_local_fallback() {
    let workflow = HeartbeatRecoveryWorkflow::new();
    let environment = DexDevTestEnvironment::start(Registry::new().register(workflow.clone()));
    let flow_id = flow_id("rust-heartbeat-recovery");
    environment
        .client
        .start_flow(&workflow, &flow_id, ())
        .expect("start heartbeat recovery Flow");
    assert_eq!(
        "heartbeat-ok",
        environment
            .client
            .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(45))
            .and_then(|result| result.single_output::<String>())
            .expect("complete heartbeat recovery Flow")
    );

    let mut resume_token = String::new();
    let mut sources = Vec::new();
    for expected in ["sync-progress", "local-1", "local-2", "local-3"] {
        let message = environment
            .client
            .read_stream_with_timeout(
                &flow_id,
                &HEARTBEAT_PROGRESS,
                &resume_token,
                Duration::from_secs(30),
            )
            .expect("read heartbeat progress");
        assert_eq!(expected, message.value);
        assert!(message.source.starts_with('#'));
        sources.push(message.source);
        resume_token = message.resume_token;
    }
    assert_ne!(sources[0], sources[1]);
    assert_eq!(sources[1], sources[2]);
    assert_eq!(sources[2], sources[3]);
}

#[test]
#[ignore = "requires dexcli dev"]
fn test_no_output_handler_reaches_heartbeat_timeout() {
    if skip_if_server_zd("Step failure policies") {
        return;
    }
    let workflow = NoOutputHeartbeatTimeoutWorkflow::new();
    let environment = DexDevTestEnvironment::start(Registry::new().register(workflow.clone()));
    let flow_id = flow_id("rust-heartbeat-timeout");
    environment
        .client
        .start_flow(&workflow, &flow_id, ())
        .expect("start heartbeat timeout Flow");
    assert_eq!(
        "heartbeat-timeout",
        environment
            .client
            .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
            .and_then(|result| result.single_output::<String>())
            .expect("recover from heartbeat timeout")
    );
}
