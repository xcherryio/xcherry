// Portions of this file are derived from indeedeng/iwf-java-sdk.
// Those portions are licensed under the Apache License, Version 2.0.
// See LICENSES/Apache-2.0.txt and LEGACY_NOTICES.md.
//
// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications are licensed under the Sustainable Use License 1.0.
// Third-Party Materials remain under the Apache License, Version 2.0.
// See LICENSE and LEGACY_NOTICES.md.

use std::time::{Duration, Instant};

use dex_sdk::{Client, Registry, SdkResult, StepExecutionId, WaitForStepCompletionOptions};

use crate::support::{DexDevTestEnvironment, flow_id};
use crate::timer_workflow::TimerWorkflow;

#[test]
#[ignore = "requires dexcli dev"]
fn test_basic_timer_workflow() {
    let environment = DexDevTestEnvironment::start(Registry::new().register(TimerWorkflow::new()));
    let workflow = TimerWorkflow::new();
    let flow_id = flow_id("basic-timer");
    let started_at = Instant::now();
    environment
        .client
        .start_flow(&workflow, &flow_id, 5)
        .expect("start timer Flow");
    environment
        .client
        .wait_for_step_completion(
            &flow_id,
            StepExecutionId::of(&workflow.start),
            WaitForStepCompletionOptions::new()
                .request_id(format!("{flow_id}-wait-timer"))
                .request_timeout(Duration::from_secs(10)),
        )
        .expect("wait for Timer Step");
    environment
        .client
        .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
        .expect("complete timer Flow");
    let elapsed = started_at.elapsed();
    assert!(
        (Duration::from_secs(4)..=Duration::from_secs(7)).contains(&elapsed),
        "actual duration: {elapsed:?}"
    );
}

#[expect(
    dead_code,
    reason = "Integration fixtures implement schema variants exercised by selected test targets."
)]
fn compile_timer_and_step_wait(client: &Client) -> SdkResult<()> {
    let workflow = TimerWorkflow::new();
    client.start_flow(&workflow, "timer", 1)?;
    client.wait_for_step_completion(
        "timer",
        StepExecutionId::of(&workflow.start),
        WaitForStepCompletionOptions::new()
            .request_id("wait-timer-step")
            .request_timeout(Duration::from_secs(10)),
    )?;
    let _ = client.wait_for_flow("timer")?;
    Ok(())
}
