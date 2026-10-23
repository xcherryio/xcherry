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
use std::sync::atomic::Ordering;
use std::time::Duration;

use dex_sdk::{Registry, StepExecutionId};

use crate::step_cancellation_workflow::{
    CancellationBlockingExecute, CancellationBlockingWaitFor, CancellationScenario,
    CancellationState, StepCancellationWorkflow,
};
use crate::support::{DexDevTestEnvironment, flow_id, skip_if_server_zd};

#[test]
#[ignore = "requires dexcli dev"]
fn test_step_cancellation() {
    let is_server_zd = skip_if_server_zd("Step failure policies");
    for scenario in CancellationScenario::ALL {
        if is_server_zd
            && !matches!(
                scenario,
                CancellationScenario::GlobalSelector | CancellationScenario::SiblingSelector
            )
        {
            continue;
        }
        run_scenario(scenario);
    }
}

fn run_scenario(scenario: CancellationScenario) {
    let state = CancellationState::new(scenario);
    let workflow = StepCancellationWorkflow::new(Arc::clone(&state));
    let environment = DexDevTestEnvironment::start(Registry::new().register(workflow));
    let client_workflow = StepCancellationWorkflow::new(Arc::clone(&state));
    let flow_id = flow_id(&format!("rust-cancellation-{}", scenario.name()));
    environment
        .client
        .start_flow(&client_workflow, &flow_id, scenario.name().to_string())
        .expect("start cancellation Flow");

    if !matches!(
        scenario,
        CancellationScenario::GlobalSelector | CancellationScenario::SiblingSelector
    ) {
        assert!(state.blocking_started.wait(Duration::from_secs(10)));
        if scenario == CancellationScenario::HeartbeatWaitFor {
            environment
                .client
                .wait_for_step_completion(
                    &flow_id,
                    StepExecutionId::of(&CancellationBlockingWaitFor(Arc::clone(&state))),
                    dex_sdk::WaitForStepCompletionOptions::new()
                        .request_id(format!("{flow_id}-wait-canceled-wait-for"))
                        .request_timeout(Duration::from_secs(30)),
                )
                .expect("wait for canceled WaitFor Step");
        } else {
            environment
                .client
                .wait_for_step_completion(
                    &flow_id,
                    StepExecutionId::of(&CancellationBlockingExecute {
                        state: Arc::clone(&state),
                    }),
                    dex_sdk::WaitForStepCompletionOptions::new()
                        .request_id(format!("{flow_id}-wait-canceled-execute"))
                        .request_timeout(Duration::from_secs(30)),
                )
                .expect("wait for canceled Execute Step");
        }
    }

    assert_eq!(
        scenario.name(),
        environment
            .client
            .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
            .and_then(|result| result.single_output::<String>())
            .expect("complete cancellation Flow")
    );

    match scenario {
        CancellationScenario::GlobalSelector => {
            assert!(!state.first_selector_executed.load(Ordering::SeqCst));
            assert!(!state.second_selector_executed.load(Ordering::SeqCst));
            return;
        }
        CancellationScenario::SiblingSelector => {
            assert!(!state.first_selector_executed.load(Ordering::SeqCst));
            assert!(state.second_selector_executed.load(Ordering::SeqCst));
            return;
        }
        CancellationScenario::NoHeartbeat => {
            assert!(!state.handler_canceled.load(Ordering::SeqCst));
            assert!(!state.late_handler_returned.is_set());
            assert!(state.late_handler_returned.wait(Duration::from_secs(8)));
        }
        _ => {
            assert!(state.cancellation_observed.wait(Duration::from_secs(8)));
            assert!(state.handler_canceled.load(Ordering::SeqCst));
            assert!(state.context_reported_cancellation.load(Ordering::SeqCst));
        }
    }
    let expected_invocations = if matches!(
        scenario,
        CancellationScenario::LocalExecute | CancellationScenario::LocalTimeoutFallback
    ) {
        2
    } else {
        1
    };
    assert_eq!(
        expected_invocations,
        state.blocking_invocations.load(Ordering::SeqCst),
        "{}",
        scenario.name()
    );
    assert!(!state.recovery_ran.load(Ordering::SeqCst));
}
