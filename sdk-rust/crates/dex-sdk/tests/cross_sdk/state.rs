// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::time::Duration;

use dex_sdk::{
    Context, Flow, FlowErrorType, FlowStatus, HandlerError, HandlerResult, Registry, RetryPolicy,
    Step, StepDecision, StepList, StepOptions, Wait,
};

use crate::support::{DexDevTestEnvironment, flow_id, skip_if_server_zd};

struct ExecuteRecoveryWorkflow {
    start: ExecuteRecoveryFailStep,
    finish: ExecuteRecoveryFinishStep,
}

impl Flow for ExecuteRecoveryWorkflow {
    type StartInput = String;

    fn steps(&self) -> StepList<'_, Self::StartInput> {
        StepList::start(&self.start).and(&self.finish)
    }
}

struct ExecuteRecoveryFailStep;

impl Step for ExecuteRecoveryFailStep {
    type Input = String;

    fn execute(&self, _context: &mut Context, _input: String) -> HandlerResult<StepDecision> {
        Err(HandlerError::new("StateFailure", "planned Execute failure"))
    }

    fn options(&self) -> StepOptions<Self::Input> {
        StepOptions::new()
            .execute_retry(RetryPolicy::new().maximum_attempts(1))
            .on_execute_failure_proceed_to(&ExecuteRecoveryFinishStep)
    }
}

struct ExecuteRecoveryFinishStep;

impl Step for ExecuteRecoveryFinishStep {
    type Input = String;

    fn execute(&self, _context: &mut Context, _input: String) -> HandlerResult<StepDecision> {
        Ok(StepDecision::graceful_complete(
            "this is flow step 2".to_string(),
        ))
    }
}

struct WaitForFailureWorkflow {
    start: WaitForFailureStep,
}

impl Flow for WaitForFailureWorkflow {
    type StartInput = ();

    fn steps(&self) -> StepList<'_, Self::StartInput> {
        StepList::start(&self.start)
    }
}

struct WaitForFailureStep;

impl Step for WaitForFailureStep {
    type Input = ();

    fn wait_for(&self, _context: &mut Context, (): ()) -> HandlerResult<Wait> {
        Err(HandlerError::new("StateFailure", "test WaitFor failing"))
    }

    fn execute(&self, _context: &mut Context, (): ()) -> HandlerResult<StepDecision> {
        Ok(StepDecision::force_fail("must not execute"))
    }

    fn options(&self) -> StepOptions<Self::Input> {
        StepOptions::new().wait_for_retry(RetryPolicy::new().maximum_attempts(1))
    }
}

struct WaitForTimeoutWorkflow {
    start: WaitForTimeoutStep,
}

impl Flow for WaitForTimeoutWorkflow {
    type StartInput = ();

    fn steps(&self) -> StepList<'_, Self::StartInput> {
        StepList::start(&self.start)
    }
}

struct WaitForTimeoutStep;

impl Step for WaitForTimeoutStep {
    type Input = ();

    fn wait_for(&self, context: &mut Context, (): ()) -> HandlerResult<Wait> {
        context.wait_for_cancellation();
        Err(HandlerError::new(
            "StateFailure",
            "wait_for invocation was cancelled",
        ))
    }

    fn execute(&self, _context: &mut Context, (): ()) -> HandlerResult<StepDecision> {
        Ok(StepDecision::force_fail("must not execute"))
    }

    fn options(&self) -> StepOptions<Self::Input> {
        StepOptions::new()
            .wait_for_method_timeout(Duration::from_secs(1))
            .wait_for_retry(RetryPolicy::new().maximum_attempts(1))
    }
}

#[test]
#[ignore = "requires dexcli dev"]
fn execute_recovery_contract_preserves_input_type_and_completes() {
    // C21: server-zd does not support Step failure policies.
    if skip_if_server_zd("Step failure policies") {
        return;
    }
    let environment =
        DexDevTestEnvironment::start(Registry::new().register(ExecuteRecoveryWorkflow {
            start: ExecuteRecoveryFailStep,
            finish: ExecuteRecoveryFinishStep,
        }));
    let workflow = ExecuteRecoveryWorkflow {
        start: ExecuteRecoveryFailStep,
        finish: ExecuteRecoveryFinishStep,
    };
    let flow_id = flow_id("go-execute-recovery");
    environment
        .client
        .start_flow(&workflow, &flow_id, "unchanged input".to_string())
        .expect("start Go execute-recovery Flow");
    assert_eq!(
        "this is flow step 2",
        environment
            .client
            .wait_for_flow_with_timeout(&flow_id, Duration::from_secs(30))
            .and_then(|result| result.single_output::<String>())
            .expect("complete Go execute-recovery Flow")
    );
}

#[test]
#[ignore = "requires dexcli dev"]
fn wait_for_failure_reports_handler_message() {
    let environment =
        DexDevTestEnvironment::start(Registry::new().register(WaitForFailureWorkflow {
            start: WaitForFailureStep,
        }));
    let workflow = WaitForFailureWorkflow {
        start: WaitForFailureStep,
    };
    let flow_id = flow_id("wait-for-failure");
    let run_id = environment
        .client
        .start_flow(&workflow, &flow_id, ())
        .expect("start waitFor-failure Flow");
    assert_worker_failure(
        wait_for_failure(&environment, &flow_id),
        &run_id,
        |message| message.contains("test WaitFor failing"),
    );
}

#[test]
#[ignore = "requires dexcli dev"]
fn wait_for_method_timeout_reports_timeout_message() {
    let environment =
        DexDevTestEnvironment::start(Registry::new().register(WaitForTimeoutWorkflow {
            start: WaitForTimeoutStep,
        }));
    let workflow = WaitForTimeoutWorkflow {
        start: WaitForTimeoutStep,
    };
    let flow_id = flow_id("wait-for-timeout");
    let run_id = environment
        .client
        .start_flow(&workflow, &flow_id, ())
        .expect("start waitFor-timeout Flow");
    assert_worker_failure(
        wait_for_failure(&environment, &flow_id),
        &run_id,
        |message| !message.is_empty(),
    );
}

fn wait_for_failure(environment: &DexDevTestEnvironment, flow_id: &str) -> dex_sdk::FlowResult {
    environment
        .client
        .wait_for_flow_with_timeout(flow_id, Duration::from_secs(15))
        .expect("wait for failed Flow result")
}

fn assert_worker_failure(
    failure: dex_sdk::FlowResult,
    _run_id: &str,
    message_matches: impl FnOnce(&str) -> bool,
) {
    assert_eq!(FlowStatus::Failed, failure.status());
    assert_eq!(Some(FlowErrorType::WorkerApiFailed), failure.error_type());
    assert!(failure.error_message().is_some_and(message_matches));
    assert_eq!(0, failure.completions().len());
}
