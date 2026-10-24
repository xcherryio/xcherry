// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_apply::{ApplyOutcome, StateWrite};
use dexzd_keyspace::{ColumnFamily, RunId, TenantId};
use dexzd_protocol::dex::{
    AttributeWrite, FlowConfig, FlowStartOptions, SetAttributesRequest, StartFlowRequest, Value,
    WorkerTarget, value,
};
use dexzd_protocol::dexzd::v1::{
    AttributesSetCommand, FlowCommand, StepAttemptStartedCommand, StepInvocationKey, StepMethod,
    flow_command,
};

use crate::runtime::tests::model_applier::ok_reply;
use crate::runtime::tests::test_support::RuntimeHarness;
use crate::runtime::{FlowAddress, RuntimeError};

pub(crate) const TENANT: TenantId = TenantId::DEFAULT;

pub(crate) fn flow(flow_id: &str) -> FlowAddress {
    FlowAddress::new(TENANT, flow_id)
}

pub(crate) fn start_request(flow_id: &str) -> StartFlowRequest {
    StartFlowRequest {
        flow_id: flow_id.to_owned(),
        flow_type: "OrderFlow".to_owned(),
        start_step_type: "InitStep".to_owned(),
        request_id: format!("start-{flow_id}"),
        flow_start_options: Some(FlowStartOptions {
            flow_config_override: Some(FlowConfig {
                worker_target: Some(WorkerTarget {
                    is_pull_mode: true,
                    ..WorkerTarget::default()
                }),
                ..FlowConfig::default()
            }),
            ..FlowStartOptions::default()
        }),
        ..StartFlowRequest::default()
    }
}

pub(crate) fn string_value(text: &str) -> Value {
    Value {
        kind: Some(value::Kind::StringValue(text.to_owned())),
    }
}

pub(crate) fn int_value(number: i64) -> Value {
    Value {
        kind: Some(value::Kind::IntValue(number)),
    }
}

pub(crate) fn set_attribute_request(
    flow_id: &str,
    key: &str,
    value: Value,
) -> SetAttributesRequest {
    SetAttributesRequest {
        flow_id: flow_id.to_owned(),
        attributes: vec![AttributeWrite {
            key: key.to_owned(),
            value: Some(value),
            ..AttributeWrite::default()
        }],
        request_id: format!("set-{key}"),
        ..SetAttributesRequest::default()
    }
}

/// An unstamped AttributesSet command for `run_id`.
pub(crate) fn attributes_set_command(
    flow_id: &str,
    run_id: RunId,
    key: &str,
    value: Value,
) -> FlowCommand {
    FlowCommand {
        flow_id: flow_id.to_owned(),
        run_id: run_id.as_bytes().to_vec(),
        command: Some(flow_command::Command::AttributesSet(AttributesSetCommand {
            request: Some(set_attribute_request(flow_id, key, value)),
            ..AttributesSetCommand::default()
        })),
        ..FlowCommand::default()
    }
}

/// A StepAttemptStarted whose model outcome locks `lock_key`.
pub(crate) fn locking_attempt_command(flow_id: &str, run_id: RunId, lock_key: &str) -> FlowCommand {
    FlowCommand {
        flow_id: flow_id.to_owned(),
        run_id: run_id.as_bytes().to_vec(),
        command: Some(flow_command::Command::StepAttemptStarted(
            StepAttemptStartedCommand {
                invocation: Some(locking_invocation(lock_key)),
                attempt: 1,
                task_id: vec![1; 16],
                pull_routing: None,
            },
        )),
        ..FlowCommand::default()
    }
}

pub(crate) fn locking_invocation(lock_key: &str) -> StepInvocationKey {
    StepInvocationKey {
        step_execution_id: format!("Lock-{lock_key}"),
        method: StepMethod::Execute as i32,
    }
}

/// Starts a Flow through the registry and commits it; returns its run ID.
pub(crate) async fn start_flow(
    harness: &RuntimeHarness,
    flow_id: &str,
) -> Result<RunId, RuntimeError> {
    let response = harness
        .drive(harness.registry.start_flow(TENANT, start_request(flow_id)))
        .await?;
    Ok(response.run_id.parse().unwrap())
}

/// Size of the value that [`large_outcome`] writes.
pub(crate) const LARGE_OUTCOME_VALUE_BYTES: usize = 64 * 1024;

/// An outcome far larger than the command that produced it.
pub(crate) fn large_outcome(run_id: RunId) -> ApplyOutcome {
    ApplyOutcome {
        writes: vec![StateWrite::Put {
            column_family: ColumnFamily::Flow,
            key: b"large-outcome".to_vec(),
            value: vec![0; LARGE_OUTCOME_VALUE_BYTES],
        }],
        effects: Vec::new(),
        reply: ok_reply(),
        run_id: Some(run_id),
    }
}
