// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Builds the Worker request of a running attempt from the snapshot that apply stored in its
//! Step execution, with the same fields the Go Server sends.

use std::collections::BTreeMap;

use dexzd_apply::FlowStateReader;
use dexzd_keyspace::row_keys::value_row_key;
use dexzd_keyspace::{ColumnFamily, RunId, RunTable, ValueId, ValueReference};
use dexzd_protocol::dex::{
    self, ChannelMessage, ChannelResult, ChannelValues, ConditionResults, Context, FlowResult,
    InvokeExecuteMethodRequest, InvokeWaitForMethodRequest, Kv, StepCompletionOutput, Value, value,
};
use dexzd_protocol::dexzd::v1::{
    ChannelMessageRecord, ConditionResultsRecord, HardDeleteObjectReference, RunRecord,
    StepExecutionRecord, StepInvocationKey, StepInvocationPhase, StepInvocationRecord, StepMethod,
    StoredFlowResult, StoredKeyValue, StoredValue, ValueRecord, stored_value,
};

use crate::effects::EffectError;
use crate::effects::state_reads::{RunLocation, decode_row, read_run_record, read_step_execution};

const MICROS_PER_SECOND: i64 = 1_000_000;

/// The request a Worker receives for one method attempt.
#[derive(Debug, Clone, PartialEq)]
pub enum MethodRequest {
    /// A WaitFor attempt.
    WaitFor(InvokeWaitForMethodRequest),
    /// An Execute attempt, including the Flow timeout handler.
    Execute(InvokeExecuteMethodRequest),
}

impl MethodRequest {
    /// Returns the encoded size of the request.
    pub fn encoded_len(&self) -> usize {
        use prost::Message;
        match self {
            Self::WaitFor(request) => request.encoded_len(),
            Self::Execute(request) => request.encoded_len(),
        }
    }
}

/// Committed or speculative state of one running attempt.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MethodAttemptState {
    pub(crate) run_record: RunRecord,
    pub(crate) step_execution: StepExecutionRecord,
}

impl MethodAttemptState {
    /// Returns the running invocation; reading the state checked that it exists.
    pub(crate) fn invocation(&self) -> &StepInvocationRecord {
        self.step_execution
            .invocation
            .as_ref()
            .expect("read_method_attempt_state returns only states with an invocation")
    }

    /// Returns the hard-delete Attribute objects of the snapshot, keyed by Attribute key.
    pub(crate) fn hard_delete_attributes(&self) -> BTreeMap<String, HardDeleteObjectReference> {
        self.invocation()
            .snapshot
            .iter()
            .flat_map(|snapshot| &snapshot.attributes)
            .filter_map(|attribute| match stored_kind(attribute.value.as_ref()) {
                Some(stored_value::Kind::HardDeleteObject(object)) => {
                    Some((attribute.key.clone(), object.clone()))
                }
                _ => None,
            })
            .collect()
    }
}

/// Identity of one dispatched attempt.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct MethodAttemptKey {
    pub(crate) run_id: RunId,
    pub(crate) run_attempt: u32,
    pub(crate) step_execution_id: String,
    pub(crate) method: i32,
    pub(crate) attempt: u32,
}

impl MethodAttemptKey {
    pub(crate) fn new(
        run_id: RunId,
        run_attempt: u32,
        invocation: &StepInvocationKey,
        attempt: u32,
    ) -> Self {
        Self {
            run_id,
            run_attempt,
            step_execution_id: invocation.step_execution_id.clone(),
            method: invocation.method,
            attempt,
        }
    }

    pub(crate) fn invocation(&self) -> StepInvocationKey {
        StepInvocationKey {
            step_execution_id: self.step_execution_id.clone(),
            method: self.method,
        }
    }
}

/// Reads the state of a running attempt, or `None` when the attempt is no longer running with
/// this dispatch ID (it finished, was canceled, or time travel replaced the run attempt).
///
/// With `is_hydrating`, value-table references that the Worker request reads become inline values
/// from the same reader.
///
/// Returns [`EffectError::InvariantViolation`] when such a held value has no value row.
pub(crate) fn read_method_attempt_state(
    reader: &dyn FlowStateReader,
    run: RunLocation,
    attempt_key: &MethodAttemptKey,
    task_id: &[u8],
    is_hydrating: bool,
) -> Result<Option<MethodAttemptState>, EffectError> {
    let Some(run_record) = read_run_record(reader, run)? else {
        return Ok(None);
    };
    if run_record.current_attempt != attempt_key.run_attempt {
        return Ok(None);
    }
    let Some(mut step_execution) =
        read_step_execution(reader, run, &attempt_key.step_execution_id)?
    else {
        return Ok(None);
    };
    let is_running = step_execution
        .invocation
        .as_ref()
        .is_some_and(|invocation| {
            invocation.method == attempt_key.method
                && invocation.phase == StepInvocationPhase::Running as i32
                && invocation.attempt == attempt_key.attempt
                && invocation.task_id == task_id
        });
    if !is_running {
        return Ok(None);
    }
    if is_hydrating {
        load_request_values(reader, run, &mut step_execution)?;
    }
    Ok(Some(MethodAttemptState {
        run_record,
        step_execution,
    }))
}

/// Replaces the value-table references that [`build_method_request`] reads with inline values.
fn load_request_values(
    reader: &dyn FlowStateReader,
    run: RunLocation,
    step_execution: &mut StepExecutionRecord,
) -> Result<(), EffectError> {
    let mut stored_values: Vec<&mut StoredValue> = Vec::new();
    stored_values.extend(step_execution.input.as_mut());
    stored_values.extend(stored_key_values(&mut step_execution.locals));
    if let Some(results) = step_execution.condition_results.as_mut() {
        for channel_result in &mut results.channel_results {
            stored_values.extend(stored_message_values(&mut channel_result.messages));
        }
        for sub_flow_result in &mut results.sub_flow_results {
            stored_values.extend(
                sub_flow_result
                    .results
                    .iter_mut()
                    .filter_map(|output| output.completed_step_output.as_mut()),
            );
        }
    }
    if let Some(invocation) = step_execution.invocation.as_mut() {
        stored_values.extend(invocation.heartbeat_checkpoint.as_mut());
        if let Some(snapshot) = invocation.snapshot.as_mut() {
            stored_values.extend(stored_key_values(&mut snapshot.attributes));
            for loaded in &mut snapshot.loaded_channel_messages {
                stored_values.extend(stored_message_values(&mut loaded.messages));
            }
        }
    }
    for stored in stored_values {
        let Some(stored_value::Kind::TableValue(table_value)) = stored.kind.as_ref() else {
            continue;
        };
        let value_id = table_value
            .value_id
            .as_ref()
            .ok_or_else(|| EffectError::CorruptState {
                table: "StoredValue",
                key: run.run_id.to_string(),
                detail: "value-table reference has no value ID".to_owned(),
            })?;
        let key = value_row_key(
            run.flow_hash,
            run.run_id,
            RunTable::Value,
            ValueId {
                run_seq: value_id.run_seq,
                index_in_command: value_id.index_in_command,
            },
        );
        let bytes = reader.get(ColumnFamily::Value, &key)?.ok_or_else(|| {
            EffectError::InvariantViolation {
                detail: format!(
                    "held value {}-{} of run {} has no value row",
                    value_id.run_seq, value_id.index_in_command, run.run_id
                ),
            }
        })?;
        let record: ValueRecord = decode_row("Value", &key, &bytes)?;
        let value = record.value.ok_or_else(|| EffectError::CorruptState {
            table: "Value",
            key: run.run_id.to_string(),
            detail: "value row has no value".to_owned(),
        })?;
        stored.kind = Some(stored_value::Kind::InlineValue(value));
    }
    Ok(())
}

fn stored_key_values(key_values: &mut [StoredKeyValue]) -> impl Iterator<Item = &mut StoredValue> {
    key_values
        .iter_mut()
        .filter_map(|key_value| key_value.value.as_mut())
}

fn stored_message_values(
    messages: &mut [ChannelMessageRecord],
) -> impl Iterator<Item = &mut StoredValue> {
    messages
        .iter_mut()
        .filter_map(|message| message.value.as_mut())
}

/// Builds the Worker request of the attempt.
///
/// `hard_delete_values` holds the current content of each hard-delete Attribute of the
/// snapshot, keyed by Attribute key; a missing key omits the Attribute, as the Go Server does
/// for a deleted object. Returns [`EffectError::CorruptState`] when the stored snapshot is
/// missing or has a value kind apply never writes there.
pub(crate) fn build_method_request(
    state: &MethodAttemptState,
    hard_delete_values: &BTreeMap<String, Value>,
) -> Result<MethodRequest, EffectError> {
    let run_record = &state.run_record;
    let step_execution = &state.step_execution;
    let invocation = state.invocation();
    let snapshot = invocation
        .snapshot
        .as_ref()
        .ok_or_else(|| corrupt_step(step_execution, "running attempt has no snapshot"))?;
    let run_id = RunId::from_slice(&run_record.run_id)
        .map_err(|error| corrupt_step(step_execution, &error.to_string()))?;
    let values = WireValues { run_id };

    let context = Context {
        flow_id: run_record.flow_id.clone(),
        run_id: run_id.to_string(),
        flow_started_timestamp: run_record.started_at_micros.div_euclid(MICROS_PER_SECOND),
        step_execution_id: step_execution.step_execution_id.clone(),
        first_attempt_timestamp: invocation
            .first_attempt_at_micros
            .div_euclid(MICROS_PER_SECOND),
        attempt: i32::try_from(invocation.attempt)
            .map_err(|_| corrupt_step(step_execution, "attempt exceeds i32"))?,
        from_step_execution_id: step_execution.from_step_execution_id.clone(),
        recovery_error: None,
        last_heartbeat_value: values.optional(invocation.heartbeat_checkpoint.as_ref())?,
        run_attempt: i32::try_from(run_record.current_attempt)
            .map_err(|_| corrupt_step(step_execution, "run attempt exceeds i32"))?,
    };
    let mut attributes = Vec::with_capacity(snapshot.attributes.len());
    for attribute in &snapshot.attributes {
        if let Some(stored_value::Kind::HardDeleteObject(_)) = stored_kind(attribute.value.as_ref())
        {
            if let Some(value) = hard_delete_values.get(&attribute.key) {
                attributes.push(Kv {
                    key: attribute.key.clone(),
                    value: Some(value.clone()),
                });
            }
            continue;
        }
        attributes.push(values.key_value(attribute)?);
    }
    let loaded_channel_messages = snapshot
        .loaded_channel_messages
        .iter()
        .map(|loaded| {
            let messages = loaded
                .messages
                .iter()
                .map(|message| values.channel_message(message))
                .collect::<Result<Vec<_>, _>>()?;
            Ok((loaded.channel_name.clone(), ChannelValues { messages }))
        })
        .collect::<Result<BTreeMap<_, _>, EffectError>>()?;
    let step_input = values.optional(step_execution.input.as_ref())?;

    if invocation.method == StepMethod::WaitFor as i32 {
        return Ok(MethodRequest::WaitFor(InvokeWaitForMethodRequest {
            context: Some(context),
            flow_type: run_record.flow_type.clone(),
            step_type: step_execution.step_type.clone(),
            step_input,
            attributes,
            channel_infos: snapshot.channel_infos.clone(),
            loaded_channel_messages,
            loaded_attribute_map_instances: snapshot.loaded_attribute_map_instances.clone(),
            loaded_channel_names: snapshot.loaded_channel_names.clone(),
            loaded_channel_map_instances: snapshot.loaded_channel_map_instances.clone(),
        }));
    }
    if invocation.method != StepMethod::Execute as i32 {
        return Err(corrupt_step(step_execution, "invocation has no method"));
    }
    let step_exe_locals = step_execution
        .locals
        .iter()
        .map(|local| values.key_value(local))
        .collect::<Result<Vec<_>, _>>()?;
    let condition_results = step_execution
        .condition_results
        .as_ref()
        .map(|results| values.condition_results(results))
        .transpose()?;
    Ok(MethodRequest::Execute(InvokeExecuteMethodRequest {
        context: Some(context),
        flow_type: run_record.flow_type.clone(),
        step_type: step_execution.step_type.clone(),
        step_input,
        attributes,
        step_exe_locals,
        condition_results,
        channel_infos: snapshot.channel_infos.clone(),
        loaded_channel_messages,
        loaded_attribute_map_instances: snapshot.loaded_attribute_map_instances.clone(),
        loaded_channel_names: snapshot.loaded_channel_names.clone(),
        loaded_channel_map_instances: snapshot.loaded_channel_map_instances.clone(),
    }))
}

/// Converts stored values of one run to wire values: inline values as they are, value-table
/// rows as server-zd value references.
struct WireValues {
    run_id: RunId,
}

impl WireValues {
    fn optional(&self, stored: Option<&StoredValue>) -> Result<Option<Value>, EffectError> {
        match stored_kind(stored) {
            None => Ok(None),
            Some(stored_value::Kind::InlineValue(value)) => Ok(Some(value.clone())),
            Some(stored_value::Kind::TableValue(table_value)) => {
                let value_id =
                    table_value
                        .value_id
                        .as_ref()
                        .ok_or_else(|| EffectError::CorruptState {
                            table: "StoredValue",
                            key: self.run_id.to_string(),
                            detail: "value-table reference has no value ID".to_owned(),
                        })?;
                let reference = ValueReference {
                    run_id: self.run_id,
                    value_id: ValueId {
                        run_seq: value_id.run_seq,
                        index_in_command: value_id.index_in_command,
                    },
                }
                .to_string();
                let kind = if table_value.is_string_value {
                    value::Kind::InternalBlobIdForStringValue(reference)
                } else {
                    value::Kind::InternalBlobIdForObjValue(reference)
                };
                Ok(Some(Value { kind: Some(kind) }))
            }
            Some(stored_value::Kind::HardDeleteObject(_)) => Err(EffectError::CorruptState {
                table: "StoredValue",
                key: self.run_id.to_string(),
                detail: "hard-delete object outside an Attribute".to_owned(),
            }),
        }
    }

    fn key_value(&self, stored: &StoredKeyValue) -> Result<Kv, EffectError> {
        Ok(Kv {
            key: stored.key.clone(),
            value: self.optional(stored.value.as_ref())?,
        })
    }

    fn channel_message(
        &self,
        message: &ChannelMessageRecord,
    ) -> Result<ChannelMessage, EffectError> {
        Ok(ChannelMessage {
            channel_name: message.channel_name.clone(),
            value: self.optional(message.value.as_ref())?,
            message_id: message.message_id.clone(),
        })
    }

    fn condition_results(
        &self,
        results: &ConditionResultsRecord,
    ) -> Result<ConditionResults, EffectError> {
        let channel_results = results
            .channel_results
            .iter()
            .map(|result| {
                let values = result
                    .messages
                    .iter()
                    .map(|message| Ok(self.optional(message.value.as_ref())?.unwrap_or_default()))
                    .collect::<Result<Vec<_>, EffectError>>()?;
                Ok(ChannelResult {
                    condition_id: result.condition_id.clone(),
                    condition_status: result.condition_status,
                    channel_name: result.channel_name.clone(),
                    values,
                })
            })
            .collect::<Result<Vec<_>, EffectError>>()?;
        let sub_flow_results = results
            .sub_flow_results
            .iter()
            .map(|result| self.flow_result(result))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ConditionResults {
            channel_results,
            timer_results: results.timer_results.clone(),
            wait_for_failed: results.wait_for_failed,
            sub_flow_results,
        })
    }

    fn flow_result(&self, result: &StoredFlowResult) -> Result<FlowResult, EffectError> {
        let results = result
            .results
            .iter()
            .map(|output| {
                Ok(StepCompletionOutput {
                    completed_step_type: output.completed_step_type.clone(),
                    completed_step_execution_id: output.completed_step_execution_id.clone(),
                    completed_step_output: self.optional(output.completed_step_output.as_ref())?,
                })
            })
            .collect::<Result<Vec<_>, EffectError>>()?;
        Ok(FlowResult {
            flow_status: result.flow_status,
            results,
            error_type: result.error_type,
            error_message: result.error_message.clone(),
        })
    }
}

/// Returns the attempt's current heartbeat checkpoint as the Worker receives it.
pub(crate) fn heartbeat_checkpoint_value(state: &MethodAttemptState) -> Option<Value> {
    let run_id = RunId::from_slice(&state.run_record.run_id).ok()?;
    WireValues { run_id }
        .optional(state.invocation().heartbeat_checkpoint.as_ref())
        .ok()
        .flatten()
}

fn stored_kind(stored: Option<&StoredValue>) -> Option<&stored_value::Kind> {
    stored.and_then(|stored| stored.kind.as_ref())
}

fn corrupt_step(step_execution: &StepExecutionRecord, detail: &str) -> EffectError {
    EffectError::CorruptState {
        table: "StepExecution",
        key: step_execution.step_execution_id.clone(),
        detail: detail.to_owned(),
    }
}

/// Returns the Step durability that applies to the invocation's method.
pub(crate) fn resolve_method_durability(
    run_record: &RunRecord,
    step_execution: &StepExecutionRecord,
    method: i32,
) -> dex::StepDurability {
    let options = step_execution.options.as_ref();
    let override_durability = if method == StepMethod::WaitFor as i32 {
        options.map(|options| options.wait_for_durability_override)
    } else {
        options.map(|options| options.execute_durability_override)
    };
    let flow_durability = run_record
        .flow_config
        .as_ref()
        .and_then(|config| config.step_durability);
    [override_durability, flow_durability]
        .into_iter()
        .flatten()
        .find_map(|durability| {
            dex::StepDurability::try_from(durability)
                .ok()
                .filter(|durability| *durability != dex::StepDurability::Unspecified)
        })
        .unwrap_or(dex::StepDurability::Sync)
}

/// Returns the configured timeout in seconds of `method`, or 0 when the Step options leave it
/// to the default; apply reports the same value when the attempt starts.
pub(crate) fn configured_method_timeout_seconds(
    options: Option<&dex::StepOptions>,
    method: i32,
) -> i32 {
    options.map_or(0, |options| {
        if method == StepMethod::WaitFor as i32 {
            options.wait_for_timeout_seconds
        } else {
            options.execute_timeout_seconds
        }
    })
}
