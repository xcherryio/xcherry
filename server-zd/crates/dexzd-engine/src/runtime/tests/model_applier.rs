// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! A small deterministic stand-in for apply that writes real state rows for a few commands.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use dexzd_apply::{
    ApplyContext, ApplyEffect, ApplyError, ApplyOutcome, FlowStateReader, StateWrite,
    WaitableChange,
};
use dexzd_keyspace::row_keys::channel_message_key;
use dexzd_keyspace::{ColumnFamily, RunId, RunTable, TenantId, flow_directory_key, run_key};
use dexzd_protocol::dex::{ErrorSubStatus, FlowStatus, StopType};
use dexzd_protocol::dexzd::v1::{
    AttributeRecord, ChannelMessageRecord, ChannelMetaRecord, CommandErrorReply, CommandReply,
    EmptyReply, FlowCommand, FlowDirectoryRecord, FlowDirectoryRun, LockRecord, RpcReply,
    RunRecord, StartFlowReply, StepInvocationKey, StoredValue, TimeTravelReply, command_reply,
    flow_command, stored_value,
};
use prost::Message;

use crate::CommandApplier;

/// Scripted result for the next apply call; `None` falls back to the model.
pub(crate) type ScriptedApply = Result<ApplyOutcome, ApplyError>;

/// The model: FlowStart, FlowStop, AttributesSet, ChannelPublish, RpcCommitted, and
/// StepAttemptStarted (a `Lock-<key>` Step execution ID locks `<key>`). Other commands reply OK.
#[derive(Default)]
pub(crate) struct ModelApplier {
    pub(crate) scripted: Mutex<VecDeque<ScriptedApply>>,
    pub(crate) applied: Mutex<Vec<FlowCommand>>,
    /// Runs at the start of every apply call.
    pub(crate) apply_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    /// Applies unscripted commands with the real apply instead of the model.
    pub(crate) uses_real_apply: AtomicBool,
}

impl CommandApplier for ModelApplier {
    fn apply(
        &self,
        context: &ApplyContext,
        state: &dyn FlowStateReader,
        command: &FlowCommand,
    ) -> Result<ApplyOutcome, ApplyError> {
        let hook = self.apply_hook.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook();
        }
        self.applied.lock().unwrap().push(command.clone());
        if let Some(scripted) = self.scripted.lock().unwrap().pop_front() {
            return scripted;
        }
        if self.uses_real_apply.load(Ordering::SeqCst) {
            return dexzd_apply::apply_flow_command(context, state, command);
        }
        Ok(apply_model(context, state, command))
    }
}

fn apply_model(
    context: &ApplyContext,
    state: &dyn FlowStateReader,
    command: &FlowCommand,
) -> ApplyOutcome {
    let flow_hash = context.flow_hash;
    let tenant_id = TenantId(command.tenant_id);
    let Some(body) = command.command.as_ref() else {
        return ApplyOutcome::rejected(ok_reply());
    };
    if let flow_command::Command::FlowStart(start) = body {
        let run_id = RunId::from_slice(&start.new_run_id).unwrap();
        let request = start.request.clone().unwrap_or_default();
        let directory_key = flow_directory_key(flow_hash, tenant_id, &command.flow_id);
        let mut directory: FlowDirectoryRecord =
            decode(state, ColumnFamily::Flow, &directory_key).unwrap_or_default();
        directory.runs.push(FlowDirectoryRun {
            run_id: run_id.as_bytes().to_vec(),
            status: FlowStatus::Running as i32,
            started_at_micros: command.proposed_at_micros,
            start_request_id: request.request_id.clone(),
            ..FlowDirectoryRun::default()
        });
        let record = RunRecord {
            tenant_id: tenant_id.0,
            flow_id: command.flow_id.clone(),
            run_id: run_id.as_bytes().to_vec(),
            flow_type: request.flow_type.clone(),
            start_request_id: request.request_id,
            started_at_micros: command.proposed_at_micros,
            settings: start.settings,
            status: FlowStatus::Running as i32,
            current_attempt: 1,
            next_run_seq: 2,
            next_event_id: 2,
            ..RunRecord::default()
        };
        return ApplyOutcome {
            writes: vec![
                put(ColumnFamily::Flow, directory_key, &directory),
                put(
                    ColumnFamily::Flow,
                    run_key(flow_hash, run_id, RunTable::RunRecord, &[]),
                    &record,
                ),
            ],
            effects: vec![ApplyEffect::WakeWaiters {
                run_id,
                change: WaitableChange::HistoryAppended { last_event_id: 1 },
            }],
            reply: CommandReply {
                reply: Some(command_reply::Reply::StartFlow(StartFlowReply {
                    run_id: run_id.as_bytes().to_vec(),
                    is_existing_run: false,
                })),
            },
            run_id: Some(run_id),
        };
    }
    let Ok(run_id) = RunId::from_slice(&command.run_id) else {
        return ApplyOutcome::rejected(not_found_reply(&command.flow_id));
    };
    let record_key = run_key(flow_hash, run_id, RunTable::RunRecord, &[]);
    let Some(mut record) = decode::<RunRecord>(state, ColumnFamily::Flow, &record_key) else {
        return ApplyOutcome::rejected(not_found_reply(&command.flow_id));
    };
    let mut writes = Vec::new();
    let mut effects = Vec::new();
    let mut reply = ok_reply();
    match body {
        flow_command::Command::FlowStop(stop) => {
            let stop_type = stop.request.as_ref().map_or(0, |request| request.stop_type);
            record.status = match StopType::try_from(stop_type) {
                Ok(StopType::Terminate) => FlowStatus::Terminated,
                Ok(StopType::Fail) => FlowStatus::Failed,
                _ => FlowStatus::Canceled,
            } as i32;
            record.closed_at_micros = command.proposed_at_micros;
            record.next_event_id += 1;
            writes.push(put(ColumnFamily::Flow, record_key, &record));
            effects.push(ApplyEffect::WakeWaiters {
                run_id,
                change: WaitableChange::FlowClosed,
            });
        }
        flow_command::Command::AttributesSet(set) => {
            let request = set.request.clone().unwrap_or_default();
            write_attributes(
                flow_hash,
                run_id,
                &request.attributes,
                &mut writes,
                &mut effects,
            );
        }
        flow_command::Command::ChannelPublish(publish) => {
            let request = publish.request.clone().unwrap_or_default();
            for (index, message) in request.messages.iter().enumerate() {
                let seq = u64::try_from(command.proposed_at_micros).unwrap() + index as u64;
                writes.push(put(
                    ColumnFamily::Flow,
                    channel_message_key(flow_hash, run_id, &message.channel_name, seq),
                    &ChannelMessageRecord {
                        channel_name: message.channel_name.clone(),
                        message_id: message.message_id.clone(),
                        value: message.value.clone().map(inline),
                        enqueued_seq: seq,
                    },
                ));
                let meta_key = run_key(
                    flow_hash,
                    run_id,
                    RunTable::ChannelMeta,
                    message.channel_name.as_bytes(),
                );
                let mut meta: ChannelMetaRecord =
                    decode(state, ColumnFamily::Flow, &meta_key).unwrap_or_default();
                meta.pending_count += 1;
                writes.push(put(ColumnFamily::Flow, meta_key, &meta));
            }
        }
        flow_command::Command::RpcCommitted(rpc) => {
            let response = rpc.response.clone().unwrap_or_default();
            write_attributes(
                flow_hash,
                run_id,
                &response.upsert_attributes,
                &mut writes,
                &mut effects,
            );
            reply = CommandReply {
                reply: Some(command_reply::Reply::Rpc(RpcReply {
                    output: response.output.map(inline),
                })),
            };
        }
        flow_command::Command::StepAttemptStarted(started) => {
            let invocation = started.invocation.clone().unwrap_or_default();
            if let Some(lock_key) = invocation.step_execution_id.strip_prefix("Lock-") {
                let lock_record = LockRecord {
                    holder: Some(StepInvocationKey {
                        step_execution_id: invocation.step_execution_id.clone(),
                        method: invocation.method,
                    }),
                    holder_attempt: started.attempt,
                    waiters: Vec::new(),
                };
                writes.push(put(
                    ColumnFamily::Flow,
                    run_key(flow_hash, run_id, RunTable::LockTable, lock_key.as_bytes()),
                    &lock_record,
                ));
            }
        }
        flow_command::Command::TimeTravel(_) => {
            record.current_attempt += 1;
            writes.push(put(ColumnFamily::Flow, record_key, &record));
            reply = CommandReply {
                reply: Some(command_reply::Reply::TimeTravel(TimeTravelReply {
                    run_attempt: record.current_attempt,
                })),
            };
        }
        _ => {}
    }
    ApplyOutcome {
        writes,
        effects,
        reply,
        run_id: Some(run_id),
    }
}

fn write_attributes(
    flow_hash: dexzd_keyspace::FlowHash,
    run_id: RunId,
    attributes: &[dexzd_protocol::dex::AttributeWrite],
    writes: &mut Vec<StateWrite>,
    effects: &mut Vec<ApplyEffect>,
) {
    let mut keys = Vec::new();
    for attribute in attributes {
        let key = run_key(
            flow_hash,
            run_id,
            RunTable::Attribute,
            attribute.key.as_bytes(),
        );
        match attribute.value.clone() {
            Some(value) => writes.push(put(
                ColumnFamily::Flow,
                key,
                &AttributeRecord {
                    value: Some(inline(value)),
                    ..AttributeRecord::default()
                },
            )),
            None => writes.push(StateWrite::Delete {
                column_family: ColumnFamily::Flow,
                key,
            }),
        }
        keys.push(attribute.key.clone());
    }
    keys.sort();
    keys.dedup();
    effects.push(ApplyEffect::WakeWaiters {
        run_id,
        change: WaitableChange::AttributesChanged { keys },
    });
}

pub(crate) fn inline(value: dexzd_protocol::dex::Value) -> StoredValue {
    StoredValue {
        kind: Some(stored_value::Kind::InlineValue(value)),
    }
}

fn decode<RowMessage: Message + Default>(
    state: &dyn FlowStateReader,
    column_family: ColumnFamily,
    key: &[u8],
) -> Option<RowMessage> {
    let bytes = state.get(column_family, key).unwrap()?;
    Some(RowMessage::decode(bytes.as_slice()).unwrap())
}

fn put(column_family: ColumnFamily, key: Vec<u8>, row: &impl Message) -> StateWrite {
    StateWrite::Put {
        column_family,
        key,
        value: row.encode_to_vec(),
    }
}

pub(crate) fn ok_reply() -> CommandReply {
    CommandReply {
        reply: Some(command_reply::Reply::Ok(EmptyReply {})),
    }
}

pub(crate) fn not_found_reply(flow_id: &str) -> CommandReply {
    CommandReply {
        reply: Some(command_reply::Reply::Error(CommandErrorReply {
            grpc_code: 5,
            sub_status: ErrorSubStatus::FlowNotExists as i32,
            message: format!("flow {flow_id} does not exist"),
        })),
    }
}
