// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Client write operations: validate, build the command, propose it, and wait for its commit.

mod hard_delete_uploads;
mod request_fingerprint;
mod rpc_invocation;

use dexzd_keyspace::{RunId, TenantId};
use dexzd_protocol::dex::{
    DeleteChannelMessageRequest, PublishToChannelRequest, ResetFlowRequest, ResetFlowResponse,
    SetAttributesRequest, SkipTimerRequest, StartFlowRequest, StartFlowResponse, StopFlowRequest,
    UpdateFlowConfigRequest, WorkerTarget,
};
use dexzd_protocol::dexzd::v1::{
    AttributesSetCommand, ChannelMessageDeleteCommand, ChannelPublishCommand, FlowCommand,
    FlowConfigUpdateCommand, FlowStartCommand, FlowStopCommand, TimeTravelCommand,
    TimerSkipCommand, command_reply, flow_command,
};
use dexzd_request_validation::{ClientRequest, validate_client_request};

use crate::runtime::client_operations::hard_delete_uploads::{
    committed_reply, hard_delete_references, has_hard_delete_values,
};
use crate::runtime::client_operations::request_fingerprint::request_fingerprint;
use crate::runtime::read_operations::state_rows::resolve_command_run;
use crate::runtime::registry::ProposalScope;
use crate::runtime::{
    CommitTicket, CommitUrgency, CommittedCommand, FlowAddress, FlowRuntimeRegistry, RuntimeError,
};

/// Flow timeout policy FAIL, the protocol default for a positive timeout.
const FLOW_TIMEOUT_POLICY_FAIL: i32 = 1;

impl FlowRuntimeRegistry {
    /// Starts a run of a Flow and returns its run ID once the start is committed.
    pub async fn start_flow(
        &self,
        tenant_id: TenantId,
        request: StartFlowRequest,
    ) -> Result<StartFlowResponse, RuntimeError> {
        validate_client_request(
            ClientRequest::StartFlow(&request),
            &self.inner.config.validation,
        )?;
        let flow = FlowAddress::new(tenant_id, request.flow_id.clone());
        let resolved_timeout_policy =
            if request.flow_timeout_seconds > 0 && request.flow_timeout_policy == 0 {
                FLOW_TIMEOUT_POLICY_FAIL
            } else {
                request.flow_timeout_policy
            };
        let new_run_id = RunId(self.inner.dependencies.id_source.run_id().into_bytes());
        let fingerprint = request_fingerprint(&request);
        let mut request = request;
        let uploads = match request.flow_start_options.as_mut() {
            Some(options) => {
                self.upload_hard_delete_values(&flow, new_run_id, &mut options.attributes)
                    .await?
            }
            None => None,
        };
        let start = FlowStartCommand {
            new_run_id: new_run_id.as_bytes().to_vec(),
            request_fingerprint: fingerprint,
            request: Some(request),
            settings: Some(self.inner.config.run_settings),
            resolved_timeout_policy,
            // Validation accepted only pull targets, which normalize to this value.
            worker_target: Some(WorkerTarget {
                is_pull_mode: true,
                ..WorkerTarget::default()
            }),
            hard_delete_objects: hard_delete_references(uploads.as_ref()),
        };
        let command = unstamped_flow_command(&flow, None, flow_command::Command::FlowStart(start));
        let reply = self
            .propose_client_command_with_uploads(&flow, uploads, |_| Ok(command.clone()))
            .await?;
        match reply {
            command_reply::Reply::StartFlow(started) => {
                let run_id = RunId::from_slice(&started.run_id)
                    .map_err(|error| RuntimeError::corrupt_row("start reply", error))?;
                Ok(StartFlowResponse {
                    run_id: run_id.to_string(),
                })
            }
            other => Err(unexpected_reply("StartFlow", &other)),
        }
    }

    /// Stops a run (cancel, fail, or terminate) once the stop is committed.
    pub async fn stop_flow(
        &self,
        tenant_id: TenantId,
        request: StopFlowRequest,
    ) -> Result<(), RuntimeError> {
        if request.flow_id.is_empty() {
            return Err(RuntimeError::invalid_argument("flow ID is required"));
        }
        let flow = FlowAddress::new(tenant_id, request.flow_id.clone());
        self.propose_targeted(&flow, &request.run_id.clone(), |_| {
            flow_command::Command::FlowStop(FlowStopCommand {
                request: Some(request.clone()),
            })
        })
        .await
        .and_then(expect_ok_reply)
    }

    /// Writes Attributes of a run once the write is committed.
    pub async fn set_attributes(
        &self,
        tenant_id: TenantId,
        request: SetAttributesRequest,
    ) -> Result<(), RuntimeError> {
        validate_client_request(
            ClientRequest::SetAttributes(&request),
            &self.inner.config.validation,
        )?;
        let flow = FlowAddress::new(tenant_id, request.flow_id.clone());
        let fingerprint = request_fingerprint(&request);
        if !has_hard_delete_values(&request.attributes) {
            return self
                .propose_targeted(&flow, &request.run_id.clone(), |_| {
                    flow_command::Command::AttributesSet(AttributesSetCommand {
                        request: Some(request.clone()),
                        hard_delete_objects: Vec::new(),
                        request_fingerprint: fingerprint.clone(),
                    })
                })
                .await
                .and_then(expect_ok_reply);
        }
        // The uploaded objects belong to one run, so the command targets the run resolved first.
        let run_id = {
            let state = self.flow_state_reader(&flow, true)?;
            resolve_command_run(
                state.as_ref(),
                self.flow_context(&flow),
                &flow,
                &request.run_id,
            )?
        };
        let requested_run_id = request.run_id.clone();
        let mut request = request;
        let uploads = self
            .upload_hard_delete_values(&flow, run_id, &mut request.attributes)
            .await?;
        let command = unstamped_flow_command(
            &flow,
            Some(run_id),
            flow_command::Command::AttributesSet(AttributesSetCommand {
                hard_delete_objects: hard_delete_references(uploads.as_ref()),
                request_fingerprint: fingerprint,
                request: Some(request),
            }),
        );
        self.propose_client_command_with_uploads(&flow, uploads, |scope| {
            let current =
                resolve_command_run(scope.state, scope.context, &flow, &requested_run_id)?;
            if current != run_id {
                return Err(RuntimeError::Unavailable {
                    detail: format!(
                        "the current run of Flow {} changed while its hard-delete values uploaded",
                        flow.flow_id
                    ),
                });
            }
            Ok(command.clone())
        })
        .await
        .and_then(expect_ok_reply)
    }

    /// Publishes Channel messages once the publish is committed. Every message gets a new
    /// UUIDv7 message ID; an empty list succeeds without checking that the Flow exists.
    pub async fn publish_to_channel(
        &self,
        tenant_id: TenantId,
        mut request: PublishToChannelRequest,
    ) -> Result<(), RuntimeError> {
        validate_client_request(
            ClientRequest::PublishToChannel(&request),
            &self.inner.config.validation,
        )?;
        if request.messages.is_empty() {
            return Ok(());
        }
        for message in &mut request.messages {
            message.message_id = self
                .inner
                .dependencies
                .id_source
                .channel_message_id()
                .to_string();
        }
        let flow = FlowAddress::new(tenant_id, request.flow_id.clone());
        self.propose_targeted(&flow, &request.run_id.clone(), |_| {
            flow_command::Command::ChannelPublish(ChannelPublishCommand {
                request: Some(request.clone()),
            })
        })
        .await
        .and_then(expect_ok_reply)
    }

    /// Deletes one pending Channel message once the delete is committed.
    pub async fn delete_channel_message(
        &self,
        tenant_id: TenantId,
        request: DeleteChannelMessageRequest,
    ) -> Result<(), RuntimeError> {
        validate_client_request(
            ClientRequest::DeleteChannelMessage(&request),
            &self.inner.config.validation,
        )?;
        let flow = FlowAddress::new(tenant_id, request.flow_id.clone());
        let fingerprint = request_fingerprint(&request);
        self.propose_targeted(&flow, &request.run_id.clone(), |_| {
            flow_command::Command::ChannelMessageDelete(ChannelMessageDeleteCommand {
                request: Some(request.clone()),
                request_fingerprint: fingerprint.clone(),
            })
        })
        .await
        .and_then(expect_ok_reply)
    }

    /// Changes the FlowConfig of a run once the change is committed.
    pub async fn update_flow_config(
        &self,
        tenant_id: TenantId,
        request: UpdateFlowConfigRequest,
    ) -> Result<(), RuntimeError> {
        validate_client_request(
            ClientRequest::UpdateFlowConfig(&request),
            &self.inner.config.validation,
        )?;
        let flow = FlowAddress::new(tenant_id, request.flow_id.clone());
        self.propose_targeted(&flow, &request.run_id.clone(), |_| {
            flow_command::Command::FlowConfigUpdate(FlowConfigUpdateCommand {
                request: Some(request.clone()),
            })
        })
        .await
        .and_then(expect_ok_reply)
    }

    /// Fires a pending timer condition early once the skip is committed.
    pub async fn skip_timer(
        &self,
        tenant_id: TenantId,
        request: SkipTimerRequest,
    ) -> Result<(), RuntimeError> {
        validate_client_request(
            ClientRequest::SkipTimer(&request),
            &self.inner.config.validation,
        )?;
        let flow = FlowAddress::new(tenant_id, request.flow_id.clone());
        self.propose_targeted(&flow, &request.run_id.clone(), |_| {
            flow_command::Command::TimerSkip(TimerSkipCommand {
                request: Some(request.clone()),
            })
        })
        .await
        .and_then(expect_ok_reply)
    }

    /// Time-travels a run to an earlier point once the reset is committed. The run ID stays
    /// the same, so the response carries the target run's ID.
    pub async fn reset_flow(
        &self,
        tenant_id: TenantId,
        request: ResetFlowRequest,
    ) -> Result<ResetFlowResponse, RuntimeError> {
        validate_client_request(
            ClientRequest::ResetFlow(&request),
            &self.inner.config.validation,
        )?;
        let flow = FlowAddress::new(tenant_id, request.flow_id.clone());
        let mut target_run_id = None;
        let reply = self
            .propose_client_command(&flow, |scope| {
                let run_id =
                    resolve_command_run(scope.state, scope.context, &flow, &request.run_id)?;
                target_run_id = Some(run_id);
                Ok(unstamped_flow_command(
                    &flow,
                    Some(run_id),
                    flow_command::Command::TimeTravel(TimeTravelCommand {
                        request: Some(request.clone()),
                    }),
                ))
            })
            .await?;
        match (reply, target_run_id) {
            (command_reply::Reply::TimeTravel(time_travel), Some(run_id)) => {
                Ok(ResetFlowResponse {
                    run_id: run_id.to_string(),
                    run_attempt: i32::try_from(time_travel.run_attempt)
                        .map_err(|error| RuntimeError::corrupt_row("time travel reply", error))?,
                })
            }
            (other, _) => Err(unexpected_reply("ResetFlow", &other)),
        }
    }

    /// Proposes a command for the run named by `run_id` (or the newest run) and waits for its
    /// committed reply.
    async fn propose_targeted(
        &self,
        flow: &FlowAddress,
        run_id: &str,
        mut build: impl FnMut(&mut ProposalScope<'_>) -> flow_command::Command,
    ) -> Result<command_reply::Reply, RuntimeError> {
        self.propose_client_command(flow, |scope| {
            let target = resolve_command_run(scope.state, scope.context, flow, run_id)?;
            Ok(unstamped_flow_command(flow, Some(target), build(scope)))
        })
        .await
    }

    /// Proposes a client command promptly and returns its committed reply; an error reply
    /// becomes a [`RuntimeError`].
    pub(crate) async fn propose_client_command(
        &self,
        flow: &FlowAddress,
        build: impl FnMut(&mut ProposalScope<'_>) -> Result<FlowCommand, RuntimeError>,
    ) -> Result<command_reply::Reply, RuntimeError> {
        self.propose_and_wait_for_commit(flow, build)
            .await
            .and_then(committed_reply)
    }

    /// Proposes a client command promptly and returns the committed command.
    async fn propose_and_wait_for_commit(
        &self,
        flow: &FlowAddress,
        mut build: impl FnMut(&mut ProposalScope<'_>) -> Result<FlowCommand, RuntimeError>,
    ) -> Result<CommittedCommand, RuntimeError> {
        let proposal = self
            .propose_with(flow, CommitUrgency::Prompt, |scope| {
                build(scope).map(|command| Some((command, ())))
            })
            .await?;
        let (result, ()) = proposal.expect("client builders always propose");
        self.wait_for_commit(flow, result.ticket).await
    }

    /// Waits for a proposed command's commit.
    ///
    /// A failed commit halts the registry: later commands, possibly already queued, were built
    /// on the failed command's speculative outcome (plan 8.2). Returns the committed command,
    /// the commit error after halting, or `NodeStopping` when the registry halts while waiting.
    pub async fn wait_for_commit(
        &self,
        flow: &FlowAddress,
        ticket: CommitTicket,
    ) -> Result<CommittedCommand, RuntimeError> {
        let halted = self.halted();
        let result = tokio::select! {
            // The failure observer halts before the ticket resolves; report the commit error.
            biased;
            result = ticket.wait() => result,
            () = halted.cancelled() => {
                return Err(self.check_running().expect_err("halted"));
            }
        };
        result.map_err(|error| {
            self.halt(format!(
                "commit of a command of Flow {} failed: {error}",
                flow.flow_id
            ));
            RuntimeError::from(error)
        })
    }
}

/// Returns an unstamped command; the committer fills epoch, behavior version, time, and ID.
pub(crate) fn unstamped_flow_command(
    flow: &FlowAddress,
    run_id: Option<RunId>,
    command: flow_command::Command,
) -> FlowCommand {
    FlowCommand {
        flow_id: flow.flow_id.clone(),
        run_id: run_id
            .map(|run_id| run_id.as_bytes().to_vec())
            .unwrap_or_default(),
        tenant_id: flow.tenant_id.0,
        command: Some(command),
        ..FlowCommand::default()
    }
}

fn expect_ok_reply(reply: command_reply::Reply) -> Result<(), RuntimeError> {
    match reply {
        command_reply::Reply::Ok(_) => Ok(()),
        other => Err(unexpected_reply("a write", &other)),
    }
}

fn unexpected_reply(operation: &str, reply: &command_reply::Reply) -> RuntimeError {
    RuntimeError::Internal {
        detail: format!("apply returned an unexpected reply for {operation}: {reply:?}"),
    }
}
