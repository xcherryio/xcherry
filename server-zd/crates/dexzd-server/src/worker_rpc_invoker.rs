// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::time::SystemTime;

use dexzd_engine::{RpcInvokeError, RpcWorkerInvoker, WorkerRpcInvocation, WorkerRpcResult};
use dexzd_protocol::dex::{
    DispatchTaskErrorReason, PullRouting, PullRoutingSource, WorkerErrorResponse,
    poll_worker_task_frame,
};
use dexzd_worker_dispatch::{
    DispatchEvent, DispatchOutcome, DispatchRequest, DispatchTaskPayload, WorkerDispatch,
};
use futures::future::BoxFuture;

/// Sends Flow RPCs to pull Workers through the node's matching owner.
pub(crate) struct PullWorkerRpcInvoker {
    dispatch: WorkerDispatch,
}

impl PullWorkerRpcInvoker {
    pub(crate) fn new(dispatch: WorkerDispatch) -> Self {
        Self { dispatch }
    }

    async fn invoke(
        &self,
        invocation: WorkerRpcInvocation,
    ) -> Result<WorkerRpcResult, RpcInvokeError> {
        let remaining = invocation
            .deadline
            .saturating_duration_since(tokio::time::Instant::now());
        let sticky_app_worker_id = invocation.sticky_app_worker_id;
        let mut handle = self
            .dispatch
            .dispatch(DispatchRequest {
                tenant_id: invocation.flow.tenant_id.0,
                flow_id: invocation.flow.flow_id,
                flow_type: invocation.flow_type,
                sticky_app_worker_id: Some(sticky_app_worker_id.clone())
                    .filter(|worker| !worker.is_empty()),
                payload: DispatchTaskPayload::Rpc(invocation.request),
                deadline: SystemTime::now() + remaining,
                sticky_wait: None,
                dispatch_wait: None,
            })
            .map_err(|status| RpcInvokeError::NoWorkerAvailable {
                detail: status.message().to_owned(),
            })?;
        loop {
            match handle.next().await {
                Some(DispatchEvent::Output(_)) => {}
                Some(DispatchEvent::Completed(outcome)) => {
                    return rpc_result(outcome, &sticky_app_worker_id);
                }
                None => {
                    return Err(RpcInvokeError::WorkerDisconnected {
                        detail: "dispatch ended without an outcome".to_owned(),
                    });
                }
            }
        }
    }
}

impl RpcWorkerInvoker for PullWorkerRpcInvoker {
    fn invoke_worker_rpc(
        &self,
        invocation: WorkerRpcInvocation,
    ) -> BoxFuture<'_, Result<WorkerRpcResult, RpcInvokeError>> {
        Box::pin(self.invoke(invocation))
    }
}

fn rpc_result(
    outcome: DispatchOutcome,
    sticky_app_worker_id: &str,
) -> Result<WorkerRpcResult, RpcInvokeError> {
    match outcome {
        DispatchOutcome::WorkerResult {
            frame,
            app_worker_id,
        } => match frame.output {
            Some(poll_worker_task_frame::Output::Rpc(response)) => Ok(WorkerRpcResult {
                response,
                pull_routing: Some(pull_routing(sticky_app_worker_id, &app_worker_id)),
                app_worker_id,
            }),
            _ => Err(RpcInvokeError::WorkerError {
                response: WorkerErrorResponse {
                    detail: "Worker answered an RPC task with a non-RPC frame".to_owned(),
                    ..WorkerErrorResponse::default()
                },
            }),
        },
        DispatchOutcome::WorkerError { frame, .. } => Err(RpcInvokeError::WorkerError {
            response: match frame.output {
                Some(poll_worker_task_frame::Output::WorkerError(response)) => response,
                _ => WorkerErrorResponse {
                    detail: "Worker returned an error frame without an error".to_owned(),
                    ..WorkerErrorResponse::default()
                },
            },
        }),
        DispatchOutcome::DispatchError { reason, detail } => Err(match reason {
            DispatchTaskErrorReason::UnknownName => RpcInvokeError::UnknownRpc { detail },
            DispatchTaskErrorReason::WorkerDisconnected => {
                RpcInvokeError::WorkerDisconnected { detail }
            }
            DispatchTaskErrorReason::NoWorkerAvailable | DispatchTaskErrorReason::Unspecified => {
                RpcInvokeError::NoWorkerAvailable { detail }
            }
        }),
        DispatchOutcome::DeadlineExceeded => Err(RpcInvokeError::DeadlineExceeded),
        DispatchOutcome::Cancelled => Err(RpcInvokeError::WorkerDisconnected {
            detail: "the RPC dispatch was cancelled".to_owned(),
        }),
        DispatchOutcome::ServerClosing => Err(RpcInvokeError::NoWorkerAvailable {
            detail: "matching server is closing".to_owned(),
        }),
    }
}

/// Uses the same source rule as Step dispatch: a sticky hit when the sticky Worker served it.
fn pull_routing(sticky_app_worker_id: &str, served_by_app_worker_id: &str) -> PullRouting {
    let source = if served_by_app_worker_id.is_empty() {
        PullRoutingSource::Unspecified
    } else if served_by_app_worker_id == sticky_app_worker_id {
        PullRoutingSource::StickyQueue
    } else {
        PullRoutingSource::FlowTypeQueue
    };
    PullRouting {
        sticky_app_worker_id: sticky_app_worker_id.to_owned(),
        served_by_app_worker_id: served_by_app_worker_id.to_owned(),
        source: source.into(),
    }
}
