// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! The Worker transport behind method dispatch: pull Worker matching in production, a scripted
//! fake in tests.

use std::time::SystemTime;

use dexzd_worker_dispatch::{
    DispatchEvent, DispatchHandle, DispatchRequest, DispatchTaskPayload, WorkerDispatch,
};
use futures::future::BoxFuture;

use crate::effects::method_request::MethodRequest;

/// One method attempt to send to a Worker.
#[derive(Debug, Clone)]
pub struct MethodDispatchRequest {
    /// Tenant of the Flow.
    pub tenant_id: u64,
    /// Flow ID, for same-Flow Worker affinity.
    pub flow_id: String,
    /// Flow type queue.
    pub flow_type: String,
    /// Worker that last served the run, tried first.
    pub sticky_app_worker_id: Option<String>,
    /// The Worker request.
    pub request: MethodRequest,
    /// Method deadline, including Worker processing time.
    pub deadline: SystemTime,
}

/// The dispatch could not start.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("method dispatch could not start: {detail}")]
pub struct MethodDispatchStartError {
    /// Why matching refused the dispatch.
    pub detail: String,
}

/// Ordered Worker frames and the single outcome of one dispatch. Dropping it cancels the
/// Worker task.
pub trait MethodDispatchSession: Send {
    /// Returns the next Worker frame or the final outcome, then `None`.
    fn next_event(&mut self) -> BoxFuture<'_, Option<DispatchEvent>>;
    /// Asks the Worker to stop the task.
    fn cancel(&self);
}

/// Sends method attempts to Workers.
pub trait MethodDispatcher: Send + Sync + 'static {
    /// Starts one dispatch.
    fn dispatch(
        &self,
        request: MethodDispatchRequest,
    ) -> Result<Box<dyn MethodDispatchSession>, MethodDispatchStartError>;
}

impl MethodDispatcher for WorkerDispatch {
    fn dispatch(
        &self,
        request: MethodDispatchRequest,
    ) -> Result<Box<dyn MethodDispatchSession>, MethodDispatchStartError> {
        let payload = match request.request {
            MethodRequest::WaitFor(request) => DispatchTaskPayload::WaitFor(request),
            MethodRequest::Execute(request) => DispatchTaskPayload::Execute(request),
        };
        let handle = WorkerDispatch::dispatch(
            self,
            DispatchRequest {
                tenant_id: request.tenant_id,
                flow_id: request.flow_id,
                flow_type: request.flow_type,
                sticky_app_worker_id: request.sticky_app_worker_id,
                payload,
                deadline: request.deadline,
                sticky_wait: None,
                dispatch_wait: None,
            },
        )
        .map_err(|status| MethodDispatchStartError {
            detail: status.message().to_owned(),
        })?;
        Ok(Box::new(WorkerDispatchSession { handle }))
    }
}

struct WorkerDispatchSession {
    handle: DispatchHandle,
}

impl MethodDispatchSession for WorkerDispatchSession {
    fn next_event(&mut self) -> BoxFuture<'_, Option<DispatchEvent>> {
        Box::pin(self.handle.next())
    }

    fn cancel(&self) {
        self.handle.cancel();
    }
}
