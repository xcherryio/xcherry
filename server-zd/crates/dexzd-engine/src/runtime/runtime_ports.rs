// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Services the runtime calls but other modules implement: effects, Worker RPCs, and
//! hard-delete values.

use dexzd_apply::ApplyEffect;
use dexzd_keyspace::RunId;
use dexzd_protocol::dex::{
    AttributeWrite, InvokeWorkerRpcRequest, InvokeWorkerRpcResponse, PullRouting, Value,
    WorkerErrorResponse,
};
use dexzd_protocol::dexzd::v1::HardDeleteObjectReference;
use futures::future::BoxFuture;
use tokio::time::Instant;

use crate::runtime::FlowAddress;

/// Receives apply effects from the runtime. The effect runner implements it.
pub trait RuntimeEffectSink: Send + Sync + 'static {
    /// Handles the effects of one outcome. `is_committed` is false only for speculative
    /// effects of ASYNC Steps whose external visibility does not matter.
    ///
    /// Must not block: the runtime calls it while it serializes the Flow.
    fn handle(&self, flow: &FlowAddress, effects: &[ApplyEffect], is_committed: bool);
}

/// One RPC call to a Worker.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkerRpcInvocation {
    /// Flow the RPC targets.
    pub flow: FlowAddress,
    /// Run the RPC targets.
    pub run_id: RunId,
    /// Worker queue of the Flow type.
    pub flow_type: String,
    /// Worker that served the run most recently; empty when none.
    pub sticky_app_worker_id: String,
    /// The request the Worker receives.
    pub request: InvokeWorkerRpcRequest,
    /// The call fails with [`RpcInvokeError::DeadlineExceeded`] after this instant.
    pub deadline: Instant,
}

/// A Worker's successful RPC response.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkerRpcResult {
    /// The response.
    pub response: InvokeWorkerRpcResponse,
    /// Worker that produced the response.
    pub app_worker_id: String,
    /// How the call was routed.
    pub pull_routing: Option<PullRouting>,
}

/// Why a Worker RPC call failed.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum RpcInvokeError {
    /// The Worker returned an error response.
    #[error("worker returned an error: {}", response.detail)]
    WorkerError {
        /// The Worker's error.
        response: WorkerErrorResponse,
    },
    /// No Worker serves the Flow type.
    #[error("no worker is available: {detail}")]
    NoWorkerAvailable {
        /// Dispatch detail.
        detail: String,
    },
    /// Every Worker rejected the RPC name.
    #[error("no worker knows the RPC: {detail}")]
    UnknownRpc {
        /// Dispatch detail.
        detail: String,
    },
    /// The Worker disconnected after it started the call.
    #[error("worker disconnected: {detail}")]
    WorkerDisconnected {
        /// Dispatch detail.
        detail: String,
    },
    /// The deadline passed.
    #[error("worker RPC deadline exceeded")]
    DeadlineExceeded,
}

/// Calls RPC handlers on Workers. The Worker dispatch layer implements it.
pub trait RpcWorkerInvoker: Send + Sync + 'static {
    /// Sends `invocation` to a Worker and waits for its response or the deadline.
    ///
    /// Returns the response, or [`RpcInvokeError`] when no Worker produced one.
    fn invoke_worker_rpc(
        &self,
        invocation: WorkerRpcInvocation,
    ) -> BoxFuture<'_, Result<WorkerRpcResult, RpcInvokeError>>;
}

/// Error from reading a hard-delete Attribute object.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("reading hard-delete object {object_id} failed: {detail}")]
pub struct HardDeleteReadError {
    /// The object.
    pub object_id: String,
    /// Store message.
    pub detail: String,
}

/// Reads hard-delete Attribute values from object storage.
pub trait HardDeleteValueReader: Send + Sync + 'static {
    /// Returns the value stored in the object, or `None` when the object no longer exists.
    ///
    /// Returns [`HardDeleteReadError`] when object storage fails.
    fn read_hard_delete_value<'a>(
        &'a self,
        flow: &'a FlowAddress,
        run_id: RunId,
        object: &'a HardDeleteObjectReference,
    ) -> BoxFuture<'a, Result<Option<Value>, HardDeleteReadError>>;
}

/// Hard-delete Attribute values that one command uploaded before it was proposed.
///
/// The runtime puts [`Self::references`] into the command and hands the value back to
/// [`HardDeleteValueUploader::finish_hard_delete_uploads`] once the command's outcome is known.
#[derive(Debug, Clone, Default, PartialEq)]
#[must_use = "uploads must be finished once the command's outcome is known"]
pub struct HardDeleteUploads {
    /// One reference per uploaded write; `write_index` is the write's index in the command's
    /// write list.
    pub references: Vec<HardDeleteObjectReference>,
    /// Object storage keys of the uploaded objects, which identify the uploads when they finish.
    pub object_keys: Vec<String>,
}

/// What became of a command whose hard-delete values were uploaded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HardDeleteUploadOutcome {
    /// The command committed and apply stored the objects. Keep the objects and clear their
    /// pending-upload records.
    Committed,
    /// Nothing references the objects: the command was never submitted, or apply rejected it.
    /// Delete the objects and clear their pending-upload records.
    NotCommitted,
    /// The command may be in the log: its commit failed or the registry halted. Keep the objects
    /// and their pending-upload records; recovery decides from committed state.
    Unknown,
}

/// Error from uploading or finishing hard-delete Attribute values.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("hard-delete object storage failed: {detail}")]
pub struct HardDeleteUploadError {
    /// Store message.
    pub detail: String,
}

/// Uploads hard-delete Attribute values to object storage before the runtime proposes the
/// command that writes them, so the values never enter the log. The hard-delete object store
/// implements it.
///
/// The runtime uploads the writes of StartFlow, SetAttributes, and committed RPC effects, and
/// finishes every successful upload exactly once. A caller that drops its request future
/// between the two calls skips the finish; pending-upload recovery then handles those uploads.
pub trait HardDeleteValueUploader: Send + Sync + 'static {
    /// Uploads every hard-delete string or object value in `writes` as a new object of `flow`'s
    /// run `run_id`. It may replace each uploaded value in `writes`; apply reads the value from
    /// the object reference.
    ///
    /// Returns the uploads, with one reference per uploaded write. Returns
    /// [`HardDeleteUploadError`] when object storage fails; nothing is left to finish then.
    fn upload_hard_delete_values<'a>(
        &'a self,
        flow: &'a FlowAddress,
        run_id: RunId,
        writes: &'a mut [AttributeWrite],
    ) -> BoxFuture<'a, Result<HardDeleteUploads, HardDeleteUploadError>>;

    /// Finishes one command's uploads once it committed, was rejected, or failed.
    ///
    /// Returns [`HardDeleteUploadError`] when the cleanup fails; the runtime logs it and the
    /// command's result stands.
    fn finish_hard_delete_uploads(
        &self,
        uploads: HardDeleteUploads,
        outcome: HardDeleteUploadOutcome,
    ) -> BoxFuture<'_, Result<(), HardDeleteUploadError>>;
}
