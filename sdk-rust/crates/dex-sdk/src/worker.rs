// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use crate::credential::{CredentialChannel, CredentialInterceptor};
use dex_protocol::dex::WorkerErrorResponse;
use dex_protocol::dex::flow_service_client::FlowServiceClient;
use dex_protocol::dex::worker_service_server::{WorkerService, WorkerServiceServer};
use dex_protocol::dex::{
    InvokeExecuteMethodOutput, InvokeExecuteMethodRequest, InvokeWaitForMethodOutput,
    InvokeWaitForMethodRequest, InvokeWorkerRpcRequest, InvokeWorkerRpcResponse,
    SyncAttributeIndexRequest,
};
use prost::Message;
use prost_types::Any;
use tokio::runtime::Runtime;
use tokio::sync::watch;
use tonic::transport::{Endpoint, Server};
use tonic::{Code, Request, Response, Status};

use crate::server_protocol::{
    NegotiatedServerProtocolVersion, negotiate_server_protocol, server_info_request_error,
};
use crate::value_hydrator::ValueHydrator;
use crate::worker_dispatcher::WorkerDispatcher;
use crate::worker_output::WorkerResponseStream;
use crate::worker_pull::PullWorker;
use crate::{BlobCache, HandlerError, Registry, SdkError, SdkResult, WorkerOptions, WorkerTarget};

// Keep the complete worker error status below default gRPC trailer limits after
// the server wraps WorkerErrorResponse in ServiceErrorResponse.
const MAX_WORKER_ERROR_DETAIL_BYTES: usize = 1024;
const MAX_WORKER_ERROR_TYPE_BYTES: usize = 256;
const MAX_WORKER_STACK_TRACE_BYTES: usize = 4 * 1024;
const ERROR_DETAIL_TRUNCATION_MARKER: &str = "\n... error detail truncated by Dex Rust SDK ...";
const ERROR_TYPE_TRUNCATION_MARKER: &str = "\n... error type truncated by Dex Rust SDK ...";
const STACK_TRACE_TRUNCATION_MARKER: &str = "\n... stack trace truncated by Dex Rust SDK ...";

const CREATED: u8 = 0;
const RUNNING: u8 = 1;
const STOPPED: u8 = 2;

/// Hosts registered Flow Step and RPC handlers through a listener or outbound task streams.
///
/// A Worker owns an internal Tokio runtime and is one-shot. [`Self::start`] negotiates the Server
/// protocol, synchronizes Attribute indexes, then blocks while serving. Call [`Self::stop`]
/// from another thread to request graceful shutdown. Pull mode runs at most the configured number
/// of handlers concurrently and does not bind a listener. See [`crate::PullWorkerOptions`].
///
/// # Examples
///
/// ```no_run
/// use dex_sdk::{BlobCache, BlobCacheConfig, Registry, Worker, WorkerOptions};
/// use std::sync::Arc;
///
/// let cache = Arc::new(BlobCache::open(BlobCacheConfig::new(
///     "/tmp/dex-blobs", 64 * 1024 * 1024, 0,
/// )?)?);
/// let worker = Worker::try_new(Registry::new(), cache, WorkerOptions::new())?;
/// println!("advertise {}", worker.worker_target().address());
/// worker.start()?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct Worker {
    runtime: Runtime,
    service: RustWorkerService,
    flow_service: FlowServiceClient<CredentialChannel>,
    attribute_indexes: HashMap<String, i32>,
    attribute_index_sync_timeout: std::time::Duration,
    bind_address: Option<SocketAddr>,
    pull: Option<PullWorker>,
    worker_target: WorkerTarget,
    shutdown: watch::Sender<bool>,
    state: AtomicU8,
    negotiated_protocol_version: NegotiatedServerProtocolVersion,
}

impl Worker {
    /// Creates a Worker or panics when options or runtime initialization are invalid.
    ///
    /// # Panics
    ///
    /// Panics for an invalid bind address, zero index-sync timeout, invalid Dex endpoint, or Tokio
    /// runtime creation failure. Prefer [`Self::try_new`] for recoverable startup.
    pub fn new(registry: Registry, blob_cache: Arc<BlobCache>, options: WorkerOptions) -> Self {
        Self::try_new(registry, blob_cache, options)
            .unwrap_or_else(|error| panic!("cannot create Rust Worker: {error}"))
    }

    /// Creates a one-shot Worker without binding a listener during construction.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::FlowDefinition`] for invalid Worker options, including conflicting pull
    /// and listening settings, or Service for runtime and endpoint initialization failures. The Dex
    /// connection is lazy.
    pub fn try_new(
        registry: Registry,
        blob_cache: Arc<BlobCache>,
        options: WorkerOptions,
    ) -> SdkResult<Self> {
        let runtime = Runtime::new().map_err(service_error)?;
        if options.attribute_index_sync_timeout_value().is_zero() {
            return Err(SdkError::FlowDefinition {
                message: "attribute index sync timeout must be positive".to_string(),
            });
        }
        if let Some(target) = options.worker_target_value() {
            target.validate()?;
        }
        if options.pull_value().is_some()
            && (options.has_bind_address() || options.worker_target_value().is_some())
        {
            return Err(SdkError::FlowDefinition {
                message: "pull Worker cannot configure a bind address or advertised Worker target"
                    .to_string(),
            });
        }
        if options.pull_value().is_none()
            && options
                .worker_target_value()
                .is_some_and(WorkerTarget::is_pull)
        {
            return Err(SdkError::FlowDefinition {
                message: "pull Worker target requires pull Worker options".to_string(),
            });
        }
        let bind_address = if options.pull_value().is_some() {
            None
        } else {
            Some(
                options
                    .bind_address_value()
                    .parse::<SocketAddr>()
                    .map_err(|error| SdkError::FlowDefinition {
                        message: format!("invalid Worker bind address: {error}"),
                    })?,
            )
        };
        let worker_target = if let Some(bind_address) = bind_address {
            options.worker_target_value().cloned().unwrap_or_else(|| {
                let host = if bind_address.ip().is_unspecified() {
                    "127.0.0.1".to_string()
                } else {
                    bind_address.ip().to_string()
                };
                WorkerTarget::new(format!("{host}:{}", bind_address.port()))
            })
        } else {
            WorkerTarget::pull()
        };
        let endpoint = Endpoint::from_shared(endpoint_address(options.server_address_value()))
            .map_err(service_error)?;
        let flow_service = {
            let _runtime_guard = runtime.enter();
            FlowServiceClient::new(CredentialChannel::new(
                endpoint.connect_lazy(),
                CredentialInterceptor::new(options.credential_value()),
            ))
        };
        let pull_channel = {
            let _runtime_guard = runtime.enter();
            options.pull_value().map(|_| {
                crate::worker_pull::outbound_task_channel(
                    endpoint.clone(),
                    options.credential_value(),
                )
            })
        };
        let attribute_indexes = registry.attribute_indexes().clone();
        let hydrator = ValueHydrator::new(flow_service.clone(), blob_cache);
        let negotiated_protocol_version = NegotiatedServerProtocolVersion::default();
        let dispatcher =
            WorkerDispatcher::new(registry, hydrator, negotiated_protocol_version.clone());
        let pull = options
            .pull_value()
            .zip(pull_channel)
            .map(|(options, channel)| PullWorker::new(options, &dispatcher, channel))
            .transpose()?;
        let (shutdown, _) = watch::channel(false);
        Ok(Self {
            runtime,
            service: RustWorkerService { dispatcher },
            flow_service,
            attribute_indexes,
            attribute_index_sync_timeout: options.attribute_index_sync_timeout_value(),
            bind_address,
            pull,
            worker_target,
            shutdown,
            state: AtomicU8::new(CREATED),
            negotiated_protocol_version,
        })
    }

    /// Returns the resolved target that Clients should advertise to Dex.
    ///
    /// Pull mode returns a pull target with no address. The value is also suitable for a
    /// [`crate::FlowConfig`] override on a StartFlow or SubFlow.
    pub fn worker_target(&self) -> &WorkerTarget {
        &self.worker_target
    }

    /// Negotiates protocol, synchronizes Attribute indexes, and serves until stopped or failed.
    ///
    /// In pull mode, stream errors are logged and retried. After [`Self::stop`], this method drains
    /// active handlers for the configured timeout, cancels any remaining handlers, and waits for
    /// them to return. Call this method only once, on a dedicated thread.
    ///
    /// # Errors
    ///
    /// Returns FlowDefinition if called more than once, or Service for index synchronization,
    /// timeout, bind, and server failures.
    /// Returns a service error exposing [`SdkError::pull_registration_error`] after draining when permanent-rejection shutdown is enabled.
    pub fn start(&self) -> SdkResult<()> {
        let result = self.start_worker();
        if let Some(pull) = &self.pull {
            pull.registration.finish(result.as_ref().err().cloned());
        }
        result
    }

    fn start_worker(&self) -> SdkResult<()> {
        self.state
            .compare_exchange(CREATED, RUNNING, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| SdkError::FlowDefinition {
                message: "Worker can only be started once".to_string(),
            })?;
        let mut shutdown = self.shutdown.subscribe();
        let mut flow_service = self.flow_service.clone();
        let server_info_result = self.runtime.block_on(async {
            tokio::time::timeout(
                self.attribute_index_sync_timeout,
                flow_service.get_server_info(()),
            )
            .await
        });
        let server_info = match server_info_result {
            Ok(Ok(response)) => response.into_inner(),
            Ok(Err(status)) => {
                self.state.store(STOPPED, Ordering::Release);
                return Err(service_error(server_info_request_error(status)));
            }
            Err(elapsed) => {
                self.state.store(STOPPED, Ordering::Release);
                return Err(service_error(server_info_request_error(elapsed)));
            }
        };
        let negotiated_protocol_version =
            negotiate_server_protocol(&server_info).map_err(|error| {
                self.state.store(STOPPED, Ordering::Release);
                service_error(error)
            })?;
        self.negotiated_protocol_version
            .store(negotiated_protocol_version);
        let sync_result = self.runtime.block_on(async {
            tokio::time::timeout(
                self.attribute_index_sync_timeout,
                flow_service.sync_attribute_indexes(SyncAttributeIndexRequest {
                    attribute_indexes: self.attribute_indexes.clone(),
                }),
            )
            .await
        });
        match sync_result {
            Ok(Ok(_)) => {}
            Ok(Err(status)) => {
                self.state.store(STOPPED, Ordering::Release);
                return Err(service_error(status));
            }
            Err(elapsed) => {
                self.state.store(STOPPED, Ordering::Release);
                return Err(service_error(elapsed));
            }
        }
        let result = if let Some(pull) = &self.pull {
            self.runtime.block_on(pull.run(shutdown))
        } else {
            self.runtime
                .block_on(
                    Server::builder()
                        .add_service(WorkerServiceServer::new(self.service.clone()))
                        .serve_with_shutdown(
                            self.bind_address.expect("listening Worker address"),
                            async move {
                                while !*shutdown.borrow() && shutdown.changed().await.is_ok() {}
                            },
                        ),
                )
                .map_err(service_error)
        };
        self.state.store(STOPPED, Ordering::Release);
        result
    }

    /// Waits until every Flow type queue and the app Worker ID queue has registered once.
    ///
    /// Safe before or after [`Self::start`] and for concurrent callers. Start the Worker on a
    /// dedicated thread. Drop this future to cancel the wait, or use `tokio::time::timeout`.
    /// Completion remains latched after shutdown; use [`Self::pull_registration_state`] for probes.
    ///
    /// # Errors
    ///
    /// Returns a service error exposing [`SdkError::pull_registration_error`] for a permanent rejection without another registered
    /// stream before readiness. Returns the startup error if startup fails, or InvalidArgument
    /// for a listening Worker or shutdown before readiness.
    ///
    /// ```no_run
    /// # async fn ready(worker: &dex_sdk::Worker) -> Result<(), Box<dyn std::error::Error>> {
    /// tokio::time::timeout(std::time::Duration::from_secs(10), worker.wait_pull_registered()).await??;
    /// assert!(worker.pull_registration_state().is_ready);
    /// # Ok(()) }
    /// ```
    pub async fn wait_pull_registered(&self) -> SdkResult<()> {
        let pull = self
            .pull
            .as_ref()
            .ok_or_else(|| SdkError::InvalidArgument {
                message: "wait_pull_registered requires a pull Worker".to_string(),
            })?;
        pull.registration.wait().await
    }

    /// Returns cumulative Step declines caused by reserved RPC capacity for this Worker.
    ///
    /// Full-capacity rejections and unknown names are excluded. The count never resets and
    /// remains available after stop. Concurrent reads are safe. Listening Workers return zero.
    /// Export the count to a monitoring counter to identify reserve pressure.
    ///
    /// ```no_run
    /// # fn inspect(worker: &dex_sdk::Worker) {
    /// let declines: u64 = worker.pull_reserved_rpc_declines();
    /// # }
    /// ```
    pub fn pull_reserved_rpc_declines(&self) -> u64 {
        self.pull
            .as_ref()
            .map_or(0, |pull| pull.reserved_rpc_declines())
    }

    /// Returns an independent snapshot of current pull queue registrations.
    ///
    /// Readiness latches after every queue registers once. Only a permanent rejection with no
    /// registered stream makes it false. Successful registration clears that queue's error.
    /// Transient disconnects and Stop do not clear readiness. Listening Workers return false
    /// with no queues. See [`Self::wait_pull_registered`] for a startup and probe example.
    pub fn pull_registration_state(&self) -> crate::PullRegistrationState {
        self.pull
            .as_ref()
            .map(|pull| pull.registration.snapshot())
            .unwrap_or_default()
    }

    /// Requests shutdown of a running Worker and returns immediately.
    ///
    /// Calling `stop` before or after `start` is safe; [`Self::start`] observes the shutdown signal.
    /// Pull mode first drains active handlers. After the drain timeout it cancels them, and
    /// [`Self::start`] waits for their return. Half-closed pull streams wait for server EOF
    /// until the remaining drain deadline. Stop is idempotent.
    pub fn stop(&self) {
        if let Some(pull) = &self.pull {
            pull.registration.finish(None);
        }
        self.shutdown.send_replace(true);
    }
}

#[derive(Clone)]
struct RustWorkerService {
    dispatcher: WorkerDispatcher,
}

#[tonic::async_trait]
impl WorkerService for RustWorkerService {
    type InvokeWaitForMethodStream = WorkerResponseStream<InvokeWaitForMethodOutput>;

    async fn invoke_wait_for_method(
        &self,
        request: Request<InvokeWaitForMethodRequest>,
    ) -> Result<Response<Self::InvokeWaitForMethodStream>, Status> {
        Ok(Response::new(
            self.dispatcher
                .invoke_wait_for(request.into_inner())
                .into_stream(worker_status),
        ))
    }

    type InvokeExecuteMethodStream = WorkerResponseStream<InvokeExecuteMethodOutput>;

    async fn invoke_execute_method(
        &self,
        request: Request<InvokeExecuteMethodRequest>,
    ) -> Result<Response<Self::InvokeExecuteMethodStream>, Status> {
        Ok(Response::new(
            self.dispatcher
                .invoke_execute(request.into_inner())
                .into_stream(worker_status),
        ))
    }

    async fn invoke_worker_rpc(
        &self,
        request: Request<InvokeWorkerRpcRequest>,
    ) -> Result<Response<InvokeWorkerRpcResponse>, Status> {
        self.dispatcher
            .invoke_rpc(request.into_inner())
            .await
            .map(Response::new)
            .map_err(worker_status)
    }
}

#[derive(Clone, PartialEq, Message)]
struct GoogleRpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<Any>,
}

pub(crate) fn worker_error_response(error: HandlerError) -> WorkerErrorResponse {
    let message = truncate_worker_failure_field(
        &error.to_string(),
        MAX_WORKER_ERROR_DETAIL_BYTES,
        ERROR_DETAIL_TRUNCATION_MARKER,
    );
    let error_type = truncate_worker_failure_field(
        error.error_type(),
        MAX_WORKER_ERROR_TYPE_BYTES,
        ERROR_TYPE_TRUNCATION_MARKER,
    );
    let stack_trace = truncate_worker_failure_field(
        &format!("{message}\n{}", error.stack_trace()),
        MAX_WORKER_STACK_TRACE_BYTES,
        STACK_TRACE_TRUNCATION_MARKER,
    );
    WorkerErrorResponse {
        detail: message.clone(),
        error_type,
        stack_trace,
        retry_after_seconds: error.retry_after_seconds(),
        grpc_status: Code::Unknown as i32,
    }
}

pub(crate) fn worker_status(error: HandlerError) -> Status {
    let worker_error = worker_error_response(error);
    let message = worker_error.detail.clone();
    let status = GoogleRpcStatus {
        code: Code::Unknown as i32,
        message: message.clone(),
        details: vec![Any {
            type_url: "type.googleapis.com/dex.WorkerErrorResponse".to_string(),
            value: worker_error.encode_to_vec(),
        }],
    };
    Status::with_details(Code::Unknown, message, status.encode_to_vec().into())
}

fn truncate_worker_failure_field(
    value: &str,
    maximum_bytes: usize,
    truncation_marker: &str,
) -> String {
    let encoded = value.as_bytes();
    if encoded.len() <= maximum_bytes {
        return value.to_string();
    }
    let mut prefix_length = maximum_bytes - truncation_marker.len();
    while prefix_length > 0 && encoded[prefix_length] & 0xc0 == 0x80 {
        prefix_length -= 1;
    }
    format!(
        "{}{}",
        std::str::from_utf8(&encoded[..prefix_length]).expect("UTF-8 boundary"),
        truncation_marker
    )
}

fn endpoint_address(address: &str) -> String {
    if address.contains("://") {
        address.to_string()
    } else {
        format!("http://{address}")
    }
}

fn service_error(error: impl std::fmt::Display) -> SdkError {
    SdkError::Service {
        service: crate::ServiceError::local("worker", error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_status_bounds_all_text_fields_at_utf8_boundaries() {
        let error = HandlerError::new(
            "世界".repeat(MAX_WORKER_ERROR_TYPE_BYTES),
            "世界".repeat(MAX_WORKER_STACK_TRACE_BYTES),
        );

        let status = worker_status(error);
        let rpc_status = GoogleRpcStatus::decode(status.details()).expect("decode status");
        let worker = WorkerErrorResponse::decode(rpc_status.details[0].value.as_slice())
            .expect("decode worker error");

        assert!(worker.detail.len() <= MAX_WORKER_ERROR_DETAIL_BYTES);
        assert!(worker.error_type.len() <= MAX_WORKER_ERROR_TYPE_BYTES);
        assert!(worker.stack_trace.len() <= MAX_WORKER_STACK_TRACE_BYTES);
        assert!(
            worker
                .detail
                .ends_with("... error detail truncated by Dex Rust SDK ...")
        );
        assert!(
            worker
                .error_type
                .ends_with("... error type truncated by Dex Rust SDK ...")
        );
        assert_eq!(rpc_status.message, worker.detail);
        assert!(status.details().len() < 7 * 1024);
        assert!(!worker.detail.contains('\u{fffd}'));
        assert!(!worker.error_type.contains('\u{fffd}'));
        assert!(!worker.stack_trace.contains('\u{fffd}'));
    }

    #[test]
    fn truncation_preserves_utf8_boundaries_and_markers() {
        let truncated = truncate_worker_failure_field(
            &"世界".repeat(MAX_WORKER_STACK_TRACE_BYTES),
            MAX_WORKER_STACK_TRACE_BYTES,
            STACK_TRACE_TRUNCATION_MARKER,
        );

        assert!(truncated.len() <= MAX_WORKER_STACK_TRACE_BYTES);
        assert!(truncated.ends_with("... stack trace truncated by Dex Rust SDK ..."));
        assert!(!truncated.contains('\u{fffd}'));
    }
}
