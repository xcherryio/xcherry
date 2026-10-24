// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

mod bounded_teardown;
mod client_deadline;
mod search_projection;

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;

use dexzd_api::{
    AttributeIndexes, DexFlowService, FlowServiceDependencies, HealthStatus, RequestMetricsLayer,
    StreamOperations,
};
use dexzd_apply::FlowStateReader;
use dexzd_clock::{SystemWallClock, WallClock};
use dexzd_engine::{
    CommandApplier, CommandCommitter, DeterministicCommandApplier, EffectDependencies,
    EffectRunner, EffectTasksHandle, EngineNode, EngineNodeDependencies, EngineRuntimeHooks,
    EngineServices, FlowRuntimeDependencies, FlowRuntimeRegistry, HardDeleteObjects,
    NodeMetaUploadLedger, NodeStopReason, SystemEngineIdSource,
};
use dexzd_metrics::NodeMetrics;
use dexzd_protocol::dex::flow_service_server::FlowServiceServer;
use dexzd_protocol::dex::worker_task_service_server::WorkerTaskServiceServer;
use dexzd_search::builtin::{BuiltinSearchBackend, CommittedSearchViewProvider};
use dexzd_search::{SearchBackend, SearchError};
use dexzd_stream_store::StreamStore;
use dexzd_worker_dispatch::{WorkerDispatch, WorkerTaskService};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tonic::body::Body;
use tonic::codegen::{BoxFuture, Service, http};
use tonic::server::NamedService;

use crate::ServerError;
use crate::component_configs::ComponentConfigs;
use crate::config::{Config, SearchBackend as SearchBackendKind};
use crate::embedded_s3::EmbeddedS3;
use crate::metrics_server::MetricsServer;
use crate::object_stores::ObjectStores;
use crate::runtime_hooks::RegistryCommittedCommandSink;
use crate::worker_rpc_invoker::PullWorkerRpcInvoker;

pub(crate) use search_projection::CommittedSearchFrames;

use bounded_teardown::BoundedTeardown;
use search_projection::{ParadeDbSearch, SearchProjection};

/// Part of the budget for open calls, so the engine still stops cleanly after a stuck call.
const GRPC_DRAIN_BUDGET: Duration = Duration::from_secs(4);
/// Shutdown budgets of the later steps; with the gRPC drain they total 14.5 seconds.
const EFFECT_TASKS_STOP_BUDGET: Duration = Duration::from_secs(2);
const SEARCH_PROJECTION_STOP_BUDGET: Duration = Duration::from_secs(1);
/// Covers the engine's three-second task stop and its lease release.
const ENGINE_STOP_BUDGET: Duration =
    EngineNode::SHUTDOWN_BUDGET.saturating_add(Duration::from_millis(500));
const STREAM_STORE_CLOSE_BUDGET: Duration = Duration::from_secs(1);
const EMBEDDED_S3_STOP_BUDGET: Duration = Duration::from_secs(1);
/// gRPC keepalive, equal to the Go Server's.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(20);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);
/// Tenant that serves pull Workers until authentication assigns tenants.
const DEFAULT_TENANT_ID: u64 = 0;

/// A started server. Dropping it without [`RunningServer::shutdown`] aborts it.
pub struct RunningServer {
    local_address: SocketAddr,
    metrics_address: Option<SocketAddr>,
    shutdown_request: CancellationToken,
    serving: JoinHandle<Result<(), ServerError>>,
}

impl RunningServer {
    /// Returns the address that serves FlowService, WorkerTaskService, and gRPC health.
    pub fn local_address(&self) -> SocketAddr {
        self.local_address
    }

    /// Returns the separate metrics address when metrics are enabled.
    pub fn metrics_address(&self) -> Option<SocketAddr> {
        self.metrics_address
    }

    /// Stops gracefully within 14.5 seconds and returns how serving ended.
    ///
    /// Returns [`ServerError::NodeStopped`] when the node had already stopped for another reason,
    /// such as a lost lease, and [`ServerError::ShutdownIncomplete`] when a shutdown step overran
    /// its budget or failed, such as a lease release that object storage rejected.
    pub async fn shutdown(self) -> Result<(), ServerError> {
        self.shutdown_request.cancel();
        self.wait().await
    }

    /// Waits until the server stops by itself, for example after losing its lease.
    pub async fn wait(mut self) -> Result<(), ServerError> {
        match (&mut self.serving).await {
            Ok(result) => result,
            Err(error) => Err(ServerError::ServingTaskFailed {
                detail: error.to_string(),
            }),
        }
    }

    /// Returns a token that stops the server when cancelled, for signal handlers.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown_request.clone()
    }
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        self.serving.abort();
    }
}

/// Starts a server from a validated configuration.
///
/// The server stops `embedded_s3`, the dev mode emulator, last.
pub(crate) async fn start_server(
    config: &Config,
    embedded_s3: Option<EmbeddedS3>,
    cancellation: &mut watch::Receiver<bool>,
) -> Result<Option<RunningServer>, ServerError> {
    let components = ComponentConfigs::from_config(config)?;
    let listener = TcpListener::bind(config.api.listen)
        .await
        .map_err(|source| ServerError::Listen {
            address: config.api.listen,
            source,
        })?;
    let local_address = listener
        .local_addr()
        .map_err(|source| ServerError::Listen {
            address: config.api.listen,
            source,
        })?;
    let (registry, metrics) = NodeMetrics::registered(
        config.local_storage.directory.clone(),
        config.cluster.partition_count as usize,
    );
    let metrics = Arc::new(metrics);
    metrics.set_role(dexzd_metrics::NodeRole::Starting);
    let metrics_server =
        match MetricsServer::start(&config.metrics.listen, Arc::new(registry)).await {
            Ok(server) => server,
            Err(error) => {
                if let Some(embedded_s3) = embedded_s3 {
                    embedded_s3.shutdown().await;
                }
                return Err(error);
            }
        };
    let metrics_address = metrics_server.as_ref().map(MetricsServer::address);
    let grpc = GrpcServer::start(
        listener,
        components.grpc_max_message_bytes,
        metrics.api_requests.clone(),
        metrics.api_latency.clone(),
    )
    .await;
    let started = ServerNode::start(config, &components, metrics.clone(), cancellation).await;
    let node = match started {
        Ok(Some(node)) => node,
        Ok(None) => {
            grpc.abort().await;
            if let Some(metrics_server) = metrics_server {
                metrics_server.shutdown().await;
            }
            if let Some(embedded_s3) = embedded_s3 {
                embedded_s3.shutdown().await;
            }
            return Ok(None);
        }
        Err(error) => {
            grpc.abort().await;
            if let Some(metrics_server) = metrics_server {
                metrics_server.shutdown().await;
            }
            if let Some(embedded_s3) = embedded_s3 {
                embedded_s3.shutdown().await;
            }
            return Err(error);
        }
    };
    let shutdown_request = CancellationToken::new();
    let serving =
        tokio::spawn(node.serve(grpc, metrics_server, shutdown_request.clone(), embedded_s3));
    tracing::info!(address = %local_address, "dex-server-zd serving");
    Ok(Some(RunningServer {
        local_address,
        metrics_address,
        shutdown_request,
        serving,
    }))
}

struct GrpcServer {
    health: HealthStatus,
    flow_service_gate: StartupService<FlowServiceServer<DexFlowService>>,
    worker_task_service_gate: StartupService<WorkerTaskServiceServer<WorkerTaskService>>,
    max_message_bytes: usize,
    stop: CancellationToken,
    task: JoinHandle<Result<(), tonic::transport::Error>>,
}

impl GrpcServer {
    async fn start(
        listener: TcpListener,
        max_message_bytes: usize,
        api_requests: dexzd_metrics::ApiRequests,
        api_latency: dexzd_metrics::ApiLatency,
    ) -> Self {
        let (health, health_server) = HealthStatus::new().await;
        let flow_service_gate = StartupService::<FlowServiceServer<DexFlowService>>::new();
        let worker_task_service_gate =
            StartupService::<WorkerTaskServiceServer<WorkerTaskService>>::new();
        let stop = CancellationToken::new();
        let stopped = stop.clone();
        let task = tokio::spawn(
            tonic::transport::Server::builder()
                .http2_keepalive_interval(Some(KEEPALIVE_INTERVAL))
                .http2_keepalive_timeout(Some(KEEPALIVE_TIMEOUT))
                .layer(RequestMetricsLayer::new(api_requests, api_latency))
                .layer(tower_layer::layer_fn(
                    client_deadline::client_deadline_layer,
                ))
                .add_service(health_server)
                .add_service(flow_service_gate.clone())
                .add_service(worker_task_service_gate.clone())
                .serve_with_incoming_shutdown(
                    futures::stream::unfold(listener, |listener| async move {
                        let accepted = listener.accept().await.map(|(stream, _)| stream);
                        Some((accepted, listener))
                    }),
                    async move { stopped.cancelled().await },
                ),
        );
        Self {
            health,
            flow_service_gate,
            worker_task_service_gate,
            max_message_bytes,
            stop,
            task,
        }
    }

    async fn abort(mut self) {
        self.stop.cancel();
        self.task.abort();
        match (&mut self.task).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::warn!(%error, "startup gRPC server failed"),
            Err(join_error) if join_error.is_cancelled() => {}
            Err(join_error) => {
                tracing::warn!(%join_error, "startup gRPC server task failed");
            }
        }
    }
}

impl Drop for GrpcServer {
    fn drop(&mut self) {
        self.stop.cancel();
        self.task.abort();
    }
}

#[derive(Clone)]
struct StartupService<S> {
    serving: Arc<RwLock<Option<S>>>,
}

impl<S> StartupService<S> {
    fn new() -> Self {
        Self {
            serving: Arc::new(RwLock::new(None)),
        }
    }

    fn activate(&self, service: S) {
        *self
            .serving
            .write()
            .expect("startup service lock is poisoned only if a holder panicked") = Some(service);
    }
}

impl<S: NamedService> NamedService for StartupService<S> {
    const NAME: &'static str = S::NAME;
}

impl<S> Service<http::Request<Body>> for StartupService<S>
where
    S: Service<http::Request<Body>, Response = http::Response<Body>, Error = Infallible>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
{
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let serving = self
            .serving
            .read()
            .expect("startup service lock is poisoned only if a holder panicked")
            .clone();
        Box::pin(async move {
            match serving {
                Some(mut service) => service.call(request).await,
                None => Ok(Status::unavailable("the node is starting").into_http()),
            }
        })
    }
}

/// Every component of one node, in start order.
struct ServerNode {
    metrics: Arc<NodeMetrics>,
    engine: EngineNode,
    registry: FlowRuntimeRegistry,
    effect_tasks: EffectTasksHandle,
    dispatch: WorkerDispatch,
    stream_store: Arc<StreamStore>,
    flow_service: DexFlowService,
    search_projection: Option<SearchProjection>,
}

/// What the engine's runtime factory builds; captured so the node can own it after start.
struct RuntimeParts {
    registry: FlowRuntimeRegistry,
    effect_tasks: EffectTasksHandle,
}

impl ServerNode {
    async fn start(
        config: &Config,
        components: &ComponentConfigs,
        metrics: Arc<NodeMetrics>,
        cancellation: &mut watch::Receiver<bool>,
    ) -> Result<Option<Self>, ServerError> {
        let object_stores = ObjectStores::connect(
            &config.object_store,
            config.commit.multipart_threshold_bytes,
            metrics.as_ref(),
        )
        .await?;
        let clock: Arc<dyn WallClock> = Arc::new(SystemWallClock);
        let applier: Arc<dyn CommandApplier> = Arc::new(DeterministicCommandApplier);
        let dispatch = WorkerDispatch::new_with_metrics(
            components.matching.clone(),
            clock.clone(),
            metrics.as_ref().clone(),
        );
        let stream_store = Arc::new(
            StreamStore::new(&components.stream_store)
                .map_err(|source| ServerError::StreamStore { source })?,
        );
        let partition_count = config.cluster.partition_count;
        let parade_db_search = match config.search.backend {
            SearchBackendKind::Builtin => None,
            SearchBackendKind::Paradedb => Some(ParadeDbSearch::connect(&config.search).await?),
        };
        let committed_search_frames = parade_db_search
            .as_ref()
            .map(ParadeDbSearch::committed_frames);
        let runtime_parts: Arc<Mutex<Option<RuntimeParts>>> = Arc::new(Mutex::new(None));
        let started = EngineNode::start_until_cancelled(
            &components.engine_node,
            EngineNodeDependencies {
                metrics: metrics.clone(),
                object_store: object_stores.primary.clone(),
                clock: clock.clone(),
                applier: applier.clone(),
                id_source: Arc::new(SystemEngineIdSource),
            },
            |services: EngineServices| {
                let partition_hasher = services.partition_hasher.clone();
                let hard_delete_objects = HardDeleteObjects::new(
                    object_stores.hard_delete.clone(),
                    Arc::new(NodeMetaUploadLedger::new(
                        services.local_store.clone(),
                        services.node_meta_lock.clone(),
                    )),
                    clock.clone(),
                    partition_hasher.clone(),
                    services.id_source.clone(),
                );
                let (effect_runner, effect_tasks) = EffectRunner::new(
                    &components.effects,
                    EffectDependencies {
                        metrics: metrics.clone(),
                        dispatcher: Arc::new(dispatch.clone()),
                        hard_delete_objects: hard_delete_objects.clone(),
                        partition_hasher: partition_hasher.clone(),
                        clock: clock.clone(),
                        validation_config: components.validation().clone(),
                        stream_store: stream_store.clone(),
                        id_source: services.id_source.clone(),
                    },
                );
                let registry = FlowRuntimeRegistry::new(
                    &components.runtime,
                    FlowRuntimeDependencies {
                        hasher: partition_hasher,
                        committer: Arc::new(services.committer),
                        applier: applier.clone(),
                        effects: Arc::new(effect_runner.clone()),
                        rpc_invoker: Arc::new(PullWorkerRpcInvoker::new(dispatch.clone())),
                        hard_delete_reader: Arc::new(hard_delete_objects.clone()),
                        hard_delete_uploader: Arc::new(hard_delete_objects),
                        id_source: services.id_source,
                    },
                );
                let effect_tasks = effect_tasks.start(Arc::new(registry.clone()));
                let hooks = EngineRuntimeHooks {
                    committed_commands: Arc::new(RegistryCommittedCommandSink::new(
                        registry.clone(),
                        committed_search_frames.clone(),
                    )),
                    rebuilder: Arc::new(effect_runner),
                };
                *runtime_parts
                    .lock()
                    .expect("runtime parts lock is poisoned only if a holder panicked") =
                    Some(RuntimeParts {
                        registry,
                        effect_tasks,
                    });
                hooks
            },
            cancellation,
        )
        .await;
        let parts = runtime_parts
            .lock()
            .expect("runtime parts lock is poisoned only if a holder panicked")
            .take();
        let engine = match started {
            Ok(Some(engine)) => engine,
            Ok(None) => {
                close_stream_store(&stream_store).await;
                return Ok(None);
            }
            Err(source) => {
                if let Some(parts) = parts {
                    parts.registry.close_long_polls();
                    parts.effect_tasks.shutdown().await;
                }
                close_stream_store(&stream_store).await;
                return Err(ServerError::EngineStart { source });
            }
        };
        let RuntimeParts {
            registry,
            effect_tasks,
        } = parts.expect("the engine calls the runtime factory before it starts serving");
        let (search_backend, search_projection) = match parade_db_search {
            Some(parade_db_search) => (
                Some(parade_db_search.search_backend()),
                Some(parade_db_search.start_projection(
                    engine.committer().clone(),
                    partition_count,
                    engine.wait_for_stop(),
                )),
            ),
            None => match builtin_search_backend(config, &engine, partition_count) {
                Ok(search_backend) => (Some(search_backend), None),
                Err(error) => {
                    registry.close_long_polls();
                    effect_tasks.shutdown().await;
                    if let Err(release_error) = engine.shutdown().await {
                        tracing::error!(
                            error = %release_error,
                            "the node lease stays held after a failed start"
                        );
                    }
                    close_stream_store(&stream_store).await;
                    return Err(error);
                }
            },
        };
        let flow_service = DexFlowService::new(
            &components.flow_service,
            FlowServiceDependencies {
                flow_operations: Arc::new(registry.clone()),
                stream_operations: StreamOperations::new(
                    stream_store.clone(),
                    components.flow_service.max_wait,
                ),
                attribute_indexes: Arc::new(AttributeIndexes::new(
                    object_stores.primary.clone(),
                    clock,
                )),
                search_backend,
            },
        );
        Ok(Some(Self {
            metrics,
            engine,
            registry,
            effect_tasks,
            dispatch,
            stream_store,
            flow_service,
            search_projection,
        }))
    }

    /// Serves until shutdown is requested or the node stops, then tears everything down.
    async fn serve(
        self,
        mut grpc: GrpcServer,
        metrics_server: Option<MetricsServer>,
        shutdown_request: CancellationToken,
        embedded_s3: Option<EmbeddedS3>,
    ) -> Result<(), ServerError> {
        let Self {
            metrics,
            engine,
            registry,
            effect_tasks,
            dispatch,
            stream_store,
            flow_service,
            search_projection,
        } = self;
        let worker_task_service =
            WorkerTaskServiceServer::new(dispatch.service_for_tenant(DEFAULT_TENANT_ID))
                .max_decoding_message_size(grpc.max_message_bytes)
                .max_encoding_message_size(grpc.max_message_bytes);
        grpc.flow_service_gate
            .activate(flow_service.into_server(grpc.max_message_bytes));
        grpc.worker_task_service_gate.activate(worker_task_service);
        grpc.health.set_serving().await;
        let node_stopped = engine.wait_for_stop();
        let registry_halted = registry.halted();
        let outcome = tokio::select! {
            () = shutdown_request.cancelled() => Ok(()),
            reason = node_stopped => Err(ServerError::NodeStopped { reason }),
            () = registry_halted.cancelled() => Err(ServerError::NodeStopped {
                reason: NodeStopReason::CommitPipelineFailed {
                    detail: "the Flow runtime registry halted".to_owned(),
                },
            }),
            served = &mut grpc.task => Err(match served {
                Ok(Ok(())) => ServerError::ServingTaskFailed {
                    detail: "the gRPC server stopped unexpectedly".to_owned(),
                },
                Ok(Err(source)) => ServerError::Serve { source },
                Err(error) => ServerError::ServingTaskFailed { detail: error.to_string() },
            }),
        };
        match &outcome {
            Ok(()) => tracing::info!("dex-server-zd shutting down"),
            Err(error) => tracing::error!(%error, "dex-server-zd stopping after a failure"),
        }
        metrics.set_role(dexzd_metrics::NodeRole::Draining);
        let mut teardown = BoundedTeardown::default();
        grpc.health.set_not_serving().await;
        dispatch.shutdown();
        registry.close_long_polls();
        grpc.stop.cancel();
        if !grpc.task.is_finished() {
            match tokio::time::timeout(GRPC_DRAIN_BUDGET, &mut grpc.task).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => {
                    tracing::warn!(%error, "gRPC server stopped with an error");
                }
                Ok(Err(error)) => tracing::warn!(%error, "gRPC server task failed"),
                Err(_) => {
                    tracing::warn!("open gRPC calls did not finish in time; closing them");
                    grpc.task.abort();
                }
            }
        }
        teardown
            .run(
                "effect tasks",
                EFFECT_TASKS_STOP_BUDGET,
                effect_tasks.shutdown(),
            )
            .await;
        if let Some(search_projection) = search_projection
            && !search_projection
                .shutdown(SEARCH_PROJECTION_STOP_BUDGET)
                .await
        {
            teardown.record_overrun("search projection", SEARCH_PROJECTION_STOP_BUDGET);
        }
        teardown
            .run_fallible("engine", ENGINE_STOP_BUDGET, engine.shutdown())
            .await;
        if let Some(metrics_server) = metrics_server {
            metrics_server.shutdown().await;
        }
        teardown
            .run(
                "Stream Store",
                STREAM_STORE_CLOSE_BUDGET,
                close_stream_store(&stream_store),
            )
            .await;
        if let Some(embedded_s3) = embedded_s3 {
            teardown
                .run(
                    "embedded S3",
                    EMBEDDED_S3_STOP_BUDGET,
                    embedded_s3.shutdown(),
                )
                .await;
        }
        teardown.finish(outcome)
    }
}

/// Returns builtin SearchFlows over committed views of the engine.
fn builtin_search_backend(
    config: &Config,
    engine: &EngineNode,
    partition_count: u32,
) -> Result<Arc<dyn SearchBackend>, ServerError> {
    let scan_budget =
        usize::try_from(config.search.builtin_scan_budget_documents).map_err(|_| {
            ServerError::ConfigValueOutOfRange {
                field: "search.builtinScanBudgetDocuments",
            }
        })?;
    let backend = BuiltinSearchBackend::with_scan_budget(
        Arc::new(CommittedSearchViews {
            committer: engine.committer().clone(),
        }),
        partition_count,
        scan_budget,
    )
    .map_err(|source| ServerError::SearchBackend { source })?;
    Ok(Arc::new(backend))
}

async fn close_stream_store(stream_store: &StreamStore) {
    if let Err(error) = stream_store.close().await {
        tracing::warn!(%error, "Stream Store did not close cleanly");
    }
}

/// Opens committed state views of the engine for builtin SearchFlows.
struct CommittedSearchViews {
    committer: CommandCommitter,
}

impl CommittedSearchViewProvider for CommittedSearchViews {
    fn committed_view(&self) -> Result<Arc<dyn FlowStateReader + Send + Sync>, SearchError> {
        let view = self
            .committer
            .committed_view()
            .map_err(|error| SearchError::Unavailable(error.to_string()))?;
        Ok(Arc::new(view))
    }
}
