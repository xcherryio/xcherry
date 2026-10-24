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

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dexzd_api::{
    AttributeIndexes, DexFlowService, FlowServiceDependencies, HealthStatus, StreamOperations,
};
use dexzd_apply::FlowStateReader;
use dexzd_clock::{SystemWallClock, WallClock};
use dexzd_engine::{
    CommandApplier, CommandCommitter, DeterministicCommandApplier, EffectDependencies,
    EffectRunner, EffectTasksHandle, EngineNode, EngineNodeDependencies, EngineRuntimeHooks,
    EngineServices, FlowRuntimeDependencies, FlowRuntimeRegistry, HardDeleteObjects,
    NodeMetaUploadLedger, NodeStopReason,
};
use dexzd_protocol::dex::worker_task_service_server::WorkerTaskServiceServer;
use dexzd_search::builtin::{BuiltinSearchBackend, CommittedSearchViewProvider};
use dexzd_search::{SearchBackend, SearchError};
use dexzd_stream_store::StreamStore;
use dexzd_worker_dispatch::WorkerDispatch;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::ServerError;
use crate::component_configs::ComponentConfigs;
use crate::config::{Config, SearchBackend as SearchBackendKind};
use crate::embedded_s3::EmbeddedS3;
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
    shutdown_request: CancellationToken,
    serving: JoinHandle<Result<(), ServerError>>,
}

impl RunningServer {
    /// Returns the address that serves FlowService, WorkerTaskService, and gRPC health.
    pub fn local_address(&self) -> SocketAddr {
        self.local_address
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
) -> Result<RunningServer, ServerError> {
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
    let started = ServerNode::start(config, &components).await;
    let node = match started {
        Ok(node) => node,
        Err(error) => {
            if let Some(embedded_s3) = embedded_s3 {
                embedded_s3.shutdown().await;
            }
            return Err(error);
        }
    };
    let shutdown_request = CancellationToken::new();
    let serving = tokio::spawn(node.serve(
        listener,
        components.grpc_max_message_bytes,
        shutdown_request.clone(),
        embedded_s3,
    ));
    tracing::info!(address = %local_address, "dex-server-zd serving");
    Ok(RunningServer {
        local_address,
        shutdown_request,
        serving,
    })
}

/// Every component of one node, in start order.
struct ServerNode {
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
    async fn start(config: &Config, components: &ComponentConfigs) -> Result<Self, ServerError> {
        let object_stores = ObjectStores::connect(
            &config.object_store,
            config.commit.multipart_threshold_bytes,
        )
        .await?;
        let clock: Arc<dyn WallClock> = Arc::new(SystemWallClock);
        let applier: Arc<dyn CommandApplier> = Arc::new(DeterministicCommandApplier);
        let dispatch = WorkerDispatch::new(components.matching.clone(), clock.clone());
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
        let started = EngineNode::start(
            &components.engine_node,
            EngineNodeDependencies {
                object_store: object_stores.primary.clone(),
                clock: clock.clone(),
                applier: applier.clone(),
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
                );
                let (effect_runner, effect_tasks) = EffectRunner::new(
                    &components.effects,
                    EffectDependencies {
                        dispatcher: Arc::new(dispatch.clone()),
                        hard_delete_objects: hard_delete_objects.clone(),
                        partition_hasher: partition_hasher.clone(),
                        clock: clock.clone(),
                        validation_config: components.validation().clone(),
                        stream_store: stream_store.clone(),
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
        )
        .await;
        let parts = runtime_parts
            .lock()
            .expect("runtime parts lock is poisoned only if a holder panicked")
            .take();
        let engine = match started {
            Ok(engine) => engine,
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
        Ok(Self {
            engine,
            registry,
            effect_tasks,
            dispatch,
            stream_store,
            flow_service,
            search_projection,
        })
    }

    /// Serves until shutdown is requested or the node stops, then tears everything down.
    async fn serve(
        self,
        listener: TcpListener,
        grpc_max_message_bytes: usize,
        shutdown_request: CancellationToken,
        embedded_s3: Option<EmbeddedS3>,
    ) -> Result<(), ServerError> {
        let Self {
            engine,
            registry,
            effect_tasks,
            dispatch,
            stream_store,
            flow_service,
            search_projection,
        } = self;
        let (health, health_server) = HealthStatus::new().await;
        let worker_task_service =
            WorkerTaskServiceServer::new(dispatch.service_for_tenant(DEFAULT_TENANT_ID))
                .max_decoding_message_size(grpc_max_message_bytes)
                .max_encoding_message_size(grpc_max_message_bytes);
        let grpc_stop = CancellationToken::new();
        let grpc_stopped = grpc_stop.clone();
        let mut grpc_server = tokio::spawn(
            tonic::transport::Server::builder()
                .http2_keepalive_interval(Some(KEEPALIVE_INTERVAL))
                .http2_keepalive_timeout(Some(KEEPALIVE_TIMEOUT))
                .layer(tower_layer::layer_fn(
                    client_deadline::client_deadline_layer,
                ))
                .add_service(health_server)
                .add_service(flow_service.into_server(grpc_max_message_bytes))
                .add_service(worker_task_service)
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async move { grpc_stopped.cancelled().await },
                ),
        );
        health.set_serving().await;
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
            served = &mut grpc_server => Err(match served {
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
        let mut teardown = BoundedTeardown::default();
        health.set_not_serving().await;
        dispatch.shutdown();
        registry.close_long_polls();
        grpc_stop.cancel();
        if !grpc_server.is_finished() {
            match tokio::time::timeout(GRPC_DRAIN_BUDGET, &mut grpc_server).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => {
                    tracing::warn!(%error, "gRPC server stopped with an error");
                }
                Ok(Err(error)) => tracing::warn!(%error, "gRPC server task failed"),
                Err(_) => {
                    tracing::warn!("open gRPC calls did not finish in time; closing them");
                    grpc_server.abort();
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
