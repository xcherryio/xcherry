// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

mod client_deadline;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dexzd_api::{
    AttributeIndexes, DexFlowService, FlowServiceDependencies, HealthStatus, StreamOperations,
};
use dexzd_clock::{SystemWallClock, WallClock};
use dexzd_engine::{
    CommandApplier, DeterministicCommandApplier, EffectDependencies, EffectRunner,
    EffectTasksHandle, EngineNode, EngineNodeDependencies, EngineRuntimeHooks, EngineServices,
    FlowRuntimeDependencies, FlowRuntimeRegistry, HardDeleteObjects, NodeMetaUploadLedger,
    NodeStopReason,
};
use dexzd_protocol::dex::worker_task_service_server::WorkerTaskServiceServer;
use dexzd_search::SearchBackend;
use dexzd_search::builtin::BuiltinSearchBackend;
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

/// Total time allowed for a graceful shutdown before remaining work is abandoned.
const SHUTDOWN_BUDGET: Duration = Duration::from_secs(10);
/// Part of the budget for open calls, so the engine still stops cleanly after a stuck call.
const GRPC_DRAIN_BUDGET: Duration = Duration::from_secs(4);
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

    /// Stops gracefully within ten seconds and returns how serving ended.
    ///
    /// Returns [`ServerError::NodeStopped`] when the node had already stopped for another reason,
    /// such as a lost lease.
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
                        hard_delete_reader: Arc::new(hard_delete_objects),
                    },
                );
                let effect_tasks = effect_tasks.start(Arc::new(registry.clone()));
                let hooks = EngineRuntimeHooks {
                    committed_commands: Arc::new(RegistryCommittedCommandSink::new(
                        registry.clone(),
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
                search_backend: search_backend(config, &engine)?,
            },
        );
        Ok(Self {
            engine,
            registry,
            effect_tasks,
            dispatch,
            stream_store,
            flow_service,
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
        let teardown = async {
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
            effect_tasks.shutdown().await;
            engine.shutdown().await;
            close_stream_store(&stream_store).await;
            if let Some(embedded_s3) = embedded_s3 {
                embedded_s3.shutdown().await;
            }
        };
        if tokio::time::timeout(SHUTDOWN_BUDGET, teardown)
            .await
            .is_err()
        {
            grpc_server.abort();
            tracing::error!(
                budget_seconds = SHUTDOWN_BUDGET.as_secs(),
                "graceful shutdown exceeded its budget; remaining work was abandoned"
            );
        }
        outcome
    }
}

/// Returns the configured SearchFlows backend, or `None` while the backend is not available.
fn search_backend(
    config: &Config,
    engine: &EngineNode,
) -> Result<Option<Arc<dyn SearchBackend>>, ServerError> {
    match config.search.backend {
        SearchBackendKind::Builtin => {
            let partition_count = u16::try_from(config.cluster.partition_count).map_err(|_| {
                ServerError::ConfigValueOutOfRange {
                    field: "cluster.partitionCount",
                }
            })?;
            let scan_budget = usize::try_from(config.search.builtin_scan_budget_documents)
                .map_err(|_| ServerError::ConfigValueOutOfRange {
                    field: "search.builtinScanBudgetDocuments",
                })?;
            let backend = BuiltinSearchBackend::with_scan_budget(
                Arc::new(engine.committer().committed_view()),
                partition_count,
                scan_budget,
            )
            .map_err(|source| ServerError::SearchBackend { source })?;
            Ok(Some(Arc::new(backend)))
        }
        SearchBackendKind::Paradedb => {
            tracing::warn!(
                "the ParadeDB search backend is not available yet; SearchFlows returns Unimplemented"
            );
            Ok(None)
        }
    }
}

async fn close_stream_store(stream_store: &StreamStore) {
    if let Err(error) = stream_store.close().await {
        tracing::warn!(%error, "Stream Store did not close cleanly");
    }
}
