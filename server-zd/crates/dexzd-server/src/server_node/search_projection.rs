// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

mod committed_search_frames;
mod projection_worker;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use dexzd_engine::{CommandCommitter, NodeStopReason};
use dexzd_search::SearchBackend;
use dexzd_search::paradedb::{ParadeDbProjector, ParadeDbSearchBackend, ProjectionVisibilityWait};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::ServerError;
use crate::config::{ConfigError, SearchConfig};

pub(crate) use committed_search_frames::CommittedSearchFrames;

use committed_search_frames::CommittedSearchFrame;
use projection_worker::SearchProjectionWorker;

/// Frames waiting for the projection; when full, commits continue and the partition rebuilds later.
const PROJECTION_QUEUE_FRAMES: usize = 4_096;
/// Search document bytes waiting for the projection; beyond it, frames also become rebuilds.
const PROJECTION_QUEUE_BYTES: usize = 64 * 1024 * 1024;
/// Startup waits for another node's schema migration, which holds its lock for at most 5 s.
const CONNECT_ATTEMPTS: u32 = 3;
const CONNECT_RETRY_DELAY: Duration = Duration::from_secs(1);
const DEFAULT_SCHEMA: &str = "public";

/// The ParadeDB SearchFlows backend and the committed frames that its projection consumes.
pub(crate) struct ParadeDbSearch {
    backend: Arc<ParadeDbSearchBackend>,
    frames: Arc<CommittedSearchFrames>,
    queue: mpsc::Receiver<CommittedSearchFrame>,
}

impl ParadeDbSearch {
    /// Connects the pool and ensures the schema; call it before the engine replays the log.
    pub(crate) async fn connect(search: &SearchConfig) -> Result<Self, ServerError> {
        let configuration = postgres_configuration(search)?;
        let pool_size = usize::try_from(search.parade_db_connection_pool_size).map_err(|_| {
            ServerError::ConfigValueOutOfRange {
                field: "search.paradeDbConnectionPoolSize",
            }
        })?;
        let (frames, queue) =
            CommittedSearchFrames::new(PROJECTION_QUEUE_FRAMES, PROJECTION_QUEUE_BYTES);
        let frames = Arc::new(frames);
        let visibility_wait: Arc<dyn ProjectionVisibilityWait> = frames.clone();
        let mut attempt = 1;
        let backend = loop {
            match ParadeDbSearchBackend::connect(
                &configuration,
                pool_size,
                Some(visibility_wait.clone()),
                search.read_your_writes_wait,
            )
            .await
            {
                Ok(backend) => break backend,
                Err(error) if attempt < CONNECT_ATTEMPTS => {
                    tracing::warn!(%error, attempt, "connecting to ParadeDB failed; retrying");
                    attempt += 1;
                    tokio::time::sleep(CONNECT_RETRY_DELAY).await;
                }
                Err(source) => return Err(ServerError::SearchBackend { source }),
            }
        };
        tracing::info!(
            pool_size,
            "connected to ParadeDB and ensured its search schema"
        );
        Ok(Self {
            backend: Arc::new(backend),
            frames,
            queue,
        })
    }

    /// Returns the frames that the engine's committed-command sink feeds.
    pub(crate) fn committed_frames(&self) -> Arc<CommittedSearchFrames> {
        self.frames.clone()
    }

    pub(crate) fn search_backend(&self) -> Arc<dyn SearchBackend> {
        self.backend.clone()
    }

    /// Starts projecting in the background; it stops at `node_stopped` or [`SearchProjection::shutdown`].
    pub(crate) fn start_projection(
        self,
        committer: CommandCommitter,
        partition_count: u32,
        node_stopped: impl Future<Output = NodeStopReason> + Send + 'static,
    ) -> SearchProjection {
        let worker = SearchProjectionWorker::new(
            ParadeDbProjector::new(self.backend),
            committer,
            self.frames,
            self.queue,
            partition_count,
        );
        let stop = CancellationToken::new();
        let stopped = stop.clone();
        let task = tokio::spawn(async move {
            tokio::select! {
                () = stopped.cancelled() => {}
                reason = node_stopped => {
                    tracing::info!(%reason, "the search projection stopped with the node");
                }
                () = worker.run() => {}
            }
        });
        SearchProjection { stop, task }
    }
}

/// The running ParadeDB projection; only the partition owner may project, so stop it before the lease is released.
pub(crate) struct SearchProjection {
    stop: CancellationToken,
    task: JoinHandle<()>,
}

impl SearchProjection {
    /// Stops the projection, aborting it when it does not stop within `budget`.
    ///
    /// Returns false when the projection had to be aborted.
    pub(crate) async fn shutdown(mut self, budget: Duration) -> bool {
        self.stop.cancel();
        match tokio::time::timeout(budget, &mut self.task).await {
            Ok(Ok(())) => true,
            Ok(Err(error)) => {
                tracing::warn!(%error, "the search projection task failed");
                true
            }
            Err(_) => {
                self.task.abort();
                false
            }
        }
    }
}

/// Applies `search.paradeDbSchema` through the search path, as transaction poolers keep no session state.
fn postgres_configuration(search: &SearchConfig) -> Result<tokio_postgres::Config, ServerError> {
    let mut configuration = search
        .parade_db_connection_string
        .parse::<tokio_postgres::Config>()
        .map_err(|_| {
            ServerError::Config(ConfigError::Invalid {
                field: "search.paradeDbConnectionString",
                message: "is not a valid PostgreSQL connection string".to_owned(),
            })
        })?;
    if search.parade_db_schema != DEFAULT_SCHEMA {
        let search_path = format!("-c search_path={},public,pdb", search.parade_db_schema);
        let options = match configuration.get_options() {
            Some(options) => format!("{options} {search_path}"),
            None => search_path,
        };
        configuration.options(options);
    }
    Ok(configuration)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Only the lease owner may project, so a projection that ignores cancellation must end before release.
    #[tokio::test(start_paused = true)]
    async fn a_projection_that_ignores_cancellation_is_aborted() {
        let (liveness, mut is_alive) = mpsc::channel::<()>(1);
        let task = tokio::spawn(async move {
            let _liveness = liveness;
            std::future::pending::<()>().await;
        });
        let projection = SearchProjection {
            stop: CancellationToken::new(),
            task,
        };
        assert!(!projection.shutdown(Duration::from_secs(1)).await);
        assert!(
            is_alive.recv().await.is_none(),
            "the aborted task released its state"
        );
    }
}
