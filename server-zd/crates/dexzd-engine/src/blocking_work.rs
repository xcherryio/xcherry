// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Where the engine runs synchronous local store and snapshot file work.

use std::any::Any;
use std::fmt;
use std::future::Future;
#[cfg(feature = "test-util")]
use std::panic::{AssertUnwindSafe, catch_unwind};

use tokio_util::task::TaskTracker;

/// Where the engine runs synchronous local store reads and writes, frame applies, and snapshot
/// file work.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum BlockingWorkPlacement {
    /// Tokio's blocking thread pool, so a slow store call never stalls async tasks. The default.
    #[default]
    BlockingThreadPool,
    /// The calling task's thread, for single-threaded simulation; a slow store call stalls the
    /// runtime.
    #[cfg(feature = "test-util")]
    CallingTaskForTest,
}

/// Blocking work that panicked, or that the runtime dropped before it ran.
pub(crate) enum BlockingWorkFailure {
    /// The work panicked; the payload can resume the panic.
    Panicked(Box<dyn Any + Send + 'static>),
    /// The runtime shut down before the work finished.
    Cancelled,
}

impl fmt::Display for BlockingWorkFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Panicked(payload) => {
                let message = payload
                    .downcast_ref::<&str>()
                    .map(|message| (*message).to_owned())
                    .or_else(|| payload.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "a non-text panic payload".to_owned());
                write!(formatter, "blocking work panicked: {message}")
            }
            Self::Cancelled => formatter.write_str("blocking work was cancelled"),
        }
    }
}

impl From<tokio::task::JoinError> for BlockingWorkFailure {
    fn from(error: tokio::task::JoinError) -> Self {
        match error.try_into_panic() {
            Ok(payload) => Self::Panicked(payload),
            Err(_) => Self::Cancelled,
        }
    }
}

impl BlockingWorkPlacement {
    /// Runs `work` where this placement says.
    ///
    /// Returns the output of `work`, or how it failed: a panic, or a runtime that dropped it.
    pub(crate) async fn run<T: Send + 'static>(
        self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> Result<T, BlockingWorkFailure> {
        match self {
            Self::BlockingThreadPool => Ok(tokio::task::spawn_blocking(work).await?),
            #[cfg(feature = "test-util")]
            Self::CallingTaskForTest => run_on_calling_task(work),
        }
    }
}

impl BlockingWorkPlacement {
    /// Runs `work` like [`Self::run`]; `tracker` tracks thread-pool work, so its shutdown waits.
    ///
    /// Returns the output of `work`, or how it failed: a panic, or a runtime that dropped it.
    pub(crate) fn run_tracked<T, Work>(
        self,
        tracker: &TaskTracker,
        work: Work,
    ) -> impl Future<Output = Result<T, BlockingWorkFailure>> + use<T, Work>
    where
        T: Send + 'static,
        Work: FnOnce() -> T + Send + 'static,
    {
        let handle = match self {
            Self::BlockingThreadPool => Ok(tracker.spawn_blocking(work)),
            #[cfg(feature = "test-util")]
            Self::CallingTaskForTest => Err(run_on_calling_task(work)),
        };
        async move {
            match handle {
                Ok(handle) => Ok(handle.await?),
                Err(output) => output,
            }
        }
    }
}

/// Runs `work` on the calling task and catches its panic, as `spawn_blocking` does.
///
/// Returns the output of `work`, or its panic.
#[cfg(feature = "test-util")]
pub(crate) fn run_on_calling_task<T>(work: impl FnOnce() -> T) -> Result<T, BlockingWorkFailure> {
    catch_unwind(AssertUnwindSafe(work)).map_err(BlockingWorkFailure::Panicked)
}
