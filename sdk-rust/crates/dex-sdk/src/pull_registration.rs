// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::sync::Mutex;

use tokio::sync::watch;
use tonic::Status;

use crate::{SdkError, SdkResult};

/// Permanent rejection that leaves a pull queue without any registered stream.
///
/// Use [`SdkError::pull_registration_error`] on errors from [`crate::Worker::wait_pull_registered`] or
/// [`crate::Worker::start`] to inspect the queue and original gRPC status.
///
/// ```
/// fn report(error: dex_sdk::SdkError) {
///     if let Some(rejection) = error.pull_registration_error() {
///         eprintln!("queue {}: {}", rejection.queue, rejection.status.code());
///     }
/// }
/// ```
#[derive(Clone, Debug)]
pub struct PullRegistrationError {
    /// Rejected Flow type or app Worker ID queue.
    pub queue: String,
    /// Original permanent gRPC rejection, preserving its code and details.
    pub status: Status,
}

impl Display for PullRegistrationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "pull queue {} rejected: {}",
            self.queue, self.status
        )
    }
}

impl std::error::Error for PullRegistrationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.status)
    }
}

/// Independent snapshot of one queue's registration state.
#[derive(Clone, Debug)]
pub struct PullQueueRegistrationState {
    /// Flow type or app Worker ID queue name.
    pub queue: String,
    /// Whether the queue currently has an acknowledged stream.
    pub has_registered_stream: bool,
    /// Last permanent rejection, cleared when a stream successfully registers.
    pub last_permanent_error: Option<Status>,
}

/// Independent readiness snapshot returned by [`crate::Worker::pull_registration_state`].
#[derive(Clone, Debug, Default)]
pub struct PullRegistrationState {
    /// True after all queues register once, unless a queue has a permanent error and no registered stream.
    pub is_ready: bool,
    /// Queue snapshots sorted by name; empty for a listening Worker.
    pub queues: Vec<PullQueueRegistrationState>,
}

#[derive(Default)]
struct QueueRegistration {
    has_registered: bool,
    registered_streams: usize,
    last_permanent_error: Option<Status>,
}

#[derive(Default)]
struct RegistrationState {
    queues: BTreeMap<String, QueueRegistration>,
    has_registered_all_queues: bool,
    initial_error: Option<SdkError>,
    permanent_error: Option<PullRegistrationError>,
    has_stopped: bool,
}

pub(crate) struct PullRegistration {
    state: Mutex<RegistrationState>,
    changed: watch::Sender<()>,
}

impl Default for PullRegistration {
    fn default() -> Self {
        Self::new(&[])
    }
}

impl PullRegistration {
    pub(crate) fn new(queues: &[String]) -> Self {
        Self {
            state: Mutex::new(RegistrationState {
                queues: queues
                    .iter()
                    .map(|queue| (queue.clone(), QueueRegistration::default()))
                    .collect(),
                ..RegistrationState::default()
            }),
            changed: watch::channel(()).0,
        }
    }

    pub(crate) fn registered(&self, queue: &str) {
        let mut state = self.state.lock().expect("pull registration lock");
        let registration = state.queues.entry(queue.to_string()).or_default();
        registration.registered_streams += 1;
        registration.has_registered = true;
        registration.last_permanent_error = None;
        state.has_registered_all_queues |= state.queues.values().all(|queue| queue.has_registered);
        self.changed.send_replace(());
    }

    pub(crate) fn ended(&self, queue: &str, was_registered: bool, failure: Option<&Status>) {
        let mut state = self.state.lock().expect("pull registration lock");
        let registration = state.queues.entry(queue.to_string()).or_default();
        if was_registered {
            registration.registered_streams -= 1;
        }
        if let Some(status) = failure {
            registration.last_permanent_error = Some(status.clone());
            if registration.registered_streams == 0 {
                let rejection = PullRegistrationError {
                    queue: queue.to_string(),
                    status: status.clone(),
                };
                if !state.has_registered_all_queues && state.initial_error.is_none() {
                    state.initial_error = Some(SdkError::from_pull_registration(rejection.clone()));
                }
                state.permanent_error.get_or_insert(rejection);
            }
        }
        self.changed.send_replace(());
    }

    pub(crate) fn finish(&self, failure: Option<SdkError>) {
        let mut state = self.state.lock().expect("pull registration lock");
        if state.initial_error.is_none() {
            state.initial_error = failure;
        }
        state.has_stopped = true;
        self.changed.send_replace(());
    }

    pub(crate) fn permanent_error(&self) -> Option<PullRegistrationError> {
        self.state
            .lock()
            .expect("pull registration lock")
            .permanent_error
            .clone()
    }

    pub(crate) fn snapshot(&self) -> PullRegistrationState {
        let state = self.state.lock().expect("pull registration lock");
        let queues: Vec<_> = state
            .queues
            .iter()
            .map(|(queue, registration)| PullQueueRegistrationState {
                queue: queue.clone(),
                has_registered_stream: registration.registered_streams > 0,
                last_permanent_error: registration.last_permanent_error.clone(),
            })
            .collect();
        PullRegistrationState {
            is_ready: state.has_registered_all_queues
                && !queues.iter().any(|queue| {
                    !queue.has_registered_stream && queue.last_permanent_error.is_some()
                }),
            queues,
        }
    }

    pub(crate) async fn wait(&self) -> SdkResult<()> {
        let mut changed = self.changed.subscribe();
        loop {
            {
                let state = self.state.lock().expect("pull registration lock");
                if state.has_registered_all_queues {
                    return Ok(());
                }
                if let Some(error) = &state.initial_error {
                    return Err(error.clone());
                }
                if state.has_stopped {
                    return Err(SdkError::InvalidArgument {
                        message: "pull Worker stopped before registration".to_string(),
                    });
                }
            }
            changed
                .changed()
                .await
                .expect("registration sender remains alive");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn readiness_latches_and_recovers_from_permanent_rejections() {
        let registration = PullRegistration::new(&["app".to_string(), "flow".to_string()]);
        registration.registered("app");
        assert!(!registration.snapshot().is_ready);
        registration.registered("flow");
        registration.wait().await.expect("all queues registered");
        let snapshot = registration.snapshot();
        registration.ended("flow", true, None);
        assert!(registration.snapshot().is_ready);
        registration.ended("flow", false, Some(&Status::permission_denied("rejected")));
        assert!(!registration.snapshot().is_ready);
        registration
            .wait()
            .await
            .expect("completion remains latched");
        registration.registered("flow");
        assert!(registration.snapshot().is_ready);
        assert!(
            registration.snapshot().queues[1]
                .last_permanent_error
                .is_none()
        );
        registration.finish(None);
        registration.ended("app", true, None);
        registration.ended("flow", true, None);
        assert!(registration.snapshot().is_ready);
        assert!(
            snapshot
                .queues
                .iter()
                .all(|queue| queue.has_registered_stream)
        );
    }

    #[tokio::test]
    async fn another_registered_stream_prevents_a_fatal_rejection() {
        let registration = PullRegistration::new(&["app".to_string(), "flow".to_string()]);
        registration.registered("app");
        registration.ended("app", false, Some(&Status::invalid_argument("rejected")));
        assert!(registration.permanent_error().is_none());
        registration.registered("flow");
        registration.wait().await.expect("all queues registered");
        assert!(registration.snapshot().is_ready);
        registration.ended("app", true, None);
        assert!(!registration.snapshot().is_ready);
        registration.registered("app");
        assert!(registration.snapshot().is_ready);
    }

    #[tokio::test]
    async fn stopping_before_readiness_wakes_all_waiters() {
        let registration = std::sync::Arc::new(PullRegistration::new(&["app".to_string()]));
        let first = std::sync::Arc::clone(&registration);
        let second = std::sync::Arc::clone(&registration);
        let waiting_first = tokio::spawn(async move { first.wait().await });
        let waiting_second = tokio::spawn(async move { second.wait().await });
        registration.finish(None);
        assert!(waiting_first.await.expect("first waiter").is_err());
        assert!(waiting_second.await.expect("second waiter").is_err());
    }
}
