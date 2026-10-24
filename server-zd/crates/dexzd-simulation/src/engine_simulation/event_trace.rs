// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! The ordered trace of simulation events, exactly as they happened.

use std::sync::Mutex;

use tokio::time::Instant;
use xxhash_rust::xxh3::Xxh3;

use crate::engine_simulation::simulated_clock::elapsed_micros;

/// Events kept for a failure report.
const RECENT_EVENT_COUNT: usize = 400;

/// Records events in order and hashes them as they arrive.
pub(crate) struct EventTrace {
    origin: Instant,
    state: Mutex<TraceState>,
}

struct TraceState {
    hasher: Xxh3,
    events: Vec<String>,
}

impl EventTrace {
    pub(crate) fn new(origin: Instant) -> Self {
        Self {
            origin,
            state: Mutex::new(TraceState {
                hasher: Xxh3::new(),
                events: Vec::new(),
            }),
        }
    }

    /// Appends one event, stamped with the simulated time.
    pub(crate) fn record(&self, event: &str) {
        let mut state = self.state.lock().expect("trace lock is not poisoned");
        let line = format!("{:>12} {event}", elapsed_micros(self.origin));
        state.hasher.update(line.as_bytes());
        state.hasher.update(b"\n");
        state.events.push(line);
    }

    /// Returns the hash of every event so far.
    pub(crate) fn digest(&self) -> u64 {
        self.state
            .lock()
            .expect("trace lock is not poisoned")
            .hasher
            .digest()
    }

    pub(crate) fn event_count(&self) -> u64 {
        self.state
            .lock()
            .expect("trace lock is not poisoned")
            .events
            .len() as u64
    }

    /// Returns the last events, oldest first.
    pub(crate) fn recent_events(&self) -> Vec<String> {
        let state = self.state.lock().expect("trace lock is not poisoned");
        let first = state.events.len().saturating_sub(RECENT_EVENT_COUNT);
        state.events[first..].to_vec()
    }

    /// Returns every event, oldest first.
    pub(crate) fn all_events(&self) -> Vec<String> {
        self.state
            .lock()
            .expect("trace lock is not poisoned")
            .events
            .clone()
    }
}
