// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! The ordered trace of simulation events, with UUIDs replaced by first-appearance ordinals.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;

use tokio::time::Instant;
use xxhash_rust::xxh3::Xxh3;

use crate::engine_simulation::simulated_clock::elapsed_micros;

/// Events kept for a failure report.
const RECENT_EVENT_COUNT: usize = 400;
/// Length of a hyphenated UUID such as `0190a6f1-7c2e-7d4f-9a3b-5c6d7e8f9a0b`.
const HYPHENATED_UUID_LENGTH: usize = 36;

/// Records events in order and hashes them as they arrive.
pub(crate) struct EventTrace {
    origin: Instant,
    state: Mutex<TraceState>,
}

struct TraceState {
    hasher: Xxh3,
    event_count: u64,
    recent_events: VecDeque<String>,
    uuid_ordinals: BTreeMap<String, usize>,
}

impl EventTrace {
    pub(crate) fn new(origin: Instant) -> Self {
        Self {
            origin,
            state: Mutex::new(TraceState {
                hasher: Xxh3::new(),
                event_count: 0,
                recent_events: VecDeque::with_capacity(RECENT_EVENT_COUNT),
                uuid_ordinals: BTreeMap::new(),
            }),
        }
    }

    /// Appends one event, stamped with the simulated time.
    pub(crate) fn record(&self, event: &str) {
        let mut state = self.state.lock().expect("trace lock is not poisoned");
        let canonical = canonicalize_uuids(event, &mut state.uuid_ordinals);
        let line = format!("{:>12} {canonical}", elapsed_micros(self.origin));
        state.hasher.update(line.as_bytes());
        state.hasher.update(b"\n");
        state.event_count += 1;
        if state.recent_events.len() == RECENT_EVENT_COUNT {
            state.recent_events.pop_front();
        }
        state.recent_events.push_back(line);
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
            .event_count
    }

    /// Returns the last events, oldest first.
    pub(crate) fn recent_events(&self) -> Vec<String> {
        self.state
            .lock()
            .expect("trace lock is not poisoned")
            .recent_events
            .iter()
            .cloned()
            .collect()
    }
}

/// Replaces each hyphenated UUID with `uuid#N` by first appearance; engine UUIDs follow the
/// real clock.
fn canonicalize_uuids(text: &str, ordinals: &mut BTreeMap<String, usize>) -> String {
    let bytes = text.as_bytes();
    let mut canonical = String::with_capacity(text.len());
    let mut copied_until = 0;
    let mut index = 0;
    while index + HYPHENATED_UUID_LENGTH <= bytes.len() {
        let candidate = &bytes[index..index + HYPHENATED_UUID_LENGTH];
        if is_hyphenated_uuid(candidate) {
            canonical.push_str(&text[copied_until..index]);
            let uuid = text[index..index + HYPHENATED_UUID_LENGTH].to_ascii_lowercase();
            let next_ordinal = ordinals.len();
            let ordinal = *ordinals.entry(uuid).or_insert(next_ordinal);
            canonical.push_str(&format!("uuid#{ordinal}"));
            index += HYPHENATED_UUID_LENGTH;
            copied_until = index;
        } else {
            index += 1;
        }
    }
    canonical.push_str(&text[copied_until..]);
    canonical
}

fn is_hyphenated_uuid(candidate: &[u8]) -> bool {
    candidate.iter().enumerate().all(|(position, byte)| {
        if matches!(position, 8 | 13 | 18 | 23) {
            *byte == b'-'
        } else {
            byte.is_ascii_hexdigit()
        }
    })
}
