// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Wall clocks that follow Tokio's paused clock, each with its own adjustable offset.

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use dexzd_clock::WallClock;
use tokio::time::Instant;

/// Unix time at the simulation start, 2027-01-15, matching the apply simulation.
pub(crate) const START_UNIX_MICROS: i64 = 1_800_000_000_000_000;

/// A wall clock of one process or of object storage. Jumps change only this clock.
pub(crate) struct SimulatedWallClock {
    origin: Instant,
    offset_micros: AtomicI64,
}

impl SimulatedWallClock {
    pub(crate) fn new(origin: Instant, offset_micros: i64) -> Self {
        Self {
            origin,
            offset_micros: AtomicI64::new(offset_micros),
        }
    }

    /// Moves the clock by `micros`, which may be negative.
    pub(crate) fn jump(&self, micros: i64) {
        self.offset_micros.fetch_add(micros, Ordering::SeqCst);
    }

    pub(crate) fn offset_micros(&self) -> i64 {
        self.offset_micros.load(Ordering::SeqCst)
    }
}

impl WallClock for SimulatedWallClock {
    fn now_unix_micros(&self) -> i64 {
        START_UNIX_MICROS + elapsed_micros(self.origin) + self.offset_micros()
    }
}

/// Simulated time since `origin` in microseconds.
pub(crate) fn elapsed_micros(origin: Instant) -> i64 {
    duration_micros(origin.elapsed())
}

pub(crate) fn duration_micros(duration: Duration) -> i64 {
    i64::try_from(duration.as_micros()).expect("simulated durations fit in i64 microseconds")
}
