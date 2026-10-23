// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Wall-clock abstraction that lets server-zd run on real time in production and on simulated time in tests.
//!
//! Only wall-clock reads go through [`WallClock`]: command timestamps and lease expiry. Timers and
//! sleeps use `tokio::time`, which tests pause and advance.

use std::time::{SystemTime, UNIX_EPOCH};

/// Source of wall-clock time in microseconds since the Unix epoch.
pub trait WallClock: Send + Sync + 'static {
    /// Returns the current wall-clock time in microseconds since the Unix epoch.
    ///
    /// The value can move backwards when the host clock is adjusted; callers that need
    /// monotonic timestamps must enforce it themselves.
    fn now_unix_micros(&self) -> i64;
}

/// [`WallClock`] backed by the operating system clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemWallClock;

impl WallClock for SystemWallClock {
    fn now_unix_micros(&self) -> i64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(elapsed) => i64::try_from(elapsed.as_micros()).unwrap_or(i64::MAX),
            Err(before_epoch) => i64::try_from(before_epoch.duration().as_micros())
                .map_or(i64::MIN, |micros| -micros),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{SystemWallClock, WallClock};

    #[test]
    fn system_wall_clock_reports_time_after_2026() {
        let january_first_2026_micros = 1_767_225_600_000_000;
        assert!(SystemWallClock.now_unix_micros() > january_first_2026_micros);
    }
}
