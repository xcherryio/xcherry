// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use prometheus_client::encoding::{EncodeMetric, MetricEncoder};
use prometheus_client::metrics::MetricType;

/// Oldest due timer age across partitions, without per-partition series.
#[derive(Clone, Debug)]
pub struct TimerLag {
    oldest_due_by_partition: Arc<Vec<AtomicI64>>,
}

impl TimerLag {
    pub(crate) fn new(partition_count: usize) -> Self {
        Self {
            oldest_due_by_partition: Arc::new(
                (0..partition_count).map(|_| AtomicI64::new(0)).collect(),
            ),
        }
    }

    /// Sets the oldest due, unfired timer in one partition, or clears it.
    pub fn set_due(&self, partition: usize, fire_at_unix_micros: Option<i64>) {
        if let Some(oldest) = self.oldest_due_by_partition.get(partition) {
            oldest.store(fire_at_unix_micros.unwrap_or(0), Ordering::Relaxed);
        }
    }
}

impl EncodeMetric for TimerLag {
    fn encode(&self, mut encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
        let oldest = self
            .oldest_due_by_partition
            .iter()
            .map(|value| value.load(Ordering::Relaxed))
            .filter(|value| *value > 0)
            .min();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        let lag = oldest.map_or(0.0, |fire_at| (now - fire_at as f64 / 1_000_000.0).max(0.0));
        encoder.encode_gauge(&lag)
    }

    fn metric_type(&self) -> MetricType {
        MetricType::Gauge
    }
}
