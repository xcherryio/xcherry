// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use prometheus_client::encoding::{EncodeMetric, MetricEncoder, NoLabelSet};
use prometheus_client::metrics::MetricType;

const BOUNDS: [f64; 9] = [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, f64::MAX];

#[derive(Debug)]
struct HistogramState {
    buckets: [AtomicU64; 9],
    count: AtomicU64,
    sum_bits: AtomicU64,
}

/// Fixed commit-latency histogram with atomic, nonblocking observations.
#[derive(Clone, Debug)]
pub struct CommitLatency {
    state: Arc<HistogramState>,
}

impl Default for CommitLatency {
    fn default() -> Self {
        Self {
            state: Arc::new(HistogramState {
                buckets: std::array::from_fn(|_| AtomicU64::new(0)),
                count: AtomicU64::new(0),
                sum_bits: AtomicU64::new(0_f64.to_bits()),
            }),
        }
    }
}

impl CommitLatency {
    /// Records one completed batch's seal-to-ack latency in seconds.
    pub fn observe(&self, seconds: f64) {
        let bucket = BOUNDS
            .iter()
            .position(|bound| seconds <= *bound)
            .unwrap_or(8);
        self.state.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.state.count.fetch_add(1, Ordering::Relaxed);
        let mut previous = self.state.sum_bits.load(Ordering::Relaxed);
        loop {
            let next = (f64::from_bits(previous) + seconds).to_bits();
            match self.state.sum_bits.compare_exchange_weak(
                previous,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => previous = current,
            }
        }
    }
}

impl EncodeMetric for CommitLatency {
    fn encode(&self, mut encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
        let buckets: [(f64, u64); 9] = std::array::from_fn(|index| {
            (
                BOUNDS[index],
                self.state.buckets[index].load(Ordering::Relaxed),
            )
        });
        encoder.encode_histogram::<NoLabelSet>(
            f64::from_bits(self.state.sum_bits.load(Ordering::Relaxed)),
            self.state.count.load(Ordering::Relaxed),
            &buckets,
            None,
        )
    }

    fn metric_type(&self) -> MetricType {
        MetricType::Histogram
    }
}
