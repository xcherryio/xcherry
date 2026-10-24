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

pub(crate) const API_LATENCY_BOUNDS: [f64; 6] = [0.005, 0.025, 0.1, 0.5, 2.5, f64::MAX];
pub(crate) const FLOW_LATENCY_BOUNDS: [f64; 8] =
    [1.0, 10.0, 60.0, 600.0, 3600.0, 21600.0, 86400.0, f64::MAX];
pub(crate) const SCHEDULE_LATENCY_BOUNDS: [f64; 7] = [0.005, 0.025, 0.1, 0.5, 2.5, 10.0, f64::MAX];

#[derive(Debug)]
struct HistogramState<const N: usize> {
    buckets: [AtomicU64; N],
    count: AtomicU64,
    sum_bits: AtomicU64,
}

/// A fixed-bucket histogram with atomic observations.
#[derive(Clone, Debug)]
pub struct AtomicHistogram<const N: usize> {
    bounds: &'static [f64; N],
    state: Arc<HistogramState<N>>,
}

impl<const N: usize> AtomicHistogram<N> {
    /// Creates handles sharing a fixed bucket set.
    pub fn new(bounds: &'static [f64; N]) -> Self {
        Self {
            bounds,
            state: Arc::new(HistogramState {
                buckets: std::array::from_fn(|_| AtomicU64::new(0)),
                count: AtomicU64::new(0),
                sum_bits: AtomicU64::new(0_f64.to_bits()),
            }),
        }
    }

    /// Records a duration in seconds without allocating or locking.
    pub fn observe(&self, seconds: f64) {
        let bucket = self
            .bounds
            .iter()
            .position(|bound| seconds <= *bound)
            .unwrap_or(N - 1);
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

impl<const N: usize> EncodeMetric for AtomicHistogram<N> {
    fn encode(&self, mut encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
        let buckets: [(f64, u64); N] = std::array::from_fn(|index| {
            (
                self.bounds[index],
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
