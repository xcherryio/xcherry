// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;

use prometheus_client::encoding::{EncodeLabelSet, EncodeMetric, MetricEncoder, NoLabelSet};
use prometheus_client::metrics::MetricType;
use prometheus_client::metrics::counter::Counter;

/// A persistent Worker stream event.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum WorkerStreamEvent {
    /// A task was delivered to a Worker.
    Delivered,
    /// A Worker stream closed.
    Closed,
}

label_value!(WorkerStreamEvent { Delivered => "delivered", Closed => "closed" });

impl WorkerStreamEvent {
    const ALL: [Self; 2] = [Self::Delivered, Self::Closed];

    const fn index(self) -> usize {
        match self {
            Self::Delivered => 0,
            Self::Closed => 1,
        }
    }
}

#[derive(Clone, Debug, EncodeLabelSet)]
struct WorkerStreamEventLabel {
    event: WorkerStreamEvent,
}

/// Delivered tasks and closed persistent Worker streams.
#[derive(Clone, Debug)]
pub struct WorkerStreamEvents {
    counts: Arc<[Counter; 2]>,
}

impl Default for WorkerStreamEvents {
    fn default() -> Self {
        Self {
            counts: Arc::new(std::array::from_fn(|_| Counter::default())),
        }
    }
}

impl WorkerStreamEvents {
    /// Records one task delivery or stream close.
    pub fn record(&self, event: WorkerStreamEvent) {
        self.counts[event.index()].inc();
    }
}

impl EncodeMetric for WorkerStreamEvents {
    fn encode(&self, mut encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
        for event in WorkerStreamEvent::ALL {
            encoder
                .encode_family(&WorkerStreamEventLabel { event })?
                .encode_counter::<NoLabelSet, _, u64>(&self.counts[event.index()].get(), None)?;
        }
        Ok(())
    }

    fn metric_type(&self) -> MetricType {
        MetricType::Counter
    }
}
