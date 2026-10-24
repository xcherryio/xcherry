// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;

use prometheus_client::encoding::{EncodeMetric, MetricEncoder, NoLabelSet};
use prometheus_client::metrics::MetricType;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;

use crate::{NodeRole, ObjectOperation, ObjectOperationLabel, Outcome, OutcomeLabel, RoleLabel};

/// Fixed lifecycle gauges; exactly one role has value one.
#[derive(Clone, Debug)]
pub struct RoleGauges {
    values: Arc<[Gauge; 5]>,
}

impl Default for RoleGauges {
    fn default() -> Self {
        Self {
            values: Arc::new(std::array::from_fn(|_| Gauge::default())),
        }
    }
}

impl RoleGauges {
    pub(crate) fn set(&self, current: NodeRole) {
        for (index, role) in NodeRole::ALL.into_iter().enumerate() {
            self.values[index].set(i64::from(role == current));
        }
    }
}

impl EncodeMetric for RoleGauges {
    fn encode(&self, mut encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
        for (index, role) in NodeRole::ALL.into_iter().enumerate() {
            encoder
                .encode_family(&RoleLabel { role })?
                .encode_gauge(&self.values[index].get())?;
        }
        Ok(())
    }

    fn metric_type(&self) -> MetricType {
        MetricType::Gauge
    }
}

/// Fixed success and failure counters.
#[derive(Clone, Debug)]
pub struct OutcomeCounters {
    values: Arc<[Counter; 2]>,
}

impl Default for OutcomeCounters {
    fn default() -> Self {
        Self {
            values: Arc::new(std::array::from_fn(|_| Counter::default())),
        }
    }
}

impl OutcomeCounters {
    pub(crate) fn record(&self, outcome: Outcome) {
        self.values[match outcome {
            Outcome::Ok => 0,
            Outcome::Error => 1,
        }]
        .inc();
    }
}

impl EncodeMetric for OutcomeCounters {
    fn encode(&self, mut encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
        for (index, outcome) in [Outcome::Ok, Outcome::Error].into_iter().enumerate() {
            encoder
                .encode_family(&OutcomeLabel { outcome })?
                .encode_counter::<NoLabelSet, _, u64>(&self.values[index].get(), None)?;
        }
        Ok(())
    }

    fn metric_type(&self) -> MetricType {
        MetricType::Counter
    }
}

/// Fixed failed-object-request counters.
#[derive(Clone, Debug)]
pub struct ObjectErrorCounters {
    values: Arc<[Counter; 4]>,
}

impl Default for ObjectErrorCounters {
    fn default() -> Self {
        Self {
            values: Arc::new(std::array::from_fn(|_| Counter::default())),
        }
    }
}

impl ObjectErrorCounters {
    pub(crate) fn record(&self, operation: ObjectOperation) {
        let index = match operation {
            ObjectOperation::Put => 0,
            ObjectOperation::Get => 1,
            ObjectOperation::List => 2,
            ObjectOperation::Delete => 3,
        };
        self.values[index].inc();
    }
}

impl EncodeMetric for ObjectErrorCounters {
    fn encode(&self, mut encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
        for (index, operation) in ObjectOperation::ALL.into_iter().enumerate() {
            encoder
                .encode_family(&ObjectOperationLabel { operation })?
                .encode_counter::<NoLabelSet, _, u64>(&self.values[index].get(), None)?;
        }
        Ok(())
    }

    fn metric_type(&self) -> MetricType {
        MetricType::Counter
    }
}
