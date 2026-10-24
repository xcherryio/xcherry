// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use prometheus_client::collector::Collector;
use prometheus_client::encoding::{DescriptorEncoder, EncodeMetric};
use prometheus_client::metrics::counter::ConstCounter;
use prometheus_client::metrics::gauge::ConstGauge;

#[derive(Debug)]
pub(crate) struct ProcessCollector {
    ticks_per_second: f64,
    boot_time_seconds: Option<f64>,
}

impl ProcessCollector {
    pub(crate) fn new() -> Self {
        let boot_time_seconds = read_proc("/proc/stat").and_then(|contents| {
            contents.lines().find_map(|line| {
                line.strip_prefix("btime ")
                    .and_then(|value| value.parse::<f64>().ok())
            })
        });
        Self {
            ticks_per_second: rustix::param::clock_ticks_per_second() as f64,
            boot_time_seconds,
        }
    }
}

impl Collector for ProcessCollector {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        if let Some(status) = read_proc("/proc/self/status")
            && let Some(kibibytes) = status.lines().find_map(|line| {
                line.strip_prefix("VmRSS:")
                    .and_then(|value| value.split_whitespace().next())
                    .and_then(|value| value.parse::<u64>().ok())
            })
        {
            let metric = ConstGauge::new(kibibytes.saturating_mul(1024));
            metric.encode(encoder.encode_descriptor(
                "process_resident_memory_bytes",
                "Resident process memory.",
                None,
                metric.metric_type(),
            )?)?;
        }
        if let Some(stat) = read_proc("/proc/self/stat")
            && let Some((cpu_ticks, start_ticks)) = parse_stat(&stat)
        {
            let cpu = ConstCounter::new(cpu_ticks as f64 / self.ticks_per_second);
            cpu.encode(encoder.encode_descriptor(
                "process_cpu_seconds",
                "Process CPU time.",
                None,
                cpu.metric_type(),
            )?)?;
            if let Some(boot_time_seconds) = self.boot_time_seconds {
                let start =
                    ConstGauge::new(boot_time_seconds + start_ticks as f64 / self.ticks_per_second);
                start.encode(encoder.encode_descriptor(
                    "process_start_time_seconds",
                    "Process start time since Unix epoch.",
                    None,
                    start.metric_type(),
                )?)?;
            }
        }
        match std::fs::read_dir("/proc/self/fd") {
            Ok(mut entries) => match entries
                .try_fold(0_u64, |count, entry| entry.map(|_| count + 1))
            {
                Ok(count) => {
                    let open_fds = ConstGauge::new(count);
                    open_fds.encode(encoder.encode_descriptor(
                        "process_open_fds",
                        "Open file descriptors.",
                        None,
                        open_fds.metric_type(),
                    )?)?;
                }
                Err(error) => tracing::warn!(%error, "could not count process file descriptors"),
            },
            Err(error) => {
                tracing::warn!(%error, "could not open process file descriptor directory")
            }
        }
        Ok(())
    }
}

fn read_proc(path: &str) -> Option<String> {
    match std::fs::read_to_string(path) {
        Ok(contents) => Some(contents),
        Err(error) => {
            tracing::warn!(%error, path, "could not read process metrics");
            None
        }
    }
}

fn parse_stat(stat: &str) -> Option<(u64, u64)> {
    let fields = stat
        .rsplit_once(") ")?
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    let user_ticks = fields.get(11)?.parse::<u64>().ok()?;
    let system_ticks = fields.get(12)?.parse::<u64>().ok()?;
    let start_ticks = fields.get(19)?.parse::<u64>().ok()?;
    Some((user_ticks.saturating_add(system_ticks), start_ticks))
}
