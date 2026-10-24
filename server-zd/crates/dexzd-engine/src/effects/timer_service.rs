// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Per-partition timer services over the committed Timer index. Each loads the near future,
//! fires due timers in time order merged per run into `TimersFired`, and never fires a timer
//! before the owner's wall clock reaches its fire time.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use dexzd_keyspace::{ColumnFamily, IndexKind, PartitionId, TenantId, partition_index_prefix};
use dexzd_protocol::dexzd::v1::{FlowCommand, TimerId, TimersFiredCommand, flow_command};
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::effects::state_reads::{
    RunLocation, TimerIndexEntry, parse_timer_index_key, read_run_record, timer_index_start_key,
};
use crate::effects::{EffectContext, EffectError};
use crate::runtime::{CommitUrgency, FlowAddress};

/// The router's handles to the running partition timer services.
pub(crate) struct TimerServices {
    context: Arc<EffectContext>,
    partitions: BTreeMap<PartitionId, Arc<Notify>>,
}

impl TimerServices {
    pub(crate) fn new(context: Arc<EffectContext>) -> Self {
        Self {
            context,
            partitions: BTreeMap::new(),
        }
    }

    /// Starts the timer service of `partition` once.
    pub(crate) fn start_partition(&mut self, partition: PartitionId) {
        if self.partitions.contains_key(&partition) {
            return;
        }
        let wake = Arc::new(Notify::new());
        self.partitions.insert(partition, Arc::clone(&wake));
        let service = PartitionTimerService {
            context: Arc::clone(&self.context),
            partition,
            wake,
            retry_after: BTreeMap::new(),
        };
        self.context.tracker.spawn(service.run());
    }

    /// Wakes the partition's service so it rereads the index for a newly armed timer.
    pub(crate) fn notify_armed(&mut self, partition: PartitionId, fire_at_micros: i64) {
        match self.partitions.get(&partition) {
            Some(wake) => wake.notify_one(),
            None => tracing::warn!(
                %partition,
                fire_at_micros,
                "timer armed in a partition without a timer service"
            ),
        }
    }
}

struct PartitionTimerService {
    context: Arc<EffectContext>,
    partition: PartitionId,
    wake: Arc<Notify>,
    /// Index keys of fired timers still in the index: when to try again, and the delay that
    /// doubles on each try.
    retry_after: BTreeMap<Vec<u8>, (Instant, Duration)>,
}

impl PartitionTimerService {
    async fn run(mut self) {
        let mut backoff = self.context.retry_backoff();
        loop {
            if self.context.shutdown.is_cancelled() {
                return;
            }
            let wake_at = match self.fire_due_timers().await {
                Ok(wake_at) => {
                    backoff = self.context.retry_backoff();
                    wake_at
                }
                Err(EffectError::Stopped) => return,
                Err(error) => {
                    tracing::warn!(%error, partition = %self.partition, "timer service pass failed");
                    if !backoff.wait(&self.context.shutdown).await {
                        return;
                    }
                    continue;
                }
            };
            let Some(wake_at) = wake_at else {
                continue;
            };
            // Biased for reproducible simulation; every wake starts a pass that fires due
            // timers.
            tokio::select! {
                biased;
                () = self.context.shutdown.cancelled() => return,
                () = self.wake.notified() => {}
                () = tokio::time::sleep_until(wake_at) => {}
            }
        }
    }

    /// Fires every due timer. Returns when to look again, or `None` to look again at once.
    async fn fire_due_timers(&mut self) -> Result<Option<Instant>, EffectError> {
        let now_micros = self.context.clock.now_unix_micros();
        let lookahead_micros =
            i64::try_from(self.context.config.timer_lookahead.as_micros()).unwrap_or(i64::MAX);
        let horizon_micros = now_micros.saturating_add(lookahead_micros);
        let now = Instant::now();
        let entries = self.load_timers(horizon_micros, now)?;
        let is_waiting = |retry_after: &BTreeMap<Vec<u8>, (Instant, Duration)>, key: &Vec<u8>| {
            retry_after
                .get(key)
                .is_some_and(|(retry_at, _)| *retry_at > now)
        };

        let mut runs: Vec<(RunLocation, Vec<TimerId>)> = Vec::new();
        let mut run_positions: BTreeMap<RunLocation, usize> = BTreeMap::new();
        let mut next_fire_micros = None;
        for (key, entry) in &entries {
            if is_waiting(&self.retry_after, key) {
                continue;
            }
            if entry.fire_at_micros > now_micros {
                next_fire_micros = Some(entry.fire_at_micros);
                break;
            }
            let position = *run_positions.entry(entry.run).or_insert_with(|| {
                runs.push((entry.run, Vec::new()));
                runs.len() - 1
            });
            runs[position].1.push(entry.timer_id.clone());
        }
        if !runs.is_empty() {
            let fired_keys: Vec<Vec<u8>> = entries
                .iter()
                .filter(|(key, entry)| {
                    entry.fire_at_micros <= now_micros && !is_waiting(&self.retry_after, key)
                })
                .map(|(key, _)| key.clone())
                .collect();
            let proposals = runs
                .into_iter()
                .map(|(run, timers)| self.propose_timers_fired(run, timers));
            for result in futures::future::join_all(proposals).await {
                result?;
            }
            let fired_at = Instant::now();
            for key in fired_keys {
                let delay = self
                    .retry_after
                    .get(&key)
                    .map_or(self.context.config.fired_timer_retry_delay, |(_, delay)| {
                        (*delay * 2).min(self.context.config.timer_lookahead)
                    });
                self.retry_after.insert(key, (fired_at + delay, delay));
            }
            return Ok(None);
        }
        let fire_wait_micros = next_fire_micros
            .unwrap_or(horizon_micros)
            .saturating_sub(now_micros)
            .max(0);
        let mut wake_at = now + Duration::from_micros(u64::try_from(fire_wait_micros).unwrap_or(0));
        if let Some(retry_at) = self
            .retry_after
            .values()
            .map(|(retry_at, _)| *retry_at)
            .filter(|retry_at| *retry_at > now)
            .min()
        {
            wake_at = wake_at.min(retry_at);
        }
        Ok(Some(wake_at))
    }

    /// Reads up to the load limit of timer index rows outside retry backoff, firing at or before
    /// `horizon_micros`.
    ///
    /// Returns the scanned rows in index order, including rows in backoff.
    fn load_timers(
        &mut self,
        horizon_micros: i64,
        now: Instant,
    ) -> Result<Vec<(Vec<u8>, TimerIndexEntry)>, EffectError> {
        let limit = self.context.config.timer_load_limit;
        let reader = self.context.runtime.committed_state_reader()?;
        let mut start = partition_index_prefix(self.partition, IndexKind::Timer);
        let end = timer_index_start_key(self.partition, horizon_micros.saturating_add(1));
        let mut entries = Vec::new();
        let mut ready_count = 0;
        loop {
            let page = reader.scan(ColumnFamily::PartitionIndex, &start, &end, limit)?;
            let is_last_page = page.len() < limit;
            for (key, _) in page {
                let entry = parse_timer_index_key(self.partition, &key)?;
                let is_waiting = self
                    .retry_after
                    .get(&key)
                    .is_some_and(|(retry_at, _)| *retry_at > now);
                if !is_waiting {
                    ready_count += 1;
                }
                entries.push((key, entry));
            }
            if is_last_page || ready_count >= limit {
                break;
            }
            let Some((last_key, _)) = entries.last() else {
                break;
            };
            start = last_key.iter().copied().chain([0]).collect();
        }
        let scanned_keys: BTreeSet<&Vec<u8>> = entries.iter().map(|(key, _)| key).collect();
        let last_scanned = entries.last().map(|(key, _)| key.clone());
        // Rows beyond the scan keep their backoff; scanned rows no longer in the index lose it.
        self.retry_after.retain(|key, _| {
            scanned_keys.contains(key) || last_scanned.as_ref().is_some_and(|last| key > last)
        });
        Ok(entries)
    }

    async fn propose_timers_fired(
        &self,
        run: RunLocation,
        timers: Vec<TimerId>,
    ) -> Result<(), EffectError> {
        let run_record = {
            let reader = self.context.runtime.committed_state_reader()?;
            read_run_record(reader.as_ref(), run)?
        };
        let Some(run_record) = run_record else {
            tracing::warn!(run_id = %run.run_id, "timer index refers to a missing run");
            return Ok(());
        };
        let flow = FlowAddress {
            tenant_id: TenantId(run_record.tenant_id),
            flow_id: run_record.flow_id,
        };
        let command = FlowCommand {
            flow_id: flow.flow_id.clone(),
            run_id: run.run_id.as_bytes().to_vec(),
            tenant_id: flow.tenant_id.0,
            command: Some(flow_command::Command::TimersFired(TimersFiredCommand {
                timers,
            })),
            ..FlowCommand::default()
        };
        self.context
            .propose_until_committed(&flow, command, CommitUrgency::Prompt)
            .await
            .ok_or(EffectError::Stopped)?;
        Ok(())
    }
}
