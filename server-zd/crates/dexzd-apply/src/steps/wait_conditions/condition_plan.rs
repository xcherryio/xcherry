// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Selection of the satisfied candidate of a wait condition and its exact channel consumption,
//! ported from the Go Server's `condition/plan.go`.

use std::collections::BTreeMap;

use dexzd_protocol::dex::{ChannelCondition, WaitingCondition, WaitingConditionType};

/// Messages one winning channel condition takes from the head of its channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChannelConsumption {
    pub(crate) channel_condition_index: usize,
    pub(crate) count: u64,
}

/// What the wait condition's members currently satisfy.
pub(crate) struct ConditionProgress<'progress> {
    /// Pending (unclaimed) message count by channel name.
    pub(crate) pending_counts: &'progress BTreeMap<String, u64>,
    /// Whether each timer condition fired or was skipped, in declaration order.
    pub(crate) is_timer_completed: &'progress [bool],
    /// Whether each SubFlow condition has a child result, in declaration order.
    pub(crate) is_sub_flow_completed: &'progress [bool],
}

/// Returns the consumption of the first satisfied candidate, or `None` while unsatisfied.
///
/// An empty condition is satisfied with no consumption. The winning candidate's channel
/// conditions appear in declaration order, each with its exact count (possibly zero).
pub(crate) fn plan_condition(
    condition: &WaitingCondition,
    progress: &ConditionProgress<'_>,
) -> Option<Vec<ChannelConsumption>> {
    let is_empty = condition.timer_conditions.is_empty()
        && condition.channel_conditions.is_empty()
        && condition.sub_flow_conditions.is_empty();
    if is_empty {
        return Some(Vec::new());
    }
    let bounds: Vec<ChannelBounds> = condition
        .channel_conditions
        .iter()
        .map(ChannelBounds::from_condition)
        .collect();
    build_candidates(condition)
        .iter()
        .find_map(|candidate| check_candidate(candidate, &bounds, progress))
}

/// A subset of the condition's members that must all be satisfied together.
#[derive(Debug, Default)]
struct TriggerCandidate {
    timer_indexes: Vec<usize>,
    channel_indexes: Vec<usize>,
    sub_flow_indexes: Vec<usize>,
}

fn build_candidates(condition: &WaitingCondition) -> Vec<TriggerCandidate> {
    let timer_count = condition.timer_conditions.len();
    let channel_count = condition.channel_conditions.len();
    let sub_flow_count = condition.sub_flow_conditions.len();
    match WaitingConditionType::try_from(condition.waiting_condition_type) {
        Ok(WaitingConditionType::AllCompleted) => vec![TriggerCandidate {
            timer_indexes: (0..timer_count).collect(),
            channel_indexes: (0..channel_count).collect(),
            sub_flow_indexes: (0..sub_flow_count).collect(),
        }],
        Ok(WaitingConditionType::AnyCompleted) => {
            let timers = (0..timer_count).map(|index| TriggerCandidate {
                timer_indexes: vec![index],
                ..TriggerCandidate::default()
            });
            let channels = (0..channel_count).map(|index| TriggerCandidate {
                channel_indexes: vec![index],
                ..TriggerCandidate::default()
            });
            let sub_flows = (0..sub_flow_count).map(|index| TriggerCandidate {
                sub_flow_indexes: vec![index],
                ..TriggerCandidate::default()
            });
            timers.chain(channels).chain(sub_flows).collect()
        }
        Ok(WaitingConditionType::AnyCombinationCompleted) => condition
            .condition_combinations
            .iter()
            .map(|combination| {
                let mut candidate = TriggerCandidate::default();
                for condition_id in &combination.condition_ids {
                    add_combination_member(condition, condition_id, &mut candidate);
                }
                candidate.channel_indexes.sort_unstable();
                candidate
            })
            .collect(),
        Ok(WaitingConditionType::Unspecified) | Err(_) => Vec::new(),
    }
}

/// Resolves a combination member by condition ID: timers first, then channels, then SubFlows.
/// Go builds ID maps, so a duplicated ID resolves to its last declaration.
fn add_combination_member(
    condition: &WaitingCondition,
    condition_id: &str,
    candidate: &mut TriggerCandidate,
) {
    if let Some(index) = condition
        .timer_conditions
        .iter()
        .rposition(|timer| timer.condition_id == condition_id)
    {
        candidate.timer_indexes.push(index);
    } else if let Some(index) = condition
        .channel_conditions
        .iter()
        .rposition(|channel| channel.condition_id == condition_id)
    {
        candidate.channel_indexes.push(index);
    } else if let Some(index) = condition
        .sub_flow_conditions
        .iter()
        .rposition(|sub_flow| sub_flow.condition_id == condition_id)
    {
        candidate.sub_flow_indexes.push(index);
    }
}

/// Consumption bounds of one channel condition; `max == None` takes everything present.
struct ChannelBounds<'condition> {
    channel_name: &'condition str,
    min: u64,
    max: Option<u64>,
}

impl<'condition> ChannelBounds<'condition> {
    fn from_condition(condition: &'condition ChannelCondition) -> Self {
        let bound = |value: i32| u64::try_from(value).unwrap_or(0);
        let (min, max) = match (condition.at_least, condition.at_most) {
            (None, None) => (1, Some(1)),
            (None, Some(at_most)) => (0, Some(bound(at_most))),
            (Some(at_least), None) => (bound(at_least), None),
            (Some(at_least), Some(at_most)) => (bound(at_least), Some(bound(at_most))),
        };
        Self {
            channel_name: &condition.channel_name,
            min,
            max,
        }
    }
}

/// Reserves every minimum first, then fills toward maximums in declaration order, so a later
/// condition's minimum is never taken by an earlier condition's optional extra.
fn check_candidate(
    candidate: &TriggerCandidate,
    bounds: &[ChannelBounds<'_>],
    progress: &ConditionProgress<'_>,
) -> Option<Vec<ChannelConsumption>> {
    let is_flag_set = |flags: &[bool], index: usize| flags.get(index).copied().unwrap_or(false);
    if !candidate
        .timer_indexes
        .iter()
        .all(|&index| is_flag_set(progress.is_timer_completed, index))
        || !candidate
            .sub_flow_indexes
            .iter()
            .all(|&index| is_flag_set(progress.is_sub_flow_completed, index))
    {
        return None;
    }
    let mut remaining: BTreeMap<&str, u64> = BTreeMap::new();
    let mut consumptions = Vec::with_capacity(candidate.channel_indexes.len());
    for &index in &candidate.channel_indexes {
        let channel = bounds.get(index)?;
        let available = *remaining.entry(channel.channel_name).or_insert_with(|| {
            progress
                .pending_counts
                .get(channel.channel_name)
                .copied()
                .unwrap_or(0)
        });
        if available < channel.min {
            return None;
        }
        remaining.insert(channel.channel_name, available - channel.min);
        consumptions.push(ChannelConsumption {
            channel_condition_index: index,
            count: channel.min,
        });
    }
    for consumption in &mut consumptions {
        let channel = &bounds[consumption.channel_condition_index];
        let available = remaining.get(channel.channel_name).copied().unwrap_or(0);
        let extra = match channel.max {
            None => available,
            Some(max) => max.saturating_sub(channel.min).min(available),
        };
        consumption.count += extra;
        remaining.insert(channel.channel_name, available - extra);
    }
    Some(consumptions)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use dexzd_protocol::dex::{
        ChannelCondition, TimerCondition, WaitingCondition, WaitingConditionType,
    };

    use super::{ChannelConsumption, ConditionProgress, plan_condition};

    // Apply evaluates on every change, so a timer and a message never become ready in the
    // same evaluation there; the Go Server can see both at once.
    #[test]
    fn any_of_prefers_a_timer_when_a_timer_and_a_message_are_ready_at_once() {
        let condition = WaitingCondition {
            waiting_condition_type: WaitingConditionType::AnyCompleted as i32,
            timer_conditions: vec![TimerCondition::default()],
            channel_conditions: vec![ChannelCondition {
                channel_name: "orders".to_owned(),
                ..ChannelCondition::default()
            }],
            ..WaitingCondition::default()
        };
        let pending_counts = BTreeMap::from([("orders".to_owned(), 1)]);
        let progress = ConditionProgress {
            pending_counts: &pending_counts,
            is_timer_completed: &[true],
            is_sub_flow_completed: &[],
        };
        assert_eq!(plan_condition(&condition, &progress), Some(Vec::new()));
        let progress = ConditionProgress {
            is_timer_completed: &[false],
            ..progress
        };
        assert_eq!(
            plan_condition(&condition, &progress),
            Some(vec![ChannelConsumption {
                channel_condition_index: 0,
                count: 1,
            }])
        );
    }
}
