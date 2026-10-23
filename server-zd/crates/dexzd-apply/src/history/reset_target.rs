// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Maps a ResetFlow request to a point of the selected lineage (plan section 8.4). Targets
//! other than the beginning must be semantic points inside the window.

use dexzd_protocol::dex::{ErrorSubStatus, FlowResetStepMethod, FlowResetType, ResetFlowRequest};
use dexzd_protocol::dexzd::v1::{CommandReply, HistoryRecord, StepMethod};

use crate::ApplyError;
use crate::command_handlers::command_replies::{error_reply, grpc_code};
use crate::command_handlers::run_cursor::RunCursor;
use crate::history::lineage::{Lineage, genesis_seq, history_records};
use crate::history::state_rebuild::HistoryPoint;
use crate::state_access::FlowTransaction;

/// The point a time travel returns to.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TravelTarget {
    pub(crate) point: HistoryPoint,
    /// Largest history event ID at the point, for the fork event.
    pub(crate) last_event_id: i64,
}

/// Resolves the target point, or returns the reply that rejects the request.
pub(crate) fn resolve_target(
    transaction: &FlowTransaction<'_>,
    cursor: &RunCursor,
    now_micros: i64,
    request: &ResetFlowRequest,
    lineage: &Lineage,
) -> Result<Result<TravelTarget, CommandReply>, ApplyError> {
    let reset_type =
        FlowResetType::try_from(request.reset_type).unwrap_or(FlowResetType::Unspecified);
    let genesis = genesis_seq(transaction, cursor.rows)?;
    let window_start = cursor.record.window_start_seq;
    let mut candidates = Vec::new();
    if genesis < window_start
        && let Some(record) = cursor.rows.history_record(transaction, genesis)?
    {
        candidates.push(record);
    }
    candidates.extend(
        history_records(transaction, cursor.rows, window_start, u64::MAX)?
            .into_iter()
            .filter(|record| record.is_semantic && lineage.contains(record.attempt, record.seq)),
    );
    let selection = match reset_type {
        FlowResetType::Beginning => candidates.iter().find(|record| record.seq == genesis),
        FlowResetType::HistoryEventTime => {
            let target_nanos = match parse_reset_time(&request.history_event_time, now_micros) {
                Ok(nanos) => nanos,
                Err(detail) => return Ok(Err(invalid_argument(detail))),
            };
            candidates
                .iter()
                .find(|record| i128::from(record.timestamp_micros) * 1000 >= target_nanos)
        }
        FlowResetType::StepType => candidates.iter().find(|record| {
            record
                .pending_step_methods
                .iter()
                .any(|pending| pending.step_type == request.step_type)
        }),
        FlowResetType::StepExecutionId => {
            let method = FlowResetStepMethod::try_from(request.step_method)
                .unwrap_or(FlowResetStepMethod::Unspecified);
            if method == FlowResetStepMethod::Unspecified {
                return Ok(Err(invalid_argument(
                    "step method must be WaitFor or Execute".to_owned(),
                )));
            }
            candidates.iter().find(|record| {
                record.pending_step_methods.iter().any(|pending| {
                    pending.step_execution_id == request.step_execution_id
                        && match method {
                            FlowResetStepMethod::WaitFor => pending.is_step_created,
                            _ => pending.method == StepMethod::Execute as i32,
                        }
                })
            })
        }
        FlowResetType::Unspecified => {
            return Ok(Err(invalid_argument(
                "time travel type is required".to_owned(),
            )));
        }
    };
    let Some(selected) = selection else {
        return Ok(Err(error_reply(
            grpc_code::FAILED_PRECONDITION,
            ErrorSubStatus::Uncategorized,
            format!(
                "no {} point exists in the time travel window (from run seq {window_start}) of \
                 the selected run attempt",
                describe_reset_type(reset_type)
            ),
        )));
    };
    Ok(Ok(TravelTarget {
        point: HistoryPoint {
            attempt: selected.attempt,
            seq: selected.seq,
        },
        last_event_id: last_event_id_at(&candidates, selected.seq),
    }))
}

fn last_event_id_at(candidates: &[HistoryRecord], seq: u64) -> i64 {
    candidates
        .iter()
        .filter(|record| record.seq <= seq)
        .flat_map(|record| record.events.iter().map(|event| event.event_id))
        .max()
        .unwrap_or(0)
}

fn describe_reset_type(reset_type: FlowResetType) -> &'static str {
    match reset_type {
        FlowResetType::Beginning => "beginning",
        FlowResetType::HistoryEventTime => "history event time",
        FlowResetType::StepType => "Step type",
        FlowResetType::StepExecutionId => "Step execution",
        FlowResetType::Unspecified => "unspecified",
    }
}

fn invalid_argument(detail: String) -> CommandReply {
    error_reply(
        grpc_code::INVALID_ARGUMENT,
        ErrorSubStatus::Uncategorized,
        detail,
    )
}

/// Parses a reset time like the Go Server: RFC 3339 with optional fraction, Unix nanoseconds,
/// or a relative time such as `3d` before `now_micros`. Returns Unix nanoseconds.
pub(crate) fn parse_reset_time(text: &str, now_micros: i64) -> Result<i128, String> {
    if let Some(nanos) = parse_rfc3339_nanos(text) {
        return Ok(nanos);
    }
    if let Ok(nanos) = text.parse::<i64>() {
        return Ok(i128::from(nanos));
    }
    parse_relative_time(text, now_micros).ok_or_else(|| {
        format!(
            "cannot parse time {text:?}; use RFC 3339 time, a time range such as 3d, or Unix \
             nanoseconds"
        )
    })
}

fn parse_relative_time(text: &str, now_micros: i64) -> Option<i128> {
    let digits_end = text
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(text.len());
    let (digits, unit) = text.split_at(digits_end);
    if digits.is_empty() || digits.starts_with('0') {
        return None;
    }
    let multiplier: i128 = digits.parse().ok()?;
    if multiplier >= 1_000_000 {
        return None;
    }
    const SECOND: i128 = 1_000_000_000;
    let unit_nanos = match unit {
        "s" | "second" => SECOND,
        "m" | "minute" => 60 * SECOND,
        "h" | "hour" => 3600 * SECOND,
        "d" | "day" => 24 * 3600 * SECOND,
        "w" | "week" => 7 * 24 * 3600 * SECOND,
        "M" | "month" => 30 * 24 * 3600 * SECOND,
        "y" | "year" => 365 * 24 * 3600 * SECOND,
        _ => return None,
    };
    Some((i128::from(now_micros) * 1000 - multiplier * unit_nanos).max(0))
}

/// Parses `YYYY-MM-DDTHH:MM:SS[.fraction](Z|±HH:MM)`.
fn parse_rfc3339_nanos(text: &str) -> Option<i128> {
    let bytes = text.as_bytes();
    if bytes.len() < 20 || bytes[4] != b'-' || bytes[7] != b'-' || bytes[10] != b'T' {
        return None;
    }
    if bytes[13] != b':' || bytes[16] != b':' {
        return None;
    }
    let number = |range: std::ops::Range<usize>| -> Option<i64> {
        let part = text.get(range)?;
        part.bytes()
            .all(|byte| byte.is_ascii_digit())
            .then(|| part.parse().ok())?
    };
    let (year, month, day) = (number(0..4)?, number(5..7)?, number(8..10)?);
    let (hour, minute, second) = (number(11..13)?, number(14..16)?, number(17..19)?);
    let is_valid_clock = hour < 24 && minute < 60 && second < 60;
    if !(1..=12).contains(&month) || day < 1 || day > days_in_month(year, month) || !is_valid_clock
    {
        return None;
    }
    let mut rest = &text[19..];
    let mut fraction_nanos: i128 = 0;
    if let Some(after_dot) = rest.strip_prefix('.') {
        let digits_end = after_dot
            .find(|character: char| !character.is_ascii_digit())
            .unwrap_or(after_dot.len());
        if digits_end == 0 || digits_end > 9 {
            return None;
        }
        let digits = &after_dot[..digits_end];
        fraction_nanos = digits.parse::<i128>().ok()? * 10_i128.pow(9 - digits_end as u32);
        rest = &after_dot[digits_end..];
    }
    let offset_seconds: i64 = match rest {
        "Z" => 0,
        _ => {
            let sign = match rest.as_bytes().first() {
                Some(b'+') => 1,
                Some(b'-') => -1,
                _ => return None,
            };
            if rest.len() != 6 || rest.as_bytes()[3] != b':' {
                return None;
            }
            let offset_hours: i64 = rest.get(1..3)?.parse().ok()?;
            let offset_minutes: i64 = rest.get(4..6)?.parse().ok()?;
            if offset_hours > 23 || offset_minutes > 59 {
                return None;
            }
            sign * (offset_hours * 3600 + offset_minutes * 60)
        }
    };
    let days = days_from_civil(year, month, day);
    let seconds = days * 86_400 + hour * 3600 + minute * 60 + second - offset_seconds;
    Some(i128::from(seconds) * 1_000_000_000 + fraction_nanos)
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        2 if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Days since 1970-01-01 of a proleptic Gregorian date (Howard Hinnant's algorithm).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let shifted_year = if month <= 2 { year - 1 } else { year };
    let era = shifted_year.div_euclid(400);
    let year_of_era = shifted_year - era * 400;
    let shifted_month = (month + 9) % 12;
    let day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

#[cfg(test)]
mod tests {
    use super::parse_reset_time;

    #[test]
    fn reset_times_parse_like_the_go_server() {
        assert_eq!(
            parse_reset_time("2026-10-23T08:00:00Z", 0),
            Ok(1_792_742_400_000_000_000)
        );
        assert_eq!(
            parse_reset_time("2026-10-23T01:00:00.5-07:00", 0),
            Ok(1_792_742_400_500_000_000)
        );
        assert_eq!(parse_reset_time("1970-01-01T00:00:00Z", 0), Ok(0));
        assert_eq!(parse_reset_time("42", 0), Ok(42));
        assert_eq!(
            parse_reset_time("3d", 5 * 86_400_000_000),
            Ok(2 * 86_400_000_000_000)
        );
        assert_eq!(
            parse_reset_time("2minute", 600_000_000),
            Ok(480_000_000_000)
        );
        assert_eq!(parse_reset_time("1000y", 1_000_000), Ok(0));
        assert!(parse_reset_time("0d", 0).is_err());
        assert!(parse_reset_time("3x", 0).is_err());
        assert!(parse_reset_time("2026-02-30T00:00:00Z", 0).is_err());
    }
}
