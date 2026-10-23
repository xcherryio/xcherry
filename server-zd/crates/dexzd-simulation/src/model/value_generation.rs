// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Random values, Step options, and identifiers for generated commands.

use dexzd_keyspace::{RunId, ValueId, ValueReference};
use dexzd_protocol::dex::{EncodedObject, RetryPolicy, StepOptions, Value, value};
use dexzd_protocol::dexzd::v1::{StoredValue, stored_value};
use prost::Message;
use rand::RngExt;
use rand_chacha::ChaCha8Rng;

/// Largest value the generated runs accept; see the run settings in the client model.
pub(crate) const MAX_VALUE_BYTES: u64 = 4096;
/// Values above this size become value-table references.
pub(crate) const WIRE_INLINE_THRESHOLD_BYTES: u32 = 100;

/// Attribute keys clients and Workers write. The guarded key `counter` is not among them.
pub(crate) const ATTRIBUTE_KEYS: &[&str] = &[
    "status",
    "notes",
    "report",
    "orders/1",
    "orders/2",
    "users/alice",
];
/// Keys a method may lock.
pub(crate) const LOCK_KEYS: &[&str] = &["counter", "status", "orders/1"];
pub(crate) const CHANNEL_NAMES: &[&str] = &["inbox", "events", "queue/a", "queue/b"];
pub(crate) const STEP_TYPES: &[&str] = &["ChargeStep", "ShipStep", "NotifyStep"];
pub(crate) const LOCAL_KEYS: &[&str] = &["progress", "cursor"];

/// Returns a value that is small, large enough for the value table, or a non-referenceable
/// scalar; never null.
pub(crate) fn random_value(rng: &mut ChaCha8Rng) -> Value {
    let kind = match rng.random_range(0..10) {
        0 | 1 => value::Kind::IntValue(rng.random_range(-5..1000)),
        2 => value::Kind::BoolValue(rng.random_bool(0.5)),
        3 | 4 => value::Kind::StringValue(text(rng, 1, 40)),
        5 | 6 => value::Kind::StringValue(text(rng, 120, 600)),
        7 => value::Kind::ObjValue(EncodedObject {
            encoding: "json".to_owned(),
            payload: format!("{{\"note\":\"{}\"}}", text(rng, 1, 30)).into_bytes(),
        }),
        8 => value::Kind::ObjValue(EncodedObject {
            encoding: "json".to_owned(),
            payload: format!("{{\"blob\":\"{}\"}}", text(rng, 150, 700)).into_bytes(),
        }),
        _ => value::Kind::DoubleValue(f64::from(rng.random_range(0..1000_u32)) / 8.0),
    };
    Value { kind: Some(kind) }
}

/// A value larger than the run's maximum value size.
pub(crate) fn oversized_value(rng: &mut ChaCha8Rng) -> Value {
    let length = usize::try_from(MAX_VALUE_BYTES).unwrap_or(4096) + rng.random_range(1..200);
    Value {
        kind: Some(value::Kind::StringValue("z".repeat(length))),
    }
}

pub(crate) fn null_value() -> Value {
    Value {
        kind: Some(value::Kind::NullValue(0)),
    }
}

pub(crate) fn int_value(number: i64) -> Value {
    Value {
        kind: Some(value::Kind::IntValue(number)),
    }
}

/// The wire form of a stored value as a Worker receives it; references echo back as-is.
pub(crate) fn wire_value(run_id: RunId, stored: &StoredValue) -> Option<Value> {
    match stored.kind.as_ref()? {
        stored_value::Kind::InlineValue(inline) => Some(inline.clone()),
        stored_value::Kind::TableValue(reference) => {
            let value_id = reference.value_id.unwrap_or_default();
            let text = ValueReference {
                run_id,
                value_id: ValueId {
                    run_seq: value_id.run_seq,
                    index_in_command: value_id.index_in_command,
                },
            }
            .to_string();
            let kind = if reference.is_string_value {
                value::Kind::InternalBlobIdForStringValue(text)
            } else {
                value::Kind::InternalBlobIdForObjValue(text)
            };
            Some(Value { kind: Some(kind) })
        }
        stored_value::Kind::HardDeleteObject(_) => None,
    }
}

/// A reference that names no live value of the run: a forged ID or another run's value.
pub(crate) fn forged_reference(rng: &mut ChaCha8Rng, run_id: RunId) -> Value {
    let target_run = if rng.random_bool(0.5) {
        run_id
    } else {
        RunId(random_bytes(rng))
    };
    let text = ValueReference {
        run_id: target_run,
        value_id: ValueId {
            run_seq: rng.random_range(500_000..900_000),
            index_in_command: rng.random_range(0..4),
        },
    }
    .to_string();
    Value {
        kind: Some(value::Kind::InternalBlobIdForStringValue(text)),
    }
}

/// Picks a value for a Worker response: new, echoed from `visible`, or rarely invalid.
pub(crate) fn worker_value(
    rng: &mut ChaCha8Rng,
    run_id: RunId,
    visible: &[Value],
    invalid_probability: f64,
) -> Value {
    if rng.random_bool(invalid_probability) {
        return if rng.random_bool(0.5) {
            forged_reference(rng, run_id)
        } else {
            oversized_value(rng)
        };
    }
    if !visible.is_empty() && rng.random_bool(0.3) {
        return visible[rng.random_range(0..visible.len())].clone();
    }
    random_value(rng)
}

pub(crate) fn step_options(rng: &mut ChaCha8Rng) -> Option<StepOptions> {
    if rng.random_bool(0.15) {
        return None;
    }
    Some(StepOptions {
        skip_wait_for: rng.random_bool(0.4),
        wait_for_lock_attribute_keys: subset(rng, LOCK_KEYS, 0.2),
        execute_lock_attribute_keys: subset(rng, LOCK_KEYS, 0.35),
        wait_for_retry_policy: retry_policy(rng),
        execute_retry_policy: retry_policy(rng),
        wait_for_load_attribute_map_instances: subset(rng, &["orders/", "users/alice"], 0.3),
        execute_load_attribute_map_instances: subset(rng, &["orders/", "orders/2"], 0.3),
        wait_for_load_channel_names: subset(rng, CHANNEL_NAMES, 0.25),
        execute_load_channel_names: subset(rng, CHANNEL_NAMES, 0.3),
        execute_load_channel_map_instances: subset(rng, &["queue/"], 0.3),
        ..StepOptions::default()
    })
}

fn retry_policy(rng: &mut ChaCha8Rng) -> Option<RetryPolicy> {
    if rng.random_bool(0.3) {
        return None;
    }
    Some(RetryPolicy {
        initial_interval_seconds: rng.random_range(0..3),
        backoff_coefficient: if rng.random_bool(0.5) { 2.0 } else { 0.0 },
        maximum_interval_seconds: if rng.random_bool(0.5) { 5 } else { 0 },
        maximum_attempts: rng.random_range(0..5),
        total_duration_seconds: 0,
    })
}

/// Each item independently with `probability`, in the given order.
pub(crate) fn subset(rng: &mut ChaCha8Rng, items: &[&str], probability: f64) -> Vec<String> {
    items
        .iter()
        .filter(|_| rng.random_bool(probability))
        .map(|item| (*item).to_owned())
        .collect()
}

pub(crate) fn pick<'items>(rng: &mut ChaCha8Rng, items: &[&'items str]) -> &'items str {
    items[rng.random_range(0..items.len())]
}

pub(crate) fn random_bytes(rng: &mut ChaCha8Rng) -> [u8; 16] {
    let mut bytes = [0_u8; 16];
    rng.fill(&mut bytes);
    bytes
}

/// A random UUID-shaped text identifier.
pub(crate) fn random_identifier(rng: &mut ChaCha8Rng) -> String {
    RunId(random_bytes(rng)).to_string()
}

/// A stable 16-byte fingerprint of a request, standing in for the owner's xxh3-128.
pub(crate) fn fingerprint(request: &impl Message) -> Vec<u8> {
    let encoded = request.encode_to_vec();
    let mut fingerprint = Vec::with_capacity(16);
    for seed in [0xcbf2_9ce4_8422_2325_u64, 0x8422_2325_cbf2_9ce4] {
        let mut hash = seed;
        for byte in &encoded {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        fingerprint.extend_from_slice(&hash.to_be_bytes());
    }
    fingerprint
}

fn text(rng: &mut ChaCha8Rng, minimum: usize, maximum: usize) -> String {
    let length = rng.random_range(minimum..=maximum);
    (0..length)
        .map(|_| char::from(b'a' + rng.random_range(0..26_u8)))
        .collect()
}
