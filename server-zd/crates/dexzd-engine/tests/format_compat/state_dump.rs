// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;
use std::fmt::Write;

use dexzd_apply::FlowStateReader;
use dexzd_engine::CommittedStateView;
use dexzd_keyspace::ColumnFamily;
use dexzd_protocol::dexzd::v1::PartitionMeta;
use prost::Message;
use xxhash_rust::xxh3::xxh3_128;

/// Values up to this size are dumped in full; longer ones as their length and xxh3-128.
const FULL_VALUE_DUMP_BYTES: usize = 64;
/// Sorts after every stored key: keys never start with this many 0xFF bytes.
const SCAN_END_KEY: [u8; 256] = [0xFF; 256];
const DUMP_HEADER: &str = "# column family, key hex, value: =hex or #length:xxh3-128\n";

/// Committed rows keyed by column family name and key hex, with the dumped value.
pub(crate) type StateDump = BTreeMap<(String, String), String>;

/// Dumps the rows replay must reproduce: no NodeMeta, and PartitionMeta without its position.
pub(crate) async fn dump_committed_state(view: CommittedStateView) -> StateDump {
    tokio::task::spawn_blocking(move || dump_rows(&view))
        .await
        .expect("the state dump task completes")
}

fn dump_rows(view: &dyn FlowStateReader) -> StateDump {
    let mut dump = StateDump::new();
    for column_family in ColumnFamily::ALL {
        if column_family == ColumnFamily::NodeMeta {
            continue;
        }
        let rows = view
            .scan(column_family, &[], &SCAN_END_KEY, usize::MAX)
            .expect("scan committed state");
        for (key, value) in rows {
            let value = if column_family == ColumnFamily::PartitionMeta {
                without_applied_position(&value)
            } else {
                value
            };
            dump.insert(
                (column_family.name().to_owned(), hex(&key)),
                dumped_value(&value),
            );
        }
    }
    dump
}

fn without_applied_position(value: &[u8]) -> Vec<u8> {
    let mut meta = PartitionMeta::decode(value).expect("PartitionMeta decodes");
    meta.applied = None;
    meta.applied_epoch = 0;
    meta.encode_to_vec()
}

fn dumped_value(value: &[u8]) -> String {
    if value.len() <= FULL_VALUE_DUMP_BYTES {
        format!("={}", hex(value))
    } else {
        format!("#{}:{:032x}", value.len(), xxh3_128(value))
    }
}

pub(crate) fn render_state_dump(dump: &StateDump) -> String {
    let mut text = DUMP_HEADER.to_owned();
    for ((column_family, key), value) in dump {
        writeln!(text, "{column_family} {key} {value}").expect("writing to a String succeeds");
    }
    text
}

pub(crate) fn parse_state_dump(text: &str) -> StateDump {
    let body = text
        .strip_prefix(DUMP_HEADER)
        .expect("the state dump starts with its header");
    let mut dump = StateDump::new();
    for line in body.lines() {
        let mut fields = line.split(' ');
        let (Some(column_family), Some(key), Some(value), None) =
            (fields.next(), fields.next(), fields.next(), fields.next())
        else {
            panic!("state dump line {line:?} does not have three fields");
        };
        let previous = dump.insert((column_family.to_owned(), key.to_owned()), value.to_owned());
        assert!(
            previous.is_none(),
            "state dump repeats {column_family} {key}"
        );
    }
    dump
}

/// Returns one line per row that differs, is missing, or is unexpected.
pub(crate) fn state_dump_differences(recorded: &StateDump, replayed: &StateDump) -> Vec<String> {
    let mut differences = Vec::new();
    for (row, recorded_value) in recorded {
        match replayed.get(row) {
            None => differences.push(format!("missing {} {}", row.0, row.1)),
            Some(value) if value != recorded_value => differences.push(format!(
                "differs {} {}: recorded {recorded_value}, replayed {value}",
                row.0, row.1
            )),
            Some(_) => {}
        }
    }
    for (row, value) in replayed {
        if !recorded.contains_key(row) {
            differences.push(format!("unexpected {} {} {value}", row.0, row.1));
        }
    }
    differences
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(text, "{byte:02x}").expect("writing to a String succeeds");
    }
    text
}
