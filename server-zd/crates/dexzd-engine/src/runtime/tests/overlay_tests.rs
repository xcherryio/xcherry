// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;

use dexzd_apply::{ApplyOutcome, FlowStateReader, StateWrite};
use dexzd_keyspace::ColumnFamily;

use crate::runtime::speculative_overlay::{CommittedMatch, OverlayEntry, SpeculativeOverlay};
use crate::runtime::tests::model_applier::ok_reply;
use crate::runtime::tests::test_support::MemoryView;

const FLOW: ColumnFamily = ColumnFamily::Flow;

fn key(text: &str) -> Vec<u8> {
    text.as_bytes().to_vec()
}

fn put(text: &str, value: &str) -> StateWrite {
    StateWrite::Put {
        column_family: FLOW,
        key: key(text),
        value: key(value),
    }
}

fn delete(text: &str) -> StateWrite {
    StateWrite::Delete {
        column_family: FLOW,
        key: key(text),
    }
}

fn delete_range(start: &str, end: &str) -> StateWrite {
    StateWrite::DeleteRange {
        column_family: FLOW,
        start: key(start),
        end: key(end),
    }
}

fn entry(command_number: u8, writes: Vec<StateWrite>) -> OverlayEntry {
    OverlayEntry::new(
        vec![command_number],
        ApplyOutcome {
            writes,
            effects: Vec::new(),
            reply: ok_reply(),
            run_id: None,
        },
    )
}

fn committed(rows: &[(&str, &str)]) -> MemoryView {
    MemoryView::from_rows(
        rows.iter()
            .map(|(row_key, value)| ((FLOW, key(row_key)), key(value)))
            .collect::<BTreeMap<_, _>>(),
    )
}

fn texts(rows: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<(String, String)> {
    rows.into_iter()
        .map(|(row_key, value)| {
            (
                String::from_utf8(row_key).unwrap(),
                String::from_utf8(value).unwrap(),
            )
        })
        .collect()
}

fn pairs(rows: &[(&str, &str)]) -> Vec<(String, String)> {
    rows.iter()
        .map(|(row_key, value)| ((*row_key).to_owned(), (*value).to_owned()))
        .collect()
}

#[test]
fn get_returns_newest_overlay_decision_then_committed_value() {
    let base = committed(&[
        ("a", "committed-a"),
        ("b", "committed-b"),
        ("c", "committed-c"),
    ]);
    let mut overlay = SpeculativeOverlay::default();
    overlay.push(entry(1, vec![put("a", "first-a"), delete("b")]));
    overlay.push(entry(2, vec![put("a", "second-a"), put("b", "revived-b")]));
    overlay.push(entry(3, vec![delete_range("c", "d")]));
    let reader = overlay.reader(&base);

    assert_eq!(reader.get(FLOW, b"a").unwrap(), Some(key("second-a")));
    assert_eq!(reader.get(FLOW, b"b").unwrap(), Some(key("revived-b")));
    assert_eq!(reader.get(FLOW, b"c").unwrap(), None);
    assert_eq!(reader.get(FLOW, b"z").unwrap(), None);
    assert_eq!(
        reader.get(ColumnFamily::History, b"a").unwrap(),
        None,
        "writes of another column family are invisible"
    );
}

#[test]
fn put_after_delete_range_in_one_outcome_survives() {
    let base = committed(&[("k1", "old"), ("k2", "old")]);
    let mut overlay = SpeculativeOverlay::default();
    overlay.push(entry(1, vec![delete_range("k", "l"), put("k2", "new")]));
    let reader = overlay.reader(&base);

    assert_eq!(reader.get(FLOW, b"k1").unwrap(), None);
    assert_eq!(reader.get(FLOW, b"k2").unwrap(), Some(key("new")));
    assert_eq!(
        texts(reader.scan(FLOW, b"k", b"l", 10).unwrap()),
        pairs(&[("k2", "new")])
    );
}

#[test]
fn scan_merges_overlay_and_committed_rows_in_key_order() {
    let base = committed(&[("a", "1"), ("c", "3"), ("e", "5"), ("g", "7")]);
    let mut overlay = SpeculativeOverlay::default();
    overlay.push(entry(1, vec![put("b", "2"), put("c", "33"), delete("e")]));
    overlay.push(entry(2, vec![put("f", "6"), put("h", "8")]));
    let reader = overlay.reader(&base);

    assert_eq!(
        texts(reader.scan(FLOW, b"a", b"z", 100).unwrap()),
        pairs(&[
            ("a", "1"),
            ("b", "2"),
            ("c", "33"),
            ("f", "6"),
            ("g", "7"),
            ("h", "8")
        ])
    );
    assert_eq!(
        texts(reader.scan(FLOW, b"b", b"g", 100).unwrap()),
        pairs(&[("b", "2"), ("c", "33"), ("f", "6")])
    );
    assert_eq!(
        texts(reader.scan(FLOW, b"a", b"z", 3).unwrap()),
        pairs(&[("a", "1"), ("b", "2"), ("c", "33")])
    );
    assert!(reader.scan(FLOW, b"a", b"z", 0).unwrap().is_empty());
}

#[test]
fn scan_keeps_paging_committed_rows_when_overlay_deletes_a_whole_page() {
    let rows: Vec<(String, String)> = (0..10)
        .map(|index| (format!("k{index}"), format!("v{index}")))
        .collect();
    let row_refs: Vec<(&str, &str)> = rows
        .iter()
        .map(|(row_key, value)| (row_key.as_str(), value.as_str()))
        .collect();
    let base = committed(&row_refs);
    let mut overlay = SpeculativeOverlay::default();
    overlay.push(entry(1, vec![delete_range("k0", "k6")]));
    let reader = overlay.reader(&base);

    assert_eq!(
        texts(reader.scan(FLOW, b"k", b"l", 3).unwrap()),
        pairs(&[("k6", "v6"), ("k7", "v7"), ("k8", "v8")])
    );
    assert_eq!(
        texts(reader.scan(FLOW, b"k", b"l", 2).unwrap()),
        pairs(&[("k6", "v6"), ("k7", "v7")])
    );
}

#[test]
fn take_committed_matches_only_the_head_entry() {
    let mut overlay = SpeculativeOverlay::default();
    overlay.push(entry(1, vec![put("a", "1")]));
    overlay.push(entry(2, vec![put("b", "2")]));
    let bytes_before = overlay.total_bytes();
    assert!(bytes_before > 0);

    assert!(matches!(
        overlay.take_committed(&[2]),
        CommittedMatch::OutOfOrder
    ));
    assert!(matches!(
        overlay.take_committed(&[9]),
        CommittedMatch::Foreign
    ));
    assert!(matches!(
        overlay.take_committed(&[1]),
        CommittedMatch::Head(_)
    ));
    assert_eq!(overlay.len(), 1);
    assert!(overlay.total_bytes() < bytes_before);
    assert!(matches!(
        overlay.take_committed(&[2]),
        CommittedMatch::Head(_)
    ));
    assert!(overlay.is_empty());
    assert_eq!(overlay.total_bytes(), 0);
}
