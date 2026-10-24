// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_protocol::dex::{Value, value};
use dexzd_stream_store::{StreamStore, StreamStoreConfig, StreamStoreError, WriteStreamInput};
use prost::Message as ProstMessage;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn enabled_config() -> StreamStoreConfig {
    StreamStoreConfig {
        enabled: true,
        ..StreamStoreConfig::default()
    }
}

fn string_value(value: &str) -> Value {
    Value {
        kind: Some(value::Kind::StringValue(value.to_owned())),
    }
}

fn write_input(
    tenant_id: u64,
    flow_type: &str,
    flow_id: &str,
    stream_name: &str,
    source: &str,
    value: &str,
    capacity: u64,
) -> WriteStreamInput {
    WriteStreamInput {
        tenant_id,
        flow_id: flow_id.to_owned(),
        flow_type: flow_type.to_owned(),
        stream_name: stream_name.to_owned(),
        stream_capacity_bytes: Some(capacity as i64),
        value: string_value(value),
        source: source.to_owned(),
    }
}

async fn read_one(
    store: &StreamStore,
    tenant_id: u64,
    flow_type: &str,
    flow_id: &str,
    stream_name: &str,
    token: &str,
) -> dexzd_stream_store::StreamMessage {
    store
        .read(
            tenant_id,
            flow_type,
            flow_id,
            stream_name,
            token,
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .expect("Stream read succeeds")
        .expect("a message was written")
}

fn string_content(value: &Value) -> &str {
    match value.kind.as_ref().expect("Value has a kind") {
        value::Kind::StringValue(content) => content,
        _ => panic!("expected a string Value"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn reverse_pages_are_exclusive_and_newest_first() {
    let store = StreamStore::new(&enabled_config()).unwrap();
    for index in 0..5 {
        store
            .write(write_input(
                0,
                "OrderFlow",
                "flow-a",
                "Progress",
                &format!("source-{index}"),
                &format!("value-{index}"),
                1 << 20,
            ))
            .await
            .unwrap();
    }
    let first = store
        .list(0, "OrderFlow", "flow-a", "Progress", 2, "")
        .await
        .unwrap();
    assert_eq!(
        first
            .messages
            .iter()
            .map(|message| message.source.as_str())
            .collect::<Vec<_>>(),
        ["source-4", "source-3"]
    );
    assert!(!first.next_page_token.is_empty());
    store
        .write(write_input(
            0,
            "OrderFlow",
            "flow-a",
            "Progress",
            "source-5",
            "new",
            1 << 20,
        ))
        .await
        .unwrap();
    let second = store
        .list(
            0,
            "OrderFlow",
            "flow-a",
            "Progress",
            2,
            &first.next_page_token,
        )
        .await
        .unwrap();
    assert_eq!(
        second
            .messages
            .iter()
            .map(|message| message.source.as_str())
            .collect::<Vec<_>>(),
        ["source-2", "source-1"]
    );
    let last = store
        .list(
            0,
            "OrderFlow",
            "flow-a",
            "Progress",
            2,
            &second.next_page_token,
        )
        .await
        .unwrap();
    assert_eq!(last.messages.len(), 1);
    assert_eq!(last.messages[0].source, "source-0");
    assert!(last.next_page_token.is_empty());
    let empty = store
        .list(0, "OrderFlow", "missing-flow", "Progress", 2, "")
        .await
        .unwrap();
    assert!(empty.messages.is_empty());
    assert_eq!(
        store
            .list(0, "OrderFlow", "flow-a", "Progress", 0, "")
            .await
            .err(),
        Some(StreamStoreError::InvalidArgument(
            "page_size must be between 1 and max_read_messages"
        ))
    );
    assert!(matches!(
        store
            .list(0, "OrderFlow", "flow-a", "Progress", 1001, "")
            .await,
        Err(StreamStoreError::InvalidArgument(_))
    ));
    store.close().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn capacity_is_shared_across_flows_and_trimmed_in_global_fifo_order() {
    let store = StreamStore::new(&enabled_config()).unwrap();
    let sample = write_input(
        0,
        "OrderFlow",
        "flow-a",
        "Progress",
        "source-0",
        "value-0000",
        1,
    );
    let charge = sample.value.encoded_len() as u64
        + sample.flow_id.len() as u64
        + sample.source.len() as u64
        + 512;
    let capacity = charge * 10;
    for index in 0..8 {
        let flow_id = if index % 2 == 0 { "flow-a" } else { "flow-b" };
        store
            .write(write_input(
                0,
                "OrderFlow",
                flow_id,
                "Progress",
                &format!("source-{index}"),
                "value-0000",
                capacity,
            ))
            .await
            .unwrap();
    }
    tokio::task::yield_now().await;
    let below_trigger = store
        .list(0, "OrderFlow", "flow-a", "Progress", 10, "")
        .await
        .unwrap();
    assert_eq!(below_trigger.messages.len(), 4);
    assert_eq!(below_trigger.messages.last().unwrap().source, "source-0");
    for index in 8..10 {
        let flow_id = if index % 2 == 0 { "flow-a" } else { "flow-b" };
        store
            .write(write_input(
                0,
                "OrderFlow",
                flow_id,
                "Progress",
                &format!("source-{index}"),
                "value-0000",
                capacity,
            ))
            .await
            .unwrap();
    }
    let trimmed_token = read_one(&store, 0, "OrderFlow", "flow-a", "Progress", "")
        .await
        .resume_token;
    assert_eq!(
        store
            .write(write_input(
                0,
                "OrderFlow",
                "flow-a",
                "Progress",
                "source-x",
                "value-0000",
                capacity
            ))
            .await,
        Err(StreamStoreError::CapacityExceeded)
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let first = store
                .list(0, "OrderFlow", "flow-a", "Progress", 10, "")
                .await
                .unwrap();
            let second = store
                .list(0, "OrderFlow", "flow-b", "Progress", 10, "")
                .await
                .unwrap();
            if first.messages.len() + second.messages.len() == 8 {
                assert_eq!(first.messages.last().unwrap().source, "source-2");
                assert_eq!(second.messages.last().unwrap().source, "source-3");
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        read_one(&store, 0, "OrderFlow", "flow-a", "Progress", &trimmed_token)
            .await
            .source,
        "source-2"
    );
    assert!(
        store
            .list(0, "OrderFlow", "flow-a", "Progress", 2, &trimmed_token)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    store
        .write(write_input(
            0,
            "OrderFlow",
            "flow-a",
            "Progress",
            "source-8",
            "different",
            capacity,
        ))
        .await
        .unwrap();
    let page = store
        .list(0, "OrderFlow", "flow-a", "Progress", 10, "")
        .await
        .unwrap();
    assert_eq!(page.messages[0].source, "source-8");
    assert_eq!(string_content(&page.messages[0].value), "different");
    store.close().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn flow_type_and_tenant_isolate_capacity_and_messages() {
    let store = StreamStore::new(&enabled_config()).unwrap();
    let input = write_input(0, "OrderFlow", "flow-a", "Progress", "source", "value", 1);
    let charge = input.value.encoded_len() as u64
        + input.flow_id.len() as u64
        + input.source.len() as u64
        + 512;
    store
        .write(write_input(
            0,
            "OrderFlow",
            "flow-a",
            "Progress",
            "source",
            "value",
            charge,
        ))
        .await
        .unwrap();
    assert_eq!(
        store
            .write(write_input(
                0,
                "OrderFlow",
                "flow-b",
                "Progress",
                "source",
                "value",
                charge
            ))
            .await,
        Err(StreamStoreError::CapacityExceeded)
    );
    store
        .write(write_input(
            1,
            "OrderFlow",
            "flow-a",
            "Progress",
            "source",
            "tenant-one",
            charge + 100,
        ))
        .await
        .unwrap();
    store
        .write(write_input(
            0,
            "OtherFlow",
            "flow-a",
            "Progress",
            "source",
            "other-type",
            charge + 100,
        ))
        .await
        .unwrap();
    assert_eq!(
        string_content(
            &read_one(&store, 1, "OrderFlow", "flow-a", "Progress", "")
                .await
                .value
        ),
        "tenant-one"
    );
    assert_eq!(
        string_content(
            &read_one(&store, 0, "OtherFlow", "flow-a", "Progress", "")
                .await
                .value
        ),
        "other-type"
    );
    store.close().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn exact_protobuf_size_and_configuration_limits_apply() {
    let mut config = enabled_config();
    config.max_message_bytes = string_value("accepted").encoded_len();
    config.max_read_messages = 2;
    let store = StreamStore::new(&config).unwrap();
    store
        .write(write_input(
            0,
            "OrderFlow",
            "flow-a",
            "Progress",
            "source",
            "accepted",
            1 << 20,
        ))
        .await
        .unwrap();
    assert_eq!(
        store
            .write(write_input(
                0,
                "OrderFlow",
                "flow-a",
                "Progress",
                "source",
                "oversized",
                1 << 20
            ))
            .await,
        Err(StreamStoreError::MessageTooLarge)
    );
    assert!(matches!(
        store
            .list(0, "OrderFlow", "flow-a", "Progress", 3, "")
            .await,
        Err(StreamStoreError::InvalidArgument(_))
    ));
    assert_eq!(
        store
            .list(0, "OrderFlow", "flow-a", "Progress", 2, "")
            .await
            .unwrap()
            .messages
            .len(),
        1
    );
    store.close().await.unwrap();
    let default_store = StreamStore::new(&enabled_config()).unwrap();
    let exactly_100_kib = "a".repeat(100 * 1024 - 4);
    assert_eq!(string_value(&exactly_100_kib).encoded_len(), 100 * 1024);
    default_store
        .write(write_input(
            0,
            "OrderFlow",
            "flow-a",
            "Progress",
            "source",
            &exactly_100_kib,
            1 << 20,
        ))
        .await
        .unwrap();
    assert_eq!(
        default_store
            .write(write_input(
                0,
                "OrderFlow",
                "flow-a",
                "Progress",
                "source",
                &format!("{exactly_100_kib}a"),
                1 << 20,
            ))
            .await,
        Err(StreamStoreError::MessageTooLarge)
    );
    default_store.close().await.unwrap();
    let disabled = StreamStore::new(&StreamStoreConfig::default()).unwrap();
    assert_eq!(
        disabled
            .write(write_input(
                0,
                "OrderFlow",
                "flow-a",
                "Progress",
                "source",
                "value",
                1 << 20
            ))
            .await,
        Err(StreamStoreError::Disabled)
    );
    disabled.close().await.unwrap();
    let invalid = StreamStoreConfig {
        trim_trigger_percent: 80,
        trim_target_percent: 80,
        ..enabled_config()
    };
    assert!(matches!(
        StreamStore::new(&invalid),
        Err(StreamStoreError::InvalidConfig(_))
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn tokens_validate_every_scope_dimension_and_survive_trimming() {
    let store = StreamStore::new(&enabled_config()).unwrap();
    store
        .write(write_input(
            0,
            "OrderFlow",
            "flow-a",
            "Progress",
            "source-0",
            "first",
            1 << 20,
        ))
        .await
        .unwrap();
    let first = read_one(&store, 0, "OrderFlow", "flow-a", "Progress", "").await;
    for (tenant_id, flow_type, flow_id, stream_name) in [
        (1, "OrderFlow", "flow-a", "Progress"),
        (0, "OtherFlow", "flow-a", "Progress"),
        (0, "OrderFlow", "flow-b", "Progress"),
        (0, "OrderFlow", "flow-a", "OtherStream"),
    ] {
        assert!(matches!(
            store
                .list(
                    tenant_id,
                    flow_type,
                    flow_id,
                    stream_name,
                    1,
                    &first.resume_token
                )
                .await,
            Err(StreamStoreError::InvalidResumeToken)
        ));
    }
    assert!(matches!(
        store
            .read(
                0,
                "OrderFlow",
                "flow-a",
                "Progress",
                "malformed",
                Instant::now() + Duration::from_secs(1)
            )
            .await,
        Err(StreamStoreError::InvalidResumeToken)
    ));
    let mut tampered = first.resume_token.clone();
    tampered.push('0');
    assert!(matches!(
        store
            .list(0, "OrderFlow", "flow-a", "Progress", 1, &tampered)
            .await,
        Err(StreamStoreError::InvalidResumeToken)
    ));
    store
        .write(write_input(
            0,
            "OrderFlow",
            "flow-a",
            "Progress",
            "source-1",
            "second",
            1 << 20,
        ))
        .await
        .unwrap();
    let second = read_one(
        &store,
        0,
        "OrderFlow",
        "flow-a",
        "Progress",
        &first.resume_token,
    )
    .await;
    assert_eq!(second.source, "source-1");
    let older = store
        .list(
            0,
            "OrderFlow",
            "flow-a",
            "Progress",
            10,
            &second.resume_token,
        )
        .await
        .unwrap();
    assert_eq!(older.messages[0].source, "source-0");
    store.close().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn long_poll_wakes_only_for_its_flow_and_expires_at_deadline() {
    let store = Arc::new(StreamStore::new(&enabled_config()).unwrap());
    store
        .write(write_input(
            0,
            "OrderFlow",
            "flow-a",
            "Progress",
            "source-0",
            "first",
            1 << 20,
        ))
        .await
        .unwrap();
    let first = read_one(&store, 0, "OrderFlow", "flow-a", "Progress", "").await;
    let reader_store = Arc::clone(&store);
    let mut reader = tokio::spawn(async move {
        reader_store
            .read(
                0,
                "OrderFlow",
                "flow-a",
                "Progress",
                &first.resume_token,
                Instant::now() + Duration::from_secs(1),
            )
            .await
    });
    tokio::task::yield_now().await;
    store
        .write(write_input(
            0,
            "OrderFlow",
            "flow-b",
            "Progress",
            "other",
            "other",
            1 << 20,
        ))
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut reader)
            .await
            .is_err()
    );
    store
        .write(write_input(
            0,
            "OrderFlow",
            "flow-a",
            "Progress",
            "source-1",
            "second",
            1 << 20,
        ))
        .await
        .unwrap();
    let second = reader.await.unwrap().unwrap().unwrap();
    assert_eq!(second.source, "source-1");
    assert!(
        store
            .read(
                0,
                "OrderFlow",
                "flow-a",
                "Progress",
                &second.resume_token,
                Instant::now() + Duration::from_millis(10)
            )
            .await
            .unwrap()
            .is_none()
    );
    store.close().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn worker_write_is_best_effort_and_close_wakes_readers() {
    let store = Arc::new(StreamStore::new(&enabled_config()).unwrap());
    assert!(
        store
            .write_worker_best_effort(
                write_input(
                    0,
                    "OrderFlow",
                    "flow-a",
                    "Progress",
                    "ignored",
                    "worker",
                    1 << 20
                ),
                "step-17"
            )
            .await
    );
    let message = read_one(&store, 0, "OrderFlow", "flow-a", "Progress", "").await;
    assert_eq!(message.source, "#step-17");
    assert!(
        !store
            .write_worker_best_effort(
                write_input(
                    0,
                    "OrderFlow",
                    "flow-a",
                    "Progress",
                    "ignored",
                    "rejected",
                    1 << 20,
                ),
                "",
            )
            .await
    );
    assert_eq!(
        store
            .list(0, "OrderFlow", "flow-a", "Progress", 10, "")
            .await
            .unwrap()
            .messages
            .len(),
        1,
    );
    assert!(
        !store
            .write_worker_best_effort(
                write_input(0, "OrderFlow", "flow-a", "Progress", "ignored", "worker", 1),
                "step-17"
            )
            .await
    );
    let reader_store = Arc::clone(&store);
    let reader = tokio::spawn(async move {
        reader_store
            .read(
                0,
                "OrderFlow",
                "flow-a",
                "Progress",
                &message.resume_token,
                Instant::now() + Duration::from_secs(10),
            )
            .await
    });
    tokio::task::yield_now().await;
    store.close().await.unwrap();
    assert!(matches!(
        reader.await.unwrap(),
        Err(StreamStoreError::Closed)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_capacity_rejections_do_not_over_trim() {
    let store = Arc::new(StreamStore::new(&enabled_config()).unwrap());
    let sample = write_input(
        0,
        "OrderFlow",
        "flow-a",
        "Progress",
        "run#step",
        "payload-0000",
        1,
    );
    let charge = sample.value.encoded_len() as u64
        + sample.flow_id.len() as u64
        + sample.source.len() as u64
        + 512;
    for _ in 0..80 {
        store
            .write(write_input(
                0,
                "OrderFlow",
                "flow-a",
                "Progress",
                "run#step",
                "payload-0000",
                charge * 100,
            ))
            .await
            .unwrap();
    }
    let start_barrier = Arc::new(tokio::sync::Barrier::new(21));
    let mut writers = Vec::new();
    for _ in 0..20 {
        let writer_store = Arc::clone(&store);
        let writer_barrier = Arc::clone(&start_barrier);
        let writer = tokio::spawn(async move {
            writer_barrier.wait().await;
            writer_store
                .write(write_input(
                    0,
                    "OrderFlow",
                    "flow-a",
                    "Progress",
                    "run#step",
                    "payload-0000",
                    charge * 10,
                ))
                .await
        });
        writers.push(writer);
    }
    start_barrier.wait().await;
    let mut rejected = 0;
    for writer in writers {
        match writer.await.unwrap() {
            Ok(()) => {}
            Err(StreamStoreError::CapacityExceeded) => rejected += 1,
            Err(error) => panic!("unexpected write error: {error}"),
        }
    }
    assert!(rejected > 0);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let count = store
                .list(0, "OrderFlow", "flow-a", "Progress", 100, "")
                .await
                .unwrap()
                .messages
                .len();
            if (8..=9).contains(&count) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    store
        .write(write_input(
            0,
            "OrderFlow",
            "flow-a",
            "Progress",
            "run#step",
            "replacement",
            charge * 10,
        ))
        .await
        .unwrap();
    store.close().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn resume_token_can_read_after_a_store_restart() {
    let first_store = StreamStore::new(&enabled_config()).unwrap();
    first_store
        .write(write_input(
            0,
            "OrderFlow",
            "flow-a",
            "Progress",
            "before",
            "first",
            1 << 20,
        ))
        .await
        .unwrap();
    let first = read_one(&first_store, 0, "OrderFlow", "flow-a", "Progress", "").await;
    first_store.close().await.unwrap();
    let first_milliseconds = first
        .created_time
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    tokio::time::timeout(Duration::from_secs(1), async {
        while std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            <= first_milliseconds
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let second_store = StreamStore::new(&enabled_config()).unwrap();
    second_store
        .write(write_input(
            0,
            "OrderFlow",
            "flow-a",
            "Progress",
            "after",
            "second",
            1 << 20,
        ))
        .await
        .unwrap();
    let second = read_one(
        &second_store,
        0,
        "OrderFlow",
        "flow-a",
        "Progress",
        &first.resume_token,
    )
    .await;
    assert_eq!(second.source, "after");
    second_store.close().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn missing_and_nonpositive_capacity_are_rejected_by_default() {
    let store = StreamStore::new(&enabled_config()).unwrap();
    let mut missing = write_input(
        0,
        "OrderFlow",
        "flow-a",
        "Progress",
        "source",
        "value",
        1 << 20,
    );
    missing.stream_capacity_bytes = None;
    assert_eq!(
        store.write(missing).await,
        Err(StreamStoreError::InvalidArgument(
            "stream_capacity_bytes is required"
        ))
    );
    for capacity in [0, -1] {
        let mut invalid = write_input(
            0,
            "OrderFlow",
            "flow-a",
            "Progress",
            "source",
            "value",
            1 << 20,
        );
        invalid.stream_capacity_bytes = Some(capacity);
        assert_eq!(
            store.write(invalid).await,
            Err(StreamStoreError::InvalidArgument(
                "stream_capacity_bytes must be positive"
            ))
        );
    }
    store.close().await.unwrap();

    let configured = StreamStoreConfig {
        capacity_per_stream_bytes: 1 << 20,
        ..enabled_config()
    };
    let fallback_store = StreamStore::new(&configured).unwrap();
    let mut internal_write = write_input(
        0,
        "OrderFlow",
        "flow-a",
        "Progress",
        "source",
        "value",
        1 << 20,
    );
    internal_write.stream_capacity_bytes = None;
    fallback_store.write(internal_write).await.unwrap();
    fallback_store.close().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn expired_deadline_and_post_close_calls_have_distinct_results() {
    let store = StreamStore::new(&enabled_config()).unwrap();
    assert!(
        store
            .read(
                0,
                "OrderFlow",
                "flow-a",
                "Progress",
                "",
                Instant::now() - Duration::from_millis(1)
            )
            .await
            .unwrap()
            .is_none()
    );
    store.close().await.unwrap();
    assert_eq!(
        store
            .write(write_input(
                0,
                "OrderFlow",
                "flow-a",
                "Progress",
                "source",
                "value",
                1 << 20
            ))
            .await,
        Err(StreamStoreError::Closed)
    );
    assert!(matches!(
        store
            .read(
                0,
                "OrderFlow",
                "flow-a",
                "Progress",
                "",
                Instant::now() + Duration::from_secs(1)
            )
            .await,
        Err(StreamStoreError::Closed)
    ));
    assert!(matches!(
        store
            .list(0, "OrderFlow", "flow-a", "Progress", 1, "")
            .await,
        Err(StreamStoreError::Closed)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_close_calls_both_finish_after_shutdown() {
    let store = Arc::new(StreamStore::new(&enabled_config()).unwrap());
    store
        .write(write_input(
            0,
            "OrderFlow",
            "flow-a",
            "Progress",
            "source",
            "value",
            1 << 20,
        ))
        .await
        .unwrap();
    let start_barrier = Arc::new(tokio::sync::Barrier::new(3));
    let first_store = Arc::clone(&store);
    let first_barrier = Arc::clone(&start_barrier);
    let first_close = tokio::spawn(async move {
        first_barrier.wait().await;
        first_store.close().await
    });
    let second_store = Arc::clone(&store);
    let second_barrier = Arc::clone(&start_barrier);
    let second_close = tokio::spawn(async move {
        second_barrier.wait().await;
        second_store.close().await
    });
    start_barrier.wait().await;
    first_close.await.unwrap().unwrap();
    second_close.await.unwrap().unwrap();
    assert!(matches!(
        store
            .list(0, "OrderFlow", "flow-a", "Progress", 1, "")
            .await,
        Err(StreamStoreError::Closed)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn configuration_rules_fail_at_startup() {
    let configurations = [
        StreamStoreConfig {
            max_message_bytes: 0,
            ..enabled_config()
        },
        StreamStoreConfig {
            max_read_messages: 0,
            ..enabled_config()
        },
        StreamStoreConfig {
            max_read_messages: 1001,
            ..enabled_config()
        },
        StreamStoreConfig {
            trim_trigger_percent: 0,
            ..enabled_config()
        },
        StreamStoreConfig {
            trim_trigger_percent: 100,
            ..enabled_config()
        },
        StreamStoreConfig {
            trim_target_percent: 0,
            ..enabled_config()
        },
        StreamStoreConfig {
            trim_target_percent: 90,
            ..enabled_config()
        },
    ];
    for configuration in configurations {
        assert!(matches!(
            StreamStore::new(&configuration),
            Err(StreamStoreError::InvalidConfig(_))
        ));
    }
}
