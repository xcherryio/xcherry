// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;
use std::time::Duration;

use dexzd_apply::{FlowStateReader, StateRow, StateWrite};
use dexzd_keyspace::{ColumnFamily, RunId, RunTable, run_key};
use dexzd_protocol::dex::{ChannelMessage, PublishToChannelRequest};
use dexzd_protocol::dexzd::v1::{
    ChannelMetaRecord, ChannelPublishCommand, FlowCommand, flow_command,
};
use prost::Message;

use crate::runtime::tests::fixtures::{
    attributes_set_command, flow, int_value, start_flow, string_value,
};
use crate::runtime::tests::test_support::RuntimeHarness;
use crate::runtime::{CommitError, CommitUrgency, RuntimeError};

const CHANNEL: &str = "orders";

fn channel_publish_command(flow_id: &str, run_id: RunId, message_id: &str) -> FlowCommand {
    FlowCommand {
        flow_id: flow_id.to_owned(),
        run_id: run_id.as_bytes().to_vec(),
        command: Some(flow_command::Command::ChannelPublish(
            ChannelPublishCommand {
                request: Some(PublishToChannelRequest {
                    flow_id: flow_id.to_owned(),
                    messages: vec![ChannelMessage {
                        channel_name: CHANNEL.to_owned(),
                        value: Some(string_value(message_id)),
                        message_id: message_id.to_owned(),
                    }],
                    ..PublishToChannelRequest::default()
                }),
            },
        )),
        ..FlowCommand::default()
    }
}

fn committed_rows(harness: &RuntimeHarness) -> Vec<StateRow> {
    harness
        .store
        .view()
        .scan(ColumnFamily::Flow, &[], &[0xff; 64], usize::MAX)
        .unwrap()
}

fn committed_effect_count(harness: &RuntimeHarness) -> usize {
    harness
        .effect_calls()
        .iter()
        .filter(|(_, _, is_committed)| *is_committed)
        .count()
}

#[tokio::test(start_paused = true)]
async fn unobserved_failed_ticket_halts_and_no_dependent_command_is_persisted() {
    let harness = RuntimeHarness::new();
    let run_id = start_flow(&harness, "order-1").await.unwrap();
    let address = flow("order-1");
    let rows_before = committed_rows(&harness);
    let committed_effects_before = committed_effect_count(&harness);

    let first = harness
        .registry
        .propose(
            address.clone(),
            channel_publish_command("order-1", run_id, "message-a"),
            CommitUrgency::Batched,
        )
        .await
        .unwrap();
    // Nobody waits for the first ticket.
    drop(first);
    let second = harness
        .registry
        .propose(
            address.clone(),
            channel_publish_command("order-1", run_id, "message-b"),
            CommitUrgency::Batched,
        )
        .await
        .unwrap();
    let meta_key = run_key(
        harness.registry.flow_context(&address).flow_hash,
        run_id,
        RunTable::ChannelMeta,
        CHANNEL.as_bytes(),
    );
    let speculative_meta = second
        .speculative
        .writes
        .iter()
        .find_map(|write| match write {
            StateWrite::Put { key, value, .. } if *key == meta_key => {
                Some(ChannelMetaRecord::decode(value.as_slice()).unwrap())
            }
            _ => None,
        })
        .expect("the second publish writes the channel meta");
    assert_eq!(
        speculative_meta.pending_count, 2,
        "the second command depends on the first one's speculative state"
    );

    let barrier = harness.registry.sync_barrier(&address);
    tokio::pin!(barrier);
    tokio::select! {
        _ = &mut barrier => panic!("the SYNC barrier waits for both commands"),
        () = tokio::time::sleep(Duration::from_millis(10)) => {}
    }
    assert!(harness.fail_next(CommitError::Unavailable {
        detail: "object storage outage".to_owned(),
    }));
    assert!(harness.registry.halted().is_cancelled());
    let barrier_error = barrier.await.unwrap_err();
    assert!(
        matches!(barrier_error, RuntimeError::NodeStopping { .. }),
        "{barrier_error:?}"
    );

    assert!(harness.commit_next(), "the second command was queued");
    let second_error = second.ticket.wait().await.unwrap_err();
    assert!(
        matches!(second_error, CommitError::NodeStopping { .. }),
        "{second_error:?}"
    );
    assert_eq!(
        committed_rows(&harness),
        rows_before,
        "neither command is persisted"
    );
    assert_eq!(committed_effect_count(&harness), committed_effects_before);

    let proposal_error = harness
        .registry
        .propose(
            address.clone(),
            channel_publish_command("order-1", run_id, "message-c"),
            CommitUrgency::Batched,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(proposal_error, RuntimeError::NodeStopping { .. }),
        "{proposal_error:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn ticket_that_failed_before_the_proposal_returned_still_halts() {
    let harness = RuntimeHarness::new();
    let run_id = start_flow(&harness, "order-1").await.unwrap();
    // Fails the ticket after submit, before the proposal installs its failure observer.
    *harness.applier.apply_hook.lock().unwrap() = Some({
        let committer = Arc::clone(&harness.committer);
        Arc::new(move || {
            if let Some(pending) = committer.take_next() {
                pending.sender.resolve(Err(CommitError::NodeStopping {
                    detail: "pipeline fenced".to_owned(),
                }));
            }
        })
    });
    let proposal = harness
        .registry
        .propose(
            flow("order-1"),
            attributes_set_command("order-1", run_id, "count", int_value(1)),
            CommitUrgency::Batched,
        )
        .await;
    assert!(proposal.is_ok(), "{proposal:?}");
    assert!(harness.registry.halted().is_cancelled());
}
