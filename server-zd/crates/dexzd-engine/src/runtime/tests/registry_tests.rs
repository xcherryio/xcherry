// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::time::Duration;

use dexzd_apply::{ApplyEffect, ApplyOutcome, StateWrite, WaitableChange};
use dexzd_keyspace::{ColumnFamily, RunId};
use dexzd_protocol::dex::{
    AttributeMatch, AttributeMatchOperator, GetAttributesRequest, WaitForAttributeRequest,
};
use dexzd_protocol::dexzd::v1::{
    ChildFlowStartMessage, FlowCommand, InboxMessageCommand, flow_command, inbox_message_command,
};

use crate::runtime::tests::fixtures::{
    TENANT, attributes_set_command, flow, int_value, start_flow, string_value,
};
use crate::runtime::tests::model_applier::ok_reply;
use crate::runtime::tests::test_support::{RuntimeHarness, invariant_error};
use crate::runtime::{
    CommitError, CommitUrgency, CommittedCommand, CommittedFlowCommand, FlowAddress,
    ReplayMismatch, RuntimeConfig, RuntimeError,
};

async fn started(harness: &RuntimeHarness, flow_id: &str) -> RunId {
    start_flow(harness, flow_id).await.unwrap()
}

fn attribute_command(flow_id: &str, run_id: RunId, key: &str, number: i64) -> FlowCommand {
    attributes_set_command(flow_id, run_id, key, int_value(number))
}

#[tokio::test(start_paused = true)]
async fn speculative_apply_sees_earlier_uncommitted_commands_and_commit_drops_them() {
    let harness = RuntimeHarness::new();
    let run_id = started(&harness, "order-1").await;
    let address = flow("order-1");

    let first = harness
        .registry
        .propose(
            address.clone(),
            attribute_command("order-1", run_id, "count", 1),
            CommitUrgency::Batched,
        )
        .await
        .unwrap();
    let second = harness
        .registry
        .propose(
            address.clone(),
            attribute_command("order-1", run_id, "status", 2),
            CommitUrgency::Batched,
        )
        .await
        .unwrap();

    let applied = harness.applier.applied.lock().unwrap().clone();
    let stamped_second = applied.last().unwrap();
    assert_eq!(
        stamped_second.epoch, 1,
        "overlay applies the stamped command"
    );
    assert!(!stamped_second.command_id.is_empty());
    assert_eq!(harness.committer.pending_count(), 2);
    assert!(harness.registry.uncommitted_bytes() > 0);
    assert_eq!(harness.registry.runtime_count(), 1);
    assert!(!first.speculative.writes.is_empty());
    assert!(!second.speculative.writes.is_empty());

    assert_eq!(harness.commit_all(), 2);
    assert!(first.ticket.wait().await.is_ok());
    assert!(second.ticket.wait().await.is_ok());
    assert_eq!(harness.registry.uncommitted_bytes(), 0);
    assert_eq!(
        harness.registry.runtime_count(),
        0,
        "an idle runtime is reclaimed"
    );
    assert!(!harness.registry.halted().is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn overlay_reader_layers_uncommitted_writes_for_the_next_proposal() {
    let harness = RuntimeHarness::new();
    let run_id = started(&harness, "order-1").await;
    let address = flow("order-1");
    harness
        .registry
        .propose(
            address.clone(),
            attribute_command("order-1", run_id, "count", 1),
            CommitUrgency::Batched,
        )
        .await
        .unwrap();

    let mut observed = None;
    let context = harness.registry.flow_context(&address);
    let proposal = harness
        .registry
        .propose_with(&address, CommitUrgency::Batched, |scope| {
            let key = dexzd_keyspace::run_key(
                context.flow_hash,
                run_id,
                dexzd_keyspace::RunTable::Attribute,
                b"count",
            );
            observed = Some(scope.state.get(ColumnFamily::Flow, &key)?.is_some());
            Ok(None::<(FlowCommand, ())>)
        })
        .await
        .unwrap();
    assert!(proposal.is_none());
    assert_eq!(
        observed,
        Some(true),
        "uncommitted write is visible to proposals"
    );
    let committed_view = harness.store.view();
    let key = dexzd_keyspace::run_key(
        context.flow_hash,
        run_id,
        dexzd_keyspace::RunTable::Attribute,
        b"count",
    );
    assert!(
        dexzd_apply::FlowStateReader::get(&committed_view, ColumnFamily::Flow, &key)
            .unwrap()
            .is_none(),
        "committed state does not see the speculative write"
    );
}

#[tokio::test(start_paused = true)]
async fn mismatched_committed_outcome_halts_the_registry() {
    let harness = RuntimeHarness::new();
    let run_id = started(&harness, "order-1").await;
    let address = flow("order-1");
    harness
        .registry
        .propose(
            address.clone(),
            attribute_command("order-1", run_id, "count", 1),
            CommitUrgency::Prompt,
        )
        .await
        .unwrap();
    *harness.tamper.lock().unwrap() = Some(Box::new(|outcome: &mut ApplyOutcome| {
        outcome.writes.push(StateWrite::Delete {
            column_family: ColumnFamily::Flow,
            key: b"unexpected".to_vec(),
        });
    }));

    assert!(harness.commit_next());

    assert!(harness.registry.halted().is_cancelled());
    let error = harness
        .registry
        .propose(
            address,
            attribute_command("order-1", run_id, "count", 2),
            CommitUrgency::Prompt,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, RuntimeError::NodeStopping { .. }),
        "{error:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn reply_bytes_are_part_of_the_comparison() {
    let harness = RuntimeHarness::new();
    let run_id = started(&harness, "order-1").await;
    harness
        .registry
        .propose(
            flow("order-1"),
            attribute_command("order-1", run_id, "count", 1),
            CommitUrgency::Prompt,
        )
        .await
        .unwrap();
    *harness.tamper.lock().unwrap() = Some(Box::new(|outcome: &mut ApplyOutcome| {
        outcome.reply = crate::runtime::tests::model_applier::not_found_reply("order-1");
    }));
    harness.commit_next();
    assert!(harness.registry.halted().is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn out_of_order_commit_halts_the_registry() {
    let harness = RuntimeHarness::new();
    let run_id = started(&harness, "order-1").await;
    let address = flow("order-1");
    for number in 0..2 {
        harness
            .registry
            .propose(
                address.clone(),
                attribute_command("order-1", run_id, "count", number),
                CommitUrgency::Batched,
            )
            .await
            .unwrap();
    }
    let first = harness.committer.take_next().unwrap();
    let second = harness.committer.take_next().unwrap();
    let view = harness.store.view();
    let outcome = dexzd_apply::ApplyOutcome {
        writes: Vec::new(),
        effects: Vec::new(),
        reply: ok_reply(),
        run_id: Some(run_id),
    };
    drop((first, view));
    harness.registry.apply_committed(CommittedFlowCommand {
        command: second.command,
        context: second.context,
        committed: CommittedCommand {
            outcome,
            position: Default::default(),
        },
    });
    assert!(harness.registry.halted().is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn committed_outcome_of_a_replayed_command_does_not_forward_effects() {
    let harness = RuntimeHarness::new();
    let address = FlowAddress::new(TENANT, "replayed");
    let run_id = RunId([9; 16]);
    harness.registry.apply_committed(CommittedFlowCommand {
        command: FlowCommand {
            flow_id: "replayed".to_owned(),
            command_id: vec![42; 16],
            ..FlowCommand::default()
        },
        context: harness.registry.flow_context(&address),
        committed: CommittedCommand {
            outcome: ApplyOutcome {
                writes: Vec::new(),
                effects: vec![ApplyEffect::WakeWaiters {
                    run_id,
                    change: WaitableChange::FlowClosed,
                }],
                reply: ok_reply(),
                run_id: Some(run_id),
            },
            position: Default::default(),
        },
    });
    assert!(
        harness.effect_calls().is_empty(),
        "partition rebuild owns the effects of replayed commands"
    );
    assert!(!harness.registry.halted().is_cancelled());
    assert_eq!(harness.registry.runtime_count(), 0);
    assert_eq!(address.flow_id, "replayed");
}

#[tokio::test(start_paused = true)]
async fn batched_proposals_forward_speculative_effects_and_prompt_ones_do_not() {
    let harness = RuntimeHarness::new();
    let run_id = started(&harness, "order-1").await;
    harness.effects.calls.lock().unwrap().clear();
    let address = flow("order-1");
    harness
        .registry
        .propose(
            address.clone(),
            attribute_command("order-1", run_id, "count", 1),
            CommitUrgency::Prompt,
        )
        .await
        .unwrap();
    assert!(
        harness.effect_calls().is_empty(),
        "SYNC effects wait for the commit"
    );
    harness
        .registry
        .propose(
            address.clone(),
            attribute_command("order-1", run_id, "count", 2),
            CommitUrgency::Batched,
        )
        .await
        .unwrap();
    let calls = harness.effect_calls();
    assert_eq!(calls.len(), 1);
    assert!(!calls[0].2, "ASYNC speculative effects are not committed");

    harness.commit_all();
    let committed_calls: Vec<_> = harness
        .effect_calls()
        .into_iter()
        .filter(|(_, _, is_committed)| *is_committed)
        .collect();
    assert_eq!(committed_calls.len(), 2);
}

#[tokio::test(start_paused = true)]
async fn per_flow_speculation_limit_waits_for_commits_then_times_out() {
    let config = RuntimeConfig {
        max_speculative_commands_per_flow: 2,
        speculation_capacity_wait: Duration::from_secs(1),
        ..RuntimeConfig::default()
    };
    let harness = RuntimeHarness::with_config(&config);
    let run_id = started(&harness, "order-1").await;
    let address = flow("order-1");
    for number in 0..2 {
        harness
            .registry
            .propose(
                address.clone(),
                attribute_command("order-1", run_id, "count", number),
                CommitUrgency::Batched,
            )
            .await
            .unwrap();
    }

    let error = harness
        .registry
        .propose(
            address.clone(),
            attribute_command("order-1", run_id, "count", 3),
            CommitUrgency::Batched,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, RuntimeError::Unavailable { .. }),
        "{error:?}"
    );
    assert_eq!(
        harness.committer.pending_count(),
        2,
        "nothing was submitted"
    );

    let third = harness.registry.propose(
        address.clone(),
        attribute_command("order-1", run_id, "count", 3),
        CommitUrgency::Batched,
    );
    tokio::pin!(third);
    tokio::select! {
        _ = &mut third => panic!("the third proposal must wait"),
        () = tokio::time::sleep(Duration::from_millis(100)) => {}
    }
    harness.commit_next();
    assert!(third.await.is_ok(), "a commit frees capacity");
}

#[tokio::test(start_paused = true)]
async fn node_byte_limit_admits_one_command_and_waits_for_the_rest() {
    let config = RuntimeConfig {
        max_speculative_bytes_per_node: 1,
        speculation_capacity_wait: Duration::from_millis(500),
        ..RuntimeConfig::default()
    };
    let harness = RuntimeHarness::with_config(&config);
    let first_run = started(&harness, "order-1").await;
    let second_run = started(&harness, "order-2").await;
    harness
        .registry
        .propose(
            flow("order-1"),
            attribute_command("order-1", first_run, "count", 1),
            CommitUrgency::Batched,
        )
        .await
        .unwrap();
    let error = harness
        .registry
        .propose(
            flow("order-2"),
            attribute_command("order-2", second_run, "count", 1),
            CommitUrgency::Batched,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, RuntimeError::Unavailable { .. }),
        "{error:?}"
    );
    harness.commit_all();
    harness
        .registry
        .propose(
            flow("order-2"),
            attribute_command("order-2", second_run, "count", 1),
            CommitUrgency::Batched,
        )
        .await
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn submit_backpressure_returns_unavailable_without_an_overlay_entry() {
    let harness = RuntimeHarness::new();
    let run_id = started(&harness, "order-1").await;
    harness
        .committer
        .submit_errors
        .lock()
        .unwrap()
        .push_back(CommitError::Unavailable {
            detail: "object storage is slow".to_owned(),
        });
    let error = harness
        .registry
        .propose(
            flow("order-1"),
            attribute_command("order-1", run_id, "count", 1),
            CommitUrgency::Prompt,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, RuntimeError::Unavailable { .. }),
        "{error:?}"
    );
    assert_eq!(harness.registry.uncommitted_bytes(), 0);
    assert_eq!(harness.registry.runtime_count(), 0);
}

#[tokio::test(start_paused = true)]
async fn speculative_apply_error_halts_the_registry() {
    let harness = RuntimeHarness::new();
    let run_id = started(&harness, "order-1").await;
    harness
        .applier
        .scripted
        .lock()
        .unwrap()
        .push_back(Err(invariant_error("corrupt state")));
    let error = harness
        .registry
        .propose(
            flow("order-1"),
            attribute_command("order-1", run_id, "count", 1),
            CommitUrgency::Prompt,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, RuntimeError::NodeStopping { .. }),
        "{error:?}"
    );
    assert!(harness.registry.halted().is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn failed_commit_halts_the_registry_and_fails_the_client() {
    let harness = RuntimeHarness::new();
    started(&harness, "order-1").await;
    let address = flow("order-1");
    let set = harness.registry.set_attributes(
        TENANT,
        crate::runtime::tests::fixtures::set_attribute_request(
            "order-1",
            "status",
            string_value("paid"),
        ),
    );
    tokio::pin!(set);
    tokio::select! {
        _ = &mut set => panic!("set_attributes waits for its commit"),
        () = harness.committer.submitted.notified() => {}
    }
    assert!(harness.fail_next(CommitError::NodeStopping {
        detail: "pipeline fenced".to_owned(),
    }));
    let error = set.await.unwrap_err();
    assert!(
        matches!(error, RuntimeError::NodeStopping { .. }),
        "{error:?}"
    );
    assert!(harness.registry.halted().is_cancelled());
    let barrier_error = harness.registry.sync_barrier(&address).await.unwrap_err();
    assert!(
        matches!(barrier_error, RuntimeError::NodeStopping { .. }),
        "{barrier_error:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn failed_commit_never_lets_a_queued_later_command_settle_as_foreign() {
    let harness = RuntimeHarness::new();
    let run_id = started(&harness, "order-1").await;
    let address = flow("order-1");
    let mut proposals = Vec::new();
    for number in 1..=2 {
        proposals.push(
            harness
                .registry
                .propose(
                    address.clone(),
                    attribute_command("order-1", run_id, "count", number),
                    CommitUrgency::Batched,
                )
                .await
                .unwrap(),
        );
    }
    assert!(harness.fail_next(CommitError::Unavailable {
        detail: "object storage outage".to_owned(),
    }));
    let first = proposals.remove(0);
    let error = harness
        .registry
        .wait_for_commit(&address, first.ticket)
        .await
        .unwrap_err();
    assert!(
        matches!(error, RuntimeError::Unavailable { .. }),
        "{error:?}"
    );
    assert!(harness.registry.halted().is_cancelled());
    let barrier_error = harness.registry.sync_barrier(&address).await.unwrap_err();
    assert!(
        matches!(barrier_error, RuntimeError::NodeStopping { .. }),
        "{barrier_error:?}"
    );

    let committed_effects_before = committed_effect_count(&harness);
    assert!(harness.commit_next(), "the second command was queued");
    assert_eq!(
        committed_effect_count(&harness),
        committed_effects_before,
        "a halted registry runs no committed effects"
    );
}

fn committed_effect_count(harness: &RuntimeHarness) -> usize {
    harness
        .effect_calls()
        .iter()
        .filter(|(_, _, is_committed)| *is_committed)
        .count()
}

#[tokio::test(start_paused = true)]
async fn dropped_ticket_reports_node_stopping() {
    let harness = RuntimeHarness::new();
    let run_id = started(&harness, "order-1").await;
    let proposal = harness
        .registry
        .propose(
            flow("order-1"),
            attribute_command("order-1", run_id, "count", 1),
            CommitUrgency::Prompt,
        )
        .await
        .unwrap();
    assert!(harness.drop_next());
    assert!(
        harness.registry.halted().is_cancelled(),
        "a dropped ticket halts without a waiter"
    );
    let error = proposal.ticket.wait().await.unwrap_err();
    assert!(matches!(error, CommitError::NodeStopping { .. }));
}

#[tokio::test(start_paused = true)]
async fn sync_barrier_waits_for_every_earlier_proposal() {
    let harness = RuntimeHarness::new();
    let run_id = started(&harness, "order-1").await;
    let address = flow("order-1");
    harness.registry.sync_barrier(&address).await.unwrap();
    for number in 0..2 {
        harness
            .registry
            .propose(
                address.clone(),
                attribute_command("order-1", run_id, "count", number),
                CommitUrgency::Batched,
            )
            .await
            .unwrap();
    }
    let barrier = harness.registry.sync_barrier(&address);
    tokio::pin!(barrier);
    tokio::select! {
        _ = &mut barrier => panic!("barrier must wait for uncommitted commands"),
        () = tokio::time::sleep(Duration::from_millis(10)) => {}
    }
    harness.commit_next();
    tokio::select! {
        _ = &mut barrier => panic!("barrier must wait for the second command"),
        () = tokio::time::sleep(Duration::from_millis(10)) => {}
    }
    harness.commit_next();
    barrier.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn halt_ends_a_waiting_sync_barrier() {
    let harness = RuntimeHarness::new();
    let run_id = started(&harness, "order-1").await;
    let address = flow("order-1");
    harness
        .registry
        .propose(
            address.clone(),
            attribute_command("order-1", run_id, "count", 1),
            CommitUrgency::Batched,
        )
        .await
        .unwrap();
    let barrier = harness.registry.sync_barrier(&address);
    tokio::pin!(barrier);
    tokio::select! {
        _ = &mut barrier => panic!("barrier must wait"),
        () = tokio::time::sleep(Duration::from_millis(10)) => {}
    }
    harness.registry.halt("test halt".to_owned());
    let error = barrier.await.unwrap_err();
    assert!(
        matches!(error, RuntimeError::NodeStopping { .. }),
        "{error:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn owner_crash_before_commit_keeps_only_committed_state() {
    let crashed = RuntimeHarness::new();
    let run_id = started(&crashed, "order-1").await;
    crashed
        .registry
        .propose(
            flow("order-1"),
            attribute_command("order-1", run_id, "count", 1),
            CommitUrgency::Batched,
        )
        .await
        .unwrap();
    let store = crashed.store.clone();
    drop(crashed);

    let restarted = RuntimeHarness::with_store(&RuntimeConfig::default(), store);
    let attributes = restarted
        .registry
        .get_attributes(
            TENANT,
            GetAttributesRequest {
                flow_id: "order-1".to_owned(),
                all_keys: true,
                ..GetAttributesRequest::default()
            },
        )
        .await
        .unwrap();
    assert!(
        attributes.attributes.is_empty(),
        "the uncommitted write never happened"
    );
    assert_eq!(restarted.registry.uncommitted_bytes(), 0);

    let wait = restarted.registry.wait_for_attribute(
        TENANT,
        WaitForAttributeRequest {
            flow_id: "order-1".to_owned(),
            r#match: Some(AttributeMatch {
                key: "count".to_owned(),
                operator: AttributeMatchOperator::Equal as i32,
                operand: Some(int_value(1)),
            }),
            ..WaitForAttributeRequest::default()
        },
    );
    tokio::pin!(wait);
    tokio::select! {
        _ = &mut wait => panic!("nothing is committed yet"),
        () = tokio::time::sleep(Duration::from_millis(10)) => {}
    }

    // Recovery replays a command this registry never proposed.
    let address = flow("order-1");
    let context = restarted.registry.flow_context(&address);
    let mut replayed = attribute_command("order-1", run_id, "count", 1);
    replayed.command_id = vec![77; 16];
    let outcome = crate::CommandApplier::apply(
        restarted.applier.as_ref(),
        &context,
        &restarted.store.view(),
        &replayed,
    )
    .unwrap();
    restarted.store.apply_writes(&outcome.writes);
    restarted.registry.apply_committed(CommittedFlowCommand {
        command: replayed,
        context,
        committed: CommittedCommand {
            outcome,
            position: Default::default(),
        },
    });
    let response = wait.await.unwrap();
    assert_eq!(response.matched_value, Some(int_value(1)));
    assert!(!restarted.registry.halted().is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn committer_replay_check_failure_halts_the_registry() {
    let harness = RuntimeHarness::new();
    let run_id = started(&harness, "order-1").await;
    *harness.committer.replay_mismatch.lock().unwrap() = Some(ReplayMismatch {
        detail: "debug replay differs".to_owned(),
    });
    harness
        .registry
        .propose(
            flow("order-1"),
            attribute_command("order-1", run_id, "count", 1),
            CommitUrgency::Prompt,
        )
        .await
        .unwrap();
    harness.commit_next();
    assert!(harness.registry.halted().is_cancelled());
}

#[tokio::test(start_paused = true)]
async fn child_start_inbox_messages_get_a_run_id_and_settings() {
    let harness = RuntimeHarness::new();
    let command = FlowCommand {
        flow_id: "child".to_owned(),
        command: Some(flow_command::Command::InboxMessage(InboxMessageCommand {
            message_id: vec![1; 16],
            payload: Some(inbox_message_command::Payload::ChildStart(
                ChildFlowStartMessage::default(),
            )),
            ..InboxMessageCommand::default()
        })),
        ..FlowCommand::default()
    };
    harness
        .registry
        .propose(flow("child"), command, CommitUrgency::Prompt)
        .await
        .unwrap();
    let Some(flow_command::Command::InboxMessage(inbox)) = harness.submitted()[0].command.clone()
    else {
        panic!("expected InboxMessage");
    };
    assert_eq!(inbox.new_run_id.len(), 16);
    assert_eq!(
        inbox.settings.unwrap().retention_after_close_micros,
        7 * 24 * 60 * 60 * 1_000_000
    );
}

#[tokio::test(start_paused = true)]
async fn flow_state_reader_optionally_includes_uncommitted_commands() {
    let harness = RuntimeHarness::new();
    let run_id = started(&harness, "order-1").await;
    let address = flow("order-1");
    harness
        .registry
        .propose(
            address.clone(),
            attribute_command("order-1", run_id, "count", 1),
            CommitUrgency::Batched,
        )
        .await
        .unwrap();
    let key = dexzd_keyspace::run_key(
        harness.registry.flow_context(&address).flow_hash,
        run_id,
        dexzd_keyspace::RunTable::Attribute,
        b"count",
    );
    let speculative = harness.registry.flow_state_reader(&address, true).unwrap();
    let committed = harness.registry.flow_state_reader(&address, false).unwrap();
    assert!(speculative.get(ColumnFamily::Flow, &key).unwrap().is_some());
    assert!(committed.get(ColumnFamily::Flow, &key).unwrap().is_none());
}
