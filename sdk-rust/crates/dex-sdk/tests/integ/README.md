# Rust SDK integration tests

This suite mirrors `sdk-java/src/test/java/io/superdurable/dex/integ` in
idiomatic, strongly typed Rust. Each Java integration test and Workflow has a
corresponding Rust file with the same behavior and assertions. SDK-native coverage adds
Step streaming, heartbeat recovery, and cancellation scenarios.

| Java test | Rust module |
| --- | --- |
| `AnyCommandCombinationTest` | `any_command_combination_test.rs` |
| `BasicTest` | `basic_test.rs` |
| `ConditionalCompleteTest` | `conditional_complete_test.rs` |
| Rust heartbeat recovery | `heartbeat_recovery_test.rs` |
| `InternalChannelTest` | `internal_channel_test.rs` |
| `NoStartStateTest` | `no_start_state_test.rs` |
| `PersistenceTest` | `persistence_test.rs` |
| `ResetTest` | `reset_test.rs` |
| `RpcTest` | `rpc_test.rs` |
| `RpcWithMemoTest` | `rpc_with_memo_test.rs` |
| `SearchFlowsTest` | `search_flows_test.rs` |
| `SignalTest` | `signal_test.rs` |
| `SkipWaitUntilTest` | `skip_wait_until_test.rs` |
| `StateOptionsOverrideTest` | `state_options_override_test.rs` |
| `StateOptionsTest` | `state_options_test.rs` |
| `StateRecoveryTest` | `state_recovery_test.rs` |
| `StreamTest` | `stream_test.rs` |
| `TimerTest` | `timer_test.rs` |
| `WorkflowUncompletedTest` | `workflow_uncompleted_test.rs` |

Run all scenarios against an isolated `dexcli dev` stack from `sdk-rust`:

```bash
./run-integration-tests.sh
DEX_INTEG_WORKER_MODE=pull ./run-integration-tests.sh
```

`DEX_INTEG_WORKER_MODE` defaults to `push`. Each run starts a fresh Dex stack;
the pull setting applies to the shared Worker and Client in both integration
targets. The explicit pull Worker E2E scenarios run in either mode.

To collect production `dex-sdk` coverage from only these integration targets,
install `cargo-llvm-cov` and run `./run-integration-tests.sh --coverage`.

Go's erased `ChannelDef` variant has no Rust counterpart: Rust keeps typed
`Channel<T>` values through registration, so the runtime channel scenario does
not need a second type-erased test. Go's pointer/value Flow cases likewise map
to one Rust ownership model and share one runtime scenario. Go-only behavioral
contracts live separately under `tests/cross_sdk` so they cannot obscure the
one-to-one Java suite.

## Error coverage

| Scenario | `SdkError` variant |
| --- | --- |
| Duplicate start | `FlowAlreadyStarted` |
| Missing describe, attribute read, or Flow wait | `FlowNotFound` |
| Missing query-only RPC target | `FlowNotActiveOrNotFound` |
| Read-only RPC on retained terminal execution | Retained snapshot |
| Missing or closed mutation | `FlowNotActiveOrNotFound` |
| Worker handler failure | `WorkerInvocation` |
| Locking RPC contention | `RpcLockConflict` |
| Long-poll expiry | `LongPollTimeout` |
| Terminal Flow result | `FlowResult` for every terminal status |
| Durable SubFlow condition | Identity, all/any, reuse, reset, and continue-as-new |

Local contract tests also cover malformed rich details, fallible registration,
invalid Step-result worker metadata, user-owned Condition IDs, and map
introspection. Persistence integration covers singleton Attribute equality waits.

Heartbeat recovery verifies typed checkpoints, explicit clearing, null versus an absent Value,
Stream implicit-heartbeat preservation, and the absence of local-activity details after regular
fallback. Stream integration interleaves WaitFor and Execute heartbeat frames with repeated Step
writes, exercises default and custom buffered text writers, then verifies repeated client sources
containing `#` all append. Cancellation coverage uses active heartbeat output so blocked
synchronous handlers observe response-stream closure promptly.
