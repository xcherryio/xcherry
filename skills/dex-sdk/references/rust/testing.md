# Testing Rust Dex applications

Prefer integration tests against a real Dex Server. Unit tests can validate pure helpers, but they cannot prove WaitFor/Execute commit boundaries, retries, Worker replacement, Channel delivery, RPC locking, durable timers, or terminal behavior.

## Test harness

Start `dexcli dev`, build the same Registry used by production, create a temporary BlobCache, bind the Worker to a free address, and run it on a dedicated thread. Construct a Client with the same Worker target. Stop and join the Worker and close the cache during teardown.

[Integration source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/rust/tests/dex_integration.rs)
<!-- dex-source: examples/rust/tests/dex_integration.rs -->
```rust
let client = Client::try_new(
    registry,
    Arc::clone(&cache),
    ClientOptions::new()
        .server_address(server_address)
        .worker_target(worker.worker_target().clone()),
)
.expect("create Rust examples Client");
client.health_check().expect("Dex health check");
```

Use `tempfile::TempDir` for BlobCache isolation and ask the OS for a free Worker port. Do not share a fixed Flow ID across tests.

## Deadline polling

Poll observable Dex state until a short deadline. Do not use a fixed sleep as the assertion mechanism; scheduling and retries are asynchronous.

[Integration source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/rust/tests/dex_integration.rs)
<!-- dex-source: examples/rust/tests/dex_integration.rs -->
```rust
fn await_engagement_status(&self, flow_id: &str, expected: &str) -> EngagementStatus {
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        let status = self
            .client
            .invoke_rpc_without_input(flow_id, DESCRIBE_ENGAGEMENT)
            .expect("describe Rust Engagement Flow");
        if status.status == expected {
            return status;
        }
        thread::yield_now();
    }
    panic!("Rust Engagement Flow did not reach {expected}");
}
```

Poll via RPC or Client state that represents the business invariant. Include the last observed value in new failure messages where possible.

## Required scenarios

For a nontrivial Flow, cover:

1. Happy path from `start_flow` to the expected terminal output.
2. WaitFor that remains waiting until Channel, RPC, Timer, or SubFlow input arrives.
3. Execute retry exhaustion and the modeled compensation, manual recovery, or terminal failure.
4. WaitFor retry exhaustion, including `WaitForFailurePolicy::Proceed` when used.
5. Channel publication, message identity, deletion, and idempotent retry behavior.
6. RPC success plus typed `SdkError` for invalid state, lock conflict, or missing data.
7. Timer behavior using a bounded deadline, not wall-clock exactness.
8. Worker replacement: stop the Worker while the Flow is nonterminal, start a replacement with the same Registry, then prove progress continues.
9. Cancellation observed by long-running Execute work and any cleanup path.
10. Terminal behavior: the Flow reaches the intended status and no sibling or background Step keeps mutating durable state.

## Client and persistence assertions

Read Flow-owned state through typed application RPCs. The baseline test for Channel moves asserts source ordering, deletion by `message_id`, destination publication, and the typed missing-message error. Use the same style for Attribute, ChannelMap, RPC, and Stream behavior. Avoid removed direct Client state methods and Dex Server storage.

## Registry and compile contracts

Validate every Flow in one Registry and reject duplicate names. `examples/rust/tests/catalog_integration.rs` checks the entire example catalog. `sdk-rust/crates/dex-sdk/tests/compile_contracts.rs` is useful evidence for public API shapes that are intentionally compile-only.

Run the repository's commands:

```bash
cd examples/rust
make fmt-check
make clippy
make test
./run-integration-tests.sh
```

The integration script expects a working `dexcli` and exercises the published Rust SDK. For an application repo, provide equivalent setup explicitly in CI rather than silently skipping when Dex is absent.
