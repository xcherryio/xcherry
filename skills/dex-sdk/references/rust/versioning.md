# Rust versioning and compatibility

Start by inspecting the application's `Cargo.toml` and `Cargo.lock`. The pinned Dex baseline examples use a published exact crate version, but the installed version is authoritative for an existing application.

[Pinned manifest](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/rust/Cargo.toml)
<!-- dex-source: examples/rust/Cargo.toml -->
```toml
dex-sdk = "=1.2.1"
```

## Server protocol compatibility

Rust Workers compile their diagnostic version from Cargo package metadata. Startup calls `GetServerInfo`, negotiates the highest common protocol, synchronizes Attribute indexes, and then binds WorkerService. A missing RPC, invalid interval, or disjoint interval fails before binding. Upgrade a legacy Server first, and stop running Workers before a breaking Server release because they do not renegotiate.

## Evidence order

1. Application lockfile and dependency declaration.
2. Public APIs and tests in the matching SDK tag or crate documentation.
3. This skill's pinned `DEX_BASELINE` examples.
4. Current Dex documentation for product semantics.

Do not copy an API from the baseline if the application resolves another crate version. Do not “upgrade” syntax while answering an unrelated modeling question.

## Open Flow compatibility

Open Flows can outlive deployments. Before changing a registered Flow, compare:

- Flow type and Step type names;
- persistent Attribute, AttributeMap, Channel, ChannelMap, and Stream names;
- serde field names and representations for inputs, outputs, and stored values;
- Step input types and graph reachability;
- RPC names, input/output types, locks, loads, and transaction behavior;
- timeout handler presence and policy;
- retry, timeout, and cancellation semantics.

Renaming a Rust struct does not necessarily rename the logical type if the SDK provides an explicit name override, but never assume that. Verify the resolved registry definition. Removing a Step that an open Flow can still schedule is unsafe. Adding a field requires a serde-compatible default or an explicit migration strategy.

## Deployment sequence

Register old and new reachable definitions during a rolling change when open executions may reference both. Deploy Workers that can read existing durable payloads before controllers begin producing a new representation. Test with a Flow started by the old version, replace the Worker with the new version, and drive it to completion.

Use the [Dex Application Operations versioning guidance](https://docs.superdurable.io/production/application-operations#versioning-flow-code) for the service-level procedure.

## Updating the baseline

When maintaining this skill, update `DEX_BASELINE`, refresh pinned source links and exact excerpts together, run source-fidelity validation against that release tag, and bump the skill version according to SemVer. Do not point exact API links at floating `main`.
