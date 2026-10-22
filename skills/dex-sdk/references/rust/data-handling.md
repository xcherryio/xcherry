# Rust data handling

For application indexes, keep business Attribute names and supply explicit generic typed keys such as `keyword1`, `keywordList1`, `text1`, or `int1`; reuse the same typed slots across Flow types and constrain application run searches by FlowType. Parenthesize additional caller filters when composing the query. See [core primitives](../core/primitives.md#attribute). Pinned excerpts preserve their baseline names rather than defining the naming policy.

Register a non-indexed singleton `Attribute<bool>` alongside the map and use its `.lock()` in the applicable StepOptions or RPC registration locks. Follow [whole-map coordination](../core/data-handling.md#whole-map-coordination): every protected writer uses the same singleton lock, its bool value is not an acquisition flag, and map loads remain explicit.

Dex values cross process boundaries. Model them as owned, serde-compatible types with stable field meaning. Keep ephemeral handles, clients, sockets, and service objects out of Flow inputs, Attribute values, Channel messages, RPC payloads, and Step movements.

## Types and ownership

Use `#[derive(Clone, Debug, Deserialize, Serialize)]` for application payloads that traverse Dex. Prefer owned `String`, `Vec<T>`, and owned nested structs at durable boundaries. Borrow within one handler where useful, but return owned values in decisions and RPC results. Treat schema names and serialized fields as compatibility contracts for open Flows.

Use module-level `static LazyLock<T>` for `Attribute`, `Channel`, and `Stream` definitions. An `AttributeMap` or `ChannelMap` may be an owned Flow field when the Flow must clone it into multiple Step values; clone the schema object, not live durable data.

## Persistence schema

Register every definition used by a Flow. Registration catches a load for an undeclared definition. Keep logical names stable and unique within the Flow. Use maps when the set of keys grows dynamically; do not create dynamic logical definition names.

[Runnable source](https://github.com/superdurable/dex/blob/sdk-rust/v1.15.0/examples/rust/src/primitives/attribute/flow.rs)
<!-- dex-source: examples/rust/src/primitives/attribute/flow.rs -->
```rust
fn persistence(&self) -> PersistenceSchema {
    PersistenceSchema::new()
        .attribute(&STATUS)
        .attribute(&EMAIL)
        .attribute(&MODEL_API_KEY)
        .attribute_map(&self.progress)
}
```

## Selective loading

Map and Channel state is not implicitly hydrated for every invocation. Declare phase-specific loads on `StepOptions`, or attach loads to an RPC definition. Load a full map only when the algorithm truly needs all instances; prefer `attribute_map.load(instance)` or `channel_map.load_messages(instance)` for one partition.

[Runnable source](https://github.com/superdurable/dex/blob/sdk-rust/v1.15.0/examples/rust/src/primitives/channel/flow.rs)
<!-- dex-source: examples/rust/src/primitives/channel/flow.rs -->
```rust
fn options(&self) -> StepOptions<Self::Input> {
    StepOptions::new().execute_load_channel(&QUEUED_MESSAGES)
}
```

Loading controls availability in Worker Context; it does not request serialization between competing writers.

For append-only history, bound a `current` collection, archive each full chunk under its zero-padded first sequence, and load one page-token instance rather than the whole map. For keyed records, keep a map keyed by the complete canonical value inside one stable hash bucket. The official email pattern trims ASCII whitespace, lowercases ASCII letters, rejects empty or non-ASCII values, computes wrapping FNV-1a 32-bit, and selects `hash % 1000`. Keep the partition count and canonicalization stable. See [sequential chunking](https://github.com/superdurable/dex/blob/sdk-rust/v1.15.0/examples/rust/src/patterns/sequentially_chunked_attribute_map/flow.rs) and [hash partitioning](https://github.com/superdurable/dex/blob/sdk-rust/v1.15.0/examples/rust/src/patterns/hash_partitioned_attribute_map/flow.rs).

## Locks and transactions

Use `Attribute::lock()` or `AttributeMap::lock(instance)` for a read-modify-write invariant. Locks select transactional RPC execution. If an RPC deletes and republishes Channel messages without an Attribute lock, request `Rpc::is_transactional()` explicitly. Attach every state load the transaction reads.

Do not hold application-process mutexes as a substitute. A Worker can be replaced, and multiple Workers can execute the same Flow type.

## Commit boundaries

WaitFor and Execute are separate retryable method executions. Durable writes made in one phase are committed according to that phase's result; do not rely on Rust stack state crossing the boundary. RPC handlers have their own atomicity rules. Model checkpoints in Attributes or Channels before external side effects when recovery needs to distinguish “not attempted” from “possibly accepted.”

## Large values and BlobCache

Client and Worker share an `Arc<BlobCache>` so payload hydration and upload can use local content-addressed storage. Choose a process-local directory, byte capacity, and entry capacity; close the cache during shutdown. The example bootstrap is the source of truth for constructor shape.

[Runnable source](https://github.com/superdurable/dex/blob/sdk-rust/v1.15.0/examples/rust/src/main.rs)
<!-- dex-source: examples/rust/src/main.rs -->
```rust
let cache = Arc::new(BlobCache::open(BlobCacheConfig::new(
    cache_directory,
    64 * 1024 * 1024,
    10_000,
)?)?);
```

Do not use BlobCache as business persistence. Durable references remain owned by Dex; local cache entries are replaceable acceleration.

The Server keeps payloads through 100 bytes inline by default. Treat internal blob references as opaque: hydration and cache lookup use the owning Flow ID with the reference, and Dex rewrites blob-backed values that cross into another Flow. String and Object references share a compact Base36-date shape such as `p1|c/ab3de7kp2x`; their Value arms distinguish them. Object Blobs store the complete EncodedObject with `json`, `raw`, or a custom encoding, so references have no encoding suffix. The Server's `objectIdLength` defaults to 10, accepts any positive length, and treats zero as the default; readers accept any nonempty lowercase Base36 object ID.

ASYNC local Step input snapshots are disabled by default. Enable the Server's `blobStore.asyncStepInputSnapshotsEnabled` only when semantic history needs exact method inputs; it does not affect execution, retry, or recovery.

Rust JSON null uses the Value null arm and decodes as `serde_json::Value::Null`. In an Attribute write null deletes the Attribute, permanently for a hard-delete Attribute, and as a Flow completion output it is discarded. Return an explicit result type when terminal null and no output must differ.

## Hard-delete Attributes

Call `.hard_delete_on_null()` on an `Attribute` or `AttributeMap` for a short-lived secret, such as an API key. Define it as a module-level `static LazyLock` and register it in `persistence()` as usual. Read the Core [hard-delete Attribute](../core/data-handling.md#hard-delete-attributes-for-short-lived-secrets) requirements and limitations first; it needs Rust SDK v1.11.0 and Server v1.11.0.

[Pinned SDK integration test](https://github.com/superdurable/dex/blob/sdk-rust/v1.15.0/sdk-rust/crates/dex-sdk/tests/integ/hard_delete_attribute_workflow.rs)
<!-- dex-source: sdk-rust/crates/dex-sdk/tests/integ/hard_delete_attribute_workflow.rs -->
```rust
pub(crate) static API_KEY: LazyLock<Attribute<String>> =
    LazyLock::new(|| Attribute::new("api-key").hard_delete_on_null());
pub(crate) static TENANT_TOKENS: LazyLock<AttributeMap<TenantToken>> =
    LazyLock::new(|| AttributeMap::new("tenant-tokens").hard_delete_on_null());
```

Delete the secret as soon as the Flow no longer needs it. `delete`, or a null write, permanently deletes it. An AttributeMap deletes one instance:

[Pinned SDK integration test](https://github.com/superdurable/dex/blob/sdk-rust/v1.15.0/sdk-rust/crates/dex-sdk/tests/integ/hard_delete_attribute_workflow.rs)
<!-- dex-source: sdk-rust/crates/dex-sdk/tests/integ/hard_delete_attribute_workflow.rs -->
```rust
        API_KEY.delete(context)?;
        TENANT_TOKENS.delete(context, TENANT)?;
```

- A deleted value reads as absent, so `get` returns `Ok(None)` and `get_required` returns a `HandlerError`. Handle absence by requesting a fresh secret.
- Pass the secret at start with `StartFlowOptions::new().initial_attribute(&API_KEY, value)` or `.initial_attribute_map(&TENANT_TOKENS, instance, value)`. Every write must use a definition whose hard-delete setting matches the one the Flow registered.
- `Registry::register` returns `SdkError::FlowDefinition` for an integer, floating-point, or `bool` value type, an `Option` of one, an index, or `sync_to_attribute_store`. A value of another type that encodes to a number or Boolean fails when written. Wrap a numeric secret in a serde struct so it encodes as an object.
- `Client::wait_for_attribute_match` rejects it with `SdkError::InvalidArgument` before contacting Dex. Action and Action permission conditions cannot read it.
- The Worker learns the Server protocol at startup. The Client requests it once, before its first hard-delete write. Writes fail against a Server older than protocol 3.
- Writes and deletes can become visible before the attempt commits. Keep handlers safe to repeat. Do not read-modify-write the value. See the [released API documentation](https://github.com/superdurable/dex/blob/sdk-rust/v1.15.0/sdk-rust/crates/dex-sdk/src/attribute.rs) for every limitation.

## Attribute Store and search

Mark selected Attributes with `sync_to_attribute_store` only when an external projection needs them. Configure store names in `FlowConfig`. Index only fields needed for search, and keep index keys stable. Search is for discovery; an RPC or Attribute read should confirm current authoritative Flow state before mutation.

For a store that sets `rowKey`, `columnNaming`, or `attributeMaps` (Dex Server v1.6.0 or later), follow the Core [row keys, column names, and AttributeMap tables](../core/data-handling.md#row-keys-column-names-and-attributemap-tables) guidance.

- Derive each Flow ID in exactly the format the store's `rowKey` template expects. A Flow ID that does not match skips its batch.
- A struct AttributeMap instance is serialized with serde, so its serialized field names, including `#[serde(rename = "...")]` and `rename_all`, are the top-level fields that become the columns of an AttributeMap table. Rust field names are snake_case already, so `columnNaming: snake_case` matters when `rename_all = "camelCase"` or `"PascalCase"` applies: `deploymentUrl` writes the `deployment_url` column. A `String`, number, `bool`, or `Vec` value is written to the `value` column.
- The instance key is stored in `instanceKeyColumn` as written. Deleting the instance deletes its row in an AttributeMap table, while deleting a plain opted-in Attribute writes SQL `NULL`.
