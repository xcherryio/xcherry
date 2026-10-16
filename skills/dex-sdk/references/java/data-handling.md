# Java data handling

For application indexes, keep business Attribute names and supply explicit generic typed keys such as `keyword1`, `keywordList1`, `text1`, or `int1`; reuse the same typed slots across Flow types and constrain application run searches by FlowType. Parenthesize additional caller filters when composing the query. See [core primitives](../core/primitives.md#attribute). Pinned excerpts preserve their baseline names rather than defining the naming policy.

Register a non-indexed singleton Boolean Attribute alongside the map and use `AttributeLock.of(currentMessagesLock)` in the applicable StepOptions or RPC registration locks. Follow [whole-map coordination](../core/data-handling.md#whole-map-coordination): every protected writer uses the same singleton lock, its bool value is not an acquisition flag, and map loads remain explicit.

## Types and serialization

`Step<I>` exposes `Class<I> getInputType()`. Use concrete DTO classes, records supported by the configured mapper, scalar wrapper classes, or arrays. Do not use `List<Foo>.class`; wrap parameterized structures in a named input type. Keep wire field names and meanings compatible with open Flows.

Define each Attribute, AttributeMap, Channel, ChannelMap, and Stream once per Flow type. Register every definition in `PersistenceSchema`. Names are durable identifiers, not refactoring-only symbols.

## Attributes and Channels

Use Attributes for current authoritative state and Channels for ordered pending work. AttributeMap and ChannelMap isolate values by stable instance key. Avoid loading an entire map when one instance is enough. RPC and Step load options control what reaches the Worker; reads outside the selected scope fail rather than silently fetching.

For append-only history, keep at most 100 records in `current`, archive a full chunk under its zero-padded first sequence, and load one page by token. Archived chunks remain immutable. For stable-key lookup, keep a dictionary keyed by the complete canonical value inside a fixed hash bucket. The official email pattern trims ASCII whitespace, lowercases ASCII letters, rejects empty or non-ASCII input, computes wrapping FNV-1a 32-bit, and selects `hash % 1000`. See [sequential chunking](https://github.com/superdurable/dex/blob/sdk-go/v1.11.0/examples/java/src/main/java/io/superdurable/dex/patterns/sequentiallychunkedattributemap/ChunkedSubscriberFlow.java) and [hash partitioning](https://github.com/superdurable/dex/blob/sdk-go/v1.11.0/examples/java/src/main/java/io/superdurable/dex/patterns/hashpartitionedattributemap/CustomerDirectoryFlow.java).

Locks provide cooperation among handlers using the same lock. Mark an RPC transactional when Channel deletion must abort the whole RPC on a missing message. A staged publish or Attribute write commits only with the successful handler result.

## Large values and BlobCache

Share one disk `BlobCache` between Worker and Client and size it for active payload locality. Blob storage makes large values feasible; it does not make repeatedly rewriting a growing aggregate cheap. Prefer an AttributeMap for independently updated records, Channels for durable queues, and Streams for best-effort deltas.

The Server keeps payloads through 100 bytes inline by default. Treat internal blob references as opaque: hydration and cache lookup use the owning Flow ID with the reference, and Dex rewrites blob-backed values that cross into another Flow. String and Object references share a compact Base36-date shape such as `p1|c/ab3de7kp2x`; their Value arms distinguish them. Object Blobs store the complete EncodedObject with `json`, `raw`, or a custom encoding, so references have no encoding suffix. The Server's `objectIdLength` defaults to 10, accepts any positive length, and treats zero as the default; readers accept any nonempty lowercase Base36 object ID.

ASYNC local Step input snapshots are disabled by default. Enable the Server's `blobStore.asyncStepInputSnapshotsEnabled` only when semantic history needs exact method inputs; it does not affect execution, retry, or recovery.

Java `null` uses the Value null arm and decodes back to `null`. In an Attribute write it deletes the Attribute, permanently for a hard-delete Attribute, and as a Flow completion output it is discarded. Return an explicit result DTO when terminal null and no output must differ.

[Pinned cache construction](https://github.com/superdurable/dex/blob/sdk-go/v1.11.0/examples/java/src/main/java/io/superdurable/dex/config/DexConfig.java)
<!-- dex-source: examples/java/src/main/java/io/superdurable/dex/config/DexConfig.java -->
```java
        return BlobCache.open(new BlobCacheConfig(blobCacheDir, 1L << 30));
```

## Hard-delete Attributes

Call `hardDeleteOnNull()` on an `Attribute` or `AttributeMap` definition for a short-lived secret, such as an API key. It returns a new immutable definition. Register that definition in `PersistenceSchema`. Read the Core [hard-delete Attribute](../core/data-handling.md#hard-delete-attributes-for-short-lived-secrets) requirements and limitations first; it needs Java SDK v1.11.0 and Server v1.11.0.

[Pinned SDK guide](https://github.com/superdurable/dex/blob/sdk-go/v1.11.0/sdk-java/README.md)
<!-- dex-source: sdk-java/README.md -->
```java
Attribute<String> apiKey = Attribute.define("api-key", String.class)
        .hardDeleteOnNull();
```

Delete the secret as soon as the Flow no longer needs it. `delete`, or `set` with `null`, permanently deletes it. An AttributeMap deletes one instance:

[Pinned SDK integration test](https://github.com/superdurable/dex/blob/sdk-go/v1.11.0/sdk-java/src/test/java/io/superdurable/dex/integ/HardDeleteAttributeWorkflow.java)
<!-- dex-source: sdk-java/src/test/java/io/superdurable/dex/integ/HardDeleteAttributeWorkflow.java -->
```java
            apiKey.delete(context);
            tenantCredentials.delete(context, "tenant-a");
            tenantCredentials.set(context, "tenant-b", null);
```

- A deleted value reads as absent, so `get` returns `null`. Handle it by requesting a fresh secret.
- Pass the secret at start with `StartFlowOptions.newBuilder().addAttribute(flow.apiKey, value)`, or `addAttribute(map, instanceKey, value)` for a map instance, using the definition registered with the target Flow. `startFlow` throws `IllegalArgumentException` when an initial Attribute's hard-delete setting differs from that definition.
- `hardDeleteOnNull()` throws `IllegalStateException` for an indexed definition, `syncToAttributeStore()`, or a numeric or Boolean value type such as `Integer`, `Long`, `Double`, or `Boolean`. With a broader type such as `Object`, `set` throws `IllegalArgumentException` for a numeric or Boolean value.
- `Client.waitForAttributeMatch` rejects it before contacting the Server. Use an RPC that reads the value.
- A Step, RPC, `startFlow`, or SubFlow start that writes it fails with `IllegalStateException` when the negotiated Server protocol is below 3.
- When concurrent writers must keep order, use a single writer, or make every Step and RPC that writes the key hold an `AttributeLock` on it. A failed locked attempt can still overwrite the value.
- Writes and deletes can become visible before the attempt commits, and `Client.timeTravel` does not restore a deleted value. Keep handlers safe to repeat. See the [released API documentation](https://github.com/superdurable/dex/blob/sdk-go/v1.11.0/sdk-java/src/main/java/io/superdurable/dex/Attribute.java) and [AttributeMap documentation](https://github.com/superdurable/dex/blob/sdk-go/v1.11.0/sdk-java/src/main/java/io/superdurable/dex/AttributeMap.java) for every limitation.

## Attribute Store

Call `syncToAttributeStore()` on definitions that need an external latest-state projection and select Store names in `FlowConfig`. Projection is asynchronous and does not replace Flow state. Deletion projects null. Never make correctness depend on projection timing. For a store that sets `rowKey`, `columnNaming`, or `attributeMaps` (Dex Server v1.6.0 or later), follow the Core [row keys, column names, and AttributeMap tables](../core/data-handling.md#row-keys-column-names-and-attributemap-tables) guidance.

- Derive each Flow ID in exactly the format the store's `rowKey` template expects. A Flow ID that does not match skips its batch.
- A DTO or record AttributeMap instance is serialized by the configured Jackson `ObjectMapper`, so its property names, including `@JsonProperty` renames and any naming strategy, are the top-level fields that become the columns of an AttributeMap table. Under `columnNaming: snake_case` a `deploymentUrl` property writes the `deployment_url` column. A scalar wrapper, `List`, or array value is written to the `value` column; a `Map` serializes as a JSON object.
- The instance key is stored in `instanceKeyColumn` as written. Deleting the instance deletes its row in an AttributeMap table, while deleting a plain opted-in Attribute projects null.

## Schema review

- Is each definition name stable and unique within the Flow?
- Is every map instance key non-empty, slash-free, and derived from a business identity?
- Does each handler load only the collections it reads?
- Are large mutable aggregates split by locality?
- Are external effects idempotent with keys derived from durable Flow state?
