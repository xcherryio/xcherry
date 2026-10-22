# TypeScript data handling

For application indexes, keep business Attribute names and supply explicit generic typed keys such as `keyword1`, `keywordList1`, `text1`, or `int1`; reuse the same typed slots across Flow types and constrain application run searches by FlowType. Parenthesize additional caller filters when composing the query. See [core primitives](../core/primitives.md#attribute). Pinned excerpts preserve their baseline names rather than defining the naming policy.

Register a non-indexed singleton Attribute with `booleanCodec` alongside the map and use `currentMessagesLock.lock()` in the applicable StepOptions or registration-time RPCOptions locks. Follow [whole-map coordination](../core/data-handling.md#whole-map-coordination): every protected writer uses the same singleton lock, its bool value is not an acquisition flag, and map loads remain explicit.

## Codecs are runtime contracts

TypeScript types disappear at runtime. Use scalar codecs for scalar wire values and a deliberate `jsonCodec<T>` for objects. Add decode validation for untrusted or evolving data. Omitted codecs use JSON but do not validate object structure.

Stable Flow, Step, Attribute, Channel, Stream, RPC, and codec type names are part of open-Flow compatibility. Avoid renaming them as a cosmetic refactor.

## State shapes

Use Attributes for authoritative current state, AttributeMaps for independently loaded records, Channels for durable FIFO commands, ChannelMaps for per-key queues, and Streams for best-effort progress. Register each definition in one Flow schema.

[Pinned typed state source](https://github.com/superdurable/dex/blob/sdk-typescript/v1.12.0/examples/typescript/src/primitives/attribute/attribute-flow.ts)
<!-- dex-source: examples/typescript/src/primitives/attribute/attribute-flow.ts -->
```typescript
const status = new Attribute("primitive-attribute-status", stringCodec, {
  type: IndexType.KEYWORD,
  indexKey: "keyword1",
});
const email = new Attribute("primitive-attribute-email", stringCodec).syncToAttributeStore();
const modelApiKey = new Attribute(
  "primitive-attribute-model-api-key",
  stringCodec,
).hardDeleteOnNull();
const progress = new AttributeMap("primitive-attribute-progress", stringCodec, {
  type: IndexType.KEYWORD,
  indexKey: "keyword2",
});
```

Map instance keys must be stable, non-empty, and slash-free. Load only the instances/pending messages a handler reads. The effective map view includes mutations staged in the current handler, but RPC pending-message snapshots do not refresh after later staged writes.

For append-only history, keep at most 100 records in `current`, archive a full chunk under its zero-padded first sequence, and load one token-selected instance per page. For stable-key lookup, keep a dictionary keyed by the complete canonical value inside one fixed hash bucket. The official email pattern trims ASCII whitespace, lowercases ASCII letters, rejects empty or non-ASCII values, computes wrapping FNV-1a 32-bit, and selects `hash % 1000`. See [sequential chunking](https://github.com/superdurable/dex/blob/sdk-typescript/v1.12.0/examples/typescript/src/patterns/sequentially-chunked-attribute-map/chunked-subscriber-flow.ts) and [hash partitioning](https://github.com/superdurable/dex/blob/sdk-typescript/v1.12.0/examples/typescript/src/patterns/hash-partitioned-attribute-map/customer-directory-flow.ts).

## Commit boundary

Attribute changes and Channel publishes/deletes are staged and commit with the successful handler result. Ordinary module state, filesystem writes, and remote API calls are not durable mutations. Derive external idempotency keys from Flow ID plus a persisted business sequence.

## BlobCache and large data

Share one process BlobCache between Client and Worker. Large payload support does not make a growing JSON aggregate efficient. Partition mutable data into AttributeMap instances, put queued work on Channels, and emit transient progress on Streams.

The Server keeps payloads through 100 bytes inline by default. Treat internal blob references as opaque: hydration and cache lookup use the owning Flow ID with the reference, and Dex rewrites blob-backed values that cross into another Flow. String and Object references share a compact Base36-date shape such as `p1|c/ab3de7kp2x`; their Value arms distinguish them. Object Blobs store the complete EncodedObject with `json`, `raw`, or a custom encoding, so references have no encoding suffix. The Server's `objectIdLength` defaults to 10, accepts any positive length, and treats zero as the default; readers accept any nonempty lowercase Base36 object ID.

ASYNC local Step input snapshots are disabled by default. Enable the Server's `blobStore.asyncStepInputSnapshotsEnabled` only when semantic history needs exact method inputs; it does not affect execution, retry, or recovery.

TypeScript `null` and `undefined` use the Value null arm. The default JSON codec decodes it as `null`; `voidCodec` and `optionalCodec` decode it as `undefined`. In an Attribute write it deletes the Attribute, permanently for a hard-delete Attribute, and as a Flow completion output it is discarded. Return an explicit result object when terminal null and no output must differ.

Attribute Store is an asynchronous latest-state projection. Select Store names in `FlowConfig`; do not wait on projection as a correctness barrier. For a store that sets `rowKey`, `columnNaming`, or `attributeMaps` (Dex Server v1.6.0 or later), follow the Core [row keys, column names, and AttributeMap tables](../core/data-handling.md#row-keys-column-names-and-attributemap-tables) guidance.

- Derive each Flow ID in exactly the format the store's `rowKey` template expects. A Flow ID that does not match skips its batch.
- A `jsonCodec` AttributeMap instance is written with `JSON.stringify`, or from the result of the codec's `encode`, so the object keys are the top-level fields that become the columns of an AttributeMap table. Under `columnNaming: snake_case` a `deploymentUrl` key writes the `deployment_url` column. A scalar-codec value such as `stringCodec` or `booleanCodec` output, or a JSON array, is written to the `value` column.
- The instance key is stored in `instanceKeyColumn` as written. Deleting the instance deletes its row in an AttributeMap table, while deleting a plain opted-in Attribute writes SQL `NULL`.

## Hard-delete Attributes

Call `hardDeleteOnNull()` on an `Attribute` or `AttributeMap` for a short-lived secret, such as an API key. It returns a new definition and leaves the original unchanged. Register the returned definition in `PersistenceSchema`. Read the Core [hard-delete Attribute](../core/data-handling.md#hard-delete-attributes-for-short-lived-secrets) requirements and limitations first; it needs TypeScript SDK v1.11.0 and Server v1.11.0.

[Pinned SDK guide](https://github.com/superdurable/dex/blob/sdk-typescript/v1.12.0/sdk-typescript/README.md)
<!-- dex-source: sdk-typescript/README.md -->
```typescript
const apiKey = new Attribute("api-key", stringCodec).hardDeleteOnNull();
const vendorTokens = new AttributeMap("vendor-tokens", jsonCodec<VendorToken>())
  .hardDeleteOnNull();
```

Delete the secret as soon as the Flow no longer needs it. `delete`, or `set` with `null` or `undefined`, permanently deletes it. An AttributeMap deletes one instance:

[Pinned SDK integration test](https://github.com/superdurable/dex/blob/sdk-typescript/v1.12.0/sdk-typescript/test/integ/hard_delete_attribute_flow.ts)
<!-- dex-source: sdk-typescript/test/integ/hard_delete_attribute_flow.ts -->
```typescript
    apiKey.delete(context);
    vendorTokens.delete(context, vendorInstance);
```

- A deleted value reads as absent, so `get` returns `undefined`. Handle it by requesting a fresh secret.
- Pass the secret at start with `InitialAttribute.of(apiKey, value)` or `InitialAttribute.mapValue(vendorTokens, instance, value)` in the `attributes` start option. Use the definition registered with the target Flow; `startFlow` throws `TypeError` otherwise.
- `hardDeleteOnNull()` throws `TypeError` for an indexed definition, Attribute Store sync, or a built-in numeric or Boolean codec. A custom codec that encodes a number or Boolean fails when the value is written.
- `Client.waitForAttributeMatch` rejects it before contacting Dex.
- The Client checks the Server protocol once, before its first hard-delete write, and `startFlow` throws for a Server below protocol 3. The Worker fails a handler result that writes a hard-delete Attribute to an older Server.
- Writes and deletes can become visible before the attempt commits. Keep handlers safe to repeat. Do not read-modify-write the value. See the [released API documentation](https://github.com/superdurable/dex/blob/sdk-typescript/v1.12.0/sdk-typescript/src/persistence.ts) for every limitation.
