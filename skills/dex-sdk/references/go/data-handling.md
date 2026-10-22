# Go data handling

For application indexes, keep business Attribute names and supply explicit generic typed keys such as `keyword1`, `keywordList1`, `text1`, or `int1`; reuse the same typed slots across Flow types and constrain application run searches by FlowType. Parenthesize additional caller filters when composing the query. See [core primitives](../core/primitives.md#attribute). Pinned excerpts preserve their baseline names rather than defining the naming policy.

Use `dex.LockAttribute(CurrentMessagesLock)` in `ExecuteLockAttributes` (or the applicable method lock option) and registration-time `RPCOptions.LockAttributes`. Define the non-indexed singleton `currentMessagesLock` with `dex.DefineAttribute` and type `bool`, then register it alongside the map. Follow [whole-map coordination](../core/data-handling.md#whole-map-coordination): every protected writer uses the same singleton lock, its bool value is not an acquisition flag, and map loads remain explicit.

Step input/output describes transitions; Attributes hold current state; AttributeMaps partition it; Channels hold queued intent; Streams hold incremental output; BlobCache carries large serialized values. Choose one authority for each fact.

## Schema and commit boundary

Definitions are stable package-level values registered in `PersistenceSchema`. Concrete generic types determine codecs. Treat exported fields/types as contracts for open Flows.

Writes through `dex.Context` are buffered in an invocation and commit with its successful result. Do not report success externally before return. On error, expect retry from the prior durable boundary.

## Maps and selective loading

Map instance names must be non-empty and contain no `/`. Request exact instances for entity handlers and whole maps only for scans. An unrequested instance raises a typed not-loaded error; that differs from an absent key.

For append-only history, keep a bounded `current` instance and archive each full chunk under its zero-padded first sequence. Page by loading one chunk with `RPCInvokeOptions`; do not enumerate the whole map. Archived chunks stay immutable, while appenders lock and rewrite only `current`.

For keyed records, derive a stable partition name from the lookup key and keep a map keyed by the full canonical value inside the bucket. The official email pattern trims ASCII whitespace, lowercases ASCII letters, rejects empty or non-ASCII values, computes wrapping 32-bit FNV-1a, and selects `hash % 1000`. Keep those rules and the partition count stable across deployments. See the pinned [sequential pattern](https://github.com/superdurable/dex/blob/sdk-go/v1.14.1/examples/go/patterns/sequentially-chunked-attribute-map/workflow.go) and [hash pattern](https://github.com/superdurable/dex/blob/sdk-go/v1.14.1/examples/go/patterns/hash-partitioned-attribute-map/workflow.go).

## Streams and large values

Use Stream for replayable progress and Attribute for latest authoritative state. Persist resume tokens. Buffered text reduces write amplification but adds a flush boundary; flush final chunks.

Client and Worker share BlobCache for values beyond inline limits. Size it for the working set and use suitable local storage. Do not put large opaque payloads in indexed Attributes. See pinned [bootstrap](https://github.com/superdurable/dex/blob/sdk-go/v1.14.1/examples/go/cmd/server/dex/dex.go) and [SDK guide](https://github.com/superdurable/dex/blob/sdk-go/v1.14.1/sdk-go/README.md).

The Server keeps payloads through 100 bytes inline by default. Treat internal blob references as opaque: hydration and cache lookup use the owning Flow ID with the reference, and Dex rewrites blob-backed values that cross into another Flow. String and Object references share a compact Base36-date shape such as `p1|c/ab3de7kp2x`; their Value arms distinguish them. Object Blobs store the complete EncodedObject with `json`, `raw`, or a custom encoding, so references have no encoding suffix. The Server's `objectIdLength` defaults to 10, accepts any positive length, and treats zero as the default; readers accept any nonempty lowercase Base36 object ID.

ASYNC local Step input snapshots are disabled by default. Enable the Server's `blobStore.asyncStepInputSnapshotsEnabled` only when semantic history needs exact method inputs; it does not affect execution, retry, or recovery.

Go nil values use the Value null arm and decode through the requested Go type. In an Attribute write null deletes the Attribute, permanently for a hard-delete Attribute, and as a Flow completion output it is discarded. Return an explicit result struct when terminal null and no output must differ.

## Hard-delete Attributes

Pass `dex.HardDeleteOnNull()` to `dex.DefineAttribute` or `dex.DefineAttributeMap` for a short-lived secret, such as an API key. Register the definition in `PersistenceSchema` as usual. Read the Core [hard-delete Attribute](../core/data-handling.md#hard-delete-attributes-for-short-lived-secrets) requirements and limitations first; it needs Go SDK v1.11.0 and Server v1.11.0.

[Pinned SDK guide](https://github.com/superdurable/dex/blob/sdk-go/v1.14.1/sdk-go/README.md)
<!-- dex-source: sdk-go/README.md -->
```go
var ModelAPIKey = dex.DefineAttribute[string](
	"model-api-key",
	dex.HardDeleteOnNull(),
)
```

Delete the secret as soon as the Flow no longer needs it. `Delete`, or `Set` with a nil value, permanently deletes it. An AttributeMap deletes one instance:

[Pinned SDK integration test](https://github.com/superdurable/dex/blob/sdk-go/v1.14.1/sdk-go/integ/hard_delete_attribute_test.go)
<!-- dex-source: sdk-go/integ/hard_delete_attribute_test.go -->
```go
	if err := hardDeleteSecret.Delete(ctx); err != nil {
		return nil, err
	}
	if err := hardDeleteTokens.Delete(ctx, hardDeleteTokenInstance); err != nil {
		return nil, err
	}
```

- A deleted value reads as absent, so `Get` returns `*dex.AttributeNotFoundError`. Handle it by requesting a fresh secret, not by assuming the value still exists.
- Pass the secret at start with `dex.InitialAttribute(ModelAPIKey, apiKey)` or `dex.InitialAttributeMapValue` in `StartFlowOptions.Attributes`, and check the returned error. Build it from the same definition that the target Flow registers. StartFlow returns an error for a mismatched hard-delete setting or a Server older than protocol 3.
- Registration rejects integer, unsigned integer, floating-point, and Boolean value types. An interface value type is checked when a value is encoded.
- The option cannot be combined with `Indexed` or `SyncToAttributeStore`. `WaitForAttributeMatch`, `WaitForAttributeMapInstanceMatch`, and Action conditions reject it.
- A Step or RPC that writes the Attribute fails against an older Server or one with Blob Store disabled.
- Writes and deletes can become visible before the attempt commits. Keep handlers safe to repeat. Do not read-modify-write the value. See the [released API documentation](https://github.com/superdurable/dex/blob/sdk-go/v1.14.1/sdk-go/dex/attribute.go) for every limitation.

## Attribute Store projection

Opt a definition in with `dex.SyncToAttributeStore()` and select Server-configured stores in `dex.FlowConfig.AttributeStoreNames`. The projection is an asynchronous latest-state copy and the Flow stays authoritative. For a store that sets `rowKey`, `columnNaming`, or `attributeMaps` (Dex Server v1.6.0 or later), follow the Core [row keys, column names, and AttributeMap tables](../core/data-handling.md#row-keys-column-names-and-attributemap-tables) guidance.

- Derive each Flow ID in exactly the format the store's `rowKey` template expects. A Flow ID that does not match skips its batch.
- A struct-valued AttributeMap instance is encoded with `encoding/json`, so its `json` tag names, or its exported field names when untagged, are the top-level fields that become the columns of an AttributeMap table. Under `columnNaming: snake_case` an untagged `DeploymentURL` field writes the `deployment_url` column. A value that does not encode to a JSON object, such as a `string`, number, `bool`, or slice, is written to the `value` column.
- The instance name passed to the map is stored in `instanceKeyColumn` as written. Deleting the instance deletes its row in an AttributeMap table, while deleting a plain opted-in Attribute writes SQL `NULL`.

## Evolution

Add payload fields compatibly, keep old variants readable, and never repurpose persisted fields. For breaking changes, add a new Flow/Step type and retain old definitions until open runs drain.
