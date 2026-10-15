# Python data handling

For application indexes, keep business Attribute names and supply explicit generic typed keys such as `keyword1`, `keywordList1`, `text1`, or `int1`; reuse the same typed slots across Flow types and constrain application run searches by FlowType. Parenthesize additional caller filters when composing the query. See [core primitives](../core/primitives.md#attribute). Pinned excerpts preserve their baseline names rather than defining the naming policy.

Register a non-indexed `Attribute("currentMessagesLock", bool)` alongside the map and use its `.lock()` in the applicable StepOptions or registration-time RPCOptions lock collection. Follow [whole-map coordination](../core/data-handling.md#whole-map-coordination): every protected writer uses the same singleton lock, its bool value is not an acquisition flag, and map loads remain explicit.

Use dataclasses or other explicit typed models for durable payloads. Avoid `Any`, arbitrary object graphs, lambdas, open file handles, and process-local state. Persisted field names/types are compatibility contracts.

Attributes hold current state; AttributeMaps partition it; Channels hold queued intent; Streams hold progress; Step inputs/outputs describe transitions. Context writes commit with successful invocation return. On exception, expect replay from the prior boundary.

Map instance names are non-empty and contain no `/`. Select exact instances for entity handlers. `AttributeMapNotLoadedError` means the snapshot omitted the entry, not that it does not exist.

For append-only history, bound a `current` list, archive each full chunk under its zero-padded first sequence, and load one page-token instance instead of the whole map. For keyed records, keep a dictionary keyed by the full canonical value inside one stable hash bucket. The official email pattern trims ASCII whitespace, lowercases ASCII letters, rejects empty or non-ASCII values, computes wrapping FNV-1a 32-bit, and selects `hash % 1000`. See [sequential chunking](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/patterns/sequentially-chunked-attribute-map/chunked_subscriber_flow.py) and [hash partitioning](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/patterns/hash-partitioned-attribute-map/customer_directory_flow.py).

Use Stream for incremental output and Attribute for authoritative latest state. Persist read tokens. Buffered text adds a flush boundary; async writer `write` is synchronous while `context.heartbeat` is awaited. A sync buffered writer produces outputs that must be yielded.

Share one BlobCache with Client and Worker for large payloads. Capacity/path are deployment concerns. BlobCache is payload locality, not general durable filesystem state. See pinned [async composition](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/dex_examples/app.py) and [SDK guide](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/sdk-python/README.md).

The Server keeps payloads through 100 bytes inline by default. Treat internal blob references as opaque: hydration and cache lookup use the owning Flow ID with the reference, and Dex rewrites blob-backed values that cross into another Flow. String and Object references share a compact Base36-date shape such as `p1|c/ab3de7kp2x`; their Value arms distinguish them. Object Blobs store the complete EncodedObject with `json`, `raw`, or a custom encoding, so references have no encoding suffix. The Server's `objectIdLength` defaults to 10, accepts any positive length, and treats zero as the default; readers accept any nonempty lowercase Base36 object ID.

ASYNC local Step input snapshots are disabled by default. Enable the Server's `blobStore.asyncStepInputSnapshotsEnabled` only when semantic history needs exact method inputs; it does not affect execution, retry, or recovery.

Python `None` uses the Value null arm and decodes back to `None`. In an Attribute write it deletes the Attribute, and as a Flow completion output it is discarded. Return an explicit result object when terminal null and no output must differ.

## Attribute Store projection

Opt a definition in with `sync_to_attribute_store=True` and select Server-configured stores with `FlowConfig(attribute_store_names=[...])`. The projection is an asynchronous latest-state copy and the Flow stays authoritative. For a store that sets `rowKey`, `columnNaming`, or `attributeMaps` (Dex Server v1.6.0 or later), follow the Core [row keys, column names, and AttributeMap tables](../core/data-handling.md#row-keys-column-names-and-attributemap-tables) guidance.

- Derive each Flow ID in exactly the format the store's `rowKey` template expects. A Flow ID that does not match skips its batch.
- A dataclass AttributeMap instance is encoded as a JSON object of its field names (`dataclasses.asdict`), so those names are the columns of an AttributeMap table. Python field names are usually already snake_case, so `columnNaming: snake_case` matters mostly for camelCase Attribute names. A `str`, number, `bool`, or list value is written to the `value` column.
- The instance key is stored in `instanceKeyColumn` as written. Deleting the instance deletes its row in an AttributeMap table, while deleting a plain opted-in Attribute writes SQL `NULL`.

For incompatible serialization changes, introduce a new Flow/Step type and keep old definitions until open runs drain.
