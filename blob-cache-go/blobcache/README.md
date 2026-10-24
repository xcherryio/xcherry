# Blob Cache Go

`blobcache` stores hydrated Dex string and object blobs on disk. It uses
Ristretto v2 for TinyLFU admission and SampledLFU eviction, while Ristretto
entries contain metadata only.

The cache is never authoritative. Callers must use a fresh `LoadBlobs` value on
a miss, admission rejection, or cache write failure.

## Usage

```go
cache, err := blobcache.New(&blobcache.Config{
    Dir:               "/var/tmp/dex-worker/blobs",
    MaxBytes:          1 << 30,
    FrequencyCounters: 100_000,
    Logger:             logger,
})
if err != nil {
    return err
}

cached, err := cache.Put(blobID, payload)
if err != nil {
    // Keep using the freshly loaded payload and report the cache error.
}
if !cached {
    // Continue with the freshly loaded payload.
}

payload, found, err := cache.Get(blobID)
if err != nil {
    return err
}
if found {
    use(payload)
}
```

The cache treats payloads as opaque bytes. The hydration layer stores string
bytes directly and deterministically marshals the complete `EncodedObject`,
including its encoding, for object blobs.

`Config.Logger` defaults to `slog.Default` and is available through
`Cache.Logger` for callers that need consistent component logging.

`Dir` must be dedicated to one cache process. Client and Worker code in the
same process may share a `Cache`; separate processes must use separate
directories.

`MaxBytes` counts each complete logical cache file: its 24-byte header, blob ID,
and payload. It is not filesystem-block or operating-system quota accounting.

### What FrequencyCounters means

Ristretto's TinyLFU policy approximately remembers how often keys are accessed.
`FrequencyCounters` controls the size of that in-memory frequency sketch. It is
mapped to Ristretto's `NumCounters` setting. It is not:

- the maximum number of cached blobs;
- the maximum number of reads retained exactly;
- part of the disk-capacity calculation.

For example, suppose `MaxBytes` usually holds about 1,000 blobs. Setting
`FrequencyCounters` to 10,000 follows Ristretto's recommended starting point of
roughly 10 counters per expected cached item.

Setting it to 1,000 still works and does not reduce the 1,000-blob disk
capacity. It gives the frequency sketch less room, however. Resident blobs,
cache misses, and newly considered blobs all contribute access events to the
same approximate sketch. With only 1,000 requested counters, unrelated keys
collide more often and one-use scan traffic can make cold blobs look hotter
than they are. Admission and eviction decisions therefore become less
accurate, so a genuinely hot blob may be rejected or evicted more often.

The tradeoff in this example is approximately 3 KB instead of 30 KB of policy
memory before Ristretto's internal rounding. Cache correctness, file integrity,
and `MaxBytes` enforcement are unchanged; only the expected cache hit ratio is
affected.

If blob `A` is read 100 times while thousands of other blobs are each read
once, the sketch lets Ristretto recognize `A` as relatively hot. When disk
space is needed, a one-use blob is then more likely to be rejected or evicted
than `A`.

Counters are approximate and shared between keys; they do not store exact
per-key histories. Ristretto documents roughly 3 bytes of memory per requested
counter before internal rounding, so 10,000 counters uses approximately 30 KB
for policy tracking. Increase it for a cache expected to hold many more blobs;
changing it does not change `MaxBytes`.

## Lifecycle

- `Delete(id)` removes one cached blob.
- `DeleteAll()` removes all cache-owned files and resets policy state. The
  cache remains usable.
- An orderly `Close()` releases resources without deleting committed files.

For ephemeral shutdown, always close even if deletion fails:

```go
purgeErr := cache.DeleteAll()
closeErr := cache.Close()
return errors.Join(purgeErr, closeErr)
```

Startup removes incomplete temp files, validates committed entries, and
reconciles them against the current byte limit. Access-frequency history is not
persisted. Cache writes do not synchronize files or directories to disk. An
operating-system or power failure may lose a recent entry or leave an invalid
file; validation rejects it, and the caller can fetch the blob again.

## Development

Run the component and race suite through the Makefile:

```text
make -C blob-cache-go blobCacheTests
```

The cache does not call `LoadBlobs` or require a Dex server or Temporal.
