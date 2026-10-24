# Blob Cache Go

`blob-cache-go` is an independently versioned disk blob-cache module shared by
Dex components. The [blobcache package](blobcache/README.md) uses Ristretto
TinyLFU admission and SampledLFU eviction while keeping payloads on disk.
Cache writes are not synchronized to disk; recovery discards incomplete or
corrupt files so callers can fetch those blobs again.

Dex Server and the Go SDK consume released versions of this module. Use
path-style tags such as `blob-cache-go/v0.1.0` for releases.

Run its tests from the repository root:

```text
make -C blob-cache-go tests
```
