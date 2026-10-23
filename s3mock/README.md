# s3mock

Purpose: an S3-compatible object store on local disk, so Dex development and tests need no MinIO or cloud account.

## Crates

| Crate | Purpose |
| --- | --- |
| [`s3mock`](crates/s3mock/) | The store: redb metadata plus one self-describing data file per object version. |
| [`s3mock-server`](crates/s3mock-server/) | The `s3mock` binary and library: path-style S3 over HTTP/1.1. |
| [`s3mock-conformance`](crates/s3mock-conformance/) | Conformance tests that run against s3mock or real S3 through `aws-sdk-s3`. |

## Run

```bash
make run ARGS="--data-directory /tmp/s3mock-data --listen-address 127.0.0.1:9000"
```

Every flag also reads an environment variable: `S3MOCK_LISTEN_ADDRESS`, `S3MOCK_DATA_DIRECTORY`,
and `S3MOCK_DURABILITY`. Point an S3 client at `http://127.0.0.1:9000` with path-style addressing
and any access key. Request signatures are not verified.

`--durability strict` (the default) flushes every acknowledged write with fsync, so it survives a
crash. `--durability relaxed` skips fsync. It is much faster, but a crash may lose acknowledged
writes, so do not use it for crash-recovery tests.

## Supported S3 API

| Area | Operations |
| --- | --- |
| Buckets | CreateBucket, HeadBucket, DeleteBucket, ListBuckets, GetBucketLocation |
| Versioning | GetBucketVersioning, PutBucketVersioning, ListObjectVersions, version-addressed GET, HEAD, and DELETE |
| Objects | PutObject, GetObject, HeadObject, DeleteObject |
| Listing | ListObjectsV2 with prefix, delimiter, max-keys, continuation-token, start-after, and encoding-type |
| Conditions | PutObject `If-None-Match: *` and `If-Match`; GET and HEAD `If-Match`, `If-None-Match`, `If-Modified-Since`, and `If-Unmodified-Since` |
| Integrity | `Content-MD5`; CRC32, CRC32C, CRC64NVME, SHA1, and SHA256 checksums in headers or aws-chunked trailers |
| Ranges | One `Range: bytes=` range per request |

Other operations and subresources return `501 NotImplemented`.

## Test

```bash
make test
S3MOCK_CONFORMANCE_TARGET=aws make test
```

The second command runs the conformance suite against real S3. It creates and deletes buckets
named `s3mock-conformance-<ULID>`.
