# s3mock

## Purpose

An S3-compatible object store on local disk, so Dex development and tests need no MinIO or cloud account.

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
and any access key. Request signatures are not verified by default.

Lifecycle rules run every `--lifecycle-scan-interval-seconds` (default 60; `0` disables).
A pass removes everything due at the lifecycle clock, so expiration happens within one interval
of its due time instead of on S3's daily schedule. Tests advance the lifecycle clock instead of
waiting days; dates, tag and size filters, and transitions return `501 NotImplemented`.

`--durability strict` (the default) flushes every acknowledged write with fsync, so it survives a
crash. `--durability relaxed` skips fsync. It is much faster, but a crash may lose acknowledged
writes, so do not use it for crash-recovery tests.

## Supported S3 API

| Area | Operations |
| --- | --- |
| Buckets | CreateBucket, HeadBucket, DeleteBucket (ends in-progress uploads), ListBuckets, GetBucketLocation |
| Versioning | GetBucketVersioning, PutBucketVersioning, ListObjectVersions, version-addressed GET, HEAD, and DELETE |
| Objects | PutObject, GetObject, HeadObject, DeleteObject |
| Listing | ListObjectsV2 with prefix, delimiter, max-keys, continuation-token, start-after, and encoding-type |
| Conditions | PutObject `If-None-Match: *` and `If-Match`; GET and HEAD `If-Match`, `If-None-Match`, `If-Modified-Since`, and `If-Unmodified-Since` |
| Integrity | `Content-MD5`; CRC32, CRC32C, CRC64NVME, SHA1, and SHA256 checksums in headers or aws-chunked trailers |
| Ranges | One `Range: bytes=` range per request |
| Multipart | CreateMultipartUpload, UploadPart, CompleteMultipartUpload (with `If-None-Match` and `If-Match`), AbortMultipartUpload, ListParts, ListMultipartUploads |
| Batch delete | DeleteObjects with Quiet mode and per-key results |
| Object Lock | Get/PutObjectLockConfiguration and CreateBucket with Object Lock; retention is stored, never enforced |
| Lifecycle | Put/Get/DeleteBucketLifecycle with prefix filters: `Expiration` `Days` and `ExpiredObjectDeleteMarker`, `NoncurrentVersionExpiration` (with `NewerNoncurrentVersions`), and `AbortIncompleteMultipartUpload` |

Other operations and subresources return `501 NotImplemented`.

## Test

```bash
make test
S3MOCK_CONFORMANCE_TARGET=aws make test
```

The second command runs the conformance suite against real S3; all 45 tests passed against AWS S3
in us-east-1 on 2026-10-23. AWS answered 500 InternalError when a checksummed multipart upload
listed part 2 without part 1; s3mock returns the documented 400 InvalidPartOrder instead. Run it on
demand, for example before relying on a newly emulated S3 behavior, with local AWS credentials:

```bash
AWS_PROFILE=<profile> AWS_REGION=us-east-1 S3MOCK_CONFORMANCE_TARGET=aws make test
```

It creates and deletes buckets named `s3mock-conformance-<ULID>`; set
`S3MOCK_CONFORMANCE_BUCKET_LOG=<file>` to record them so leftovers from failed tests can be deleted.
The `S3mock AWS conformance` workflow can run the same suite manually once the repository variable
`S3MOCK_CONFORMANCE_AWS_ROLE_ARN` names an IAM role that GitHub OIDC can assume; it has no schedule.

## Testing controls

All controls are opt-in and local to one server process. They are independent of the store's
persisted objects. `s3mock_server::start_with_config` accepts the same `ServerConfig` used by the
binary; `start` uses explicit defaults and ignores `S3MOCK_*` environment variables.

| Flag | Environment variable | Default | Meaning |
| --- | --- | --- | --- |
| `--fault-seed` | `S3MOCK_FAULT_SEED` | `0` | Seed for deterministic fault selection. |
| `--counter-prefix-depth` | `S3MOCK_COUNTER_PREFIX_DEPTH` | `1` | Slash-separated key components included in prefix counters; `0` groups all keys. |
| `--request-log-capacity` | `S3MOCK_REQUEST_LOG_CAPACITY` | `1024` | Recent completed requests retained; `0` disables retention. |
| `--etag-mode` | `S3MOCK_ETAG_MODE` | `md5` | `md5` or `opaque`. |
| `--signature-verification` | `S3MOCK_SIGNATURE_VERIFICATION` | `off` | `off` or `strict`. |
| `--access-key-id` | `S3MOCK_ACCESS_KEY_ID` | `s3mock` | Accepted strict-mode access key. |
| `--secret-access-key` | `S3MOCK_SECRET_ACCESS_KEY` | `s3mock` | Strict-mode signing secret. |

Opaque ETags identify a committed version rather than its MD5. Repeated HEAD/GET requests and
server restarts return the same ETag for that version. Writing identical bytes creates a different
opaque ETag. Use the returned ETag in `If-Match`; use checksum headers to compare content.

Strict mode verifies SigV4 header authentication, SHA256 payload hashes, `UNSIGNED-PAYLOAD`,
`STREAMING-UNSIGNED-PAYLOAD-TRAILER`, and chained signatures for
`STREAMING-AWS4-HMAC-SHA256-PAYLOAD` and `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`.
The latter also requires a valid trailer signature covering the declared trailers. Invalid signatures
return `403 SignatureDoesNotMatch`; absent or invalid header credentials return `403 AccessDenied`.
Presigned URLs return `501 NotImplemented` in strict mode. Signature timestamps must match the
credential date and use the YYYYMMDDTHHMMSSZ format. The region must be `us-east-1`.
All `x-amz-*` headers must be signed except `x-amz-content-sha256`, which is already bound by the
canonical payload hash. Malformed streaming framing returns `400 IncompleteBody`; clock freshness and session token issuance are not simulated. Only use test credentials.
Strict mode spools the encoded body to an anonymous temporary file under the store’s `tmp/` directory
and verifies it before any S3
handler runs, then streams it to the handler. Memory use stays bounded, but uploads require extra
local disk space and I/O. Encoded request bodies are limited to 6 GiB. Bodiless requests avoid spooling.
Off mode keeps the original streaming upload path.

### Admin HTTP API

The same listener serves JSON endpoints under `/_s3mock/`. Bucket names cannot start with `_`.
Admin endpoints are not authenticated, even with `--signature-verification strict`. Admin requests
bypass fault injection and do not affect S3 counters or logs.
Keep the listener on loopback when using these test controls. Successful calls return HTTP 200;
invalid rule documents return HTTP 400 with `{"error":"..."}` and preserve existing rules.
Unknown admin paths or methods return HTTP 404 JSON.

| Method and path | Request | Response |
| --- | --- | --- |
| `PUT /_s3mock/faults` | `{"rules":[...]}` | Replaces all rules atomically and restarts the seeded sequence; returns `rules` and `seed`. |
| `GET /_s3mock/faults` | None | `{"rules":[...],"seed":0}`. |
| `DELETE /_s3mock/faults` | None | Clears rules and restarts the sequence; `{"rules":[]}`. |
| `GET /_s3mock/counters` | None | `{"prefix_depth":1,"counters":{"by_operation":{...},"by_prefix":{...}}}`. |
| `DELETE /_s3mock/counters` | None | Resets counters only; `{"reset":true}`. In-flight requests are counted when they finish. |
| `GET /_s3mock/requests` | None | `{"capacity":1024,"requests":[...]}` in completion order, oldest first. |
| `POST /_s3mock/gc` | None | Runs orphan collection; `{"examined_files":0,"removed_files":0}`. |
| `GET /_s3mock/lifecycle/clock` | None | Returns the lifecycle time and its offset from wall-clock time; `{"now_unix_ms":0,"offset_ms":0}`. |
| `POST /_s3mock/lifecycle/advance` | `{"milliseconds":86400000}` | Moves the lifecycle clock forward (never backward) and returns the clock. |
| `POST /_s3mock/lifecycle/run` | None | Applies lifecycle rules once at the lifecycle time; returns `expired_current_versions`, `removed_delete_markers`, `expired_noncurrent_versions`, and `aborted_uploads`. |

GC uses the store's safety boundary: it examines data files older than the store's open time, so
an upload in flight cannot lose its data file. It does not advance a clock or expire live objects.

A rule has optional `operation`, `bucket`, `key_prefix`, and `has_conditional_header` match fields,
an optional `probability` from 0 through 1 (default 1), and one required `action`. Match fields
combine with AND. Omitted fields match any value. Operation names use S3 API spelling, such as
`PutObject`, `GetObject`, or `HeadObject`. Conditional headers mean `If-Match`, `If-None-Match`, `If-Modified-Since`, or `If-Unmodified-Since`.
Unknown operation names and action fields are rejected. Rules run in order; each matching rule consumes a seeded random draw, and the first selected rule
wins. Replacing the rules reproduces a sequence for the same matching request arrival order;
concurrent scheduling can change that order. An SDK retry is a separate request and draw.

| Action JSON | Behavior |
| --- | --- |
| `{"type":"commit_then_drop"}` | Requires `operation` to be `PutObject`, `UploadPart`, or `CompleteMultipartUpload`. A successful store commit closes the connection without a response; an unsuccessful condition still returns its normal error. |
| `{"type":"delay","milliseconds":100}` | Waits before invoking the S3 handler. Maximum 600000 milliseconds (ten minutes); deleting faults releases sleeping requests. |
| `{"type":"error","status":500}` | Returns `InternalError` without invoking the store. Status 503 returns `SlowDown`; 409 returns `ConditionalRequestConflict`. Other statuses are rejected. |
| `{"type":"fail_before_commit"}` | Returns `500 InternalError` without invoking the store. |
| `{"type":"corrupt_body"}` | Requires `operation: "GetObject"`. For successful 200 or 206 responses, flips the first returned byte and preserves the original headers, including checksums and ETag. Empty bodies have no byte to flip. |
| `{"type":"truncate_response"}` | Requires `GetObject`, `ListBuckets`, `ListObjectsV2`, `ListObjectVersions`, `ListParts`, or `ListMultipartUploads`. Sends part of a successful response body, then terminates the stream with an error. |

For example, test an ambiguous conditional write with automatic SDK retries disabled:

```bash
curl -X PUT http://127.0.0.1:9000/_s3mock/faults \
  -H 'content-type: application/json' \
  -d '{"rules":[{"operation":"PutObject","bucket":"example","key_prefix":"pending/","has_conditional_header":true,"action":{"type":"commit_then_drop"}}]}'
curl http://127.0.0.1:9000/_s3mock/requests
curl -X DELETE http://127.0.0.1:9000/_s3mock/faults
```

The first PUT commits but fails at the client. A retry with `If-None-Match: *` receives 412;
reading the object returns the committed content.

Each counter contains `requests`, `bytes_received`, and `bytes_sent`. `by_operation` maps API
names to counters. `by_prefix` maps bucket names to prefix-to-counter maps. Body bytes count
encoded request bytes consumed and response bytes yielded to HTTP, excluding headers. They are
not transport acknowledgments. A response truncated by a fault counts only its yielded prefix.
Requests are recorded when the response body is released; clients should finish reading before
inspecting counters. Error responses count too. Prefix groups are created only for existing buckets and supported operations.
The first 1024 bucket/prefix groups are retained until reset; new groups fold into
`by_prefix["<overflow>"]["<overflow>"]`. Operation counters are unaffected by this cap.

Every S3 request also emits one structured tracing event with `operation`, `bucket`, `key`,
`if_match`, `if_none_match`, `if_modified_since`, `if_unmodified_since`, `status`, `duration_ms`,
`bytes_received`, `bytes_sent`, and
`injected_fault`. The same fields appear in the recent request ring. Status 0 means the server
intentionally dropped a committed response or truncated a response before yielding any body bytes. No authorization header or signing secret is logged.

The integration suite exercises all fault actions through `aws-sdk-s3`, seeded replay, admin
validation and resets, counters and bounded logs, opaque ETag conditions, SDK memory/file
uploads in strict mode, and signed chunk/trailer tampering through raw HTTP.
