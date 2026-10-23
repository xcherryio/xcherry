# dexzd-log

## Purpose

Command log batch format and the conditional-write commit protocol that makes server-zd writes durable in object storage.

The codec stores a protobuf header, one zstd frame per partition, and an xxh3-128 trailer. Partition reads use two object-store range requests and verify the frame checksum. Full reads verify the trailer and every frame.

`recover_stream` scans the stream, skips batches voided by an earlier EpochStart, and writes the new EpochStart at the first gap. The returned batches are in apply order. `CommitPipeline` accepts opaque record bytes, writes batches with `CreateOnly`, and emits committed batches in sequence order. The engine applies those batches before answering application requests.
