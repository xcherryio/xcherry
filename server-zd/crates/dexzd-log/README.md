# dexzd-log

## Purpose

Command log batch format and the conditional-write commit protocol that makes server-zd writes durable in object storage.

The codec stores a protobuf header, one zstd frame per partition, and an xxh3-128 trailer. Partition reads use two object-store range requests and verify the frame checksum. Full reads verify the trailer and every frame.

`recover_stream` scans forward from the last applied sequence and effective epoch. An empty store uses `recover_stream_from` with the manifest segment's first sequence and epoch zero. Recovery writes EpochStart at the first gap. It then lists the stream beyond it, voids every older pipelined object there, and fills each other gap below the last object with another EpochStart of the new epoch, so the log stays dense and replay reads it seq by seq. The pipeline starts after the last listed object. An object of the new or a newer epoch in that range fences recovery. An older write that lands after the listing still stops the node, and the next start recovers again. Log cleanup deletes only batches below a published snapshot's applied position, so a pipelined gap always follows an existing batch. If the first batch to read and the batch before it are both missing while a later batch exists, cleanup deleted batches the caller still needs. Recovery then returns `LogError::MissingBatches` and writes nothing. The engine records the effective epoch with its applied position so recovery can resume after older log objects are removed. The returned batches are in apply order. `CommitPipeline` accepts opaque record bytes, writes batches with `CreateOnly`, and emits committed batches in sequence order. The engine applies those batches before answering application requests.
