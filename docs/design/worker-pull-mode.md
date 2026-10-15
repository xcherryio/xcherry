# Worker pull mode and Stream routing

Worker pull mode would let a Worker request dispatches from Dex instead of exposing a WorkerService address. Matching would assign each request to an available Worker for its Flow type.

## Stream routing (STREAMROUTE)

A consistent hash of Flow ID over the live server replicas would assign each Stream to one replica. A replica receiving a read or write for a Stream it does not own would forward it to the owner over the internal listener. The replica set would come from a headless Service. When membership changes, moved Streams would restart empty. Readers could resume from the Flow's durable state. This is a later design, not current server behavior.
