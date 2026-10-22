# Getting started

Use this guide for a new Dex application or when Worker and Client wiring is missing.

## Local environment

Install **dexcli** with the supported package for the user's platform. On macOS:

```bash
brew install superdurable/tap/dexcli
dexcli dev
```

Use the Dex Server and Dex Web addresses printed by the command. Defaults are usually **127.0.0.1:8801** and **http://127.0.0.1:8802**, but another local process can cause different ports. Pass the printed server address to the application instead of assuming the default.

## First vertical slice

Build the smallest end-to-end path in this order:

1. Define one typed start input and one start Step.
2. Make the Step return a terminal decision with a typed output.
3. Define the Flow's Step list.
4. Register the Flow in the application registry.
5. Create one Worker using that registry and a reachable bind/advertise address.
6. Create one Client using the same registry and blob/payload configuration.
7. Start the Worker before accepting application requests.
8. Read [error handling](error-handling.md) and the language error page; define stable request identity, duplicate-start behavior, uncertain acceptance reconciliation, and local-defect handling.
9. Start the Flow through an HTTP handler, CLI command, or service method.
10. Add an integration test that starts the Flow and waits for its result.

Add Attributes, Channels, RPCs, Streams, or SubFlows only when the first path runs successfully.

## Required topology

- Dex Server receives public Client calls and dispatches handler work.
- The Worker hosts registered Flow, Step, and RPC implementations.
- The Client starts and interacts with Flow executions.
- Dex Web inspects Flow state and Step progress.
- The application owns its HTTP, messaging, database, and third-party integrations.

The Worker must be reachable from Dex Server. A bind address controls where the Worker listens; an advertised target controls the address Dex Server calls. These differ when containers, proxies, or load balancers are involved.

## Configuration checks

Before debugging code, verify:

- the application uses the server address printed by **dexcli dev**
- the Worker advertised target is reachable from Dex Server
- the Flow type is present in the Worker and Client registries
- the Worker and Client use compatible payload codecs and blob cache/storage
- all Attributes and Channels used by handlers are in the Flow persistence schema
- Indexed Attributes are synchronized before the Worker starts serving
- local Go Connector applications start with the configuration their pinned connector release reads, as described in [Local Connector credentials](operations.md#local-connector-credentials)

## Source material

- Quick start: https://docs.superdurable.io/quick-start
- Primitive overview: https://docs.superdurable.io/primitives
- Baseline runnable examples: https://github.com/superdurable/dex/tree/sdk-go/v1.13.0/examples

When a local Dex checkout is available, copy API usage from its runnable **examples/** files rather than from prose or memory.
