# Dex - Durable Execution(D-EX)

> ⚠️ **Beta:** Dex's core features are complete and its core APIs are stable. Minor API refinements may still be introduced before general availability.

**Durable Execution** provides programming model that makes an application's execution durable. This includes local state and control flow such as branches and loops, as well as parallel execution and coordination, waiting for timers or external events, error handling, and remote procedure invocations. The application logic is expressed directly in ordinary code, while the platform reliably restores and resumes the execution after failures and restarts. 

Temporal is the leading Durable Execution platform.

Dex extends Temporal to be even more powerful and more area. Dex is a simple durable execution framework optimized for AI agents, human+AI processes, with high performance & scalability. It includes in-memory/best-effort streaming, attribute storage sync, and automatic offloading and cleanup of large payloads in blob storage. [Dex Connectors Library](https://github.com/superdurable/dex-connectors-library) provides reusable integrations that help you move workflows quickly from prototype to production. 

**Dex** provides a structural programming model with only a few concepts as [durable primitives](https://docs.superdurable.io/primitives). You use Dex to write a Flow filled with ordinary code: durable Steps, Attributes, RPCs, and durable conditions using Channels and Timers. Then you run Workers hosting your Flow. The Client calls Dex Server to start and interact with Flow instances. Dex Server dispatches Step and RPC invocation tasks to your Workers.

<img width="901" height="719" alt="dex-arch3" src="https://github.com/user-attachments/assets/4b70a5ec-8c94-4f13-acc4-8f7958245bda" />


Learn more: [What is Durable Execution?](https://docs.superdurable.io/intro/what-is-durable-execution) · [Why Dex?](https://docs.superdurable.io/intro/what-is-dex)

AI coding assistants can use the official [Dex Skills](https://docs.superdurable.io/build-with-ai/dex-developer-skill) to build, test, and operate Dex applications through Dex's public programming model. Install either the recommended Dex Plugin or the complete standalone Skills bundle from [superdurable/dex-skills](https://github.com/superdurable/dex-skills), but not both.

The [skills/](skills/) module contains the Dex Skills source.

## 💬 Community

🎉 Join the [SuperDurable Dex community on Slack](https://join.slack.com/t/superdurableworkspace/shared_invite/zt-4aby5e0b6-6rNT9zN6BzbroHZpGyia0A) to ask questions, share feedback, and connect with other Dex users and contributors.

## Quick start

See [Quick start](https://docs.superdurable.io/quick-start) on docs.superdurable.io.

## Modules

- [`server/all-in-one/`](server/all-in-one/README.md): builds Dex Server and its backend in one image.
