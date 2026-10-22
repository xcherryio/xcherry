# Dex - Durable Execution(D-EX)

> ⚠️ **Beta:** Dex's core features are complete and its core APIs are stable. Minor API refinements may still be introduced before general availability.

**Durable Execution** provides programming model that makes a process survive any disruptions. 

There are three types of disruptions:
* **Business disruption**
* **External disruption**
* **Internal disruption**
  
Take a simple refund. A customer asks for a refund. A manager approves it. Then the system calls an API to move the money.
But the manager may forget(business discruption). The payment API may be down(external discuprtion). Our own server may restart or has a bug(internal disruption). Then the refund gets stuck, and the business gets hurt. And real refund processes are a hundred times more complex so they have 100 times more disruptions like those.

Dex make every process is a durable execution. Every step is saved, retried, and can wait. If the manager forgets, we send a reminder, or escalate. If the API is down, we keep retrying. If our server restarts, the run picks up right where it stopped.

Underneath, Dex provide local state and control flow such as branches and loops, as well as parallel execution and coordination, waiting for timers or external events, error handling, and remote procedure invocations. The application logic is expressed directly in ordinary code, while the platform reliably restores and resumes the execution after failures and restarts. 

Dex is a simple durable execution framework optimized for AI agents, human+AI processes, with high performance & scalability. It includes in-memory/best-effort streaming, attribute storage sync, and automatic offloading and cleanup of large payloads in blob storage. [Dex Connectors Library](https://github.com/superdurable/dex-connectors-library) provides reusable integrations that help you move workflows quickly from prototype to production. 

**Dex** provides a structural programming model with only a few concepts as [durable primitives](https://docs.superdurable.io/primitives). You use Dex to write a Flow filled with ordinary code: durable Steps, Attributes, RPCs, and durable conditions using Channels and Timers. Then you run Workers hosting your Flow. The Client calls Dex Server to start and interact with Flow instances. Dex Server dispatches Step and RPC invocation tasks to your Workers.

<img width="901" height="719" alt="dex-arch3" src="https://github.com/user-attachments/assets/4b70a5ec-8c94-4f13-acc4-8f7958245bda" />

### Durable Execution Without the Replay Footguns

Unlike most durable execution frameworks, **Dex takes a fundamentally different approach with its non-replay-based programming model**. It eliminates entire classes of footguns—such as replay-induced non-determinism errors and the need for Continue-As-New—by design, rather than forcing developers to work around them.

### Battle-Tested, Not Built From Scratch

Dex is forked from [iWF](https://github.com/indeedeng/iwf) by its original creator, building on over **5 years of production experience**, with tens of engineering teams and millions of workflows running in production, without reported incidents.

This isn't just a promising architectural idea. It's a programming model proven at scale in real-world production environments.

### Built for Real-World Complexity

To demonstrate what developers can build with Dex, we're also open-sourcing sophisticated platforms built on top of it—including **Superverse, a durable AI agent harness**.

Superverse demonstrates how Dex can orchestrate long-lived, interactive agent sessions, parallel tool execution, subagents, human-in-the-loop interactions, failure recovery, and complex resource lifecycles—all within a structured, durable execution model.

Explore the [Superverse Agent Harness Architecture](https://github.com/superdurable/superverse/blob/main/docs/architecture/agent-harness-design/session.md).

Learn more: [What is Durable Execution?](https://docs.superdurable.io/intro/what-is-durable-execution) · [Why Dex?](https://docs.superdurable.io/intro/what-is-dex)

AI coding assistants can use the official [Dex Skills](https://docs.superdurable.io/build-with-ai/dex-developer-skill) to build, test, and operate Dex applications through Dex's public programming model. Install either the recommended Dex Plugin or the complete standalone Skills bundle from [superdurable/dex-skills](https://github.com/superdurable/dex-skills), but not both.

The [skills/](skills/) module contains the Dex Skills source.

## 💬 Community

🎉 Join the [SuperDurable Dex community on Slack](https://join.slack.com/t/superdurableworkspace/shared_invite/zt-4aby5e0b6-6rNT9zN6BzbroHZpGyia0A) to ask questions, share feedback, and connect with other Dex users and contributors.

## Quick start

See [Quick start](https://docs.superdurable.io/quick-start) on docs.superdurable.io.
