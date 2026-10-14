# Durable AI Agents

Use this guide when Dex owns an Agent's conversation, model calls, tools, approvals, and long waits.

## Own the model context in Dex

Persist provider-neutral user, assistant, tool-call, and tool-result messages. Rebuild the model input for every call from a configured system prompt, a cumulative summary, and recent messages. Do not depend on a provider conversation ID when the application must support multiple LLM APIs.

Use an AttributeMap with monotonic sequence keys for independently stored messages. Keep the next sequence, retained range, summarized range, pending calls, and active status in a small Attribute. Do not enumerate an unbounded AttributeMap to paginate history; track the sequence range and read known keys.

Compaction changes the model context, not the durable truth. Commit a summary that covers an explicit sequence range before deleting any covered message instances. If old messages only need Flow-history retention, bound the current AttributeMap and document that they are unavailable through the application after deletion.

## Make plans durable

Keep a short, atomically replaced plan in a regular Attribute. Use AttributeMap
for independently updated large collections, not for a todo list that a model
replaces as one value.

Separate plan lifecycle from Flow activity. A Flow waiting for input may still
have pending work. Derive completion from task states and expose both values to
the UI.

For planning-only interactions, hide executable tools instead of relying on a
prompt. Carry explicit plan approval or continuation through a Channel and bind
it to the current plan revision. Stream plan progress for responsiveness, but
reload the durable Attribute after loss or refresh.

## Separate durable and live output

Commit the completed assistant message and tool result to Attributes. Use a Stream for token deltas, tool progress, and UI status. A retry may repeat Stream messages, so the UI must recover from durable message state.

For streaming LLM output, use separate buffered text writers for provider-authored reasoning summaries and visible assistant text. Keep structured tool and lifecycle events on a third Stream.

Never expose hidden chain-of-thought or label ordinary text as reasoning. Show a Thinking surface only for an explicit provider reasoning-summary event. Providers without that event should expose response text and Agent activity without fabricated thinking.

When a stateless provider requires opaque reasoning items on later calls, persist the encrypted items beside the assistant message. Replay them as model context without interpreting or displaying them.

## Route tools through Steps

Persist the assistant tool call before executing it. Give every call a stable ID and execute calls in an intentional order with bounded concurrency.

Classify external operations before choosing retry behavior:

- read-only or idempotent calls may use bounded retries
- non-idempotent writes default to one attempt unless the application accepts duplicate effects
- exhausted retries become an explicit failure result or recovery Step

Dex commits the tool result separately from the external effect. Pass a stable call ID to integrations that support idempotency, and surface an unknown outcome when a timeout cannot prove whether the effect happened.

## Make approval durable

Use a ChannelMap keyed by tool-call ID for approvals. Store the pending request in an Attribute so the UI can reload it. Unknown, destructive, or write tools should wait for approval unless trusted application policy classifies them otherwise.

Do not accept executable MCP commands, remote URLs, or credentials from an untrusted Flow input. Register trusted servers in Worker configuration and let a Flow select only from that registry.

When the model needs a user reply, route the question through a durable input tool instead of assistant prose alone. Persist the pending question in an Attribute and wait on the user Channel. Expand an inline input panel in the conversation. Let the tool provide known choices for selection buttons; use free-form input when valid answers are not enumerable.

## Separate queued messages from steering

Use one Channel for queued user messages and another for steered messages. While the Agent loop is active, leave queued messages pending so the user can inspect, edit, delete, or explicitly steer them. Consume queued messages in FIFO order after the Agent reaches its user-input wait.

Implement Steer with a transactional RPC that explicitly loads the queued Channel. Send only the selected message ID, find its original Value in the loaded snapshot, then stage its deletion and publication to the steered Channel. Treat a missing ID as stale UI state and refresh the queue. Do not copy queued messages into conversation history until the Agent consumes them.

Apply steered messages at safe Step boundaries. Do not cancel an in-flight model or tool invocation. Before the next model call, tool call, approval continuation, or timer continuation, drain steered messages, persist structured cancellation results for abandoned calls, clear stale approval or timer state, and let the model replan.

Only steered messages should interrupt a pending approval or durable Timer. A queued message remains editable and does not alter active work until the Agent becomes idle or the user chooses Steer.

Expose one read-only application snapshot RPC for the browser. Explicitly load the conversation AttributeMap and both pending-message Channels, then return application history, Agent description, queued messages, and steered messages from that invocation. Include a Run ID only when a consumer must pin or correlate one exact execution across Flow ID reuse. This history is the application's durable message history, not Dex execution history. A retained terminal run can serve the query, so RPC success does not prove the Agent is active. Use a lifecycle API when the response must include current execution status. Reconcile after mutations and live events, on focus or reconnect, and with a low-frequency fallback poll.

## Model long waits as Timer tools

A durable wait tool should transition to a Step whose **WaitFor** returns a Timer condition. Do not keep a model call, MCP call, coroutine, or worker process blocked for the delay.

To support explicit interruption, race the Timer against the steered-message Channel. If that message wins, persist an interrupted tool result, consume the message, clear stale pending calls, and ask the model to replan. Leave queued messages pending until the Agent reaches its input wait.

## Verification

Integration-test context reconstruction after Worker replacement, compaction before deletion, approval after page reload, retry exhaustion, Stream loss recovery, Timer firing, and user interruption.
