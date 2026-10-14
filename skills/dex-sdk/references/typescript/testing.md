# Testing TypeScript Dex applications

Use Node's test runner or the application's framework for assertions, but run durable behavior against a real Dex Server. Mocks cannot prove Worker replacement, durable wait, retry exhaustion, or terminal semantics.

## Environment

Create an isolated Registry, free Worker port, temporary BlobCache directory, Worker, and Client. Close them in `finally`. Generate a UUID-based Flow ID for every test.

[Pinned test environment](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/sdk-typescript/test/integ/environment.ts)
<!-- dex-source: sdk-typescript/test/integ/environment.ts -->
```typescript
export async function withEnvironment(
  flows: readonly Flow<any>[],
  run: (environment: TestEnvironment) => Promise<void>,
): Promise<void> {
  const environment = await startEnvironment(...flows);
  try {
    await run(environment);
  } finally {
    await environment.close();
  }
}

export function flowId(prefix: string): string {
  return `${prefix}-${randomUUID()}`;
}
```

## Required scenarios

1. Start and terminal output with a bounded `waitForFlow`.
2. Worker replacement while waiting, executing, and retrying; replacement uses the same registered types and a new process/cache handle.
3. Deterministic wait/execute retry exhaustion and configured recovery.
4. Channel publish before and after wait registration, RPC concurrency, Timer readiness, and SubFlow outcome.
5. Unique Flow IDs and explicit ID reuse behavior.
6. Deadline polling for asynchronously registered Timer/Channel state; preserve the last failure.
7. Post-terminal publish/RPC/stop behavior with typed SDK errors.
8. Heartbeat checkpoint recovery, cancellation through `AbortSignal`, and Stream resume tokens.

[Pinned terminal assertion](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/sdk-typescript/test/integ/basic.integration.test.ts)
<!-- dex-source: sdk-typescript/test/integ/basic.integration.test.ts -->
```typescript
test("basic workflow completes and disallows duplicate IDs", async () => {
  const flow = new BasicFlow();
  await withEnvironment([flow], async ({ client }) => {
    const id = flowId("basic");
    const options = { idReusePolicy: IdReusePolicy.DISALLOW };
    await client.startFlow(flow, id, 0, options);
    assert.equal(await client.waitForFlow(id, 30_000).then((result) => result.singleOutput(doubleCodec)), 2);
    await expectError(client.startFlow(flow, id, 0, options), FlowAlreadyStartedError);
  });
});
```

Use polling with a deadline only when the Client has no direct wait. Do not use an unbounded retry or fixed delay as the assertion.

## Commands

```bash
cd examples/typescript
npm test
npm run test:integ
npm run smoke
./run-integration-tests.sh
```

Run type checking and linting from the package scripts as well. A test plan should state which scenarios are pure unit tests and which require Dex.
