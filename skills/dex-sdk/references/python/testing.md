# Testing Python applications

Use a real Dex Server for durable behavior. Mock-only tests cannot prove registration, codecs, replacement, retries, waits, or terminal semantics.

## Harness

Run `dexcli dev`; start `AsyncWorker`/`Worker`; create the matching Client with the same Registry/BlobCache. Generate every Flow ID with `uuid4().hex`. Bound operations with `timedelta` arguments or `asyncio.timeout`; shut workers down in fixtures.

[Pinned integration source](https://github.com/superdurable/dex/blob/sdk-go/v1.5.0/examples/python/tests/integ/conftest.py)
<!-- dex-source: examples/python/tests/integ/conftest.py -->
```python
def make(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex}"
```

```bash
cd examples/python
make unitTests
make e2eTests
```

## Scenarios

1. Start/await a result and assert output plus terminal status.
2. Replace Worker after a durable boundary and prove resumption.
3. Exhaust retry and assert typed failure or recovery.
4. Exercise pre-wait and concurrent Channel publication through RPCs, RPC idempotency, and message deletion.
5. Restart across Timer without fixed test sleeps.
6. Read Stream frames with resume tokens.
7. Separate graceful/force completion, failure, cancellation, timeout handler, and uncompleted behavior.
8. Run both sync generator and async coroutine paths when the product supports both.

Poll with a deadline or Client long polls. Do not `await asyncio.sleep(...)` merely for convergence. On timeout, print IDs, summary, history, and active Step attempt. Assert business invariants rather than scheduling order. Use unique map/entity instances with shared stores.
