# Sync Python examples

Showcase of the **sync** Dex Python SDK surface (`Client` / `Worker`) next to
the primary async examples under `examples/python/`.

Targets [`dex-python-sdk==0.6.0`](https://pypi.org/project/dex-python-sdk/0.6.0/).
Requires Python 3.11+.

The samples use concrete SDK errors for missing, inactive, and duplicate Flows
instead of inspecting Dex sub-status metadata.

Six Flows:

| Demo | Route prefix | Notes |
|------|--------------|--------|
| Basic | `/primitives/step` | smallest Flow |
| Money transfer | `/products/money-transfer` | saga |
| Engagement | `/products/engagement` | channels + RPC |
| Subscription | `/products/subscription` | timers |
| Parent–child | `/patterns/parent-child` | sync `Client` inside `Step.execute` |
| Interruptible | `/patterns/interruptible` | interrupt RPC |

Defaults: Worker `127.0.0.1:8804`, HTTP `127.0.0.1:8081` (does not clash with
the async app on `8803` / `8080`).

## Run

From `examples/python` (shared `uv` project):

```bash
dexcli dev
# Indexed Attributes — same as the async README

uv sync --locked
uv run --frozen python sync-python/main.py
```

Env overrides: `DEX_FLOW_SERVICE_ADDRESS`, `DEX_SYNC_WORKER_BIND_ADDRESS`,
`DEX_SYNC_WORKER_TARGET`, `DEX_SYNC_EXAMPLES_HTTP_ADDRESS`,
`DEX_SYNC_BLOB_CACHE_DIR`, `DEX_EXAMPLES_WORKER_MODE` (`push` by default, or
`pull` to connect to Dex as a pull Worker and ignore the Worker bind address
and target).

## Verify

```bash
# from examples/python, with Dex running
DEX_FLOW_SERVICE_ADDRESS=127.0.0.1:8801 \
  uv run --frozen pytest sync-python/tests/integ -v
```

Or run the full e2e suite (`make e2eTests`), which includes these sync integ tests.
