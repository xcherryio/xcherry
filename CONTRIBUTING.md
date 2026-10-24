# Contributing

## Repository layout

| Path | Contents |
|------|----------|
| [server/](server/) | Dex server (Temporal/Cadence backend) |
| [protos/](protos/) | Protobuf IDL ([`dex.proto`](protos/dex.proto); renames in [`docs/design/idl-renames.md`](docs/design/idl-renames.md)) |
| [docs/](docs/) | Product docs site ([docs.superdurable.io](https://docs.superdurable.io)); design notes in [`design/`](docs/design/); start at [README.md](docs/README.md) |
| [cli/](cli/) | `dexcli` local development environment |
| [web/](web/) | Dex Web console |
| [sdk-go/](sdk-go/) | Go SDK |
| [blob-cache-go/](blob-cache-go/) | Shared Go disk blob cache |
| [examples/go/](examples/go/) | Go examples |
| [sdk-java/](sdk-java/) | Java SDK |
| [examples/java/](examples/java/) | Java examples |
| [sdk-python/](sdk-python/) | Python SDK |
| [examples/python/](examples/python/) | Python examples |
| [sdk-rust/](sdk-rust/) | Shared Rust SDK Core |
| [examples/rust/](examples/rust/) | Rust examples |
| [sdk-typescript/](sdk-typescript/) | TypeScript SDK |
| [examples/typescript/](examples/typescript/) | TypeScript examples |
| [examples/playground/](examples/playground/) | Shared examples playground |
| [s3mock/](s3mock/) | S3-compatible object store on local disk for development and tests |

Go SDK + samples use root [`go.work`](go.work). `blob-cache-go` remains outside the workspace so consumers resolve released versions. Build the server separately (`cd server && go build ./...`) to avoid a Cadence/Temporal `genproto` workspace conflict.

## Prerequisites

- Go 1.24+ (see `server/go.mod`; root `go.work` pins the workspace)
- JDK 17+ and Gradle wrapper (for `sdk-java` / `examples/java`; test JDKs
  are Gradle toolchains downloaded automatically)
- Python 3.9+ and uv (for `sdk-python` / `examples/python`)
- Docker (for integration tests / local Temporal+Cadence stacks)

Install git hooks once per clone with `make githooks`. They reject Cursor
co-author trailers and inline backticks in product-doc prose. Do not pass
`--no-verify`.

## Go workspace

Root [`go.work`](go.work) includes `sdk-go` and `examples/go` (local `replace` for the SDK).

```bash
go work sync
go build ./sdk-go/...
go build ./examples/go/...
```

### Server

Build outside the workspace (or with `GOWORK=off`) so Cadence’s older `genproto` does not clash with Temporal’s split modules:

```bash
cd server && go build ./...
make -C server unitTests
# Integration tests need Cadence/Temporal; see server/CONTRIBUTING.md
```

### Go SDK

```bash
make -C sdk-go ci-tests
make -C sdk-go e2eTests  # starts an isolated dexcli dev environment
```

### Blob Cache Go

```bash
make -C blob-cache-go tests
```

### s3mock

```bash
make -C s3mock test  # unit tests and the conformance suite against an in-process s3mock
make -C s3mock run ARGS="--data-directory /tmp/s3mock-data"
```

`S3MOCK_CONFORMANCE_TARGET=aws make -C s3mock test` runs the same conformance suite against real S3
with the default AWS credential chain.

### Dex CLI and Web

```bash
make -C cli build
make -C cli test
make -C cli integration-test
```

The CLI integration suite requires Temporal CLI. Web contributors can run
`cli/dexcli dev --web-port 8902` and `npm run dev` in `web/` for hot reload.

## IDL (`protos/`)

Protobuf source lives in [`protos/dex.proto`](protos/dex.proto). Rename catalog: [`docs/design/idl-renames.md`](docs/design/idl-renames.md).

Server-only regen (leave SDK trees alone during the server rewrite):

```bash
make -C protos proto-server-go
# or: make -C server idl-code-gen-server
```

Full regen (server + all SDKs):

```bash
make -C protos proto
# or: make -C server idl-code-gen
```

Interpreter Temporal/Cadence history encoding uses binary protobuf DataConverters
in `server/service/common/converter` (see `server/CONTRIBUTING.md`).

## Java

```bash
cd sdk-java && ./gradlew build
cd ../examples/java && ./gradlew build
```

## Python

```bash
cd sdk-python && uv sync --locked && uv run --frozen pytest
cd ../examples/python && uv sync --locked
```

## License headers

Managed source files use the classifications recorded in
[`script/licenseheaders/legacy-manifest.json`](script/licenseheaders/legacy-manifest.json):

- `legacy-only` preserves the original license while its normalized body still
  matches the cutoff snapshot.
- `mixed` preserves the original header and adds the Super Durable modification
  notice.
- `new` uses `LicenseRef-Sustainable-Use-1.0`.

`docs/` is not relicensed. Go examples retain MIT; Java and Python examples
retain Apache-2.0. See [LICENSING.md](LICENSING.md) for the repository policy.

From the repo root:

```bash
make copyright         # add missing headers
make copyright-check   # verify classifications, body hashes, and headers
```

Skip generated trees (`**/gen/**`, `*.pb.go`, `*_pb.go`, `*.gen.*`). Prefer
`make copyright` over hand-copying when adding files. It upgrades a modified
`legacy-only` file to `mixed` without replacing its legacy notice. CI runs
`make copyright-check` via [`.github/workflows/copyright-ci.yml`](.github/workflows/copyright-ci.yml).

Files absent from the cutoff manifest use the new header. File-level renames
and copies with at least 20% similarity retain source lineage; non-new copies
use the mixed header. Preserve applicable notices when copying smaller excerpts.

## Contributor License Agreement

All contributors must sign an [individual or corporate CLA](CLA.md). Email the
signed agreement and GitHub handle to licensing@superdurable.io. Pull
requests from handles absent from `.github/cla-signatures.json` fail the CLA
check until the record is updated.

## Skills mirror

Contribute skills in [`skills/`](skills/) in this repository. Its CI runs the
skills validators. A merge to Dex main syncs that directory to the read-only
[`superdurable/dex-skills`](https://github.com/superdurable/dex-skills) mirror;
the mirror publishes releases when `VERSION` changes. If the sync fails its
drift check, compare the mirror HEAD tree with the last synced Dex commit's
`skills/` tree (from the mirror commit's `Dex-Commit` trailer). Restore the
mirror to that tree, then rerun the sync; do not overwrite unexplained edits.

## CI

Root workflows under [`.github/workflows/`](.github/workflows/) run path-filtered jobs for server and each SDK/samples tree, plus the copyright check. Prefer fixing those over re-adding nested `*/.github/workflows` duplicates.

CI jobs use GitHub-hosted runners for pushes, pull requests, and manual runs. Linux jobs use `ubuntu-latest`; the Rust SDK test matrix also covers `macos-latest` and `windows-latest`. Run `make ci-runner-check` before submitting workflow changes.

To run every CI workflow against the latest `main` commit, dispatch **Main CI (all)** in GitHub Actions or run `gh workflow run main-ci-all.yml --ref main`. The dispatcher starts every non-release CI workflow, then starts Check Gate to report their aggregate result.

Check Gate CI is the only required check. It waits for every workflow whose paths a pull request touches. A pull request with the `ci:fast-merge` label does not wait for the Temporal/Cadence server, SDK, CLI, Web, and examples suites; they still run on the pull request and again on `main`, and a failure on `main` must be fixed forward right away. Generated Code CI, Server ZD CI, S3mock CI, Rust Rules CI, Copyright, CLA, Skills CI, and Release Tooling CI still block. Use the label only for changes that cannot affect the existing engine's behavior, such as additive protocol fields or test runner support for another engine.

## Releases (monorepo tags)

Each component has its own version and tag prefix. Create a GitHub Release for that tag only — workflows filter on the prefix so one release does not publish another component.

For coordinated releases, run **Release changed components** from the **main** branch and enter one semantic version. The workflow applies that version to every selected component. It compares each component with its own latest reachable release tag, uses that tag as the start of the component's generated release notes, preflights every selected target tag, creates the GitHub Releases, and directly invokes each publisher. Every generated release starts with a **Breaking Changes** section containing `None.`. The CLI release workflow adds that section to manually created CLI Releases when it is absent. Release owners may replace `None.` with the actual breaking changes. The run succeeds only after every selected registry, Docker, CLI asset, and Homebrew publication succeeds. Go SDK publication is complete when its module tag and GitHub Release exist.

Coordinated releases do not create a cross-component compatibility manifest.
Each selected component publishes its native immutable artifacts. CLI publishes
`checksums.txt`, Server publishes its versioned image, and SDK registries retain
their normal package checksums. Applications update explicit component versions
in ordinary pull requests and use compilation and integration tests as the
compatibility gate.

Components without relevant changes are skipped. A run with no relevant changes succeeds without creating tags. Documentation and workflow changes alone do not select a product release. A missing component baseline is treated as its first release.

The dependency-aware paths are:

| Component | Relevant paths |
|-----------|----------------|
| Go SDK | `sdk-go/**` |
| Rust SDK | `sdk-rust/**` |
| Java SDK | `sdk-java/**`, shared Rust manifests and Blob Cache core, and `dex-blob-cache-jni/**` |
| Python SDK | `sdk-python/**`, shared Rust manifests and Blob Cache core, and `dex-blob-cache-python/**` |
| TypeScript SDK | `sdk-typescript/**`, shared Rust manifests and Blob Cache core, and `dex-blob-cache-node/**` |
| Server | `server/**`, `protos/**` |
| CLI | `cli/**`, `web/**`, `packages/flow-definition-renderer/**`, `server/**`, `protos/**`, `go.work` |

Server baselines may use either current `server/vX.Y.Z` tags or historical `server-vX.Y.Z` tags. New server releases always use `server/vX.Y.Z`.

| Component | Tag format | Example | What it publishes |
|-----------|------------|---------|-------------------|
| Server / Docker | `server/vX.Y.Z` | `server/v1.0.0` | Docker Hub `dex-server:v1.0.0` |
| Server ZD / Docker | `server-zd/vX.Y.Z` | `server-zd/v0.1.0` | Docker Hub `dex-server-zd:X.Y.Z` through [`.github/workflows/server-zd-image-release.yml`](.github/workflows/server-zd-image-release.yml) (version from the tag) |
| Python SDK | `sdk-python/vX.Y.Z` | `sdk-python/v0.1.0` | PyPI [`dex-python-sdk`](https://pypi.org/project/dex-python-sdk/) via [`.github/workflows/sdk-python-publish.yml`](.github/workflows/sdk-python-publish.yml) (version from the tag) |
| Java SDK | `sdk-java/vX.Y.Z` | `sdk-java/v0.0.3` | Maven Central `io.superdurable:dex-sdk` via [`.github/workflows/sdk-java-publish.yml`](.github/workflows/sdk-java-publish.yml) (version from the tag) |
| Go SDK | `sdk-go/vX.Y.Z` | `sdk-go/v1.2.3` | Go module tag for `github.com/superdurable/dex/sdk-go` |
| Blob Cache Go | `blob-cache-go/vX.Y.Z` | `blob-cache-go/v0.1.0` | Go module tag for `github.com/superdurable/dex/blob-cache-go` |
| TypeScript SDK | `sdk-typescript/vX.Y.Z` | `sdk-typescript/v0.1.0` | npm [`@superdurable/dex`](https://www.npmjs.com/package/@superdurable/dex) via [`.github/workflows/sdk-typescript-publish.yml`](.github/workflows/sdk-typescript-publish.yml) (version from the tag) |
| Rust SDK | `sdk-rust/vX.Y.Z` | `sdk-rust/v0.1.0` | crates.io `dex-sdk`, `dex-blob-cache`, and `dex-protocol` via [`.github/workflows/sdk-rust-publish.yml`](.github/workflows/sdk-rust-publish.yml) (version from the tag) |
| Dex CLI | `cli-vX.Y.Z` | `cli-v0.1.0` | macOS/Linux archives and Homebrew formula input |

Notes:

- Java, TypeScript, Python, and Rust derive publish versions from tags (CI stamps the package metadata for the build). Bump committed version files when you want the repo tip to match.
- Go modules use path-style tags (`sdk-go/v…`, `blob-cache-go/v…`) so `go get` resolves each subdirectory module.
- Python, Java, and Docker release workflows also support **workflow_dispatch** for manual runs. Python manual runs build without publishing unless `publish` is selected.
- TypeScript publishing requires a GitHub Release after its one-time npm bootstrap publish.
- Publish a manually created Dex CLI Release without marking it latest (`gh release create cli-vX.Y.Z --latest=false`). **Release Dex CLI** uploads the archives and `checksums.txt` minutes after the Release is published, and consumers that resolve the latest stable `cli-v` Release, such as a connector library's compatibility gate, fail until they exist. After that workflow succeeds, mark it latest with `gh release edit cli-vX.Y.Z --latest`.

### Committed `version` vs release tags (Python / TypeScript / Java / Rust)

For the Python, TypeScript, Java, and Rust SDKs, the **GitHub Release tag** is the source of
truth for what gets published (`sdk-python/vX.Y.Z` → PyPI, `sdk-typescript/vX.Y.Z` → npm,
`sdk-java/vX.Y.Z` → Maven Central, `sdk-rust/vX.Y.Z` → crates.io). CI applies that version
for the release build (Python, TypeScript, and Rust stamp package metadata on the runner;
Java passes `-PreleaseVersion`) and does **not** commit the change.

The committed fields still matter; they are not unused:

| Field | Role |
|-------|------|
| [`sdk-python/pyproject.toml`](sdk-python/pyproject.toml) `project.version` | Required package metadata. Used for local / editable installs (`pip` / `uv`), metadata queries, and aligning `uv.lock`. Also used when [`.github/workflows/sdk-python-publish.yml`](.github/workflows/sdk-python-publish.yml) runs on a **pull_request** (no tag): that dry-run builds wheels/sdists using the committed version. |
| [`sdk-typescript/package.json`](sdk-typescript/package.json) `version` | Required package metadata. Used for local installs, `npm pack`, and keeping `package-lock.json` in sync. The TypeScript publish workflow only runs on release tags, so the committed value is not what npm receives unless CI has just rewritten it for that job. |
| [`sdk-java/build.gradle`](sdk-java/build.gradle) default `releaseVersion` | Fallback when `-PreleaseVersion` is omitted (local / SNAPSHOT builds). Maven Central publish always supplies the version from the tag (or workflow_dispatch). |
| [`sdk-rust/Cargo.toml`](sdk-rust/Cargo.toml) `workspace.package.version` | Required Cargo metadata shared by workspace crates. Local builds use it; the publish workflow temporarily stamps it and internal registry dependency requirements from the release tag. |

What the committed value is **not**:

- Not the source of the next PyPI / npm / Maven Central / crates.io release (the tag /
  workflow_dispatch version is).
- Not automatically updated in git when you publish a tag.
- Not what examples pin when they depend on a registry version.

Practical workflow:

1. Publish with a tag (for example `sdk-python/v0.1.0`) — no need to bump the committed
   file first.
2. Follow up with a PR so tip versions match what you shipped:
   - Python: `pyproject.toml` (+ `uv.lock`) and docs install pins
   - TypeScript: `package.json` (+ `package-lock.json`)
   - Java: default `…-SNAPSHOT` in `build.gradle` / smoke-test coords, plus README pins
   - Rust: workspace package and internal dependency versions in `Cargo.toml`, plus `Cargo.lock`

`package.json` cannot carry comments; the TypeScript and Rust SDK README Releases
sections document the same tag-driven rule. `pyproject.toml` and `build.gradle` may
note that publish takes the version from the tag.

## Package-specific guides

- [server/CONTRIBUTING.md](server/CONTRIBUTING.md)
- [sdk-go/CONTRIBUTION.md](sdk-go/CONTRIBUTION.md)
- [sdk-java/README.md](sdk-java/README.md)
- [sdk-python/README.md](sdk-python/README.md)
- [sdk-typescript/README.md](sdk-typescript/README.md)

## Rust rules

Run `make rust-rules-check` from the repository root. It discovers every Cargo
workspace and standalone package, checks structural and dependency conventions,
and runs the Python fixture suite. Each workspace must also pass
`cargo fmt --all --check`, `cargo clippy --all-targets -- -D warnings`, and
`cargo test`. CI runs these checks and the applicable real-server integration suites.

Register new workspaces and crates in `script/rust-crate-layers.json`. Crates may
depend only on crates at the same or a lower layer. The registry also declares
FFI and printing crates; their complete lint tables must match the workspace
except for the explicitly permitted unsafe and printing levels.

`script/rust-rules-baseline.json` records only existing R1/R2 structural debt.
Keys combine the rule and repository-relative path. R1 values are line-count
ceilings. R2 values contain separate `files` and `submodules` ceilings, each
at least the normal limit of 10; the baseline cannot hide a new violation
in another dimension. Never add entries or increase ceilings.
The checker compares the baseline with origin/main when that baseline exists.
CI fetches full history for this comparison. Remove an entry when its file or
directory meets the limit; fixed entries left in the baseline fail. Lower a
ceiling when reducing existing debt. New violations must be fixed immediately.

Use `#[expect(lint, reason = "...")]` for a necessary lint suppression, with a
concrete reason explaining the invariant. Do not use `#[allow]` or weaken a
crate's lint table. Keep each expectation close to the relevant declaration or
expression. Remove it when the underlying lint is no longer emitted; Rust
reports unfulfilled expectations.

```rust
#[expect(unsafe_code, reason = "Java owns this allocation until the native close call.")]
let cache = unsafe { Box::from_raw(handle) };
```

Production code uses `expect("why failure is impossible")` for invariants and
typed errors for untrusted input. Tests may use unwrap. Generated code belongs
to its generator; lint expectations for generated declarations belong at the
include boundary, without editing generated output.
