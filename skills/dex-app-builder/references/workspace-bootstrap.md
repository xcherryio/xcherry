# Workspace, stack, and local tooling

Inspect the repository before selecting a language, creating manifests, or
installing dependencies. Treat a repository as effectively empty when it has no
application source or build manifests, even if it contains a README, license,
editor configuration, or other project placeholders.

The installed Dex Skills release loaded by the current coding-agent host is the
only skill authority. Never assume a fixed skill filesystem path, and never add
a skill submodule or a floating skill branch to the application repository.

## Existing application

When the repository already contains an application and build manifests,
preserve its intentional structure, Go module, Dex SDK requirement, `dexcli`
expectation, lockfiles, generators, and commands. Reuse its declared build,
generation, and run commands instead of inventing parallel ones. Change a
dependency only when the user requests it or a recorded compatibility rule
requires it, and explain the verification consequence first.

If the existing backend is not Go, App Builder's FDG 2.0, Dex Web v2 management
metadata, and application Connector SDK integration are unavailable. Stop before
implementation and ask whether to add a Go backend or continue as an explicitly
requested standalone `dex-sdk` project without those capabilities.

## Effectively empty repository

Create a Go application that depends only on open-source Dex:

1. Install `dexcli` with the supported package for the platform (on macOS,
   `brew install superdurable/tap/dexcli`) and run `dexcli version`.
2. Create the Go module and add the Dex Go SDK at an exact released version,
   for example `go get github.com/superdurable/dex/sdk-go@v1.5.0`. Select the
   newest stable SDK release that pairs with the installed `dexcli` and the
   target Dex Server; do not copy versions from a neighboring project or
   resolve a branch, pseudo-version, or commit.
3. Follow the [Dex SDK getting-started path](../../dex-sdk/references/core/getting-started.md)
   and the [Go handbook](../../dex-sdk/references/go/go.md) for the registry,
   Worker, Client, and first vertical slice. Copy API shapes from the
   version-matched runnable examples, not from memory.
4. Keep each Flow in its own source file named for its domain, a registry
   that composes every Flow, and one Worker entry point. Add an application HTTP
   server only for a confirmed custom UI or integration ingress, and a frontend
   directory only for a confirmed custom UI.
5. Record the project's own commands for FDG rendering, code generation,
   checks, builds, and local runs in its README or build file so later work
   reuses them.

In the first implementation update, name the selected Dex Go SDK and `dexcli`
releases. If a release cannot be resolved or verified, report that blocker; do
not fall back to an unreleased dependency.

TypeScript is limited to an optional frontend and its generated client. Do not
start a TypeScript, Node, or other non-Go Dex backend for an App Builder
product. Route an explicitly requested non-Go Dex application through
`dex-sdk`; do not imply that Go-only Dex Web v2 metadata or Connector
capabilities remain available.

## Version pairing

Project scripts and FDG rendering call the `dexcli` on `PATH`; run
`dexcli version` first. The FDG analyzer names Flow and Step types the same way
as the paired Go SDK: Go SDK v1.5.0 and later register names without the Go
package, and `dexcli` v1.5.0 and later generate matching names. Pair a Go SDK
before v1.5.0 with a `dexcli` release before v1.5.0, or the rendered graph will
not match the Worker; see [default type names](../../dex-sdk/references/go/versioning.md#default-flow-and-step-type-names).
When upgrading the global CLI would break other projects, install a
project-local `dexcli` and put its directory first on `PATH` for this project.

Before connecting a Worker to a non-local Dex Server, run
`dexcli version check --server ADDRESS` against that Server. It reports the
highest common protocol and exits non-zero for a nonoverlapping interval.

`DEX_BASELINE`, `DEX_SERVER_BASELINE`, and `DEX_CLI_BASELINE` in this skill
repository record the releases this guidance was checked against. They do not
authorize upgrading an existing application's pinned dependencies.
