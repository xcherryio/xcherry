# Dex Skills Repository Instructions

This repository publishes one `superdurable-dex` plugin for Codex, Claude Code,
and Cursor. Its only public skills are `dex-sdk`, `dex-app-builder`, and
`dex-connector-contributor`, stored directly at the repository root.

## Skill boundaries

- Public Skills contain reusable decision rules and platform guidance, not
  application-specific scenarios, product names, or named regression examples.
  When a real application exposes a modeling failure, extract the general
  decision criteria and validate those invariants generically. Keep
  application-specific architecture and regression fixtures in that
  application's repository or its issue/PR context, not in the published
  Skills.
- All three Skills may be selected by matching natural-language descriptions.
- `dex-app-builder` is the default end-to-end product workflow for business
  discovery, Flow and FDG modeling, an explicit no-custom-UI or custom-UI
  decision, Go backend implementation, and local verification with open-source
  Dex tooling.
- `dex-sdk` is the specialist for implementing, debugging, testing, and
  operating applications through the public Dex SDK.
- `dex-connector-contributor` is the thin bootstrap for official
  connector-library work. It resolves the correct checkout, then defers to that
  repository's agent rules and documentation.
- If App Builder receives a standalone SDK or official connector-library
  request, route it to the matching specialist before product discovery. A
  product or application that uses a released connector remains in App Builder.
- If the user explicitly invokes a specialist, honor that choice and do not
  expand it into the App Builder workflow.
- During backend implementation, `dex-app-builder` loads `dex-sdk` and follows
  only its Core and Go guidance. Do not duplicate those references.
- `dex-connector-contributor` loads `dex-sdk` Core and Go guidance when needed.
  The target connector repository owns connector-specific implementation,
  acceptance, release, and pull-request rules; `dex-app-builder` owns
  application use of released connectors.
- App Builder constraints are stricter: Go only, strict FDG 2.0, provider effects
  only in `Execute`, and no provider or Dex mutations in `WaitFor`.

## Open-source product boundary

Dex Skills is open-source Dex material and contains no information about any
product built on Dex (no product names, template contracts or platform
concepts). Describe only open-source Dex: the Dex SDKs, `dexcli` including
`dexcli dev`, Dex Server, Dex Web, the FDG analyzer, and the official
connectors. A product built on Dex keeps its own platform-specific guidance in
its own repository; nothing here refers to it.

## Dex concept boundary

Skills describe Dex concepts and guarantees only. Never mention
Continue-As-New in any skill file, including its threshold and trigger. Do not
describe Temporal or Cadence internals (activity timeout names, Signals,
Queries, Updates, workflow history mechanics) in application guidance; state the
Dex behavior and guarantee instead. When a guarantee differs by backend, say so
in Dex terms without explaining the backend mechanism. Deployment and operations
guidance may name the backend and its configuration, such as a Temporal
namespace or Cadence domain, Temporal Cloud index provisioning, local SQLite
index slots, and the codec server for raw payloads.

## Packaging and release

Keep Codex, Claude Code, and Cursor manifests synchronized on plugin ID,
version, repository, skill paths, and display metadata. Do not reintroduce a
`plugins/` wrapper or an unplanned public skill.

The Plugin and standalone Skills are mutually exclusive installation paths.
Standalone installation always copies all three Skills together. Every local
Markdown link must resolve using only those three copied directories; never
depend on a repository-root file that an Agent Skills installer omits.

Never add a skill submodule or a floating branch reference to an application
repository; the coding-agent host supplies the installed Dex Skills release.

Every skill change updates `VERSION` and `CHANGELOG.md`. Validate Dex SDK source
excerpts against `DEX_BASELINE`, and validate Server and CLI guidance against
`DEX_SERVER_BASELINE` and `DEX_CLI_BASELINE`. Dex Web is embedded in the Server
and CLI releases.
