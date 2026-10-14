# Dex Skills

Dex Skills is the official agent-skill monorepo for building with
[Superdurable Dex](https://docs.superdurable.io). One `superdurable-dex` plugin
ships three skills for Codex, Claude Code, and Cursor:

The plugin is displayed as **Dex**. `Super Durable` remains the marketplace
publisher, while the stable plugin ID remains `superdurable-dex`.

| Skill | Responsibility |
| --- | --- |
| `dex-app-builder` | Discover the business process, model Flows and FDG, implement a Go backend and UI, and verify locally with open-source Dex. |
| `dex-sdk` | Implement, debug, test, and operate Dex applications in Python, Go, Java, TypeScript, or Rust. |
| `dex-connector-contributor` | Create, verify, and upstream official connector operations, Triggers, and configuration UI units. |

Start with `dex-app-builder` for Dex product, application, and process work.
The three Skills can each be selected from a matching natural-language request;
their descriptions keep standalone SDK and official connector-library work in
the appropriate specialist. Install the complete plugin so all three skills
and their sibling links are present.

## Requires a coding workspace

Dex Skills is a developer plugin, not a hosted application builder. To create
or modify an application, use it in Codex or another coding-agent host that has
a writable repository or project workspace plus file-editing and command tools.
The workspace can be an existing repository or a new empty repository.

Without that workspace, the skills can still help with business discovery,
architecture, and an implementation handoff. They cannot create project files,
run tests, build artifacts, or deploy the application from an ordinary chat.

## Install

Choose exactly one installation path:

1. **Plugin (recommended):** installs all three Skills together and uses the
   host's marketplace lifecycle.
2. **Standalone Skills:** installs all three Skills directly with the Agent
   Skills CLI.

Do not install both. Duplicate copies can shadow one another and make the
loaded version ambiguous.

The invocation syntax depends on both the host and the installation path.

### Codex desktop app

Open the plugin browser, choose **Add a marketplace**, and use:

| Field | Value |
| --- | --- |
| Source | `https://github.com/superdurable/dex-skills` |
| Git ref | `main` |
| Sparse paths | Leave empty. |

Add the marketplace, open **Super Durable**, and install **Dex**. This route
does not require the separate Codex CLI.

To use the Plugin, type `@`, select **Dex** from the picker, then enter a
natural-language request. The selected Plugin routes the request using the
three Skill descriptions. Typing the plain text `@Dex` without selecting the
picker item does not create the Plugin binding.

### Codex CLI

Only use this route when `codex --version` works in the shell:

```bash
codex plugin marketplace add superdurable/dex-skills
codex plugin add superdurable-dex@superdurable
```

Open an existing repository or create an empty one, then start a new task.
Type `@`, select **Dex**, and enter the request. For example:

```text
Add <XYZ> to Dex official connector library
```

Raw `@Dex` text in the initial `codex` prompt or `codex exec` does not create
the structured Plugin binding. Codex may route an installed Plugin from a
matching natural-language request, but scripts that require deterministic
explicit invocation should use the standalone Skills path below.

### Claude web and desktop

Open **Customize → Plugins → Add → Add marketplace**, choose **Add from a
repository**, and enter `https://github.com/superdurable/dex-skills`. Install
**Dex** from the **Super Durable** marketplace. Claude saves the installation
to the account, so it is also available in Claude Code when signed in with the
same account on Claude Code 2.1.273 or later. On older Claude Code versions,
use the manual CLI installation below.

Claude web and desktop use natural-language routing or the `/` picker. This
repository does not distribute standalone ZIP Skills for those surfaces.

### Claude Code CLI

Run from a shell where the `claude` command is installed:

```bash
claude plugin marketplace add superdurable/dex-skills
claude plugin install superdurable-dex@superdurable
```

Or run the slash-command equivalents inside an interactive Claude Code session:

```text
/plugin marketplace add superdurable/dex-skills
/plugin install superdurable-dex@superdurable
```

Invoke `/superdurable-dex:dex-sdk`, `/superdurable-dex:dex-app-builder`, or
`/superdurable-dex:dex-connector-contributor`. For example:

```text
/superdurable-dex:dex-connector-contributor Add <XYZ> to Dex official connector library
```

### Cursor desktop app

Open a project, then use **Customize → Plugins → Add → From GitHub Repository**
and enter `github.com/superdurable/dex-skills`. Open **Dex**, select
**Install**, and choose a user or project scope. The three Skills appear as
`/dex-sdk`, `/dex-app-builder`, and `/dex-connector-contributor`.

### Cursor CLI

Cursor's official documentation describes marketplace installation through
**Customize** and does not provide a non-interactive shell command for
installing this GitHub marketplace. A user-scope desktop installation is
available in Cursor CLI. Alternatively, start `cursor-agent`, enter `/plugin`,
open the Marketplace tab, and install **Dex**. Do not guess a
`cursor plugin install` command.

### Standalone Skills

Use this path instead of the Plugin. Install all three Skills together for the
selected coding agent:

```bash
npx skills add superdurable/dex-skills --skill '*' --agent codex --global
npx skills add superdurable/dex-skills --skill '*' --agent claude-code --global
npx skills add superdurable/dex-skills --skill '*' --agent cursor --global
```

Invoke the standalone connector contributor with the host's Skill syntax:

| Host | Invocation |
| --- | --- |
| Codex | `$dex-connector-contributor Add <XYZ> to Dex official connector library` |
| Claude Code | `/dex-connector-contributor Add <XYZ> to Dex official connector library` |
| Cursor | `/dex-connector-contributor Add <XYZ> to Dex official connector library` |

Cursor uses the same short slash command for Plugin and standalone installs;
the installation source differs even though the invocation text does not.

The standalone bundle is for coding-agent hosts. Use the Plugin path for
Claude web and desktop.

## Upgrade

An active chat keeps the Plugin or Skills version it loaded when the chat
started. After an upgrade, restart the client when applicable and begin a new
chat in the repository.

### Standalone Skills

Update all three Skills together:

```bash
npx skills update dex-app-builder dex-sdk dex-connector-contributor --global
```

If the installed `skills` CLI does not support `update`, rerun the exact
host-specific `npx skills add ... --skill '*' --agent ... --global` command
from the installation section.

### Automatic future-version reminder

Versions `0.25.4` and earlier do not contain the lifecycle hook, so they cannot
reliably discover `0.25.5`. Upgrade to `0.25.5` manually once. Starting with
`0.25.5`, each new session checks GitHub's latest formal release and can remind
you about a future `0.25.6` or later release.

Codex and Claude Code run the check at session start. Cursor warms the cache
when a workspace opens and reads it when a session starts. A result is cached
for 15 minutes. When the cache has expired, the GitHub request waits at most
about one second and uses ETag revalidation. Until you upgrade, every new Dex
chat can include the reminder; unrelated chats remain silent. Restoring an old
chat or compacting its context does not repeat it.

The Plugin hook reads the installed `VERSION` and requests only public release
metadata from GitHub. It does not read or upload the prompt, repository files,
credentials, or project content. It only suggests an upgrade; it never changes
or upgrades the Plugin automatically. Standalone Skills use the same
best-effort version comparison from their packaged reference, but they have no
Plugin lifecycle hook and are never updated automatically by Dex.

Codex asks you to review and trust the hook when a hook-enabled release is first
enabled. Because `0.25.6` adds clearer review metadata, upgrading from `0.25.5`
requires one more review; later hook-definition changes may do the same. The
review description states that the hook reads public GitHub release metadata
only and never installs updates. Claude Code and Cursor use their own plugin
hooks; disable the hook in the client's hook settings, or disable the plugin,
to turn it off. If hooks are disabled, untrusted, blocked by enterprise policy,
missing a Node runtime, or unsupported in the current cloud environment, the
loaded Dex skill falls back to one best-effort release check. All hook and
fallback failures are silent and never block the requested work.

### Codex desktop app

Open the plugin browser's marketplace management view, find **Super Durable**,
and select **Upgrade**. Wait for the upgrade to finish, restart the desktop app,
and start a new task. No Codex CLI command is required for this route.

### Codex CLI

Refresh only the Super Durable marketplace, then start a new Codex session:

```bash
codex plugin marketplace upgrade superdurable
codex
```

Inside Codex CLI, open `/plugins` and confirm that **Dex** is enabled.

### Claude Code CLI

Update Dex directly from a shell:

```bash
claude plugin update superdurable-dex@superdurable
```

Or update the marketplace inside an interactive Claude Code session:

```text
/plugin marketplace update superdurable
```

Start a new session after a manual update. To receive future releases
automatically, open `/plugin`, select **Marketplaces → Super Durable**, and
select **Enable auto-update**.

### Claude web and desktop

Claude's public web and desktop documentation does not describe a separate
per-user upgrade command. Organization-managed GitHub marketplaces are updated
by an owner from **Organization settings → Plugins & skills → Marketplaces →
Update**; account users receive the synced version. The Claude Code commands
above are the documented manual route for a personally registered marketplace.

### Cursor

For a GitHub-imported team marketplace, an administrator can click **Refresh**
or enable **Auto Refresh**, then users reload the Cursor window. Cursor does not
document a personal `cursor plugin update` shell command for a direct GitHub
import; manage that installation through **Customize** instead of guessing one.

## Migrating from the old plugin

The former Plugin ID `dex` and Skill name `dex-developer` were replaced by
`superdurable-dex` and `dex-sdk`. Remove or update the old marketplace install,
choose either the current Plugin or standalone bundle, and start a new task so
the new Skill names are discovered.

A direct skill install is not replaced by the plugin. If
`~/.claude/skills/dex-developer`, another client's skills-directory copy, or an
`npx skills` install of `dex-developer` exists, it keeps loading its older
pinned Server and CLI guidance and can shadow the current installation. Remove
it with the tool that installed it, install exactly one current distribution,
and start a new task.

## Dex SDK

`dex-sdk` uses progressive disclosure. Shared semantics live under
`dex-sdk/references/core/`, and each supported language has a matching handbook.
The skill inspects the project's installed SDK and version-matched runnable
sources before choosing exact APIs.

Exact source excerpts are pinned to the Dex release in `DEX_BASELINE`. CI checks
that every marked snippet remains a contiguous excerpt of its declared source.
The project dependency and lockfile remain authoritative when versions differ.

## Dex App Builder

`dex-app-builder` builds a Dex application with open-source Dex only: the Dex
Go SDK, `dexcli` (including `dexcli dev` and the FDG analyzer), Dex Server,
the Dex Web embedded in Server and CLI, and released official connectors.

It starts with business discovery: process maintainers, managers, terminal
users, permissions, triggers, actions, waits, approvals, recovery, and audit
requirements. It models Flows, their primitives, and their Flow Definition
Graph (FDG) before code, and maps management needs to Dex Web v2 before
confirming **No custom UI** or **Custom UI**. Runs and Indexed Attributes cover
list and search, Summary RPCs provide list fields, Display RPCs provide details
and editable scalars, Action RPC metadata provides forms and permission-gated
operations, Work Queue discovers actionable Runs, and timeline/graph views show
progress and failures. A request for an admin backend is not itself a reason
for Custom UI; discovery must record a specific Dex Web v2 capability gap.

The backend is Go-only and must satisfy strict Dex Web v2 / FDG 2.0 rendering.
An existing application keeps its pinned Go, Dex SDK, `dexcli`, and frontend
versions. An effectively empty repository becomes a Go module on an exact
released Dex Go SDK paired with the installed `dexcli`. App Builder does not
invent a TypeScript backend or borrow SDK setup from a neighboring project.
The `DEX_BASELINE`, `DEX_SERVER_BASELINE`, and `DEX_CLI_BASELINE` files record
the releases the guidance was checked against; they do not authorize upgrading
an application.

No custom UI uses Dex Web v2 for every management interaction, so the
application ships only its Go Worker plus any confirmed integration ingress.
For a custom frontend, App Builder first builds only low-fidelity static pages
when an interaction choice is unresolved, then designs the Flow, Connector
boundaries, and application API contract together, generates the Go server
interfaces and TypeScript client, implements the Go backend, wires the UI
through the generated client after the real Dex and Connector paths run, and
adds visual polish only after real E2E passes. The application does not ship a
mock backend or mock controls.

Connector integrations reuse released dedicated connectors from
`superdurable/dex-connectors-library`. Selection starts from the canonical
published [`catalog.yaml`](https://superdurable.github.io/dex-connectors-library/catalog.yaml),
then verifies every exact Trigger, Query, Mutation, or UI capability against the
selected component tag's immutable `connector.yaml`. If the catalog or release
manifest cannot be verified, App Builder stops connector-dependent
implementation instead of guessing from memory or falling back to a provider
SDK. Query/Mutation factories become Connector Steps, Triggers start typed
Flows or invoke typed RPCs, and RPC-requested provider work moves to a
Connector Step. A missing or defective public connector capability routes to
`dex-connector-contributor`; the application can test the local connector
through an uncommitted Go `replace` while its upstream PR is reviewed, then
must pin the exact release. Generic HTTP is reserved for controlled internal
systems.

Local verification runs on one long-lived `dexcli dev` stack started as soon as
the first Flow renders. Every Flow is rendered with
`dexcli visualize --schema-version 2.0 --json` into the stack's
`--flow-rendering-dir`, and the user receives the Dex Web URL right away. Dex
Web configures released connectors in its local **Connectors** view; the
default plaintext development store is `~/.dex/connectors/connections.json`,
and Dex Web shows the **DEX_CONNECTOR_CONFIG_FILE** launch command for an
application that reads it. The same validated definitions can be packaged for a deployed
Dex Web, which loads them from a definition directory.

UI test doubles validate component behavior only; they cannot prove Dex
durability, Worker replacement, Timer, RPC, Connector, or application E2E
semantics. The workflow finishes with checked source, the rendered FDG result,
real local E2E evidence or explicit gaps, and a clean reviewable commit. A
deployment claim must include observed identities rather than a mock or
image-only build.

## Dex Connector Contributor

`dex-connector-contributor` is limited to official work in
`superdurable/dex-connectors-library`. It is deliberately thin: it verifies the
official repository or the user's fork, resolves a safe checkout, and then
loads that checkout's `AGENTS.md`, documentation, module README, acceptance
criteria, and pull-request template. Those repository-local sources own
manifest authoring, generated contracts, configuration guidance, provider
behavior, examples, testing, release ordering, and upstream contribution
details.

The bootstrap loads `dex-sdk` Core and Go guidance when the connector change
touches Dex application semantics. It does not carry a snapshot of connector
implementation rules, so a contribution always follows the target checkout's
current contract.

## Releases

The three skills and all client manifests share the version in `VERSION`. A merge
to `main` creates GitHub Release `v<version>` when that tag does not already
exist. See [CONTRIBUTING.md](CONTRIBUTING.md) for validation and release rules.

## License

[MIT](LICENSE)
