---
name: dex-connector-contributor
description: Thin bootstrap for creating or modifying official Superdurable Dex connectors, operations, Triggers, and configuration UI units in superdurable/dex-connectors-library. Use for standalone official connector-library work and when dex-app-builder routes a confirmed connector gap to it. Do not use as the primary workflow for an application that only consumes released connectors.
---

# Dex Connector Contributor

Enter the correct official Connector library checkout, load its current rules,
and let that repository own the contribution workflow.

The host-neutral request template is:

```text
Add <XYZ> to Dex official connector library
```

## Session start

Before the first substantive Dex-related response, follow the shared
[Dex Skills version check](../dex-sdk/references/core/plugin-version-check.md).
Prefer the lifecycle hook status; run the Skill fallback only when that status
is `unavailable` or absent. Run it only once and never delay or block the task.

## Scope

Use this skill only for changes to the official
`superdurable/dex-connectors-library`. Route standalone Dex SDK work to the
sibling [Dex SDK skill](../dex-sdk/SKILL.md). A product or application that
only consumes a released connector remains in `dex-app-builder`.

## Resolve the checkout

Do not infer the target repository from the current directory or a folder name.
Verify it through Git remote identity as the official repository or a GitHub
fork whose parent is the official repository. Prefer a clean existing checkout
that no other task uses; otherwise create an isolated checkout without changing
or repointing an in-use clone.

The default contribution path uses the user's verified GitHub fork as `origin`
and the official repository as `upstream`. Do not guess the user's GitHub
identity. Obtain explicit authorization before creating a fork, changing a
remote, pushing a branch, or opening a pull request unless the user has already
requested that exact external action. Use an official maintainer branch only
when push access is verified and the user explicitly chooses it.

Before implementation, report the exact checkout path and the remote identity
that established it.

## Hand off to repository authority

After entering the checkout:

1. Read its `AGENTS.md` and any host-native repository instructions completely.
2. Read the relevant files under `docs/` and the owning connector or SDK module
   README selected by those instructions.
3. When the change touches Dex application semantics, read the sibling
   [Dex SDK skill](../dex-sdk/SKILL.md) completely and follow its progressive
   disclosure routing for the required Core references and the
   [Go handbook](../dex-sdk/references/go/go.md).
   For Dex writes followed by reads or completion checks, include the shared
   [read-after-write matrix](../dex-sdk/references/core/read-after-write.md).
4. Treat the checkout's current rules, commands, examples, acceptance criteria,
   and pull-request template as the sole connector-authoring authority.

Do not substitute instructions copied into this Skill for the target
repository's current guidance. If the checkout or its required instructions
cannot be verified, stop before connector implementation and report the
blocker.
