#!/usr/bin/env python3
# SPDX-License-Identifier: MIT

import argparse
import json
import re
import shutil
import subprocess
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SDK = ROOT / "dex-sdk"
APP_BUILDER = ROOT / "dex-app-builder"
CONNECTOR_CONTRIBUTOR = ROOT / "dex-connector-contributor"
SDK_REFERENCES = SDK / "references"
PLUGIN_VERSION_CHECK = SDK_REFERENCES / "core" / "plugin-version-check.md"
BUNDLE_VERSION = SDK / "VERSION"
BUNDLE_BASELINES = SDK_REFERENCES / "core" / "bundle-baselines.md"
BASELINE_FILES = (
    "DEX_BASELINE",
    "DEX_SERVER_BASELINE",
    "DEX_CLI_BASELINE",
    "DEX_JAVA_BASELINE",
    "DEX_PYTHON_BASELINE",
    "DEX_TYPESCRIPT_BASELINE",
    "DEX_RUST_BASELINE",
)
VERSION_CHECK_SCRIPT = ROOT / "hooks" / "version-check.mjs"
HOOK_CONFIGS = {
    "codex": ROOT / "hooks" / "codex.json",
    "claude": ROOT / "hooks" / "claude.json",
    "cursor": ROOT / "hooks" / "cursor.json",
}
LOGO = ROOT / "assets" / "logo.png"
MANIFESTS = {
    "codex": ROOT / ".codex-plugin" / "plugin.json",
    "claude": ROOT / ".claude-plugin" / "plugin.json",
    "cursor": ROOT / ".cursor-plugin" / "plugin.json",
}
MARKETPLACES = {
    "codex": ROOT / ".agents" / "plugins" / "marketplace.json",
    "claude": ROOT / ".claude-plugin" / "marketplace.json",
    "cursor": ROOT / ".cursor-plugin" / "marketplace.json",
}
CORE_TOPICS = {
    "read-after-write.md",
    "ai-agents.md",
    "bundle-baselines.md",
    "data-handling.md",
    "error-handling.md",
    "getting-started.md",
    "modeling.md",
    "operations.md",
    "patterns.md",
    "plugin-version-check.md",
    "primitives.md",
    "pull-workers.md",
    "step-options.md",
    "testing.md",
    "troubleshooting.md",
    "versioning.md",
}
LANGUAGES = ("python", "go", "java", "typescript", "rust")
LANGUAGE_TOPICS = {
    "advanced-features.md",
    "data-handling.md",
    "error-handling.md",
    "gotchas.md",
    "observability.md",
    "patterns.md",
    "primitives.md",
    "testing.md",
    "versioning.md",
}
APP_BUILDER_REFERENCES = {
    "workspace-bootstrap.md",
    "fdg-authoring.md",
    "business-contract.md",
    "application-surface.md",
    "backend-implementation.md",
    "verification-handoff.md",
    "build-test-handoff.md",
    "connector-architecture.md",
    "dex-web-v2.md",
    "product-discovery.md",
    "ui-workflow.md",
    "workflow-import.md",
    "n8n-semantics.md",
}
APP_BUILDER_SCRIPTS = {
    "n8n_inventory.py",
    "n8n_code_golden.mjs",
    "n8n_expression_golden.mjs",
    "n8n_schedule_golden.mjs",
}
SEMVER = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")
PUBLISHED_RELEASE_TAG = re.compile(
    r"^(?:[a-z0-9][a-z0-9-]*/)?v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$"
)
PUBLISHED_CLI_RELEASE_TAG = re.compile(
    r"^cli-v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$"
)
MARKDOWN_LINK = re.compile(r"\[[^]]+\]\(([^)]+)\)")
FENCED_BLOCK = re.compile(r"```.*?```", re.DOTALL)
SOURCE_MARKER = re.compile(r"<!-- dex-source: ([^\s]+) -->")
FLOATING_SOURCE_LINK = re.compile(
    r"https://github\.com/superdurable/dex/(?:blob|tree)/main/"
)
# Dex Skills documents open-source Dex only; reject a downstream product name.
DOWNSTREAM_PRODUCT_NAME = re.compile(r"super[\s_-]*verse", re.IGNORECASE)


def fail(message: str) -> None:
    raise SystemExit(message)


def load_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        fail(f"invalid JSON in {path.relative_to(ROOT)}: {error}")
    if not isinstance(value, dict):
        fail(f"{path.relative_to(ROOT)} must contain a JSON object")
    return value


def version_tuple(version: str) -> tuple[int, int, int]:
    match = SEMVER.fullmatch(version)
    if match is None:
        fail(f"VERSION must be stable SemVer: {version}")
    return tuple(int(part) for part in match.groups())


def local_link_target(markdown: Path, target: str) -> Path | None:
    if "://" in target or target.startswith(("#", "mailto:")):
        return None
    relative_target = target.split("#", 1)[0].strip("<>")
    if not relative_target:
        return None
    return (markdown.parent / relative_target).resolve()


def check_reachable_links(skill: Path, references: list[Path]) -> None:
    markdown_files = [skill / "SKILL.md", *references]
    graph = {path.resolve(): set() for path in markdown_files}
    for markdown in markdown_files:
        content = markdown.read_text()
        if "[TODO:" in content:
            fail(f"unfinished placeholder in {markdown.relative_to(ROOT)}")
        prose = FENCED_BLOCK.sub("", content)
        for target in MARKDOWN_LINK.findall(prose):
            resolved = local_link_target(markdown, target)
            if resolved is None:
                continue
            if not resolved.exists():
                fail(f"broken link in {markdown.relative_to(ROOT)}: {target}")
            if resolved in graph:
                graph[markdown.resolve()].add(resolved)

    reachable: set[Path] = set()
    pending = [(skill / "SKILL.md").resolve()]
    while pending:
        current = pending.pop()
        if current in reachable:
            continue
        reachable.add(current)
        pending.extend(graph[current] - reachable)
    unreachable = sorted(path for path in references if path.resolve() not in reachable)
    if unreachable:
        rendered = ", ".join(str(path.relative_to(ROOT)) for path in unreachable)
        fail(f"references not reachable from {skill.name}/SKILL.md: {rendered}")


def reference_baseline(source_path: str, sdk_baseline: str) -> str:
    parts = Path(source_path).parts
    prefix = parts[0]
    if prefix == "examples" and len(parts) > 1:
        prefix = f"sdk-{parts[1]}"
    if prefix in {"sdk-java", "sdk-python", "sdk-typescript", "sdk-rust"}:
        return (ROOT / f"DEX_{prefix[4:].upper()}_BASELINE").read_text().strip()
    if prefix in {"server", "web", "protos"}:
        return (ROOT / "DEX_SERVER_BASELINE").read_text().strip()
    if prefix == "cli":
        return (ROOT / "DEX_CLI_BASELINE").read_text().strip()
    return sdk_baseline


def check_sdk(baseline: str) -> None:
    core_files = {path.name for path in (SDK_REFERENCES / "core").glob("*.md")}
    if core_files != CORE_TOPICS:
        fail(f"Core references must be exactly: {', '.join(sorted(CORE_TOPICS))}")

    for language in LANGUAGES:
        language_dir = SDK_REFERENCES / language
        expected = {*LANGUAGE_TOPICS, f"{language}.md"}
        actual = {path.name for path in language_dir.glob("*.md")}
        if actual != expected:
            fail(f"{language} references must be exactly: {', '.join(sorted(expected))}")

    references = sorted(SDK_REFERENCES.rglob("*.md"))
    expected_count = len(CORE_TOPICS) + len(LANGUAGES) * 10
    if len(references) != expected_count:
        fail(f"expected {expected_count} Dex SDK references, found {len(references)}")
    check_reachable_links(SDK, references)

    for markdown in [SDK / "SKILL.md", *references]:
        content = markdown.read_text()
        if FLOATING_SOURCE_LINK.search(content):
            fail(f"floating Dex source link in {markdown.relative_to(ROOT)}")
        for source_path in SOURCE_MARKER.findall(content):
            expected_link = (
                "https://github.com/superdurable/dex/blob/"
                f"{reference_baseline(source_path, baseline)}/{source_path}"
            )
            marker = f"<!-- dex-source: {source_path} -->"
            marker_position = content.index(marker)
            preceding = content[max(0, marker_position - 600):marker_position]
            if expected_link not in preceding:
                fail(
                    f"source marker in {markdown.relative_to(ROOT)} needs visible "
                    f"pinned link: {source_path}"
                )

    skill_content = (SDK / "SKILL.md").read_text()
    for text in (
        "### Flow boundary default",
        "Default a new design to no SubFlows",
        "A separate top-level Flow is not a SubFlow",
        "200-concurrent-Step architecture-review threshold",
        "Code reuse, provider",
        "### Dex-first state ownership",
        "Default durable application state to typed Flow Attributes",
        "ownership-design question",
        "complex/ad-hoc indexes",
        "sustained high-contention",
        "Never create ambiguous dual",
    ):
        if text not in skill_content:
            fail(f"dex-sdk/SKILL.md must contain storage principle: {text}")

    core_modeling = (SDK_REFERENCES / "core" / "modeling.md").read_text()
    for text in (
        "different authoritative owner and retention or cleanup",
        "independent top-level Flow",
        "state must not pollute the authoritative store",
        "## Start with parallel Steps",
        "## Gate SubFlows as an evolution",
        "more than 200 concurrent Step executions",
        "architecture-review threshold",
        "not a Dex Server",
        "the user explicitly confirms the SubFlow design",
    ):
        if text not in core_modeling:
            fail(f"Core modeling must contain Flow-boundary principle: {text}")
    if "Prefer a SubFlow when work needs its own identity" in core_modeling:
        fail("Core modeling must not present SubFlow as an initial-design preference")

    core_patterns = (SDK_REFERENCES / "core" / "patterns.md").read_text()
    for text in (
        "Parallel Steps are the default",
        "Do not introduce SubFlows in an initial design",
        "200-concurrent-Step",
        "obtain explicit user confirmation",
        "does not automatically select a",
        "alone is insufficient",
    ):
        if text not in core_patterns:
            fail(f"Core patterns must contain SubFlow gate: {text}")
    if "Choose SubFlows for independent retry, scaling, ownership, or identity" in core_patterns:
        fail("Core patterns must not allow one-factor SubFlow selection")

    go_patterns = (SDK_REFERENCES / "go" / "patterns.md").read_text()
    for text in (
        "Apply the Core SubFlow evolution gate",
        "do not make SubFlows the",
        "default for new designs or for ordinary parallel work",
    ):
        if text not in go_patterns:
            fail(f"Go patterns must route through the SubFlow gate: {text}")

    core_data_handling = (SDK_REFERENCES / "core" / "data-handling.md").read_text()
    for text in (
        "## Dex-first storage decision",
        "begin with Dex as the durable system of record",
        "Data shared by several processes does not automatically need a database",
        "Do not add a database, cache, ORM, outbox, or shadow read model",
        "One fact must never have two ambiguous authorities",
    ):
        if text not in core_data_handling:
            fail(f"Core data handling must contain storage principle: {text}")

    operations = (SDK_REFERENCES / "core" / "operations.md").read_text()
    for text in (
        "## FDG 2.0 management metadata",
        "generated only by the Go analyzer",
        "Action input capture hints",
    ):
        if text not in operations:
            fail(f"Core operations must contain FDG language boundary: {text}")
    for text in (
        "## Deployed Connector configuration",
        "`sdkgo/projectconfig`",
        "`projectconfig.LoadFromEnvironment(ctx)`",
        "`NewProjectConnection(project, \"connection-name\")`",
        "dex-connectors-library/blob/sdkgo/v0.25.0/sdkgo/projectconfig/README.md",
        "dex-connectors-library/blob/sdkgo/v0.25.0/sdkgo/projectconfig/environment.go",
        "`sharedConnectionId`",
        "`UnmarshalSharedCredential`",
        "`CONNECTION_NOT_SELECTED`",
        "never follows a changed selection",
        "different settings need a different connection name",
        "no equivalent SDK exists today",
    ):
        if text not in operations:
            fail(f"Core operations must describe the Connector SDK deployment path: {text}")

    for language in LANGUAGES:
        handbook = (SDK_REFERENCES / language / f"{language}.md").read_text()
        for text in (
            "## Dex Web v2 management metadata",
            "FDG 2.0 management-interface analyzer",
        ):
            if text not in handbook:
                fail(f"{language} handbook must contain FDG language boundary: {text}")
        for text in (
            "## Deployed configuration and recovery boundary",
            "`sdkgo/projectconfig`",
            "projectconfig.LoadFromEnvironment" if language == "go" else "no equivalent",
        ):
            if text not in handbook:
                fail(f"{language} handbook must contain the Connector SDK deployment path: {text}")


def check_app_builder() -> None:
    references_dir = APP_BUILDER / "references"
    references = sorted(references_dir.rglob("*.md"))
    actual = {path.name for path in references}
    if actual != APP_BUILDER_REFERENCES:
        fail(
            "Dex App Builder references must be exactly: "
            f"{', '.join(sorted(APP_BUILDER_REFERENCES))}"
        )
    if (references_dir / "core").exists() or (references_dir / "go").exists():
        fail("Dex App Builder must not vendor Dex SDK Core or Go references")
    check_reachable_links(APP_BUILDER, references)

    content = (APP_BUILDER / "SKILL.md").read_text()
    if len(content.encode()) > 8192:
        fail("App Builder entrypoint exceeds the bounded 8 KiB discovery budget")
    # Validate the whole reachable package after progressive-disclosure moves,
    # rather than requiring every detailed rule to be repeated in SKILL.md.
    guidance = content + "\n" + "\n".join(path.read_text() for path in references)
    for text in (
        "../dex-sdk/SKILL.md", "../dex-connector-contributor/SKILL.md",
        "writable workspace", "strict FDG 2.0", "dexcli dev",
        "generated operation factory", "management UI capability mapping",
        "Summary RPC", "Display RPC", "Action RPC", "trusted server boundary",
        "public external product", "https://superdurable.github.io/dex-connectors-library/catalog.yaml",
        "no SubFlows", "selective loads", "locks/CAS", "retention",
        "complete RequestID", "explicit reuse policy", "typed error",
        "data-lifecycle and execution-shape boundary matrices",
        "Use Dex Flow state as the default durable application store",
        "Do not add an external database, cache, ORM, outbox, or shadow read model",
        "## Version pairing",
        "go get github.com/superdurable/dex/sdk-go@",
        "Only after the real Dex and Connector end-to-end journey passes",
        "Do not create an application-level mock server",
        "Mock evidence never replaces real Dex durability",
        "Default application authoring produces source without integration/browser test",
        "## Local verification with dexcli dev", "## Package Flow definitions",
        "`web.flowRenderingDirectory`", "## Deployment, only when requested",
        "ports 8801 and 8802 unreachable around that boundary",
        "deployed Connector configuration",
    ):
        if text not in guidance:
            fail(f"Dex App Builder guidance must preserve: {text}")
    if "TEMPLATE_BASELINE" in guidance:
        fail("Dex App Builder guidance must not depend on an application template baseline")
    for name in ("workspace-bootstrap", "business-contract", "application-surface",
                 "backend-implementation", "verification-handoff"):
        if f"references/{name}.md" not in content:
            fail(f"App Builder entrypoint must route to {name}")

    ui_workflow = (references_dir / "ui-workflow.md").read_text()
    ui_stages = (
        "### 1. Resolve material interaction choices",
        "### 2. Contract and backend design",
        "### 3. Integration and durable verification",
        "### 4. Visual polish",
    )
    for text in (
        *ui_stages,
        "no hooks or",
        "application state",
        "Use no images",
        "custom icons, animation, branding",
        "frontend development server",
        "generate the Go server interfaces and TypeScript client",
        "application's business boundary",
        "generated TypeScript client calls",
        "hand-edit generated code",
        "real-dependency test policy",
        "Do not add component mocks",
        "application-level mock API",
        "actual Dex and Connector end-to-end path",
        "Use this only after discovery completes the management UI capability mapping",
        "requires a recorded",
        "Dex Web v2 capability gap",
    ):
        if text not in ui_workflow:
            fail(f"UI workflow must contain: {text}")
    ui_stage_positions = [ui_workflow.index(marker) for marker in ui_stages]
    if ui_stage_positions != sorted(ui_stage_positions):
        fail("Custom UI workflow must keep wireframe, contract/backend, integration, and polish in order")
    for obsolete in (
        "### Mock checkpoint",
        "start with `make mock`",
        "wait for explicit user approval. Do not connect a real Dex backend",
        "Bring the template mock server into conformance",
        "retain Mock Controls only in mock mode",
    ):
        if obsolete in content or obsolete in ui_workflow:
            fail(f"Custom UI workflow must not restore the mock-first gate: {obsolete}")

    build_handoff = (references_dir / "build-test-handoff.md").read_text()
    for text in (
        "low-fidelity static",
        "never hand-edit generated Go or TypeScript clients",
        "never stage, commit, or include it in a pull request",
        "renders and validates every strict",
        "Keep it as one project command",
        "production backend and any frontend",
        "A source handoff and real business acceptance are separate milestones",
        "Generate application tests only when the user explicitly requests them",
        "Missing configuration does not make otherwise complete source blocked",
        "real-dependency policy",
        "no unit tests",
        "intercepted API responses",
        "Run real Dex and Connector",
        "before visual polish",
        "generated server interfaces",
        "storage decision matrix",
        "Remove an unneeded dependency when Dex meets the requirement",
        "approved discovery artifact must name the Dex Web v2 capability gap",
        "management UI capability mapping was completed before the UI-mode",
        "renders to a `valid: true` FDG 2.0 definition",
        "known token expiry, concurrent calls, credential rotation",
    ):
        if text not in build_handoff:
            fail(f"build and handoff must contain: {text}")

    connector_architecture = (references_dir / "connector-architecture.md").read_text()
    for text in (
        "application integration only from a Go backend",
        "it is not a Connector SDK backend",
        "## Application composition",
        "## Released capability discovery",
        "connectors.dex.dev/catalog/v1alpha1",
        "<directory>/<version>",
        "verification blocker",
        "User/API RPC requests provider work",
        "connector, operation, or Trigger is a connector contribution",
        "## Internal connector library decision",
        "Do not infer access to a private repository",
        "uncommitted `go.work` or temporary Go",
        "## Deployed configuration and credential boundary",
        "plaintext development store",
        "Business code neither reads credential objects",
        "Open-source Dex provides no deployed Connector configuration",
        "`sdkgo/projectconfig`",
        "`projectconfig.LoadFromEnvironment`",
        "`NewProjectConnection(project, connectionName)`",
        "sdkgo/v0.25.0",
    ):
        if text not in connector_architecture:
            fail(f"connector architecture must contain: {text}")

    product_discovery = (references_dir / "product-discovery.md").read_text()
    for text in (
        "## Stack checkpoint",
        "defaults to the exact stack",
        "without Connector SDK support",
        "connector capability matrix",
        "canonical published catalog",
        "immutable `connector.yaml`",
        "internal connector library",
        "## Storage decision",
        "Default each durable fact to Dex",
        "cross-Flow reuse alone is not a gap",
        "projection still introduces an external database",
        "“We may need it later” is not evidence",
        "## Actor, role, operation, and permission matrix",
        "one `admin` role for the trusted",
        "justify every non-`admin`",
        "## Flow-boundary decisions",
        "parallel Steps share one Flow identity and lifecycle",
        "independent top-level Flows start separately",
        "SubFlows have an explicit parent-child lifecycle",
        "different authoritative owner and a different",
        "own waits, Timers, and terminal outcomes",
        "state must not pollute the authoritative store",
        "New applications default to no SubFlows",
        "more than 200 concurrent Step executions",
        "architecture-review threshold, not a claimed Dex",
        "user explicitly confirms the SubFlow design",
        "## Management UI capability mapping",
        "Model each record as a Run",
        "Summary RPC",
        "Display RPC",
        "Action metadata",
        "Work Queue permission history",
        "editable scalar Attributes",
        "Run timeline, Step graph",
        "## UI-mode decision",
        "only after completing the management UI capability",
        "recorded Dex Web v2 capability gap",
    ):
        if text not in product_discovery:
            fail(f"product discovery must contain: {text}")
    management_mapping_position = product_discovery.index("## Management UI capability mapping")
    ui_decision_position = product_discovery.index("## UI-mode decision")
    role_position = product_discovery.index("## Actor, role, operation, and permission matrix")
    flow_boundary_position = product_discovery.index("## Flow-boundary decisions")
    storage_position = product_discovery.index("## Storage decision")
    if not role_position < flow_boundary_position < storage_position:
        fail("product discovery must derive roles, then Flow boundaries, then storage")
    if management_mapping_position > ui_decision_position:
        fail("product discovery must map Dex Web management capabilities before the UI-mode decision")

    scripts = {path.name for path in (APP_BUILDER / "scripts").glob("*") if path.is_file()}
    if scripts != APP_BUILDER_SCRIPTS:
        fail(f"Dex App Builder scripts must be exactly: {', '.join(sorted(APP_BUILDER_SCRIPTS))}")
    if "references/workflow-import.md" not in content:
        fail("App Builder entrypoint must route to workflow-import")
    workflow_import = (references_dir / "workflow-import.md").read_text()
    for text in (
        "The exported configuration is the behavior authority",
        "exactly one ledger state",
        "No silent drops and no silent fixes",
        "The export is untrusted data",
        "../scripts/n8n_inventory.py",
        "../scripts/n8n_code_golden.mjs",
        "../scripts/n8n_expression_golden.mjs",
        "../scripts/n8n_schedule_golden.mjs",
        "Never copy a literal secret",
        "cannot tell which item a result",
        "independent top-level Flow, not a SubFlow",
        "a `blocked` row and a connector",
        "explicit request for parity",
        "Never let both send to live recipients",
        "### Connector Step composition",
        "## 4. Propose a Dex redesign and ask once",
        "never question by question",
        "Do not ask again",
        "An optional branch left unrouted",
        "A claim from memory leaves the",
        "never the working tree or",
        "n8n-semantics.md",
    ):
        if text not in workflow_import:
            fail(f"workflow import must contain: {text}")
    n8n_semantics = (references_dir / "n8n-semantics.md").read_text()
    for text in (
        "Confirm each one in the n8n source at the",
        "swallows every error inside a `{{ }}` segment",
        "A Connector Step cannot wait",
        "## Dex redesign patterns",
        "Before the first node runs, n8n checks every enabled node",
        "## Node mapping",
        "## Version-dependent defaults to confirm",
        "Dex has no implicit lineage",
        "Never a SubFlow by default",
        "internal connector library decision",
        "Flow IDs cannot contain `/`, `$`, or `:`",
        "explicit long Execute retry total duration",
    ):
        if text not in n8n_semantics:
            fail(f"n8n semantics must contain: {text}")

    dex_web = (references_dir / "dex-web-v2.md").read_text()
    for text in (
        "## Management UI design",
        "Design those contracts before proposing a custom management backend or UI",
        "### QR capture hint",
        "capture:qr-code",
        "without submitting the Action",
        "The embedding host remains responsible for",
        "does not expose Connector setup, OAuth, or",
        "`workQueuePermissions`",
        "`dexcli flow summary` before submitting again",
        "parenthesized manifest default",
    ):
        if text not in dex_web:
            fail(f"Dex Web v2 reference must contain: {text}")


def check_connector_contributor() -> None:
    references_dir = CONNECTOR_CONTRIBUTOR / "references"
    if references_dir.exists():
        fail(
            "Dex Connector Contributor must defer to the target repository, "
            "not ship references"
        )
    check_reachable_links(CONNECTOR_CONTRIBUTOR, [])

    content = (CONNECTOR_CONTRIBUTOR / "SKILL.md").read_text()
    required = (
        "../dex-sdk/references/core/plugin-version-check.md",
        "../dex-sdk/SKILL.md",
        "../dex-sdk/references/go/go.md",
        "superdurable/dex-connectors-library",
        "Add <XYZ> to Dex official connector library",
        "Git remote identity",
        "user's verified GitHub fork",
        "explicit authorization",
        "`origin`",
        "`upstream`",
        "`AGENTS.md`",
        "files under `docs/`",
        "sole connector-authoring authority",
        "stop before connector implementation",
    )
    for text in required:
        if text not in content:
            fail(f"dex-connector-contributor/SKILL.md must contain: {text}")

    forbidden = (
        "../dex-app-builder/references/dex-web-v2.md",
        "<!-- connector-source:",
        "## Non-negotiable boundaries",
        "## Example gate",
        "## Handoff gate",
    )
    for text in forbidden:
        if text in content:
            fail(f"Dex Connector Contributor must not duplicate target-repository guidance: {text}")

    agent = CONNECTOR_CONTRIBUTOR / "agents" / "openai.yaml"
    if not agent.is_file():
        fail("Dex Connector Contributor must define agents/openai.yaml")
    agent_content = agent.read_text()
    for text in ("Dex Connector Contributor", "$dex-connector-contributor"):
        if text not in agent_content:
            fail(f"dex-connector-contributor/agents/openai.yaml must contain: {text}")


def check_invocation_policy() -> None:
    expected = {
        APP_BUILDER: "true",
        SDK: "true",
        CONNECTOR_CONTRIBUTOR: "true",
    }
    for skill, allow_implicit in expected.items():
        agent = skill / "agents" / "openai.yaml"
        content = agent.read_text()
        policy = f"allow_implicit_invocation: {allow_implicit}"
        if policy not in content:
            fail(f"{agent.relative_to(ROOT)} must contain: {policy}")
        skill_content = (skill / "SKILL.md").read_text()
        frontmatter = skill_content.split("---", 2)[1]
        if "disable-model-invocation: true" in frontmatter:
            fail(f"{(skill / 'SKILL.md').relative_to(ROOT)} must allow model invocation")


def check_plugin_version_check() -> None:
    if not PLUGIN_VERSION_CHECK.is_file():
        fail("dex-sdk/references/core/plugin-version-check.md must exist")
    content = PLUGIN_VERSION_CHECK.read_text()
    for text in (
        "Dex Skills lifecycle version check",
        "status=current",
        "status=outdated",
        "status=unavailable",
        "https://api.github.com/repos/superdurable/dex-skills/releases/latest",
        "without additional user approval",
        "vMAJOR.MINOR.PATCH",
        "first substantive Dex-related",
        "Do not show the notice in a non-Dex conversation",
    ):
        if text not in content:
            fail(f"plugin version check must contain: {text}")

    relative_links = {
        APP_BUILDER: "../dex-sdk/references/core/plugin-version-check.md",
        SDK: "references/core/plugin-version-check.md",
        CONNECTOR_CONTRIBUTOR: "../dex-sdk/references/core/plugin-version-check.md",
    }
    for skill, relative_link in relative_links.items():
        content = (skill / "SKILL.md").read_text()
        if relative_link not in content:
            fail(f"{skill.name}/SKILL.md must load the Dex Skills version check")


def check_standalone_bundle(version: str) -> None:
    if BUNDLE_VERSION.read_text().strip() != version:
        fail("dex-sdk/VERSION must match the root VERSION")

    baselines = BUNDLE_BASELINES.read_text()
    for baseline_name in BASELINE_FILES:
        expected = (ROOT / baseline_name).read_text().strip()
        if baseline_name in {"DEX_JAVA_BASELINE", "DEX_PYTHON_BASELINE", "DEX_TYPESCRIPT_BASELINE", "DEX_RUST_BASELINE"}:
            language = baseline_name.removeprefix("DEX_").removesuffix("_BASELINE").lower()
            if re.fullmatch(rf"sdk-{language}/v\d+\.\d+\.\d+", expected) is None:
                fail(f"{baseline_name} must pin its SDK's published release tag")
        if f"{baseline_name}={expected}" not in baselines:
            fail(f"bundle baselines must mirror {baseline_name}={expected}")

    with tempfile.TemporaryDirectory() as temporary_directory:
        install_root = Path(temporary_directory).resolve()
        for skill in (APP_BUILDER, SDK, CONNECTOR_CONTRIBUTOR):
            shutil.copytree(skill, install_root / skill.name)

        installed_skills = sorted(
            path.parent.name for path in install_root.glob("*/SKILL.md")
        )
        expected_skills = sorted(
            skill.name for skill in (APP_BUILDER, SDK, CONNECTOR_CONTRIBUTOR)
        )
        if installed_skills != expected_skills:
            fail("standalone bundle must copy exactly the three public Skills")

        for markdown in install_root.rglob("*.md"):
            prose = FENCED_BLOCK.sub("", markdown.read_text())
            for target in MARKDOWN_LINK.findall(prose):
                resolved = local_link_target(markdown, target)
                if resolved is None:
                    continue
                if not resolved.is_relative_to(install_root):
                    fail(
                        "standalone link escapes the installed bundle in "
                        f"{markdown.relative_to(install_root)}: {target}"
                    )
                if not resolved.exists():
                    fail(
                        "broken standalone link in "
                        f"{markdown.relative_to(install_root)}: {target}"
                    )


def command_hook(config: dict, event: str, matcher: str, path: Path) -> dict:
    events = config.get("hooks")
    if not isinstance(events, dict):
        fail(f"{path.relative_to(ROOT)} must define hooks")
    entries = events.get(event)
    if not isinstance(entries, list) or len(entries) != 1:
        fail(f"{path.relative_to(ROOT)} must define exactly one {event} hook")
    entry = entries[0]
    if matcher:
        if entry.get("matcher") != matcher:
            fail(f"{path.relative_to(ROOT)} {event} matcher must be {matcher}")
        handlers = entry.get("hooks")
        if not isinstance(handlers, list) or len(handlers) != 1:
            fail(f"{path.relative_to(ROOT)} {event} must contain one command hook")
        return handlers[0]
    return entry


def check_hook_configs(manifests: dict) -> None:
    expected_paths = {
        "codex": "./hooks/codex.json",
        "claude": "./hooks/claude.json",
        "cursor": "./hooks/cursor.json",
    }
    for name, expected in expected_paths.items():
        if manifests[name].get("hooks") != expected:
            fail(f"{name} manifest hooks must be {expected}")
        if not HOOK_CONFIGS[name].is_file():
            fail(f"missing {HOOK_CONFIGS[name].relative_to(ROOT)}")

    shared = ROOT / "hooks" / "hooks.json"
    if shared.exists():
        fail("hooks/hooks.json must not exist; clients require dedicated hook configs")
    if not VERSION_CHECK_SCRIPT.is_file():
        fail("hooks/version-check.mjs must exist")
    script = VERSION_CHECK_SCRIPT.read_text()
    for text in (
        "releases/latest",
        "CACHE_TTL_MS = 15 * 60 * 1000",
        "HTTP_TIMEOUT_MS = 1000",
        "PLUGIN_DATA",
        "CLAUDE_PLUGIN_DATA",
        'allowNetwork: client !== "cursor" || refreshOnly',
        "additionalContext",
        "additional_context",
    ):
        if text not in script:
            fail(f"version hook script must contain: {text}")
    for forbidden in (
        "npm install",
        "npx skills",
        "plugin update",
        "plugin marketplace upgrade",
    ):
        if forbidden in script:
            fail(f"version check hook must not install or update: {forbidden}")

    codex_path = HOOK_CONFIGS["codex"]
    codex = load_json(codex_path)
    if codex.get("description") != (
        "Checks GitHub for a newer stable Dex Skills version. "
        "Reads public release metadata only and never installs updates."
    ):
        fail("Codex hook review description must explain its read-only version check")
    codex_hook = command_hook(codex, "SessionStart", "startup|clear", codex_path)
    if codex_hook.get("type") != "command":
        fail("Codex SessionStart hook must be a command hook")
    if codex_hook.get("command") != 'node "${PLUGIN_ROOT}/hooks/version-check.mjs" --client codex':
        fail("Codex hook must run the shared version script with the Codex client")
    if codex_hook.get("timeout") != 2:
        fail("Codex hook timeout must be two seconds")
    if codex_hook.get("statusMessage") != "Checking for Dex Skills updates":
        fail("Codex hook status message must identify the version check")

    claude_path = HOOK_CONFIGS["claude"]
    claude = load_json(claude_path)
    claude_hook = command_hook(
        claude, "SessionStart", "startup|clear|fork", claude_path
    )
    if claude_hook.get("type") != "command" or claude_hook.get("command") != "node":
        fail("Claude SessionStart hook must run Node directly")
    if claude_hook.get("args") != [
        "${CLAUDE_PLUGIN_ROOT}/hooks/version-check.mjs",
        "--client",
        "claude",
    ]:
        fail("Claude hook must run the shared version script with the Claude client")
    if claude_hook.get("timeout") != 2:
        fail("Claude hook timeout must be two seconds")

    cursor_path = HOOK_CONFIGS["cursor"]
    cursor = load_json(cursor_path)
    if cursor.get("version") != 1:
        fail("Cursor hook config version must be 1")
    cursor_workspace = command_hook(cursor, "workspaceOpen", "", cursor_path)
    cursor_session = command_hook(cursor, "sessionStart", "", cursor_path)
    if cursor_workspace.get("command") != (
        'node "${CURSOR_PLUGIN_ROOT}/hooks/version-check.mjs" '
        "--client cursor --refresh-only"
    ):
        fail("Cursor workspaceOpen must prewarm the version cache")
    if cursor_session.get("command") != (
        'node "${CURSOR_PLUGIN_ROOT}/hooks/version-check.mjs" --client cursor'
    ):
        fail("Cursor sessionStart must inject the cached version result")
    if cursor_workspace.get("timeout") != 2 or cursor_session.get("timeout") != 2:
        fail("Cursor hook timeouts must be two seconds")
    for forbidden in ("resume", "compact", "SessionEnd", "sessionEnd"):
        if forbidden in json.dumps({"codex": codex, "claude": claude, "cursor": cursor}):
            fail(f"version hooks must not run on {forbidden}")


def check_skills(baseline: str) -> None:
    skills = sorted(path.parent for path in ROOT.glob("*/SKILL.md"))
    expected = sorted((SDK, APP_BUILDER, CONNECTOR_CONTRIBUTOR))
    if skills != expected:
        rendered = ", ".join(str(path.relative_to(ROOT)) for path in skills)
        fail(f"expected the three public Dex skills, found: {rendered}")
    if (ROOT / "plugins").exists():
        fail("plugins/ wrapper must not exist")
    if any(path.name == "dex-ai-platform-backend" for path in ROOT.rglob("*")):
        fail("backend companion skill must not exist")
    check_plugin_version_check()
    check_sdk(baseline)
    check_app_builder()
    check_connector_contributor()
    check_invocation_policy()


def check_manifest_common(path: Path, manifest: dict, version: str) -> None:
    if manifest.get("name") != "superdurable-dex":
        fail(f"{path.relative_to(ROOT)} must name plugin superdurable-dex")
    if manifest.get("version") != version:
        fail(f"{path.relative_to(ROOT)} version must be {version}")
    if manifest.get("repository") != "https://github.com/superdurable/dex-skills":
        fail(f"{path.relative_to(ROOT)} must use the dex-skills repository")
    if manifest.get("author", {}).get("name") != "Super Durable":
        fail(f"{path.relative_to(ROOT)} publisher must remain Super Durable")


def check_manifests(version: str) -> None:
    manifests = {name: load_json(path) for name, path in MANIFESTS.items()}
    for name, manifest in manifests.items():
        check_manifest_common(MANIFESTS[name], manifest, version)
    check_hook_configs(manifests)

    codex = manifests["codex"]
    if codex.get("skills") != "./":
        fail("Codex manifest must discover root skills with ./")
    if "mainSkill" in codex:
        fail("Codex manifest must not invent a mainSkill field")
    interface = codex.get("interface")
    if not isinstance(interface, dict):
        fail("Codex manifest must define interface metadata")
    if interface.get("displayName") != "Dex":
        fail("Codex display name must be Dex")
    if interface.get("developerName") != "Super Durable":
        fail("Codex developer name must remain Super Durable")
    long_description = interface.get("longDescription", "")
    for routing_text in (
        "Dex App Builder",
        "Dex SDK",
        "Dex Connector Contributor",
    ):
        if routing_text not in long_description:
            fail(f"Codex long description must contain: {routing_text}")
    for field in ("composerIcon", "logo"):
        if interface.get(field) != "./assets/logo.png":
            fail(f"Codex {field} must use ./assets/logo.png")
    prompts = interface.get("defaultPrompt")
    if not isinstance(prompts, list) or len(prompts) != 4:
        fail("Codex default prompts must expose all three Dex workflows")
    required_prompts = {
        "Discover, design, and build this Dex process application.",
        "Add <XYZ> to Dex official connector library.",
        "Design a reliable Dex Flow for this application.",
        "Prototype and build this Dex workflow.",
    }
    if set(prompts) != required_prompts:
        fail("Codex default prompts must use the natural-language workflow templates")
    if any("$dex-" in prompt for prompt in prompts):
        fail("Codex Plugin default prompts must not use standalone Skill syntax")

    cursor = manifests["cursor"]
    expected_skills = ["./dex-app-builder", "./dex-sdk", "./dex-connector-contributor"]
    if cursor.get("skills") != expected_skills:
        fail("Cursor manifest must expose all three public skills")
    if cursor.get("logo") != "assets/logo.png":
        fail("Cursor manifest must use assets/logo.png")

    if not LOGO.is_file() or not LOGO.read_bytes().startswith(b"\x89PNG\r\n\x1a\n"):
        fail("assets/logo.png must be a PNG file")

    marketplaces = {name: load_json(path) for name, path in MARKETPLACES.items()}
    for name, marketplace in marketplaces.items():
        if marketplace.get("name") != "superdurable":
            fail(f"{MARKETPLACES[name].relative_to(ROOT)} must name marketplace superdurable")
        plugins = marketplace.get("plugins")
        if not isinstance(plugins, list) or len(plugins) != 1:
            fail(f"{MARKETPLACES[name].relative_to(ROOT)} must contain one plugin")
        if plugins[0].get("name") != "superdurable-dex":
            fail(f"{MARKETPLACES[name].relative_to(ROOT)} has the wrong plugin ID")

    if marketplaces["codex"].get("interface", {}).get("displayName") != "Super Durable":
        fail("Codex marketplace publisher must remain Super Durable")
    for name in ("claude", "cursor"):
        if marketplaces[name].get("owner", {}).get("name") != "Super Durable":
            fail(f"{name} marketplace publisher must remain Super Durable")

    codex_entry = marketplaces["codex"]["plugins"][0]
    source = codex_entry.get("source")
    if not isinstance(source, dict) or source.get("path") != "./":
        fail("Codex marketplace must point to the repository root")
    if "version" in codex_entry:
        fail("Codex marketplace entry must remain unversioned")

    claude_entry = marketplaces["claude"]["plugins"][0]
    if claude_entry.get("source") != "./" or claude_entry.get("version") != version:
        fail("Claude marketplace must point to the versioned repository root")
    if claude_entry.get("skills") != expected_skills:
        fail("Claude marketplace must expose all three public skills")

    cursor_entry = marketplaces["cursor"]["plugins"][0]
    if cursor_entry.get("source") != "./" or cursor_entry.get("version") != version:
        fail("Cursor marketplace must point to the versioned repository root")
    if cursor_entry.get("logo") != "assets/logo.png":
        fail("Cursor marketplace must use assets/logo.png")


def check_agent_rules() -> None:
    agents = (ROOT / "AGENTS.md").read_text()
    claude = (ROOT / "CLAUDE.md").read_text()
    cursor = (ROOT / ".cursor" / "rules" / "dex-skills.mdc").read_text()
    if agents != claude:
        fail("AGENTS.md and CLAUDE.md must contain equivalent rules")
    if not cursor.endswith(agents):
        fail("Cursor rule must contain the same repository instructions")
    normalized_agents = " ".join(agents.split())
    for required in (
        "contains no information about any product built on Dex",
        "Never add a skill submodule or a floating branch reference",
    ):
        if required not in normalized_agents:
            fail(f"repository agent rules must contain: {required}")
    for removed in ("TEMPLATE_BASELINE", "template baseline workflow"):
        if removed in normalized_agents:
            fail(f"repository agent rules must not restore: {removed}")
    for removed in (
        ROOT / "TEMPLATE_BASELINE",
        ROOT / "script" / "update-template-baseline.py",
        ROOT / ".github" / "workflows" / "update-template-baseline.yml",
    ):
        if removed.exists():
            fail(f"application template baseline mechanism must not return: {removed.relative_to(ROOT)}")


REMOVED_DEX_CONTRACTS = (
    "PROJECT_CONFIGURATION.md",
    "dynamic-definition-bundles",
    "active-manifest",
    "trusted-header",
    "X-Dex-Work-Queue-Permissions",
    "X-Dex-Web-CSRF-Token",
    "start/recover",
    "flowRenderingSource",
    "local-selector",
    "project Connector mode",
    "native-hosted-project-configuration",
)


# The Connector SDK removed its local loader in sdkgo v0.22.0; connectors from v0.21.0 generate only
# NewProjectConnection. Text may name the older loader only next to the release boundary.
OLDER_CONNECTOR_LOADER_TERMS = ("localconfig", "NewLocalConnection")


def check_removed_dex_contracts() -> None:
    """Dex Server v1.6.1 and Dex CLI v1.7.0 no longer define these Dex Web contracts.

    The Connector SDK's own sdkgo/projectconfig package still exists and is cited for deployed Go applications.
    """
    for markdown in sorted([ROOT / "README.md", *ROOT.glob("dex-*/**/*.md")]):
        content = markdown.read_text()
        for removed in REMOVED_DEX_CONTRACTS:
            if removed in content:
                fail(f"{markdown.relative_to(ROOT)} cites a Dex Web contract that the baselines removed: {removed}")
        for term in OLDER_CONNECTOR_LOADER_TERMS:
            if term in content and "v0.21.0" not in content:
                fail(f"{markdown.relative_to(ROOT)} names the older Connector loader {term} without the v0.21.0 release boundary")


def check_open_source_boundary() -> None:
    for relative_path in git_output("ls-files").splitlines():
        path = ROOT / relative_path
        if not path.is_file():
            continue
        try:
            content = path.read_text()
        except UnicodeDecodeError:
            continue
        if DOWNSTREAM_PRODUCT_NAME.search(content):
            fail(f"{relative_path} must not describe a product built on Dex")


def git_output(*arguments: str) -> str:
    return subprocess.run(
        ["git", *arguments],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def check_release_change(base_ref: str, current_version: str) -> None:
    changed = set(git_output("diff", "--name-only", f"{base_ref}...HEAD").splitlines())
    skill_prefixes = ("dex-sdk/", "dex-app-builder/", "dex-connector-contributor/")
    if not any(path.startswith(skill_prefixes) for path in changed):
        return
    required = {"CHANGELOG.md", "VERSION"}
    missing = required - changed
    if missing:
        fail(f"skill changes must update: {', '.join(sorted(missing))}")
    try:
        base_version = git_output("show", f"{base_ref}:VERSION").strip()
    except subprocess.CalledProcessError:
        return
    if version_tuple(current_version) <= version_tuple(base_version):
        fail(f"VERSION must increase beyond {base_version}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-ref")
    arguments = parser.parse_args()

    current_version = (ROOT / "VERSION").read_text().strip()
    version_tuple(current_version)
    baseline = (ROOT / "DEX_BASELINE").read_text().strip()
    if PUBLISHED_RELEASE_TAG.fullmatch(baseline) is None:
        fail("DEX_BASELINE must contain a published Dex release tag")
    server_baseline = (ROOT / "DEX_SERVER_BASELINE").read_text().strip()
    if PUBLISHED_RELEASE_TAG.fullmatch(server_baseline) is None:
        fail("DEX_SERVER_BASELINE must contain a published Dex release tag")
    cli_baseline = (ROOT / "DEX_CLI_BASELINE").read_text().strip()
    if PUBLISHED_CLI_RELEASE_TAG.fullmatch(cli_baseline) is None:
        fail("DEX_CLI_BASELINE must contain a published Dex CLI release tag")
    check_skills(baseline)
    check_standalone_bundle(current_version)
    check_manifests(current_version)
    check_agent_rules()
    check_removed_dex_contracts()
    check_open_source_boundary()
    if arguments.base_ref:
        check_release_change(arguments.base_ref, current_version)
    print(f"validated superdurable-dex {current_version} with three root skills")


if __name__ == "__main__":
    main()
