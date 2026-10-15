# Copyright (c) 2026 Super Durable, Inc.
#
# Licensed under the Sustainable Use License 1.0.
# You may not use this file except in compliance with the License.
# See the LICENSE file in the repository root.
#
# SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

VERSION_PATTERN = re.compile(
    r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
    r"(?:-[0-9A-Za-z]+(?:[.-][0-9A-Za-z]+)*)?$"
)


@dataclass(frozen=True)
class Component:
    key: str
    name: str
    tag_prefix: str
    baseline_patterns: tuple[str, ...]
    paths: tuple[str, ...]
    excluded_paths: tuple[str, ...] = ()


SHARED_RUST_PATHS = (
    "sdk-rust/Cargo.toml",
    "sdk-rust/Cargo.lock",
    "sdk-rust/crates/dex-blob-cache",
)

COMPONENTS = (
    Component("go", "Go SDK", "sdk-go/v", ("sdk-go/v*",), ("sdk-go",)),
    Component("rust", "Rust SDK", "sdk-rust/v", ("sdk-rust/v*",), ("sdk-rust",)),
    Component(
        "java",
        "Java SDK",
        "sdk-java/v",
        ("sdk-java/v*",),
        ("sdk-java", *SHARED_RUST_PATHS, "sdk-rust/crates/dex-blob-cache-jni"),
    ),
    Component(
        "python",
        "Python SDK",
        "sdk-python/v",
        ("sdk-python/v*",),
        ("sdk-python", *SHARED_RUST_PATHS, "sdk-rust/crates/dex-blob-cache-python"),
    ),
    Component(
        "typescript",
        "TypeScript SDK",
        "sdk-typescript/v",
        ("sdk-typescript/v*",),
        ("sdk-typescript", *SHARED_RUST_PATHS, "sdk-rust/crates/dex-blob-cache-node"),
    ),
    Component(
        "server",
        "Server",
        "server/v",
        ("server/v*", "server-v*"),
        ("server", "web", "packages/flow-definition-renderer", "protos"),
    ),
    Component(
        "cli",
        "CLI",
        "cli-v",
        ("cli-v*",),
        ("cli", "web", "packages/flow-definition-renderer", "server", "protos", "go.work"),
        ("server/all-in-one",),
    ),
)


def git(*arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ("git", *arguments),
        check=check,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def latest_reachable_tag(component: Component) -> str | None:
    result = git(
        "for-each-ref",
        "--merged=HEAD",
        "--format=%(refname:short)",
        *(f"refs/tags/{pattern}" for pattern in component.baseline_patterns),
    )
    tags = [line for line in result.stdout.splitlines() if line]
    versioned_tags = [
        (version_key, tag)
        for tag in tags
        if (version_key := tag_version_key(component, tag)) is not None
    ]
    return max(versioned_tags)[1] if versioned_tags else None


def tag_version_key(component: Component, tag: str) -> tuple[object, ...] | None:
    version = next(
        tag[len(pattern) - 1 :]
        for pattern in component.baseline_patterns
        if tag.startswith(pattern[:-1])
    )
    if not VERSION_PATTERN.fullmatch(version):
        return None
    core, separator, prerelease = version.partition("-")
    major, minor, patch = (int(part) for part in core.split("."))
    if not separator:
        return major, minor, patch, 1
    identifiers = tuple(
        (0, int(identifier)) if identifier.isdigit() else (1, identifier)
        for identifier in prerelease.split(".")
    )
    return major, minor, patch, 0, identifiers


def has_changes(component: Component, baseline: str | None) -> bool:
    if baseline is None:
        result = git("ls-files", "--", *component.paths)
    else:
        result = git("diff", "--name-only", f"{baseline}..HEAD", "--", *component.paths)
    return any(
        not any(path == excluded or path.startswith(f"{excluded}/") for excluded in component.excluded_paths)
        for path in result.stdout.splitlines()
    )


def tag_exists(tag: str) -> bool:
    result = git("show-ref", "--verify", "--quiet", f"refs/tags/{tag}", check=False)
    return result.returncode == 0


def append_summary(lines: list[str]) -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with Path(summary_path).open("a", encoding="utf-8") as summary:
            summary.write("\n".join(lines) + "\n")


def write_outputs(output_path: Path, version: str) -> int:
    selections: list[tuple[Component, str | None, bool, str]] = []
    collisions: list[str] = []

    for component in COMPONENTS:
        baseline = latest_reachable_tag(component)
        selected = has_changes(component, baseline)
        target = f"{component.tag_prefix}{version}"
        selections.append((component, baseline, selected, target))
        if selected and tag_exists(target):
            collisions.append(target)

    summary = ["### Changed-component release plan", "", "| Component | Baseline | Result |", "|---|---|---|"]
    output_lines = [f"version={version}"]
    for component, baseline, selected, target in selections:
        output_lines.extend(
            (
                f"{component.key}={'true' if selected else 'false'}",
                f"{component.key}_tag={target}",
                f"{component.key}_baseline={baseline or ''}",
            )
        )
        baseline_text = baseline or "none (first release)"
        result_text = f"release {target}" if selected else "skip (no changes)"
        summary.append(f"| {component.name} | {baseline_text} | {result_text} |")

    append_summary(summary)
    if collisions:
        print("Target release tags already exist:", file=sys.stderr)
        for collision in collisions:
            print(f"- {collision}", file=sys.stderr)
        return 1

    output_path.write_text("\n".join(output_lines) + "\n", encoding="utf-8")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("version")
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()

    if not VERSION_PATTERN.fullmatch(arguments.version):
        print(f"Invalid semantic version: {arguments.version}", file=sys.stderr)
        return 1

    return write_outputs(arguments.output, arguments.version)


if __name__ == "__main__":
    raise SystemExit(main())
