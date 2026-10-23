# Copyright (c) 2026 Super Durable, Inc.
#
# Licensed under the Sustainable Use License 1.0.
# You may not use this file except in compliance with the License.
# See the LICENSE file in the repository root.
#
# SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

"""Enforce repository Rust conventions using Cargo metadata and lexical source checks."""

import argparse
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tomllib

RUST_LINTS = {
    "unsafe_code": "forbid",
    "unreachable_pub": "warn",
    "unused_must_use": "deny",
}
CLIPPY_LINTS = dict.fromkeys(
    "unwrap_used todo unimplemented dbg_macro let_underscore_must_use "
    "let_underscore_future unused_result_ok allow_attributes "
    "allow_attributes_without_reason print_stdout print_stderr "
    "match_wildcard_for_single_variants".split(),
    "warn",
)
CLIPPY_CONFIG = {
    "allow-unwrap-in-tests": True,
    "allow-dbg-in-tests": False,
    "allow-print-in-tests": True,
}
IGNORED_DIRECTORIES = {".git", "target", "node_modules", ".venv", "vendor", "venv"}
GENERIC_MODULES = {"utils", "helpers", "common", "misc"}


@dataclass(frozen=True)
class Violation:
    path: str
    line: int
    rule: str
    message: str
    metric: int | dict = 0

    def __str__(self):
        return f"{self.path}:{self.line}: {self.rule}: {self.message}"


def is_generated(path, source):
    """Use the same generated-code boundary for discovery and every source rule."""
    return bool(
        {"gen", "OUT_DIR"}.intersection(path.parts)
        or re.search(
            r"Code generated|@generated",
            re.match(r"\A(?:\s+|//[^\n]*(?:\n|$)|/\*.*?\*/)*", source, re.S)[0],
        )
    )


def walk_files(root, filename=None):
    for directory, children, files in os.walk(root):
        children[:] = sorted(set(children) - IGNORED_DIRECTORIES)
        for name in sorted(files):
            if name == filename or (filename is None and name.endswith(".rs")):
                yield Path(directory) / name


def blank(text):
    return re.sub(r"[^\n]", " ", text)


def lexical_masks(source):
    """Preserve offsets while masking nested comments and Rust string/character literals."""
    code = list(source)
    comments_only = list(source)
    position = 0
    while position < len(source):
        start = position
        if source.startswith("//", position):
            end = source.find("\n", position)
            position = len(source) if end < 0 else end
        elif source.startswith("/*", position):
            position += 2
            depth = 1
            while position < len(source) and depth:
                if source.startswith("/*", position):
                    depth += 1
                    position += 2
                elif source.startswith("*/", position):
                    depth -= 1
                    position += 2
                else:
                    position += 1
        else:
            raw = re.match(r'(?:br|cr|r)(#*)"', source[position:])
            if raw:
                delimiter = '"' + raw[1]
                end = source.find(delimiter, position + raw.end())
                position = len(source) if end < 0 else end + len(delimiter)
            elif source[position] == '"':
                position += 1
                while position < len(source):
                    if source[position] == "\\":
                        position += 2
                    elif source[position] == '"':
                        position += 1
                        break
                    else:
                        position += 1
            elif source[position] == "'" and re.match(
                r"'(?:\\(?:u\{[^}]+\}|x[0-9a-fA-F]{2}|.)|[^'\\\n])'", source[position:]
            ):
                position += re.match(
                    r"'(?:\\(?:u\{[^}]+\}|x[0-9a-fA-F]{2}|.)|[^'\\\n])'",
                    source[position:],
                ).end()
            else:
                position += 1
                continue
            code[start:position] = blank(source[start:position])
            continue
        code[start:position] = blank(source[start:position])
        comments_only[start:position] = blank(source[start:position])
    return "".join(code), "".join(comments_only)


def matching_end(code, start, opening="{", closing="}"):
    depth = 0
    for position in range(start, len(code)):
        if code[position] == opening:
            depth += 1
        elif code[position] == closing:
            depth -= 1
            if not depth:
                return position + 1
    return len(code)


def test_ranges(path, code, literals):
    if "tests" in path.parts or path.stem.endswith(("_test", "_tests")):
        return [(0, len(code))]
    ranges = []
    for attribute in re.finditer(r"#\s*(!?)\s*\[", code):
        end = matching_end(code, attribute.end() - 1, "[", "]")
        text = literals[attribute.start() : end]
        if not (
            re.search(r"(?:tokio\s*::\s*)?test\s*\]", text)
            or re.search(r"cfg\s*\(\s*test\s*\)", text)
            or re.search(r'cfg\s*\(\s*feature\s*=\s*"test-util"\s*\)', text)
        ):
            continue
        if attribute[1]:
            ranges.append((0, len(code)))
            continue
        position = end
        while True:
            extra = re.match(r"\s*#\s*\[", code[position:])
            if not extra:
                break
            position = matching_end(code, position + extra.end() - 1, "[", "]")
        opening = code.find("{", position)
        semicolon = code.find(";", position)
        item_end = (
            semicolon + 1
            if semicolon >= 0 and (opening < 0 or semicolon < opening)
            else matching_end(code, opening)
        )
        ranges.append((attribute.start(), item_end))
    return ranges


def is_in_ranges(position, ranges):
    return any(start <= position < end for start, end in ranges)


class SourceFindings:
    def __init__(self, relative, source):
        self.relative = relative
        self.source = source
        self.findings = []

    def report(self, rule, position, message, metric=0):
        line = self.source.count("\n", 0, position) + 1
        self.findings.append(Violation(self.relative, line, rule, message, metric))


def source_checks(path, source, relative):
    if is_generated(path, source):
        return [], None
    code, literals = lexical_masks(source)
    tests = test_ranges(path, code, literals)
    checker = SourceFindings(relative, source)
    findings = checker.findings

    header = re.match(r"\A(?://(?![!/])[^\n]*\n)+\n?", source)
    header_end = header.end() if header and "Copyright" in header[0] else 0
    line_count = len(source[header_end:].splitlines())
    is_test_file = is_in_ranges(0, tests)
    if line_count > 1000 and not is_test_file:
        checker.report(
            "R1",
            0,
            f"non-test source has {line_count} lines; maximum is 1000",
            line_count,
        )
    if path.stem in GENERIC_MODULES:
        checker.report("R8", 0, "use a domain-specific module name")
    for item in re.finditer(
        r"\b(?:fn|struct|enum|const|static|type|mod)\s+(\w+_for_test)\b", code
    ):
        if not is_in_ranges(item.start(), tests):
            checker.report(
                "R5",
                item.start(),
                f'{item[1]} requires cfg(test) or cfg(feature = "test-util")',
            )
    is_config_module = any(
        part == "config" or part.endswith(("_config", "_options"))
        for part in path.with_suffix("").parts
    )
    for access in re.finditer(r"\bstd\s*::\s*env\s*::\s*(?:var|var_os)\b", code):
        if not is_config_module and not is_in_ranges(access.start(), tests):
            checker.report(
                "R7",
                access.start(),
                "read environment settings in a typed config module",
            )
    for expect in re.finditer(r"\.\s*expect\s*\(", code):
        end = matching_end(code, expect.end() - 1, "(", ")")
        argument = literals[expect.end() : end - 1].strip().rstrip(",").strip()
        empty = re.fullmatch(r'(?:r#*"\s*"#*|"(?:\s|\\[nrt])*"|)', argument)
        if empty:
            checker.report(
                "R12", expect.start(), "expect must explain why failure is impossible"
            )
    for spawn in re.finditer(r"\btokio\s*::\s*spawn\s*\(", code):
        boundary = max(
            code.rfind(";", 0, spawn.start()),
            code.rfind("{", 0, spawn.start()),
            code.rfind("}", 0, spawn.start()),
        )
        prefix = code[boundary + 1 : spawn.start()]
        if not re.search(r"\blet\s+(?!_\s*(?::[^=]+)?=)[^;=]+=(?:[^;]*)$", prefix):
            checker.report(
                "R14",
                spawn.start(),
                "bind the task JoinHandle to an owner; do not discard it",
            )
    return findings, (not is_test_file)


def read_toml(path):
    with path.open("rb") as source:
        return tomllib.load(source)


def discover_workspaces(root):
    workspaces = {}
    for manifest in walk_files(root, "Cargo.toml"):
        result = subprocess.run(
            [
                "cargo",
                "metadata",
                "--format-version",
                "1",
                "--no-deps",
                "--manifest-path",
                str(manifest),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode:
            raise ValueError(
                f"{manifest.relative_to(root)}:1: R6: cargo metadata failed: {result.stderr.strip()}"
            )
        metadata = json.loads(result.stdout)
        workspaces[Path(metadata["workspace_root"])] = metadata
    return workspaces


def check_readme(package, root):
    manifest = Path(package["manifest_path"])
    readme = manifest.parent / "README.md"
    relative = str(manifest.relative_to(root))
    if not readme.is_file():
        return [
            Violation(
                relative,
                1,
                "R3",
                "every crate needs README.md with an opening Purpose section",
            )
        ]
    text = readme.read_text()
    purpose = re.search(r"\A\s*# [^\n]+\n\s*## Purpose\s*\n+([^\n]+)", text)
    if (
        not purpose
        or len(purpose[1].split()) > 30
        or not re.fullmatch(r"[^.!?]+[.!?]", purpose[1])
    ):
        return [
            Violation(
                str(readme.relative_to(root)),
                1,
                "R3",
                "opening Purpose must be one sentence of at most 30 words",
            )
        ]
    findings = []
    for target in package["targets"]:
        if not set(target["kind"]).intersection(
            {"lib", "bin", "cdylib", "rlib", "staticlib", "proc-macro"}
        ):
            continue
        source_path = Path(target["src_path"])
        source = source_path.read_text()
        if is_generated(source_path, source):
            continue
        documentation = re.search(r"^//! (.+)", source, re.M)
        if not documentation or documentation[1] != purpose[1]:
            findings.append(
                Violation(
                    str(source_path.relative_to(root)),
                    1,
                    "R3",
                    "crate documentation must start with the README Purpose sentence",
                )
            )
    return findings


def check_lints(workspace, metadata, registry, root):
    findings = []
    workspace_manifest = read_toml(workspace / "Cargo.toml")
    expected = {"rust": RUST_LINTS, "clippy": CLIPPY_LINTS}
    is_workspace = "workspace" in workspace_manifest
    if is_workspace and workspace_manifest["workspace"].get("lints") != expected:
        findings.append(
            Violation(
                str((workspace / "Cargo.toml").relative_to(root)),
                1,
                "R15",
                "workspace must use the frozen Rust and Clippy lint tables",
            )
        )
    config = workspace / "clippy.toml"
    if not config.is_file() or read_toml(config) != CLIPPY_CONFIG:
        findings.append(
            Violation(
                str(config.relative_to(root)),
                1,
                "R16",
                "clippy.toml must use the frozen test lint settings",
            )
        )
    members = set(metadata["workspace_members"])
    names = {
        package["name"] for package in metadata["packages"] if package["id"] in members
    }
    for key in ("ffi_crates", "print_crates"):
        for name in registry.get(key, []):
            if name not in names:
                findings.append(
                    Violation(
                        "script/rust-crate-layers.json",
                        1,
                        "R15",
                        f"unknown {key} entry {name}",
                    )
                )
    for package in metadata["packages"]:
        if package["id"] not in members:
            continue
        name = package["name"]
        manifest = Path(package["manifest_path"])
        actual = read_toml(manifest).get("lints", {})
        is_ffi = name in registry.get("ffi_crates", [])
        supports_printing = name in registry.get("print_crates", [])
        if is_workspace and not (is_ffi or supports_printing):
            wanted = {"workspace": True}
        else:
            wanted = {"rust": dict(RUST_LINTS), "clippy": dict(CLIPPY_LINTS)}
            if is_ffi:
                wanted["rust"]["unsafe_code"] = "deny"
            if supports_printing:
                wanted["clippy"].update(print_stdout="allow", print_stderr="allow")
        if actual != wanted:
            findings.append(
                Violation(
                    str(manifest.relative_to(root)),
                    1,
                    "R15",
                    "crate lint table must inherit the workspace or match its registered FFI/print override exactly",
                )
            )
    return findings


def check_workspace(workspace, metadata, registry, root):
    findings = check_lints(workspace, metadata, registry, root)
    members = set(metadata["workspace_members"])
    packages = [package for package in metadata["packages"] if package["id"] in members]
    layers = registry.get("layers", {})
    package_names = {package["name"] for package in packages}
    for name in layers.keys() - package_names:
        findings.append(
            Violation(
                "script/rust-crate-layers.json", 1, "R6", f"remove unknown crate {name}"
            )
        )
    source_files = {}
    for package in packages:
        manifest = Path(package["manifest_path"])
        relative = str(manifest.relative_to(root))
        name = package["name"]
        findings.extend(check_readme(package, root))
        if name not in layers:
            findings.append(
                Violation(
                    relative,
                    1,
                    "R6",
                    f"register crate {name} in the workspace layer table",
                )
            )
        for dependency in package["dependencies"]:
            dependency_name = dependency["name"]
            if (
                dependency_name in package_names
                and name in layers
                and dependency_name in layers
                and layers[dependency_name] > layers[name]
            ):
                findings.append(
                    Violation(
                        relative,
                        1,
                        "R6",
                        f"{name} cannot depend on higher-layer crate {dependency_name}",
                    )
                )
            if (
                dependency_name == "anyhow"
                and dependency.get("kind") != "dev"
                and any(
                    set(target["kind"]).intersection(
                        {"lib", "rlib", "cdylib", "staticlib", "proc-macro"}
                    )
                    for target in package["targets"]
                )
            ):
                findings.append(
                    Violation(
                        relative,
                        1,
                        "R11",
                        "library crates must use typed errors instead of anyhow",
                    )
                )
        for path in walk_files(manifest.parent):
            if path in source_files:
                continue
            source = path.read_text()
            file_findings, is_production = source_checks(
                path, source, str(path.relative_to(root))
            )
            findings.extend(file_findings)
            source_files[path] = is_production
    directories = {}
    for path, is_production in source_files.items():
        if is_production:
            directories.setdefault(path.parent, []).append(path)
    for path, is_production in source_files.items():
        if not is_production or "src" not in path.parts:
            continue
        for ancestor in path.parents:
            directories.setdefault(ancestor, [])
            if ancestor.name == "src":
                break
    for directory, files in directories.items():
        children = {
            path.relative_to(directory).parts[0]
            for path, is_production in source_files.items()
            if is_production and directory in path.parents and path.parent != directory
        }
        metric = max(len(files), len(children))
        if metric > 10:
            findings.append(
                Violation(
                    str(directory.relative_to(root)),
                    1,
                    "R2",
                    f"module directory has {len(files)} source files and {len(children)} submodules; maximum is 10 each",
                    {"files": len(files), "submodules": len(children)},
                )
            )
    return findings


def exceeds_ceiling(metric, ceiling):
    if isinstance(metric, dict):
        if not isinstance(ceiling, dict) or set(ceiling) != {"files", "submodules"}:
            raise ValueError("R2 baselines require files and submodules ceilings")
        return any(
            exceeds_ceiling(count, ceiling[name]) for name, count in metric.items()
        )
    if type(metric) is not int or type(ceiling) is not int or ceiling < 0:
        raise ValueError("Rust baseline ceilings must be non-negative integers")
    return metric > ceiling


def apply_baseline(findings, baseline):
    remaining = []
    seen = set()
    for finding in findings:
        key = f"{finding.rule}:{finding.path}"
        if key in baseline and finding.rule in {"R1", "R2"}:
            seen.add(key)
            if exceeds_ceiling(finding.metric, baseline[key]):
                remaining.append(
                    Violation(
                        finding.path,
                        finding.line,
                        finding.rule,
                        f"baseline ceiling {baseline[key]} exceeded: {finding.metric}",
                    )
                )
        else:
            remaining.append(finding)
    for key in baseline.keys() - seen:
        rule, separator, path = key.partition(":")
        remaining.append(
            Violation(
                path or "script/rust-rules-baseline.json",
                1,
                rule if separator else "R1",
                "remove fixed or invalid baseline entry; only existing R1/R2 violations may be baselined",
            )
        )
    return remaining


def baseline_changes(baseline, previous):
    findings = []
    for key, ceiling in baseline.items():
        if key not in previous or exceeds_ceiling(ceiling, previous[key]):
            findings.append(
                Violation(
                    "script/rust-rules-baseline.json",
                    1,
                    "R1",
                    f"baseline may only shrink: {key}",
                )
            )
    return findings


def baseline_reference(root, baseline_path):
    relative = baseline_path.resolve().relative_to(root)
    repository = subprocess.run(
        ["git", "rev-parse", "--is-inside-work-tree"],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    if repository.returncode:
        return None
    tracked = subprocess.run(
        ["git", "ls-tree", "--name-only", "origin/main", "--", str(relative)],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    if tracked.returncode:
        raise ValueError("fetch origin/main to verify the shrink-only Rust baseline")
    if not tracked.stdout.strip():
        return None
    revision = subprocess.run(
        ["git", "show", f"origin/main:{relative}"],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(revision.stdout)


def run(root, baseline_path):
    registry_path = root / "script/rust-crate-layers.json"
    registries = json.loads(registry_path.read_text())
    findings = []
    workspaces = discover_workspaces(root)
    for workspace, metadata in sorted(workspaces.items()):
        relative = str(workspace.relative_to(root))
        if relative not in registries:
            findings.append(
                Violation(
                    str(registry_path.relative_to(root)),
                    1,
                    "R6",
                    f"register workspace {relative}",
                )
            )
        findings.extend(
            check_workspace(workspace, metadata, registries.get(relative, {}), root)
        )
    for workspace in registries.keys() - {
        str(path.relative_to(root)) for path in workspaces
    }:
        findings.append(
            Violation(
                str(registry_path.relative_to(root)),
                1,
                "R6",
                f"remove unknown workspace {workspace}",
            )
        )
    baseline = json.loads(baseline_path.read_text()) if baseline_path else {}
    previous = baseline_reference(root, baseline_path) if baseline_path else None
    if previous is not None:
        findings.extend(baseline_changes(baseline, previous))
    return apply_baseline(findings, baseline)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root", type=Path, default=Path(__file__).resolve().parent.parent
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        default=Path(__file__).resolve().parent / "rust-rules-baseline.json",
    )
    arguments = parser.parse_args()
    try:
        findings = run(arguments.root.resolve(), arguments.baseline)
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        print(f"{arguments.root}:1: R6: {error}", file=sys.stderr)
        return 1
    for finding in sorted(findings, key=lambda item: (item.path, item.line, item.rule)):
        print(finding)
    return bool(findings)


if __name__ == "__main__":
    sys.exit(main())
