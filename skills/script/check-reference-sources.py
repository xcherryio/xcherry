#!/usr/bin/env python3
# SPDX-License-Identifier: MIT

import argparse
import re
import subprocess
import textwrap
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REFERENCES = ROOT / "dex-sdk" / "references"
SOURCE_BLOCK = re.compile(
    r"<!-- dex-source: (?P<path>[^\s]+) -->\s*\n"
    r"```(?P<language>[^\n]*)\n(?P<snippet>.*?)\n```",
    re.DOTALL,
)
LANGUAGE_FENCE = re.compile(
    r"^```[ \t]*(?:go|python|java|typescript|ts|rust)(?:[ \t]+[^\n]*)?[ \t]*$",
    re.IGNORECASE | re.MULTILINE,
)
PINNED_LINK = re.compile(
    r"https://github\.com/superdurable/dex/(?P<kind>blob|tree)/(?P<target>[^)\s#]+)"
)


def fail(message: str) -> None:
    raise SystemExit(message)


def normalized(text: str) -> str:
    return textwrap.dedent(text).strip("\n")


def snippet_is_contiguous(source: str, snippet: str) -> bool:
    wanted = normalized(snippet)
    count = len(snippet.splitlines())
    source_lines = source.splitlines()
    return any(
        normalized("\n".join(source_lines[start:start + count])) == wanted
        for start in range(len(source_lines) - count + 1)
    )


def source_family(path: str) -> str | None:
    source = Path(path)
    if source.is_absolute() or ".." in source.parts or not source.parts:
        return None
    prefix = source.parts[0]
    if prefix in {"sdk-go", "sdk-java", "sdk-python", "sdk-rust", "sdk-typescript", "examples"}:
        return "sdk"
    if prefix in {"server", "web", "protos"}:
        return "server"
    if prefix == "cli":
        return "cli"
    return None


def git(root: Path, *arguments: str) -> str:
    result = subprocess.run(
        ["git", *arguments], cwd=root, capture_output=True, text=True, check=False,
    )
    if result.returncode != 0:
        fail(f"cannot resolve pinned source in {root}: {' '.join(arguments)}")
    return result.stdout


def validate_checkout(root: Path, baseline: str) -> None:
    if not root.is_dir():
        fail(f"not a Dex checkout: {root}")
    git(root, "rev-parse", "--git-dir")
    git(root, "rev-parse", "--verify", f"refs/tags/{baseline}^{{commit}}")


def check_file(markdown: Path, sources: dict[str, tuple[Path, str]]) -> int:
    content = markdown.read_text()
    label = str(markdown.relative_to(ROOT)) if markdown.is_relative_to(ROOT) else str(markdown)
    blocks = list(SOURCE_BLOCK.finditer(content))
    marked_fences = {match.start("language") - 3 for match in blocks}
    for fence in LANGUAGE_FENCE.finditer(content):
        if fence.start() not in marked_fences:
            fail(f"unmarked language code fence in {label}")

    for match in blocks:
        source_path = match.group("path")
        family = source_family(source_path)
        if family is None:
            fail(f"unsupported source location in {label}: {source_path}")
        root, baseline = sources[family]
        source = git(root, "show", f"refs/tags/{baseline}:{source_path}")
        preceding = content[max(0, match.start() - 600):match.start()]
        expected = f"https://github.com/superdurable/dex/blob/{baseline}/{source_path}"
        if expected not in preceding:
            fail(f"missing pinned source link before marker in {label}")
        if not snippet_is_contiguous(source, match.group("snippet")):
            fail(f"snippet in {label} is not contiguous in {source_path}")

    for link in PINNED_LINK.finditer(content):
        target = link.group("target")
        matched = False
        for family, (root, baseline) in sources.items():
            if not target.startswith(baseline + "/"):
                continue
            linked_path = target[len(baseline) + 1:]
            if source_family(linked_path) != family:
                fail(f"wrong source family/baseline in {label}: {target}")
            git(root, "cat-file", "-e", f"refs/tags/{baseline}:{linked_path}")
            matched = True
            break
        if not matched:
            fail(f"Dex source link must use its exact SDK, Server, or CLI baseline in {label}: {target}")
    return len(blocks)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dex-root", required=True, type=Path)
    parser.add_argument("--server-root", type=Path)
    parser.add_argument("--cli-root", type=Path)
    arguments = parser.parse_args()
    sources = {
        "sdk": (arguments.dex_root.resolve(), (ROOT / "DEX_BASELINE").read_text().strip()),
        "server": ((arguments.server_root or arguments.dex_root).resolve(), (ROOT / "DEX_SERVER_BASELINE").read_text().strip()),
        "cli": ((arguments.cli_root or arguments.dex_root).resolve(), (ROOT / "DEX_CLI_BASELINE").read_text().strip()),
    }
    for root, baseline in sources.values():
        validate_checkout(root, baseline)
    total = sum(check_file(markdown, sources) for markdown in sorted(REFERENCES.rglob("*.md")))
    if total == 0:
        fail("expected at least one source-marked API excerpt")
    print(f"validated {total} source-marked excerpts and links against " + ", ".join(baseline for _, baseline in sources.values()))


if __name__ == "__main__":
    main()
