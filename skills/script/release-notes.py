#!/usr/bin/env python3
# SPDX-License-Identifier: MIT

"""Extract CHANGELOG notes for a single release version."""

from __future__ import annotations

import argparse
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CHANGELOG = ROOT / "CHANGELOG.md"
HEADING = re.compile(r"^## (?P<version>\d+\.\d+\.\d+)(?: - .+)?\s*$")


def extract_notes(changelog: str, version: str) -> str:
    lines = changelog.splitlines()
    start: int | None = None
    end: int | None = None

    for index, line in enumerate(lines):
        match = HEADING.fullmatch(line)
        if match is None:
            continue
        if match.group("version") == version:
            start = index + 1
            continue
        if start is not None:
            end = index
            break

    if start is None:
        raise SystemExit(f"CHANGELOG.md has no heading for {version}")

    body = "\n".join(lines[start:end]).strip()
    if not body:
        raise SystemExit(f"CHANGELOG.md has an empty section for {version}")
    return body + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True)
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args()

    notes = extract_notes(CHANGELOG.read_text(), arguments.version)
    if arguments.output is None:
        print(notes, end="")
        return
    arguments.output.write_text(notes)


if __name__ == "__main__":
    main()
