#!/usr/bin/env python3
# SPDX-License-Identifier: MIT

import importlib.util
import subprocess
import tempfile
import unittest
from pathlib import Path

SPEC = importlib.util.spec_from_file_location(
    "reference_sources", Path(__file__).resolve().parents[1] / "script/check-reference-sources.py",
)
CHECKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECKER)


class ReferenceSourcesTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.git("init", "--quiet")
        self.git("config", "user.name", "Source validator fixture")
        self.git("config", "user.email", "fixture@example.invalid")
        self.paths = {"sdk": "sdk-go/dex/client.go", "server": "server/service/api/service.go", "cli": "cli/internal/command/flow_client_operations.go"}
        self.baselines = {"sdk": "sdk-go/v1.0.0", "server": "server/v2.0.0", "cli": "cli-v3.0.0"}
        for path in self.paths.values():
            source = self.root / path
            source.parent.mkdir(parents=True, exist_ok=True)
            source.write_text("package fixture\n\nfunc Accepted() {}\n")
        self.git("add", ".")
        self.git("commit", "--quiet", "-m", "fixture sources")
        for baseline in self.baselines.values():
            self.git("tag", baseline)
        self.sources = {family: (self.root, baseline) for family, baseline in self.baselines.items()}
        self.markdown = self.root / "reference.md"

    def git(self, *arguments):
        return subprocess.run(["git", *arguments], cwd=self.root, check=True, capture_output=True, text=True)

    def check(self, text):
        self.markdown.write_text(text)
        return CHECKER.check_file(self.markdown, self.sources)

    def link(self, family, baseline=None, path=None):
        return f"[source](https://github.com/superdurable/dex/blob/{baseline or self.baselines[family]}/{path or self.paths[family]})"

    def test_each_family_uses_its_own_immutable_source(self):
        for family in self.sources:
            with self.subTest(family=family):
                CHECKER.validate_checkout(self.root, self.baselines[family])
                self.assertEqual(0, self.check(self.link(family)))

    def test_excerpt_reads_tagged_content_instead_of_modified_worktree(self):
        (self.root / self.paths["sdk"]).write_text("unreleased replacement\n")
        self.assertEqual(1, self.check(self.link("sdk") + "\n<!-- dex-source: sdk-go/dex/client.go -->\n```go\nfunc Accepted() {}\n```\n"))
        with self.assertRaises(SystemExit):
            self.check(self.link("sdk") + "\n<!-- dex-source: sdk-go/dex/client.go -->\n```go\nunreleased replacement\n```\n")

    def test_wrong_component_pin_floating_pin_and_missing_path_rejected(self):
        for text in [self.link("server", baseline=self.baselines["sdk"]), self.link("sdk", baseline="main"), self.link("cli", path="cli/missing.go")]:
            with self.subTest(text=text), self.assertRaises(SystemExit):
                self.check(text)

    def test_missing_tag_and_traversal_rejected(self):
        with self.assertRaises(SystemExit):
            CHECKER.validate_checkout(self.root, "sdk-go/v9.9.9")
        for path in ["../secrets", "/etc/passwd", "server/../sdk-go/dex/client.go", "unknown/file.go"]:
            with self.subTest(path=path):
                self.assertIsNone(CHECKER.source_family(path))


if __name__ == "__main__":
    unittest.main()
