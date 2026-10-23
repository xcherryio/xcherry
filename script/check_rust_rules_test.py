# Copyright (c) 2026 Super Durable, Inc.
#
# Licensed under the Sustainable Use License 1.0.
# You may not use this file except in compliance with the License.
# See the LICENSE file in the repository root.
#
# SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

"""Fixture and command-line regression tests for Rust repository conventions."""

import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

CHECKER = Path(__file__).with_name("check-rust-rules.py")
SPEC = importlib.util.spec_from_file_location("rust_rules", CHECKER)
RULES = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = RULES
SPEC.loader.exec_module(RULES)


class RustRulesTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.workspace = self.root / "engine"
        self.workspace.mkdir()
        self.write(
            "script/rust-crate-layers.json",
            json.dumps(
                {
                    "engine": {
                        "layers": {"engine": 0},
                        "ffi_crates": [],
                        "print_crates": [],
                    }
                }
            ),
        )
        self.write("script/rust-rules-baseline.json", "{}")
        self.manifest = {
            "package": {"name": "engine", "version": "0.1.0", "edition": "2024"},
            "lints": {"rust": RULES.RUST_LINTS, "clippy": RULES.CLIPPY_LINTS},
        }
        self.write_manifest()
        self.write(
            "engine/clippy.toml",
            "\n".join(
                f"{name} = {str(value).lower()}"
                for name, value in RULES.CLIPPY_CONFIG.items()
            ),
        )
        self.write(
            "engine/README.md", "# Engine\n\n## Purpose\n\nAn engine for fixtures.\n"
        )
        self.write(
            "engine/src/lib.rs", "//! An engine for fixtures.\n\npub fn run() {}\n"
        )

    def write(self, relative, text):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return path

    def write_manifest(self):
        lines = []
        for section, values in self.manifest.items():
            if section == "lints":
                for category, lints in values.items():
                    lines.append(f"[lints.{category}]")
                    lines.extend(
                        f"{name} = {json.dumps(value)}" for name, value in lints.items()
                    )
            else:
                lines.append(f"[{section}]")
                lines.extend(
                    f"{name} = {json.dumps(value)}" for name, value in values.items()
                )
        self.write("engine/Cargo.toml", "\n".join(lines))

    def check(self):
        return RULES.run(self.root, self.root / "script/rust-rules-baseline.json")

    def assert_rule(self, rule):
        findings = self.check()
        self.assertIn(rule, {finding.rule for finding in findings}, findings)
        result = subprocess.run(
            [
                sys.executable,
                str(CHECKER),
                "--root",
                str(self.root),
                "--baseline",
                str(self.root / "script/rust-rules-baseline.json"),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertRegex(result.stdout, rf"[^\n]+:\d+: {rule}: ")

    def test_compliant_repository(self):
        self.assertEqual(self.check(), [])

    def test_r1_file_size(self):
        self.write("engine/src/large.rs", "// line\n" * 1001)
        self.assert_rule("R1")
        self.write("engine/src/large.rs", "// line\n" * 1000)
        self.assertEqual(self.check(), [])

    def test_r1_license_header_and_inline_tests(self):
        source = (
            "// Copyright (c) Fixture\n// License\n\n"
            + "const VALUE: () = ();\n" * 1000
        )
        self.assertFalse(
            RULES.source_checks(Path("src/file.rs"), source, "src/file.rs")[0]
        )
        self.assertTrue(
            RULES.source_checks(
                Path("src/file.rs"),
                source + "#[cfg(test)] mod tests {}\n",
                "src/file.rs",
            )[0]
        )

    def test_r2_file_count(self):
        for index in range(10):
            self.write(f"engine/src/part_{index}.rs", "fn part() {}\n")
        self.assert_rule("R2")
        (self.workspace / "src/part_9.rs").unlink()
        self.assertEqual(self.check(), [])

    def test_r2_submodule_count(self):
        for index in range(11):
            self.write(f"engine/src/part_{index}/mod.rs", "fn part() {}\n")
        self.assert_rule("R2")
        (self.workspace / "src/part_10/mod.rs").unlink()
        self.assertEqual(self.check(), [])

    def test_r3_readme(self):
        self.write("engine/README.md", "# Engine\n")
        self.assert_rule("R3")
        self.write(
            "engine/README.md", "# Engine\n\n## Purpose\n\nAn engine for fixtures.\n"
        )
        self.assertEqual(self.check(), [])

    def test_r3_purpose_constraints(self):
        for purpose in [
            " ".join(["word"] * 31) + ".",
            "Two sentences. Invalid.",
            "No punctuation",
        ]:
            self.write(
                "engine/README.md", "# Engine\n\n## Purpose\n\n" + purpose + "\n"
            )
            self.assert_rule("R3")
        self.write(
            "engine/README.md", "# Engine\n\n## Purpose\n\nAn engine for fixtures.\n"
        )
        self.write("engine/src/lib.rs", "//! Different documentation.\n")
        self.assert_rule("R3")

    def test_r5_test_utilities(self):
        self.write("engine/src/fixture.rs", "pub fn value_for_test() {}\n")
        self.assert_rule("R5")
        for configuration in ["test", 'feature = "test-util"']:
            self.write(
                "engine/src/fixture.rs",
                f"#[cfg({configuration})]\nmod fixtures {{ pub fn value_for_test() {{}} }}\n",
            )
            self.assertEqual(self.check(), [])

    def test_r6_registration(self):
        self.write("script/rust-crate-layers.json", "{}")
        self.assert_rule("R6")
        self.write(
            "script/rust-crate-layers.json",
            json.dumps({"engine": {"layers": {"engine": 0}}}),
        )
        self.assertEqual(self.check(), [])

    def test_discovers_additional_workspaces_and_members(self):
        self.write(
            "new-engine/Cargo.toml",
            '[workspace]\nmembers = ["service"]\nresolver = "3"\n',
        )
        self.write(
            "new-engine/service/Cargo.toml",
            '[package]\nname = "service"\nversion = "0.1.0"\n',
        )
        self.write("new-engine/service/src/lib.rs", "")
        discovered = RULES.discover_workspaces(self.root)
        self.assertEqual(set(discovered), {self.workspace, self.root / "new-engine"})
        self.assert_rule("R6")

    def test_r6_dependency_direction(self):
        metadata = RULES.discover_workspaces(self.root)[self.workspace]
        package = metadata["packages"][0]
        other = dict(package, name="higher", id="higher")
        package["dependencies"].append({"name": "higher", "kind": None})
        metadata["packages"].append(other)
        metadata["workspace_members"].append("higher")
        registry = {"layers": {"engine": 0, "higher": 1}}
        self.assertIn(
            "R6",
            {
                item.rule
                for item in RULES.check_workspace(
                    self.workspace, metadata, registry, self.root
                )
            },
        )
        registry["layers"]["engine"] = 1
        self.assertNotIn(
            "R6",
            {
                item.rule
                for item in RULES.check_workspace(
                    self.workspace, metadata, registry, self.root
                )
            },
        )

    def test_r7_environment(self):
        self.write(
            "engine/src/settings.rs", 'fn setting() { std::env::var("SETTING"); }\n'
        )
        self.assert_rule("R7")
        (self.workspace / "src/settings.rs").rename(self.workspace / "src/config.rs")
        self.assertEqual(self.check(), [])

    def test_r8_module_names(self):
        self.write("engine/src/helpers.rs", "fn help() {}\n")
        self.assert_rule("R8")
        (self.workspace / "src/helpers.rs").rename(self.workspace / "src/connection.rs")
        self.assertEqual(self.check(), [])

    def test_r11_library_errors(self):
        metadata = RULES.discover_workspaces(self.root)[self.workspace]
        package = metadata["packages"][0]
        package["dependencies"].append({"name": "anyhow", "kind": None})
        findings = RULES.check_workspace(
            self.workspace, metadata, {"layers": {"engine": 0}}, self.root
        )
        self.assertIn("R11", {item.rule for item in findings})
        package["dependencies"][-1]["kind"] = "dev"
        findings = RULES.check_workspace(
            self.workspace, metadata, {"layers": {"engine": 0}}, self.root
        )
        self.assertNotIn("R11", {item.rule for item in findings})

    def test_r11_command_line(self):
        self.write(
            "errors/Cargo.toml", '[package]\nname = "anyhow"\nversion = "1.0.0"\n'
        )
        self.write("errors/src/lib.rs", "")
        with (self.workspace / "Cargo.toml").open("a") as manifest:
            manifest.write('\n[dependencies]\nanyhow = { path = "../errors" }\n')
        self.assert_rule("R11")

    def test_r12_expect_reason(self):
        for argument in ['""', '"   "', 'r#" "#', '"\\n\\t"']:
            self.write(
                "engine/src/reason.rs", f"fn reason() {{ value.expect({argument}); }}\n"
            )
            self.assert_rule("R12")
        self.write(
            "engine/src/reason.rs",
            'fn reason() { value.expect("the header has exactly four bytes"); }\n',
        )
        self.assertEqual(self.check(), [])

    def test_r14_task_ownership(self):
        for prefix in ["", "let _ = "]:
            self.write(
                "engine/src/task.rs",
                "fn run() { " + prefix + "tokio::spawn(async {}); }\n",
            )
            self.assert_rule("R14")
        self.write(
            "engine/src/task.rs",
            "fn run() { let task = tokio::spawn(async {}); task.abort(); }\n",
        )
        self.assertEqual(self.check(), [])

    def test_lint_override_consistency(self):
        self.manifest["lints"] = {
            "rust": dict(RULES.RUST_LINTS),
            "clippy": dict(RULES.CLIPPY_LINTS),
        }
        self.manifest["lints"]["rust"]["unsafe_code"] = "deny"
        self.write_manifest()
        self.assert_rule("R15")
        self.write(
            "script/rust-crate-layers.json",
            json.dumps({"engine": {"layers": {"engine": 0}, "ffi_crates": ["engine"]}}),
        )
        self.assertEqual(self.check(), [])
        self.manifest["lints"]["clippy"]["print_stdout"] = "allow"
        self.write_manifest()
        self.assert_rule("R15")
        self.manifest["lints"]["clippy"]["print_stderr"] = "allow"
        self.write_manifest()
        self.write(
            "script/rust-crate-layers.json",
            json.dumps(
                {
                    "engine": {
                        "layers": {"engine": 0},
                        "ffi_crates": ["engine"],
                        "print_crates": ["engine"],
                    }
                }
            ),
        )
        self.assertEqual(self.check(), [])

    def test_clippy_config(self):
        self.write("engine/clippy.toml", "allow-unwrap-in-tests = false")
        self.assert_rule("R16")

    def test_generated_and_test_exemptions(self):
        for directory, prefix in [
            ("gen", ""),
            ("OUT_DIR", ""),
            ("src", "// Code generated. DO NOT EDIT.\n"),
            ("src", "// @generated\n"),
        ]:
            path = self.write(
                f"engine/{directory}/helpers.rs",
                prefix + "tokio::spawn(async {});\n" * 1001,
            )
            self.assertEqual(self.check(), [])
            path.unlink()
        self.write("engine/tests/fixture.rs", 'std::env::var("SETTING");\n' * 1001)
        self.assertEqual(self.check(), [])

    def test_comments_strings_and_test_scopes(self):
        source = """// tokio::spawn(async {}); value.expect("");
/* outer /* tokio::spawn(async {}); */ nested */
const SOURCE: &str = r###"tokio::spawn(async {}); value.expect("");"###;
#[cfg(test)]
mod tests { pub fn source_for_test() { std::env::var("SETTING"); } }
"""
        self.write("engine/src/source.rs", source)
        self.assertEqual(self.check(), [])
        self.write(
            "engine/src/source.rs",
            source + '\nfn setting() { std::env::var("SETTING"); }\n',
        )
        self.assert_rule("R7")

    def test_baseline_shrinks_and_rejects_new_violations(self):
        finding = RULES.Violation("src/large.rs", 1, "R1", "large", 1100)
        baseline = {"R1:src/large.rs": 1100}
        self.assertEqual(RULES.apply_baseline([finding], baseline), [])
        self.assertTrue(RULES.apply_baseline([], baseline))
        self.assertTrue(RULES.apply_baseline([finding], {}))
        self.assertTrue(RULES.apply_baseline([finding], {"R1:src/large.rs": 1099}))
        self.assertEqual(RULES.baseline_changes({}, baseline), [])
        self.assertTrue(RULES.baseline_changes({"R1:src/large.rs": 1101}, baseline))
        self.assertTrue(RULES.baseline_changes({"R2:src": 11}, baseline))
        self.assertTrue(RULES.apply_baseline([], {"R7:src/config.rs": 1}))

    def test_r2_baseline_checks_each_dimension(self):
        baseline = {"R2:src": {"files": 42, "submodules": 10}}
        existing = RULES.Violation(
            "src", 1, "R2", "wide", {"files": 42, "submodules": 0}
        )
        added_children = RULES.Violation(
            "src", 1, "R2", "wide", {"files": 42, "submodules": 11}
        )
        self.assertEqual(RULES.apply_baseline([existing], baseline), [])
        self.assertTrue(RULES.apply_baseline([added_children], baseline))
        self.assertEqual(
            RULES.baseline_changes(
                {"R2:src": {"files": 41, "submodules": 10}}, baseline
            ),
            [],
        )
        self.assertTrue(
            RULES.baseline_changes(
                {"R2:src": {"files": 43, "submodules": 10}}, baseline
            )
        )

    def test_baseline_reference_rejects_expansion(self):
        subprocess.run(["git", "init", "-q", str(self.root)], check=True)
        subprocess.run(["git", "-C", str(self.root), "add", "."], check=True)
        subprocess.run(
            [
                "git",
                "-C",
                str(self.root),
                "-c",
                "user.name=Fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "commit",
                "-qm",
                "Fixture",
            ],
            check=True,
        )
        subprocess.run(
            [
                "git",
                "-C",
                str(self.root),
                "update-ref",
                "refs/remotes/origin/main",
                "HEAD",
            ],
            check=True,
        )
        self.write("engine/src/large.rs", "// line\n" * 1001)
        self.write(
            "script/rust-rules-baseline.json",
            json.dumps({"R1:engine/src/large.rs": 1001}),
        )
        self.assert_rule("R1")


if __name__ == "__main__":
    unittest.main()
