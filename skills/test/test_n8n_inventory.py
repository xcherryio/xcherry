#!/usr/bin/env python3
# SPDX-License-Identifier: MIT

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1] / "dex-app-builder" / "scripts"
INVENTORY = SCRIPTS / "n8n_inventory.py"
GOLDEN = SCRIPTS / "n8n_code_golden.mjs"
EXPRESSION_GOLDEN = SCRIPTS / "n8n_expression_golden.mjs"
SCHEDULE_GOLDEN = SCRIPTS / "n8n_schedule_golden.mjs"
SECRET = "fixturekey0123456789abcdef"

RENDER_CODE = """let out = '';
for (row of $input.item.json.rows) {
  console.log(row)
  out += `<li>${row.label}: ${row.note}</li>`
}
return { html: out, setup: $('Settings').first().json.limit };"""


def resolve_ledger(ledger):
    """Resolve every generated row the way a finished import would."""
    lines = []
    for line in ledger.splitlines():
        if line.startswith("| D") and "| todo |   |" in line:
            line = line.replace("| todo |   |", "| decided | yes |")
        elif line.startswith("| P") and "| todo |   |" in line:
            line = line.replace("| todo |   |", "| adopted |   |")
        elif line.startswith("| R") and "(list every branch" in line:
            line = re.sub(r"\(list every branch[^|]*\)", "searched -> Render", line)
        elif line.startswith("| G") and "(list each connector-imposed" in line:
            line = re.sub(r"\(list each connector-imposed[^|]*\)", "none found", line)
        lines.append(line.replace("| todo |   |", "| dropped | not observable in this fixture |"))
    return "\n".join(lines)


def synthetic_export():
    return {
        "name": "Synthetic digest",
        "nodes": [
            {"name": "Daily at 6am", "type": "n8n-nodes-base.scheduleTrigger", "typeVersion": 1.2,
             "parameters": {"rule": {"interval": [{"field": "hours", "triggerAtMinute": 15}]}}},
            {"name": "Settings", "type": "n8n-nodes-base.set", "typeVersion": 3.2,
             "parameters": {"fields": {"values": [
                 {"name": "apiKey", "stringValue": SECRET},
                 {"name": "limit", "type": "numberValue", "numberValue": "5"},
                 {"name": "unusedRecipients"},
             ]}}},
            {"name": "Read records", "type": "n8n-nodes-base.googleSheets", "typeVersion": 4,
             "parameters": {"operation": "read", "documentId": {"__rl": True, "value": "sheet-1"}}},
            {"name": "Choose", "type": "n8n-nodes-base.if", "typeVersion": 2,
             "parameters": {"conditions": {"options": {"caseSensitive": True, "typeValidation": "strict"},
                                           "combinator": "or",
                                           "conditions": [
                                               {"operator": {"type": "string", "operation": "startsWith"},
                                                "leftValue": "={{ $json.title.toLowerCase() }}", "rightValue": "review"},
                                               {"operator": {"type": "string", "operation": "startsWith"},
                                                "leftValue": "={{ $json.title.toLowerCase() }}", "rightValue": "sync"},
                                           ]}}},
            {"name": "Normalize", "type": "n8n-nodes-base.set", "typeVersion": 3.2,
             "parameters": {"fields": {"values": [
                 {"name": "topic", "stringValue": "={{ $json.title.toLowerCase().replace('review', '').trim() }}"},
             ]}}},
            {"name": "Fetch", "type": "n8n-nodes-base.httpRequest", "typeVersion": 4.1,
             "parameters": {"url": "=https://api.example.test/search?q={{ $json.topic }}&from={{ $today.toFormat('yyyy-MM-dd') }}&limit={{ $('Settings').first().json.limit }}"}},
            {"name": "Render", "type": "n8n-nodes-base.code", "typeVersion": 2,
             "parameters": {"mode": "runOnceForEachItem", "jsCode": RENDER_CODE}},
            {"name": "Notify", "type": "n8n-nodes-base.gmail", "typeVersion": 2.1,
             "parameters": {"sendTo": "owner@example.test",
                            "subject": "=Digest for {{ $('Normalize').item.json.title }}",
                            "message": "={{ $json.html }}", "options": {}}},
            {"name": "Nothing", "type": "n8n-nodes-base.noOp", "typeVersion": 1, "parameters": {}},
            {"name": "Orphan", "type": "n8n-nodes-base.noOp", "typeVersion": 1, "parameters": {}},
            {"name": "Note", "type": "n8n-nodes-base.stickyNote", "typeVersion": 1,
             "parameters": {"content": "Runs every morning."}},
        ],
        "connections": {
            "Daily at 6am": {"main": [[{"node": "Settings", "type": "main", "index": 0}]]},
            "Settings": {"main": [[{"node": "Read records", "type": "main", "index": 0}]]},
            "Read records": {"main": [[{"node": "Choose", "type": "main", "index": 0}]]},
            "Choose": {"main": [[{"node": "Normalize", "type": "main", "index": 0}],
                                [{"node": "Nothing", "type": "main", "index": 0}]]},
            "Normalize": {"main": [[{"node": "Fetch", "type": "main", "index": 0}]]},
            "Fetch": {"main": [[{"node": "Render", "type": "main", "index": 0}]]},
            "Render": {"main": [[{"node": "Notify", "type": "main", "index": 0}]]},
        },
        "settings": {"executionOrder": "v1"},
    }


def onboarding_export():
    """A webhook-started export with fan-out, waits, and template placeholders."""
    return {
        "name": "Synthetic onboarding",
        "meta": {"templateId": "1"},
        "triggerCount": 0,
        "nodes": [
            {"name": "Signup", "type": "n8n-nodes-base.webhook", "typeVersion": 1, "position": [0, 0],
             "parameters": {"httpMethod": "POST", "path": "signup"}},
            {"name": "Valid", "type": "n8n-nodes-base.if", "typeVersion": 2, "position": [200, 0],
             "parameters": {"conditions": {"conditions": [
                 {"operator": {"type": "string", "operation": "notEmpty"}, "leftValue": "={{ $json.email }}"}]}}},
            {"name": "Welcome", "type": "n8n-nodes-base.emailSend", "typeVersion": 2.1, "position": [400, 200],
             "parameters": {"fromEmail": "team@example.test", "toEmail": "={{ $json.body.email }}",
                            "subject": "Welcome to [Company Name]", "text": "Hello"}},
            {"name": "Later", "type": "n8n-nodes-base.wait", "typeVersion": 1, "position": [400, -200],
             "parameters": {"amount": 2, "unit": "days"}},
            {"name": "Contact", "type": "n8n-nodes-base.hubspot", "typeVersion": 2, "position": [600, -200],
             "parameters": {"operation": "upsert", "authentication": "appToken", "email": "={{ $json.body.email }}"},
             "credentials": {"hubspotAppToken": {"id": "7", "name": "CRM"}}},
            {"name": "Status", "type": "n8n-nodes-base.hubspot", "typeVersion": 2, "position": [800, -200],
             "parameters": {"operation": "update", "contactId": "={{ $json.id }}"},
             "credentials": {"hubspotApi": {"id": "8", "name": "CRM key"}}},
        ],
        "connections": {
            "Signup": {"main": [[{"node": "Valid", "type": "main", "index": 0}]]},
            "Valid": {"main": [[{"node": "Welcome", "type": "main", "index": 0}, {"node": "Later", "type": "main", "index": 0}]]},
            "Later": {"main": [[{"node": "Contact", "type": "main", "index": 0}]]},
            "Contact": {"main": [[{"node": "Status", "type": "main", "index": 0}]]},
        },
        "settings": {"executionOrder": "v1"},
    }


class InventoryTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.export = self.root / "export.json"
        self.export.write_text(json.dumps(synthetic_export()))
        self.out = self.root / "import"
        self.run_script("inventory", str(self.export), "--out", str(self.out))
        self.ledger = (self.out / "ledger.md").read_text()
        self.inventory = json.loads((self.out / "inventory.json").read_text())
        self.kinds = {finding["kind"] for finding in self.inventory["findings"]}

    def run_script(self, *arguments, check=True):
        result = subprocess.run(
            [sys.executable, "-B", str(INVENTORY), *arguments],
            capture_output=True, text=True,
        )
        if check and result.returncode != 0:
            self.fail(f"{arguments[0]} failed: {result.stderr}")
        return result

    def finding(self, kind):
        return [item for item in self.inventory["findings"] if item["kind"] == kind]

    def test_redacts_literal_secret_everywhere(self):
        self.assertIn("literal-secret", self.kinds)
        for output in (self.ledger, (self.out / "inventory.json").read_text()):
            self.assertNotIn(SECRET, output)
            self.assertIn("‹redacted›", output)

    def test_reports_schedule_label_mismatch(self):
        [mismatch] = self.finding("schedule-label-mismatch")
        self.assertIn("every hour at minute 15", mismatch["message"])
        self.assertEqual(self.inventory["schedules"][0]["cron"], "15 */1 * * *")

    def test_labels_branch_outputs(self):
        labels = {(edge["from"], edge["label"], edge["to"]) for edge in self.inventory["edges"]}
        self.assertIn(("Choose", "true", "Normalize"), labels)
        self.assertIn(("Choose", "false", "Nothing"), labels)

    def test_reports_semantic_risks(self):
        for kind in (
            "timezone-unset", "dead-config", "unreachable-node", "missing-field-throws",
            "unencoded-query", "literal-set-mismatch", "paired-item", "code-port",
            "js-coercion", "hardcoded-address", "version-default", "type-coercion",
        ):
            self.assertIn(kind, self.kinds)
        dead = " ".join(item["message"] for item in self.finding("dead-config"))
        self.assertIn("`unusedRecipients`", dead)
        self.assertNotIn("`limit`", dead)
        [mismatch] = self.finding("literal-set-mismatch")
        self.assertIn("sync", mismatch["message"])
        [orphan] = self.finding("unreachable-node")
        self.assertEqual(orphan["node"], "Orphan")

    def test_paired_item_reference_to_non_ancestor_is_reported(self):
        export = synthetic_export()
        export["connections"]["Render"] = {"main": [[]]}
        export["connections"]["Choose"]["main"][1].append({"node": "Notify", "type": "main", "index": 0})
        self.export.write_text(json.dumps(export))
        self.run_script("inventory", str(self.export), "--out", str(self.out))
        inventory = json.loads((self.out / "inventory.json").read_text())
        kinds = {finding["kind"] for finding in inventory["findings"]}
        self.assertIn("non-ancestor-reference", kinds)

    def test_verify_requires_every_row_resolved(self):
        ledger = self.out / "ledger.md"
        result = self.run_script("verify", str(ledger), check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn("todo", result.stderr)

        resolved = re.sub(r"\| todo \|   \|", "| diverged |   |", self.ledger)
        ledger.write_text(resolved)
        result = self.run_script("verify", str(ledger), check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn("needs notes", result.stderr)

        resolved = resolve_ledger(self.ledger)
        ledger.write_text(resolved)
        result = self.run_script("verify", str(ledger))
        self.assertIn("ledger complete", result.stdout)

        ledger.write_text(resolved.replace("| decided | yes |", "| pending | proposed: yes |", 1))
        self.assertIn("await the user's decision", self.run_script("verify", str(ledger)).stdout)
        strict = self.run_script("verify", str(ledger), "--strict", check=False)
        self.assertEqual(strict.returncode, 1)
        self.assertIn("pending awaits", strict.stderr)

        without_row = "\n".join(line for line in resolved.splitlines() if not line.startswith("| E1 |"))
        ledger.write_text(without_row)
        result = self.run_script("verify", str(ledger), check=False)
        self.assertIn("E1: generated row is missing", result.stderr)

        split_row = resolved.replace("| E1 |", "| E1a |", 1)
        ledger.write_text(split_row)
        self.assertEqual(self.run_script("verify", str(ledger), check=False).returncode, 0, "sub-rows replace a split row")

        unverified = re.sub(r"(\| N1 \|.*?)\| dropped \| [^|]* \|", r"\1| mapped | default assumed from memory |", resolved, count=1)
        ledger.write_text(unverified)
        self.assertIn("unverified claim", self.run_script("verify", str(ledger), check=False).stderr)

        ledger.write_text(resolved.replace("| dropped | not observable in this fixture |", "| diverged | per D99 |", 1))
        self.assertIn("D99", self.run_script("verify", str(ledger), check=False).stderr)

        ledger.write_text(self.ledger)
        self.assertIn("one row per connector branch", self.run_script("verify", str(ledger), check=False).stderr)

    def test_attached_sub_nodes_are_reachable_and_placeholders_are_not_secrets(self):
        export = synthetic_export()
        export["nodes"] += [
            {"name": "Agent", "type": "@n8n/n8n-nodes-langchain.agent", "typeVersion": 2, "parameters": {}},
            {"name": "Chat Model", "type": "@n8n/n8n-nodes-langchain.lmChatOpenAi", "typeVersion": 1, "parameters": {}},
            {"name": "Keys", "type": "n8n-nodes-base.set", "typeVersion": 3.4, "parameters": {"assignments": {"assignments": [
                {"name": "botToken", "type": "string", "value": "YOUR_BOT_TOKEN"}]}}},
        ]
        export["connections"]["Render"]["main"][0].append({"node": "Agent", "type": "main", "index": 0})
        export["connections"]["Agent"] = {"main": [[{"node": "Keys", "type": "main", "index": 0}]]}
        export["connections"]["Chat Model"] = {"ai_languageModel": [[{"node": "Agent", "type": "ai_languageModel", "index": 0}]]}
        self.export.write_text(json.dumps(export))
        self.run_script("inventory", str(self.export), "--out", str(self.out))
        inventory = json.loads((self.out / "inventory.json").read_text())
        unreachable = {item["node"] for item in inventory["findings"] if item["kind"] == "unreachable-node"}
        self.assertEqual(unreachable, {"Orphan"})
        placeholders = [item for item in inventory["findings"] if item["kind"] == "placeholder-secret"]
        self.assertEqual(len(placeholders), 1)
        secrets = [item for item in inventory["findings"] if item["kind"] == "literal-secret"]
        self.assertEqual({item["node"] for item in secrets}, {"Settings"})
        self.assertIn("YOUR_BOT_TOKEN", (self.out / "ledger.md").read_text())

    def test_reports_placeholder_nodes_without_credentials(self):
        export = synthetic_export()
        for node in export["nodes"]:
            if node["name"] in ("Read records", "Notify"):
                node["credentials"] = {"oauth": {"id": "1", "name": "Account"}}
        export["nodes"] += [
            {"name": "Alert", "type": "n8n-nodes-base.telegram", "typeVersion": 1, "parameters": {"operation": "sendMessage"}},
            {"name": "Publish", "type": "@vendor/n8n-nodes-vendor.vendor", "typeVersion": 1,
             "parameters": {"platform": "video"}, "credentials": {"vendorApi": {"id": "2", "name": "Vendor"}}},
        ]
        export["connections"]["Render"]["main"][0] += [
            {"node": "Alert", "type": "main", "index": 0}, {"node": "Publish", "type": "main", "index": 0},
        ]
        self.export.write_text(json.dumps(export))
        self.run_script("inventory", str(self.export), "--out", str(self.out))
        inventory = json.loads((self.out / "inventory.json").read_text())
        by_kind = {}
        for item in inventory["findings"]:
            by_kind.setdefault(item["kind"], set()).add(item["node"])
        self.assertEqual(by_kind["missing-credential"], {"telegram nodes"})
        self.assertEqual(by_kind["hollow-node"], {"Alert"})
        self.assertEqual(by_kind["community-node"], {"@vendor/n8n-nodes-vendor"})
        self.assertIn("preflight-fails", by_kind, "a hollow node reachable from the trigger fails every execution")
        hollow = next(item for item in inventory["findings"] if item["kind"] == "hollow-node")
        self.assertIn("missing required chatId, text", hollow["message"])
        self.assertIn("Publish (vendor@1)", (self.out / "ledger.md").read_text())

    def test_graph_matrix_and_plan_drafts(self):
        catalog = self.root / "catalog.yaml"
        catalog.write_text("""apiVersion: connectors.dex.dev/catalog/v1alpha1
kind: ConnectorCatalog
connectors:
    - company: Google
      id: gmail
      name: Gmail
      version: v0.21.0
      directory: connectors/google/gmail
      uiUnits: []
      triggers:
        - name: messageReceived
          description: Receive a message.
      operations:
        - name: getMessage
          kind: query
          description: Read a message.
        - name: sendMessage
          kind: mutation
          description: Send a message.
    - company: Example
      id: example-search
      name: Example Search
      version: v0.21.0
      directory: connectors/example
      uiUnits: []
      triggers: []
      operations:
        - name: searchItems
          kind: query
          description: Search.
""")
        export = synthetic_export()
        export["nodes"][5]["parameters"]["url"] = "=https://api.example.test/search?q={{ $json.topic }}"
        export["nodes"][7]["credentials"] = {"gmailOAuth2": {"id": "1", "name": "Gmail"}}
        export["nodes"].append({"name": "Parser", "type": "@n8n/n8n-nodes-langchain.outputParserStructured", "typeVersion": 1, "parameters": {}})
        self.export.write_text(json.dumps(export))
        self.run_script("inventory", str(self.export), "--out", str(self.out), "--catalog", str(catalog))
        ledger = (self.out / "ledger.md").read_text()
        inventory = json.loads((self.out / "inventory.json").read_text())
        self.assertIn("```mermaid", ledger)
        self.assertIn("-- true -->", ledger)
        matrix = {row["node"]: row for row in inventory["connectorMatrix"]}
        self.assertEqual(matrix["Notify"]["connector"], "gmail v0.21.0")
        self.assertEqual(matrix["Notify"]["likelyOperation"], "sendMessage")
        self.assertEqual(matrix["Fetch"]["connector"], "example-search v0.21.0", "an HTTP host matches a released connector by name")
        self.assertIn("structured output", matrix["Parser"]["status"], "a LangChain sub-node is configuration, not a missing connector")
        self.assertIn("connector contribution", matrix["Read records"]["status"])
        plan = {}
        for row in inventory["planDraft"]:
            plan.setdefault(row["node"], row)
        self.assertEqual(plan["Notify"]["element"], "Connector Step Notify")
        elements = [row["element"] for row in inventory["planDraft"] if row["node"] == "Notify"]
        self.assertEqual(elements[1:], ["application Step NotifyOutcome", "application Step NotifyExecuteFailed"],
                         "each Connector Step gets its own outcome and Execute-failure Steps")
        self.assertIn("persisted cursor", plan["Notify"]["notes"], "items from a read reach a Connector Step one at a time")
        self.assertEqual(plan["Nothing"]["element"], "dropped")
        self.assertEqual(plan["Daily at 6am"]["element"], "trigger")
        kinds = {finding["kind"] for finding in inventory["findings"]}
        self.assertIn("raw-html-interpolation", kinds)
        self.assertIn("header-line-break", kinds)

    def test_rejects_non_workflow_input(self):
        self.export.write_text(json.dumps({"hello": "world"}))
        result = self.run_script("inventory", str(self.export), check=False)
        self.assertEqual(result.returncode, 2)


class DetectorTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def inventory(self, export, *extra):
        path = self.root / "export.json"
        path.write_text(json.dumps(export))
        result = subprocess.run([sys.executable, "-B", str(INVENTORY), "inventory", str(path), "--out", str(self.root / "out"), *extra],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        inventory = json.loads((self.root / "out" / "inventory.json").read_text())
        by_kind = {}
        for item in inventory["findings"]:
            by_kind.setdefault(item["kind"], []).append(item)
        return inventory, by_kind, (self.root / "out" / "ledger.md").read_text()

    def test_webhook_fan_out_and_template_findings(self):
        _, by_kind, ledger = self.inventory(onboarding_export())
        self.assertEqual([item["node"] for item in by_kind["webhook-output-shape"]], ["Valid"])
        self.assertIn("$json.body.email", by_kind["webhook-output-shape"][0]["message"])
        self.assertIn("[Company Name]", by_kind["literal-placeholder"][0]["message"])
        self.assertIn("appToken", by_kind["inconsistent-authentication"][0]["message"])
        order = by_kind["branch-order"][0]["message"]
        self.assertLess(order.index("Later"), order.index("Welcome"), "v1 runs the upper branch first")
        self.assertIn("Welcome", by_kind["branch-wait-pause"][0]["message"])
        self.assertIn("gallery template", by_kind["provenance"][0]["message"])
        notes = {item["node"]: item["message"] for item in by_kind["version-default"]}
        self.assertIn("2 days", notes["Later"])
        self.assertIn("exactly 24 hours", notes["Later"])
        self.assertIn("verified POST", notes["Signup"])
        self.assertIn("This email was sent automatically with n8n", notes["Welcome"])
        self.assertIn("| E4 | Later | main | Contact | todo |", ledger)
        self.assertIn("| S", ledger)
        self.assertIn("meta.templateId", ledger)
        self.assertRegex(ledger, r"\| B\d+ \| Duplicate or overlapping trigger \| Signup \|")
        self.assertRegex(ledger, r"\| R\d+ \| Contact \|")
        self.assertRegex(ledger, r"\| P\d+ \| fix: Read the request body fields the author meant \|", "a source defect becomes a proposal, not its own question")
        self.assertNotRegex(ledger, r"\| D\d+ \| webhook-output-shape \|")
        self.assertRegex(ledger, r"\| D1 \| migration-mode \|", "the mode question comes first")

    def test_schedule_window_secret_and_empty_reads(self):
        export = synthetic_export()
        export["nodes"][5]["parameters"]["url"] += "&key={{ $('Settings').first().json.apiKey }}"
        render = export["nodes"][6]
        render["parameters"]["jsCode"] = "if ($input.item.json.rows.length === 0) { return { html: 'none' } }\n" + RENDER_CODE
        _, by_kind, ledger = self.inventory(export)
        self.assertIn("Notify", by_kind["repeated-effects"][0]["message"])
        self.assertEqual({item["node"] for item in by_kind["secret-in-request"]}, {"Fetch"})
        self.assertNotIn(SECRET, ledger)
        zero = next(item for item in by_kind["zero-items-stop"] if item["node"] == "Read records")
        self.assertIn("Render", zero["message"], "the empty-result branch in Render never runs")
        self.assertEqual(zero["severity"], "high")
        self.assertIn("missing-field-empty", by_kind)
        self.assertIn("missing-field-throws", by_kind, "Code nodes still throw on a missing field")

    def test_code_mode_locale_identifiers_and_credentials(self):
        export = synthetic_export()
        export["nodes"][6]["parameters"] = {"jsCode": "return [{json: {when: $now.toFormat('DDDD'), total: $json.n.toLocaleString()}}];"}
        export["nodes"][2]["parameters"]["documentId"] = {"__rl": True, "value": "sheet-abc", "mode": "id"}
        export["nodes"].append({"name": "Model", "type": "@n8n/n8n-nodes-langchain.lmChatOpenAi", "typeVersion": 1.2,
                                "parameters": {"model": {"__rl": True, "value": "", "mode": "list"}},
                                "credentials": {"openAiApi": {"id": None, "name": "", "__aiGatewayManaged": True}}})
        _, by_kind, _ = self.inventory(export)
        self.assertEqual([item["node"] for item in by_kind["first-item-only"]], ["Render"])
        self.assertIn("toLocaleString", by_kind["locale-dependent"][0]["message"])
        self.assertIn("sheet-abc", by_kind["hardcoded-identifier"][0]["message"])
        self.assertIn("__aiGatewayManaged", by_kind["managed-credential"][0]["message"])
        self.assertIn("Model", {item["node"] for item in by_kind["hollow-node"]})

    def test_joins_loops_and_open_triggers(self):
        export = {
            "nodes": [
                {"name": "Chat", "type": "n8n-nodes-base.telegramTrigger", "typeVersion": 1.1, "parameters": {"updates": ["message"]}},
                {"name": "Submit", "type": "n8n-nodes-base.httpRequest", "typeVersion": 4.2,
                 "parameters": {"method": "POST", "url": "https://jobs.example.test/submit"}},
                {"name": "Pause", "type": "n8n-nodes-base.wait", "typeVersion": 1.1, "parameters": {"amount": 10}},
                {"name": "Poll", "type": "n8n-nodes-base.httpRequest", "typeVersion": 4.2, "parameters": {"url": "={{ $json.statusUrl }}"}},
                {"name": "Done", "type": "n8n-nodes-base.if", "typeVersion": 2, "parameters": {}},
                {"name": "Join", "type": "n8n-nodes-base.merge", "typeVersion": 3.2, "parameters": {"mode": "chooseBranch"}},
                {"name": "Stray", "type": "n8n-nodes-base.noOp", "typeVersion": 1, "parameters": {}},
            ],
            "connections": {
                "Chat": {"main": [[{"node": "Submit", "type": "main", "index": 0}]]},
                "Submit": {"main": [[{"node": "Pause", "type": "main", "index": 0}]]},
                "Pause": {"main": [[{"node": "Poll", "type": "main", "index": 0}]]},
                "Poll": {"main": [[{"node": "Done", "type": "main", "index": 0}]]},
                "Done": {"main": [[{"node": "Join", "type": "main", "index": 0}], [{"node": "Pause", "type": "main", "index": 0}]]},
                "Stray": {"main": [[{"node": "Join", "type": "main", "index": 1}]]},
            },
        }
        _, by_kind, ledger = self.inventory(export)
        self.assertIn("Polling pattern", by_kind["wait-poll-loop"][0]["message"])
        self.assertIn("Merge input 2", by_kind["merge-input"][0]["message"])
        self.assertEqual([item["node"] for item in by_kind["open-trigger"]], ["Chat"])
        self.assertIn("sleeps in place", next(item for item in by_kind["version-default"] if item["node"] == "Pause")["message"])
        self.assertIn("Input item lacks a field", ledger)

    def test_scopes_findings_to_what_actually_runs(self):
        export = {
            "nodes": [
                {"name": "Hourly", "type": "n8n-nodes-base.scheduleTrigger", "typeVersion": 1.2,
                 "parameters": {"rule": {"interval": [{"field": "hours"}]}}},
                {"name": "Weather", "type": "n8n-nodes-base.httpRequest", "typeVersion": 4.2,
                 "parameters": {"url": "=https://api.example.test/now?pageToken={{ $json.nextPageToken }}"}},
                {"name": "Post", "type": "n8n-nodes-base.slack", "typeVersion": 2.2,
                 "parameters": {"text": "=Weather on {{ $today.toFormat('yyyy-MM-dd') }}: see [Dashboard](https://example.test) [INFO]",
                                "otherOptions": {"includeLinkToWorkflow": False}}},
                {"name": "S3", "type": "n8n-nodes-base.awsS3", "typeVersion": 2, "parameters": {"operation": "getAll"}, "disabled": True},
                {"name": "Hook", "type": "n8n-nodes-base.webhook", "typeVersion": 2, "parameters": {"httpMethod": "POST"}},
                {"name": "Alert", "type": "n8n-nodes-base.gmail", "typeVersion": 2.1,
                 "parameters": {"subject": "=Welcome to [Company Name], {{ $json.body.name }}"}},
                {"name": "Batches", "type": "n8n-nodes-base.splitInBatches", "typeVersion": 3, "parameters": {}},
                {"name": "Pace", "type": "n8n-nodes-base.wait", "typeVersion": 1.1, "parameters": {"amount": 1}},
            ],
            "connections": {
                "Hourly": {"main": [[{"node": "Weather", "type": "main", "index": 0}]]},
                "Weather": {"main": [[{"node": "S3", "type": "main", "index": 0}]]},
                "S3": {"main": [[{"node": "Post", "type": "main", "index": 0}]]},
                "Hook": {"main": [[{"node": "Alert", "type": "main", "index": 0}]]},
                "Alert": {"main": [[{"node": "Batches", "type": "main", "index": 0}]]},
                "Batches": {"main": [[], [{"node": "Pace", "type": "main", "index": 0}]]},
                "Pace": {"main": [[{"node": "Batches", "type": "main", "index": 0}]]},
            },
        }
        _, by_kind, ledger = self.inventory(export)
        self.assertNotIn("repeated-effects", by_kind, "printing today's date is not a whole-day read")
        self.assertNotIn("secret-in-request", by_kind, "a page token is not a credential")
        self.assertNotIn("zero-items-stop", by_kind, "a disabled read never runs")
        self.assertIn("for executions started by Hook", by_kind["preflight-fails"][0]["message"])
        self.assertEqual([item["node"] for item in by_kind["wait-rate-limit"]], ["Pace"])
        self.assertNotIn("wait-poll-loop", by_kind)
        placeholders = " ".join(item["message"] for item in by_kind["literal-placeholder"])
        self.assertIn("[Company Name]", placeholders)
        self.assertNotIn("[Dashboard]", placeholders)
        self.assertNotIn("[INFO]", placeholders)
        self.assertFalse(any(item["node"] == "Post" for item in by_kind["version-default"]), "the workflow link is turned off")
        ledger_path = self.root / "out" / "ledger.md"
        resolved = resolve_ledger(ledger)
        ledger_path.write_text(resolved)
        result = subprocess.run([sys.executable, "-B", str(INVENTORY), "verify", str(ledger_path)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, "a node named like a ledger ID is not a ledger row: " + result.stderr)

    def test_verify_flags_unverified_wording_only(self):
        _, _, ledger = self.inventory(onboarding_export())
        ledger_path = self.root / "out" / "ledger.md"
        resolved = resolve_ledger(ledger)
        for note, is_flagged in (("sends an email to confirm the order", False), ("lists recalled products", False),
                                 ("default taken from memory", True), ("unconfirmed default", True), ("based on assumptions", True)):
            ledger_path.write_text(re.sub(r"(\| N1 \|.*?)\| dropped \| [^|]* \|", rf"\1| mapped | {note} |", resolved, count=1))
            result = subprocess.run([sys.executable, "-B", str(INVENTORY), "verify", str(ledger_path)], capture_output=True, text=True)
            self.assertEqual("unverified claim" in result.stderr, is_flagged, note)
        moved = self.root / "elsewhere.md"
        moved.write_text(resolved)
        result = subprocess.run([sys.executable, "-B", str(INVENTORY), "verify", str(moved)], capture_output=True, text=True)
        self.assertIn("not found", result.stderr)
        result = subprocess.run([sys.executable, "-B", str(INVENTORY), "verify", str(moved), "--inventory", str(self.root / "out" / "inventory.json")],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_acceptance_gate_overwrite_guard_and_poll_delay(self):
        export = {
            "nodes": [
                {"name": "Start", "type": "n8n-nodes-base.manualTrigger", "typeVersion": 1, "parameters": {}},
                {"name": "Submit", "type": "n8n-nodes-base.httpRequest", "typeVersion": 4.2,
                 "parameters": {"method": "POST", "url": "https://jobs.example.test/submit"}},
                {"name": "Hold", "type": "n8n-nodes-base.wait", "typeVersion": 1.1, "parameters": {"amount": 30}},
                {"name": "Result", "type": "n8n-nodes-base.httpRequest", "typeVersion": 4.2, "parameters": {"url": "={{ $json.resultUrl }}"}},
                {"name": "Keys", "type": "n8n-nodes-base.set", "typeVersion": 3.4, "parameters": {"assignments": {"assignments": [
                    {"name": "apiKey", "type": "string", "value": "YOUR_API_KEY"}]}}},
            ],
            "connections": {
                "Start": {"main": [[{"node": "Keys", "type": "main", "index": 0}]]},
                "Keys": {"main": [[{"node": "Submit", "type": "main", "index": 0}]]},
                "Submit": {"main": [[{"node": "Hold", "type": "main", "index": 0}]]},
                "Hold": {"main": [[{"node": "Result", "type": "main", "index": 0}]]},
            },
        }
        _, by_kind, ledger = self.inventory(export)
        self.assertIn("Submit and Result", by_kind["wait-poll-delay"][0]["message"])
        self.assertIn("(placeholder in workflow data)", ledger)
        self.assertNotIn("code-port", {row.split("|")[2].strip() for row in ledger.splitlines() if row.startswith("| D")})
        ledger_path = self.root / "out" / "ledger.md"
        resolved = resolve_ledger(ledger)
        ledger_path.write_text(re.sub(r"\| E1 \|(.*?)\| dropped \| [^|]* \|", r"| E1 |\1| blocked | connector gap |", resolved, count=1))
        verify = [sys.executable, "-B", str(INVENTORY), "verify", str(ledger_path)]
        self.assertEqual(subprocess.run(verify, capture_output=True, text=True).returncode, 0)
        accept = subprocess.run(verify + ["--accept"], capture_output=True, text=True)
        self.assertEqual(accept.returncode, 1)
        self.assertIn("blocked rows must be resolved", accept.stderr)
        path = self.root / "export.json"
        rerun = subprocess.run([sys.executable, "-B", str(INVENTORY), "inventory", str(path), "--out", str(self.root / "out")],
                               capture_output=True, text=True)
        self.assertEqual(rerun.returncode, 2)
        self.assertIn("--force", rerun.stderr)

    def test_release_markers_activation_and_spec_rows(self):
        export = onboarding_export()
        export["settings"]["binaryMode"] = "separate"
        export["nodes"][0]["notes"] = "Questions? Write to author@example.test"
        export["nodes"].append({"name": "Model", "type": "@n8n/n8n-nodes-langchain.lmChatOpenAi", "typeVersion": 1.2,
                                "parameters": {"model": {"__rl": True, "value": "gpt-x", "mode": "list"}, "options": {"maxRetries": 2}}})
        export["connections"]["Model"] = {"ai_languageModel": [[{"node": "Contact", "type": "ai_languageModel", "index": 0}]]}
        inventory, by_kind, ledger = self.inventory(export)
        self.assertIn("2.5.0", by_kind["release-marker"][0]["message"])
        self.assertIn("Welcome", by_kind["activation-blocked"][0]["message"], "a node without credentials blocks activation from 2.8")
        self.assertIn("maxRetries=2", by_kind["model-client-retry"][0]["message"])
        self.assertNotIn("author@example.test", json.dumps(inventory["claims"]))
        self.assertIn("‹address›", ledger)
        self.assertRegex(ledger, r"\| G\d+ \| Contact \|")
        ledger_path = self.root / "out" / "ledger.md"
        resolved = re.sub(r"(\| D\d+ \| preflight-fails .*?)\| decided \| yes \|", r"\1| pending | proposed: build the intended workflow |", resolve_ledger(ledger), count=1)
        if "| pending | proposed: build" not in resolved:
            resolved = resolve_ledger(ledger).replace("| decided | yes |", "| pending | proposed: optimize |", 1)
        decision = re.search(r"^\| (D\d+) \|(?: [^|]* \|){4} pending \|", resolved, re.MULTILINE).group(1)
        resolved = re.sub(r"(\| N1 \|.*?)\| dropped \| [^|]* \|", rf"\1| mapped | spec: {decision} |", resolved, count=1)
        ledger_path.write_text(resolved)
        result = subprocess.run([sys.executable, "-B", str(INVENTORY), "verify", str(ledger_path)], capture_output=True, text=True)
        self.assertIn("mapped (conditional)", result.stdout)

    def test_schedule_second_and_pending_references(self):
        export = synthetic_export()
        export["id"] = "wf1"
        export["nodes"][0]["id"] = "n1"
        _, by_kind, ledger = self.inventory(export)
        expected = int.from_bytes(__import__("hashlib").sha256(b"wf1:n1:second").digest()[:4], "big") % 60
        self.assertIn(f"stable second is {expected}", by_kind["schedule-second"][0]["message"])
        self.assertIn("| n8n release |", ledger)
        ledger_path = self.root / "out" / "ledger.md"
        resolved = re.sub(r"(\| N1 \|.*?)\| dropped \| [^|]* \|", r"\1| pending | proposed: keep it |", resolve_ledger(ledger), count=1)
        ledger_path.write_text(resolved)
        result = subprocess.run([sys.executable, "-B", str(INVENTORY), "verify", str(ledger_path)], capture_output=True, text=True)
        self.assertIn("must name the D row", result.stderr)

    def test_unused_outputs_windows_branch_claims_and_cutover(self):
        export = synthetic_export()
        export["triggerCount"] = 1
        export["nodes"][0]["parameters"]["rule"]["interval"] = [{"field": "days", "triggerAtHour": 6}]
        export["nodes"][5]["parameters"]["url"] = "=https://api.example.test/search?from={{ $now.minus({ days: 7 }).toISODate() }}"
        export["nodes"][6]["parameters"]["jsCode"] = "return { json: { html: 'x', debugCount: 3, meta: { inner: 1 } } };"
        _, by_kind, ledger = self.inventory(export)
        unused = by_kind["unused-output-field"][0]["message"]
        self.assertIn("`debugCount`", unused)
        self.assertNotIn("`html`", unused, "Notify reads html")
        self.assertNotIn("`inner`", unused, "nested keys are not output fields")
        self.assertIn("7-day window", by_kind["overlapping-window"][0]["message"])
        self.assertIn("Node name `Nothing`", ledger, "a node on an IF output makes a claim with its name")
        self.assertRegex(ledger, r"\| B\d+ \| Cutover \|")
        self.assertIn("| expression engine |", ledger)

    def test_caller_instant_and_agent_exit_findings(self):
        _, by_kind, ledger = self.inventory(onboarding_export())
        self.assertIn("server or a browser", by_kind["caller-type"][0]["message"])
        export = synthetic_export()
        export["nodes"] += [
            {"name": "Agent", "type": "@n8n/n8n-nodes-langchain.agent", "typeVersion": 2, "parameters": {}},
            {"name": "Parser", "type": "@n8n/n8n-nodes-langchain.outputParserStructured", "typeVersion": 1, "parameters": {}},
            {"name": "Think", "type": "@n8n/n8n-nodes-langchain.toolThink", "typeVersion": 1, "parameters": {}},
        ]
        export["connections"]["Render"]["main"][0].append({"node": "Agent", "type": "main", "index": 0})
        export["connections"]["Parser"] = {"ai_outputParser": [[{"node": "Agent", "type": "ai_outputParser", "index": 0}]]}
        export["connections"]["Think"] = {"ai_tool": [[{"node": "Agent", "type": "ai_tool", "index": 0}]]}
        _, by_kind, ledger = self.inventory(export)
        self.assertIn("Fetch", by_kind["evaluation-instant"][0]["message"])
        self.assertIn("10 iterations", by_kind["agent-exits"][0]["message"])
        ledger_path = self.root / "out" / "ledger.md"
        quoted = re.sub(r"(\| N1 \|.*?)\| dropped \| [^|]* \|", r"\1| mapped | the note says `assumed` but the rule is cited |", resolve_ledger(ledger), count=1)
        ledger_path.write_text(quoted)
        result = subprocess.run([sys.executable, "-B", str(INVENTORY), "verify", str(ledger_path)], capture_output=True, text=True)
        self.assertNotIn("unverified claim", result.stderr, "a quoted source claim is not the migrator's own wording")

    def test_redesign_proposals_and_one_mode_question(self):
        inventory, by_kind, ledger = self.inventory(synthetic_export())
        patterns = {proposal["pattern"] for proposal in inventory["redesign"]}
        self.assertIn("entity-timer", patterns, "an hourly run over today's records repeats effects")
        self.assertIn("typed-steps", patterns)
        self.assertIn("explicit-recovery", patterns, "per-item sends recover per item")
        self.assertIn("fix", patterns)
        self.assertEqual(sum(1 for line in ledger.splitlines() if "| migration-mode |" in line), 1)
        self.assertIn("## Intent contract (draft)", ledger)
        self.assertIn("once per item from Read records", ledger)
        self.assertNotRegex(ledger, r"\| D\d+ \| repeated-effects \|", "folded into the redesign proposal")
        ledger_path = self.root / "out" / "ledger.md"
        resolved = resolve_ledger(ledger)
        ledger_path.write_text(resolved.replace("| adopted |   |", "| pending | awaiting D1 |", 1))
        verify = [sys.executable, "-B", str(INVENTORY), "verify", str(ledger_path)]
        self.assertEqual(subprocess.run(verify, capture_output=True, text=True).returncode, 0)
        self.assertEqual(subprocess.run(verify + ["--strict"], capture_output=True, text=True).returncode, 1, "a pending proposal blocks --strict")
        ledger_path.write_text(resolved.replace("| adopted |   |", "| mapped |   |", 1))
        self.assertIn("is not one of adopted", subprocess.run(verify, capture_output=True, text=True).stderr)
        ledger_path.write_text(resolved.replace("| dropped | not observable in this fixture |", "| diverged | per P99 |", 1))
        self.assertIn("P99", subprocess.run(verify, capture_output=True, text=True).stderr)

    def test_rejects_a_file_that_is_not_the_published_catalog(self):
        catalog = self.root / "catalog.yaml"
        catalog.write_text("apiVersion: connectors.dex.dev/v1alpha1\nkind: ConnectorCatalogSource\n")
        path = self.root / "export.json"
        path.write_text(json.dumps(synthetic_export()))
        result = subprocess.run([sys.executable, "-B", str(INVENTORY), "inventory", str(path), "--catalog", str(catalog)],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn("not a Dex connector catalog", result.stderr)


@unittest.skipUnless(shutil.which("node"), "node is required for the golden harness")
class GoldenHarnessTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.export = self.root / "export.json"
        self.export.write_text(json.dumps(synthetic_export()))

    def golden(self, fixture, node="Render"):
        path = self.root / "fixture.json"
        path.write_text(json.dumps(fixture))
        return subprocess.run(
            ["node", str(GOLDEN), str(self.export), node, str(path)],
            capture_output=True, text=True,
        )

    def test_reproduces_javascript_coercion_per_item(self):
        result = self.golden({
            "items": [
                {"json": {"rows": [{"label": "a", "note": None}, {"label": "b"}]}},
                {"json": {"rows": []}},
            ],
            "nodes": {"Settings": [{"json": {"limit": 5}}]},
        })
        self.assertEqual(result.returncode, 0, result.stderr)
        output = json.loads(result.stdout)
        self.assertEqual(output, [
            {"json": {"html": "<li>a: null</li><li>b: undefined</li>", "setup": 5}},
            {"json": {"html": "", "setup": 5}},
        ])
        self.assertIn("[console.log]", result.stderr)

    def test_a_thrown_error_is_golden_failure(self):
        result = self.golden({"items": [{"json": {}}], "nodes": {"Settings": [{"json": {}}]}})
        self.assertEqual(result.returncode, 1)
        self.assertIn("node failed", result.stderr)

    def test_expression_golden_reproduces_javascript_and_failures(self):
        export = synthetic_export()
        export["nodes"][4]["parameters"]["fields"]["values"][0]["stringValue"] = "={{ $json.title.toLowerCase().replace('review', '').trim() }}"
        self.export.write_text(json.dumps(export))
        path = self.root / "expression.json"
        path.write_text(json.dumps({"items": [{"json": {"title": "Review \u0130stanbul\u0085"}}, {"json": {}}]}))
        result = subprocess.run(
            ["node", str(EXPRESSION_GOLDEN), str(self.export), "Normalize", "fields.values[0].stringValue", str(path)],
            capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        output = json.loads(result.stdout)
        self.assertEqual(output[0], {"value": "i\u0307stanbul\u0085"})
        self.assertTrue(output[1]["undefined"], "n8n swallows a TypeError inside an expression")
        self.assertIn("toLowerCase", output[1]["swallowedErrors"][0])
        mixed = subprocess.run(
            ["node", str(EXPRESSION_GOLDEN), str(self.export), "Notify", "subject", str(path)],
            capture_output=True, text=True,
        )
        self.assertEqual(mixed.returncode, 0, mixed.stderr)
        self.assertIn("Normalize", json.loads(mixed.stdout)[0]["error"], "a missing upstream node is reported per item")

    def test_harnesses_follow_n8n_item_and_template_rules(self):
        export = {"nodes": [
            {"name": "All", "type": "n8n-nodes-base.code", "typeVersion": 2,
             "parameters": {"jsCode": "return [{json: {first: $input.item.json.n, count: $input.all().length}}];"}},
            {"name": "Each", "type": "n8n-nodes-base.code", "typeVersion": 2,
             "parameters": {"mode": "runOnceForEachItem", "jsCode": "return {json: {n: $input.first().json.n}};"}},
            {"name": "Text", "type": "n8n-nodes-base.set", "typeVersion": 3.4,
             "parameters": {"value": "=a{{ $json.missing }}b{{ 0 }}{{ false }}{{ null }}{{ {k: 1} }}{{ $json.missing.trim() }}"}},
        ]}
        self.export.write_text(json.dumps(export))
        fixture = self.root / "items.json"
        fixture.write_text(json.dumps({"items": [{"json": {"n": 1}}, {"json": {"n": 2}}]}))
        first = subprocess.run(["node", str(GOLDEN), str(self.export), "All", str(fixture)], capture_output=True, text=True)
        self.assertEqual(json.loads(first.stdout), [{"json": {"first": 1, "count": 2}}])
        self.assertIn("first input item", first.stderr)
        each = subprocess.run(["node", str(GOLDEN), str(self.export), "Each", str(fixture)], capture_output=True, text=True)
        self.assertEqual(each.returncode, 1)
        self.assertIn("Can't use .first() here", each.stderr)
        text = subprocess.run(["node", str(EXPRESSION_GOLDEN), str(self.export), "Text", "value", str(fixture)], capture_output=True, text=True)
        rendered = json.loads(text.stdout)[0]
        self.assertEqual(rendered["value"], "ab0false[object Object]")
        self.assertEqual(len(rendered["swallowedErrors"]), 1)
        proposed = subprocess.run(["node", str(EXPRESSION_GOLDEN), str(self.export), "Text", "value", str(fixture),
                                   "--expression", "={{ $json.n * 10 }}"], capture_output=True, text=True)
        self.assertEqual(json.loads(proposed.stdout), [{"value": 10}, {"value": 20}])

    def test_harness_refuses_values_it_cannot_reproduce(self):
        export = {"nodes": [{"name": "Text", "type": "n8n-nodes-base.set", "typeVersion": 3.4, "parameters": {"value": "={{ $json.n }}"}}]}
        self.export.write_text(json.dumps(export))
        fixture = self.root / "items.json"
        fixture.write_text(json.dumps({"items": [{"json": {"n": 5, "s": ""}}], "globals": {"$execution": {"id": "e1"}}}))
        def run(expression):
            return subprocess.run(["node", str(EXPRESSION_GOLDEN), str(self.export), "Text", "value", str(fixture), "--expression", expression],
                                  capture_output=True, text=True)
        for expression in ("={{ $json.s.isEmpty() }}", "={{ $ifEmpty($json.s, 'x') }}", "=n={{ $json.n.toString().toNumber() }}"):
            result = run(expression)
            self.assertEqual(result.returncode, 3, expression)
            self.assertIn("unsupported", json.loads(result.stdout)[0])
        self.assertEqual(json.loads(run("={{ $execution.id }}").stdout), [{"value": "e1"}])
        url = subprocess.run(["node", str(EXPRESSION_GOLDEN), str(self.export), "Text", "url", str(fixture), "--expression",
                              "=https://api.example.test/search?q=at#t&from={{ $json.n }}"], capture_output=True, text=True)
        sent = json.loads(url.stdout)[0]
        self.assertEqual(sent["sent"], "https://api.example.test/search?q=at", "a # drops every later query parameter")
        self.assertEqual(sent["droppedFragment"], "#t&from=5")
        native = json.loads(run("={{ $json.n.toLowerCase() }}").stdout)
        self.assertTrue(native[0]["undefined"], "a native method missing on a number is swallowed, not unsupported")
        self.assertEqual(run("={{ $json.s.isEmpty() }}").returncode, 3)
        self.assertEqual(json.loads(run("={{ $json.n; }}").stdout), [{"value": 5}])
        self.assertEqual(json.loads(run("={{ $json.n }} ").stdout), [{"value": "5 "}])
        self.assertEqual(json.loads(run("={{ $json.s.trim }}").stdout), [{"error": "this is a function, please add ()"}])
        self.assertEqual(run("={{ $now.toISO() }}").returncode, 3, "time needs Luxon and fixture.now")
        missing = subprocess.run(["node", str(EXPRESSION_GOLDEN), str(self.export), "Text", "value", str(fixture), "extra"],
                                 capture_output=True, text=True)
        self.assertEqual(missing.returncode, 2)

    def test_fixture_time_locale_and_empty_items(self):
        export = {"nodes": [
            {"name": "Clock", "type": "n8n-nodes-base.set", "typeVersion": 3.4, "parameters": {"value": "={{ DateTime.now().toISODate() }}"}},
            {"name": "Code", "type": "n8n-nodes-base.code", "typeVersion": 2, "parameters": {"jsCode": "return $input.all();"}},
        ]}
        self.export.write_text(json.dumps(export))
        empty = self.root / "empty.json"
        empty.write_text(json.dumps({"items": []}))
        result = subprocess.run(["node", str(GOLDEN), str(self.export), "Code", str(empty)], capture_output=True, text=True)
        self.assertEqual((result.returncode, json.loads(result.stdout)), (0, []))
        self.assertIn("unreachable", result.stderr)
        # Runs when Luxon is installed, for example with N8N_GOLDEN_LUXON pointing at an npm prefix.
        luxon = os.environ.get("N8N_GOLDEN_LUXON") or subprocess.run(["node", "-e", "require.resolve('luxon')"], capture_output=True).returncode == 0
        if luxon:
            fixture = self.root / "time.json"
            fixture.write_text(json.dumps({"items": [{"json": {}}], "now": "2026-03-08T07:00:00-05:00", "timezone": "America/New_York"}))
            clock = subprocess.run(["node", str(EXPRESSION_GOLDEN), str(self.export), "Clock", "value", str(fixture)], capture_output=True, text=True)
            self.assertEqual(json.loads(clock.stdout), [{"value": "2026-03-08"}], "DateTime.now() reads fixture.now")

    def test_function_node_and_per_item_check_follow_n8n(self):
        export = {"nodes": [
            {"name": "Legacy", "type": "n8n-nodes-base.function", "typeVersion": 1,
             "parameters": {"functionCode": "return [{json: {a: $json.n}}];"}},
            {"name": "Each", "type": "n8n-nodes-base.code", "typeVersion": 2, "parameters": {
                "mode": "runOnceForEachItem", "jsCode": "// earlier code used $input.all() here\nreturn {json: {total: $json.callCount}};"}},
        ]}
        self.export.write_text(json.dumps(export))
        fixture = self.root / "items.json"
        fixture.write_text(json.dumps({"items": [{"json": {"n": 1, "callCount": 2}}, {"json": {"n": 2, "callCount": 3}}]}))
        legacy = subprocess.run(["node", str(GOLDEN), str(self.export), "Legacy", str(fixture)], capture_output=True, text=True)
        self.assertEqual(json.loads(legacy.stdout), [{"json": {"a": 1}}])
        each = subprocess.run(["node", str(GOLDEN), str(self.export), "Each", str(fixture)], capture_output=True, text=True)
        self.assertEqual(each.returncode, 1, "n8n matches 'all' inside callCount after the comment names .all()")

    def test_schedule_harness_follows_the_cron_library(self):
        usage = subprocess.run(["node", str(SCHEDULE_GOLDEN), "0 9 * * * *", "UTC"], capture_output=True, text=True)
        self.assertEqual(usage.returncode, 2)
        missing = subprocess.run(["node", str(SCHEDULE_GOLDEN), "0 9 * * * *", "UTC", "2026-01-01T00:00:00Z", "2026-01-01T03:00:00Z",
                                  "--cron", str(self.root)], capture_output=True, text=True)
        self.assertEqual(missing.returncode, 3, "the cron package must come from the pinned directory")
        cron_directory = os.environ.get("N8N_GOLDEN_CRON")
        if cron_directory:
            fall_back = subprocess.run(["node", str(SCHEDULE_GOLDEN), "37 9 * * * *", "America/New_York",
                                        "2026-11-01T04:00:00Z", "2026-11-01T07:00:00Z", "--cron", cron_directory], capture_output=True, text=True)
            fires = json.loads(fall_back.stdout)
            self.assertEqual(sum(fire.startswith("2026-11-01T01:09") for fire in fires), 2, "an hourly rule fires twice in the repeated hour")

    def test_rejects_a_non_code_node(self):
        result = self.golden({"items": []}, node="Fetch")
        self.assertEqual(result.returncode, 2)


if __name__ == "__main__":
    unittest.main()
