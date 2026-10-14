#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""Inventory an n8n workflow export into a Dex import fidelity ledger.

Usage:
  n8n_inventory.py inventory EXPORT.json [--out DIRECTORY] [--catalog CATALOG.yaml]
  n8n_inventory.py verify LEDGER.md [--strict]

``inventory`` enumerates every node, connection, expression, credential
reference, literal secret, workflow setting, and known version-dependent
default, then writes ``ledger.md`` and ``inventory.json`` (or prints the ledger
when no directory is given). The ledger also carries a Mermaid graph of the
workflow, a draft connector capability matrix matched against a downloaded
Dex connector catalog, and a draft Dex plan. Literal secrets are redacted.

``verify`` fails while a ledger row is still ``todo``, while a ``diverged``,
``dropped``, ``blocked``, or ``pending`` row has no notes, or while a row that
``inventory.json`` (next to the ledger) generated is missing. ``pending`` marks
a proposal that still awaits the user's decision; ``--strict`` also fails on it.

The export is untrusted data: nothing in it is executed or followed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import defaultdict, deque
from pathlib import Path

STATUSES = ("mapped", "diverged", "dropped", "blocked", "pending")
DECISION_STATUSES = ("decided", "pending")
PROPOSAL_STATUSES = ("adopted", "deferred", "pending")
# Nodes that only reshape items; a run of them becomes one typed application Step.
GLUE_TYPES = {"set", "if", "filter", "switch", "code", "function", "functionItem", "noOp", "splitOut", "aggregate",
              "itemLists", "dateTime", "html", "markdown", "renameKeys", "sort", "limit", "summarize", "xml", "crypto"}
# Nodes whose read and write operations together suggest a table used as workflow state.
STATE_STORE_TYPES = {"googleSheets", "airtable", "notion", "postgres", "mySql", "supabase", "baserow", "nocoDb",
                     "dataTable", "redis", "mongoDb", "microsoftExcel"}
WRITE_VERBS = ("append", "update", "upsert", "create", "insert", "set", "delete", "push")
# Source defects that the optimized design fixes, with the fix stated for the user.
DEFECT_FIXES = {
    "schedule-label-mismatch": "Run on the schedule the name promises instead of the configured rule.",
    "webhook-output-shape": "Read the request body fields the author meant, such as body.email, instead of fields that are always empty.",
    "literal-set-mismatch": "Make the filter and the later extraction handle the same literals.",
    "raw-html-interpolation": "Escape provider text before it goes into HTML.",
    "model-output-body": "Escape or validate model output before it becomes a message body.",
    "unencoded-query": "Encode query values, so characters such as &, #, and + no longer change the request.",
    "first-item-only": "Process every input item, not only the first.",
    "merge-input": "Remove or rewire the Merge input that never receives items.",
    "open-trigger": "Restrict who can start the workflow with an allow-list in the Trigger filter.",
    "zero-items-stop": "Decide what happens on an empty result instead of silently doing nothing, such as running the empty-result handling the author wrote.",
    "evaluation-instant": "Evaluate the current time once per run, so every Step agrees on the same instant.",
    "wait-poll-delay": None,
    "wait-poll-loop": None,
    "repeated-effects": None,
    "overlapping-window": None,
}
NOTE_REQUIRED = ("diverged", "dropped", "blocked", "pending")
WEBHOOK_OUTPUT_FIELDS = {"headers", "params", "query", "body", "webhookUrl", "executionMode"}
# Template placeholders such as [Company Name]; not Markdown link text and not all-caps log tags.
INLINE_PLACEHOLDER = re.compile(r"\[(?=[^\]]*[a-z])[A-Z][A-Za-z0-9 ]{1,40}\](?!\()")
SEVERITY_ORDER = ("critical", "high", "medium", "info")

SECRET_NAME = re.compile(
    r"(?i)(api.?key|access.?token|auth.?token|refresh.?token|token|secret|"
    r"passw(or)?d|private.?key|authorization|bearer)"
)
URL_SECRET = re.compile(
    r"[?&]([A-Za-z_\-]*(?:key|token|secret|password)[A-Za-z_\-]*)=([^&{}\s#'\"]+)",
    re.IGNORECASE,
)
PLACEHOLDER = re.compile(
    r"(?i)^(your[_\-\s]|<.*>$|\[.*\]$|x{4,}|placeholder|changeme|change[_\-]me|insert[_\-\s]|replace[_\-\s]|enter[_\-\s])"
)
EMAIL = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
EXPRESSION_SEGMENT = re.compile(r"\{\{(.*?)\}\}", re.DOTALL)
NODE_REFERENCE = re.compile(
    r"""\$\(\s*(['"])(.+?)\1\s*\)(?:\.(item|first|last|all|itemMatching|params|isExecuted)\b)?"""
)
LEGACY_NODE_REFERENCE = re.compile(r"""\$node\[\s*(['"])(.+?)\1\s*\]""")
JSON_FIELD = re.compile(
    r"""(?:\$json|\.json)(?:\.([A-Za-z_$][\w$]*)|\[\s*(['"])(.+?)\2\s*\])"""
)
TIME_DEPENDENT = re.compile(r"\$today|\$now|DateTime\.|new Date\(|Date\.now\(")
ENVIRONMENT = re.compile(r"\$env\b|\$vars\b|\$secrets\b")
STRING_REPLACE = re.compile(r"""\.replace\(\s*(['"])(.*?)\1""")
UNGUARDED_METHOD = re.compile(
    r"\$json\.[A-Za-z_$][\w$]*\.(?:(toLowerCase|toUpperCase|trim|split|replace|"
    r"startsWith|endsWith|includes|match|map|forEach|filter)\(|length\b)"
)
# A Code node member chain on an item field without optional chaining, such as items[0].json.list.length.
UNGUARDED_CODE_READ = re.compile(
    r"\.json\.[A-Za-z_$][\w$]*(?:\.[A-Za-z_$][\w$]*)*\.(?:length\b|map\(|forEach\(|filter\(|toLowerCase\(|split\()"
    r"|\bfor\s*\(\s*(?:const|let|var)?\s*[A-Za-z_$][\w$]*\s+of\s+[^)]*\.json\.[A-Za-z_$][\w$.]*\s*\)"
)
LOCALE_DEPENDENT = re.compile(
    r"toLocale(?:String|DateString|TimeString)\(|\bIntl\.|toFormat\(\s*['\"](?:D{1,4}|t{1,4}|f{1,4}|F{1,4}|DD{0,3}\s*t{1,4})['\"]"
)
# Field names that hold a credential, as opposed to a pagination cursor or a counter.
CREDENTIAL_FIELD = re.compile(
    r"(?i)(api.?key|access.?token|auth.?token|refresh.?token|bot.?token|secret|passw(or)?d|private.?key|authorization|bearer|^token$|_token$)"
)
NON_CREDENTIAL_FIELD = re.compile(r"(?i)(page|next|cursor|continuation|sync|count|csrf|limit)")
# Export markers whose first release was cited from the n8n tags; extend only with cited markers.
RELEASE_MARKERS = (
    ("binaryMode", "settings.binaryMode first appears in n8n 2.5.0"),
    ("recurrenceRuleSignatures", "a Schedule Trigger staticData recurrenceRuleSignatures key is written from n8n 2.29.0, so the trigger was activated on 2.29 or later"),
    ("scheduleTrigger@1.4", "Schedule Trigger typeVersion 1.4 first ships in n8n 2.36.0"),
    ("__aiGatewayManaged", "a credential reference marked __aiGatewayManaged first appears in n8n 2.17.0"),
)
# Node-level keys every release writes; anything else is a candidate release marker.
BASELINE_NODE_KEYS = {"id", "name", "type", "typeVersion", "position", "parameters", "credentials", "disabled", "notes",
                      "notesInFlow", "webhookId", "retryOnFail", "maxTries", "waitBetweenTries", "alwaysOutputData",
                      "executeOnce", "onError", "continueOnFail"}
DAY_WINDOW = re.compile(r"\$today|startOf\(\s*['\"]day['\"]\s*\)")
EMPTY_RESULT_CHECK = re.compile(r"\.length\s*(?:===?\s*0|<\s*1)|!\s*[\w$.]+\.length\b")
IDENTIFIER_KEY = re.compile(
    r"(?i)^(channel|chat|calendar|spreadsheet|document|folder|base|table|list|board|sheet|page|"
    r"database|project|team|workspace|repository|group|drive|playlist|form|space|pipeline|portal)(id|_id|ids)?$"
)
EFFECT_VERBS = ("send", "sendmessage", "create", "post", "append", "update", "upsert", "delete", "reply", "publish", "upload")
TEMPLATE_INTERPOLATION = re.compile(r"\$\{([^{}]*)\}")

TRIGGER_TYPES = {
    "scheduleTrigger", "cron", "interval", "manualTrigger", "start", "webhook",
    "formTrigger", "errorTrigger", "executeWorkflowTrigger", "n8nTrigger",
    "chatTrigger",
}
DEX_HINTS = {
    "scheduleTrigger": "Scheduler Flow on the Cron pattern: a Timer to the next occurrence in the source timezone starts one run Flow per occurrence, named <scheduler Flow ID>-run-<occurrence date> for a daily or longer rule, or -run-<UTC time to the second such as 20261006T140930Z> for a sub-daily rule (Flow IDs reject /, $, and :).",
    "cron": "Scheduler Flow on the Cron pattern (legacy Cron node rules).",
    "interval": "Scheduler Flow on the Cron pattern with a fixed interval.",
    "manualTrigger": "Dex Web Start Flow with typed start input.",
    "start": "Dex Web Start Flow with typed start input.",
    "webhook": "webhook connector requestReceived Trigger; a synchronous response body needs a recorded capability check.",
    "respondToWebhook": "Synchronous webhook response: verify the webhook connector contract; likely a capability gap.",
    "formTrigger": "Dex Web Start Flow form or a participant Custom UI.",
    "errorTrigger": "Execute-failure recovery routes to an explicit recovery Step.",
    "executeWorkflowTrigger": "Typed start input of the Flow that replaces the sub-workflow.",
    "set": "Typed Go mapping inside the consuming application Step.",
    "if": "Go predicate in an application Step that chooses the next movement.",
    "filter": "Go predicate in an application Step; discarded items end without a movement.",
    "switch": "Go predicate in an application Step that chooses one movement per output.",
    "merge": "Join: await-all parallel Steps with a Channel count; replicate the merge mode exactly.",
    "noOp": "No behavior; drop and record.",
    "stickyNote": "Documentation only; drop from behavior and check its claims against the configuration.",
    "code": "Go application Step that ports the code, verified against golden output.",
    "function": "Go application Step that ports the code, verified against golden output.",
    "functionItem": "Go application Step that ports the code, verified against golden output.",
    "httpRequest": "Dedicated connector operation for that provider; an organization-controlled internal service follows the internal connector library decision.",
    "wait": "Timer Condition, or a Channel/RPC resume for webhook or form resumes.",
    "splitInBatches": "Bounded dynamic parallel Steps or batched Steps.",
    "executeWorkflow": "Steps in the same Flow, or an independent top-level Flow under the Core boundary rules; no SubFlow by default.",
    "splitOut": "Pure Go transform in an application Step.",
    "aggregate": "Pure Go transform in an application Step.",
    "itemLists": "Pure Go transform in an application Step.",
    "removeDuplicates": "Pure Go transform; cross-execution deduplication needs durable state.",
    "sort": "Pure Go transform in an application Step.",
    "limit": "Pure Go transform in an application Step.",
    "summarize": "Pure Go transform in an application Step.",
    "dateTime": "Pure Go transform; keep the source timezone.",
    "html": "Pure Go transform in an application Step.",
    "markdown": "Pure Go transform in an application Step.",
    "xml": "Pure Go transform in an application Step.",
    "crypto": "Pure Go transform; keys belong to configuration, never Flow state.",
}
APP_HINT = "Released connector operation that matches this resource and operation in the catalog; a missing one is a connector contribution."
LANGCHAIN_HINT = "llm connector Query or a durable Dex agent; tools become Steps."
TRIGGER_HINT = "Released connector Trigger if the catalog has it; otherwise a connector contribution."

DEFAULT_RESOURCE = {"googleCalendar": "event", "gmail": "message", "slack": "message", "telegram": "message"}
# Candidate Dex catalog connector IDs for common n8n node types; the catalog decides what is released.
N8N_CONNECTOR_CANDIDATES = {
    "gmail": ["gmail"], "gmailTrigger": ["gmail"], "googleCalendar": ["google-calendar"],
    "googleSheets": ["google-sheets"], "googleDrive": ["google-drive"], "googleDocs": ["google-docs"],
    "slack": ["slack"], "slackTrigger": ["slack"], "hubspot": ["hubspot"], "notion": ["notion"],
    "airtable": ["airtable"], "emailSend": ["email"], "emailReadImap": ["email"], "webhook": ["webhook"],
    "openAi": ["openai", "llm"], "lmChatOpenAi": ["llm", "openai"], "lmChatGoogleGemini": ["llm", "gemini"],
    "lmChatAnthropic": ["llm"], "agent": ["llm"], "chainLlm": ["llm"], "jira": ["jira"], "github": ["github"],
    "stripe": ["stripe"], "stripeTrigger": ["stripe"], "typeform": ["typeform"], "typeformTrigger": ["typeform"],
    "linear": ["linear"], "asana": ["asana"], "trello": ["trello"], "salesforce": ["salesforce"],
    "zendesk": ["zendesk-support"], "microsoftOutlook": ["outlook-mail", "outlook-calendar"],
    "microsoftTeams": ["microsoft-teams"], "postgres": ["postgresql"], "mySql": ["mysql"],
    "twilio": ["twilio-messaging"], "mailchimp": ["mailchimp"], "pipedrive": ["pipedrive"], "awsS3": ["amazon-s3"],
}
# n8n operation verbs and the catalog operation-name prefixes they usually correspond to.
OPERATION_VERBS = {
    "getall": ["list", "search"], "getmany": ["list", "search"], "search": ["search", "list"], "get": ["get", "read"],
    "generate": ["generate", "create"], "sendmessage": ["send", "post"], "analyze": ["create", "generate"],
    "send": ["send", "post", "reply"], "create": ["create", "upsert", "post", "add"], "update": ["update", "upsert"],
    "upsert": ["upsert", "update"], "delete": ["delete", "cancel", "remove"], "read": ["get", "read", "list"],
    "append": ["append", "add", "create"], "post": ["post", "send", "create"],
}
MULTI_ITEM_OPERATIONS = ("getall", "getmany", "search", "list", "read", "readrows")
# n8n's default operation when a node's parameters omit it.
DEFAULT_OPERATION = {"gmail": "send", "emailSend": "send", "telegram": "sendMessage", "slack": "post", "agent": "generate", "chainLlm": "generate"}
# LangChain sub-nodes configure the consuming agent or chain; they are not separate provider calls.
LANGCHAIN_SUB_NODE_PREFIXES = {
    "lmChat": "model of the consuming agent or chain: the llm connection's provider and model",
    "lmOpenAi": "model of the consuming agent or chain: the llm connection's provider and model",
    "embeddings": "embedding model of the consuming chain",
    "outputParser": "structured output (JSON Schema) of the consuming agent's llm generateText request",
    "memory": "conversation memory: keep turns in Flow Attributes",
    "tool": "agent tool: an application Step, or a Connector Step for a provider tool",
    "textSplitter": "text splitting inside an application Step",
    "documentLoader": "document loading inside an application Step",
}
HTML_TAG = re.compile(r"<\s*(table|tr|td|div|a|p|li|ul|span|b|i|br|img)\b", re.IGNORECASE)
HEADER_PARAMETERS = ("subject", "title", "headers", "header")
MESSAGE_BODY_PARAMETERS = ("message", "html", "text", "body", "bodyContent", "content")
EMAIL_NODE_TYPES = ("gmail", "emailSend", "microsoftOutlook", "sendGrid", "mailgun", "mailjet", "awsSes")
LANGCHAIN_ROOT_PREFIXES = ("agent", "chain", "openAi", "anthropic", "googleGemini", "informationExtractor", "textClassifier", "sentimentAnalysis")
RECIPIENT_PARAMETERS = ("sendTo", "toEmail", "to", "toRecipients", "ccList", "bccList")
CORE_PACKAGES = ("n8n-nodes-base", "@n8n/n8n-nodes-langchain")
STRUCTURAL_PARAMETERS = {"resource", "operation", "authentication", "options"}
# Required fields of common send operations, keyed by (node, resource, operation) with n8n's defaults
# filled in; confirm each against the n8n node source at the exported version.
REQUIRED_PARAMETERS = {
    ("emailSend", "", "send"): ("fromEmail", "toEmail"),
    ("gmail", "message", "send"): ("sendTo", "message"),
    ("telegram", "message", "sendMessage"): ("chatId", "text"),
}


def short_type(node_type: str) -> str:
    return node_type.rsplit(".", 1)[-1]


def node_version(node: dict) -> float:
    try:
        return float(node.get("typeVersion", 1))
    except (TypeError, ValueError):
        return 1.0


def is_trigger(node: dict) -> bool:
    kind = short_type(node.get("type", ""))
    return kind in TRIGGER_TYPES or kind.endswith("Trigger")


def walk(value, path=""):
    if isinstance(value, dict):
        for key, child in value.items():
            yield from walk(child, f"{path}.{key}" if path else str(key))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from walk(child, f"{path}[{index}]")
    else:
        yield path, value


def literal_text(text: str) -> str:
    """Return the non-expression part of a parameter value."""
    if text.startswith("="):
        return EXPRESSION_SEGMENT.sub("", text[1:])
    return text


def cell(text, limit: int = 220) -> str:
    rendered = " ".join(str(text).split())
    if len(rendered) > limit:
        rendered = rendered[: limit - 1] + "…"
    return rendered.replace("|", "\\|") or " "


class Inventory:
    def __init__(self, workflow: dict, source_name: str, catalog: list | None = None):
        self.workflow = workflow
        self.source_name = source_name
        self.catalog = catalog
        self.nodes = [node for node in workflow.get("nodes", []) if isinstance(node, dict)]
        self.by_name = {node.get("name", ""): node for node in self.nodes}
        self.secrets: set[str] = set()
        self.placeholders: set[tuple] = set()
        self.findings: list[dict] = []
        self.expressions: list[dict] = []
        self.edges: list[dict] = []
        self.credentials: dict[tuple, list[str]] = defaultdict(list)
        self.set_fields: list[dict] = []
        self.schedules: list[dict] = []
        self.claims: list[dict] = []
        self.node_rows: list[dict] = []
        self.grouped: dict[tuple, list[str]] = defaultdict(list)
        self.secret_fields: set[str] = set()
        self.ledger_ids: list[str] = []

    # ----- collection -------------------------------------------------
    def build(self) -> "Inventory":
        self.collect_edges()
        self.collect_secrets()
        if self.workflow.get("name"):
            self.claims.append({"source": "Workflow name", "text": str(self.workflow["name"])})
        for node in self.nodes:
            self.collect_node(node)
        self.emit_grouped()
        self.check_reachability()
        self.check_webhook_output_shape()
        self.check_branch_order()
        self.check_authentication_consistency()
        self.check_zero_item_stops()
        self.check_repeated_effects()
        self.check_secret_requests()
        self.check_model_output_bodies()
        self.check_merge_inputs()
        self.check_wait_loops()
        self.check_provenance()
        self.check_evaluation_instant()
        self.check_agent_exits()
        self.check_release_markers()
        self.check_preflight()
        self.check_open_triggers()
        self.check_references()
        self.check_dead_fields()
        self.check_unused_code_outputs()
        self.check_overlapping_windows()
        self.check_literal_sets()
        self.check_settings()
        self.findings.sort(key=lambda item: SEVERITY_ORDER.index(item["severity"]))
        return self

    def add_finding(self, severity: str, kind: str, node: str, message: str, blocks: bool = False) -> None:
        finding = {"severity": severity, "kind": kind, "node": node, "message": message}
        if blocks:
            # The node has a parameter issue that n8n's pre-execution check rejects.
            finding["blocks"] = True
        self.findings.append(finding)

    def group(self, kind: str, node: str, item: str) -> None:
        if item not in self.grouped[(kind, node)]:
            self.grouped[(kind, node)].append(item)

    def emit_grouped(self) -> None:
        for (kind, node), items in self.grouped.items():
            listed = ", ".join(f"`{item}`" for item in items)
            if kind == "timezone-dependent":
                self.add_finding("medium", kind, node,
                                 f"Reads the current time or date in the workflow timezone: {listed}. Compute it in the same IANA zone in Dex.")
            elif kind == "missing-field-empty":
                self.add_finding("medium", kind, node,
                                 f"Reads a member of an unguarded field in an expression: {listed}. For an item missing that field, n8n swallows the TypeError, so the value is empty and the node continues. "
                                 "Trace the empty value to the next predicate or provider call, record what happens there, and port that, not a failure.")
            elif kind == "missing-field-throws":
                self.add_finding("medium", kind, node,
                                 f"Code reads a member of an unguarded field: {listed}. An item missing that field throws, which fails the node and the whole execution; decide whether Dex reproduces the failure or treats it as a non-match.")
            elif kind == "hardcoded-address":
                self.add_finding("medium", kind, node,
                                 f"Literal address(es) {listed}; make them configuration or typed start input.")
            elif kind == "no-retry":
                self.add_finding("info", kind, node,
                                 f"n8n does not retry {listed} (retryOnFail is off); a Dex connector retries per its execution policy. Record that divergence.")
            elif kind == "missing-credential":
                self.add_finding("high", kind, node,
                                 f"No credential is attached to {listed}, so n8n cannot run them as exported and the account they act for is unknown. Ask which account each Dex connection authorizes.")
            elif kind == "hardcoded-identifier":
                self.add_finding("medium", kind, node,
                                 f"Literal identifier(s) {listed} name one account's resources; make them configuration or typed start input.")
            elif kind == "locale-dependent":
                runtime = ("the Code task runner's locale (en-US for an internal runner)"
                           if short_type(self.by_name.get(node, {}).get("type", "")) in ("code", "function", "functionItem") else "the main n8n process locale")
                self.add_finding("medium", kind, node,
                                 f"Formats with the runtime locale or a locale-dependent macro ({listed}); {runtime} and the timezone decide the text. "
                                 "Pin both in Go and capture goldens under the same locale.")
            elif kind == "community-node":
                self.add_finding("high", kind, node,
                                 f"Community node(s) {listed}: behavior and defaults come from that package, not n8n core, and the export does not record its version. Ask for the installed version and read it; until then keep the rows `blocked`.")
            elif kind == "operation-check":
                self.add_finding("info", kind, node,
                                 f"Check each resource and operation against the node's option list at its typeVersion: {listed}. n8n never validates these values, "
                                 "and an unknown one does something node-specific, from an error to an empty item with no provider call.")
            elif kind == "literal-placeholder":
                self.add_finding("medium", kind, node,
                                 f"Literal template placeholder(s) {listed} would be sent as written; ask for the real value or a field to fill it.")

    def collect_edges(self) -> None:
        connections = self.workflow.get("connections", {}) or {}
        for source, outputs in connections.items():
            if not isinstance(outputs, dict):
                continue
            for connection_type, branches in outputs.items():
                for index, targets in enumerate(branches or []):
                    for target in targets or []:
                        self.edges.append({
                            "from": source,
                            "type": connection_type,
                            "output": index,
                            "label": self.output_label(source, connection_type, index),
                            "to": target.get("node", ""),
                            "input": target.get("index", 0),
                        })

    def output_label(self, source: str, connection_type: str, index: int) -> str:
        if connection_type != "main":
            return connection_type
        node = self.by_name.get(source, {})
        kind = short_type(node.get("type", ""))
        if kind == "if":
            return "true" if index == 0 else "false"
        if kind == "filter":
            return "kept" if index == 0 else "discarded"
        if kind == "splitInBatches" and node_version(node) >= 3:
            return "done" if index == 0 else "loop"
        if kind == "switch":
            rules = node.get("parameters", {}).get("rules", {}).get("values", [])
            if index < len(rules) and rules[index].get("outputKey"):
                return f"output {index} ({rules[index]['outputKey']})"
            return f"output {index}"
        return "main" if index == 0 else f"output {index}"

    def collect_secrets(self) -> None:
        for node in self.nodes:
            name = node.get("name", "")
            parameters = node.get("parameters", {}) or {}
            for field in self.node_set_fields(node):
                value = field.get("value")
                if isinstance(value, str) and SECRET_NAME.search(field["name"] or ""):
                    self.secret_fields.add(field["name"])
                    self.record_secret(name, f"Set field `{field['name']}`", value)
            for path, value in walk(parameters):
                if not isinstance(value, str):
                    continue
                key = re.split(r"[.\[]", path)[-1]
                if SECRET_NAME.search(key) and not value.startswith("="):
                    self.record_secret(name, f"parameter `{path}`", value)
                for match in URL_SECRET.finditer(literal_text(value)):
                    self.record_secret(name, f"URL query `{match.group(1)}` in `{path}`", match.group(2))
            for entry in self.name_value_pairs(parameters):
                if SECRET_NAME.search(entry[0]) and not entry[1].startswith("="):
                    self.record_secret(name, f"header or query `{entry[0]}`", entry[1])

    def record_secret(self, node: str, where: str, value: str) -> None:
        value = value.strip()
        if PLACEHOLDER.search(value):
            if (node, value) in self.placeholders:
                return
            self.placeholders.add((node, value))
            self.add_finding(
                "info", "placeholder-secret", node,
                f"Placeholder credential in {where}; the real value belongs in the connector connection, never in source or Flow state.",
            )
            return
        if len(value) < 8 or value in self.secrets:
            return
        self.secrets.add(value)
        self.add_finding(
            "critical", "literal-secret", node,
            f"Literal secret in {where} ({len(value)} characters, redacted). Move it to the connector "
            "connection, keep it out of source, Flow state, fixtures, and logs, and rotate it if the "
            "export was shared.",
        )

    @staticmethod
    def name_value_pairs(parameters: dict):
        stack = [parameters]
        while stack:
            current = stack.pop()
            if isinstance(current, dict):
                name, value = current.get("name"), current.get("value")
                if isinstance(name, str) and isinstance(value, str):
                    yield name, value
                stack.extend(current.values())
            elif isinstance(current, list):
                stack.extend(current)

    @staticmethod
    def node_set_fields(node: dict) -> list[dict]:
        if short_type(node.get("type", "")) != "set":
            return []
        parameters = node.get("parameters", {}) or {}
        fields = []
        for entry in parameters.get("fields", {}).get("values", []) or []:
            kind = entry.get("type", "stringValue")
            fields.append({"name": entry.get("name", ""), "type": kind, "value": entry.get(kind, "")})
        for entry in parameters.get("assignments", {}).get("assignments", []) or []:
            fields.append({"name": entry.get("name", ""), "type": entry.get("type", "string"), "value": entry.get("value", "")})
        values = parameters.get("values", {})
        if isinstance(values, dict):
            for kind, entries in values.items():
                for entry in entries or []:
                    fields.append({"name": entry.get("name", ""), "type": kind, "value": entry.get("value", "")})
        return fields

    def collect_node(self, node: dict) -> None:
        name = node.get("name", "")
        kind = short_type(node.get("type", ""))
        version = node_version(node)
        parameters = node.get("parameters", {}) or {}

        for credential_type, reference in (node.get("credentials") or {}).items():
            label = reference.get("name", "") if isinstance(reference, dict) else str(reference)
            self.credentials[(credential_type, label)].append(name)
            if isinstance(reference, dict) and any(str(key).startswith("__") for key in reference):
                self.add_finding("medium", "managed-credential", name,
                                 f"The `{credential_type}` reference is n8n-managed ({', '.join(sorted(str(key) for key in reference if str(key).startswith('__')))}), so nothing about the account carries over. "
                                 "The Dex connection uses an account and key the user supplies.")
        self.collect_identifiers(name, kind, parameters)

        for field in self.node_set_fields(node):
            self.set_fields.append({"node": name, **field})
            if field["type"] == "numberValue" and isinstance(field["value"], str):
                self.add_finding("info", "type-coercion", name,
                                 f"Field `{field['name']}` is a number field holding the string {field['value']!r}; n8n coerces it to a number.")

        for path, value in walk(parameters):
            if isinstance(value, str) and kind not in ("stickyNote", "code", "function", "functionItem") and "cachedResult" not in path:
                for placeholder in sorted(set(INLINE_PLACEHOLDER.findall(literal_text(value)))):
                    self.group("literal-placeholder", name, f"{placeholder}` in `{path}")
            if isinstance(value, str) and value.startswith("="):
                self.collect_expression(node, path, value)
            elif isinstance(value, str) and path.split(".")[-1] in RECIPIENT_PARAMETERS and "," in value:
                self.add_finding("info", "recipient-list", name,
                                 f"`{path}` holds a comma-separated address list; Dex email connectors take one address per entry and reject duplicates.")
                for address in sorted(set(EMAIL.findall(value))):
                    self.group("hardcoded-address", name, f"{address}` in `{path}")
            elif isinstance(value, str) and kind != "stickyNote" and "cachedResult" not in path:
                for address in sorted(set(EMAIL.findall(value))):
                    self.group("hardcoded-address", name, f"{address}` in `{path}")

        if kind in ("code", "function", "functionItem"):
            self.collect_code(node)
        if kind == "scheduleTrigger" or kind == "cron":
            self.collect_schedule(node)
        if kind == "stickyNote":
            self.claims.append({"source": f"Sticky note `{name}`", "text": redact_addresses(parameters.get("content", "").strip())})
        if str(node.get("notes") or "").strip():
            self.claims.append({"source": f"Node note `{name}`", "text": redact_addresses(str(node["notes"]).strip())})
        branch_sources = {edge["from"] for edge in self.edges if edge["to"] == name and edge["type"] == "main"
                          and short_type(self.by_name.get(edge["from"], {}).get("type", "")) in ("if", "filter", "switch")}
        if kind in ("noOp", "stopAndError") or branch_sources:
            self.claims.append({"source": f"Node name `{name}`",
                                "text": f"{name}: compare what the name asserts with the items this node receives, including the zero-item case"})
        self.collect_settings_flags(node)
        self.collect_version_defaults(node)
        self.check_runnable(node)
        if kind == "httpRequest" or (self.is_app_node(node) and not is_trigger(node)):
            if not node.get("retryOnFail"):
                self.group("no-retry", f"{kind} nodes", name)

        self.node_rows.append({
            "node": name,
            "type": f"{kind}@{version:g}",
            "behavior": self.describe(node),
            "hint": self.hint(node),
        })

    def collect_identifiers(self, name: str, kind: str, parameters, path: str = "") -> None:
        """Group literal resource identifiers, such as a channel or spreadsheet ID, that belong in configuration."""
        if kind in ("stickyNote", "code", "function", "functionItem", "set"):
            return
        if isinstance(parameters, dict):
            for key, value in parameters.items():
                child = f"{path}.{key}" if path else str(key)
                if key == "cachedResultName" or key == "cachedResultUrl":
                    continue
                if isinstance(value, dict) and value.get("__rl"):
                    literal = value.get("value")
                    if IDENTIFIER_KEY.match(str(key)) and isinstance(literal, str) and literal and not literal.startswith("=") and not EMAIL.fullmatch(literal):
                        self.group("hardcoded-identifier", name, f"{literal}` in `{child}")
                elif isinstance(value, (dict, list)):
                    self.collect_identifiers(name, kind, value, child)
                elif IDENTIFIER_KEY.match(str(key)) and isinstance(value, str) and value and not value.startswith("=") and not EMAIL.fullmatch(value):
                    self.group("hardcoded-identifier", name, f"{value}` in `{child}")
        elif isinstance(parameters, list):
            for index, value in enumerate(parameters):
                self.collect_identifiers(name, kind, value, f"{path}[{index}]")

    def is_app_node(self, node: dict) -> bool:
        kind = short_type(node.get("type", ""))
        package = node.get("type", "").rsplit(".", 1)[0]
        if package == "@n8n/n8n-nodes-langchain":
            # Vendor nodes, such as openAi, call the provider directly; agents, chains, and sub-nodes do not.
            return bool((node.get("parameters", {}) or {}).get("resource")) and not langchain_sub_node_role(node) \
                and not kind.startswith(("agent", "chain")) and not is_trigger(node)
        return kind not in DEX_HINTS and kind not in TRIGGER_TYPES

    def check_runnable(self, node: dict) -> None:
        """Report integration nodes that the export cannot run as written."""
        name = node.get("name", "")
        kind = short_type(node.get("type", ""))
        package = node.get("type", "").rsplit(".", 1)[0]
        parameters = node.get("parameters", {}) or {}
        if package not in CORE_PACKAGES:
            self.group("community-node", package, name)
        if kind.startswith("lmChat") or kind.startswith("lmOpenAi") or (package == "@n8n/n8n-nodes-langchain" and "modelId" in parameters):
            model = parameters.get("model", parameters.get("modelName", parameters.get("modelId")))
            selected = model.get("value") if isinstance(model, dict) else model
            if "model" in parameters or "modelName" in parameters or "modelId" in parameters:
                if not selected:
                    self.add_finding("high", "hollow-node", name,
                                     "The node is a placeholder (missing required model selection), so n8n cannot run this model as exported. "
                                     "Ask which provider and model the Dex llm connection uses.", blocks=True)
        if kind_is_model(node) and not node.get("disabled"):
            options = parameters.get("options", {}) or {}
            exported = ", ".join(f"{key}={options[key]}" for key in ("maxRetries", "timeout") if key in options) or "no retry or timeout option set"
            self.add_finding("medium", "model-client-retry", name,
                             f"Model sub-nodes retry transient provider failures inside the LangChain client and apply a per-call timeout ({exported}); "
                             "read the node's option defaults and the LangChain library n8n pins at the release, compare them with the llm operation's execution policy, "
                             "and record a one-attempt cap as a divergence.")
        if not self.is_app_node(node) or node.get("disabled"):
            return
        resource = parameters.get("resource", DEFAULT_RESOURCE.get(kind, "(default)"))
        self.group("operation-check", "(workflow)", f"{name}: {kind}@{node_version(node):g} {resource}/{parameters.get('operation', DEFAULT_OPERATION.get(kind, '(default)'))}")
        if not node.get("credentials"):
            self.group("missing-credential", f"{kind} nodes", name)
        payload = set(parameters) - STRUCTURAL_PARAMETERS
        key = (kind, str(parameters.get("resource", DEFAULT_RESOURCE.get(kind, ""))),
               str(parameters.get("operation", DEFAULT_OPERATION.get(kind, ""))))
        missing = [field for field in REQUIRED_PARAMETERS.get(key, ()) if not parameters.get(field)]
        empty_locators = sorted(key for key, value in parameters.items()
                                if isinstance(value, dict) and value.get("__rl") and not value.get("value") and key not in ("model", "modelId", "modelName"))
        if empty_locators:
            self.add_finding("high", "hollow-node", name,
                             f"Empty resource locator(s) {', '.join(empty_locators)}: the export does not say which account resource the node acts on. "
                             "Ask the user; when the locator is required at this version, n8n also refuses to run executions that reach the node.", blocks=True)
        if not payload or missing:
            detail = f"missing required {', '.join(missing)}" if missing else "only its resource and operation are set"
            self.add_finding("high", "hollow-node", name,
                             f"The node is a placeholder ({detail}; check the node source at typeVersion {node_version(node):g}), so the export cannot do what the node is named for. "
                             "Its real behavior must come from the user, not from notes or the node name.", blocks=bool(missing))

    def hint(self, node: dict) -> str:
        kind = short_type(node.get("type", ""))
        if "langchain" in node.get("type", ""):
            return LANGCHAIN_HINT
        if kind in DEX_HINTS:
            return DEX_HINTS[kind]
        if is_trigger(node):
            return TRIGGER_HINT
        return APP_HINT

    def describe(self, node: dict) -> str:
        kind = short_type(node.get("type", ""))
        parameters = node.get("parameters", {}) or {}
        parts = []
        if node.get("disabled"):
            parts.append("DISABLED (passes items through unchanged)")
        extra = sorted(key for key in node if key not in (
            "id", "name", "type", "typeVersion", "position", "parameters", "credentials", "disabled", "notes"))
        if extra:
            parts.append("node fields: " + ", ".join(extra))
        if kind == "set":
            fields = ", ".join(
                f"{field['name']}={self.short_value(field['value'])}" for field in self.node_set_fields(node)
            )
            parts.append(f"sets {fields or 'no fields'}")
        elif kind in ("if", "filter"):
            parts.append(self.describe_conditions(parameters))
        elif kind == "httpRequest":
            parts.append(f"{parameters.get('method', 'GET')} {self.short_value(parameters.get('url', ''), 120)}")
        elif kind in ("code", "function", "functionItem"):
            code = parameters.get("jsCode") or parameters.get("functionCode") or parameters.get("pythonCode") or ""
            mode = parameters.get("mode", "runOnceForAllItems" if kind != "functionItem" else "runOnceForEachItem")
            parts.append(f"{parameters.get('language', 'javaScript')}, {mode}, {len(code.splitlines())} lines")
        elif kind in ("scheduleTrigger", "cron"):
            parts.append("; ".join(schedule["description"] for schedule in self.schedules if schedule["node"] == node.get("name")))
        elif kind == "noOp":
            parts.append("no effect")
        elif kind == "stickyNote":
            parts.append("note: " + self.short_value(parameters.get("content", "").strip(), 120))
        else:
            resource = parameters.get("resource", DEFAULT_RESOURCE.get(kind, ""))
            operation = parameters.get("operation", DEFAULT_OPERATION.get(kind, ""))
            if self.is_app_node(node) and not is_trigger(node):
                resource = resource or "(node default resource)"
                operation = operation or "(node default operation)"
            if resource or operation:
                parts.append("/".join(part for part in (resource, operation) if part))
            for key, value in parameters.items():
                if key in ("resource", "operation", "options"):
                    continue
                if isinstance(value, dict) and value.get("__rl"):
                    parts.append(f"{key}={value.get('value')} (fixed resource)")
                elif isinstance(value, (str, int, float, bool)) and value != "":
                    parts.append(f"{key}={self.short_value(value, 80)}")
                elif isinstance(value, (dict, list)) and value:
                    parts.append(f"{key}={self.short_value(value, 120)}")
            options = parameters.get("options")
            if isinstance(options, dict):
                parts.append("options: " + (", ".join(sorted(options)) or "{} (an empty collection: its children are unset, so a code-level fallback for the whole collection does not apply)"))
        return "; ".join(part for part in parts if part)

    def short_value(self, value, limit: int = 60) -> str:
        # Redact before truncating, so a cut inside a secret never leaves its prefix behind.
        text = self.redact(json.dumps(value) if not isinstance(value, str) else value)
        return text if len(text) <= limit else text[: limit - 1] + "…"

    @staticmethod
    def describe_conditions(parameters: dict) -> str:
        block = parameters.get("conditions", {}) or {}
        conditions = block.get("conditions")
        if isinstance(conditions, list):
            rendered = []
            for condition in conditions:
                operator = condition.get("operator", {})
                rendered.append(
                    f"{condition.get('leftValue', '')} {operator.get('operation', '?')} {json.dumps(condition.get('rightValue', ''))}"
                )
            options = block.get("options", {})
            return (
                f" {block.get('combinator', 'and').upper()} ".join(rendered)
                + f" (caseSensitive={json.dumps(options.get('caseSensitive', True))}, typeValidation={options.get('typeValidation', 'strict')})"
            )
        return "legacy conditions: " + json.dumps(block)[:160]

    def collect_expression(self, node: dict, path: str, value: str) -> None:
        name = node.get("name", "")
        body = value[1:]
        segments = EXPRESSION_SEGMENT.findall(body) or [body]
        semantics = []
        references = sorted({match.group(2) for match in NODE_REFERENCE.finditer(body)}
                            | {match.group(2) for match in LEGACY_NODE_REFERENCE.finditer(body)})
        if references:
            semantics.append("reads node(s) " + ", ".join(references))
        if re.search(r"\$\([^)]*\)\.item\b", body):
            semantics.append("paired-item lineage: carry the upstream field explicitly in Dex Step input")
            self.add_finding("info", "paired-item", name,
                             f"`{path}` resolves an upstream item through paired-item lineage; Dex has no implicit lineage, so carry that field in the Step input or an AttributeMap entry.")
        fields = sorted({match.group(1) or match.group(3) for match in JSON_FIELD.finditer(body)})
        if fields:
            semantics.append("fields " + ", ".join(fields))
        if TIME_DEPENDENT.search(body):
            semantics.append("timezone-dependent (workflow timezone)")
            self.group("timezone-dependent", name, path)
        if LOCALE_DEPENDENT.search(body):
            semantics.append("locale-dependent formatting")
            self.group("locale-dependent", name, path)
        if ENVIRONMENT.search(body):
            semantics.append("reads instance environment or variables")
            self.add_finding("high", "environment", name,
                             f"`{path}` reads $env/$vars/$secrets, which the export does not contain; map each to configuration.")
        for match in STRING_REPLACE.finditer(body):
            semantics.append(f"String.replace({match.group(2)!r}) replaces only the first occurrence")
        if UNGUARDED_METHOD.search(body):
            semantics.append("a missing field makes this segment empty: n8n swallows the TypeError and the node continues")
            self.group("missing-field-empty", name, path)
        if path.split(".")[-1] in HEADER_PARAMETERS and (JSON_FIELD.search(body) or NODE_REFERENCE.search(body)):
            semantics.append("an upstream value can carry a line break into a header")
            if short_type(node.get("type", "")) in EMAIL_NODE_TYPES:
                message = (f"`{path}` interpolates an upstream value into a mail header. n8n's mail composer turns each line break into one space (check at the nodemailer "
                           "version n8n pins), and the Dex email connectors reject CR or LF, so the faithful port applies the same replacement before the Connector Step.")
            else:
                message = f"`{path}` interpolates an upstream value into a header; decide how a line break in it is handled."
            self.add_finding("info", "header-line-break", name, message)
        if path.split(".")[-1].lower() == "url" and "?" in body:
            query = body.split("?", 1)[1]
            if "{{" in query and "encodeURIComponent" not in query:
                semantics.append("interpolates into the query string without encoding")
                self.add_finding("medium", "unencoded-query", name,
                                 f"`{path}` concatenates values into the query string. n8n parses the URL as a WHATWG URL: a # drops every later parameter, tab, CR, and LF are deleted, "
                                 "& splits the value, + stays literal, and spaces and non-ASCII are percent-encoded. Golden the sent URL with n8n_expression_golden.mjs, "
                                 "and record proper encoding in Dex as a divergence.")
        self.expressions.append({
            "node": name,
            "parameter": path,
            "expression": " ⏎ ".join(segment.strip() for segment in segments),
            "semantics": semantics,
            "references": references,
        })

    def collect_code(self, node: dict) -> None:
        name = node.get("name", "")
        parameters = node.get("parameters", {}) or {}
        language = parameters.get("language", "javaScript")
        code = parameters.get("jsCode") or parameters.get("functionCode") or parameters.get("pythonCode") or ""
        if language.lower().startswith("python"):
            self.add_finding("high", "python-code", name,
                             "Python Code node: the golden harness runs only JavaScript; capture golden output from an n8n execution instead.")
        if "console.log" in code:
            self.add_finding("info", "debug-logging", name,
                             "console.log output is not saved with the execution (a manual run shows it in the browser console); drop it and record the drop.")
        interpolations = [" ".join(match.group(1).split()) for match in TEMPLATE_INTERPOLATION.finditer(code)]
        interpolations = sorted({item for item in interpolations if item})
        if interpolations:
            self.add_finding("medium", "js-coercion", name,
                             f"{len(interpolations)} template-literal interpolation(s), such as "
                             + ", ".join(f"`{item[:60]}`" for item in interpolations[:4])
                             + "; JavaScript renders null as 'null', undefined as 'undefined', and objects as '[object Object]', and NaN serializes as null in output items. The Go port must match the golden output.")
        if HTML_TAG.search(code) and interpolations and "escape" not in code.lower():
            self.add_finding("medium", "raw-html-interpolation", name,
                             "The code inserts upstream values into HTML without escaping, so provider markup renders and a stray < or & changes the page. Port it byte for byte first, then decide escaping explicitly.")
        mode = parameters.get("mode", "runOnceForAllItems")
        if node_version(node) >= 2 and short_type(node.get("type", "")) == "code" and mode == "runOnceForAllItems":
            first_item_reads = sorted(set(re.findall(r"\$input\.item\b|\$json\b", code)))
            if first_item_reads:
                self.add_finding("high", "first-item-only", name,
                                 f"Run Once for All Items mode, yet the code reads {', '.join(first_item_reads)}, which n8n resolves to the first input item only; "
                                 "the other items are ignored unless the code iterates $input.all(). Port that behavior, or record the fix as a decision.")
        if TIME_DEPENDENT.search(code):
            self.group("timezone-dependent", name, "code")
        for match in LOCALE_DEPENDENT.finditer(code):
            self.group("locale-dependent", name, match.group(0))
        for match in UNGUARDED_CODE_READ.finditer(code):
            self.group("missing-field-throws", name, " ".join(match.group(0).split())[:60])
        if re.search(r"\bfor\s*\(\s*[A-Za-z_$][\w$]*\s+of\b", code):
            self.add_finding("info", "js-implicit-global", name, "A for-of loop assigns an undeclared variable (sloppy-mode implicit global); keep the harness in sloppy mode.")
        for match in NODE_REFERENCE.finditer(code):
            self.expressions.append({
                "node": name, "parameter": "code", "expression": match.group(0),
                "semantics": ["reads node " + match.group(2)], "references": [match.group(2)],
            })
        self.add_finding("high", "code-port", name,
                         f"Port this Code node to a Go application Step and prove parity with golden output from `node n8n_code_golden.mjs EXPORT {json.dumps(name)} FIXTURE.json`, including empty and null-valued inputs.")

    def collect_schedule(self, node: dict) -> None:
        name = node.get("name", "")
        kind = short_type(node.get("type", ""))
        parameters = node.get("parameters", {}) or {}
        rules = []
        if kind == "scheduleTrigger":
            for rule in parameters.get("rule", {}).get("interval", []) or [{}]:
                rules.append(describe_schedule_rule(rule))
        else:
            for item in parameters.get("triggerTimes", {}).get("item", []) or []:
                rules.append({"description": f"legacy Cron node rule {json.dumps(item)}", "field": item.get("mode", ""), "hour": item.get("hour"), "cron": ""})
        workflow_id, node_id = self.workflow.get("id"), node.get("id")
        if kind == "scheduleTrigger":
            if workflow_id and node_id:
                digest = hashlib.sha256(f"{workflow_id}:{node_id}:second".encode()).digest()
                second = int.from_bytes(digest[:4], "big") % 60
                detail = f"from n8n 2.19 the stable second is {second}, computed from workflow ID {workflow_id} and node ID {node_id}"
            else:
                detail = "the export has no workflow ID, so the stable second from n8n 2.19 is unknown; ask for the ID, which is in the workflow's URL"
            self.add_finding("info", "schedule-second", name,
                             f"Every rule except seconds and cron rules fires at a jittered second: {detail}. Earlier releases use 0 or a second picked at each activation.")
        for rule in rules:
            self.schedules.append({"node": name, **rule})
            mismatch = schedule_label_mismatch(name, rule)
            if mismatch:
                self.add_finding("high", "schedule-label-mismatch", name,
                                 f"The name says {mismatch!r} but the rule fires {rule['description']}. The configured rule is the source behavior; ask which one the user intends.")
        self.claims.append({"source": f"Trigger name `{name}`", "text": name})

    def collect_settings_flags(self, node: dict) -> None:
        name = node.get("name", "")
        if node.get("disabled"):
            self.add_finding("medium", "disabled-node", name, "Disabled node: n8n skips it and passes items through unchanged.")
        on_error = node.get("onError") or ("continueRegularOutput" if node.get("continueOnFail") else "")
        if on_error and on_error != "stopWorkflow":
            self.add_finding("high", "on-error", name, f"onError={on_error}: failures become items or an error output instead of stopping the execution.")
        if node.get("alwaysOutputData"):
            self.add_finding("medium", "always-output-data", name, "alwaysOutputData: emits one empty item when there is no output, so downstream nodes still run.")
        if node.get("executeOnce"):
            self.add_finding("medium", "execute-once", name, "executeOnce: runs only for the first input item.")
        if node.get("retryOnFail"):
            attempts = min(5, max(2, int(node.get("maxTries") or 3)))
            wait = min(5000, max(0, int(node.get("waitBetweenTries") or 1000)))
            self.add_finding("info", "retry", name,
                             f"retryOnFail: {attempts} attempts {wait} ms apart (n8n clamps maxTries to 2 through 5 and the wait to 5000 ms); map to the Step retry policy.")

    def collect_version_defaults(self, node: dict) -> None:
        name = node.get("name", "")
        kind = short_type(node.get("type", ""))
        version = node_version(node)
        parameters = node.get("parameters", {}) or {}
        options = parameters.get("options", {}) or {}
        note = None
        if kind == "set":
            if version < 3.3:
                note = "Set before v3.3 keeps all input fields next to the set fields by default; downstream reads may rely on those pass-through fields."
            elif not parameters.get("includeOtherFields"):
                note = "Set v3.3+ drops input fields unless includeOtherFields is on."
            if version >= 3:
                conversion = {3.0: "a string field that resolves to null or undefined becomes the text 'null' or 'undefined'",
                              3.1: "a string field that resolves to null or undefined fails the node unless ignoreConversionErrors is on"}.get(
                    version, "a field that resolves to null or undefined becomes null")
                extra = f"At v{version:g}, {conversion}; binary data is dropped{' unless input fields are kept' if version >= 3.4 else ' unless includeBinary is set'}."
                note = f"{note} {extra}" if note else extra
        elif kind == "gmail" and version >= 2.1 and parameters.get("operation", "send") == "send":
            if options.get("appendAttribution", True):
                note = ("Gmail v2.1+ send appends the footer 'This email was sent automatically with n8n' unless options.appendAttribution is false (reply never appends it). "
                        f"emailType is {options.get('emailType', parameters.get('emailType', 'html (default)'))}; html mail has no text/plain part, and the message is trimmed. Record the footer as a divergence.")
        elif kind == "emailSend" and version >= 2.1 and options.get("appendAttribution", True):
            note = "Send Email v2.1 appends 'This email was sent automatically with n8n' unless options.appendAttribution is false. Record the footer as a divergence."
        elif kind == "telegram" and parameters.get("resource", "message") == "message" and parameters.get("operation", "sendMessage") == "sendMessage":
            fields = parameters.get("additionalFields", {}) or {}
            parse_mode = fields.get("parse_mode", "Markdown")
            note = f"Telegram sendMessage sends parse_mode {parse_mode} ({'the default when unset' if 'parse_mode' not in fields else 'set'}), so the text is parsed as markup."
            if version >= 1.1 and fields.get("appendAttribution", True) and parse_mode in ("Markdown", "HTML"):
                note += " v1.1+ appends 'This message was sent automatically with n8n' unless appendAttribution is false."
            if version >= 1.2:
                note += " v1.2+ disables link previews by default."
        elif (kind == "slack" and version >= 2.1 and parameters.get("operation", "post") in ("post", "update")
              and (parameters.get("otherOptions", {}) or {}).get("includeLinkToWorkflow", True) is not False):
            note = "Slack v2.1+ appends an 'Automated with this n8n workflow' link unless otherOptions.includeLinkToWorkflow is false. Record it as a divergence."
        elif (kind == "microsoftTeams" and version >= 1.1 and parameters.get("operation", "create") == "create"
              and options.get("includeLinkToWorkflow", True) is not False):
            note = "Microsoft Teams v1.1+ appends a 'Powered by this n8n workflow' link and sends HTML unless includeLinkToWorkflow is false. Record it as a divergence."
        elif kind == "googleCalendar" and parameters.get("operation") == "getAll":
            if not parameters.get("returnAll"):
                note = f"Google Calendar getAll returns one page of {parameters.get('limit', 50)} events (default 50) without paging; order is unspecified unless options.orderBy is set."
        elif kind == "httpRequest" and version >= 3:
            if not options.get("response", {}).get("response", {}).get("neverError"):
                note = ("HTTP Request v3+: every item's request starts at once (options.batching only spaces the starts), and a non-2xx response fails the node "
                        "and the execution after all requests settle; the timeout is 300000 ms unless options.timeout is set.")
            if version >= 4 and not (options.get("redirect") or {}):
                note = (note + " " if note else "") + ("With options.redirect unset, v4+ follows redirects"
                                                       + (", sending credentials on a cross-origin redirect before v4.4." if version < 4.4 else "."))
        elif (kind in ("if", "filter") and version >= 2) or (kind == "switch" and version >= 3):
            note = (f"{kind} v{version:g}: typeValidation {((parameters.get('conditions', {}) or {}).get('options', {}) or {}).get('typeValidation', 'strict')}, but null and undefined pass it; "
                    "string operators compare leftValue ?? '' and exists/notExists test null, undefined, and NaN. A TypeError inside a condition expression leaves it empty instead of failing. "
                    "Only a type mismatch of a present value or an n8n ExpressionError fails the node.")
        elif kind == "code":
            note = f"Code v2 mode is {parameters.get('mode', 'runOnceForAllItems')}; runOnceForEachItem returns one item per input item."
        elif kind == "scheduleTrigger":
            note = ("Schedule Trigger fires in the workflow timezone (settings.timezone, else the instance GENERIC_TIMEZONE, which defaults to America/New_York). "
                    "An omitted hour or minute is 0, and the second is jittered except on seconds and cron rules. Executions may overlap. "
                    "The default scheduler never runs a missed occurrence; deduplication per scheduled time exists from n8n 2.19, and misfire options from typeVersion 1.4 on the opt-in durable scheduler. Record the instance's release.")
        elif kind == "wait":
            resume = parameters.get("resume", "timeInterval")
            if resume == "timeInterval":
                amount = parameters.get("amount", 1 if version < 1.1 else 5)
                unit = parameters.get("unit", "hours" if version < 1.1 else "seconds")
                seconds = {"seconds": 1, "minutes": 60, "hours": 3600, "days": 86400}.get(str(unit), 0) * (amount if isinstance(amount, (int, float)) else 0)
                unit_text = str(unit)[:-1] if amount == 1 and str(unit).endswith("s") else unit
                note = (f"Wait resumes {amount} {unit_text} after the Wait node runs ({'defaults: amount 1, unit hours' if version < 1.1 else 'defaults: amount 5, unit seconds'})"
                        + ("; a day is exactly 24 hours, not a calendar day" if unit == "days" else "") + ". "
                        + ("The execution sleeps in place (under 65 seconds), and sibling branches that have not run wait too."
                           if 0 < seconds < 65 else
                           "A wait of 65 seconds or more pauses the whole execution, including sibling branches that have not run, "
                           "and resumes it on the workflow as it was when the execution started."))
            else:
                note = (f"Wait resume={resume}: the execution pauses until {'a call to its resume URL' if resume == 'webhook' else 'the form is submitted' if resume == 'form' else 'the specified time'}"
                        f"{'; limitWaitTime is off by default, so it can wait forever' if resume in ('webhook', 'form') else ''}.")
        elif kind == "webhook":
            method = parameters.get("httpMethod", "GET (default)")
            note = (f"Webhook httpMethod {method}, authentication {parameters.get('authentication', 'none (default)')}, responseMode {parameters.get('responseMode', 'onReceived (default)')}. "
                    "Each request starts its own execution with one item {headers, params, query, body, webhookUrl, executionMode}: a JSON body is parsed into body, "
                    "urlencoded and multipart fields land in body as strings, multipart files in binary. onReceived answers 200 {\"message\":\"Workflow was started\"}. "
                    "The Dex webhook Trigger accepts only verified POST requests with a JSON or urlencoded body up to its size limit and deduplicates by event ID, "
                    "so record each difference.")
        if note:
            self.add_finding("medium", "version-default", name, note + f" (typeVersion {version:g}; check the n8n node source at the release.)")

    # ----- graph checks -------------------------------------------------
    def adjacency(self):
        forward, backward = defaultdict(set), defaultdict(set)
        for edge in self.edges:
            forward[edge["from"]].add(edge["to"])
            backward[edge["to"]].add(edge["from"])
        return forward, backward

    def check_reachability(self) -> None:
        forward = defaultdict(set)
        for edge in self.edges:
            if edge["type"] == "main":
                forward[edge["from"]].add(edge["to"])
        reached, queue = set(), deque(node.get("name", "") for node in self.nodes if is_trigger(node))
        while True:
            while queue:
                current = queue.popleft()
                if current in reached:
                    continue
                reached.add(current)
                queue.extend(forward[current] - reached)
            # A sub-node, such as a chat model or tool, attaches to its consumer
            # through a non-main connection and runs when that consumer runs.
            attached = {
                edge["from"] for edge in self.edges
                if edge["type"] != "main" and edge["to"] in reached and edge["from"] not in reached
            }
            if not attached:
                break
            queue.extend(attached)
        for node in self.nodes:
            if short_type(node.get("type", "")) == "stickyNote":
                continue
            if node.get("name", "") not in reached:
                self.add_finding("medium", "unreachable-node", node.get("name", ""),
                                 "No trigger reaches this node, so it never runs in production; drop it and record why.")
        for node in self.nodes:
            kind = short_type(node.get("type", ""))
            if kind in ("if", "filter"):
                used = {edge["output"] for edge in self.edges if edge["from"] == node.get("name") and edge["type"] == "main"}
                for index in (0, 1):
                    if kind == "filter" and index == 1:
                        continue
                    if index not in used:
                        label = self.output_label(node.get("name", ""), "main", index)
                        self.add_finding("info", "unconnected-output", node.get("name", ""),
                                         f"The {label} output has no target; those items end silently.")

    def check_webhook_output_shape(self) -> None:
        """Webhook items wrap the request as {headers, params, query, body}; direct reads of other fields are undefined."""
        for node in self.nodes:
            if short_type(node.get("type", "")) != "webhook":
                continue
            children = {edge["to"] for edge in self.edges if edge["from"] == node.get("name") and edge["type"] == "main"}
            for expression in self.expressions:
                if expression["node"] not in children:
                    continue
                for match in re.finditer(r"\$json\.([A-Za-z_$][\w$]*)", expression["expression"]):
                    if match.group(1) not in WEBHOOK_OUTPUT_FIELDS:
                        self.add_finding("high", "webhook-output-shape", expression["node"],
                                         f"`{expression['parameter']}` reads $json.{match.group(1)}, but a Webhook item holds the request under headers, params, query, and body, so the value is undefined in n8n (did the author mean $json.body.{match.group(1)}?). Record whether Dex reproduces the undefined read or fixes it.")

    def check_branch_order(self) -> None:
        """Under executionOrder v1, n8n runs fan-out branches top to bottom by canvas position."""
        if (self.workflow.get("settings", {}) or {}).get("executionOrder", "v0") != "v1":
            return
        forward, _ = self.adjacency()
        targets_by_output = defaultdict(list)
        for edge in self.edges:
            if edge["type"] == "main":
                targets_by_output[(edge["from"], edge["output"])].append(edge["to"])
        for (source, _), targets in targets_by_output.items():
            if len(targets) < 2:
                continue
            ordered = sorted(targets, key=lambda target: tuple((self.by_name.get(target, {}).get("position") or [0, 0])[::-1]))
            self.add_finding("info", "branch-order", source,
                             f"executionOrder v1 runs these branches top to bottom on the canvas: {', '.join(ordered)}. A failure in an earlier branch stops the later ones.")
            for index, target in enumerate(ordered[:-1]):
                branch = {target} | self.descendants(target)
                waits = [name for name in branch if short_type(self.by_name.get(name, {}).get("type", "")) == "wait"]
                if waits:
                    self.add_finding("medium", "branch-wait-pause", source,
                                     f"The {target} branch contains Wait node(s) {', '.join(sorted(waits))}; n8n pauses the whole execution there, so the later branches ({', '.join(ordered[index + 1:])}) run only after the wait ends.")

    def is_multi_item_read(self, node: dict) -> bool:
        parameters = node.get("parameters", {}) or {}
        operation = str(parameters.get("operation", "")).lower()
        # A limit or returnAll parameter marks a list read even when the operation is the node default.
        is_list = operation in MULTI_ITEM_OPERATIONS or (
            self.is_app_node(node) and ("returnAll" in parameters or "limit" in parameters) and not is_trigger(node))
        return is_list and not node.get("disabled") and not self.is_effect_node(node)

    def is_effect_node(self, node: dict) -> bool:
        kind = short_type(node.get("type", ""))
        if not (self.is_app_node(node) or kind == "httpRequest") or is_trigger(node) or node.get("disabled"):
            return False
        parameters = node.get("parameters", {}) or {}
        if kind == "httpRequest":
            return str(parameters.get("method", "GET")).upper() not in ("GET", "HEAD")
        operation = str(parameters.get("operation", DEFAULT_OPERATION.get(kind, ""))).lower()
        return any(operation.startswith(verb) for verb in EFFECT_VERBS)

    def check_zero_item_stops(self) -> None:
        """A node that outputs no items stops its branch: downstream nodes do not run and the execution succeeds."""
        main_children = defaultdict(set)
        for edge in self.edges:
            if edge["type"] == "main":
                main_children[edge["from"]].add(edge["to"])
        for node in self.nodes:
            name = node.get("name", "")
            if not self.is_multi_item_read(node) or not main_children[name] or node.get("alwaysOutputData"):
                continue
            downstream = self.descendants(name)
            def handles_empty(code: str) -> bool:
                # An explicit length check, or a length read with a literal default such as `|| 'None'`.
                return bool(EMPTY_RESULT_CHECK.search(code) or (re.search(r"\.length\b", code) and re.search(r"\|\|\s*['\"`]", code)))
            fallbacks = sorted(
                other for other in downstream
                if handles_empty(str((self.by_name.get(other, {}).get("parameters", {}) or {}).get("jsCode", "")))
            )
            message = (f"When this read returns no items, n8n runs none of {', '.join(sorted(main_children[name]))} or anything after them, "
                       "and the execution still succeeds. A Dex Flow that continues with an empty list changes the effects; record which behavior Dex keeps.")
            if fallbacks:
                message += f" The empty-result handling in {', '.join(fallbacks)} never runs in n8n (dead code unless alwaysOutputData is on)."
            self.add_finding("high" if fallbacks else "medium", "zero-items-stop", name, message)

    def check_repeated_effects(self) -> None:
        """A schedule that fires more often than its read window repeats the same effects every execution."""
        for schedule in self.schedules:
            if schedule.get("field") not in ("seconds", "minutes", "hours"):
                continue
            downstream = self.descendants(schedule["node"], "main")
            # A whole-day window counts only where a read node uses it, not where an effect merely prints the date.
            windowed = sorted({
                expression["node"] for expression in self.expressions
                if expression["node"] in downstream and DAY_WINDOW.search(expression["expression"])
                and not self.is_effect_node(self.by_name.get(expression["node"], {}))
                and (self.is_app_node(self.by_name.get(expression["node"], {})) or short_type(self.by_name.get(expression["node"], {}).get("type", "")) == "httpRequest")
            })
            effects = sorted({name for read in windowed for name in self.descendants(read, "main") if self.is_effect_node(self.by_name.get(name, {}))})
            if windowed and effects:
                self.add_finding("high", "repeated-effects", schedule["node"],
                                 f"The schedule fires {schedule['description']}, but {', '.join(windowed)} read a whole-day window, so every execution repeats "
                                 f"{', '.join(effects)} for the same items; n8n keeps no memory between executions. Record the repetition as source behavior and ask whether Dex keeps it.")
                return

    def check_secret_requests(self) -> None:
        """A credential that travels from workflow data into a request belongs to the connector connection."""
        for expression in self.expressions:
            node = self.by_name.get(expression["node"], {})
            if not (short_type(node.get("type", "")) == "httpRequest" or self.is_app_node(node)) or node.get("disabled"):
                continue
            parameter = expression["parameter"].lower()
            fields = set(re.findall(r"(?:\$json|\.json)\.([A-Za-z_$][\w$]*)", expression["expression"]))
            secrets = sorted(field for field in fields
                             if field in self.secret_fields or (CREDENTIAL_FIELD.search(field) and not NON_CREDENTIAL_FIELD.search(field)))
            if not secrets:
                continue
            where = "URL" if re.split(r"[.\[]", parameter)[-1] == "url" or "queryparameters" in parameter else "request"
            self.add_finding("high", "secret-in-request", expression["node"],
                             f"`{expression['parameter']}` sends the credential field(s) {', '.join(f'`{field}`' for field in secrets)} from workflow data in the {where}"
                             + (", where provider, proxy, and n8n logs record it" if where == "URL" else "")
                             + ". In Dex the credential belongs to the connector connection, sent as its manifest declares; record the divergence. "
                             "When the credential is embedded in a URL handed to another provider, the Dex design fetches the content itself instead of forwarding the URL.")

    def check_model_output_bodies(self) -> None:
        """Email bodies built from model output carry whatever markup the model returns."""
        model_nodes = {
            node.get("name", "") for node in self.nodes
            if "langchain" in node.get("type", "") and short_type(node.get("type", "")).startswith(LANGCHAIN_ROOT_PREFIXES)
        }
        if not model_nodes:
            return
        for expression in self.expressions:
            node = self.by_name.get(expression["node"], {})
            if short_type(node.get("type", "")) not in EMAIL_NODE_TYPES:
                continue
            if re.split(r"[.\[]", expression["parameter"])[-1] not in MESSAGE_BODY_PARAMETERS:
                continue
            sources = (set(expression["references"]) | self.ancestors(expression["node"])) & model_nodes
            if sources:
                self.add_finding("medium", "model-output-body", expression["node"],
                                 f"`{expression['parameter']}` sends output of {', '.join(sorted(sources))} as the message body without escaping or validation; provider text that reached the model can add markup or links. Port it as is, then decide escaping or an allowlist explicitly.")

    def check_merge_inputs(self) -> None:
        for node in self.nodes:
            if short_type(node.get("type", "")) != "merge":
                continue
            name = node.get("name", "")
            parameters = node.get("parameters", {}) or {}
            inputs = int(parameters.get("numberInputs", 2) or 2)
            unreachable = {item["node"] for item in self.findings if item["kind"] == "unreachable-node"}
            for index in range(inputs):
                feeders = [edge["from"] for edge in self.edges if edge["to"] == name and edge["type"] == "main" and edge.get("input", 0) == index]
                live = [feeder for feeder in feeders if feeder not in unreachable]
                if not live:
                    detail = f"is fed only by unreachable {', '.join(feeders)}" if feeders else "has no connection"
                    mode = parameters.get("mode", "append")
                    if mode == "chooseBranch" and index < 2:
                        outcome = "Choose-branch requires inputs 1 and 2, so under executionOrder v1 the Merge and everything after it never run, and the execution ends without an error."
                    elif mode == "chooseBranch":
                        outcome = ("Choose-branch requires only inputs 1 and 2, so under executionOrder v1 the Merge runs once the execution stack drains, "
                                   "emitting the chosen branch without this input.")
                    else:
                        outcome = f"Mode {mode} runs with that input empty under executionOrder v1; confirm at typeVersion {node_version(node):g}."
                    self.add_finding("high", "merge-input", name, f"Merge input {index + 1} {detail}, so it never receives items in production. {outcome}")

    def check_wait_loops(self) -> None:
        for node in self.nodes:
            if short_type(node.get("type", "")) != "wait":
                continue
            name = node.get("name", "")
            predecessors = [self.by_name.get(edge["from"], {}) for edge in self.edges if edge["to"] == name and edge["type"] == "main"]
            successors = [self.by_name.get(edge["to"], {}) for edge in self.edges if edge["from"] == name and edge["type"] == "main"]
            submits = [other.get("name", "") for other in predecessors if self.is_effect_node(other)]
            reads = [other.get("name", "") for other in successors
                     if short_type(other.get("type", "")) == "httpRequest" and not self.is_effect_node(other)]
            if submits and reads and name not in self.descendants(name):
                self.add_finding("medium", "wait-poll-delay", name,
                                 f"This Wait is a fixed delay between {', '.join(submits)} and {', '.join(reads)}. If the first call only submits a job (check whether the endpoint "
                                 "returns the result or a status URL), the delay assumes the job finishes in time and n8n fails or reads an incomplete result when it does not; "
                                 "then map the Wait and the read to one Dex Polling Step. If the first call already returns the result, the Wait is a plain delay.")
            if name in self.descendants(name):
                cycle = sorted(other for other in self.descendants(name) if name in self.descendants(other))
                if any(short_type(self.by_name.get(other, {}).get("type", "")) == "splitInBatches" for other in cycle):
                    self.add_finding("info", "wait-rate-limit", name,
                                     f"This Wait paces a Loop Over Items batch loop ({', '.join(cycle)}): a rate limit, not polling. Map it to batched Steps with a Timer between batches.")
                    continue
                self.add_finding("medium", "wait-poll-loop", name,
                                 f"This Wait sits in a loop ({', '.join(cycle)}): a fixed delay standing in for an external job finishing. Map the loop to the Dex Polling pattern, "
                                 "one long-running Step bounded by its Execute method timeout and retry total duration, not a Timer plus a jump back, and record the source's missing bound if it has none.")

    def check_preflight(self) -> None:
        """n8n refuses to start an execution while a main-reachable node lacks a required displayed parameter."""
        hollow = {item["node"] for item in self.findings if item.get("blocks")}
        triggers = [node.get("name", "") for node in self.nodes if is_trigger(node) and not node.get("disabled")]
        failing = {}
        for trigger in triggers:
            reachable = {trigger} | self.descendants(trigger, "main")
            blocking = sorted(name for name in hollow & reachable if not langchain_sub_node_role(self.by_name.get(name, {})))
            if blocking:
                failing[trigger] = blocking
        uncredentialed = {
            node.get("name", "") for node in self.nodes
            if (self.is_app_node(node) or kind_is_model(node)) and not node.get("credentials") and not node.get("disabled")
        }
        reachable_all = set()
        for trigger in triggers:
            reachable_all |= {trigger} | self.descendants(trigger, "main")
        # Sub-nodes such as chat models attach to a reachable node through non-main connections.
        reachable_all |= {edge["from"] for edge in self.edges if edge["type"] != "main" and edge["to"] in reachable_all}
        lacking = sorted(uncredentialed & reachable_all)
        trigger_lacking = sorted(node.get("name", "") for node in self.nodes
                                 if is_trigger(node) and not node.get("credentials") and not node.get("disabled")
                                 and short_type(node.get("type", "")) not in TRIGGER_TYPES)
        if trigger_lacking:
            self.add_finding("high", "activation-blocked", "(workflow)",
                             f"The app trigger(s) {', '.join(trigger_lacking)} have no credential, so they cannot register with the provider at any release: "
                             "the exported workflow is never started by them. Record that as the source behavior.")
        if lacking:
            self.add_finding("high", "activation-blocked", "(workflow)",
                             f"From n8n 2.8 the server refuses to activate or publish a workflow while a node reachable from a trigger lacks a required credential "
                             f"({', '.join(lacking)}), so its production trigger never registers and a production webhook answers 404. "
                             "Before 2.8 the editor already refuses to activate it while those nodes show credential issues. Record the source behavior per release and execution mode.")
        if not failing:
            return
        scope = ("for any trigger event" if len(failing) == len(triggers)
                 else f"for executions started by {', '.join(sorted(failing))}; executions from other triggers still run")
        nodes = sorted({name for names in failing.values() for name in names})
        self.add_finding("high", "preflight-fails", "(workflow)",
                         "Before the first node runs, n8n checks every enabled node reachable from the starting trigger for empty required parameters that are displayed at its version, "
                         f"and fails the whole execution if one is empty. {', '.join(nodes)} have empty parameters that are required (confirm each at the release), "
                         f"so the exported workflow has no effect {scope}. Decide whether to build the intended workflow instead. "
                         "Confirm each parameter in the node source, record that effect as none, and do not offer a 'faithful' option that assumes later nodes run. Credentials are not part of this check. "
                         "From n8n 2.8 the server also refuses to activate such a workflow, so its production trigger never registers.")

    def check_open_triggers(self) -> None:
        for node in self.nodes:
            kind = short_type(node.get("type", ""))
            parameters = node.get("parameters", {}) or {}
            if kind == "webhook" and not node.get("disabled"):
                self.add_finding("high", "caller-type", node.get("name", ""),
                                 "Is the caller a server or a browser? n8n answers CORS preflight and echoes the Origin unless options.allowedOrigins restricts it, "
                                 "while the Dex webhook endpoint accepts only verified POST requests and sends no CORS headers; a browser cannot hold a signing secret, "
                                 "so a browser caller needs a server-side relay or a connector capability.")
            if kind == "telegramTrigger":
                fields = parameters.get("additionalFields", {}) or {}
                restricted = node_version(node) >= 1.2 and (fields.get("chatIds") or fields.get("userIds"))
                if restricted and fields.get("download"):
                    self.add_finding("high", "open-trigger", node.get("name", ""),
                                     "With the download option on, a message carrying a photo, document, or video is returned before the chat and user restrictions run, "
                                     "so media messages start the workflow from anyone. Record it as source behavior.")
                if not restricted:
                    self.add_finding("high", "open-trigger", node.get("name", ""),
                                     "The Telegram Trigger starts the workflow for anyone who messages the bot: chat and user restrictions are absent or ignored before typeVersion 1.2. "
                                     "Record the open trigger as source behavior and ask whether Dex keeps it or filters senders.")

    def check_evaluation_instant(self) -> None:
        readers = sorted({item["node"] for item in self.expressions if TIME_DEPENDENT.search(item["expression"])}
                         | {node.get("name", "") for node in self.nodes
                            if TIME_DEPENDENT.search(str((node.get("parameters", {}) or {}).get("jsCode", "")))})
        if (self.schedules and readers) or len(readers) > 1:
            self.add_finding("high", "evaluation-instant", "(workflow)",
                             f"{', '.join(readers)} read the current time, which n8n evaluates separately in each node at run time. Replacing it with one instant, "
                             "such as the occurrence time, is a decision unless that instant always equals the node's run time at the precision used; "
                             "define it for manual runs, late runs, and Step retries.")

    def check_agent_exits(self) -> None:
        for node in self.nodes:
            if not short_type(node.get("type", "")).startswith("agent"):
                continue
            name = node.get("name", "")
            attached = [self.by_name.get(edge["from"], {}) for edge in self.edges if edge["to"] == name and edge["type"] != "main"]
            has_parser = any(short_type(other.get("type", "")).startswith("outputParser") for other in attached)
            has_tools = any(short_type(other.get("type", "")).startswith("tool") for other in attached)
            if has_parser and has_tools:
                limit = ((node.get("parameters", {}) or {}).get("options", {}) or {}).get("maxIterations", 10)
                self.add_finding("medium", "agent-exits", name,
                                 f"A tools agent with an output parser can stop after {limit} iterations and return a fixed text that skips the parser, "
                                 "so downstream nodes receive unparsed text. Golden the downstream chain for each exit: parsed, parser failure, the max-iterations stop, and an empty finish.")

    def check_release_markers(self) -> None:
        """List what in the export bounds the n8n release from below."""
        highest = {}
        for node in self.nodes:
            kind = short_type(node.get("type", ""))
            highest[kind] = max(highest.get(kind, 0.0), node_version(node))
        settings = sorted((self.workflow.get("settings", {}) or {}).keys())
        static_keys = sorted({child for value in (self.workflow.get("staticData") or {}).values() if isinstance(value, dict) for child in value})
        credential_keys = sorted({key for node in self.nodes for reference in (node.get("credentials") or {}).values()
                                  if isinstance(reference, dict) for key in reference if key not in ("id", "name")})
        node_keys = sorted({key for node in self.nodes for key in node if key not in BASELINE_NODE_KEYS})
        present = set(settings) | set(static_keys) | set(credential_keys)
        known = [text for marker, text in RELEASE_MARKERS
                 if marker in present or (marker.startswith("scheduleTrigger@") and highest.get("scheduleTrigger", 0) >= float(marker.split("@")[1]))]
        message = (f"Bound the n8n release from these markers (workflow-import section 3): settings keys {', '.join(settings) or 'none'}; "
                   f"staticData keys {', '.join(static_keys) or 'none'}; credential reference keys {', '.join(credential_keys) or 'none'}; "
                   f"other node keys {', '.join(node_keys) or 'none'}; highest typeVersion per node type "
                   + ", ".join(f"{kind}@{version:g}" for kind, version in sorted(highest.items()) if kind != "stickyNote") + ".")
        if known:
            message += " Known markers: " + "; ".join(known) + "."
        self.add_finding("info", "release-marker", "(workflow)", message)

    def check_provenance(self) -> None:
        meta = self.workflow.get("meta") or {}
        reasons = []
        if meta.get("templateId"):
            reasons.append(f"meta.templateId {meta['templateId']} marks a gallery template")
        if self.workflow.get("triggerCount") == 0:
            reasons.append("triggerCount 0 means no trigger was ever active")
        if self.workflow.get("active") is False:
            reasons.append("active is false")
        if reasons:
            self.add_finding("info", "provenance", "(workflow)",
                             "; ".join(reasons) + ". Treat the export as a specification that may never have run: there may be no execution history to compare, so accept against user-approved expected effects on synthetic inputs.")

    def check_authentication_consistency(self) -> None:
        methods = defaultdict(set)
        for node in self.nodes:
            if self.is_app_node(node) and not is_trigger(node):
                methods[short_type(node.get("type", ""))].add(str((node.get("parameters", {}) or {}).get("authentication", "default")))
        for kind, values in methods.items():
            if len(values) > 1:
                self.add_finding("medium", "inconsistent-authentication", f"{kind} nodes",
                                 f"{kind} nodes use different authentication settings ({', '.join(sorted(values))}); confirm whether they act for one account (one Dex connection) or several.")

    def descendants(self, name: str, connection_type: str = "") -> set:
        forward = defaultdict(set)
        for edge in self.edges:
            if not connection_type or edge["type"] == connection_type:
                forward[edge["from"]].add(edge["to"])
        seen, queue = set(), deque(forward[name])
        while queue:
            current = queue.popleft()
            if current in seen:
                continue
            seen.add(current)
            queue.extend(forward[current] - seen)
        return seen

    def ancestors(self, name: str) -> set:
        _, backward = self.adjacency()
        seen, queue = set(), deque(backward[name])
        while queue:
            current = queue.popleft()
            if current in seen:
                continue
            seen.add(current)
            queue.extend(backward[current] - seen)
        return seen

    def check_references(self) -> None:
        for expression in self.expressions:
            for reference in expression["references"]:
                if reference not in self.by_name:
                    self.add_finding("high", "missing-node-reference", expression["node"],
                                     f"`{expression['parameter']}` reads node {reference!r}, which is not in the export.")
                elif reference not in self.ancestors(expression["node"]):
                    self.add_finding("high", "non-ancestor-reference", expression["node"],
                                     f"`{expression['parameter']}` reads node {reference!r}, which is not upstream; n8n fails or reads stale data. Resolve the intended source.")

    def check_dead_fields(self) -> None:
        texts = defaultdict(list)
        for expression in self.expressions:
            texts[expression["node"]].append(expression["expression"])
        for node in self.nodes:
            parameters = node.get("parameters", {}) or {}
            code = parameters.get("jsCode") or parameters.get("functionCode") or ""
            if code:
                texts[node.get("name", "")].append(code)
        for field in self.set_fields:
            name = field["name"]
            if not name:
                continue
            pattern = re.compile(r"(?:\.|\[\s*['\"])" + re.escape(name) + r"\b")
            used = any(
                pattern.search(text)
                for node_name, node_texts in texts.items()
                if node_name != field["node"]
                for text in node_texts
            )
            if not used:
                self.add_finding("medium", "dead-config", field["node"],
                                 f"Field `{name}` is never read explicitly downstream. Confirm that no node consumes whole items, then drop it or wire it as intended.")

    def check_unused_code_outputs(self) -> None:
        """Output keys a Code node returns that no later node reads have no observable effect."""
        for node in self.nodes:
            kind = short_type(node.get("type", ""))
            code = (node.get("parameters", {}) or {}).get("jsCode") or (node.get("parameters", {}) or {}).get("functionCode") or ""
            if kind not in ("code", "function", "functionItem") or not code:
                continue
            keys = set()
            for match in re.finditer(r"(?:json\s*:|return)\s*\{", code):
                depth, start = 0, match.end() - 1
                for index in range(start, len(code)):
                    if code[index] == "{":
                        depth += 1
                    elif code[index] == "}":
                        depth -= 1
                        if depth == 0:
                            body = code[start + 1:index]
                            keys |= {key.group(1) for key in re.finditer(r"(?:^|[,{\n])\s*([A-Za-z_$][\w$]*)\s*:", self.top_level(body))}
                            break
            keys -= {"json", "binary", "pairedItem"}
            if not keys:
                continue
            downstream = self.descendants(node.get("name", ""))
            texts = [expression["expression"] for expression in self.expressions if expression["node"] in downstream]
            texts += [str((self.by_name.get(other, {}).get("parameters", {}) or {}).get("jsCode", "")) for other in downstream]
            unused = sorted(key for key in keys if not any(re.search(r"(?:\.|\[\s*['\"])" + re.escape(key) + r"\b", text) for text in texts))
            if unused and downstream:
                self.add_finding("info", "unused-output-field", node.get("name", ""),
                                 f"No later node reads the output field(s) {', '.join(f'`{key}`' for key in unused)}, so they have no observable effect: "
                                 "mark them dropped, and their timezone or locale dependencies need no decision (check nodes that consume whole items).")

    @staticmethod
    def top_level(body: str) -> str:
        """Blank out nested braces and brackets so only top-level object keys remain."""
        depth, kept = 0, []
        for character in body:
            if character in "{[(":
                depth += 1
            elif character in "}])":
                depth -= 1
                continue
            kept.append(character if depth == 0 else " ")
        return "".join(kept)

    def check_overlapping_windows(self) -> None:
        """A read window longer than the schedule interval covers the same records in consecutive runs."""
        for schedule in self.schedules:
            interval_days = {"days": 1, "weeks": 7}.get(schedule.get("field"))
            if not interval_days:
                continue
            downstream = self.descendants(schedule["node"], "main")
            for expression in self.expressions:
                if expression["node"] not in downstream:
                    continue
                for match in re.finditer(r"minus\(\s*\{\s*(days|weeks)\s*:\s*(\d+)", expression["expression"]):
                    window = int(match.group(2)) * (7 if match.group(1) == "weeks" else 1)
                    if window > interval_days:
                        self.add_finding("medium", "overlapping-window", expression["node"],
                                         f"`{expression['parameter']}` reads a {window}-day window, but the schedule fires {schedule['description']}, "
                                         "so consecutive runs cover overlapping records and their effects repeat them. Record that as source behavior.")

    def check_literal_sets(self) -> None:
        forward, _ = self.adjacency()
        for node in self.nodes:
            if short_type(node.get("type", "")) not in ("if", "filter"):
                continue
            accepted = set()
            for condition in (node.get("parameters", {}).get("conditions", {}) or {}).get("conditions", []) or []:
                operation = condition.get("operator", {}).get("operation", "")
                right = condition.get("rightValue")
                if operation in ("startsWith", "equals", "contains") and isinstance(right, str) and right:
                    accepted.add(right.lower())
            if not accepted:
                continue
            descendants, queue = set(), deque(forward[node.get("name", "")])
            while queue:
                current = queue.popleft()
                if current in descendants:
                    continue
                descendants.add(current)
                queue.extend(forward[current] - descendants)
            for expression in self.expressions:
                if expression["node"] not in descendants:
                    continue
                stripped = {match.group(2).lower() for match in STRING_REPLACE.finditer(expression["expression"])}
                if not stripped:
                    continue
                unhandled = sorted(accepted - stripped)
                unused = sorted(stripped - accepted)
                if unhandled:
                    self.add_finding("high", "literal-set-mismatch", expression["node"],
                                     f"`{node.get('name')}` accepts {sorted(accepted)}, but `{expression['parameter']}` strips only {sorted(stripped)}: values matching {unhandled} pass the filter unnormalized"
                                     + (f", and the filter never accepts {unused} on its own, so that strip only changes values that also match an accepted literal." if unused else "."))

    def check_settings(self) -> None:
        settings = self.workflow.get("settings", {}) or {}
        if not settings.get("timezone"):
            if any(item["kind"] in ("timezone-dependent", "version-default") and "timezone" in item["message"] for item in self.findings) or self.schedules:
                self.add_finding("high", "timezone-unset", "(workflow)",
                                 "settings.timezone is absent, so schedules, $today, and $now use the n8n instance's GENERIC_TIMEZONE, which is America/New_York when the instance never set it. The export cannot tell which zone; ask the user.")
        if settings.get("errorWorkflow"):
            self.add_finding("high", "error-workflow", "(workflow)",
                             "An error workflow handles failures; map it to Execute-failure recovery Steps.")
        if self.workflow.get("pinData"):
            self.add_finding("info", "pin-data", "(workflow)",
                             "pinData holds editor test data, not production behavior; it may contain personal data. Use it only to shape synthetic fixtures.")

    # ----- plan drafts --------------------------------------------------
    def connector_matrix(self) -> list[dict]:
        """Match each integration node to candidate released connectors and operations."""
        rows = []
        for node in self.nodes:
            kind = short_type(node.get("type", ""))
            is_http = kind == "httpRequest"
            if not (is_http or (self.is_app_node(node) and kind != "stickyNote") or "langchain" in node.get("type", "")):
                continue
            parameters = node.get("parameters", {}) or {}
            operation = str(parameters.get("operation", DEFAULT_OPERATION.get(kind, "")))
            resource = str(parameters.get("resource", DEFAULT_RESOURCE.get(kind, "")))
            row = {"node": node.get("name", ""), "type": kind, "operation": "/".join(part for part in (resource, operation) if part),
                   "host": "", "connector": "", "operations": [], "likelyOperation": "", "semanticNote": "", "status": ""}
            sub_node_role = langchain_sub_node_role(node)
            if sub_node_role:
                row["status"] = sub_node_role
                rows.append(row)
                continue
            candidates = list(N8N_CONNECTOR_CANDIDATES.get(kind, []))
            if is_http:
                row["host"] = http_request_host(parameters.get("url", ""))
                label = row["host"].split(".")[-2] if row["host"].count(".") >= 1 else row["host"]
                candidates = [entry["id"] for entry in self.catalog or [] if label and label in catalog_search_text(entry)]
            elif not candidates and self.catalog:
                candidates = [entry["id"] for entry in self.catalog if kind.lower().replace("trigger", "") in catalog_search_text(entry)]
            if self.catalog is None:
                row["connector"] = ", ".join(candidates) or "-"
                row["status"] = "catalog not provided; pass --catalog"
            else:
                released = [entry for candidate in candidates for entry in self.catalog if entry["id"] == candidate]
                if released:
                    entry = released[0]
                    row["connector"] = f"{entry['id']} {entry['version']}"
                    row["operations"] = [f"{name} ({op_kind})" for name, op_kind in entry["operations"]] + [f"{name} (trigger)" for name in entry["triggers"]]
                    row["likelyOperation"] = likely_operation(operation, entry)
                    verb = re.sub(r"[^a-z]", "", operation.split("/")[-1].lower())
                    if row["likelyOperation"] and verb and not row["likelyOperation"].lower().startswith(verb[:4]):
                        row["semanticNote"] = f"semantics differ from n8n {verb}: confirm and record a divergence"
                    row["status"] = "released candidate; confirm the exact operation in its connector.yaml"
                elif is_http and not row["host"]:
                    row["status"] = "the URL host comes from an expression: resolve the host from upstream data before choosing a connector"
                elif is_http:
                    row["status"] = f"no released connector for {row['host']}: connector contribution, or the internal connector library decision for an internal service"
                else:
                    row["status"] = "no released connector: connector contribution"
            rows.append(row)
        return rows

    def plan_rows(self) -> list[dict]:
        """Draft one Dex element per node, in execution order from the triggers."""
        forward, _ = self.adjacency()
        matrix = {row["node"]: row for row in self.connector_matrix()}
        order, seen = [], set()
        is_v1 = (self.workflow.get("settings", {}) or {}).get("executionOrder", "v0") == "v1"

        def canvas_order(name: str):
            # Under executionOrder v1, sibling branches run top to bottom on the canvas.
            position = self.by_name.get(name, {}).get("position") or [0, 0]
            return (position[1], position[0], name) if is_v1 else (0, 0, name)

        queue = deque(node.get("name", "") for node in self.nodes if is_trigger(node))
        while queue:
            current = queue.popleft()
            if current in seen:
                continue
            seen.add(current)
            order.append(current)
            queue.extend(sorted(forward[current] - seen, key=canvas_order))
        for edge in self.edges:
            if edge["type"] != "main" and edge["from"] not in seen and edge["to"] in order:
                order.insert(order.index(edge["to"]) + 1, edge["from"])
                seen.add(edge["from"])
        order += [node.get("name", "") for node in self.nodes if node.get("name", "") not in seen]
        unreachable = {item["node"] for item in self.findings if item["kind"] == "unreachable-node"}
        poll_waits, poll_reads = {}, {}
        for item in self.findings:
            if item["kind"] == "wait-poll-delay":
                read = next((edge["to"] for edge in self.edges if edge["from"] == item["node"] and edge["type"] == "main"), "")
                if read:
                    poll_waits[item["node"]], poll_reads[read] = read, item["node"]
        multi_item_sources = {
            node.get("name", "") for node in self.nodes
            if str((node.get("parameters", {}) or {}).get("operation", "")).lower() in MULTI_ITEM_OPERATIONS
        }
        rows = []
        for name in order:
            node = self.by_name.get(name, {})
            kind = short_type(node.get("type", ""))
            per_item = any(source in self.ancestors(name) for source in multi_item_sources)
            if kind == "stickyNote" or kind == "noOp":
                element, notes = "dropped", "no behavior"
            elif name in unreachable:
                element, notes = "dropped", "unreachable: never runs in production"
            elif node.get("disabled"):
                element, notes = "dropped", "disabled: n8n passes its items through unchanged"
            elif kind == "set":
                element, notes = "folded into the consuming Step", self.hint(node)
            elif kind in ("if", "filter", "switch"):
                element, notes = "folded into an application Step", "the predicate chooses that Step's movement; port it with the IF semantics in n8n-semantics"
            elif name in poll_waits:
                element = f"Polling Step {pascal_case(poll_waits[name])}"
                notes = (f"if the step before only submits a job: one long-running Step that waits for the result instead of this fixed delay plus {poll_waits[name]}, "
                         "bounded by its Execute method timeout and retry total duration")
            elif name in poll_reads:
                element, notes = "merged into the Polling Step", f"read by the Polling Step that replaces {poll_reads[name]}"
            elif kind == "wait":
                element = f"application Step {pascal_case(name)}"
                resume = (node.get("parameters", {}) or {}).get("resume", "timeInterval")
                if resume in ("webhook", "form"):
                    notes = "WaitFor waits on a Channel or typed RPC resume, plus a Timer only if limitWaitTime is set; Execute builds the next Step's input"
                else:
                    notes = "WaitFor returns dex.Until(dex.Timer(duration)); Execute builds the next Step's input (a Connector Step cannot wait)"
            elif is_trigger(node):
                element, notes = "trigger", self.hint(node)
            elif langchain_sub_node_role(node):
                element, notes = "configuration", langchain_sub_node_role(node)
            elif name in matrix:
                connector = matrix[name]
                element = f"Connector Step {pascal_case(name)}"
                notes = f"{connector['connector'] or 'missing connector'}: {connector['likelyOperation'] or connector['status']}"
                if connector.get("semanticNote"):
                    notes += f"; {connector['semanticNote']}"
                if per_item:
                    notes += "; per item: loop with a persisted cursor (a Connector result carries no item context)"
                predecessors = {edge["from"] for edge in self.edges if edge["to"] == name and edge["type"] == "main"}
                if any(predecessor in matrix and not langchain_sub_node_role(self.by_name.get(predecessor, {})) for predecessor in predecessors):
                    notes += "; add an application Step before it: a Connector Step receives only the previous result, so map the input and persist context there"
                notes += "; route every branch in its connector.yaml (an unrouted optional branch fails the Flow)"
                successors = {edge["to"] for edge in self.edges if edge["from"] == name and edge["type"] == "main"}
                if len(successors) > 1:
                    notes += ("; fan-out: under executionOrder v1 chain the branches in canvas order (the faithful port); "
                              "a parallel fan-out needs an application Step after it and is a recorded divergence")
            else:
                element = f"application Step {pascal_case(name)}"
                notes = self.hint(node)
            touching = [f"P{index}" for index, proposal in enumerate(self.redesign_proposals(), 1) if name in proposal["nodes"]]
            if touching:
                notes += f"; in optimize mode see {', '.join(touching)}"
            rows.append({"node": name, "type": kind, "element": element, "notes": notes})
            if element.startswith("Connector Step"):
                step = pascal_case(name)
                rows.append({"node": name, "type": kind, "element": f"application Step {step}Outcome",
                             "notes": "input: the operation's Result; target of every optional branch; records the outcome and moves on with a normalized input"})
                rows.append({"node": name, "type": kind, "element": f"application Step {step}ExecuteFailed",
                             "notes": f"input: {step}'s own input; StepOptionsOverride.ExecuteFailure = dex.ProceedToOnExecuteFailure({step}ExecuteFailed{{}}, nil)"})
        return rows

    def to_mermaid(self) -> list[str]:
        ids = {}
        lines = ["```mermaid", "flowchart LR"]
        for index, node in enumerate(node for node in self.nodes if short_type(node.get("type", "")) != "stickyNote"):
            ids[node.get("name", "")] = f"n{index + 1}"
            label = (node.get("name", "") + "<br/>" + short_type(node.get("type", ""))).replace('"', "#quot;")
            lines.append(f'  n{index + 1}["{label}"]')
        for edge in self.edges:
            if edge["from"] in ids and edge["to"] in ids:
                arrow = f" -- {edge['label']} --> " if edge["label"] not in ("main",) else " --> "
                lines.append(f"  {ids[edge['from']]}{arrow}{ids[edge['to']]}")
        lines.append("```")
        return lines

    # ----- output -------------------------------------------------------
    def to_json(self) -> dict:
        settings = self.workflow.get("settings", {}) or {}
        data = {
            "workflow": {"name": self.workflow.get("name", ""), "source": self.source_name, "settings": settings},
            "nodes": self.node_rows,
            "edges": self.edges,
            "expressions": self.expressions,
            "setFields": self.set_fields,
            "schedules": self.schedules,
            "credentials": [
                {"type": key[0], "name": key[1], "nodes": nodes} for key, nodes in self.credentials.items()
            ],
            "claims": self.claims,
            "findings": self.findings,
            "connectorMatrix": self.connector_matrix(),
            "planDraft": self.plan_rows(),
            "intent": [dict(zip(("effect", "operation", "startedBy", "howOften"), row)) for row in self.intent_rows()],
            "redesign": self.redesign_proposals(),
            "ledgerIds": self.ledger_ids or [cells[0] for _, cells in ledger_rows(self.to_markdown())],
        }
        return json.loads(self.redact(json.dumps(data)))

    def redact(self, text: str) -> str:
        for secret in sorted(self.secrets, key=len, reverse=True):
            text = text.replace(secret, "‹redacted›")
            text = text.replace(json.dumps(secret)[1:-1], "‹redacted›")
        return text

    def to_markdown(self) -> str:
        lines = []
        name = self.workflow.get("name", "(unnamed workflow)")
        counts = {severity: sum(1 for item in self.findings if item["severity"] == severity) for severity in SEVERITY_ORDER}
        triggers = [row for row in self.node_rows if is_trigger(self.by_name[row["node"]])]
        code_nodes = [row for row in self.node_rows if row["type"].split("@")[0] in ("code", "function", "functionItem")]
        integrations = [
            row for row in self.node_rows
            if row["type"].split("@")[0] == "httpRequest"
            or (self.is_app_node(self.by_name[row["node"]]) and not is_trigger(self.by_name[row["node"]]))
        ]
        lines += [
            f"# n8n → Dex import ledger: {name}",
            "",
            f"Source `{self.source_name}`: {len(self.nodes)} nodes, {len(self.edges)} connections. "
            "Generated by `n8n_inventory.py`; literal secrets are redacted.",
            "",
            "Set every Status cell to `mapped`, `diverged`, `dropped`, `blocked`, or `pending` "
            "(redesign proposals take `adopted`, `deferred`, or `pending`; decisions take `decided` or `pending`). "
            "Use `pending` for a proposal that still awaits the user's decision, and `diverged` only once the user approved it. "
            "Every status except `mapped` needs Notes that record the reason or the decision. "
            "Then run `n8n_inventory.py verify` on this file; `--strict` also fails while anything is pending.",
            "",
            "## Summary",
            "",
            f"- Triggers: {', '.join(row['node'] + ' (' + row['type'] + ')' for row in triggers) or 'none'}",
            f"- Integrations: {', '.join(row['node'] + ' (' + row['type'] + ')' for row in integrations) or 'none'}",
            f"- Code nodes: {', '.join(row['node'] for row in code_nodes) or 'none'}",
            f"- Findings: {counts['critical']} critical, {counts['high']} high, {counts['medium']} medium, {counts['info']} info",
            "",
            "Rows may be split into sub-rows (`N3a`, `N3b`) when parts of one element map differently. "
            "Add rows found during the semantic review with the next free ID in their section.",
            "",
            "## Execution graph",
            "",
            "| ID | From | Output | To | Status | Notes |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
        for index, edge in enumerate(self.edges, 1):
            lines.append(f"| E{index} | {cell(edge['from'])} | {cell(edge['label'])} | {cell(edge['to'])} | todo |   |")
        lines += ["", "Compare this graph with the Dex Web rendering of the migrated Flows.", ""]
        lines += self.to_mermaid()
        lines += ["", "## Nodes", "", "| ID | Node | Type | Source behavior | Dex mapping hint | Status | Notes |", "| --- | --- | --- | --- | --- | --- | --- |"]
        for index, row in enumerate(self.node_rows, 1):
            lines.append(f"| N{index} | {cell(row['node'])} | {cell(row['type'])} | {cell(row['behavior'], 320)} | {cell(row['hint'])} | todo |   |")
        lines += ["", "## Expressions", "", "| ID | Node | Parameter | Expression | Semantics to preserve | Status | Notes |", "| --- | --- | --- | --- | --- | --- | --- |"]
        for index, expression in enumerate(self.expressions, 1):
            lines.append(
                f"| X{index} | {cell(expression['node'])} | {cell(expression['parameter'])} | {cell(expression['expression'], 260)} | "
                f"{cell('; '.join(expression['semantics']) or 'plain value', 260)} | todo |   |"
            )
        lines += ["", "## Credentials", "", "| ID | n8n credential | Used by | Dex connection | Status | Notes |", "| --- | --- | --- | --- | --- | --- |"]
        for index, (key, nodes) in enumerate(self.credentials.items(), 1):
            lines.append(
                f"| C{index} | {cell(key[0])} `{cell(key[1])}` | {cell(', '.join(nodes))} | "
                "Connector connection authorized in Dex Web Connectors; n8n credentials are not portable | todo |   |"
            )
        credential_index = len(self.credentials)
        uncredentialed = defaultdict(list)
        for row in self.connector_matrix():
            node = self.by_name.get(row["node"], {})
            needs_connection = self.is_app_node(node) or kind_is_model(node)
            if not node.get("credentials") and needs_connection and not node.get("disabled"):
                connector = row["connector"] if row["connector"] and row["connector"] != "-" else row["type"]
                uncredentialed[connector].append(row["node"])
        for connector, nodes in uncredentialed.items():
            credential_index += 1
            lines.append(f"| C{credential_index} | (none in the export) | {cell(', '.join(nodes))} | "
                         f"A Dex connection is still needed for {cell(connector)}; ask which account it acts for | todo |   |")
        for node_name, value in sorted(self.placeholders):
            credential_index += 1
            lines.append(f"| C{credential_index} | (placeholder in workflow data) | {cell(node_name)} | "
                         f"The placeholder `{cell(value, 60)}` stands for a credential; the Dex connector connection holds the real one | todo |   |")
        settings = self.workflow.get("settings", {}) or {}
        lines += ["", "## Connector branches", "",
                  "Replace each placeholder with one row per branch of the mapped operation, read from its connector.yaml at the release tag, "
                  "and say where each branch goes and which source outcome it matches. For a connector gap, write the planned operation "
                  "and routing instead and mark the row `blocked`.", "",
                  "| ID | n8n node | Branch and Dex target | Status | Notes |", "| --- | --- | --- | --- | --- |"]
        branch_nodes = [row["node"] for row in self.connector_matrix()
                        if not langchain_sub_node_role(self.by_name.get(row["node"], {})) and not is_trigger(self.by_name.get(row["node"], {}))
                        and not self.by_name.get(row["node"], {}).get("disabled")
                        and row["node"] not in {item["node"] for item in self.findings if item["kind"] == "unreachable-node"}]
        for index, node_name in enumerate(branch_nodes, 1):
            lines.append(f"| R{index} | {cell(node_name)} | {BRANCH_PLACEHOLDER} | todo |   |")
        lines += ["", "## Connector-imposed differences", "",
                  "Replace each placeholder with one row per difference the mapped operation imposes (required inputs the source never sent, "
                  "rejected values, identity per connection, extra connection fields, create-to-upsert, ordering, page size, encoding, and headers), "
                  "or one `mapped` row saying none was found.", "",
                  "| ID | n8n node | Difference | Status | Notes |", "| --- | --- | --- | --- | --- |"]
        trigger_nodes = [node.get("name", "") for node in self.nodes
                         if is_trigger(node) and not node.get("disabled") and short_type(node.get("type", "")) not in ("manualTrigger", "start", "scheduleTrigger", "cron", "interval")]
        for index, node_name in enumerate(trigger_nodes + branch_nodes, 1):
            lines.append(f"| G{index} | {cell(node_name)} | {DIFFERENCE_PLACEHOLDER} | todo |   |")
        lines += ["", "## Workflow settings and export metadata", "", "| ID | Setting | Value | Status | Notes |", "| --- | --- | --- | --- | --- |"]
        setting_rows = [("n8n release", "(not in the export: ask, and bound it with the release-marker finding)"),
                        ("expression engine", "(not in the export: N8N_EXPRESSION_ENGINE; legacy through 2.34, vm from 2.35.0, quickjs selectable from 2.43.0)"),
                        ("timezone", settings.get("timezone", "(absent: instance default)"))]
        setting_rows += [(key, value) for key, value in settings.items() if key != "timezone"]
        if self.schedules:
            setting_rows.append(("workflow ID", self.workflow.get("id") or "(absent: ask; the stable schedule second from n8n 2.19 depends on it)"))
        if any(short_type(node.get("type", "")) in ("code", "function", "functionItem") for node in self.nodes):
            setting_rows.append(("Code task runner mode and locale", "(not in the export: N8N_RUNNERS_MODE; an internal runner formats with en-US)"))
        if any(item["kind"] == "locale-dependent" for item in self.findings):
            setting_rows.append(("main process locale", "(not in the export: LANG or LC_ALL of the n8n process, used by expressions)"))
        meta = self.workflow.get("meta") or {}
        for key in ("active", "triggerCount"):
            if key in self.workflow:
                setting_rows.append((key, self.workflow[key]))
        for key, value in meta.items():
            setting_rows.append((f"meta.{key}", value if key != "instanceId" else "(present)"))
        if self.workflow.get("pinData"):
            setting_rows.append(("pinData", "(present: editor test data, not behavior)"))
        for key, value in (self.workflow.get("staticData") or {}).items():
            children = ", ".join(sorted(value)) if isinstance(value, dict) else type(value).__name__
            setting_rows.append((f"staticData.{key}", f"(trigger state, keys only: {children})"))
        for index, (key, value) in enumerate(setting_rows, 1):
            lines.append(f"| S{index} | {cell(key)} | {cell(json.dumps(value) if not isinstance(value, str) else value)} | todo |   |")
        lines += ["", "## Findings", "", "| ID | Severity | Kind | Node | Finding | Status | Notes |", "| --- | --- | --- | --- | --- | --- | --- |"]
        for index, finding in enumerate(self.findings, 1):
            lines.append(
                f"| F{index} | {finding['severity']} | {finding['kind']} | {cell(finding['node'])} | {cell(finding['message'], 2000)} | todo |   |"
            )
        lines += ["", "## Behavior matrix", "",
                  "What the source does in each situation; Dex must match it or record a decision.", "",
                  "| ID | Situation | Nodes | n8n behavior | Status | Notes |", "| --- | --- | --- | --- | --- | --- |"]
        for index, row in enumerate(self.behavior_rows(), 1):
            lines.append(f"| B{index} | {cell(row[0])} | {cell(row[1])} | {cell(row[2], 320)} | todo |   |")
        lines += ["", "## Claims to check against behavior", "",
                  "| ID | Source | Claim | Status | Notes |", "| --- | --- | --- | --- | --- |"]
        for index, claim in enumerate(self.claims, 1):
            lines.append(f"| K{index} | {cell(claim['source'])} | {cell(claim['text'], 400)} | todo |   |")
        lines += ["", "## Connector capability matrix (draft)", "",
                  "Confirm each candidate in the connector's release-tagged `connector.yaml` before mapping a row.", "",
                  "| n8n node | Type and operation | Candidate connector | Likely operation | Released operations and Triggers | Status |",
                  "| --- | --- | --- | --- | --- | --- |"]
        for row in self.connector_matrix():
            operation = row["operation"] + (f" ({row['host']})" if row["host"] else "")
            likely = row["likelyOperation"] + (f" ({row['semanticNote']})" if row["semanticNote"] else "")
            lines.append(f"| {cell(row['node'])} | {cell(row['type'] + ' ' + operation)} | {cell(row['connector'] or '-')} | "
                         f"{cell(likely or '-')} | {cell(', '.join(row['operations']) or '-', 300)} | {cell(row['status'])} |")
        lines += ["", "## Dex plan draft", "",
                  "A starting point in execution order. Apply the Flow-boundary, schedule, and per-item rules before coding.", "",
                  "| Order | n8n node | Type | Proposed Dex element | Notes |", "| --- | --- | --- | --- | --- |"]
        for index, row in enumerate(self.plan_rows(), 1):
            lines.append(f"| {index} | {cell(row['node'])} | {cell(row['type'])} | {cell(row['element'])} | {cell(row['notes'], 320)} |")
        lines += ["", "## Intent contract (draft)", "",
                  "What the workflow is for: each effect, what starts it, and how often. Confirm it in one sentence at the top of the single question.", "",
                  "| Effect | Node type and operation | Started by | How often |", "| --- | --- | --- | --- |"]
        for row in self.intent_rows():
            lines.append("| " + " | ".join(cell(value) for value in row) + " |")
        if not self.intent_rows():
            lines.append("| (no external effect found) | - | - | - |")
        proposals = self.redesign_proposals()
        lines += ["", "## Dex redesign proposals", "",
                  "Detected automatically. Ask the user once whether to optimize for Dex with these, or keep the exact n8n behavior and migrate first. "
                  "Set each row to `adopted` or `deferred` from that answer, or leave it `pending` until then.", "",
                  "Applies in both modes: " + "; ".join(self.mandatory_changes()) + ".", "",
                  "| ID | Pattern | Source nodes | Dex design | What changes for the user | Status | Notes |", "| --- | --- | --- | --- | --- | --- | --- |"]
        for index, proposal in enumerate(proposals, 1):
            lines.append(f"| P{index} | {proposal['pattern']}: {cell(proposal['title'])} | {cell(', '.join(proposal['nodes']), 300)} | "
                         f"{cell(proposal['design'], 600)} | {cell(proposal['change'], 400)}"
                         + (f" ({', '.join(proposal['findings'])})" if proposal["findings"] else "") + " | todo |   |")
        if not proposals:
            lines.append("| P1 | none | - | No redesign pattern detected; the exact migration is also the Dex design. | No change. | todo |   |")
        # Defects a proposal fixes are asked through the mode question; secure-first changes apply in both modes.
        folded = set(DEFECT_FIXES) | {"literal-secret", "secret-in-request", "placeholder-secret"}
        decisions = defaultdict(list)
        for index, finding in enumerate(self.findings, 1):
            # A code port is required work, not a choice; defects that a proposal fixes are asked through the mode question.
            if finding["severity"] in ("critical", "high") and finding["kind"] not in ("code-port", "python-code") and finding["kind"] not in folded:
                decisions[finding["kind"]].append((index, finding))
        lines += ["", "## Decisions for the user", "",
                  "Ask all of these in one message: D1 first, then the facts below it with their recommended defaults. "
                  "Set Status to `decided` with the user's answer in Notes, or `pending` while it is open.", "",
                  "| ID | Kind | Findings | Question | Recommendation | Status | Notes |", "| --- | --- | --- | --- | --- | --- | --- |"]
        proposal_ids = f"P1-P{len(proposals)}" if len(proposals) > 1 else "P1"
        lines.append(f"| D1 | migration-mode | {proposal_ids} | Optimize for Dex with the redesign proposals {proposal_ids} (the user may exclude some by ID), "
                     "or keep the exact n8n behavior and migrate first, leaving the proposals for a later pass? | optimize | todo |   |")
        for index, (kind, items) in enumerate(decisions.items(), 2):
            ids = ", ".join(f"F{number}" for number, _ in items)
            question = items[0][1]["message"] if len(items) == 1 else f"{len(items)} findings on {', '.join(sorted({item['node'] for _, item in items}))}: {items[0][1]['message']}"
            if kind == "preflight-fails":
                question = "Build the intended workflow? As exported, the source has no effect for these executions. " + question
            lines.append(f"| D{index} | {kind} | {ids} | {cell(question, 2000)} |   | todo |   |")

        lines.append("")
        rendered = self.redact("\n".join(lines))
        self.ledger_ids = [cells[0] for _, cells in ledger_rows(rendered)]
        return rendered

    def finding_ids(self, *kinds: str) -> list[str]:
        return [f"F{index}" for index, item in enumerate(self.findings, 1) if item["kind"] in kinds]

    def redesign_proposals(self) -> list[dict]:
        """Detect n8n shapes that Dex primitives express better, and source defects worth fixing."""
        if getattr(self, "_proposals", None) is not None:
            return self._proposals
        proposals = []

        def propose(pattern, title, nodes, design, change, findings=()):
            proposals.append({"pattern": pattern, "title": title, "nodes": sorted(set(nodes)), "design": design,
                              "change": change, "findings": list(findings)})

        active = [node for node in self.nodes if not node.get("disabled") and short_type(node.get("type", "")) != "stickyNote"]
        kinds = {item["kind"] for item in self.findings}
        by_kind = defaultdict(list)
        for item in self.findings:
            by_kind[item["kind"]].append(item["node"])

        if "repeated-effects" in kinds:
            propose("entity-timer", "Act once per entity at the intended time", by_kind["repeated-effects"],
                    "Discover entities on the schedule (or with a provider Trigger) and start one Flow per entity, identified by the provider's entity ID; "
                    "that Flow waits on a Timer for the moment the intent names, such as a set time before an event, and acts once.",
                    "Each entity's effect happens once at the intended time, instead of again on every schedule run.",
                    self.finding_ids("repeated-effects"))
        if "overlapping-window" in kinds:
            propose("process-once", "Process each record once", by_kind["overlapping-window"],
                    "Keep the last successful run's instant in an Attribute and read only records newer than it.",
                    "Consecutive runs no longer repeat records an earlier run already covered.", self.finding_ids("overlapping-window"))
        long_waits = []
        for node in active:
            parameters = node.get("parameters", {}) or {}
            if short_type(node.get("type", "")) == "wait" and parameters.get("resume", "timeInterval") == "timeInterval":
                version = node_version(node)
                amount = parameters.get("amount", 1 if version < 1.1 else 5)
                unit = parameters.get("unit", "hours" if version < 1.1 else "seconds")
                seconds = {"seconds": 1, "minutes": 60, "hours": 3600, "days": 86400}.get(str(unit), 0) * (amount if isinstance(amount, (int, float)) else 0)
                if seconds >= 65:
                    long_waits.append(node.get("name", ""))
        if long_waits:
            propose("entity-flow", "Give each entity its own Flow with cancellable waits", long_waits,
                    "One Flow per entity, such as a customer or an order. Each wait is an application Step that waits on AnyOf(Timer, cancel Channel); "
                    "progress lives in Attributes, and Dex Web Actions cancel, skip, or inspect a sequence.",
                    "A waiting sequence can be stopped or advanced per entity, and you can see where each entity is.")
        if kinds & {"wait-poll-delay", "wait-poll-loop"}:
            propose("polling-step", "Wait for submitted jobs instead of guessing a delay", by_kind["wait-poll-delay"] + by_kind["wait-poll-loop"],
                    "One Polling Step bounded by its Execute method timeout and retry total duration, or a provider callback that publishes to a Channel the Flow waits on.",
                    "A slow job no longer fails or returns an incomplete result, and a paid submit is never repeated.",
                    self.finding_ids("wait-poll-delay", "wait-poll-loop"))
        approvals = [node.get("name", "") for node in active
                     if str((node.get("parameters", {}) or {}).get("operation", "")) == "sendAndWait"
                     or (short_type(node.get("type", "")) == "wait" and (node.get("parameters", {}) or {}).get("resume") in ("webhook", "form"))
                     or short_type(node.get("type", "")) == "form"]
        if approvals:
            propose("inbox-action", "Turn approvals into Dex Web Actions", approvals,
                    "A Dex Web Action with a permission, backed by a typed RPC that publishes to a Channel the Flow waits on; Summary and Display show the status.",
                    "Approvals and replies are authenticated and recorded in Dex, instead of links in messages.")
        stateful = [node.get("name", "") for node in active if short_type(node.get("type", "")) == "removeDuplicates"
                    or "getWorkflowStaticData" in str((node.get("parameters", {}) or {}).get("jsCode", ""))]
        store_operations = defaultdict(lambda: [set(), []])
        for node in active:
            kind = short_type(node.get("type", ""))
            if kind in STATE_STORE_TYPES:
                operation = str((node.get("parameters", {}) or {}).get("operation", "")).lower()
                store_operations[kind][0].add("write" if operation.startswith(WRITE_VERBS) else "read")
                store_operations[kind][1].append(node.get("name", ""))
        for kind, (operations, names) in store_operations.items():
            if operations == {"read", "write"}:
                stateful += names
        if stateful:
            propose("durable-state", "Keep workflow state in Dex", stateful,
                    "Deduplicate with Flow identity (a stable Flow ID, IDReuseDisallow, and a request ID) and keep the state in Attributes or AttributeMaps. "
                    "Applies only to tables that track the workflow's own progress, not business data people use.",
                    "No sheet, table, or node memory to maintain for deduplication or progress; duplicates are rejected at start.")
        effects = [node for node in active if self.is_effect_node(node)]
        multi_reads = [node.get("name", "") for node in active if self.is_multi_item_read(node)]
        def aggregates(name: str) -> bool:
            node = self.by_name.get(name, {})
            kind = short_type(node.get("type", ""))
            mode = (node.get("parameters", {}) or {}).get("mode", "runOnceForAllItems")
            return kind in ("aggregate", "summarize") or (kind == "code" and mode == "runOnceForAllItems")

        per_item_effects = []
        for node in effects:
            name = node.get("name", "")
            for read in multi_reads:
                between = self.ancestors(name) & (self.descendants(read, "main") | {read})
                # An aggregating node between the read and the effect turns the items into one.
                if read in self.ancestors(name) and not any(aggregates(other) for other in between - {read}):
                    per_item_effects.append(name)
                    break
        handled = [node.get("name", "") for node in active if node.get("retryOnFail") or node.get("continueOnFail")
                   or (node.get("onError") or "stopWorkflow") != "stopWorkflow"]
        if per_item_effects or handled or (self.workflow.get("settings", {}) or {}).get("errorWorkflow"):
            propose("explicit-recovery", "Recover per item instead of stopping everything", per_item_effects + handled,
                    "Retry policies per Step, an Execute-failure recovery Step per Connector Step, per-item isolation so one failure does not stop the others, "
                    "and an uncertain send routed to recovery rather than resent.",
                    "One item's failure no longer stops the remaining items (choose all-or-nothing instead if that is the intent).")
        fan_out = [edge["from"] for edge in self.edges if edge["type"] == "main"
                   and sum(1 for other in self.edges if other["type"] == "main" and other["from"] == edge["from"] and other["output"] == edge["output"]) > 1]
        joins = [node.get("name", "") for node in active if short_type(node.get("type", "")) in ("merge", "splitInBatches")]
        if fan_out or joins:
            propose("parallel-join", "Run independent branches in parallel with an explicit join", fan_out + joins,
                    "Dynamic parallel Steps over an AttributeMap, with a join that counts results on a Channel, and an ordering stated on purpose.",
                    "Independent branches run concurrently, and one branch's failure no longer blocks the others; canvas position stops deciding the order.")
        model_roots = [node for node in active if short_type(node.get("type", "")).startswith(("agent", "chain"))]
        parsed = []
        for node in model_roots:
            name = node.get("name", "")
            attached = [self.by_name.get(edge["from"], {}) for edge in self.edges if edge["to"] == name and edge["type"] != "main"]
            downstream_parse = [other for other in self.descendants(name, "main")
                                if re.search(r"JSON\.parse|\.split\(|\.match\(", str((self.by_name.get(other, {}).get("parameters", {}) or {}).get("jsCode", "")))]
            if any(short_type(other.get("type", "")).startswith("outputParser") for other in attached) or downstream_parse:
                parsed.append(name)
        if parsed:
            propose("structured-ai", "Generate typed results instead of parsing model text", parsed,
                    "The llm connector's structured output with a provider-enforced schema, typed results, and a route for every branch (truncated, blocked, invalid).",
                    "A model answer can no longer fail parsing after a paid call, and downstream Steps get typed fields.")
        if kinds & {"hardcoded-address", "hardcoded-identifier"}:
            propose("typed-config", "Make configuration typed and editable", by_kind["hardcoded-address"] + by_kind["hardcoded-identifier"],
                    "Typed start input with the source values as defaults, and validated Actions for values an operator changes later.",
                    "No behavior change: addresses and IDs become editable without a code change.",
                    self.finding_ids("hardcoded-address", "hardcoded-identifier"))
        glue = {node.get("name", "") for node in active if short_type(node.get("type", "")) in GLUE_TYPES}
        if len(glue) >= 3:
            parent = {name: name for name in glue}

            def root(name):
                while parent[name] != name:
                    name = parent[name]
                return name

            for edge in self.edges:
                if edge["type"] == "main" and edge["from"] in glue and edge["to"] in glue:
                    parent[root(edge["from"])] = root(edge["to"])
            groups = len({root(name) for name in glue})
            if groups < len(glue):
                propose("typed-steps", f"Collapse {len(glue)} transformation nodes into {groups} typed Step(s)", sorted(glue),
                        "One domain-named application Step in typed Go per real commit boundary, ported against the goldens.",
                        "No behavior change: fewer, typed Steps that are easier to read and test.")
        for kind, fix in DEFECT_FIXES.items():
            if kind == "zero-items-stop" and not any(item["kind"] == kind and item["severity"] == "high" for item in self.findings):
                continue  # Only an empty-result branch the author wrote but n8n never runs is a defect to fix.
            if fix and kind in kinds:
                propose("fix", fix.split(",")[0].rstrip("."), by_kind[kind], fix, "Changes source behavior that the inventory found to be a defect.", self.finding_ids(kind))
        self._proposals = proposals
        return proposals

    def mandatory_changes(self) -> list[str]:
        """Changes Dex requires in both modes; they are reported, never asked."""
        changes = []
        if any(item["kind"] in ("literal-secret", "secret-in-request", "placeholder-secret") for item in self.findings):
            changes.append("credentials move out of workflow data into connector connections")
        if any(item["kind"] == "literal-secret" for item in self.findings):
            changes.append("the owner rotates every key the export exposed")
        if any(short_type(node.get("type", "")) == "webhook" for node in self.nodes):
            changes.append("the webhook Trigger accepts only authenticated requests")
        changes.append("each connector-imposed difference in the ledger, such as a required plain-text body")
        return changes

    def intent_rows(self) -> list[tuple]:
        """Draft the intent contract: each effect, what starts it, and how often."""
        rows = []
        triggers = [node for node in self.nodes if is_trigger(node) and not node.get("disabled")]
        multi_reads = [node.get("name", "") for node in self.nodes if self.is_multi_item_read(node)]
        for node in self.nodes:
            if not self.is_effect_node(node):
                continue
            name = node.get("name", "")
            started_by = []
            for trigger in triggers:
                if name in self.descendants(trigger.get("name", ""), "main"):
                    description = "; ".join(item["description"] for item in self.schedules if item["node"] == trigger.get("name"))
                    started_by.append(trigger.get("name", "") + (f" ({description})" if description else ""))
            reads = [read for read in multi_reads if read in self.ancestors(name)]
            rows.append((name, f"{short_type(node.get('type', ''))} {(node.get('parameters', {}) or {}).get('operation', '')}".strip(),
                         ", ".join(started_by) or "nothing reaches it",
                         f"once per item from {', '.join(reads)}" if reads else "once per execution"))
        return rows

    def behavior_rows(self) -> list[tuple]:
        rows = []
        for node in self.nodes:
            if self.is_multi_item_read(node):
                kept = "alwaysOutputData emits one empty item, so downstream still runs" if node.get("alwaysOutputData") else "downstream nodes do not run and the execution succeeds"
                rows.append(("Read returns zero items", node.get("name", ""), kept))
        external = [node for node in self.nodes if (self.is_app_node(node) or short_type(node.get("type", "")) == "httpRequest")
                    and not is_trigger(node) and not node.get("disabled")]
        if external:
            handled = [node.get("name", "") for node in external if (node.get("onError") or "stopWorkflow") != "stopWorkflow" or node.get("continueOnFail")]
            retried = [node.get("name", "") for node in external if node.get("retryOnFail")]
            detail = "the node fails and the execution stops; earlier effects stay done"
            if retried:
                detail += f"; {', '.join(retried)} retry first"
            if handled:
                detail += f"; {', '.join(handled)} continue per onError"
            rows.append(("Provider error, timeout, or rejection", ", ".join(node.get("name", "") for node in external), detail))
        for node in self.nodes:
            if is_trigger(node):
                kind = short_type(node.get("type", ""))
                detail = {
                    "webhook": "every request starts its own execution; n8n does not deduplicate retries or double submits",
                    "scheduleTrigger": "every occurrence starts an execution, even while the previous one still runs; missed occurrences while n8n is down do not run",
                    "manualTrigger": "every click starts an execution",
                }.get(kind, "every delivered event starts its own execution unless the trigger node deduplicates; confirm in the node source")
                rows.append(("Duplicate or overlapping trigger", node.get("name", ""), detail))
        if self.workflow.get("active") or (self.workflow.get("triggerCount") or 0) > 0:
            rows.append(("Cutover", "the source workflow",
                         "the source is live, so both systems would act on the same events; disable it before the first live Dex effect and drain or cancel waiting executions"))
        if self.expressions:
            rows.append(("Input item lacks a field an expression reads", "expressions X1 onward",
                         "an expression yields undefined, and a method call on the missing value is swallowed, so the value is empty and the node continues; "
                         "a strict IF accepts null and undefined; Code nodes throw instead"))
        return rows


def redact_addresses(text: str) -> str:
    """Keep a note's claim but not the personal addresses it carries."""
    count = len(EMAIL.findall(text))
    return EMAIL.sub("‹address›", text) + (f" ({count} address(es) redacted)" if count else "")


def kind_is_model(node: dict) -> bool:
    """A LangChain chat or language model sub-node, which carries the provider credential."""
    kind = short_type(node.get("type", ""))
    return "langchain" in node.get("type", "") and (kind.startswith("lmChat") or kind.startswith("lmOpenAi"))


def langchain_sub_node_role(node: dict) -> str:
    """Describe a LangChain sub-node's role in its consumer, or return an empty string."""
    if "langchain" not in node.get("type", ""):
        return ""
    kind = short_type(node.get("type", ""))
    for prefix, role in LANGCHAIN_SUB_NODE_PREFIXES.items():
        if kind.startswith(prefix):
            return role
    return ""


def http_request_host(url) -> str:
    """Return the host of the literal part of an HTTP Request URL, or an empty string."""
    if not isinstance(url, str):
        return ""
    literal = url[1:] if url.startswith("=") else url
    match = re.match(r"\s*(?:https?://)?([A-Za-z0-9.-]+\.[A-Za-z]{2,})", literal)
    return match.group(1).lower() if match else ""


def catalog_search_text(entry: dict) -> str:
    return re.sub(r"[^a-z0-9]", "", " ".join((entry["id"], entry["name"], entry["company"])).lower())


def likely_operation(operation: str, entry: dict) -> str:
    verb = re.sub(r"[^a-z]", "", operation.split("/")[-1].lower())
    for prefix in OPERATION_VERBS.get(verb, [verb] if verb else []):
        for name, _ in entry["operations"]:
            if name.lower().startswith(prefix):
                return name
    return ""


def pascal_case(name: str) -> str:
    words = re.findall(r"[A-Za-z0-9]+", name)
    return "".join(word[:1].upper() + word[1:] for word in words) or "Step"


def load_catalog(path: Path) -> list:
    """Read the published connector catalog's connector IDs, versions, Triggers, and operations.

    The catalog is generated YAML with a fixed shape, so a line reader suffices and keeps the
    script free of third-party packages.
    """
    text = path.read_text(encoding="utf-8")
    if "kind: ConnectorCatalog" not in text or not re.search(r"^apiVersion: connectors\.dex\.dev/catalog/", text, re.MULTILINE):
        raise ValueError(f"{path} is not a Dex connector catalog (expected apiVersion connectors.dex.dev/catalog/* and kind ConnectorCatalog)")
    entries, current, section, pending_operation = [], None, "", None
    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        indent = len(line) - len(line.lstrip(" "))
        if indent == 4 and stripped.startswith("- company:"):
            current = {"id": "", "name": "", "company": stripped.split(":", 1)[1].strip(), "version": "", "triggers": [], "operations": []}
            entries.append(current)
            section, pending_operation = "", None
            continue
        if current is None:
            continue
        if indent == 6 and ":" in stripped and not stripped.startswith("- "):
            key, value = (part.strip() for part in stripped.split(":", 1))
            if key in ("id", "name", "version"):
                current[key] = value
            section = key if key in ("triggers", "operations", "uiUnits") else section if key == "" else key
            continue
        if indent == 8 and stripped.startswith("- name:"):
            name = stripped.split(":", 1)[1].strip()
            if section == "triggers":
                current["triggers"].append(name)
            elif section == "operations":
                pending_operation = [name, ""]
                current["operations"].append(pending_operation)
            continue
        if indent == 10 and stripped.startswith("kind:") and section == "operations" and pending_operation is not None:
            pending_operation[1] = stripped.split(":", 1)[1].strip()
    for entry in entries:
        entry["operations"] = [tuple(operation) for operation in entry["operations"]]
    return [entry for entry in entries if entry["id"]]


def describe_schedule_rule(rule: dict) -> dict:
    field = rule.get("field", "days")
    minute = int(rule.get("triggerAtMinute", 0) or 0)
    hour = int(rule.get("triggerAtHour", 0) or 0)
    if field == "seconds":
        interval = rule.get("secondsInterval", 30)
        return {"field": field, "description": f"every {interval} seconds", "cron": f"*/{interval} * * * * *", "hour": None}
    if field == "minutes":
        interval = rule.get("minutesInterval", 5)
        return {"field": field, "description": f"every {interval} minutes", "cron": f"*/{interval} * * * *", "hour": None}
    if field == "hours":
        interval = rule.get("hoursInterval", 1)
        every = "every hour" if interval == 1 else f"every {interval} hours"
        return {"field": field, "description": f"{every} at minute {minute}", "cron": f"{minute} */{interval} * * *", "hour": None}
    if field == "days":
        interval = rule.get("daysInterval", 1)
        every = "every day" if interval == 1 else f"every {interval} days"
        return {"field": field, "description": f"{every} at {hour:02d}:{minute:02d}", "cron": f"{minute} {hour} * * *" if interval == 1 else "", "hour": hour}
    if field == "weeks":
        days = rule.get("triggerAtDay", [])
        return {"field": field, "description": f"every {rule.get('weeksInterval', 1)} week(s) on weekday(s) {days or 'default'} at {hour:02d}:{minute:02d}", "cron": "", "hour": hour}
    if field == "months":
        return {"field": field, "description": f"every {rule.get('monthsInterval', 1)} month(s) on day {rule.get('triggerAtDayOfMonth', 1)} at {hour:02d}:{minute:02d}", "cron": "", "hour": hour}
    if field == "cronExpression":
        expression = rule.get("expression", "")
        return {"field": field, "description": f"cron `{expression}`", "cron": expression, "hour": None}
    return {"field": field, "description": f"unrecognized rule {json.dumps(rule)}", "cron": "", "hour": None}


def schedule_label_mismatch(label: str, rule: dict):
    text = label.lower()
    field = rule.get("field", "")
    daily = re.search(r"\b(daily|every\s+(day|morning|evening|night)|each\s+(day|morning)|morning|nightly)\b", text)
    hourly = re.search(r"\b(hourly|every\s+hour)\b", text)
    weekly = re.search(r"\b(weekly|every\s+week)\b", text)
    hour_match = re.search(r"@\s*(\d{1,2})(?::\d{2})?\s*(am|pm)?|\b(\d{1,2})(?::\d{2})?\s*(am|pm)\b", text)
    if daily and field in ("seconds", "minutes", "hours"):
        return daily.group(0)
    if hourly and field != "hours":
        return hourly.group(0)
    if weekly and field in ("seconds", "minutes", "hours", "days"):
        return weekly.group(0)
    if hour_match:
        hour = int(hour_match.group(1) or hour_match.group(3))
        suffix = hour_match.group(2) or hour_match.group(4)
        if suffix == "pm" and hour < 12:
            hour += 12
        if field in ("seconds", "minutes", "hours"):
            return hour_match.group(0).strip()
        if rule.get("hour") is not None and rule["hour"] != hour:
            return hour_match.group(0).strip()
    return None


def load_workflows(path: Path) -> list:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict) and "nodes" not in data and isinstance(data.get("data"), dict):
        data = data["data"]
    workflows = data if isinstance(data, list) else [data]
    for workflow in workflows:
        if not isinstance(workflow, dict) or not isinstance(workflow.get("nodes"), list):
            raise ValueError("not an n8n workflow export: expected an object with a nodes list")
    return workflows


LEDGER_ID = r"[NEXCSFKBRGDP]\d+[a-z]?"
DIFFERENCE_PLACEHOLDER = "(list each connector-imposed difference from workflow-import section 6, or write none found)"
BRANCH_PLACEHOLDER = "(list every branch of the operation's connector.yaml at the release tag; optional branches go to this Step's own Outcome Step)"
UNVERIFIED_NOTE = re.compile(
    r"(?i)\b(from memory|unverified|not (?:yet )?verified|unconfirmed|not (?:yet )?confirmed|assum(?:ed|ing|ptions?)|"
    r"probably|i recall|to be confirmed|needs? (?:source )?verification|confirm (?:in|against|at) (?:the )?(?:n8n )?source)\b"
)


def ledger_rows(text: str):
    """Yield (line number, cells) for rows of tables whose first header cell is ID."""
    in_ledger_table = False
    for number, line in enumerate(text.splitlines(), 1):
        if not line.startswith("|"):
            in_ledger_table = False
            continue
        cells = split_row(line)
        if cells and re.fullmatch(r"-{3,}", cells[0]):
            continue
        if cells and not re.fullmatch(LEDGER_ID, cells[0]):
            in_ledger_table = cells[0] == "ID"
            continue
        if in_ledger_table and len(cells) >= 3:
            yield number, cells


def split_row(line: str) -> list:
    cells = [part.strip() for part in re.split(r"(?<!\\)\|", line.strip())]
    return cells[1:-1] if len(cells) >= 2 else cells


def verify(path: Path, is_strict: bool = False, inventory_path: Path | None = None, is_acceptance: bool = False) -> int:
    problems, counts, seen, by_section = [], defaultdict(int), set(), defaultdict(lambda: defaultdict(int))
    text = path.read_text(encoding="utf-8")
    # A decision is defined by a Decisions table row, or by a bullet or heading that starts with its ID.
    decision_ids = set(re.findall(r"^(?:\|\s*|[-*]\s+(?:\*\*)?|#+\s+)(D\d+[a-z]?)\b", text, re.MULTILINE))
    pending_decisions = {cells[0] for _, cells in ledger_rows(text) if cells[0].startswith("D") and cells[-2].lower() == "pending"}
    proposal_ids = {cells[0] for _, cells in ledger_rows(text) if cells[0].startswith("P")}
    for number, cells in ledger_rows(text):
        line = text.splitlines()[number - 1]
        seen.add(cells[0])
        status, notes = cells[-2].lower(), cells[-1]
        allowed = DECISION_STATUSES if cells[0].startswith("D") else PROPOSAL_STATUSES if cells[0].startswith("P") else STATUSES
        spec_match = re.search(r"\bspec: (D\d+[a-z]?(?:\s*,\s*D\d+[a-z]?)*)", notes)
        spec_decisions = set(re.findall(r"D\d+[a-z]?", spec_match.group(1))) if spec_match else set()
        if status == "mapped" and spec_decisions & pending_decisions:
            # A row that reproduces specified, not observed, behavior waits on its decision.
            status_label = "mapped (conditional)"
        else:
            status_label = status
        counts[status_label] += 1
        by_section[cells[0][0]][status_label] += 1
        for reference in sorted(set(re.findall(r"\bD\d+[a-z]?\b", notes)) - decision_ids):
            problems.append(f"line {number} {cells[0]}: notes cite {reference}, which is not in the Decisions table")
        for reference in sorted(set(re.findall(r"\bP\d+[a-z]?\b", notes)) - proposal_ids):
            problems.append(f"line {number} {cells[0]}: notes cite {reference}, which is not in the redesign proposals")
        if BRANCH_PLACEHOLDER in line:
            problems.append(f"line {number} {cells[0]}: replace the placeholder with one row per connector branch")
        elif DIFFERENCE_PLACEHOLDER in line:
            problems.append(f"line {number} {cells[0]}: replace the placeholder with the connector-imposed differences")
        elif status not in allowed:
            problems.append(f"line {number} {cells[0]}: status {status!r} is not one of {', '.join(allowed)}")
        elif status == "decided" and not notes:
            problems.append(f"line {number} {cells[0]}: decided needs the user's answer in notes")
        elif status == "mapped" and UNVERIFIED_NOTE.search(re.sub(r"`[^`]*`|\"[^\"]*\"", "", notes)):
            problems.append(f"line {number} {cells[0]}: mapped rests on an unverified claim; cite the source or mark it blocked")
        elif status in NOTE_REQUIRED and not notes:
            problems.append(f"line {number} {cells[0]}: {status} needs notes")
        elif status == "pending" and cells[0][0] not in "DP" and not re.search(r"\bD\d+[a-z]?\b", notes):
            problems.append(f"line {number} {cells[0]}: pending notes must name the D row that asks the user")
        elif status == "pending" and is_strict:
            problems.append(f"line {number} {cells[0]}: pending awaits the user's decision")
        elif status == "blocked" and is_acceptance:
            problems.append(f"line {number} {cells[0]}: blocked rows must be resolved before cutover")
    if not counts:
        problems.append("no ledger rows found")
    inventory_path = inventory_path or path.parent / "inventory.json"
    if not inventory_path.exists():
        print(f"warning: {inventory_path} not found, so deleted rows go undetected; keep inventory.json with the ledger or pass --inventory",
              file=sys.stderr)
    else:
        try:
            generated = json.loads(inventory_path.read_text(encoding="utf-8")).get("ledgerIds", [])
        except (OSError, ValueError):
            generated = []
        for row_id in generated:
            if row_id not in seen and not any(other.startswith(row_id) and other[len(row_id):].isalpha() for other in seen):
                problems.append(f"{row_id}: generated row is missing from the ledger (rows may be split, never deleted)")
    summary = ", ".join(f"{count} {status}" for status, count in sorted(counts.items()))
    if problems:
        print(f"ledger incomplete ({summary}):", file=sys.stderr)
        for problem in problems:
            print(f"  {problem}", file=sys.stderr)
        return 1
    pending = counts.get("pending", 0)
    print(f"ledger complete: {summary}" + (f"; {pending} row(s) await the user's decision" if pending else ""))
    for section, statuses in sorted(by_section.items()):
        print(f"  {section}: " + ", ".join(f"{count} {status}" for status, count in sorted(statuses.items())))
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    inventory_parser = commands.add_parser("inventory", help="write the fidelity ledger for an export")
    inventory_parser.add_argument("export", type=Path)
    inventory_parser.add_argument("--out", type=Path, help="directory for ledger.md and inventory.json")
    inventory_parser.add_argument("--catalog", type=Path, help="downloaded Dex connector catalog.yaml for the capability matrix")
    inventory_parser.add_argument("--force", action="store_true", help="overwrite a ledger that already has resolved rows")
    verify_parser = commands.add_parser("verify", help="check that every ledger row is resolved")
    verify_parser.add_argument("ledger", type=Path)
    verify_parser.add_argument("--strict", action="store_true", help="also fail while any row is pending")
    verify_parser.add_argument("--accept", action="store_true", help="the cutover gate: --strict, and also fail while any row is blocked")
    verify_parser.add_argument("--inventory", type=Path, help="inventory.json generated with this ledger (default: next to it)")
    arguments = parser.parse_args(argv)

    if arguments.command == "verify":
        return verify(arguments.ledger, arguments.strict or arguments.accept, arguments.inventory, arguments.accept)

    try:
        workflows = load_workflows(arguments.export)
    except (OSError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    catalog = None
    if arguments.catalog is not None:
        try:
            catalog = load_catalog(arguments.catalog)
        except (OSError, ValueError) as error:
            print(f"error: {error}", file=sys.stderr)
            return 2
    for index, workflow in enumerate(workflows):
        inventory = Inventory(workflow, arguments.export.name, catalog).build()
        if arguments.out is None:
            print(inventory.to_markdown())
            continue
        directory = arguments.out if len(workflows) == 1 else arguments.out / f"workflow-{index + 1}"
        existing = directory / "ledger.md"
        if existing.exists() and not arguments.force and any(
                cells[-2].lower() != "todo" for _, cells in ledger_rows(existing.read_text(encoding="utf-8"))):
            print(f"error: {existing} already has resolved rows; pass --force to overwrite it", file=sys.stderr)
            return 2
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "ledger.md").write_text(inventory.to_markdown(), encoding="utf-8")
        (directory / "inventory.json").write_text(json.dumps(inventory.to_json(), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"wrote {directory / 'ledger.md'} and {directory / 'inventory.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
