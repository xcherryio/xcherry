#!/usr/bin/env node
// SPDX-License-Identifier: MIT
//
// Capture golden values for one n8n expression parameter so a Go port can
// reproduce JavaScript's semantics, such as toLowerCase, trim, and replace.
//
// Usage: node n8n_expression_golden.mjs EXPORT.json "Node name" PARAMETER.PATH FIXTURE.json
//          [--expression "={{ ... }}"] [--luxon DIRECTORY] [--allow-unsupported]
//
// PARAMETER.PATH addresses the parameter inside the node's parameters, for
// example conditions.conditions[0].leftValue or fields.values[0].stringValue.
// FIXTURE.json uses the n8n_code_golden.mjs shape:
//   {"items": [{"json": {}}], "nodes": {"Setup": [{"json": {}}]},
//    "now": "2026-01-05T07:00:00-05:00", "timezone": "America/New_York",
//    "locale": "en-US",
//    "globals": {"$execution": {"id": "1"}, "$workflow": {"name": "W"}, "$vars": {}, "$runIndex": 0}}
// An item {"jsonUndefined": true} has undefined json. Run node with LC_ALL set to the instance
// locale when the expression formats with toLocaleString.
// "globals" supplies n8n globals that the export does not contain.
//
// --expression evaluates a proposed replacement, such as a fix the user must
// approve, in place of the exported value of PARAMETER.PATH.
//
// Prints one JSON entry per item: {"value": ...}, {"undefined": true} when a
// whole-value expression yields undefined (the node receives undefined), or
// {"error": "..."} when n8n fails the node. n8n's expression engine swallows
// native JavaScript errors, such as a TypeError from a method call on a
// missing field: that segment is empty and the node continues. Such entries
// carry "swallowedErrors"; record the empty value's downstream effect, not a
// failure. Only n8n's own ExpressionError, such as reading a node that has not
// run, fails the node. The harness does not implement n8n's extension methods
// (such as .isEmpty() or .toNumber()), extended functions (such as $ifEmpty),
// or globals the fixture omits: an expression that needs one gets an
// {"unsupported": "..."} entry and the script exits 3 (0 with
// --allow-unsupported), so capture that value from an n8n execution instead. Inside a mixed template, a segment that
// yields null, undefined, NaN, or an empty string renders as empty text; false
// and 0 render as text, and objects render through String(), such as
// [object Object]. Read the expression before running it; the vm module is not
// a security boundary.

import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { delimiter, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import vm from "node:vm";

function fail(message, exitCode = 2) {
  process.stderr.write(`error: ${message}\n`);
  process.exit(exitCode);
}

const usage = 'usage: node n8n_expression_golden.mjs EXPORT.json "Node name" PARAMETER.PATH FIXTURE.json [--expression "={{ ... }}"] [--luxon DIRECTORY]';
let parsed;
try {
  parsed = parseArgs({ allowPositionals: true, options: {
    expression: { type: "string" }, luxon: { type: "string" }, "allow-unsupported": { type: "boolean" } } });
} catch (error) {
  fail(`${error.message}\n${usage}`);
}
if (parsed.positionals.length !== 4) fail(usage);
const [exportPath, nodeName, parameterPath, fixturePath] = parsed.positionals;
const { expression: override, luxon: luxonDirectory } = parsed.values;

const loaded = JSON.parse(readFileSync(exportPath, "utf8"));
const workflows = Array.isArray(loaded) ? loaded : [loaded.nodes ? loaded : loaded.data ?? loaded];
const node = workflows.flatMap((workflow) => workflow.nodes ?? []).find((candidate) => candidate.name === nodeName);
if (!node) fail(`node not found: ${nodeName}`);

// With --expression, PARAMETER.PATH only labels the golden; it need not hold an expression.
let parameter = node.parameters ?? {};
for (const segment of parameterPath.match(/[^.[\]]+/g) ?? []) {
  parameter = parameter?.[/^\d+$/.test(segment) ? Number(segment) : segment];
}
if (override !== undefined) parameter = override;
if (typeof parameter !== "string" || !parameter.startsWith("=")) {
  fail(`${parameterPath} is not an expression parameter`);
}
const template = parameter.slice(1);
const segments = [...template.matchAll(/\{\{([\s\S]*?)\}\}/g)];
// Any text around the only segment, whitespace included, makes the parameter a string template in n8n.
const isSingleExpression = segments.length === 1 && segments[0][0] === template;

// {"jsonUndefined": true} stands for an item whose json is undefined, which JSON cannot express.
const toItem = (value) => (value?.jsonUndefined === true ? { json: undefined }
  : value && typeof value === "object" && "json" in value ? value : { json: value });
const fixture = JSON.parse(readFileSync(fixturePath, "utf8"));
const items = (fixture.items ?? []).map(toItem);
const otherNodes = Object.fromEntries(
  Object.entries(fixture.nodes ?? {}).map(([name, nodeItems]) => [name, nodeItems.map(toItem)]),
);

// Luxon resolves from --luxon DIRECTORY or N8N_GOLDEN_LUXON alone when either is given; otherwise from the
// working directory, this script's directory, or NODE_PATH.
function loadLuxon(reason) {
  const pinned = luxonDirectory ?? process.env.N8N_GOLDEN_LUXON;
  const directories = pinned ? [pinned]
    : [process.cwd(), dirname(fileURLToPath(import.meta.url)), ...(process.env.NODE_PATH ?? "").split(delimiter)].filter(Boolean);
  for (const directory of directories) {
    try {
      return createRequire(join(resolve(directory), "index.js"))("luxon");
    } catch {}
  }
  fail(`${reason} reads time through Luxon, which was not found in ${directories.join(", ")}. Run \`npm install --prefix DIRECTORY luxon@VERSION\` with the Luxon version that the catalog in n8n's pnpm-workspace.yaml pins at the source release, then pass --luxon DIRECTORY`, 3);
}

let time = {};
if (/\$now|\$today|DateTime\b/.test(template)) {
  const luxon = loadLuxon("the expression");
  if (!fixture.now) fail("the expression reads time; set fixture.now to a fixed ISO instant", 3);
  if (fixture.timezone) luxon.Settings.defaultZone = fixture.timezone;
  if (fixture.locale) luxon.Settings.defaultLocale = fixture.locale;
  const now = luxon.DateTime.fromISO(fixture.now, { setZone: !fixture.timezone });
  // DateTime.now() and new DateTime() read the same fixed instant as $now.
  luxon.Settings.now = () => now.toMillis();
  time = { DateTime: luxon.DateTime, $now: now, $today: now.startOf("day") };
}

// n8n raises an ExpressionError, which fails the node, when an expression reads a node that has not run.
class ExpressionError extends Error {
  name = "ExpressionError";
}

// n8n extension methods that a native JavaScript value lacks; the harness cannot evaluate them.
const EXTENSION_METHODS = new Set([
  "isEmpty", "isNotEmpty", "toNumber", "toInt", "toFloat", "toBoolean", "toDateTime", "toJsonString", "toTitleCase",
  "toSentenceCase", "toSnakeCase", "extractDomain", "extractEmail", "extractUrl", "extractUrlPath", "isEmail", "isUrl",
  "isDomain", "isNumeric", "removeMarkdown", "removeTags", "replaceSpecialChars", "hash", "base64Encode", "base64Decode",
  "urlEncode", "urlDecode", "quote", "parseJson", "first", "last", "pluck", "unique", "compact", "chunk", "randomItem",
  "sum", "average", "max", "min", "merge", "union", "difference", "intersection", "smartJoin", "renameKeys",
  "keepFieldsContaining", "removeField", "removeFieldsContaining", "hasField", "isEven", "isOdd", "isInteger", "round",
  "ceil", "floor", "abs", "format", "beginningOf", "endOfMonth", "isWeekend", "isBetween", "isInLast", "extract",
  "toDate", "isDst", "isInFuture", "isInPast", "diffToNow", "toStringOld",
]);

// Classify an error the vm raised: null when n8n would swallow it, else why the harness cannot reproduce n8n.
function harnessGap(error) {
  const message = String(error?.message ?? error);
  if (error?.code === "ERR_SCRIPT_EXECUTION_TIMEOUT") return `harness timeout (${message}); n8n has no such limit`;
  if (error?.name === "ReferenceError" && /^\$\w+ is not defined$/.test(message)) {
    return `${message}: an n8n global or extended function the harness lacks; supply it in fixture.globals or capture the value from n8n`;
  }
  const method = message.match(/\.(\w+) is not a function$/)?.[1] ?? message.match(/^(\w+) is not a function$/)?.[1]
    ?? message.match(/reading '(\w+)'\)?$/)?.[1];
  if (error?.name === "TypeError" && EXTENSION_METHODS.has(method)) {
    return `${message}: ${method} is an n8n extension method the harness lacks; capture the value from an n8n execution`;
  }
  return null;
}

let positionalPairing = false;
function nodeAccessor(name, index) {
  const nodeItems = otherNodes[name];
  if (!nodeItems) throw new ExpressionError(`Referenced node ${JSON.stringify(name)} has no items in fixture.nodes (in n8n: the node has not been executed)`);
  return {
    first: () => nodeItems[0],
    last: () => nodeItems[nodeItems.length - 1],
    all: () => nodeItems,
    get item() {
      positionalPairing = true;
      return nodeItems[index] ?? nodeItems[0];
    },
  };
}

function evaluate(source, index, item) {
  const sandbox = vm.createContext({
    $json: item.json,
    $input: { item, all: () => items, first: () => items[0], last: () => items[items.length - 1] },
    $: (name) => nodeAccessor(name, index),
    $node: new Proxy({}, { get: (_, name) => ({ json: nodeAccessor(String(name), index).first()?.json }) }),
    $itemIndex: index,
    ...(fixture.globals ?? {}),
    ...time,
  });
  let script;
  try {
    // A trailing semicolon and a closing line comment are valid in an n8n segment.
    script = new vm.Script(`(${source.replace(/;\s*$/, "")}\n)`);
  } catch (error) {
    throw new ExpressionError(`invalid syntax: ${error.message}`);
  }
  try {
    return { value: script.runInContext(sandbox, { timeout: 1000 }) };
  } catch (error) {
    if (error?.name === "ExpressionError") throw error;
    const gap = harnessGap(error);
    if (gap) return { unsupported: gap };
    return { swallowed: String(error?.message ?? error) };
  }
}

// n8n hands a rendered URL to its HTTP client, which parses it as a WHATWG URL: a `#` starts a fragment
// that is never sent, tab, CR, and LF are deleted, and unsafe characters are percent-encoded.
function sentUrl(text) {
  try {
    const url = new URL(text);
    const sent = { sent: url.origin + url.pathname + url.search };
    return url.hash ? { ...sent, droppedFragment: url.hash } : sent;
  } catch {
    return { sent: null, note: "not a valid URL" };
  }
}

let isUnsupported = false;
const results = items.map((item, index) => {
  try {
    if (isSingleExpression) {
      const { value, swallowed, unsupported } = evaluate(segments[0][1], index, item);
      if (unsupported !== undefined) {
        isUnsupported = true;
        return { unsupported };
      }
      if (swallowed !== undefined) return { undefined: true, swallowedErrors: [swallowed] };
      if (value === undefined) return { undefined: true };
      if (typeof value === "function") return { error: "this is a function, please add ()" };
      if (Number.isNaN(value)) return { value: null, note: "NaN becomes null" };
      if (typeof value === "number" && !Number.isFinite(value)) return { value: String(value), note: "not representable in JSON" };
      return { value };
    }
    let rendered = "";
    let position = 0;
    const swallowedErrors = [];
    for (const segment of segments) {
      rendered += template.slice(position, segment.index);
      const { value, swallowed, unsupported } = evaluate(segment[1], index, item);
      if (unsupported !== undefined) {
        isUnsupported = true;
        return { unsupported };
      }
      if (swallowed !== undefined) swallowedErrors.push(swallowed);
      // n8n's template engine keeps a segment only when value || value === 0 || value === false, then joins.
      rendered += value || value === 0 || value === false ? String(value) : "";
      position = segment.index + segment[0].length;
    }
    const entry = { value: rendered + template.slice(position) };
    if (/(^|[.\]])url$/i.test(parameterPath)) Object.assign(entry, sentUrl(entry.value));
    return swallowedErrors.length ? { ...entry, swallowedErrors } : entry;
  } catch (error) {
    return { error: String(error?.message ?? error) };
  }
});
if (positionalPairing) {
  process.stderr.write("note: $('Node').item was resolved by position; confirm the paired-item lineage matches\n");
}
process.stdout.write(`${JSON.stringify(results, null, 2)}\n`);
if (isUnsupported) {
  process.stderr.write("error: the harness cannot reproduce at least one value; capture it from an n8n execution\n");
  if (!parsed.values["allow-unsupported"]) process.exit(3);
}
