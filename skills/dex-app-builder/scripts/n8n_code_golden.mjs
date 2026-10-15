#!/usr/bin/env node
// SPDX-License-Identifier: MIT
//
// Capture golden output from an n8n Code or Function node so that a Go port
// can prove parity.
//
// Usage: node n8n_code_golden.mjs EXPORT.json "Node name" FIXTURE.json [--luxon DIRECTORY]
//
// FIXTURE.json:
//   {
//     "items": [{"json": {}}],              input items of the node
//     "nodes": {"Setup": [{"json": {}}]},   items of nodes read through $('Name')
//     "now": "2026-01-05T07:00:00-05:00",   required when the code reads time
//     "timezone": "America/New_York",        optional workflow timezone
//     "locale": "en-US"                      optional Luxon locale; run node with LC_ALL set to the
//   }                                        instance locale for toLocaleString
// An item {"jsonUndefined": true} has undefined json.
//
// In Run Once for All Items mode and in a Function node, $input.item and $json
// read the first input item, as in n8n. In Run Once for Each Item mode, n8n's
// own check rejects code that uses $input.all(), .first(), .last(), or
// .itemMatching() before it runs.
//
// Prints the normalized output items as JSON on stdout; console output goes to
// stderr. A thrown error prints to stderr and exits 1, which is itself golden
// behavior: the n8n node fails. The code runs in a fresh vm context in sloppy
// mode, as the n8n Code node does. The vm module is not a security boundary:
// read the code before running it.

import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { delimiter, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import vm from "node:vm";
import { inspect } from "node:util";

function fail(message, exitCode = 2) {
  process.stderr.write(`error: ${message}\n`);
  process.exit(exitCode);
}

const usage = 'usage: node n8n_code_golden.mjs EXPORT.json "Node name" FIXTURE.json [--luxon DIRECTORY]';
let parsed;
try {
  parsed = parseArgs({ allowPositionals: true, options: { luxon: { type: "string" } } });
} catch (error) {
  fail(`${error.message}\n${usage}`);
}
if (parsed.positionals.length !== 3) fail(usage);
const [exportPath, nodeName, fixturePath] = parsed.positionals;
const luxonDirectory = parsed.values.luxon;

const loaded = JSON.parse(readFileSync(exportPath, "utf8"));
const workflows = Array.isArray(loaded) ? loaded : [loaded.nodes ? loaded : loaded.data ?? loaded];
const node = workflows.flatMap((workflow) => workflow.nodes ?? []).find((candidate) => candidate.name === nodeName);
if (!node) fail(`node not found: ${nodeName}`);

const kind = String(node.type).split(".").pop();
const parameters = node.parameters ?? {};
let code;
let mode;
if (kind === "code") {
  const language = parameters.language ?? "javaScript";
  if (language !== "javaScript") {
    fail(`unsupported Code language ${language}; capture golden output from an n8n execution instead`, 3);
  }
  code = parameters.jsCode ?? "";
  mode = parameters.mode ?? "runOnceForAllItems";
} else if (kind === "function") {
  code = parameters.functionCode ?? "";
  mode = "function";
} else if (kind === "functionItem") {
  code = parameters.functionCode ?? "";
  mode = "functionItem";
} else {
  fail(`node ${nodeName} is ${node.type}, not a Code or Function node`);
}

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
if (/\$now|\$today|DateTime\b/.test(code)) {
  const luxon = loadLuxon("the code");
  if (!fixture.now) fail("the code reads time; set fixture.now to a fixed ISO instant", 3);
  if (fixture.timezone) luxon.Settings.defaultZone = fixture.timezone;
  if (fixture.locale) luxon.Settings.defaultLocale = fixture.locale;
  const now = luxon.DateTime.fromISO(fixture.now, { setZone: !fixture.timezone });
  // DateTime.now() and new DateTime() read the same fixed instant as $now.
  luxon.Settings.now = () => now.toMillis();
  time = { DateTime: luxon.DateTime, $now: now, $today: now.startOf("day") };
}

let positionalPairing = false;
function nodeAccessor(name, index) {
  const nodeItems = otherNodes[name];
  if (!nodeItems) throw new Error(`fixture.nodes has no items for node ${JSON.stringify(name)}`);
  return {
    first: () => nodeItems[0],
    last: () => nodeItems[nodeItems.length - 1],
    all: () => nodeItems,
    get item() {
      positionalPairing = true;
      return nodeItems[index] ?? nodeItems[0];
    },
    isExecuted: true,
  };
}

const format = (values) => values.map((value) => (typeof value === "string" ? value : inspect(value, { depth: 4 }))).join(" ");
const sandboxConsole = Object.fromEntries(
  ["log", "info", "warn", "error", "debug"].map((level) => [
    level,
    (...values) => process.stderr.write(`[console.${level}] ${format(values)}\n`),
  ]),
);

let firstItemOnly = false;
// All-items Code and the legacy Function node bind $json and $input.item to the first input item.
const readsFirstItem = mode === "runOnceForAllItems" || mode === "function";
function context(index, item) {
  const perItem = mode === "runOnceForEachItem" || mode === "functionItem";
  const firstItem = () => {
    firstItemOnly = true;
    if (items.length === 0) throw new Error("No execution data available");
    return items[0];
  };
  return vm.createContext({
    $input: {
      all: () => items,
      first: () => items[0],
      last: () => items[items.length - 1],
      get item() {
        return perItem ? item : readsFirstItem ? firstItem() : undefined;
      },
      params: parameters,
    },
    get $json() {
      return perItem ? item?.json : readsFirstItem ? firstItem().json : undefined;
    },
    $: (name) => nodeAccessor(name, index),
    $node: new Proxy({}, { get: (_, name) => ({ json: nodeAccessor(String(name), index).first()?.json }) }),
    items,
    item: mode === "functionItem" ? item?.json : perItem ? item : undefined,
    $itemIndex: index,
    $runIndex: 0,
    console: sandboxConsole,
    ...time,
  });
}

async function run(sandbox) {
  const script = new vm.Script(`(async () => {\n${code}\n})()`, { filename: `${nodeName}.js` });
  return script.runInContext(sandbox, { timeout: 5000 });
}

function normalize(result, perItem) {
  if (result === undefined || result === null) throw new Error("the code returned no value");
  if (perItem && Array.isArray(result)) throw new Error("runOnceForEachItem code must return one object, not an array");
  const entries = Array.isArray(result) ? result : [result];
  return entries.map((entry) => {
    if (entry === null || typeof entry !== "object") {
      throw new Error(`the code returned a non-object item: ${JSON.stringify(entry)}`);
    }
    if (!("json" in entry)) return { json: entry };
    return entry.binary ? { json: entry.json, binary: entry.binary } : { json: entry.json };
  });
}

// n8n's own check (JsCodeValidator): the first $input.<method> mention anywhere, comments included, names the
// method; the node fails when any line that is not a comment contains that method name.
if (mode === "runOnceForEachItem") {
  const method = code.match(/\$input\.(first|last|all|itemMatching)/)?.[1];
  const line = method ? code.split("\n").findIndex((text) => {
    const trimmed = text.trimStart();
    return trimmed.includes(method) && !trimmed.startsWith("//") && !trimmed.startsWith("/*") && !trimmed.startsWith("*");
  }) : -1;
  if (line !== -1) {
    process.stderr.write(`node failed: Can't use .${method}() here (line ${line + 1}): this is only available in 'Run Once for All Items' mode\n`);
    process.exit(1);
  }
}

if (items.length === 0 && mode !== "function") {
  process.stderr.write("note: n8n never runs a node with zero input items, so this case is unreachable\n");
  process.stdout.write("[]\n");
  process.exit(0);
}

const output = [];
try {
  if (mode === "runOnceForEachItem" || mode === "functionItem") {
    for (const [index, item] of items.entries()) {
      const result = await run(context(index, item));
      output.push(...(mode === "functionItem" ? [{ json: result }] : normalize(result, true)));
    }
  } else {
    output.push(...normalize(await run(context(0, items[0])), false));
  }
} catch (error) {
  process.stderr.write(`node failed: ${error?.stack ?? error}\n`);
  process.exit(1);
}

if (firstItemOnly) {
  process.stderr.write("note: $input.item or $json read only the first input item, as n8n does in this mode\n");
}
if (positionalPairing) {
  process.stderr.write("note: $('Node').item was resolved by position; confirm the paired-item lineage matches\n");
}
// Items are persisted as JSON in n8n, so undefined properties disappear.
process.stdout.write(`${JSON.stringify(JSON.parse(JSON.stringify(output)), null, 2)}\n`);
