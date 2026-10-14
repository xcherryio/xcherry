#!/usr/bin/env node
// SPDX-License-Identifier: MIT
//
// Capture the fire times of an n8n schedule, including DST days, so a Dex
// scheduler Flow can reproduce them.
//
// Usage: node n8n_schedule_golden.mjs "CRON" ZONE FROM TO [--cron DIRECTORY]
//
// CRON is the expression the Schedule node builds at the source release (read
// toCronExpression in packages/nodes-base/nodes/Schedule/GenericFunctions.ts),
// with the second first, such as "37 9 * * * *" for an hourly rule at minute 9
// whose second n8n picked as 37. ZONE is the workflow's IANA timezone. FROM and
// TO are ISO 8601 instants that bound the output.
//
// The script loads the `cron` package that n8n's scheduler uses, from --cron
// DIRECTORY or N8N_GOLDEN_CRON (install the version n8n pins at the release
// with `npm install --prefix DIRECTORY cron@VERSION`), and walks its CronTime
// the way a CronJob does. It prints one ISO 8601 instant with its offset per
// fire. The Schedule node also filters some interval rules at fire time
// (recurrence checks for intervals that do not divide the hour or day); this
// script prints the unfiltered cron fires.

import { createRequire } from "node:module";
import { join, resolve } from "node:path";
import { parseArgs } from "node:util";

function fail(message, exitCode = 2) {
  process.stderr.write(`error: ${message}\n`);
  process.exit(exitCode);
}

const usage = 'usage: node n8n_schedule_golden.mjs "CRON" ZONE FROM TO [--cron DIRECTORY]';
let parsed;
try {
  parsed = parseArgs({ allowPositionals: true, options: { cron: { type: "string" } } });
} catch (error) {
  fail(`${error.message}\n${usage}`);
}
if (parsed.positionals.length !== 4) fail(usage);
const [expression, zone, fromText, toText] = parsed.positionals;
const from = new Date(fromText);
const to = new Date(toText);
if (Number.isNaN(from.getTime()) || Number.isNaN(to.getTime()) || to <= from) fail("FROM and TO must be ISO 8601 instants with FROM before TO");

const directory = parsed.values.cron ?? process.env.N8N_GOLDEN_CRON;
if (!directory) fail("pass --cron DIRECTORY (or N8N_GOLDEN_CRON) where the cron package that n8n pins is installed", 3);
let cron;
try {
  cron = createRequire(join(resolve(directory), "index.js"))("cron");
} catch {
  fail(`the cron package was not found in ${directory}; run \`npm install --prefix ${directory} cron@VERSION\` with the version n8n pins at the release`, 3);
}

let time;
try {
  time = new cron.CronTime(expression, zone);
} catch (error) {
  fail(`cron rejected the expression or zone: ${error.message}`);
}

const fires = [];
let cursor = from;
for (let count = 0; count < 100000; count += 1) {
  const next = time.getNextDateFrom(cursor, zone);
  const instant = typeof next.toJSDate === "function" ? next.toJSDate() : new Date(next);
  if (instant > to) break;
  fires.push(typeof next.toISO === "function" ? next.toISO() : instant.toISOString());
  cursor = instant;
}
process.stdout.write(`${JSON.stringify(fires, null, 2)}\n`);
