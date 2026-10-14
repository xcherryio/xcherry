// SPDX-License-Identifier: MIT

import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import {
  CACHE_TTL_MS,
  cachePathForClient,
  checkVersion,
  compareVersions,
  parseLatestRelease,
  parseReleaseDocument,
  parseStableVersion,
  readCache,
  renderHookOutput,
  writeCacheAtomic,
} from "../hooks/version-check.mjs";

async function fixture(t, installedVersion = "0.25.5") {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "dex-version-check-"));
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  await fs.writeFile(path.join(root, "VERSION"), `${installedVersion}\n`);
  return {
    root,
    cachePath: path.join(root, "data", "version-check.json"),
  };
}

test("stable semantic versions compare numerically", () => {
  assert.deepEqual(parseStableVersion("0.25.5"), [0, 25, 5]);
  assert.equal(compareVersions("0.25.5", "0.25.5"), 0);
  assert.equal(compareVersions("0.26.0", "0.25.5"), 1);
  assert.equal(compareVersions("0.25.4", "0.25.5"), -1);
  for (const invalid of ["v0.25.5", "0.25", "01.2.3", "0.25.5-beta.1", "junk"]) {
    assert.equal(parseStableVersion(invalid), null);
  }
});

test("only a published stable GitHub release is accepted", () => {
  assert.equal(
    parseLatestRelease({ tag_name: "v0.25.5", draft: false, prerelease: false }),
    "0.25.5",
  );
  assert.equal(
    parseLatestRelease({ tag_name: "v0.25.6", draft: true, prerelease: false }),
    null,
  );
  assert.equal(
    parseLatestRelease({ tag_name: "v0.25.6", draft: false, prerelease: true }),
    null,
  );
  assert.equal(
    parseLatestRelease({ tag_name: "v0.25.6-rc.1", draft: false, prerelease: false }),
    null,
  );
  assert.throws(() => parseReleaseDocument("{broken"), SyntaxError);
});

test("a fresh cache avoids the network and reports current", async (t) => {
  const { root, cachePath } = await fixture(t);
  const now = 10_000_000;
  await writeCacheAtomic(cachePath, {
    checkedAt: now - CACHE_TTL_MS + 1,
    latestVersion: "0.25.5",
    etag: '"fresh"',
  });
  let calls = 0;
  const result = await checkVersion({
    pluginRoot: root,
    cachePath,
    now,
    fetchRelease: async () => {
      calls += 1;
      throw new Error("must not run");
    },
  });
  assert.equal(calls, 0);
  assert.deepEqual(result, {
    state: "current",
    installedVersion: "0.25.5",
    latestVersion: "0.25.5",
    source: "cache",
  });
});

test("an expired cache is refreshed and can report outdated", async (t) => {
  const { root, cachePath } = await fixture(t);
  const now = 20_000_000;
  await writeCacheAtomic(cachePath, {
    checkedAt: now - CACHE_TTL_MS,
    latestVersion: "0.25.5",
    etag: '"old"',
  });
  const result = await checkVersion({
    pluginRoot: root,
    cachePath,
    now,
    fetchRelease: async ({ etag }) => {
      assert.equal(etag, '"old"');
      return { status: "ok", latestVersion: "0.25.6", etag: '"new"' };
    },
  });
  assert.equal(result.state, "outdated");
  assert.equal(result.source, "network");
  assert.deepEqual(await readCache(cachePath), {
    checkedAt: now,
    latestVersion: "0.25.6",
    etag: '"new"',
  });
  const files = await fs.readdir(path.dirname(cachePath));
  assert.deepEqual(files, ["version-check.json"]);
});

test("ETag 304 renews the existing cache atomically", async (t) => {
  const { root, cachePath } = await fixture(t);
  const now = 30_000_000;
  await writeCacheAtomic(cachePath, {
    checkedAt: 1,
    latestVersion: "0.25.5",
    etag: '"same"',
  });
  const result = await checkVersion({
    pluginRoot: root,
    cachePath,
    now,
    fetchRelease: async ({ etag }) => {
      assert.equal(etag, '"same"');
      return { status: "not-modified" };
    },
  });
  assert.equal(result.state, "current");
  assert.equal(result.source, "etag");
  assert.equal((await readCache(cachePath)).checkedAt, now);
});

test("timeouts, GitHub failures, invalid responses, and missing cache fail open", async (t) => {
  const cases = [
    async () => { throw new Error("timeout"); },
    async () => { throw new Error("GitHub returned HTTP 503"); },
    async () => ({ status: "ok", latestVersion: "not-semver" }),
    async () => ({ status: "not-modified" }),
  ];
  for (const fetchRelease of cases) {
    const { root, cachePath } = await fixture(t);
    const result = await checkVersion({ pluginRoot: root, cachePath, fetchRelease });
    assert.equal(result.state, "unavailable");
    assert.equal(result.installedVersion, "0.25.5");
  }
});

test("client cache directories use only their documented storage", () => {
  assert.equal(
    cachePathForClient("codex", { PLUGIN_DATA: "/codex-data" }, "linux", "/home/me"),
    path.join("/codex-data", "version-check.json"),
  );
  assert.equal(
    cachePathForClient("claude", { CLAUDE_PLUGIN_DATA: "/claude-data" }, "linux", "/home/me"),
    path.join("/claude-data", "version-check.json"),
  );
  assert.equal(
    cachePathForClient("cursor", {}, "darwin", "/Users/me"),
    path.join("/Users/me", "Library", "Caches", "superdurable-dex", "version-check.json"),
  );
});

test("all clients emit the exact private context schema", () => {
  const outdated = {
    state: "outdated",
    installedVersion: "0.25.5",
    latestVersion: "0.25.6",
  };
  for (const client of ["codex", "claude"]) {
    const output = renderHookOutput(client, outdated);
    assert.deepEqual(Object.keys(output), ["hookSpecificOutput"]);
    assert.equal(output.hookSpecificOutput.hookEventName, "SessionStart");
    assert.match(output.hookSpecificOutput.additionalContext, /status=outdated/);
    assert.match(output.hookSpecificOutput.additionalContext, /first substantive Dex-related response only/);
  }
  const cursor = renderHookOutput("cursor", outdated);
  assert.deepEqual(Object.keys(cursor), ["additional_context"]);
  assert.match(cursor.additional_context, /status=outdated/);
  assert.deepEqual(renderHookOutput("cursor", outdated, { refreshOnly: true }), {});
});

test("current and unavailable states never create a visible update notice", () => {
  const current = renderHookOutput("codex", {
    state: "current",
    installedVersion: "0.25.5",
    latestVersion: "0.25.5",
  }).hookSpecificOutput.additionalContext;
  assert.match(current, /do not mention version status to the user/i);
  assert.doesNotMatch(current, /BTW/);

  const unavailable = renderHookOutput("cursor", { state: "unavailable" }).additional_context;
  assert.match(unavailable, /follow its plugin-version-check fallback once/);
  assert.doesNotMatch(unavailable, /BTW/);
});

test("Cursor workspace prewarm makes sessionStart cache-only", async (t) => {
  const { root, cachePath } = await fixture(t);
  let calls = 0;
  const fetchRelease = async () => {
    calls += 1;
    return { status: "ok", latestVersion: "0.25.6", etag: '"cursor"' };
  };
  const prewarm = await checkVersion({ pluginRoot: root, cachePath, fetchRelease });
  assert.equal(prewarm.state, "outdated");
  assert.deepEqual(renderHookOutput("cursor", prewarm, { refreshOnly: true }), {});

  const session = await checkVersion({
    pluginRoot: root,
    cachePath,
    fetchRelease: async () => {
      throw new Error("sessionStart must use the prewarmed cache");
    },
  });
  assert.equal(calls, 1);
  assert.equal(session.source, "cache");
  assert.match(renderHookOutput("cursor", session).additional_context, /status=outdated/);
});

test("Cursor sessionStart never starts a network refresh on a cache miss", async (t) => {
  const { root, cachePath } = await fixture(t);
  let calls = 0;
  const result = await checkVersion({
    pluginRoot: root,
    cachePath,
    allowNetwork: false,
    fetchRelease: async () => {
      calls += 1;
      return { status: "ok", latestVersion: "0.25.6" };
    },
  });
  assert.equal(calls, 0);
  assert.deepEqual(result, {
    state: "unavailable",
    installedVersion: "0.25.5",
  });
});
