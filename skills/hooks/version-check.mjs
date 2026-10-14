#!/usr/bin/env node
// SPDX-License-Identifier: MIT

import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import https from "node:https";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

export const RELEASE_URL =
  "https://api.github.com/repos/superdurable/dex-skills/releases/latest";
export const UPGRADE_URL =
  "https://docs.superdurable.io/build-with-ai/dex-developer-skill/#keep-the-plugin-current";
export const CACHE_TTL_MS = 15 * 60 * 1000;
export const HTTP_TIMEOUT_MS = 1000;

const STABLE_SEMVER = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/;
const STABLE_RELEASE_TAG = /^v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/;
const MAX_RESPONSE_BYTES = 64 * 1024;

export function parseStableVersion(value) {
  if (typeof value !== "string" || !STABLE_SEMVER.test(value)) {
    return null;
  }
  return value.split(".").map((part) => Number(part));
}

export function compareVersions(left, right) {
  const leftParts = parseStableVersion(left);
  const rightParts = parseStableVersion(right);
  if (leftParts === null || rightParts === null) {
    throw new TypeError("versions must be stable MAJOR.MINOR.PATCH values");
  }
  for (let index = 0; index < 3; index += 1) {
    if (leftParts[index] !== rightParts[index]) {
      return leftParts[index] < rightParts[index] ? -1 : 1;
    }
  }
  return 0;
}

export function parseLatestRelease(value) {
  if (
    value === null ||
    typeof value !== "object" ||
    value.draft !== false ||
    value.prerelease !== false ||
    typeof value.tag_name !== "string"
  ) {
    return null;
  }
  const match = STABLE_RELEASE_TAG.exec(value.tag_name);
  return match === null ? null : match.slice(1).join(".");
}

export function parseReleaseDocument(value) {
  const latestVersion = parseLatestRelease(JSON.parse(value));
  if (latestVersion === null) {
    throw new Error("GitHub did not return a stable release");
  }
  return latestVersion;
}

function normalizeCache(value) {
  if (
    value === null ||
    typeof value !== "object" ||
    !Number.isFinite(value.checkedAt) ||
    value.checkedAt < 0 ||
    parseStableVersion(value.latestVersion) === null ||
    (value.etag !== undefined && typeof value.etag !== "string")
  ) {
    return null;
  }
  return {
    checkedAt: value.checkedAt,
    latestVersion: value.latestVersion,
    ...(value.etag ? { etag: value.etag } : {}),
  };
}

export async function readCache(cachePath) {
  try {
    return normalizeCache(JSON.parse(await fs.readFile(cachePath, "utf8")));
  } catch {
    return null;
  }
}

export async function writeCacheAtomic(cachePath, cache) {
  const normalized = normalizeCache(cache);
  if (normalized === null) {
    throw new TypeError("refusing to write an invalid version cache");
  }
  const directory = path.dirname(cachePath);
  const temporaryPath = `${cachePath}.${process.pid}.${randomUUID()}.tmp`;
  await fs.mkdir(directory, { recursive: true, mode: 0o700 });
  try {
    await fs.writeFile(temporaryPath, `${JSON.stringify(normalized)}\n`, {
      encoding: "utf8",
      mode: 0o600,
    });
    await fs.rename(temporaryPath, cachePath);
  } finally {
    await fs.rm(temporaryPath, { force: true }).catch(() => {});
  }
}

export function cachePathForClient(
  client,
  environment = process.env,
  platform = process.platform,
  homeDirectory = os.homedir(),
) {
  if (client === "codex") {
    return environment.PLUGIN_DATA
      ? path.join(environment.PLUGIN_DATA, "version-check.json")
      : null;
  }
  if (client === "claude") {
    return environment.CLAUDE_PLUGIN_DATA
      ? path.join(environment.CLAUDE_PLUGIN_DATA, "version-check.json")
      : null;
  }
  if (client !== "cursor") {
    return null;
  }
  if (platform === "win32") {
    const base = environment.LOCALAPPDATA;
    return base
      ? path.join(base, "SuperDurable", "Dex", "version-check.json")
      : null;
  }
  if (platform === "darwin") {
    return homeDirectory
      ? path.join(
          homeDirectory,
          "Library",
          "Caches",
          "superdurable-dex",
          "version-check.json",
        )
      : null;
  }
  const base = environment.XDG_CACHE_HOME ||
    (homeDirectory ? path.join(homeDirectory, ".cache") : null);
  return base
    ? path.join(base, "superdurable-dex", "version-check.json")
    : null;
}

export function fetchLatestRelease({
  endpoint = RELEASE_URL,
  etag,
  timeoutMs = HTTP_TIMEOUT_MS,
} = {}) {
  return new Promise((resolve, reject) => {
    const headers = {
      Accept: "application/vnd.github+json",
      "User-Agent": "superdurable-dex-version-check",
      "X-GitHub-Api-Version": "2022-11-28",
    };
    if (etag) {
      headers["If-None-Match"] = etag;
    }

    let settled = false;
    const finish = (callback, value) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      callback(value);
    };
    const request = https.get(endpoint, { headers }, (response) => {
      if (response.statusCode === 304) {
        response.resume();
        finish(resolve, { status: "not-modified" });
        return;
      }
      if (response.statusCode !== 200) {
        response.resume();
        finish(reject, new Error(`GitHub returned HTTP ${response.statusCode}`));
        return;
      }

      const chunks = [];
      let size = 0;
      response.on("data", (chunk) => {
        size += chunk.length;
        if (size > MAX_RESPONSE_BYTES) {
          request.destroy(new Error("GitHub response exceeded the size limit"));
          return;
        }
        chunks.push(chunk);
      });
      response.on("end", () => {
        try {
          const latestVersion = parseReleaseDocument(
            Buffer.concat(chunks).toString("utf8"),
          );
          finish(resolve, {
            status: "ok",
            latestVersion,
            ...(typeof response.headers.etag === "string"
              ? { etag: response.headers.etag }
              : {}),
          });
        } catch (error) {
          finish(reject, error);
        }
      });
      response.on("error", (error) => finish(reject, error));
    });
    const timer = setTimeout(() => {
      request.destroy(new Error("GitHub release request timed out"));
    }, timeoutMs);
    request.on("error", (error) => finish(reject, error));
  });
}

export async function readInstalledVersion(pluginRoot) {
  const value = (await fs.readFile(path.join(pluginRoot, "VERSION"), "utf8")).trim();
  if (parseStableVersion(value) === null) {
    throw new Error("installed VERSION is not a stable semantic version");
  }
  return value;
}

function resultForVersions(installedVersion, latestVersion, source) {
  return {
    state: compareVersions(latestVersion, installedVersion) > 0
      ? "outdated"
      : "current",
    installedVersion,
    latestVersion,
    source,
  };
}

export async function checkVersion({
  pluginRoot,
  cachePath,
  now = Date.now(),
  ttlMs = CACHE_TTL_MS,
  allowNetwork = true,
  fetchRelease = fetchLatestRelease,
} = {}) {
  let installedVersion;
  try {
    installedVersion = await readInstalledVersion(pluginRoot);
  } catch {
    return { state: "unavailable" };
  }
  if (!cachePath) {
    return { state: "unavailable", installedVersion };
  }

  const cache = await readCache(cachePath);
  if (cache !== null && now - cache.checkedAt >= 0 && now - cache.checkedAt < ttlMs) {
    return resultForVersions(installedVersion, cache.latestVersion, "cache");
  }
  if (!allowNetwork) {
    return { state: "unavailable", installedVersion };
  }

  try {
    const release = await fetchRelease({ etag: cache?.etag });
    if (release.status === "not-modified") {
      if (cache === null) {
        return { state: "unavailable", installedVersion };
      }
      const refreshed = { ...cache, checkedAt: now };
      await writeCacheAtomic(cachePath, refreshed).catch(() => {});
      return resultForVersions(installedVersion, cache.latestVersion, "etag");
    }
    if (
      release.status !== "ok" ||
      parseStableVersion(release.latestVersion) === null
    ) {
      return { state: "unavailable", installedVersion };
    }
    const nextCache = {
      checkedAt: now,
      latestVersion: release.latestVersion,
      ...(release.etag ? { etag: release.etag } : {}),
    };
    await writeCacheAtomic(cachePath, nextCache).catch(() => {});
    return resultForVersions(installedVersion, release.latestVersion, "network");
  } catch {
    return { state: "unavailable", installedVersion };
  }
}

export function versionContext(result) {
  if (result.state === "outdated") {
    return [
      "Dex Skills lifecycle version check: status=outdated.",
      `Installed v${result.installedVersion}; latest stable v${result.latestVersion}.`,
      "Do not perform another version check in this chat.",
      "In the first substantive Dex-related response only, append one brief BTW update notice localized to the user's language, including both versions and this upgrade URL:",
      UPGRADE_URL,
      "Never show the notice for an unrelated request, never repeat it later in the chat, and never auto-upgrade the plugin.",
    ].join(" ");
  }
  if (result.state === "current") {
    return [
      "Dex Skills lifecycle version check: status=current.",
      `Installed v${result.installedVersion}; latest stable v${result.latestVersion}.`,
      "Do not perform the Skill fallback version check and do not mention version status to the user.",
    ].join(" ");
  }
  return [
    "Dex Skills lifecycle version check: status=unavailable.",
    "Do not claim the plugin is current.",
    "If a Dex skill is used, follow its plugin-version-check fallback once without blocking the task; otherwise remain silent.",
  ].join(" ");
}

export function renderHookOutput(client, result, { refreshOnly = false } = {}) {
  if (refreshOnly) {
    return {};
  }
  const context = versionContext(result);
  if (client === "cursor") {
    return { additional_context: context };
  }
  if (client === "codex" || client === "claude") {
    return {
      hookSpecificOutput: {
        hookEventName: "SessionStart",
        additionalContext: context,
      },
    };
  }
  throw new TypeError(`unsupported client: ${client}`);
}

function parseArguments(argumentsList) {
  let client = null;
  let refreshOnly = false;
  for (let index = 0; index < argumentsList.length; index += 1) {
    const argument = argumentsList[index];
    if (argument === "--client") {
      client = argumentsList[index + 1] ?? null;
      index += 1;
    } else if (argument === "--refresh-only") {
      refreshOnly = true;
    } else {
      throw new Error(`unknown argument: ${argument}`);
    }
  }
  if (!new Set(["codex", "claude", "cursor"]).has(client)) {
    throw new Error("--client must be codex, claude, or cursor");
  }
  return { client, refreshOnly };
}

export async function run(argumentsList = process.argv.slice(2)) {
  let client = "codex";
  let refreshOnly = false;
  let result = { state: "unavailable" };
  try {
    ({ client, refreshOnly } = parseArguments(argumentsList));
    const pluginRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
    result = await checkVersion({
      pluginRoot,
      cachePath: cachePathForClient(client),
      allowNetwork: client !== "cursor" || refreshOnly,
    });
  } catch {
    // Hooks must fail open. The unavailable context preserves the Skill fallback.
  }
  process.stdout.write(`${JSON.stringify(renderHookOutput(client, result, { refreshOnly }))}\n`);
}

if (
  process.argv[1] &&
  pathToFileURL(path.resolve(process.argv[1])).href === import.meta.url
) {
  await run();
}
