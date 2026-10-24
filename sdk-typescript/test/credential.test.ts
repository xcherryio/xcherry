// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createServer as createHttpServer, type IncomingHttpHeaders } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { inspect } from "node:util";

import {
  Server,
  ServerCredentials,
  credentials,
  Metadata,
  status,
  type ServiceError,
  type ServerDuplexStream,
} from "@grpc/grpc-js";

import { ApiKeyCredential, Client, FileCredential, Registry, Worker, type BlobCache, type Credential } from "../src/index.js";
import {
  FlowServiceService,
  FlowServiceClient,
  WorkerTaskServiceService,
  WorkerTaskServiceClient,
  type FlowServiceServer,
  type PollWorkerTaskRequest,
  type PollWorkerTaskResponse,
  type WorkerTaskServiceServer,
} from "../src/gen/dex.js";
import { credentialInterceptors } from "../src/credential.js";
import { PullWorker } from "../src/pull-worker.js";
import { WorkerDispatcher } from "../src/worker-dispatcher.js";

type TaskStream = ServerDuplexStream<PollWorkerTaskRequest, PollWorkerTaskResponse>;

const apiKey = "private-api-key-3c4e";
const firstAccessToken = "private-access-token-a8fb";

test("ApiKeyCredential validates required values, URL security, and timeout without I/O", () => {
  const valid = { tokenUrl: "http://127.2.3.4/token", serverId: "dex-server", apiKey };
  for (const field of ["tokenUrl", "serverId", "apiKey"] as const) {
    assert.throws(() => new ApiKeyCredential({ ...valid, [field]: "" }), TypeError);
  }
  for (const tokenUrl of ["not a URL", "ftp://localhost/token", "http://example.com/token", "http://[::2]/token"]) {
    assert.throws(() => new ApiKeyCredential({ ...valid, tokenUrl }), TypeError);
  }
  assert.throws(() => new ApiKeyCredential({ ...valid, exchangeTimeoutMs: 0 }), TypeError);
  for (const tokenUrl of ["https://example.com/token", "http://localhost/token", "http://127.255.0.1/token", "http://[::1]/token"]) {
    assert.doesNotThrow(() => new ApiKeyCredential({ ...valid, tokenUrl }));
  }
});

test("ApiKeyCredential sends the exact exchange request and accepts valid response fields", async () => {
  const endpoint = await startTokenEndpoint(() => ({ statusCode: 200, body: JSON.stringify({
    access_token: firstAccessToken, token_type: "bEaReR", expires_in: 60, extra: true,
  }) }));
  try {
    const credential = new ApiKeyCredential({ tokenUrl: endpoint.url, serverId: "dex-server", apiKey });
    assert.equal(await credential.token(), firstAccessToken);
    assert.equal(await credential.token(), firstAccessToken);
    assert.equal(endpoint.requests.length, 1);
    assert.deepEqual(endpoint.requests[0], {
      method: "POST",
      path: "/token",
      body: JSON.stringify({ server_id: "dex-server", api_key: apiKey }),
      contentType: "application/json",
      accept: "application/json",
    });
    for (const formatted of [String(credential), inspect(credential), inspect(credential, { showHidden: true }),
      inspect(credential, { customInspect: false, showHidden: true }), JSON.stringify(credential)]) {
      assert.doesNotMatch(formatted, new RegExp(`${apiKey}|${firstAccessToken}`));
    }
  } finally {
    await endpoint.close();
  }
});

test("ApiKeyCredential validates each response field and redacts every exchange error", async () => {
  const failures = [
    { statusCode: 200, body: "not-json", detail: "invalid JSON" },
    { statusCode: 200, body: "[]", detail: "invalid response object" },
    { statusCode: 200, body: JSON.stringify({ token_type: "Bearer", expires_in: 2 }), detail: "access_token" },
    { statusCode: 200, body: JSON.stringify({ access_token: 1, token_type: "Bearer", expires_in: 2 }), detail: "access_token" },
    { statusCode: 200, body: JSON.stringify({ access_token: "", token_type: "Bearer", expires_in: 2 }), detail: "access_token" },
    { statusCode: 200, body: JSON.stringify({ access_token: firstAccessToken, expires_in: 2 }), detail: "token_type" },
    { statusCode: 200, body: JSON.stringify({ access_token: firstAccessToken, token_type: "Basic", expires_in: 2 }), detail: "token_type" },
    ...[0, -1, 1.5, "2", null].map((expires_in) => ({
      statusCode: 200,
      body: JSON.stringify({ access_token: firstAccessToken, token_type: "Bearer", expires_in }),
      detail: "expires_in",
    })),
    { statusCode: 200, body: JSON.stringify({ access_token: firstAccessToken, token_type: "Bearer" }), detail: "expires_in" },
    { statusCode: 201, body: JSON.stringify({ access_token: firstAccessToken, token_type: "Bearer", expires_in: 2 }), detail: "exchange rejected" },
    { statusCode: 503, body: "not-json", detail: "invalid JSON" },
    { statusCode: 503, body: JSON.stringify({ access_token: firstAccessToken, error: "unavailable", error_description: `${apiKey} ${firstAccessToken} ${"x".repeat(250)}` }), detail: "unavailable" },
  ];
  for (const expected of failures) {
    const endpoint = await startTokenEndpoint(() => expected);
    try {
      const credential = new ApiKeyCredential({ tokenUrl: endpoint.url, serverId: "dex-server", apiKey });
      await assert.rejects(credential.token(), (failure: unknown) => {
        const formatted = `${String(failure)} ${inspect(failure)}`;
        assert.match(formatted, new RegExp(endpoint.url.replaceAll("/", "\\/")));
        assert.match(formatted, new RegExp(`HTTP status ${expected.statusCode}`));
        assert.match(formatted, new RegExp(expected.detail));
        assert.doesNotMatch(formatted, new RegExp(`${apiKey}|${firstAccessToken}`));
        assert.doesNotMatch(formatted, /x{201}/);
        return true;
      });
    } finally {
      await endpoint.close();
    }
  }
});

test("ApiKeyCredential shares the first exchange among concurrent callers", async () => {
  let releaseExchange!: () => void;
  const gate = new Promise<void>((resolve) => { releaseExchange = resolve; });
  const endpoint = await startTokenEndpoint(async () => {
    await gate;
    return tokenResponse(firstAccessToken, 60);
  });
  try {
    const credential = new ApiKeyCredential({ tokenUrl: endpoint.url, serverId: "dex-server", apiKey });
    const calls = Array.from({ length: 20 }, () => credential.token());
    assert.equal(calls[0], calls[1]);
    await until(() => endpoint.requests.length === 1);
    assert.equal(endpoint.requests.length, 1);
    releaseExchange();
    assert.deepEqual(await Promise.all(calls), Array(20).fill(firstAccessToken));
  } finally {
    releaseExchange();
    await endpoint.close();
  }
});

test("ApiKeyCredential refreshes once after 80 percent and serves the cached token during exchange", async () => {
  let releaseRefresh!: () => void;
  const refreshGate = new Promise<void>((resolve) => { releaseRefresh = resolve; });
  const endpoint = await startTokenEndpoint(async (index) => {
    if (index === 1) return tokenResponse(firstAccessToken, 2);
    await refreshGate;
    return tokenResponse("refreshed-token-b", 60);
  });
  try {
    const credential = new ApiKeyCredential({ tokenUrl: endpoint.url, serverId: "dex-server", apiKey });
    assert.equal(await credential.token(), firstAccessToken);
    await new Promise((resolve) => setTimeout(resolve, 1_650));
    assert.deepEqual(await Promise.all(Array.from({ length: 20 }, () => credential.token())), Array(20).fill(firstAccessToken));
    await until(() => endpoint.requests.length === 2);
    assert.equal(await credential.token(), firstAccessToken);
    assert.equal(endpoint.requests.length, 2);
    releaseRefresh();
    await until(async () => await credential.token() === "refreshed-token-b");
    assert.equal(endpoint.requests.length, 2);
  } finally {
    releaseRefresh();
    await endpoint.close();
  }
});

test("ApiKeyCredential serves a valid token while refresh fails, then fails after expiry", async () => {
  const endpoint = await startTokenEndpoint((index) => index === 1
    ? tokenResponse(firstAccessToken, 2)
    : { statusCode: 503, body: JSON.stringify({ error: "unavailable" }) });
  try {
    const credential = new ApiKeyCredential({ tokenUrl: endpoint.url, serverId: "dex-server", apiKey });
    assert.equal(await credential.token(), firstAccessToken);
    await new Promise((resolve) => setTimeout(resolve, 1_650));
    assert.equal(await credential.token(), firstAccessToken);
    await until(() => endpoint.requests.length === 2);
    assert.equal(await credential.token(), firstAccessToken);
    assert.equal(endpoint.requests.length, 2);
    await new Promise((resolve) => setTimeout(resolve, 400));
    await assert.rejects(credential.token(), /HTTP status 503/);
    assert.equal(endpoint.requests.length, 3);
  } finally {
    await endpoint.close();
  }
});

test("ApiKeyCredential bounds transport failures and timeouts without exposing secrets", async () => {
  const tokenUrl = "http://localhost/token";
  const transport = new ApiKeyCredential({ tokenUrl, serverId: "dex-server", apiKey,
    fetch: async () => { throw new Error(`${apiKey} ${firstAccessToken}`); } });
  await assert.rejects(transport.token(), (failure: unknown) => {
    assert.match(String(failure), /HTTP status unavailable: transport failure/);
    assert.doesNotMatch(inspect(failure), new RegExp(`${apiKey}|${firstAccessToken}`));
    return true;
  });
  const timeout = new ApiKeyCredential({ tokenUrl, serverId: "dex-server", apiKey, exchangeTimeoutMs: 20,
    fetch: async (_input, init) => new Promise<Response>((_resolve, reject) => {
      init?.signal?.addEventListener("abort", () => reject(new Error(apiKey)));
    }) });
  await assert.rejects(within(timeout.token()), (failure: unknown) => {
    assert.match(String(failure), /HTTP status unavailable: exchange timed out/);
    assert.doesNotMatch(inspect(failure), new RegExp(`${apiKey}|${firstAccessToken}`));
    return true;
  });
  const ignoresAbort = new ApiKeyCredential({ tokenUrl, serverId: "dex-server", apiKey, exchangeTimeoutMs: 20,
    fetch: async () => new Promise<Response>(() => {}) });
  await assert.rejects(within(ignoresAbort.token()), /HTTP status unavailable: exchange timed out/);
});

test("Client sends current async bearer token on each unary call and none by default", async () => {
  const server = await startServer();
  const registry = new Registry([]);
  const cache = {} as BlobCache;
  let currentToken = "first-token";
  let tokenCalls = 0;
  const credential: Credential = { async token() { tokenCalls++; return currentToken; } };
  const client = new Client(registry, cache, { serverAddress: server.address, credential });
  const anonymousClient = new Client(registry, cache, { serverAddress: server.address });
  try {
    await client.searchFlows("", 1);
    currentToken = "second-token";
    await client.searchFlows("", 1);
    await anonymousClient.searchFlows("", 1);
    assert.deepEqual(server.flowHeaders, ["Bearer first-token", "Bearer second-token", undefined]);
    assert.equal(tokenCalls, 2);
  } finally {
    await client.close();
    await anonymousClient.close();
    await server.close();
  }
});

test("credential errors fail unary calls without contacting Dex", async () => {
  const server = await startServer();
  const client = new Client(new Registry([]), {} as BlobCache, {
    serverAddress: server.address,
    credential: { token: async () => { throw new Error("token unavailable"); } },
  });
  try {
    await assert.rejects(client.searchFlows("", 1), (failure: unknown) => {
      assert.equal((failure as { code?: number }).code, status.UNAUTHENTICATED);
      assert.match(String(failure), /token unavailable/);
      return true;
    });
    assert.deepEqual(server.flowHeaders, []);
  } finally {
    await client.close();
    await server.close();
  }
});

test("a unary deadline settles before a pending token and drops the late token", async () => {
  const server = await startServer();
  let provideToken!: (token: string) => void;
  const tokenPromise = new Promise<string>((resolve) => { provideToken = resolve; });
  const client = new FlowServiceClient(server.address, credentials.createInsecure(), {
    interceptors: credentialInterceptors({ token: () => tokenPromise }),
  });
  try {
    const result = new Promise<ServiceError>((resolve, reject) => {
      client.searchFlows({ query: "", pageSize: 1, nextPageToken: "", includeContinuedAsNew: false },
        new Metadata(), { deadline: Date.now() + 80 }, (failure) => {
          if (failure === null) reject(new Error("expected deadline failure"));
          else resolve(failure);
        });
    });
    const failure = await within(result);
    assert.equal(failure.code, status.DEADLINE_EXCEEDED);
    await until(() => (client.getChannel() as unknown as { internalChannel: { callCount: number } }).internalChannel.callCount === 0);
    provideToken("too-late");
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.deepEqual(server.flowHeaders, []);
  } finally {
    provideToken("too-late");
    client.close();
    await server.close();
  }
});

test("cancelling a pull stream settles before a pending token", async () => {
  const server = await startServer();
  let provideToken!: (token: string) => void;
  const tokenPromise = new Promise<string>((resolve) => { provideToken = resolve; });
  const client = new WorkerTaskServiceClient(server.address, credentials.createInsecure(), {
    interceptors: credentialInterceptors({ token: () => tokenPromise }),
  });
  try {
    const stream = client.pollWorkerTask();
    const result = new Promise<ServiceError>((resolve) => stream.once("error", resolve));
    stream.cancel();
    const failure = await within(result);
    assert.equal(failure.code, status.CANCELLED);
    await until(() => (client.getChannel() as unknown as { internalChannel: { callCount: number } }).internalChannel.callCount === 0);
    provideToken("too-late");
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.deepEqual(server.taskHeaders, []);
  } finally {
    provideToken("too-late");
    client.close();
    await server.close();
  }
});

test("synchronous credential throws preserve the cause and never contact Dex", async () => {
  const server = await startServer();
  const original = new Error("read failed");
  const client = new Client(new Registry([]), {} as BlobCache, {
    serverAddress: server.address,
    credential: { token() { throw original; } },
  });
  try {
    await assert.rejects(client.searchFlows("", 1), (failure: unknown) => {
      assert.equal((failure as { code?: number }).code, status.UNAUTHENTICATED);
      assert.equal(((failure as Error).cause as Error).cause, original);
      return true;
    });
    assert.deepEqual(server.flowHeaders, []);
  } finally {
    await client.close();
    await server.close();
  }
});

test("pull stream credential failures back off without opening a stream", async () => {
  const server = await startServer();
  const previousConsoleError = console.error;
  const loggedFailures: unknown[][] = [];
  console.error = (...values: unknown[]) => { loggedFailures.push(values); };
  const worker = new PullWorker(new Registry([]), {} as WorkerDispatcher, server.address, {}, {
    token: () => { throw new Error("file unavailable"); },
  });
  try {
    worker.start();
    const polls = (worker as unknown as { streams: Set<{ retryTimer: unknown; failure?: ServiceError }> }).streams;
    await until(() => [...polls].some((poll) => poll.retryTimer !== undefined));
    assert.equal([...polls][0]?.failure?.code, status.UNAUTHENTICATED);
    assert.equal(loggedFailures.length, 1);
    assert.deepEqual(server.taskHeaders, []);
  } finally {
    await worker.close();
    console.error = previousConsoleError;
    await server.close();
  }
});

test("Worker startup deadline settles while its credential is pending", async () => {
  const server = await startServer();
  let provideToken!: (token: string) => void;
  const tokenPromise = new Promise<string>((resolve) => { provideToken = resolve; });
  const worker = new Worker(new Registry([]), {} as BlobCache, {
    serverAddress: server.address,
    pull: {},
    attributeIndexSyncTimeoutMs: 80,
    credential: { token: () => tokenPromise },
  });
  try {
    await assert.rejects(within(worker.start()), /cannot start TypeScript pull Worker/);
    assert.deepEqual(server.flowHeaders, []);
  } finally {
    provideToken("too-late");
    await worker.close();
    await server.close();
  }
});

test("FileCredential reads at construction, rotates, and fails on an empty or missing file", async () => {
  const directory = mkdtempSync(join(tmpdir(), "dex-credential-"));
  const path = join(directory, "token");
  const server = await startServer();
  try {
    assert.throws(() => new FileCredential(path));
    writeFileSync(path, " \n ");
    assert.throws(() => new FileCredential(path), /empty/);
    writeFileSync(path, " first-token \n");
    const credential = new FileCredential(path);
    const client = new Client(new Registry([]), {} as BlobCache, { serverAddress: server.address, credential });
    try {
      await client.searchFlows("", 1);
      writeFileSync(path, "rotated-token-longer");
      await until(() => credential.token() === "rotated-token-longer");
      await client.searchFlows("", 1);
      writeFileSync(path, "");
      await until(() => {
        try { credential.token(); return false; } catch { return true; }
      });
      await assert.rejects(client.searchFlows("", 1), /empty/);
      writeFileSync(path, "restored-token");
      await until(() => {
        try { return credential.token() === "restored-token"; } catch { return false; }
      });
      await client.searchFlows("", 1);
      rmSync(path);
      await until(() => {
        try { credential.token(); return false; } catch { return true; }
      });
      await assert.rejects(client.searchFlows("", 1));
      assert.deepEqual(server.flowHeaders, ["Bearer first-token", "Bearer rotated-token-longer", "Bearer restored-token"]);
    } finally {
      await client.close();
    }
  } finally {
    rmSync(directory, { recursive: true, force: true });
    await server.close();
  }
});

test("Worker authenticates FlowService startup and pull streams after reconnect", async () => {
  const server = await startServer();
  let currentToken = "worker-token";
  const worker = new Worker(new Registry([]), {} as BlobCache, {
    serverAddress: server.address,
    pull: {},
    credential: { token: () => currentToken },
  });
  try {
    await worker.start();
    await until(() => server.taskStreams.length === 1);
    assert.deepEqual(server.flowHeaders, ["Bearer worker-token", "Bearer worker-token"]);
    assert.equal(server.taskHeaders[0], "Bearer worker-token");
    currentToken = "rotated-worker-token";
    server.taskStreams[0]!.write({ frame: { $case: "reconnect", value: { reason: "rotate" } } });
    await until(() => server.taskStreams.length === 2);
    assert.equal(server.taskHeaders[1], "Bearer rotated-worker-token");
  } finally {
    await worker.close();
    await server.close();
  }
});

test("Worker sends no authorization metadata without a credential", async () => {
  const server = await startServer();
  const worker = new Worker(new Registry([]), {} as BlobCache, {
    serverAddress: server.address,
    pull: {},
  });
  try {
    await worker.start();
    await until(() => server.taskStreams.length === 1);
    assert.deepEqual(server.flowHeaders, [undefined, undefined]);
    assert.deepEqual(server.taskHeaders, [undefined]);
  } finally {
    await worker.close();
    await server.close();
  }
});

test("ApiKeyCredential retries a rejected unary Client call once with one fresh exchange", async () => {
  const endpoint = await startTokenEndpoint((index) => tokenResponse(index === 1 ? firstAccessToken : "accepted-token-b", 60));
  const server = await startServer({ rejectFlow: (authorization) => authorization !== "Bearer accepted-token-b" });
  const credential = new ApiKeyCredential({ tokenUrl: endpoint.url, serverId: "dex-server", apiKey });
  const client = new Client(new Registry([]), {} as BlobCache, { serverAddress: server.address, credential });
  try {
    await client.searchFlows("", 1);
    assert.deepEqual(server.flowHeaders, [`Bearer ${firstAccessToken}`, "Bearer accepted-token-b"]);
    assert.equal(endpoint.requests.length, 2);
    await client.searchFlows("", 1);
    assert.equal(endpoint.requests.length, 2);
  } finally {
    await client.close();
    await server.close();
    await endpoint.close();
  }
});

test("ApiKeyCredential stops after a second unary UNAUTHENTICATED", async () => {
  const endpoint = await startTokenEndpoint((index) => tokenResponse(index === 1 ? firstAccessToken : "rejected-token-b", 60));
  const server = await startServer({ rejectFlow: () => true });
  const credential = new ApiKeyCredential({ tokenUrl: endpoint.url, serverId: "dex-server", apiKey });
  const client = new Client(new Registry([]), {} as BlobCache, { serverAddress: server.address, credential });
  try {
    await assert.rejects(client.searchFlows("", 1), (failure: unknown) => {
      assert.equal((failure as { code?: number }).code, status.UNAUTHENTICATED);
      return true;
    });
    assert.deepEqual(server.flowHeaders, [`Bearer ${firstAccessToken}`, "Bearer rejected-token-b"]);
    assert.equal(endpoint.requests.length, 2);
  } finally {
    await client.close();
    await server.close();
    await endpoint.close();
  }
});

test("a stale unary rejection leaves a newer cached token intact", async () => {
  let releaseOldRejection!: () => void;
  const oldRejectionGate = new Promise<void>((resolve) => { releaseOldRejection = resolve; });
  const endpoint = await startTokenEndpoint((index) => tokenResponse(index === 1 ? firstAccessToken : "accepted-token-b", 60));
  const server = await startServer({
    rejectFlow: (authorization) => authorization !== "Bearer accepted-token-b",
    delayFirstFlowRejection: oldRejectionGate,
  });
  const credential = new ApiKeyCredential({ tokenUrl: endpoint.url, serverId: "dex-server", apiKey });
  const client = new Client(new Registry([]), {} as BlobCache, { serverAddress: server.address, credential });
  try {
    await credential.token();
    const firstCall = client.searchFlows("", 1);
    await until(() => server.flowHeaders.length >= 1);
    const secondCall = client.searchFlows("", 1);
    await secondCall;
    releaseOldRejection();
    await firstCall;
    assert.equal(endpoint.requests.length, 2);
    await client.searchFlows("", 1);
    assert.equal(endpoint.requests.length, 2);
  } finally {
    releaseOldRejection();
    await client.close();
    await server.close();
    await endpoint.close();
  }
});

test("ApiKeyCredential discards a rejected Worker poll token and reconnects immediately", async () => {
  const endpoint = await startTokenEndpoint((index) => tokenResponse(index === 1 ? firstAccessToken : "accepted-token-b", 60));
  const server = await startServer({ rejectPoll: (authorization) => authorization !== "Bearer accepted-token-b" });
  const credential = new ApiKeyCredential({ tokenUrl: endpoint.url, serverId: "dex-server", apiKey });
  const previousConsoleError = console.error;
  console.error = () => {};
  const worker = new Worker(new Registry([]), {} as BlobCache, {
    serverAddress: server.address,
    credential,
    pull: { stopDrainTimeoutMs: 100 },
  });
  try {
    await worker.start();
    await until(() => server.taskHeaders.length === 2);
    assert.deepEqual(server.taskHeaders, [`Bearer ${firstAccessToken}`, "Bearer accepted-token-b"]);
    assert.equal(endpoint.requests.length, 2);
  } finally {
    await worker.close();
    console.error = previousConsoleError;
    await server.close();
    await endpoint.close();
  }
});

test("ApiKeyCredential backs off after a second Worker poll token rejection", async () => {
  const endpoint = await startTokenEndpoint((index) => tokenResponse(index === 1 ? firstAccessToken : "rejected-token-b", 60));
  const server = await startServer({ rejectPoll: () => true });
  const credential = new ApiKeyCredential({ tokenUrl: endpoint.url, serverId: "dex-server", apiKey });
  const previousConsoleError = console.error;
  console.error = () => {};
  const worker = new Worker(new Registry([]), {} as BlobCache, {
    serverAddress: server.address,
    credential,
    pull: { stopDrainTimeoutMs: 100 },
  });
  try {
    await worker.start();
    await until(() => server.taskHeaders.length === 2);
    const pullWorker = (worker as unknown as { pullWorker: { streams: Set<{ retryTimer?: ReturnType<typeof setTimeout> }> } }).pullWorker;
    await until(() => [...pullWorker.streams].some((poll) => poll.retryTimer !== undefined));
    assert.deepEqual(server.taskHeaders, [`Bearer ${firstAccessToken}`, "Bearer rejected-token-b"]);
    assert.equal(endpoint.requests.length, 2);
  } finally {
    await worker.close();
    console.error = previousConsoleError;
    await server.close();
    await endpoint.close();
  }
});

type TokenReply = { statusCode: number; body: string };
type TokenRequest = { method: string | undefined; path: string | undefined; body: string; contentType: string | undefined; accept: string | undefined };

function tokenResponse(accessToken: string, expiresIn: number): TokenReply {
  return { statusCode: 200, body: JSON.stringify({ access_token: accessToken, token_type: "Bearer", expires_in: expiresIn }) };
}

async function startTokenEndpoint(respond: (index: number) => TokenReply | Promise<TokenReply>) {
  const requests: TokenRequest[] = [];
  const httpServer = createHttpServer(async (request, response) => {
    let body = "";
    for await (const chunk of request) body += chunk.toString();
    const headers: IncomingHttpHeaders = request.headers;
    requests.push({
      method: request.method,
      path: request.url,
      body,
      contentType: headers["content-type"],
      accept: headers.accept,
    });
    const reply = await respond(requests.length);
    response.writeHead(reply.statusCode, { "content-type": "application/json" });
    response.end(reply.body);
  });
  await new Promise<void>((resolve) => httpServer.listen(0, "127.0.0.1", resolve));
  const address = httpServer.address() as AddressInfo;
  return {
    url: `http://127.0.0.1:${address.port}/token`,
    requests,
    close: () => new Promise<void>((resolve, reject) => {
      httpServer.closeAllConnections();
      httpServer.close((failure) => failure === undefined ? resolve() : reject(failure));
    }),
  };
}

function unauthenticatedError(): ServiceError {
  return Object.assign(new Error("rejected token"), {
    code: status.UNAUTHENTICATED,
    details: "rejected token",
    metadata: new Metadata(),
  });
}

async function startServer(options: {
  rejectFlow?: (authorization: string | undefined) => boolean;
  rejectPoll?: (authorization: string | undefined) => boolean;
  delayFirstFlowRejection?: Promise<void>;
} = {}) {
  const grpcServer = new Server();
  const flowHeaders: (string | undefined)[] = [];
  const taskHeaders: (string | undefined)[] = [];
  const taskStreams: TaskStream[] = [];
  grpcServer.addService(FlowServiceService, {
    searchFlows(call, callback) {
      const authorization = call.metadata.get("authorization")[0]?.toString();
      flowHeaders.push(authorization);
      if (options.rejectFlow?.(authorization)) {
        if (flowHeaders.length === 1 && options.delayFirstFlowRejection !== undefined) {
          void options.delayFirstFlowRejection.then(() => callback(unauthenticatedError(), null));
        } else callback(unauthenticatedError(), null);
      } else callback(null, { flowRuns: [], nextPageToken: "" });
    },
    getServerInfo(call, callback) {
      flowHeaders.push(call.metadata.get("authorization")[0]?.toString());
      callback(null, { serverVersion: "test", minimumSupportedProtocolVersion: 1, currentProtocolVersion: 1, serverEngine: 0 });
    },
    syncAttributeIndexes(call, callback) {
      flowHeaders.push(call.metadata.get("authorization")[0]?.toString());
      callback(null, {});
    },
  } as Partial<FlowServiceServer> as FlowServiceServer);
  grpcServer.addService(WorkerTaskServiceService, {
    pollWorkerTask(call: TaskStream) {
      const authorization = call.metadata.get("authorization")[0]?.toString();
      taskHeaders.push(authorization);
      taskStreams.push(call);
      call.on("data", () => {});
      call.on("end", () => call.end());
      call.on("error", () => {});
      if (options.rejectPoll?.(authorization)) {
        queueMicrotask(() => call.emit("error", unauthenticatedError()));
      }
    },
  } as WorkerTaskServiceServer);
  const port = await new Promise<number>((resolve, reject) => {
    grpcServer.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (failure, boundPort) => {
      if (failure !== null) reject(failure);
      else resolve(boundPort);
    });
  });
  return {
    address: `127.0.0.1:${port}`,
    flowHeaders,
    taskHeaders,
    taskStreams,
    close: () => new Promise<void>((resolve, reject) => {
      grpcServer.tryShutdown((failure) => failure === undefined ? resolve() : reject(failure));
    }),
  };
}

async function until(predicate: () => boolean | Promise<boolean>): Promise<void> {
  const deadline = Date.now() + 4_000;
  while (!(await predicate())) {
    if (Date.now() >= deadline) throw new Error("timed out waiting for credential update");
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

async function within<T>(result: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      result,
      new Promise<T>((_resolve, reject) => {
        timer = setTimeout(() => reject(new Error("credential call did not settle")), 1_000);
      }),
    ]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}
