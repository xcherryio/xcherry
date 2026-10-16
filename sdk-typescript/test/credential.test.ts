// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  Server,
  ServerCredentials,
  status,
  type ServerDuplexStream,
} from "@grpc/grpc-js";

import { Client, FileCredential, Registry, Worker, type BlobCache, type Credential } from "../src/index.js";
import {
  FlowServiceService,
  WorkerTaskServiceService,
  type FlowServiceServer,
  type PollWorkerTaskRequest,
  type PollWorkerTaskResponse,
  type WorkerTaskServiceServer,
} from "../src/gen/dex.js";

type TaskStream = ServerDuplexStream<PollWorkerTaskRequest, PollWorkerTaskResponse>;

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

async function startServer() {
  const grpcServer = new Server();
  const flowHeaders: (string | undefined)[] = [];
  const taskHeaders: (string | undefined)[] = [];
  const taskStreams: TaskStream[] = [];
  grpcServer.addService(FlowServiceService, {
    searchFlows(call, callback) {
      flowHeaders.push(call.metadata.get("authorization")[0]?.toString());
      callback(null, { flowRuns: [], nextPageToken: "" });
    },
    getServerInfo(call, callback) {
      flowHeaders.push(call.metadata.get("authorization")[0]?.toString());
      callback(null, { serverVersion: "test", minimumSupportedProtocolVersion: 1, currentProtocolVersion: 1 });
    },
    syncAttributeIndexes(call, callback) {
      flowHeaders.push(call.metadata.get("authorization")[0]?.toString());
      callback(null, {});
    },
  } as Partial<FlowServiceServer> as FlowServiceServer);
  grpcServer.addService(WorkerTaskServiceService, {
    pollWorkerTask(call: TaskStream) {
      taskHeaders.push(call.metadata.get("authorization")[0]?.toString());
      taskStreams.push(call);
      call.on("data", () => {});
      call.on("end", () => call.end());
      call.on("error", () => {});
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

async function until(predicate: () => boolean): Promise<void> {
  const deadline = Date.now() + 4_000;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("timed out waiting for credential update");
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}
