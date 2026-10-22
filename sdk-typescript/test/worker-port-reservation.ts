// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer, type Server } from "node:net";

// Keep the port occupied until immediately before Worker startup.
export class WorkerPortReservation {
  private constructor(
    private readonly server: Server,
    public readonly port: number,
  ) {}

  public static async reserve(): Promise<WorkerPortReservation> {
    const server = createServer();
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const address = server.address();
    assert.ok(address !== null && typeof address !== "string");
    return new WorkerPortReservation(server, address.port);
  }

  public release(): Promise<void> {
    if (!this.server.listening) {
      return Promise.resolve();
    }
    return new Promise((resolve, reject) => {
      this.server.close((error) => error === undefined ? resolve() : reject(error));
    });
  }
}
