// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import { readFileSync, statSync } from "node:fs";

import { InterceptingCall, Metadata, status, type Interceptor } from "@grpc/grpc-js";

/**
 * Supplies the current bearer token for Client and Worker calls.
 *
 * Dex asks for a token before each FlowService call and each pull stream opens.
 * A call may overlap another call, so implementations must support concurrent requests.
 * Throwing or rejecting fails the call locally without contacting Dex. The token travels
 * over the same plaintext connection as other calls; protect the network path.
 *
 * @example
 * ```ts
 * let currentToken = "initial-token";
 * const credential: Credential = { token: () => currentToken };
 * const client = new Client(registry, cache, { credential });
 * currentToken = "rotated-token";
 * ```
 */
export interface Credential {
  /**
   * Returns the bearer token for one call or stream opening.
   * It may run concurrently and may return a Promise. Throw or reject when no token is available.
   * @returns The current bearer token.
   * @throws When the token cannot be supplied; the call fails before contacting Dex.
   */
  token(): string | Promise<string>;
}

/**
 * Reads a bearer token from a rotating file.
 *
 * Construction reads the file immediately. Calls check its modification time and size at
 * most once per second and reread when either changes. Surrounding whitespace is ignored.
 * An empty or unreadable file fails calls until a later check succeeds; no stale token is sent.
 *
 * @example
 * ```ts
 * const credential = new FileCredential("/var/run/secrets/dex/token");
 * const client = new Client(registry, cache, { credential });
 * const worker = new Worker(registry, cache, { credential, pull: {} });
 * ```
 */
export class FileCredential implements Credential {
  private currentToken = "";
  private modifiedAtMs = 0;
  private size = 0;
  private checkedAtMs = 0;
  private loadError: Error | undefined;

  /**
   * Reads the initial token from path.
   * @param path - Path to the bearer token file.
   * @throws When the file cannot be read or contains only whitespace.
   */
  public constructor(private readonly path: string) {
    this.refresh();
    if (this.loadError !== undefined) throw this.loadError;
  }

  /**
   * Returns the cached token after any due file check.
   * A changed file is reloaded on the next check, at most one second after the previous check.
   * @returns The current token without surrounding whitespace.
   * @throws When the file has become unreadable or empty.
   */
  public token(): string {
    if (performance.now() - this.checkedAtMs >= 1_000) this.refresh();
    if (this.loadError !== undefined) throw this.loadError;
    return this.currentToken;
  }

  private refresh(): void {
    this.checkedAtMs = performance.now();
    try {
      const fileInfo = statSync(this.path);
      if (this.loadError === undefined && this.currentToken !== ""
        && fileInfo.mtimeMs === this.modifiedAtMs && fileInfo.size === this.size) return;
      const token = readFileSync(this.path, "utf8").trim();
      if (token === "") throw new Error(`dex: credential file ${this.path} is empty`);
      this.currentToken = token;
      this.modifiedAtMs = fileInfo.mtimeMs;
      this.size = fileInfo.size;
      this.loadError = undefined;
    } catch (failure) {
      this.currentToken = "";
      this.loadError = failure instanceof Error ? failure : new Error(String(failure));
    }
  }
}

export function credentialInterceptors(credential: Credential | undefined): Interceptor[] {
  if (credential === undefined) return [];
  return [(options, nextCall) => new InterceptingCall(nextCall(options), {
    start(metadata, listener, next) {
      Promise.resolve().then(() => credential.token()).then((token) => {
        metadata.set("authorization", `Bearer ${token}`);
        next(metadata, listener);
      }).catch((failure: unknown) => {
        listener.onReceiveStatus({
          code: status.UNAUTHENTICATED,
          details: failure instanceof Error ? failure.message : String(failure),
          metadata: new Metadata(),
        });
      });
    },
  })];
}
