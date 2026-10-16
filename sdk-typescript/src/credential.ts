// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import { readFileSync, statSync } from "node:fs";

import {
  InterceptingCall,
  Metadata,
  status,
  type InterceptingListener,
  type Interceptor,
  type InterceptorOptions,
  type Listener,
  type NextCall,
} from "@grpc/grpc-js";

/**
 * Supplies the current bearer token for Client and Worker calls.
 *
 * Dex asks for a token before each FlowService call and each pull stream opens.
 * A call may overlap another call, so implementations must support concurrent requests.
 * Throwing or rejecting fails the call locally without contacting Dex. The token travels
 * over the same plaintext connection as other calls; protect the network path.
 * A call deadline or cancellation ends the call even if token() remains pending.
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
  const interceptor = new CredentialInterceptor(credential);
  return [interceptor.intercept.bind(interceptor)];
}

class CredentialInterceptor {
  public constructor(private readonly credential: Credential) {}

  public intercept(options: InterceptorOptions, nextCall: NextCall): InterceptingCall {
    const downstream = nextCall(options);
    const requester = new CredentialRequester(this.credential, options, downstream);
    return new InterceptingCall(downstream, {
      start: requester.start.bind(requester),
      cancel: requester.cancel.bind(requester),
    });
  }
}

class CredentialRequester {
  private isSettled = false;
  private listener: InterceptingListener | undefined;
  private metadata: Metadata | undefined;
  private next: ((metadata: Metadata, listener: InterceptingListener | Listener) => void) | undefined;
  private deadlineAtMs: number | undefined;
  private deadlineTimer: ReturnType<typeof setTimeout> | undefined;

  public constructor(
    private readonly credential: Credential,
    private readonly options: InterceptorOptions,
    private readonly downstream: ReturnType<NextCall>,
  ) {}

  public start(
    metadata: Metadata,
    listener: InterceptingListener,
    next: (metadata: Metadata, listener: InterceptingListener | Listener) => void,
  ): void {
    this.metadata = metadata;
    this.listener = listener;
    this.next = next;
    this.startDeadline();
    if (this.isSettled) return;
    void Promise.resolve().then(this.readToken.bind(this)).then(
      this.onToken.bind(this),
      this.onCredentialFailure.bind(this),
    );
  }

  public cancel(next: () => void): void {
    if (this.isSettled) {
      next();
      return;
    }
    this.isSettled = true;
    this.clearDeadline();
    try {
      next();
    } finally {
      this.listener?.onReceiveStatus({
        code: status.CANCELLED,
        details: "Cancelled on client",
        metadata: new Metadata(),
      });
    }
  }

  private readToken(): string | Promise<string> {
    return this.credential.token();
  }

  private onToken(token: string): void {
    if (this.isSettled) return;
    if (this.deadlineAtMs !== undefined && Date.now() >= this.deadlineAtMs) {
      this.fail(status.DEADLINE_EXCEEDED, "Deadline exceeded");
      return;
    }
    try {
      this.metadata!.set("authorization", `Bearer ${token}`);
    } catch (failure) {
      this.fail(status.UNAUTHENTICATED, failure instanceof Error ? failure.message : String(failure), failure);
      return;
    }
    this.isSettled = true;
    this.clearDeadline();
    try {
      this.next!(this.metadata!, this.listener!);
    } catch (failure) {
      const details = failure instanceof Error ? failure.message : String(failure);
      this.downstream.cancelWithStatus(status.INTERNAL, details);
      this.listener?.onReceiveStatus(Object.assign({
        code: status.INTERNAL,
        details,
        metadata: new Metadata(),
      }, { cause: failure }));
    }
  }

  private onCredentialFailure(failure: unknown): void {
    this.fail(status.UNAUTHENTICATED, failure instanceof Error ? failure.message : String(failure), failure);
  }

  private startDeadline(): void {
    const deadline = this.options.deadline;
    if (deadline === undefined) return;
    const deadlineAtMs = deadline instanceof Date ? deadline.getTime() : deadline;
    if (!Number.isFinite(deadlineAtMs)) return;
    this.deadlineAtMs = deadlineAtMs;
    this.checkDeadline();
  }

  private checkDeadline(): void {
    if (this.isSettled || this.deadlineAtMs === undefined) return;
    const remainingMs = this.deadlineAtMs - Date.now();
    if (remainingMs <= 0) {
      this.fail(status.DEADLINE_EXCEEDED, "Deadline exceeded");
      return;
    }
    this.deadlineTimer = setTimeout(this.checkDeadline.bind(this), Math.min(remainingMs, 2_147_483_647));
  }

  private fail(code: status, details: string, cause?: unknown): void {
    if (this.isSettled) return;
    this.isSettled = true;
    this.clearDeadline();
    this.downstream.cancelWithStatus(code, details);
    this.listener?.onReceiveStatus(Object.assign({
      code,
      details,
      metadata: new Metadata(),
    }, cause === undefined ? {} : { cause }));
  }

  private clearDeadline(): void {
    if (this.deadlineTimer !== undefined) clearTimeout(this.deadlineTimer);
    this.deadlineTimer = undefined;
  }
}
