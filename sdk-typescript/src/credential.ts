// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import { readFileSync, statSync } from "node:fs";
import { inspect } from "node:util";

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

/** Configures an API key exchange for a Dex server. */
export interface ApiKeyCredentialOptions {
  /** HTTPS token endpoint URL. HTTP is permitted only for localhost, 127.0.0.0/8, or ::1. */
  readonly tokenUrl: string;
  /** Server ID that the token endpoint uses to scope the access token. */
  readonly serverId: string;
  /** Long-lived API key sent only to the token endpoint. */
  readonly apiKey: string;
  /** Maximum exchange duration, including connection and response, in milliseconds. Defaults to 10,000. */
  readonly exchangeTimeoutMs?: number;
  /** Fetch implementation for proxies and tests. Defaults to global fetch. The credential does not own it. */
  readonly fetch?: typeof globalThis.fetch;
}

const rejectedToken = Symbol("dex.rejectedToken");

interface ApiKeyCredentialState {
  tokenUrl: string;
  serverId: string;
  apiKey: string;
  exchangeTimeoutMs: number;
  fetch: typeof globalThis.fetch;
  cachedToken: string | undefined;
  expiresAtMs: number;
  refreshAtMs: number;
  nextRefreshAtMs: number;
  pendingExchange: Promise<string> | undefined;
}

const apiKeyCredentialStates = new WeakMap<ApiKeyCredential, ApiKeyCredentialState>();

/**
 * Exchanges an API key for short-lived Dex bearer tokens.
 *
 * Use this instead of FileCredential when the application holds an API key rather than a
 * rotating token file. The first call exchanges the key. Calls after 80% of the token's
 * lifetime start one shared background refresh and continue using the valid token.
 * Failed refreshes are retried no more often than every five seconds while that token
 * remains valid. Expired-token callers await the same exchange, bounded by the exchange
 * timeout. A rejected token is discarded and a unary Dex call is retried once.
 *
 * The token URL must use HTTPS, except loopback HTTP is accepted for local endpoints.
 * Neither the API key nor access token appears in formatted credentials or errors.
 * Concurrent calls are safe. The credential owns no persistent HTTP client or timer;
 * an in-flight exchange ends within the configured timeout. An injected fetch is owned
 * by the application.
 *
 * @example
 * ```ts
 * const credential = new ApiKeyCredential({
 *   tokenUrl: "https://auth.example.com/token",
 *   serverId: "dex-server-id",
 *   apiKey: process.env.DEX_API_KEY!,
 * });
 * const client = new Client(registry, cache, { credential });
 * const worker = new Worker(registry, cache, { credential, pull: {} });
 * try { await client.searchFlows("", 10); }
 * finally { await client.close(); await worker.close(); }
 * ```
 */
export class ApiKeyCredential implements Credential {


  /**
   * Validates the exchange settings without contacting the token endpoint.
   * @param options - Token URL, server ID, API key, optional timeout and fetch implementation.
   * @throws TypeError when a required string, token URL, timeout, or fetch is invalid.
   */
  public constructor(options: ApiKeyCredentialOptions) {
    if (options === null || typeof options !== "object") throw new TypeError("dex: API key credential options are required");
    for (const [name, value] of Object.entries({ tokenUrl: options.tokenUrl, serverId: options.serverId, apiKey: options.apiKey })) {
      if (typeof value !== "string" || value.trim() === "") throw new TypeError(`dex: ${name} must be a non-empty string`);
    }
    let parsedUrl: URL;
    try { parsedUrl = new URL(options.tokenUrl); }
    catch { throw new TypeError("dex: tokenUrl must be an absolute HTTP or HTTPS URL"); }
    const hostname = parsedUrl.hostname.toLowerCase();
    const isLoopback = hostname === "localhost" || hostname === "[::1]"
      || /^127\.(?:\d{1,3}\.){2}\d{1,3}$/.test(hostname);
    if (parsedUrl.protocol !== "https:" && !(parsedUrl.protocol === "http:" && isLoopback)) {
      throw new TypeError("dex: tokenUrl must use HTTPS except for loopback HTTP");
    }
    if (parsedUrl.username !== "" || parsedUrl.password !== "") {
      throw new TypeError("dex: tokenUrl must not contain user information");
    }
    const exchangeTimeoutMs = options.exchangeTimeoutMs ?? 10_000;
    if (!Number.isFinite(exchangeTimeoutMs) || exchangeTimeoutMs <= 0) {
      throw new TypeError("dex: exchangeTimeoutMs must be a positive finite number");
    }
    if (options.fetch !== undefined && typeof options.fetch !== "function") {
      throw new TypeError("dex: fetch must be a function");
    }
    apiKeyCredentialStates.set(this, {
      tokenUrl: options.tokenUrl,
      serverId: options.serverId,
      apiKey: options.apiKey,
      exchangeTimeoutMs,
      fetch: options.fetch ?? globalThis.fetch,
      cachedToken: undefined,
      expiresAtMs: 0,
      refreshAtMs: 0,
      nextRefreshAtMs: 0,
      pendingExchange: undefined,
    });
  }

  /**
   * Returns a valid access token, exchanging the API key when needed.
   * Concurrent callers share one exchange. A valid token is returned immediately during
   * refresh. When no valid token exists, this waits up to the exchange timeout.
   * @returns The current bearer access token.
   * @throws Error when the token endpoint rejects the request, returns invalid data, or cannot be reached in time.
   */
  public token(): Promise<string> {
    const now = performance.now();
    if (this.apiKeyState.cachedToken !== undefined && now < this.apiKeyState.expiresAtMs) {
      if (now >= this.apiKeyState.refreshAtMs && now >= this.apiKeyState.nextRefreshAtMs && this.apiKeyState.pendingExchange === undefined) {
        void this.startExchange().catch(() => {});
      }
      return Promise.resolve(this.apiKeyState.cachedToken);
    }
    return this.apiKeyState.pendingExchange ?? this.startExchange();
  }

  /**
   * Hides API key and access token values from Node inspection.
   * @returns A redacted credential description.
   */
  public [inspect.custom](): string {
    return "ApiKeyCredential {}";
  }

  /**
   * Hides API key and access token values from JSON serialization.
   * @returns An empty object without credential secrets.
   */
  public toJSON(): object {
    return {};
  }

  private [rejectedToken](token: string): void {
    if (this.apiKeyState.cachedToken === token) {
      this.apiKeyState.cachedToken = undefined;
      this.apiKeyState.nextRefreshAtMs = 0;
    }
  }

  private startExchange(): Promise<string> {
    const exchange = this.exchange().then(({ token, startedAtMs, lifetimeMs }) => {
      if (performance.now() >= startedAtMs + lifetimeMs) {
        throw this.exchangeError(200, "access token expired before exchange completed");
      }
      this.apiKeyState.cachedToken = token;
      this.apiKeyState.expiresAtMs = startedAtMs + lifetimeMs;
      this.apiKeyState.refreshAtMs = startedAtMs + lifetimeMs * 0.8;
      this.apiKeyState.nextRefreshAtMs = 0;
      return token;
    }, (failure: unknown) => {
      if (this.apiKeyState.cachedToken !== undefined && performance.now() < this.apiKeyState.expiresAtMs) {
        this.apiKeyState.nextRefreshAtMs = performance.now() + 5_000;
      }
      throw failure;
    });
    this.apiKeyState.pendingExchange = exchange;
    void exchange.finally(() => {
      if (this.apiKeyState.pendingExchange === exchange) this.apiKeyState.pendingExchange = undefined;
    }).catch(() => {});
    return exchange;
  }

  private async exchange(): Promise<{ token: string; startedAtMs: number; lifetimeMs: number }> {
    const startedAtMs = performance.now();
    const deadline = new ApiKeyExchangeDeadline(
      this.apiKeyState.exchangeTimeoutMs,
      (statusCode) => this.exchangeError(statusCode, "exchange timed out"),
    );
    try {
      const response = await Promise.race([this.apiKeyState.fetch(this.apiKeyState.tokenUrl, {
        method: "POST",
        headers: { "content-type": "application/json", accept: "application/json" },
        body: JSON.stringify({ server_id: this.apiKeyState.serverId, api_key: this.apiKeyState.apiKey }),
        signal: deadline.signal,
      }), deadline.timeout]);
      deadline.statusCode = response.status;
      const body = await Promise.race([response.text(), deadline.timeout]);
      let payload: unknown;
      try { payload = JSON.parse(body); }
      catch { throw this.exchangeError(deadline.statusCode, "invalid JSON response"); }
      if (response.status !== 200) {
        throw this.exchangeError(deadline.statusCode, this.responseErrorDetails(payload));
      }
      if (payload === null || typeof payload !== "object" || Array.isArray(payload)) {
        throw this.exchangeError(deadline.statusCode, "invalid response object");
      }
      const fields = payload as Record<string, unknown>;
      if (typeof fields.access_token !== "string" || fields.access_token === "") {
        throw this.exchangeError(deadline.statusCode, "invalid access_token");
      }
      if (typeof fields.token_type !== "string" || fields.token_type.toLowerCase() !== "bearer") {
        throw this.exchangeError(deadline.statusCode, "invalid token_type");
      }
      if (typeof fields.expires_in !== "number" || !Number.isSafeInteger(fields.expires_in) || fields.expires_in <= 0) {
        throw this.exchangeError(deadline.statusCode, "invalid expires_in");
      }
      return { token: fields.access_token, startedAtMs, lifetimeMs: fields.expires_in * 1_000 };
    } catch (failure) {
      if (failure instanceof ApiKeyExchangeError) throw failure;
      throw this.exchangeError(deadline.statusCode, deadline.didTimeout ? "exchange timed out" : "transport failure");
    } finally {
      deadline.close();
    }
  }

  private responseErrorDetails(payload: unknown): string {
    if (payload === null || typeof payload !== "object" || Array.isArray(payload)) return "exchange rejected";
    const fields = payload as Record<string, unknown>;
    const responseToken = typeof fields.access_token === "string" ? fields.access_token : undefined;
    const details = [fields.error, fields.error_description]
      .filter((value): value is string => typeof value === "string")
      .map((value) => this.redact(value, responseToken).slice(0, 200));
    return details.length === 0 ? "exchange rejected" : details.join(": ");
  }

  private redact(value: string, responseToken?: string): string {
    let redacted = value.replaceAll(this.apiKeyState.apiKey, "[REDACTED]");
    if (this.apiKeyState.cachedToken !== undefined) redacted = redacted.replaceAll(this.apiKeyState.cachedToken, "[REDACTED]");
    if (responseToken !== undefined && responseToken !== "") redacted = redacted.replaceAll(responseToken, "[REDACTED]");
    return redacted;
  }

  private exchangeError(statusCode: number | undefined, detail: string): ApiKeyExchangeError {
    return new ApiKeyExchangeError(`dex: token exchange ${this.redact(this.apiKeyState.tokenUrl)} HTTP status ${statusCode ?? "unavailable"}: ${detail}`);
  }

  private get apiKeyState(): ApiKeyCredentialState {
    const state = apiKeyCredentialStates.get(this);
    if (state === undefined) throw new Error("dex: API key credential was not initialized");
    return state;
  }
}

class ApiKeyExchangeError extends Error {
  public constructor(message: string) {
    super(message);
    this.name = "ApiKeyExchangeError";
  }
}

class ApiKeyExchangeDeadline {
  public readonly signal: AbortSignal;
  public readonly timeout: Promise<never>;
  public statusCode: number | undefined;
  public didTimeout = false;
  private readonly controller = new AbortController();
  private readonly timer: ReturnType<typeof setTimeout>;
  private rejectTimeout!: (failure: Error) => void;

  public constructor(timeoutMs: number, private readonly timeoutError: (statusCode: number | undefined) => Error) {
    this.signal = this.controller.signal;
    this.timeout = new Promise<never>((_resolve, reject) => { this.rejectTimeout = reject; });
    this.timer = setTimeout(this.onTimeout.bind(this), timeoutMs);
  }

  public close(): void {
    clearTimeout(this.timer);
  }

  private onTimeout(): void {
    this.didTimeout = true;
    this.controller.abort();
    this.rejectTimeout(this.timeoutError(this.statusCode));
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
    const requester = new CredentialRequester(this.credential, options, nextCall, downstream);
    return new InterceptingCall(downstream, {
      start: requester.start.bind(requester),
      sendMessage: requester.sendMessage.bind(requester),
      halfClose: requester.halfClose.bind(requester),
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
  private tokenUsed: string | undefined;
  private requestMessage: unknown;
  private hasHalfClosed = false;
  private hasRetried = false;
  private isCancelled = false;
  private retryCall: ReturnType<NextCall> | undefined;

  public constructor(
    private readonly credential: Credential,
    private readonly options: InterceptorOptions,
    private readonly nextCall: NextCall,
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
    this.isCancelled = true;
    if (this.retryCall !== undefined) {
      this.retryCall.cancelWithStatus(status.CANCELLED, "Cancelled on client");
      return;
    }
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

  public sendMessage(message: unknown, next: (message: unknown) => void): void {
    if (this.isUnary()) this.requestMessage = message;
    next(message);
  }

  public halfClose(next: () => void): void {
    this.hasHalfClosed = true;
    next();
  }

  private onToken(token: string): void {
    if (this.isSettled) return;
    if (this.deadlineAtMs !== undefined && Date.now() >= this.deadlineAtMs) {
      this.fail(status.DEADLINE_EXCEEDED, "Deadline exceeded");
      return;
    }
    try {
      this.metadata!.set("authorization", `Bearer ${token}`);
    } catch {
      this.fail(status.UNAUTHENTICATED, "invalid bearer token");
      return;
    }
    this.isSettled = true;
    this.tokenUsed = token;
    this.clearDeadline();
    try {
      this.next!(this.metadata!, this.responseListener());
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

  private responseListener(): InterceptingListener {
    return {
      onReceiveMetadata: (metadata) => this.listener?.onReceiveMetadata(metadata),
      onReceiveMessage: (message) => this.listener?.onReceiveMessage(message),
      onReceiveStatus: (receivedStatus) => this.onResponseStatus(receivedStatus),
    };
  }

  private onResponseStatus(receivedStatus: Parameters<InterceptingListener["onReceiveStatus"]>[0]): void {
    if (receivedStatus.code !== status.UNAUTHENTICATED || this.tokenUsed === undefined
      || !(rejectedToken in this.credential)) {
      this.listener?.onReceiveStatus(receivedStatus);
      return;
    }
    const rejectedCredential = this.credential as unknown as { [rejectedToken]: (token: string) => void };
    rejectedCredential[rejectedToken](this.tokenUsed);
    if (!this.isUnary() || this.hasRetried || !this.hasHalfClosed || this.isCancelled) {
      this.listener?.onReceiveStatus(receivedStatus);
      return;
    }
    this.hasRetried = true;
    void Promise.resolve().then(this.readToken.bind(this)).then(
      (token) => this.retryWithToken(token),
      (failure: unknown) => {
        this.listener?.onReceiveStatus({
          code: status.UNAUTHENTICATED,
          details: failure instanceof Error ? failure.message : String(failure),
          metadata: new Metadata(),
        });
      },
    );
  }

  private retryWithToken(token: string): void {
    if (this.isCancelled) return;
    try {
      this.tokenUsed = token;
      const retryMetadata = this.metadata!.clone();
      retryMetadata.set("authorization", `Bearer ${token}`);
      this.retryCall = this.nextCall(this.options);
      this.retryCall.start(retryMetadata, this.responseListener());
      this.retryCall.sendMessage(this.requestMessage);
      this.retryCall.halfClose();
    } catch {
      this.listener?.onReceiveStatus({
        code: status.UNAUTHENTICATED,
        details: "could not start credential retry",
        metadata: new Metadata(),
      });
    }
  }

  private isUnary(): boolean {
    return !this.options.method_definition.requestStream && !this.options.method_definition.responseStream;
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
