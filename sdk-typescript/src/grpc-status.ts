// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import { BinaryReader, BinaryWriter } from "@bufbuild/protobuf/wire";
import { Metadata, status, type ServiceError as GrpcServiceError } from "@grpc/grpc-js";

import {
  ServiceErrorResponse,
  ErrorSubStatus as ProtoErrorSubStatus,
  WorkerErrorResponse,
} from "./gen/dex.js";
import {
  DexServiceError,
  ErrorSubStatus,
  FlowAlreadyStartedError,
  FlowNotActiveOrNotFoundError,
  FlowNotFoundError,
  LongPollTimeoutError,
  RequestTimeoutError,
  ChannelMessageNotFoundError,
  RpcLockConflictError,
  WorkerInvocationError,
  type ErrorSubStatus as ErrorSubStatusValue,
} from "./errors.js";
import { RetryAfterError } from "./retry-after.js";

const statusDetailsKey = "grpc-status-details-bin";
const maxWorkerErrorDetailBytes = 1024;
const maxWorkerErrorTypeBytes = 256;
const maxWorkerStackTraceBytes = 4 * 1024;
const errorDetailTruncationMarker = Buffer.from("\n... error detail truncated by Dex TypeScript SDK ...");
const errorTypeTruncationMarker = Buffer.from("\n... error type truncated by Dex TypeScript SDK ...");
const stackTraceTruncationMarker = Buffer.from("\n... stack trace truncated by Dex TypeScript SDK ...");

interface AnyDetail {
  readonly typeUrl: string;
  readonly value: Uint8Array;
}

export function workerServiceError(failure: unknown): GrpcServiceError {
  const response = workerErrorResponse(failure);
  const worker = WorkerErrorResponse.encode(response).finish();
  const metadata = new Metadata();
  metadata.set(
    statusDetailsKey,
    Buffer.from(
      encodeStatus(response.grpcStatus, response.detail, [
        { typeUrl: "type.googleapis.com/dex.WorkerErrorResponse", value: worker },
      ]),
    ),
  );
  return Object.assign(new Error(response.detail), {
    code: response.grpcStatus,
    details: response.detail,
    metadata,
  });
}

export function workerErrorResponse(failure: unknown): WorkerErrorResponse {
  const retryAfterError = failure instanceof RetryAfterError ? failure : undefined;
  const cause =
    retryAfterError?.cause instanceof Error
      ? retryAfterError.cause
      : failure instanceof Error
        ? failure
        : new Error(String(failure));
  const detail = truncateWorkerFailureField(
    cause.message || cause.name,
    maxWorkerErrorDetailBytes,
    errorDetailTruncationMarker,
  );
  const grpcStatus = grpcStatusForWorkerFailure(cause);
  return WorkerErrorResponse.create({
    detail,
    errorType: truncateWorkerFailureField(
      cause.name,
      maxWorkerErrorTypeBytes,
      errorTypeTruncationMarker,
    ),
    stackTrace: truncateWorkerFailureField(
      cause.stack ?? "",
      maxWorkerStackTraceBytes,
      stackTraceTruncationMarker,
    ),
    retryAfterSeconds: retryAfterError?.afterSeconds ?? 0,
    grpcStatus,
  });
}

function grpcStatusForWorkerFailure(failure: Error): status {
  const code = (failure as Error & { code?: unknown }).code;
  if (typeof code === "number" && code !== status.OK && Object.values(status).includes(code)) {
    return code as status;
  }
  if (failure.name === "AbortError") {
    return status.CANCELLED;
  }
  return status.UNKNOWN;
}

function truncateWorkerFailureField(
  value: string,
  maximumBytes: number,
  truncationMarker: Uint8Array,
): string {
  const encoded = Buffer.from(value);
  if (encoded.length <= maximumBytes) {
    return value;
  }
  let prefixLength = maximumBytes - truncationMarker.length;
  while (prefixLength > 0 && (encoded[prefixLength]! & 0xc0) === 0x80) {
    prefixLength -= 1;
  }
  return Buffer.concat([encoded.subarray(0, prefixLength), truncationMarker]).toString("utf8");
}

export type FlowTargetRequirement = "none" | "existing" | "active";

export function translateServiceError(
  error: GrpcServiceError,
  operation: string,
  flowId: string | undefined,
  requirement: FlowTargetRequirement,
): DexServiceError {
  let response: ServiceErrorResponse | undefined;
  try {
    response = serviceErrorResponse(error);
  } catch (failure) {
    return new DexServiceError(
      error.code,
      ErrorSubStatus.UNCATEGORIZED,
      `Dex returned malformed error details: ${failure instanceof Error ? failure.message : String(failure)}`,
      operation,
      flowId,
      { cause: error },
    );
  }
  const detail = response?.detail || error.details || error.message;
  const subStatus =
    response === undefined ? ErrorSubStatus.UNCATEGORIZED : mapSubStatus(response.subStatus);
  const parameters = [
    error.code,
    subStatus,
    detail,
    operation,
    flowId,
    { cause: error },
  ] as const;
  switch (subStatus) {
    case ErrorSubStatus.FLOW_ALREADY_STARTED:
      return new FlowAlreadyStartedError(...parameters);
    case ErrorSubStatus.FLOW_NOT_EXISTS:
      if (requirement === "existing") {
        return new FlowNotFoundError(...parameters);
      }
      if (requirement === "active") {
        return new FlowNotActiveOrNotFoundError(...parameters);
      }
      return new DexServiceError(...parameters);
    case ErrorSubStatus.WORKER_API_ERROR:
      if (error.code === status.ABORTED) {
        return new RpcLockConflictError(...parameters);
      }
      return new WorkerInvocationError(
        error.code,
        subStatus,
        detail,
        operation,
        flowId,
        workerCode(response),
        response?.originalWorkerErrorType ?? "",
        response?.originalWorkerErrorDetail ?? "",
        { cause: error },
      );
    case ErrorSubStatus.LONG_POLL_TIMEOUT:
      return new LongPollTimeoutError(...parameters);
    case ErrorSubStatus.CHANNEL_MESSAGE_NOT_FOUND:
      return new ChannelMessageNotFoundError(...parameters);
    case ErrorSubStatus.REQUEST_TIMEOUT:
      return new RequestTimeoutError(...parameters);
    default:
      return new DexServiceError(...parameters);
  }
}

function serviceErrorResponse(error: GrpcServiceError): ServiceErrorResponse | undefined {
  const encoded = error.metadata.get(statusDetailsKey)[0];
  if (!(encoded instanceof Buffer)) {
    return undefined;
  }
  const decoded = decodeStatus(encoded);
  const detail = decoded.details.find((candidate) =>
    candidate.typeUrl.endsWith("/dex.ServiceErrorResponse"),
  );
  return detail === undefined ? undefined : ServiceErrorResponse.decode(detail.value);
}

function workerCode(response: ServiceErrorResponse | undefined): status | undefined {
  if (response === undefined || response.originalWorkerErrorStatus === 0) {
    return undefined;
  }
  const code = response.originalWorkerErrorStatus as status;
  return Object.values(status).includes(code) ? code : status.UNKNOWN;
}

function mapSubStatus(value: ProtoErrorSubStatus): ErrorSubStatusValue {
  switch (value) {
    case ProtoErrorSubStatus.ERROR_SUB_STATUS_UNCATEGORIZED:
      return ErrorSubStatus.UNCATEGORIZED;
    case ProtoErrorSubStatus.ERROR_SUB_STATUS_FLOW_ALREADY_STARTED:
      return ErrorSubStatus.FLOW_ALREADY_STARTED;
    case ProtoErrorSubStatus.ERROR_SUB_STATUS_FLOW_NOT_EXISTS:
      return ErrorSubStatus.FLOW_NOT_EXISTS;
    case ProtoErrorSubStatus.ERROR_SUB_STATUS_WORKER_API_ERROR:
      return ErrorSubStatus.WORKER_API_ERROR;
    case ProtoErrorSubStatus.ERROR_SUB_STATUS_LONG_POLL_TIME_OUT:
      return ErrorSubStatus.LONG_POLL_TIMEOUT;
    case ProtoErrorSubStatus.ERROR_SUB_STATUS_CHANNEL_MESSAGE_NOT_FOUND:
      return ErrorSubStatus.CHANNEL_MESSAGE_NOT_FOUND;
    case ProtoErrorSubStatus.ERROR_SUB_STATUS_REQUEST_TIMEOUT:
      return ErrorSubStatus.REQUEST_TIMEOUT;
    default:
      return ErrorSubStatus.UNCATEGORIZED;
  }
}

function encodeStatus(code: number, message: string, details: readonly AnyDetail[]): Uint8Array {
  const writer = new BinaryWriter();
  if (code !== 0) {
    writer.uint32(8).int32(code);
  }
  if (message !== "") {
    writer.uint32(18).string(message);
  }
  for (const detail of details) {
    writer.uint32(26).fork();
    writer.uint32(10).string(detail.typeUrl);
    writer.uint32(18).bytes(detail.value);
    writer.join();
  }
  return writer.finish();
}

function decodeStatus(input: Uint8Array): { details: AnyDetail[] } {
  const reader = new BinaryReader(input);
  const details: AnyDetail[] = [];
  while (reader.pos < reader.len) {
    const tag = reader.uint32();
    if (tag >>> 3 === 3) {
      details.push(decodeAny(reader, reader.uint32()));
    } else {
      reader.skip(tag & 7);
    }
  }
  return { details };
}

function decodeAny(reader: BinaryReader, length: number): AnyDetail {
  const end = reader.pos + length;
  let typeUrl = "";
  let value: Uint8Array<ArrayBufferLike> = new Uint8Array();
  while (reader.pos < end) {
    const tag = reader.uint32();
    if (tag >>> 3 === 1) {
      typeUrl = reader.string();
    } else if (tag >>> 3 === 2) {
      value = reader.bytes();
    } else {
      reader.skip(tag & 7);
    }
  }
  return { typeUrl, value };
}
