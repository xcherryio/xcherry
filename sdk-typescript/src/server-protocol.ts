// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import type { ServerInfo } from "./gen/dex.js";

export const minimumSupportedServerProtocolVersion: number = 1;
export const maximumSupportedServerProtocolVersion: number = 3;

export function negotiateServerProtocol(
  serverInfo: ServerInfo,
  sdkVersion: string,
): number {
  const serverMinimum = serverInfo.minimumSupportedProtocolVersion;
  const serverCurrent = serverInfo.currentProtocolVersion;
  if (
    minimumSupportedServerProtocolVersion === 0
    || maximumSupportedServerProtocolVersion === 0
    || minimumSupportedServerProtocolVersion > maximumSupportedServerProtocolVersion
  ) {
    throw compatibilityError(
      serverInfo,
      sdkVersion,
      "TypeScript SDK protocol interval is invalid",
    );
  }
  if (serverMinimum === 0 || serverCurrent === 0 || serverMinimum > serverCurrent) {
    throw compatibilityError(serverInfo, sdkVersion, "Server protocol interval is invalid");
  }
  const negotiated = Math.min(serverCurrent, maximumSupportedServerProtocolVersion);
  if (negotiated < serverMinimum || negotiated < minimumSupportedServerProtocolVersion) {
    throw compatibilityError(serverInfo, sdkVersion, "protocol intervals do not overlap");
  }
  return negotiated;
}

export function serverInfoRequestError(sdkVersion: string, cause: unknown): Error {
  return new Error(
    `TypeScript SDK version "${sdkVersion}" protocol `
      + `[${minimumSupportedServerProtocolVersion},${maximumSupportedServerProtocolVersion}] `
      + "is incompatible with "
      + "Server version \"unknown\" protocol [unknown,unknown]: GetServerInfo failed",
    { cause },
  );
}

function compatibilityError(serverInfo: ServerInfo, sdkVersion: string, reason: string): Error {
  return new Error(
    `TypeScript SDK version "${sdkVersion}" protocol `
      + `[${minimumSupportedServerProtocolVersion},${maximumSupportedServerProtocolVersion}] `
      + `is incompatible with Server version "${serverInfo.serverVersion || "unknown"}" `
      + `protocol [${serverInfo.minimumSupportedProtocolVersion},`
      + `${serverInfo.currentProtocolVersion}]: ${reason}`,
  );
}
