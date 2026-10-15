// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import type { WorkerTarget as ProtoWorkerTarget } from "./gen/dex.js";
import type { WorkerTarget } from "./options.js";

export function mapWorkerTarget(target: WorkerTarget): ProtoWorkerTarget {
  if (target.pull === true) {
    if (target.address !== "" || target.headless === true) {
      throw new TypeError("pull WorkerTarget cannot include address or headless");
    }
    return { address: "", isHeadlessAddress: false, isPullMode: true };
  }
  if (target.address.trim() === "") {
    throw new TypeError("WorkerTarget address is required unless pull is true");
  }
  return {
    address: target.address,
    isHeadlessAddress: target.headless ?? false,
    isPullMode: false,
  };
}
