// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import type { WorkerTarget as ProtoWorkerTarget } from "./gen/dex.js";
import type { PullWorkerTarget, WorkerTarget } from "./options.js";

export function mapWorkerTarget(target: WorkerTarget | PullWorkerTarget): ProtoWorkerTarget {
  if ((target as PullWorkerTarget).pull === true) {
    const provided = target as { readonly address?: string; readonly headless?: boolean };
    if (provided.address !== undefined && provided.address !== "" || provided.headless === true) {
      throw new TypeError("pull WorkerTarget cannot include address or headless");
    }
    return { address: "", isHeadlessAddress: false, isPullMode: true };
  }
  const listener = target as WorkerTarget;
  return {
    address: listener.address,
    isHeadlessAddress: listener.headless ?? false,
    isPullMode: false,
  };
}
