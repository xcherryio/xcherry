// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package service

const (
	// MinimumSupportedProtocolVersion is the oldest client protocol accepted by this Server.
	MinimumSupportedProtocolVersion uint32 = 1
	// CurrentProtocolVersion is the newest client protocol implemented by this Server.
	CurrentProtocolVersion uint32 = 3
)

// DexServerVersion identifies the running artifact and is replaced by release builds.
var DexServerVersion = "dev"
