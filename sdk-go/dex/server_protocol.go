// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package dex

import (
	"fmt"
	"runtime/debug"
	"strings"
	"sync/atomic"

	"github.com/superdurable/dex/sdk-go/gen/dexpb"
)

const (
	minimumSupportedServerProtocolVersion uint32 = 2
	maximumSupportedServerProtocolVersion uint32 = 3
	// hardDeleteAttributeProtocolVersion is the first Server protocol that keeps hard-delete Attributes out of history.
	hardDeleteAttributeProtocolVersion uint32 = 3
	goSDKModulePath                           = "github.com/superdurable/dex/sdk-go"
)

func negotiateServerProtocol(
	serverInfo *dexpb.ServerInfo,
	sdkVersion string,
) (uint32, error) {
	if serverInfo == nil {
		return 0, serverProtocolError(
			sdkVersion,
			"unknown",
			0,
			0,
			"Server returned an empty response",
		)
	}
	serverMinimum := serverInfo.GetMinimumSupportedProtocolVersion()
	serverCurrent := serverInfo.GetCurrentProtocolVersion()
	if minimumSupportedServerProtocolVersion == 0 ||
		maximumSupportedServerProtocolVersion == 0 ||
		minimumSupportedServerProtocolVersion > maximumSupportedServerProtocolVersion {
		return 0, serverProtocolError(
			sdkVersion,
			serverInfo.GetServerVersion(),
			serverMinimum,
			serverCurrent,
			"Go SDK protocol interval is invalid",
		)
	}
	if serverMinimum == 0 || serverCurrent == 0 || serverMinimum > serverCurrent {
		return 0, serverProtocolError(
			sdkVersion,
			serverInfo.GetServerVersion(),
			serverMinimum,
			serverCurrent,
			"Server protocol interval is invalid",
		)
	}
	negotiatedProtocolVersion := min(serverCurrent, maximumSupportedServerProtocolVersion)
	if negotiatedProtocolVersion < serverMinimum ||
		negotiatedProtocolVersion < minimumSupportedServerProtocolVersion {
		return 0, serverProtocolError(
			sdkVersion,
			serverInfo.GetServerVersion(),
			serverMinimum,
			serverCurrent,
			"protocol intervals do not overlap",
		)
	}
	return negotiatedProtocolVersion, nil
}

// negotiatedServerProtocol holds the protocol version agreed with the Server; zero means not negotiated.
type negotiatedServerProtocol struct {
	version atomic.Uint32
}

// requireHardDeleteAttributeSupport fails closed because older Servers would store hard-delete values in history.
func (protocol *negotiatedServerProtocol) requireHardDeleteAttributeSupport(
	writes []*dexpb.AttributeWrite,
) error {
	if protocol.version.Load() >= hardDeleteAttributeProtocolVersion {
		return nil
	}
	for _, write := range writes {
		if write.GetIsHardDeleteOnNull() {
			return &HardDeleteAttributeUnsupportedError{
				AttributeName:                 write.GetKey(),
				NegotiatedServerProtocol:      protocol.version.Load(),
				RequiredServerProtocolVersion: hardDeleteAttributeProtocolVersion,
			}
		}
	}
	return nil
}

func serverProtocolRequestError(sdkVersion string, err error) error {
	return fmt.Errorf(
		"dex: Go SDK version %q protocol [%d,%d] is incompatible with Server version %q protocol [unknown,unknown]: GetServerInfo failed: %w",
		sdkVersion,
		minimumSupportedServerProtocolVersion,
		maximumSupportedServerProtocolVersion,
		"unknown",
		err,
	)
}

func serverProtocolError(
	sdkVersion string,
	serverVersion string,
	serverMinimum uint32,
	serverCurrent uint32,
	reason string,
) error {
	if serverVersion == "" {
		serverVersion = "unknown"
	}
	return fmt.Errorf(
		"dex: Go SDK version %q protocol [%d,%d] is incompatible with Server version %q protocol [%d,%d]: %s",
		sdkVersion,
		minimumSupportedServerProtocolVersion,
		maximumSupportedServerProtocolVersion,
		serverVersion,
		serverMinimum,
		serverCurrent,
		reason,
	)
}

func currentGoSDKVersion() string {
	buildInfo, ok := debug.ReadBuildInfo()
	if !ok {
		return "dev"
	}
	if buildInfo.Main.Path == goSDKModulePath {
		return displayGoModuleVersion(buildInfo.Main.Version)
	}
	for _, dependency := range buildInfo.Deps {
		if dependency.Path != goSDKModulePath {
			continue
		}
		if dependency.Replace != nil {
			return displayGoModuleVersion(dependency.Replace.Version)
		}
		return displayGoModuleVersion(dependency.Version)
	}
	return "dev"
}

func displayGoModuleVersion(version string) string {
	if version == "" || version == "(devel)" {
		return "dev"
	}
	return strings.TrimPrefix(version, "v")
}
