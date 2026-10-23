// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package integ

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestServerInfoTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	testServerInfo(t, service.BackendTypeTemporal)
}

func TestServerInfoCadence(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}
	testServerInfo(t, service.BackendTypeCadence)
}

func testServerInfo(t *testing.T, backendType service.BackendType) {
	previousVersion := service.DexServerVersion
	service.DexServerVersion = "integration-test-version"
	t.Cleanup(func() { service.DexServerVersion = previousVersion })

	runtime := startDexService(t, DexServiceTestConfig{BackendType: backendType})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	serverInfo, err := runtime.FlowClient.GetServerInfo(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	require.Equal(t, "integration-test-version", serverInfo.GetServerVersion())
	require.Equal(t, uint32(1), serverInfo.GetMinimumSupportedProtocolVersion())
	require.Equal(t, uint32(3), serverInfo.GetCurrentProtocolVersion())
	if backendType == service.BackendTypeTemporal {
		require.Equal(t, dexpb.ServerEngine_SERVER_ENGINE_TEMPORAL, serverInfo.GetServerEngine())
	} else {
		require.Equal(t, dexpb.ServerEngine_SERVER_ENGINE_CADENCE, serverInfo.GetServerEngine())
	}
}
