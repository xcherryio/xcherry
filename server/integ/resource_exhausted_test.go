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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/bootstrap"
	"github.com/superdurable/dex/service/common/ptr"
	"go.temporal.io/api/enums/v1"
	temporalerrordetails "go.temporal.io/api/errordetails/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.uber.org/cadence/.gen/go/shared"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

const engineResourceExhaustedNamespace = "dex-engine-resource-exhausted"

func TestSearchFlowsResourceExhaustedTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	registerTemporalResourceExhaustedNamespace(t, ctx)
	runtime := startDexService(t, DexServiceTestConfig{
		BackendType:       service.BackendTypeTemporal,
		TemporalNamespace: engineResourceExhaustedNamespace,
	})
	assertSearchFlowsResourceExhausted(t, ctx, runtime.FlowClient, "namespace rate limit exceeded", func(returnedStatus *status.Status) {
		require.Len(t, returnedStatus.Details(), 1)
		resourceExhaustedFailure, ok := returnedStatus.Details()[0].(*temporalerrordetails.ResourceExhaustedFailure)
		require.True(t, ok)
		require.Equal(t, enums.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT, resourceExhaustedFailure.GetCause())
		require.Equal(t, enums.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE, resourceExhaustedFailure.GetScope())
	})
}

func TestSearchFlowsResourceExhaustedCadence(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	registerCadenceResourceExhaustedDomain(t, ctx)
	runtime := startDexService(t, DexServiceTestConfig{
		BackendType:   service.BackendTypeCadence,
		CadenceDomain: engineResourceExhaustedNamespace,
	})
	assertSearchFlowsResourceExhausted(
		t,
		ctx,
		runtime.FlowClient,
		"Too many outstanding requests to the cadence service",
		func(returnedStatus *status.Status) { require.Empty(t, returnedStatus.Details()) },
	)
}

func registerTemporalResourceExhaustedNamespace(t *testing.T, ctx context.Context) {
	t.Helper()
	namespaceClient, err := client.NewNamespaceClient(client.Options{HostPort: *temporalHostPort})
	require.NoError(t, err)
	t.Cleanup(namespaceClient.Close)
	err = namespaceClient.Register(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        engineResourceExhaustedNamespace,
		WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
	})
	var namespaceAlreadyExistsError *serviceerror.NamespaceAlreadyExists
	if !errors.As(err, &namespaceAlreadyExistsError) {
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		_, describeErr := namespaceClient.Describe(ctx, engineResourceExhaustedNamespace)
		return describeErr == nil
	}, time.Minute, 100*time.Millisecond)
}

func registerCadenceResourceExhaustedDomain(t *testing.T, ctx context.Context) {
	t.Helper()
	cadenceService, _, closeService, err := bootstrap.BuildCadenceServiceClient(bootstrap.DefaultCadenceHostPort)
	require.NoError(t, err)
	t.Cleanup(closeService)
	err = cadenceService.RegisterDomain(ctx, &shared.RegisterDomainRequest{
		Name:                                   ptr.Any(engineResourceExhaustedNamespace),
		WorkflowExecutionRetentionPeriodInDays: ptr.Any(int32(1)),
	})
	var domainAlreadyExistsError *shared.DomainAlreadyExistsError
	if !errors.As(err, &domainAlreadyExistsError) {
		require.NoError(t, err)
	}
}

func assertSearchFlowsResourceExhausted(
	t *testing.T,
	ctx context.Context,
	flowClient dexpb.FlowServiceClient,
	expectedMessage string,
	assertDetails func(*status.Status),
) {
	t.Helper()
	var resourceExhaustedError error
	for attempt := 0; attempt < 10; attempt++ {
		_, searchErr := flowClient.SearchFlows(ctx, &dexpb.SearchFlowsRequest{})
		if status.Code(searchErr) == codes.ResourceExhausted {
			resourceExhaustedError = searchErr
			break
		}
		require.NoError(t, searchErr)
	}
	require.Error(t, resourceExhaustedError)
	returnedStatus := status.Convert(resourceExhaustedError)
	require.Equal(t, codes.ResourceExhausted, returnedStatus.Code())
	require.Equal(t, expectedMessage, returnedStatus.Message())
	assertDetails(returnedStatus)
}
