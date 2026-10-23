// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package integ

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
	temporalapi "github.com/superdurable/dex/service/client/temporal"
	dexconverter "github.com/superdurable/dex/service/common/converter"
	"github.com/superdurable/dex/service/common/ptr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type externalDexFlowClient struct {
	dexpb.FlowServiceClient
	maxWaitSeconds int32
}

var localDexDevTestAttributeIndexes = map[string]dexpb.IndexType{
	"CustomKeywordField":      dexpb.IndexType_INDEX_TYPE_KEYWORD,
	"CustomKeywordArrayField": dexpb.IndexType_INDEX_TYPE_KEYWORD_ARRAY,
	"CustomIntField":          dexpb.IndexType_INDEX_TYPE_INT,
	"CustomBoolField":         dexpb.IndexType_INDEX_TYPE_BOOL,
	"CustomDoubleField":       dexpb.IndexType_INDEX_TYPE_DOUBLE,
	"CustomDatetimeField":     dexpb.IndexType_INDEX_TYPE_DATETIME,
	"CustomTextField":         dexpb.IndexType_INDEX_TYPE_TEXT,
}

func prepareExternalDex(ctx context.Context) error {
	if err := waitForExternalDex(ctx); err != nil {
		return err
	}
	connection, err := newDexClientConnection(*dexServerAddress, config.DefaultGrpcMaxMessageBytes)
	if err != nil {
		return err
	}
	_, syncErr := dexpb.NewFlowServiceClient(connection).SyncAttributeIndexes(
		ctx,
		&dexpb.SyncAttributeIndexRequest{AttributeIndexes: localDexDevTestAttributeIndexes},
	)
	return errors.Join(syncErr, connection.Close())
}

func waitForExternalDex(ctx context.Context) (waitErr error) {
	connection, err := newDexClientConnection(*dexServerAddress, config.DefaultGrpcMaxMessageBytes)
	if err != nil {
		return err
	}
	defer func() {
		waitErr = errors.Join(waitErr, connection.Close())
	}()
	healthClient := healthpb.NewHealthClient(connection)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		checkCtx, cancel := context.WithTimeout(ctx, time.Second)
		response, checkErr := healthClient.Check(checkCtx, &healthpb.HealthCheckRequest{
			Service: dexpb.FlowService_ServiceDesc.ServiceName,
		})
		cancel()
		if checkErr == nil && response.GetStatus() == healthpb.HealthCheckResponse_SERVING {
			return nil
		}
		if checkErr != nil {
			lastErr = checkErr
		} else {
			lastErr = fmt.Errorf("Dex FlowService health status is %s", response.GetStatus())
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for Dex at %s: %w", *dexServerAddress, errors.Join(ctx.Err(), lastErr))
		case <-ticker.C:
		}
	}
}

func connectToExternalDexService(t *testing.T, testConfig DexServiceTestConfig) *integRuntime {
	t.Helper()
	require.Equal(t, service.BackendTypeTemporal, testConfig.BackendType)
	if unsupportedReason := externalDexUnsupportedReason(testConfig); unsupportedReason != "" {
		t.Skipf("requires per-test in-process Dex configuration: %s", unsupportedReason)
	}
	cfg := createTestConfig(t, testConfig)
	dataConverter := dexconverter.NewTemporalDataConverter()
	temporalClient := createTemporalClient(t, dataConverter, nil)
	unifiedClient := temporalapi.NewTemporalClient(
		temporalClient,
		testNamespace,
		dataConverter,
		false,
		cfg.Api.QueryWorkflowFailedRetryPolicy,
		nil,
	)
	t.Cleanup(unifiedClient.Close)
	connection, err := newDexClientConnection(
		*dexServerAddress,
		cfg.Api.EffectiveGrpcMaxMessageBytes(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, connection.Close())
	})
	return &integRuntime{
		PublicAddress: *dexServerAddress,
		FlowClient: &externalDexFlowClient{
			FlowServiceClient: dexpb.NewFlowServiceClient(connection),
			maxWaitSeconds:    int32(cfg.Api.EffectiveMaxWaitSeconds()),
		},
		UnifiedClient: unifiedClient,
		defaultFlowConfig: &dexpb.FlowConfig{
			ContinueAsNewThreshold: ptr.Any(int32(100)),
			StepDurability: ptr.Any(
				dexpb.StepDurability_STEP_DURABILITY_SYNC,
			),
		},
		internalDumpCapture: &internalDumpHeaderCapture{},
	}
}

func (client *externalDexFlowClient) StartFlow(
	ctx context.Context,
	request *dexpb.StartFlowRequest,
	options ...grpc.CallOption,
) (*dexpb.StartFlowResponse, error) {
	if request.FlowStartOptions == nil {
		request.FlowStartOptions = &dexpb.FlowStartOptions{}
	}
	if request.FlowStartOptions.FlowConfigOverride == nil {
		request.FlowStartOptions.FlowConfigOverride = syncDurabilityConfig()
	} else if request.FlowStartOptions.FlowConfigOverride.StepDurability == nil {
		request.FlowStartOptions.FlowConfigOverride.StepDurability = ptr.Any(
			dexpb.StepDurability_STEP_DURABILITY_SYNC,
		)
	}
	return client.FlowServiceClient.StartFlow(ctx, request, options...)
}

func (client *externalDexFlowClient) WaitForFlow(
	ctx context.Context,
	request *dexpb.WaitForFlowRequest,
	options ...grpc.CallOption,
) (*dexpb.FlowResult, error) {
	if request.WaitTimeSeconds == 0 || request.WaitTimeSeconds > client.maxWaitSeconds {
		request.WaitTimeSeconds = client.maxWaitSeconds
	}
	return client.FlowServiceClient.WaitForFlow(ctx, request, options...)
}

func (client *externalDexFlowClient) WaitForStepCompletion(
	ctx context.Context,
	request *dexpb.WaitForStepCompletionRequest,
	options ...grpc.CallOption,
) (*dexpb.WaitForStepCompletionResponse, error) {
	return client.FlowServiceClient.WaitForStepCompletion(ctx, request, options...)
}

func (client *externalDexFlowClient) WaitForAttribute(
	ctx context.Context,
	request *dexpb.WaitForAttributeRequest,
	options ...grpc.CallOption,
) (*dexpb.WaitForAttributeResponse, error) {
	return client.FlowServiceClient.WaitForAttribute(ctx, request, options...)
}

func externalDexUnsupportedReason(testConfig DexServiceTestConfig) string {
	if testConfig.MemoEncryption {
		return "memo encryption"
	}
	if len(testConfig.DefaultHeaders) > 0 {
		return "default headers"
	}
	if testConfig.S3TestThreshold > 0 || testConfig.BlobStoreEnabled != nil || testConfig.LazyLoading != nil {
		return "external storage"
	}
	if *disableStickyCache {
		return "disabled sticky cache"
	}
	return ""
}

func newDexClientConnection(address string, maxMessageBytes int) (*grpc.ClientConn, error) {
	return grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMessageBytes),
			grpc.MaxCallSendMsgSize(maxMessageBytes),
		),
	)
}
