// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package integ

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/gen/dexpb"
	hardDeleteAttributes "github.com/superdurable/dex/integ/workflow/hard-delete-attributes"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/bootstrap"
	"github.com/superdurable/dex/service/common/ptr"
	"go.temporal.io/api/workflowservice/v1"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	hardDeleteTransactionalRPCSelector = "transactional"
	hardDeleteSignalRPCSelector        = "signal"
	hardDeleteTransactionalRPCSecret   = hardDeleteAttributes.RPCSecretPrefix + hardDeleteTransactionalRPCSelector
	hardDeleteSignalRPCSecret          = hardDeleteAttributes.RPCSecretPrefix + hardDeleteSignalRPCSelector
	hardDeleteAPISecret                = "api-secret-a71e42"
	hardDeleteAPISecretAfterDelete     = "api-secret-after-delete-b93f05"
	hardDeleteContinuedSecret          = "continued-secret-c08d16"
)

var hardDeleteSecrets = []string{
	hardDeleteAttributes.InitialSecret,
	hardDeleteAttributes.InitialToken,
	hardDeleteAttributes.StepSecret,
	hardDeleteTransactionalRPCSecret,
	hardDeleteSignalRPCSecret,
	hardDeleteAPISecret,
	hardDeleteAPISecretAfterDelete,
	hardDeleteContinuedSecret,
}

func TestHardDeleteAttributesTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	runHardDeleteAttributeMatrix(t, service.BackendTypeTemporal)
}

func TestHardDeleteAttributesCadence(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}
	runHardDeleteAttributeMatrix(t, service.BackendTypeCadence)
}

func runHardDeleteAttributeMatrix(t *testing.T, backendType service.BackendType) {
	durabilities := []dexpb.StepDurability{
		dexpb.StepDurability_STEP_DURABILITY_SYNC,
		dexpb.StepDurability_STEP_DURABILITY_ASYNC,
	}
	for _, lazyLoading := range []bool{true, false} {
		for _, durability := range durabilities {
			for i := 0; i < *repeatIntegTest; i++ {
				t.Run(fmt.Sprintf("lazy=%v-%v-%d", lazyLoading, durability, i), func(t *testing.T) {
					doTestHardDeleteAttributes(t, backendType, lazyLoading, durability)
					smallWaitForFastTest()
				})
			}
		}
	}
}

// doTestHardDeleteAttributes covers every write path, reads in Steps and RPCs, deletion, and history contents.
func doTestHardDeleteAttributes(
	t *testing.T,
	backendType service.BackendType,
	lazyLoading bool,
	durability dexpb.StepDurability,
) {
	blobDirectory := t.TempDir()
	runtime := startDexService(t, DexServiceTestConfig{
		BackendType:        backendType,
		LocalBlobDirectory: blobDirectory,
		LazyLoading:        ptr.Any(lazyLoading),
	})
	workerHandler := hardDeleteAttributes.NewHandler()
	workerTarget := startWorker(t, workerHandler)
	flowClient := runtime.FlowClient

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	flowID := hardDeleteAttributes.WorkflowType + "-" + uuid.NewString()
	startOptions := withWorkerTarget(&dexpb.FlowStartOptions{
		Attributes: []*dexpb.AttributeWrite{
			hardDeleteAttributes.HardDeleteStringWrite(hardDeleteAttributes.SecretKey, hardDeleteAttributes.InitialSecret),
			hardDeleteAttributes.HardDeleteJSONWrite(hardDeleteAttributes.TokenKey, `"`+hardDeleteAttributes.InitialToken+`"`),
			{Key: hardDeleteAttributes.PlainKey, Value: stringValue(hardDeleteAttributes.PlainAttribute)},
		},
	}, workerTarget)
	startOptions.FlowConfigOverride.StepDurability = ptr.Any(durability)
	_, err := flowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowID,
		FlowType:           hardDeleteAttributes.WorkflowType,
		FlowTimeoutSeconds: 100,
		StartStepType:      hardDeleteAttributes.State1,
		StepOptions:        &dexpb.StepOptions{SkipWaitFor: true, ExecuteLoadAttributeMapInstances: []string{hardDeleteAttributes.TokenMapName + "/"}},
		FlowStartOptions:   startOptions,
	})
	require.NoError(t, err)

	requireWorkerSawAttributes(t, workerHandler, "S1_execute", map[string]string{
		hardDeleteAttributes.SecretKey: hardDeleteAttributes.InitialSecret,
		hardDeleteAttributes.TokenKey:  `"` + hardDeleteAttributes.InitialToken + `"`,
		hardDeleteAttributes.PlainKey:  hardDeleteAttributes.PlainAttribute,
	})
	requireWorkerSawAttributes(t, workerHandler, "S2_waitFor", map[string]string{
		hardDeleteAttributes.SecretKey: hardDeleteAttributes.StepSecret,
	})
	requireHardDeleteAttribute(t, ctx, flowClient, flowID, hardDeleteAttributes.SecretKey, hardDeleteAttributes.StepSecret)
	require.Len(t, hardDeleteObjectFiles(t, blobDirectory, flowID), 2)

	_, err = flowClient.LoadBlobs(ctx, &dexpb.LoadBlobsRequest{
		Entries: []*dexpb.LoadBlobRequestEntry{{
			FlowId: flowID,
			BlobValue: &dexpb.Value{Kind: &dexpb.Value_InternalBlobIdForStringValue{
				InternalBlobIdForStringValue: "local-store-id|hd/0123456789",
			}},
		}},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	for _, isTransactional := range []bool{true, false} {
		selector, nextSecret := hardDeleteSignalRPCSelector, hardDeleteSignalRPCSecret
		if isTransactional {
			selector, nextSecret = hardDeleteTransactionalRPCSelector, hardDeleteTransactionalRPCSecret
		}
		_, err = flowClient.InvokeRPC(ctx, &dexpb.InvokeRPCRequest{
			FlowId:          flowID,
			RpcName:         hardDeleteAttributes.RPCNameRotateSecret,
			Input:           stringValue(selector),
			RequestId:       newRequestID(),
			IsTransactional: isTransactional,
		})
		require.NoError(t, err)
		requireHardDeleteAttribute(t, ctx, flowClient, flowID, hardDeleteAttributes.SecretKey, nextSecret)
	}
	requireWorkerSawAttributes(t, workerHandler, hardDeleteAttributes.RPCNameRotateSecret+"_"+hardDeleteSignalRPCSelector,
		map[string]string{hardDeleteAttributes.SecretKey: hardDeleteTransactionalRPCSecret})

	setHardDeleteAttribute(t, ctx, flowClient, flowID,
		hardDeleteAttributes.HardDeleteStringWrite(hardDeleteAttributes.SecretKey, hardDeleteAPISecret))
	requireHardDeleteAttribute(t, ctx, flowClient, flowID, hardDeleteAttributes.SecretKey, hardDeleteAPISecret)

	publishToHardDeleteChannel(t, ctx, flowClient, flowID, hardDeleteAttributes.ProceedChannelName)
	requireWorkerSawAttributes(t, workerHandler, "S2_execute", map[string]string{
		hardDeleteAttributes.SecretKey: hardDeleteAPISecret,
		hardDeleteAttributes.TokenKey:  `"` + hardDeleteAttributes.InitialToken + `"`,
	})
	requireWorkerSawAttributes(t, workerHandler, "S3_waitFor", map[string]string{
		hardDeleteAttributes.SecretKey: hardDeleteAttributes.AbsentAttribute,
		hardDeleteAttributes.TokenKey:  hardDeleteAttributes.AbsentAttribute,
		hardDeleteAttributes.PlainKey:  hardDeleteAttributes.PlainAttribute,
	})
	requireHardDeleteAttributeAbsent(t, ctx, flowClient, flowID, hardDeleteAttributes.SecretKey)
	require.Empty(t, hardDeleteObjectFiles(t, blobDirectory, flowID))

	setHardDeleteAttribute(t, ctx, flowClient, flowID,
		hardDeleteAttributes.HardDeleteStringWrite(hardDeleteAttributes.SecretKey, hardDeleteAPISecretAfterDelete))
	requireHardDeleteAttribute(t, ctx, flowClient, flowID, hardDeleteAttributes.SecretKey, hardDeleteAPISecretAfterDelete)
	setHardDeleteAttribute(t, ctx, flowClient, flowID, hardDeleteAttributes.HardDeleteNullWrite(hardDeleteAttributes.SecretKey))
	requireHardDeleteAttributeAbsent(t, ctx, flowClient, flowID, hardDeleteAttributes.SecretKey)
	require.Empty(t, hardDeleteObjectFiles(t, blobDirectory, flowID))

	setHardDeleteAttribute(t, ctx, flowClient, flowID,
		hardDeleteAttributes.HardDeleteStringWrite(hardDeleteAttributes.SecretKey, hardDeleteContinuedSecret))
	requireHardDeleteAttribute(t, ctx, flowClient, flowID, hardDeleteAttributes.SecretKey, hardDeleteContinuedSecret)
	_, err = flowClient.TriggerContinueAsNew(ctx, &dexpb.TriggerContinueAsNewRequest{FlowId: flowID})
	require.NoError(t, err)
	requireHardDeleteAttribute(t, ctx, flowClient, flowID, hardDeleteAttributes.SecretKey, hardDeleteContinuedSecret)
	setHardDeleteAttribute(t, ctx, flowClient, flowID, hardDeleteAttributes.HardDeleteNullWrite(hardDeleteAttributes.SecretKey))
	requireHardDeleteAttributeAbsent(t, ctx, flowClient, flowID, hardDeleteAttributes.SecretKey)

	publishToHardDeleteChannel(t, ctx, flowClient, flowID, hardDeleteAttributes.CompletionChannelName)
	_, err = flowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID})
	require.NoError(t, err)
	requireWorkerSawAttributes(t, workerHandler, "S3_execute", map[string]string{
		hardDeleteAttributes.SecretKey: hardDeleteAttributes.AbsentAttribute,
		hardDeleteAttributes.PlainKey:  hardDeleteAttributes.PlainAttribute,
	})

	require.Empty(t, hardDeleteObjectFiles(t, blobDirectory, flowID))
	requireNoSecretInBlobFiles(t, blobDirectory)
	historyText := flowHistoryText(t, ctx, runtime, flowID)
	require.Contains(t, historyText, hardDeleteAttributes.PlainAttribute, "history text must expose inline payloads")
	for _, secret := range hardDeleteSecrets {
		require.NotContains(t, historyText, secret, "history must not store hard-delete Attribute values")
	}
}

func TestHardDeleteAttributeResetTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	doTestHardDeleteAttributeReset(t, service.BackendTypeTemporal)
}

func TestHardDeleteAttributeResetCadence(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}
	doTestHardDeleteAttributeReset(t, service.BackendTypeCadence)
}

// doTestHardDeleteAttributeReset verifies a reset before the deletion reads the Attribute as absent.
func doTestHardDeleteAttributeReset(t *testing.T, backendType service.BackendType) {
	runtime := startDexService(t, DexServiceTestConfig{BackendType: backendType})
	workerHandler := hardDeleteAttributes.NewHandler()
	workerTarget := startWorker(t, workerHandler)
	flowClient := runtime.FlowClient
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	flowID := hardDeleteAttributes.WorkflowType + "-" + uuid.NewString()
	startResponse, err := flowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowID,
		FlowType:           hardDeleteAttributes.WorkflowType,
		FlowTimeoutSeconds: 100,
		StartStepType:      hardDeleteAttributes.State2,
		StepOptions:        hardDeleteAttributes.StepOptions,
		FlowStartOptions: withWorkerTarget(&dexpb.FlowStartOptions{Attributes: []*dexpb.AttributeWrite{
			hardDeleteAttributes.HardDeleteStringWrite(hardDeleteAttributes.SecretKey, hardDeleteAttributes.InitialSecret),
		}}, workerTarget),
	})
	require.NoError(t, err)
	requireWorkerSawAttributes(t, workerHandler, "S2_waitFor", map[string]string{
		hardDeleteAttributes.SecretKey: hardDeleteAttributes.InitialSecret,
	})
	publishToHardDeleteChannel(t, ctx, flowClient, flowID, hardDeleteAttributes.ProceedChannelName)
	requireWorkerSawAttributes(t, workerHandler, "S3_waitFor", map[string]string{
		hardDeleteAttributes.SecretKey: hardDeleteAttributes.AbsentAttribute,
	})

	_, err = flowClient.ResetFlow(ctx, &dexpb.ResetFlowRequest{
		RunId:     startResponse.GetRunId(),
		FlowId:    flowID,
		ResetType: dexpb.FlowResetType_FLOW_RESET_TYPE_STEP_TYPE,
		StepType:  hardDeleteAttributes.State2,
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		seen, isFound := workerHandler.SeenAttributes("S2_waitFor")
		return isFound && seen[hardDeleteAttributes.SecretKey] == hardDeleteAttributes.AbsentAttribute
	}, 30*time.Second, 100*time.Millisecond, "reset Step must read the deleted Attribute as absent")
	requireHardDeleteAttributeAbsent(t, ctx, flowClient, flowID, hardDeleteAttributes.SecretKey)
}

func TestHardDeleteAttributeSubFlowTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	doTestHardDeleteAttributeSubFlow(t, service.BackendTypeTemporal)
}

func TestHardDeleteAttributeSubFlowCadence(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}
	doTestHardDeleteAttributeSubFlow(t, service.BackendTypeCadence)
}

// doTestHardDeleteAttributeSubFlow verifies SubFlow initial hard-delete Attributes stay out of both histories.
func doTestHardDeleteAttributeSubFlow(t *testing.T, backendType service.BackendType) {
	runtime := startDexService(t, DexServiceTestConfig{BackendType: backendType})
	workerHandler := hardDeleteAttributes.NewHandler()
	workerTarget := startWorker(t, workerHandler)
	flowClient := runtime.FlowClient
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	parentFlowID := hardDeleteAttributes.WorkflowType + "-" + uuid.NewString()
	_, err := flowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             parentFlowID,
		FlowType:           hardDeleteAttributes.WorkflowType,
		FlowTimeoutSeconds: 100,
		StartStepType:      hardDeleteAttributes.State0,
		FlowStartOptions:   withWorkerTarget(nil, workerTarget),
	})
	require.NoError(t, err)
	childFlowID := "SubFlow:" + parentFlowID + "-" + hardDeleteAttributes.State0 + "-1-0"
	requireHardDeleteAttribute(t, ctx, flowClient, childFlowID, hardDeleteAttributes.SecretKey, hardDeleteAttributes.SubFlowSecret)

	publishToHardDeleteChannel(t, ctx, flowClient, childFlowID, hardDeleteAttributes.ProceedChannelName)
	requireHardDeleteAttributeAbsent(t, ctx, flowClient, childFlowID, hardDeleteAttributes.SecretKey)
	publishToHardDeleteChannel(t, ctx, flowClient, childFlowID, hardDeleteAttributes.CompletionChannelName)
	_, err = flowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: parentFlowID})
	require.NoError(t, err)

	for _, flowID := range []string{parentFlowID, childFlowID} {
		require.NotContains(t, flowHistoryText(t, ctx, runtime, flowID), hardDeleteAttributes.SubFlowSecret)
	}
}

func TestHardDeleteAttributeValidationTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	runtime := startDexService(t, DexServiceTestConfig{BackendType: service.BackendTypeTemporal})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	invalidWrites := map[string]*dexpb.AttributeWrite{
		"int value": {
			Key:                hardDeleteAttributes.SecretKey,
			Value:              &dexpb.Value{Kind: &dexpb.Value_IntValue{IntValue: 1}},
			IsHardDeleteOnNull: true,
		},
		"indexed": {
			Key:                hardDeleteAttributes.SecretKey,
			Value:              stringValue(hardDeleteAttributes.InitialSecret),
			IndexConfig:        &dexpb.IndexConfig{Enable: true, Type: dexpb.IndexType_INDEX_TYPE_KEYWORD},
			IsHardDeleteOnNull: true,
		},
		"synced": {
			Key:                hardDeleteAttributes.SecretKey,
			Value:              stringValue(hardDeleteAttributes.InitialSecret),
			SyncConfig:         &dexpb.AttributeSyncConfig{Enabled: true},
			IsHardDeleteOnNull: true,
		},
	}
	for name, write := range invalidWrites {
		t.Run(name, func(t *testing.T) {
			_, err := runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
				RequestId:        newRequestID(),
				FlowId:           hardDeleteAttributes.WorkflowType + "-" + uuid.NewString(),
				FlowType:         hardDeleteAttributes.WorkflowType,
				StartStepType:    hardDeleteAttributes.State1,
				FlowStartOptions: &dexpb.FlowStartOptions{Attributes: []*dexpb.AttributeWrite{write}},
			})
			require.Equal(t, codes.InvalidArgument, status.Code(err), err)
		})
	}
}

func TestHardDeleteAttributeRequiresBlobStoreTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	runtime := startDexService(t, DexServiceTestConfig{
		BackendType:      service.BackendTypeTemporal,
		BlobStoreEnabled: ptr.Any(false),
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	_, err := runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:     newRequestID(),
		FlowId:        hardDeleteAttributes.WorkflowType + "-" + uuid.NewString(),
		FlowType:      hardDeleteAttributes.WorkflowType,
		StartStepType: hardDeleteAttributes.State1,
		FlowStartOptions: &dexpb.FlowStartOptions{Attributes: []*dexpb.AttributeWrite{
			hardDeleteAttributes.HardDeleteStringWrite(hardDeleteAttributes.SecretKey, hardDeleteAttributes.InitialSecret),
		}},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err), err)
}

// TestHardDeleteAttributeBlobCacheTemporal verifies S3 storage overwrites in place and never fills the blob cache.
func TestHardDeleteAttributeBlobCacheTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	blobCacheDirectory := t.TempDir()
	runtime := startDexService(t, DexServiceTestConfig{
		BackendType:        service.BackendTypeTemporal,
		S3TestThreshold:    50,
		BlobCacheDirectory: blobCacheDirectory,
	})
	workerTarget := startWorker(t, hardDeleteAttributes.NewHandler())
	flowClient := runtime.FlowClient
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	flowID := hardDeleteAttributes.WorkflowType + "-" + uuid.NewString()
	_, err := flowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowID,
		FlowType:           hardDeleteAttributes.WorkflowType,
		FlowTimeoutSeconds: 100,
		StartStepType:      hardDeleteAttributes.State2,
		StepOptions:        hardDeleteAttributes.StepOptions,
		FlowStartOptions: withWorkerTarget(&dexpb.FlowStartOptions{Attributes: []*dexpb.AttributeWrite{
			hardDeleteAttributes.HardDeleteStringWrite(hardDeleteAttributes.SecretKey, hardDeleteAttributes.InitialSecret),
		}}, workerTarget),
	})
	require.NoError(t, err)
	requireHardDeleteAttribute(t, ctx, flowClient, flowID, hardDeleteAttributes.SecretKey, hardDeleteAttributes.InitialSecret)
	setHardDeleteAttribute(t, ctx, flowClient, flowID,
		hardDeleteAttributes.HardDeleteStringWrite(hardDeleteAttributes.SecretKey, hardDeleteAPISecret))
	requireHardDeleteAttribute(t, ctx, flowClient, flowID, hardDeleteAttributes.SecretKey, hardDeleteAPISecret)
	setHardDeleteAttribute(t, ctx, flowClient, flowID, hardDeleteAttributes.HardDeleteNullWrite(hardDeleteAttributes.SecretKey))
	requireHardDeleteAttributeAbsent(t, ctx, flowClient, flowID, hardDeleteAttributes.SecretKey)

	requireNoSecretInBlobFiles(t, blobCacheDirectory)
	historyText := flowHistoryText(t, ctx, runtime, flowID)
	require.NotContains(t, historyText, hardDeleteAttributes.InitialSecret)
	require.NotContains(t, historyText, hardDeleteAPISecret)
}

// TestHardDeleteAttributeCleanupTemporal verifies blob cleanup removes hard-delete objects of deleted Flows only.
func TestHardDeleteAttributeCleanupTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	const storeID = "local-store-id"
	blobDirectory := t.TempDir()
	runtime := startDexService(t, DexServiceTestConfig{
		BackendType:        service.BackendTypeTemporal,
		LocalBlobDirectory: blobDirectory,
	})
	workerTarget := startWorker(t, hardDeleteAttributes.NewHandler())
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	runningFlowID := hardDeleteAttributes.WorkflowType + "-" + uuid.NewString()
	_, err := runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             runningFlowID,
		FlowType:           hardDeleteAttributes.WorkflowType,
		FlowTimeoutSeconds: 100,
		StartStepType:      hardDeleteAttributes.State2,
		StepOptions:        hardDeleteAttributes.StepOptions,
		FlowStartOptions: withWorkerTarget(&dexpb.FlowStartOptions{Attributes: []*dexpb.AttributeWrite{
			hardDeleteAttributes.HardDeleteStringWrite(hardDeleteAttributes.SecretKey, hardDeleteAttributes.InitialSecret),
		}}, workerTarget),
	})
	require.NoError(t, err)
	deletedFlowID := hardDeleteAttributes.WorkflowType + "-deleted-" + uuid.NewString()
	_, _, err = runtime.BlobStore.WriteHardDeleteObject(ctx, deletedFlowID, hardDeleteAttributes.SecretKey, []byte(hardDeleteAPISecret))
	require.NoError(t, err)
	require.Len(t, hardDeleteObjectFiles(t, blobDirectory, runningFlowID), 1)
	require.Len(t, hardDeleteObjectFiles(t, blobDirectory, deletedFlowID), 1)

	cleanupWorkflowID := "test-hard-delete-cleanup-" + uuid.NewString()
	require.NoError(t, runtime.UnifiedClient.StartBlobStoreCleanupWorkflow(ctx, service.TaskQueue, cleanupWorkflowID, "", storeID))
	_, err = runtime.UnifiedClient.GetWorkflowResult(ctx, nil, cleanupWorkflowID, "")
	require.NoError(t, err)

	require.Len(t, hardDeleteObjectFiles(t, blobDirectory, runningFlowID), 1)
	require.Empty(t, hardDeleteObjectFiles(t, blobDirectory, deletedFlowID))
	requireHardDeleteAttribute(t, ctx, runtime.FlowClient, runningFlowID, hardDeleteAttributes.SecretKey, hardDeleteAttributes.InitialSecret)
}

func requireWorkerSawAttributes(
	t *testing.T,
	workerHandler interface {
		SeenAttributes(string) (map[string]string, bool)
	},
	invocation string,
	expected map[string]string,
) {
	t.Helper()
	var seen map[string]string
	require.Eventually(t, func() bool {
		var isFound bool
		seen, isFound = workerHandler.SeenAttributes(invocation)
		return isFound
	}, 30*time.Second, 100*time.Millisecond, "Worker never received %s", invocation)
	for key, value := range expected {
		require.Equal(t, value, seen[key], "%s Attribute %q", invocation, key)
	}
}

func requireHardDeleteAttribute(
	t *testing.T,
	ctx context.Context,
	flowClient dexpb.FlowServiceClient,
	flowID string,
	key string,
	expected string,
) {
	t.Helper()
	var lastValue *dexpb.Value
	require.Eventually(t, func() bool {
		response, err := flowClient.GetAttributes(ctx, &dexpb.GetAttributesRequest{FlowId: flowID, Keys: []string{key}})
		if err != nil {
			return false
		}
		lastValue = attributeMap(response.GetAttributes())[key]
		return lastValue.GetStringValue() == expected
	}, 30*time.Second, 100*time.Millisecond, "Attribute %q never became %q; last value %v", key, expected, lastValue)
}

func requireHardDeleteAttributeAbsent(
	t *testing.T,
	ctx context.Context,
	flowClient dexpb.FlowServiceClient,
	flowID string,
	key string,
) {
	t.Helper()
	require.Eventually(t, func() bool {
		response, err := flowClient.GetAttributes(ctx, &dexpb.GetAttributesRequest{FlowId: flowID, Keys: []string{key}})
		if err != nil {
			return false
		}
		_, isPresent := attributeMap(response.GetAttributes())[key]
		return !isPresent
	}, 30*time.Second, 100*time.Millisecond, "Attribute %q was never deleted", key)
}

func setHardDeleteAttribute(
	t *testing.T,
	ctx context.Context,
	flowClient dexpb.FlowServiceClient,
	flowID string,
	write *dexpb.AttributeWrite,
) {
	t.Helper()
	_, err := flowClient.SetAttributes(ctx, &dexpb.SetAttributesRequest{
		RequestId:  newRequestID(),
		FlowId:     flowID,
		Attributes: []*dexpb.AttributeWrite{write},
	})
	require.NoError(t, err)
}

func publishToHardDeleteChannel(
	t *testing.T,
	ctx context.Context,
	flowClient dexpb.FlowServiceClient,
	flowID string,
	channelName string,
) {
	t.Helper()
	_, err := flowClient.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{
		FlowId:   flowID,
		Messages: []*dexpb.ChannelMessage{{ChannelName: channelName, Value: stringValue("go")}},
	})
	require.NoError(t, err)
}

// hardDeleteObjectFiles lists local files stored under the hard-delete directory of flowID.
func hardDeleteObjectFiles(t *testing.T, blobDirectory string, flowID string) []string {
	t.Helper()
	marker := string(filepath.Separator) + "000101$" + flowID + string(filepath.Separator) + "hd" + string(filepath.Separator)
	var files []string
	err := filepath.WalkDir(blobDirectory, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.Contains(path, marker) && !strings.HasPrefix(entry.Name(), ".") {
			files = append(files, path)
		}
		return nil
	})
	require.NoError(t, err)
	return files
}

func requireNoSecretInBlobFiles(t *testing.T, directory string) {
	t.Helper()
	err := filepath.WalkDir(directory, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, secret := range hardDeleteSecrets {
			require.False(t, bytes.Contains(data, []byte(secret)), "blob file %s contains %q", path, secret)
		}
		return nil
	})
	require.NoError(t, err)
}

// flowHistoryText renders every run of a Flow so tests can search for leaked payload bytes.
func flowHistoryText(t *testing.T, ctx context.Context, runtime *integRuntime, flowID string) string {
	t.Helper()
	if runtime.UnifiedClient.GetBackendType() == service.BackendTypeTemporal {
		return temporalHistoryText(t, ctx, runtime.UnifiedClient.GetApiService().(workflowservice.WorkflowServiceClient), flowID)
	}
	return cadenceHistoryText(t, ctx, runtime.UnifiedClient.GetApiService().(client.Client), flowID)
}

// cadenceHistoryText renders the current run plus every closed run, including continued-as-new runs.
func cadenceHistoryText(t *testing.T, ctx context.Context, api client.Client, flowID string) string {
	t.Helper()
	runIDs := []string{""}
	response, err := api.ListClosedWorkflow(ctx, &shared.ListClosedWorkflowExecutionsRequest{
		Domain: ptr.Any(bootstrap.DefaultCadenceDomain),
		StartTimeFilter: &shared.StartTimeFilter{
			EarliestTime: ptr.Any(int64(0)),
			LatestTime:   ptr.Any(time.Now().Add(time.Hour).UnixNano()),
		},
		ExecutionFilter: &shared.WorkflowExecutionFilter{WorkflowId: ptr.Any(flowID)},
	})
	require.NoError(t, err)
	for _, execution := range response.GetExecutions() {
		runIDs = append(runIDs, execution.GetExecution().GetRunId())
	}
	var builder strings.Builder
	for _, runID := range runIDs {
		iterator := api.GetWorkflowHistory(ctx, flowID, runID, false, shared.HistoryEventFilterTypeAllEvent)
		for iterator.HasNext() {
			event, err := iterator.Next()
			require.NoError(t, err)
			appendReflectedText(&builder, reflect.ValueOf(event))
		}
	}
	return builder.String()
}

// appendReflectedText writes every string and byte slice reachable from value.
func appendReflectedText(builder *strings.Builder, value reflect.Value) {
	switch value.Kind() {
	case reflect.Pointer, reflect.Interface:
		if !value.IsNil() {
			appendReflectedText(builder, value.Elem())
		}
	case reflect.Struct:
		for index := 0; index < value.NumField(); index++ {
			appendReflectedText(builder, value.Field(index))
		}
	case reflect.Slice:
		if value.Type().Elem().Kind() == reflect.Uint8 {
			builder.Write(value.Bytes())
			appendDecodedBase64Text(builder, value.Bytes())
			return
		}
		for index := 0; index < value.Len(); index++ {
			appendReflectedText(builder, value.Index(index))
		}
	case reflect.Map:
		iterator := value.MapRange()
		for iterator.Next() {
			appendReflectedText(builder, iterator.Key())
			appendReflectedText(builder, iterator.Value())
		}
	case reflect.String:
		builder.WriteString(value.String())
		appendDecodedBase64Text(builder, []byte(value.String()))
	}
}

// appendDecodedBase64Text decodes base64 runs because the Cadence data converter stores payloads as text.
func appendDecodedBase64Text(builder *strings.Builder, data []byte) {
	isBase64Byte := func(character rune) bool {
		return character >= 'A' && character <= 'Z' || character >= 'a' && character <= 'z' ||
			character >= '0' && character <= '9' || character == '+' || character == '/' || character == '='
	}
	for _, token := range bytes.FieldsFunc(data, func(character rune) bool { return !isBase64Byte(character) }) {
		decoded, err := base64.StdEncoding.DecodeString(string(token))
		if err == nil {
			builder.Write(decoded)
		}
	}
}
