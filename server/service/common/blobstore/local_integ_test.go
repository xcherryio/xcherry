// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package blobstore

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/common/log/loggerimpl"
	"github.com/superdurable/dex/service/common/ptr"
	"go.temporal.io/sdk/client"
)

func TestLocalBlobStoreIntegration(t *testing.T) {
	root := t.TempDir()
	logger, err := loggerimpl.NewDevelopment()
	require.NoError(t, err)
	store, err := NewBlobStore(nil, "local-namespace", &config.BlobStoreConfig{
		Enabled: ptr.Any(true),
		SupportedStorages: []config.BlobStoreConfigEntry{{
			Status:         config.StorageStatusActive,
			StorageId:      "local",
			StorageType:    config.StorageTypeLocal,
			LocalDirectory: root,
		}},
	}, logger, client.MetricsNopHandler)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	ctx := context.Background()

	valueFlowID := "value-flow"
	storeID, objectPath, err := store.WriteObject(ctx, valueFlowID, "invocation", []byte("value"))
	require.NoError(t, err)
	require.Equal(t, "local", storeID)
	loaded, err := store.ReadObject(ctx, storeID, valueFlowID, objectPath)
	require.NoError(t, err)
	require.Equal(t, []byte("value"), loaded)

	runStarted := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	flowID := "flow/with$difficult characters"
	runID := "run/with$difficult characters"
	stepExecutionID := "step/with$difficult characters"
	err = store.WriteStepEventInput(
		ctx,
		runStarted,
		flowID,
		runID,
		stepExecutionID,
		StepEventInputMethodExecute,
		[]byte("request"),
	)
	require.NoError(t, err)
	request, found, err := store.ReadStepEventInput(
		ctx,
		runStarted,
		flowID,
		runID,
		stepExecutionID,
		StepEventInputMethodExecute,
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("request"), request)

	paths, err := store.ListWorkflowPaths(ctx, ListObjectPathsInput{StoreId: storeID})
	require.NoError(t, err)
	require.Len(t, paths.WorkflowPaths, 2)
	var runPath string
	for _, path := range paths.WorkflowPaths {
		parsed, parseErr := ParseWorkflowPath(path)
		require.NoError(t, parseErr)
		if parsed.RunID == runID {
			runPath = path
			require.Equal(t, flowID, parsed.FlowID)
		}
	}
	require.NotEmpty(t, runPath)
	require.NoError(t, store.DeleteWorkflowObjects(ctx, storeID, runPath))
	_, found, err = store.ReadStepEventInput(
		ctx,
		runStarted,
		flowID,
		runID,
		stepExecutionID,
		StepEventInputMethodExecute,
	)
	require.NoError(t, err)
	require.False(t, found)

	err = filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		require.NoError(t, walkErr)
		require.False(t, strings.HasPrefix(entry.Name(), ".dex-blob-"), path)
		return nil
	})
	require.NoError(t, err)
}

func TestLocalBlobStoreRejectsEscapingPaths(t *testing.T) {
	_, err := localObjectPath(t.TempDir(), "../../outside")
	require.Error(t, err)
}

func TestLocalBlobStoreTransfersCrossFlowOwnership(t *testing.T) {
	root := t.TempDir()
	logger, err := loggerimpl.NewDevelopment()
	require.NoError(t, err)
	store, err := NewBlobStore(nil, "local-namespace", &config.BlobStoreConfig{
		Enabled: ptr.Any(true),
		SupportedStorages: []config.BlobStoreConfigEntry{{
			Status:         config.StorageStatusActive,
			StorageId:      "local",
			StorageType:    config.StorageTypeLocal,
			LocalDirectory: root,
		}},
	}, logger, client.MetricsNopHandler)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	ctx := context.Background()
	sourceFlowID := "source-flow"
	destinationFlowID := "destination-flow"
	threshold := config.DefaultBlobStoreThresholdInBytes
	payload := bytes.Repeat([]byte("x"), threshold+1)
	value := &dexpb.Value{Kind: &dexpb.Value_ObjValue{ObjValue: &dexpb.EncodedObject{
		Encoding: "json",
		Payload:  payload,
	}}}
	require.NoError(t, OffloadLargeValue(ctx, value, sourceFlowID, "invocation", threshold, store, true))
	sourceRef := value.GetInternalBlobIdForObjValue()
	require.Regexp(t, regexp.MustCompile(`^local\|[0-9a-z]{1,3}/[0-9a-z]{10}$`), sourceRef)

	rawValue := &dexpb.Value{Kind: &dexpb.Value_ObjValue{ObjValue: &dexpb.EncodedObject{
		Encoding: "raw",
		Payload:  payload,
	}}}
	require.NoError(t, OffloadLargeValue(ctx, rawValue, sourceFlowID, "invocation", threshold, store, true))
	rawRef := rawValue.GetInternalBlobIdForObjValue()
	require.Regexp(t, regexp.MustCompile(`^local\|[0-9a-z]{1,3}/[0-9a-z]{10}$`), rawRef)
	require.NotEqual(t, sourceRef, rawRef)
	require.NoError(t, HydrateValue(ctx, sourceFlowID, rawValue, store))
	require.Equal(t, "raw", rawValue.GetObjValue().GetEncoding())
	require.Equal(t, payload, rawValue.GetObjValue().GetPayload())

	stringPayload := string(bytes.Repeat([]byte("s"), threshold+1))
	stringValue := &dexpb.Value{Kind: &dexpb.Value_StringValue{StringValue: stringPayload}}
	require.NoError(t, OffloadLargeValue(ctx, stringValue, sourceFlowID, "string", threshold, store, true))
	require.Regexp(
		t,
		regexp.MustCompile(`^local\|[0-9a-z]{1,3}/[0-9a-z]{10}$`),
		stringValue.GetInternalBlobIdForStringValue(),
	)
	require.NoError(t, HydrateValue(ctx, sourceFlowID, stringValue, store))
	require.Equal(t, stringPayload, stringValue.GetStringValue())

	legacyReference := &dexpb.Value{Kind: &dexpb.Value_InternalBlobIdForObjValue{
		InternalBlobIdForObjValue: sourceRef + "|json",
	}}
	require.ErrorContains(t, HydrateValue(ctx, sourceFlowID, legacyReference, store), "invalid Blob ID")

	require.NoError(t, TransferValueBlobOwnership(
		ctx, value, sourceFlowID, destinationFlowID, "invocation", threshold, store, true,
	))
	require.Equal(t, sourceRef, value.GetInternalBlobIdForObjValue())
	require.Equal(t, int64(3), mustCountFlowObjects(t, ctx, store, sourceFlowID))
	require.Equal(t, int64(1), mustCountFlowObjects(t, ctx, store, destinationFlowID))

	referenceDate := strings.SplitN(strings.SplitN(sourceRef, "|", 2)[1], "/", 2)[0]
	writeDate, err := parseBlobReferenceDate(referenceDate)
	require.NoError(t, err)
	require.NoError(t, store.DeleteWorkflowObjects(
		ctx, "local", formatBlobStorageDate(writeDate)+"$"+encodeFlowIDPathPart(sourceFlowID),
	))
	require.NoError(t, HydrateValue(ctx, destinationFlowID, value, store))
	require.Equal(t, "json", value.GetObjValue().GetEncoding())
	require.Equal(t, payload, value.GetObjValue().GetPayload())

	inline := &dexpb.Value{Kind: &dexpb.Value_StringValue{StringValue: string(bytes.Repeat([]byte("y"), threshold))}}
	require.NoError(t, OffloadLargeValue(ctx, inline, destinationFlowID, "inline", threshold, store, true))
	require.NotEmpty(t, inline.GetStringValue())
}

func mustCountFlowObjects(t *testing.T, ctx context.Context, store BlobStore, flowID string) int64 {
	t.Helper()
	count, err := store.CountWorkflowObjectsForTesting(ctx, flowID)
	require.NoError(t, err)
	return count
}
