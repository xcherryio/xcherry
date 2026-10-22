// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//go:build attributestore_integration

package integ

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/integ/workflow/signal"
	"github.com/superdurable/dex/service"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestAttributeSyncTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	doTestAttributeSync(t, service.BackendTypeTemporal)
}

func TestAttributeSyncCadence(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}
	doTestAttributeSync(t, service.BackendTypeCadence)
}

func TestMongoDBAttributeSyncTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	doTestAttributeSyncMongoDB(t, service.BackendTypeTemporal)
}

func TestMongoDBAttributeSyncCadence(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}
	doTestAttributeSyncMongoDB(t, service.BackendTypeCadence)
}

func TestAttributeSyncRedisTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	doTestAttributeSyncRedis(t, service.BackendTypeTemporal)
}

func TestAttributeSyncRedisCadence(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}
	doTestAttributeSyncRedis(t, service.BackendTypeCadence)
}

func TestAttributeSyncFlowTimeoutTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	doTestAttributeSyncFlowTimeout(t, service.BackendTypeTemporal)
}

func TestAttributeSyncFlowTimeoutCadence(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}
	doTestAttributeSyncFlowTimeout(t, service.BackendTypeCadence)
}

func TestAttributeSyncRetryExhaustionTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	doTestAttributeSyncRetryExhaustion(t, service.BackendTypeTemporal)
}

func TestAttributeSyncRetryExhaustionCadence(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}
	doTestAttributeSyncRetryExhaustion(t, service.BackendTypeCadence)
}

func TestAttributeSyncInvokeRPCGracefulCompleteTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	postgresDSN := os.Getenv("DEX_ATTRIBUTE_STORE_POSTGRES_DSN")
	if postgresDSN == "" {
		postgresDSN = "postgres://dex:dex@127.0.0.1:55432/dex?sslmode=disable"
	}
	database, err := sql.Open("pgx", postgresDSN)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	tableName := "flow_attributes_rpc_graceful_" + strings.ReplaceAll(newRequestID(), "-", "")
	_, err = database.ExecContext(ctx, fmt.Sprintf(
		`CREATE TABLE public.%s (flow_id TEXT PRIMARY KEY, message TEXT)`,
		tableName,
	))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, dropErr := database.ExecContext(context.Background(), "DROP TABLE IF EXISTS public."+tableName)
		require.NoError(t, dropErr)
	})
	lockTransaction, err := database.BeginTx(ctx, nil)
	require.NoError(t, err)
	lockReleased := false
	t.Cleanup(func() {
		if !lockReleased {
			require.NoError(t, lockTransaction.Rollback())
		}
	})
	_, err = lockTransaction.ExecContext(ctx, "LOCK TABLE public."+tableName+" IN ACCESS EXCLUSIVE MODE")
	require.NoError(t, err)

	runtime := startDexService(t, DexServiceTestConfig{
		BackendType:                            service.BackendTypeTemporal,
		UseTemporalSynchronousUpdateForAllRPCs: true,
		AttributeStore: config.AttributeStoreConfig{
			Stores: map[string]config.AttributeStoreConfigEntry{
				"reporting": {
					Type:      config.AttributeStoreTypePostgres,
					DSN:       postgresDSN,
					TableName: "public." + tableName,
				},
			},
		},
	})
	handler := newFinalizingRPCHandler(true)
	workerTarget := startWorker(t, handler)
	flowID := terminalRPCFlowType + "-graceful-" + newRequestID()
	startResponse, err := runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowID,
		FlowType:           terminalRPCFlowType,
		FlowTimeoutSeconds: 30,
		StartStepType:      terminalRPCFinishStep,
		FlowStartOptions: &dexpb.FlowStartOptions{
			Attributes: []*dexpb.AttributeWrite{
				syncedStringAttribute("message", "graceful-finalization"),
			},
			FlowConfigOverride: &dexpb.FlowConfig{
				AttributeStoreNames: &dexpb.AttributeStoreNames{Names: []string{"reporting"}},
				WorkerTarget:        workerTarget,
			},
		},
	})
	require.NoError(t, err)
	select {
	case <-handler.finishExecuted:
	case <-ctx.Done():
		require.FailNow(t, "graceful step did not start", ctx.Err())
	}
	acceptedResponse, err := runtime.FlowClient.InvokeRPC(ctx, &dexpb.InvokeRPCRequest{
		RequestId: newRequestID(), FlowId: flowID, RpcName: terminalRPCAccepted,
	})
	require.NoError(t, err)
	require.Equal(t, terminalRPCAcceptedOutput, acceptedResponse.GetOutput().GetStringValue())
	close(handler.releaseFinish)
	assertRPCUpdateRejectedAfterStop(t, ctx, runtime.FlowClient, flowID, handler)
	require.NoError(t, lockTransaction.Commit())
	lockReleased = true
	response, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, response.GetFlowStatus())
	assertAcceptedRPCResultInHistory(
		t, ctx, runtime.FlowClient, flowID, startResponse.GetRunId(),
	)
}

func doTestAttributeSync(t *testing.T, backendType service.BackendType) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	postgresDSN := os.Getenv("DEX_ATTRIBUTE_STORE_POSTGRES_DSN")
	if postgresDSN == "" {
		postgresDSN = "postgres://dex:dex@127.0.0.1:55432/dex?sslmode=disable"
	}
	database, err := sql.Open("pgx", postgresDSN)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	tableNames := []string{
		"flow_attributes_" + strings.ReplaceAll(newRequestID(), "-", ""),
		"flow_attributes_audit_" + strings.ReplaceAll(newRequestID(), "-", ""),
	}
	for _, tableName := range tableNames {
		_, err = database.ExecContext(ctx, fmt.Sprintf(
			`CREATE TABLE public.%s (flow_id TEXT PRIMARY KEY, message TEXT, document JSONB)`,
			tableName,
		))
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		for _, tableName := range tableNames {
			_, dropErr := database.ExecContext(context.Background(), "DROP TABLE IF EXISTS public."+tableName)
			require.NoError(t, dropErr)
		}
	})

	runtime := startDexService(t, DexServiceTestConfig{
		BackendType:        backendType,
		S3TestThreshold:    1,
		BlobCacheDirectory: t.TempDir(),
		AttributeStore: config.AttributeStoreConfig{
			Stores: map[string]config.AttributeStoreConfigEntry{
				"reporting": {
					Type:      config.AttributeStoreTypePostgres,
					DSN:       postgresDSN,
					TableName: "public." + tableNames[0],
				},
				"audit": {
					Type:      config.AttributeStoreTypePostgres,
					DSN:       postgresDSN,
					TableName: "public." + tableNames[1],
				},
			},
			SyncBatchSize: 2,
		},
	})
	workerTarget := startWorker(t, signal.NewHandler())
	for _, invalidStoreNames := range [][]string{{"reporting", "reporting"}, {"unknown"}, {""}} {
		_, err = runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
			RequestId:     newRequestID(),
			FlowId:        "attribute-sync-invalid-" + newRequestID(),
			FlowType:      "attribute-sync",
			StartStepType: "start",
			FlowStartOptions: &dexpb.FlowStartOptions{
				FlowConfigOverride: &dexpb.FlowConfig{
					AttributeStoreNames: &dexpb.AttributeStoreNames{Names: invalidStoreNames},
				},
			},
		})
		require.Error(t, err)
	}
	lockTransaction, err := database.BeginTx(ctx, nil)
	require.NoError(t, err)
	lockReleased := false
	t.Cleanup(func() {
		if !lockReleased {
			require.NoError(t, lockTransaction.Rollback())
		}
	})
	for _, tableName := range tableNames {
		_, err = lockTransaction.ExecContext(ctx, "LOCK TABLE public."+tableName+" IN ACCESS EXCLUSIVE MODE")
		require.NoError(t, err)
	}
	flowID := "attribute-sync-" + newRequestID()
	_, err = runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowID,
		FlowType:           "attribute-sync",
		FlowTimeoutSeconds: 30,
		FlowStartOptions: &dexpb.FlowStartOptions{
			Attributes: []*dexpb.AttributeWrite{
				syncedStringAttribute("message", strings.Repeat("cache-backed-string", 8)),
				syncedObjectAttribute("document", `{"source":"blob-cache"}`),
			},
			FlowConfigOverride: &dexpb.FlowConfig{
				AttributeStoreNames: &dexpb.AttributeStoreNames{Names: []string{"reporting", "audit"}},
				WorkerTarget:        workerTarget,
			},
		},
	})
	require.NoError(t, err)

	_, err = runtime.FlowClient.StopFlow(ctx, &dexpb.StopFlowRequest{
		FlowId:   flowID,
		StopType: dexpb.StopType_STOP_TYPE_CANCEL,
	})
	require.NoError(t, err)
	_, err = runtime.FlowClient.SetAttributes(ctx, &dexpb.SetAttributesRequest{
		RequestId: newRequestID(),
		FlowId:    flowID,
		Attributes: []*dexpb.AttributeWrite{
			syncedStringAttribute("message", "terminal-signal"),
		},
	})
	require.NoError(t, err)
	require.NoError(t, lockTransaction.Commit())
	lockReleased = true
	response, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_CANCELED, response.GetFlowStatus())

	for _, tableName := range tableNames {
		var message string
		var document string
		err = database.QueryRowContext(
			ctx,
			"SELECT message, document::text FROM public."+tableName+" WHERE flow_id = $1",
			flowID,
		).Scan(&message, &document)
		require.NoError(t, err)
		require.Equal(t, "terminal-signal", message)
		require.JSONEq(t, `{"source":"blob-cache"}`, document)
	}
}

func doTestAttributeSyncMongoDB(t *testing.T, backendType service.BackendType) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	mongodbDSN := os.Getenv("DEX_ATTRIBUTE_STORE_MONGODB_DSN")
	if mongodbDSN == "" {
		mongodbDSN = "mongodb://127.0.0.1:57017"
	}
	mongodbClient, err := mongo.Connect(options.Client().ApplyURI(mongodbDSN))
	require.NoError(t, err)
	require.NoError(t, mongodbClient.Ping(ctx, nil))
	t.Cleanup(func() { require.NoError(t, mongodbClient.Disconnect(context.Background())) })
	databaseName := "dex"
	collectionName := "flow_attributes_" + strings.ReplaceAll(newRequestID(), "-", "")
	require.NoError(t, mongodbClient.Database(databaseName).CreateCollection(ctx, collectionName))
	t.Cleanup(func() {
		require.NoError(t, mongodbClient.Database(databaseName).Collection(collectionName).Drop(context.Background()))
	})

	runtime := startDexService(t, DexServiceTestConfig{
		BackendType:        backendType,
		S3TestThreshold:    1,
		BlobCacheDirectory: t.TempDir(),
		AttributeStore: config.AttributeStoreConfig{
			Stores: map[string]config.AttributeStoreConfigEntry{
				"documents": {
					Type:           config.AttributeStoreTypeMongoDB,
					DSN:            mongodbDSN,
					DatabaseName:   databaseName,
					CollectionName: collectionName,
				},
			},
			SyncBatchSize: 2,
		},
	})
	workerTarget := startWorker(t, signal.NewHandler())
	flowID := "attribute-sync-mongodb-" + newRequestID()
	_, err = runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowID,
		FlowType:           "attribute-sync-mongodb",
		FlowTimeoutSeconds: 30,
		FlowStartOptions: &dexpb.FlowStartOptions{
			Attributes: []*dexpb.AttributeWrite{
				syncedStringAttribute("message", strings.Repeat("cache-backed-string", 8)),
				syncedObjectAttribute("document", `{"source":"blob-cache","sequence":1}`),
			},
			FlowConfigOverride: &dexpb.FlowConfig{
				AttributeStoreNames: &dexpb.AttributeStoreNames{Names: []string{"documents"}},
				WorkerTarget:        workerTarget,
			},
		},
	})
	require.NoError(t, err)
	_, err = runtime.FlowClient.SetAttributes(ctx, &dexpb.SetAttributesRequest{
		RequestId: newRequestID(),
		FlowId:    flowID,
		Attributes: []*dexpb.AttributeWrite{
			syncedStringAttribute("message", "terminal-signal"),
		},
	})
	require.NoError(t, err)
	_, err = runtime.FlowClient.StopFlow(ctx, &dexpb.StopFlowRequest{
		FlowId:   flowID,
		StopType: dexpb.StopType_STOP_TYPE_CANCEL,
	})
	require.NoError(t, err)
	response, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_CANCELED, response.GetFlowStatus())

	collection := mongodbClient.Database(databaseName).Collection(collectionName)
	require.Eventually(t, func() bool {
		var document struct {
			Message  string `bson:"message"`
			Document struct {
				Source   string `bson:"source"`
				Sequence int64  `bson:"sequence"`
			} `bson:"document"`
		}
		findErr := collection.FindOne(ctx, bson.D{{Key: "_id", Value: flowID}}).Decode(&document)
		if findErr != nil {
			return false
		}
		return document.Message == "terminal-signal" &&
			document.Document.Source == "blob-cache" && document.Document.Sequence == int64(1)
	}, 5*time.Second, 20*time.Millisecond)
}

func doTestAttributeSyncRedis(t *testing.T, backendType service.BackendType) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	redisURL := os.Getenv("DEX_ATTRIBUTE_STORE_REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://127.0.0.1:56379/0"
	}
	redisOptions, err := redis.ParseURL(redisURL)
	require.NoError(t, err)
	redisClient := redis.NewClient(redisOptions)
	require.NoError(t, redisClient.Ping(ctx).Err())
	t.Cleanup(func() { require.NoError(t, redisClient.Close()) })
	keyPrefix := "dex-integ:" + newRequestID() + ":"

	runtime := startDexService(t, DexServiceTestConfig{
		BackendType:        backendType,
		S3TestThreshold:    1,
		BlobCacheDirectory: t.TempDir(),
		AttributeStore: config.AttributeStoreConfig{
			Stores: map[string]config.AttributeStoreConfigEntry{
				"cache": {
					Type:      config.AttributeStoreTypeRedis,
					DSN:       redisURL,
					KeyPrefix: keyPrefix,
				},
			},
			SyncBatchSize: 2,
		},
	})
	workerTarget := startWorker(t, signal.NewHandler())
	flowID := "attribute-sync-redis-" + newRequestID()
	t.Cleanup(func() { require.NoError(t, redisClient.Del(context.Background(), keyPrefix+flowID).Err()) })
	_, err = runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowID,
		FlowType:           "attribute-sync-redis",
		FlowTimeoutSeconds: 30,
		FlowStartOptions: &dexpb.FlowStartOptions{
			Attributes: []*dexpb.AttributeWrite{
				syncedStringAttribute("message", strings.Repeat("cache-backed-string", 8)),
				syncedObjectAttribute("document", `{"source":"blob-cache","sequence":1}`),
			},
			FlowConfigOverride: &dexpb.FlowConfig{
				AttributeStoreNames: &dexpb.AttributeStoreNames{Names: []string{"cache"}},
				WorkerTarget:        workerTarget,
			},
		},
	})
	require.NoError(t, err)
	_, err = runtime.FlowClient.SetAttributes(ctx, &dexpb.SetAttributesRequest{
		RequestId: newRequestID(),
		FlowId:    flowID,
		Attributes: []*dexpb.AttributeWrite{
			syncedStringAttribute("message", "terminal-signal"),
		},
	})
	require.NoError(t, err)
	_, err = runtime.FlowClient.StopFlow(ctx, &dexpb.StopFlowRequest{
		FlowId:   flowID,
		StopType: dexpb.StopType_STOP_TYPE_CANCEL,
	})
	require.NoError(t, err)
	response, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_CANCELED, response.GetFlowStatus())

	require.Eventually(t, func() bool {
		fields, readErr := redisClient.HGetAll(ctx, keyPrefix+flowID).Result()
		if readErr != nil || fields["message"] != `"terminal-signal"` {
			return false
		}
		var document struct {
			Source   string `json:"source"`
			Sequence int64  `json:"sequence"`
		}
		if decodeErr := json.Unmarshal([]byte(fields["document"]), &document); decodeErr != nil {
			return false
		}
		return document.Source == "blob-cache" && document.Sequence == 1
	}, 5*time.Second, 20*time.Millisecond)
}

func doTestAttributeSyncFlowTimeout(t *testing.T, backendType service.BackendType) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	postgresDSN := os.Getenv("DEX_ATTRIBUTE_STORE_POSTGRES_DSN")
	if postgresDSN == "" {
		postgresDSN = "postgres://dex:dex@127.0.0.1:55432/dex?sslmode=disable"
	}
	database, err := sql.Open("pgx", postgresDSN)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	tableName := "flow_attributes_timeout_" + strings.ReplaceAll(newRequestID(), "-", "")
	_, err = database.ExecContext(ctx, fmt.Sprintf(
		`CREATE TABLE public.%s (flow_id TEXT PRIMARY KEY, message TEXT)`,
		tableName,
	))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, dropErr := database.ExecContext(context.Background(), "DROP TABLE IF EXISTS public."+tableName)
		require.NoError(t, dropErr)
	})

	runtime := startDexService(t, DexServiceTestConfig{
		BackendType: backendType,
		AttributeStore: config.AttributeStoreConfig{
			Stores: map[string]config.AttributeStoreConfigEntry{
				"reporting": {
					Type:      config.AttributeStoreTypePostgres,
					DSN:       postgresDSN,
					TableName: "public." + tableName,
				},
			},
		},
	})
	handler := &timeoutAttributeSyncHandler{started: make(chan struct{}, 1)}
	workerTarget := startWorker(t, handler)
	lockTransaction, err := database.BeginTx(ctx, nil)
	require.NoError(t, err)
	lockReleased := false
	t.Cleanup(func() {
		if !lockReleased {
			require.NoError(t, lockTransaction.Rollback())
		}
	})
	_, err = lockTransaction.ExecContext(ctx, "LOCK TABLE public."+tableName+" IN ACCESS EXCLUSIVE MODE")
	require.NoError(t, err)

	flowID := "attribute-sync-timeout-" + newRequestID()
	_, err = runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowID,
		FlowType:           "attribute-sync-timeout",
		FlowTimeoutSeconds: 10,
		StartStepType:      signal.State1,
		FlowStartOptions: &dexpb.FlowStartOptions{
			Attributes: []*dexpb.AttributeWrite{
				syncedStringAttribute("message", "persisted-after-timeout"),
			},
			FlowConfigOverride: &dexpb.FlowConfig{
				AttributeStoreNames: &dexpb.AttributeStoreNames{Names: []string{"reporting"}},
				WorkerTarget:        workerTarget,
			},
		},
	})
	require.NoError(t, err)
	select {
	case <-handler.started:
	case <-ctx.Done():
		require.FailNow(t, "Flow Step did not start", ctx.Err())
	}
	probe := timeoutAttributeSyncProbe{ctx: ctx, runtime: runtime, flowID: flowID}
	require.Eventually(t, probe.isStepCanceled, 15*time.Second, 50*time.Millisecond)
	require.NoError(t, lockTransaction.Commit())
	lockReleased = true

	response, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_FAILED, response.GetFlowStatus())
	require.Equal(t, dexpb.FlowErrorType_FLOW_ERROR_TYPE_FLOW_TIMEOUT, response.GetErrorType())
	var message string
	err = database.QueryRowContext(
		ctx,
		"SELECT message FROM public."+tableName+" WHERE flow_id = $1",
		flowID,
	).Scan(&message)
	require.NoError(t, err)
	require.Equal(t, "persisted-after-timeout", message)
}

type timeoutAttributeSyncHandler struct {
	dexpb.UnimplementedWorkerServiceServer
	started chan struct{}
}

func (h *timeoutAttributeSyncHandler) InvokeWaitForMethod(
	ctx context.Context,
	_ *dexpb.InvokeWaitForMethodRequest,
) (*dexpb.InvokeWaitForMethodResponse, error) {
	select {
	case h.started <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &dexpb.InvokeWaitForMethodResponse{
		WaitingCondition: &dexpb.WaitingCondition{
			WaitingConditionType: dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ALL_COMPLETED,
			ChannelConditions: []*dexpb.ChannelCondition{
				{ChannelName: "never-published"},
			},
		},
	}, nil
}

type timeoutAttributeSyncProbe struct {
	ctx     context.Context
	runtime *integRuntime
	flowID  string
}

func (p timeoutAttributeSyncProbe) isStepCanceled() bool {
	var dump dexpb.DebugDumpResponse
	if err := p.runtime.UnifiedClient.QueryWorkflow(
		p.ctx,
		&dump,
		p.flowID,
		"",
		service.DebugDumpQueryType,
	); err != nil {
		return false
	}
	for _, execution := range dump.GetActiveStepExecutions() {
		if execution.GetStepType() == signal.State1 {
			return false
		}
	}
	return true
}

func doTestAttributeSyncRetryExhaustion(t *testing.T, backendType service.BackendType) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	postgresDSN := os.Getenv("DEX_ATTRIBUTE_STORE_POSTGRES_DSN")
	if postgresDSN == "" {
		postgresDSN = "postgres://dex:dex@127.0.0.1:55432/dex?sslmode=disable"
	}
	database, err := sql.Open("pgx", postgresDSN)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	tableSuffix := strings.ReplaceAll(newRequestID(), "-", "")
	failingTable := "flow_attributes_failing_" + tableSuffix
	healthyTable := "flow_attributes_healthy_" + tableSuffix
	for _, tableName := range []string{failingTable, healthyTable} {
		_, err = database.ExecContext(ctx, fmt.Sprintf(
			`CREATE TABLE public.%s (flow_id TEXT PRIMARY KEY, message TEXT)`,
			tableName,
		))
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		for _, tableName := range []string{failingTable, healthyTable} {
			_, dropErr := database.ExecContext(context.Background(), "DROP TABLE IF EXISTS public."+tableName)
			require.NoError(t, dropErr)
		}
	})

	runtime := startDexService(t, DexServiceTestConfig{
		BackendType: backendType,
		AttributeStore: config.AttributeStoreConfig{
			Stores: map[string]config.AttributeStoreConfigEntry{
				"failing": {
					Type:      config.AttributeStoreTypePostgres,
					DSN:       postgresDSN,
					TableName: "public." + failingTable,
				},
				"healthy": {
					Type:      config.AttributeStoreTypePostgres,
					DSN:       postgresDSN,
					TableName: "public." + healthyTable,
				},
			},
			SyncBatchSize:      1,
			SyncAttemptTimeout: time.Second,
			SyncRetryPolicy: &config.RetryPolicy{
				InitialInterval:    time.Second,
				MaximumInterval:    time.Second,
				BackoffCoefficient: 1,
				TotalDuration:      time.Second,
			},
		},
	})
	workerTarget := startWorker(t, signal.NewHandler())
	_, err = database.ExecContext(ctx, "DROP TABLE public."+failingTable)
	require.NoError(t, err)

	flowID := "attribute-sync-retry-" + newRequestID()
	_, err = runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowID,
		FlowType:           "attribute-sync-retry",
		FlowTimeoutSeconds: 25,
		FlowStartOptions: &dexpb.FlowStartOptions{
			Attributes: []*dexpb.AttributeWrite{
				syncedStringAttribute("message", "skipped"),
			},
			FlowConfigOverride: &dexpb.FlowConfig{
				AttributeStoreNames: &dexpb.AttributeStoreNames{Names: []string{"failing"}},
				WorkerTarget:        workerTarget,
			},
		},
	})
	require.NoError(t, err)

	_, err = runtime.FlowClient.UpdateFlowConfig(ctx, &dexpb.UpdateFlowConfigRequest{
		FlowId: flowID,
		FlowConfig: &dexpb.FlowConfig{
			AttributeStoreNames: &dexpb.AttributeStoreNames{Names: []string{"healthy", "healthy"}},
		},
	})
	require.Error(t, err)

	_, err = runtime.FlowClient.UpdateFlowConfig(ctx, &dexpb.UpdateFlowConfigRequest{
		FlowId: flowID,
		FlowConfig: &dexpb.FlowConfig{
			AttributeStoreNames: &dexpb.AttributeStoreNames{Names: []string{"healthy"}},
		},
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		var dump dexpb.DebugDumpResponse
		queryErr := runtime.UnifiedClient.QueryWorkflow(ctx, &dump, flowID, "", service.DebugDumpQueryType)
		return queryErr == nil && len(dump.GetConfig().GetAttributeStoreNames().GetNames()) == 1 &&
			dump.GetConfig().GetAttributeStoreNames().GetNames()[0] == "healthy"
	}, 20*time.Second, 50*time.Millisecond)

	_, err = runtime.FlowClient.SetAttributes(ctx, &dexpb.SetAttributesRequest{
		RequestId: newRequestID(),
		FlowId:    flowID,
		Attributes: []*dexpb.AttributeWrite{
			syncedStringAttribute("message", "persisted"),
		},
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		var persisted string
		queryErr := database.QueryRowContext(
			ctx,
			"SELECT message FROM public."+healthyTable+" WHERE flow_id = $1",
			flowID,
		).Scan(&persisted)
		return queryErr == nil && persisted == "persisted"
	}, 20*time.Second, 50*time.Millisecond)

	_, err = runtime.FlowClient.StopFlow(ctx, &dexpb.StopFlowRequest{
		FlowId:   flowID,
		StopType: dexpb.StopType_STOP_TYPE_CANCEL,
	})
	require.NoError(t, err)
	response, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_CANCELED, response.GetFlowStatus())

	var message string
	err = database.QueryRowContext(
		ctx,
		"SELECT message FROM public."+healthyTable+" WHERE flow_id = $1",
		flowID,
	).Scan(&message)
	require.NoError(t, err)
	require.Equal(t, "persisted", message)
}

func syncedStringAttribute(key, value string) *dexpb.AttributeWrite {
	return &dexpb.AttributeWrite{
		Key:        key,
		Value:      stringValue(value),
		SyncConfig: &dexpb.AttributeSyncConfig{Enabled: true},
	}
}

func syncedObjectAttribute(key, value string) *dexpb.AttributeWrite {
	return &dexpb.AttributeWrite{
		Key:        key,
		Value:      objJSONValue(value),
		SyncConfig: &dexpb.AttributeSyncConfig{Enabled: true},
	}
}
