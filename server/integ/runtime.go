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
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/api"
	"github.com/superdurable/dex/service/bootstrap"
	uclient "github.com/superdurable/dex/service/client"
	cadenceapi "github.com/superdurable/dex/service/client/cadence"
	temporalapi "github.com/superdurable/dex/service/client/temporal"
	"github.com/superdurable/dex/service/common/attributestore"
	"github.com/superdurable/dex/service/common/blobstore"
	dexconverter "github.com/superdurable/dex/service/common/converter"
	"github.com/superdurable/dex/service/common/log"
	"github.com/superdurable/dex/service/common/log/loggerimpl"
	"github.com/superdurable/dex/service/common/ptr"
	"github.com/superdurable/dex/service/common/streamstore"
	"github.com/superdurable/dex/service/common/workerclient"
	"github.com/superdurable/dex/service/indexsync"
	"github.com/superdurable/dex/service/interpreter/cadence"
	"github.com/superdurable/dex/service/interpreter/temporal"
	"github.com/superdurable/dex/service/tenancy"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// integRuntime is the in-process gRPC Dex stack started for one test.
type integRuntime struct {
	FlowClient    dexpb.FlowServiceClient
	UnifiedClient uclient.UnifiedClient
	BlobStore     blobstore.BlobStore

	defaultFlowConfig *dexpb.FlowConfig

	internalDumpCapture *internalDumpHeaderCapture

	// PublicAddress serves FlowService; InternalAddress serves InternalService only in multi-tenant mode.
	PublicAddress   string
	InternalAddress string
}

type internalDumpHeaderCapture struct {
	mu      sync.Mutex
	headers []metadata.MD
}

func (capture *internalDumpHeaderCapture) append(incoming metadata.MD) {
	capture.mu.Lock()
	capture.headers = append(capture.headers, incoming)
	capture.mu.Unlock()
}

func (capture *internalDumpHeaderCapture) snapshot() []metadata.MD {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	return append([]metadata.MD(nil), capture.headers...)
}

func newInternalDumpHeaderCaptureInterceptor(
	capture *internalDumpHeaderCapture,
) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		if info.FullMethod == dexpb.InternalService_DumpFlowForContinueAsNew_FullMethodName {
			captured := metadata.MD{}
			if incomingMetadata, ok := metadata.FromIncomingContext(ctx); ok {
				captured = incomingMetadata.Copy()
			}
			capture.append(captured)
		}
		return handler(ctx, req)
	}
}

type interpreterWorker interface {
	Start() error
	StartWithStickyCacheDisabledForTest() error
	Close()
}

type resultOnlyWaitForWorker interface {
	InvokeWaitForMethod(context.Context, *dexpb.InvokeWaitForMethodRequest) (
		*dexpb.InvokeWaitForMethodResponse,
		error,
	)
}

type resultOnlyExecuteWorker interface {
	InvokeExecuteMethod(context.Context, *dexpb.InvokeExecuteMethodRequest) (
		*dexpb.InvokeExecuteMethodResponse,
		error,
	)
}

type resultOnlyRPCWorker interface {
	InvokeWorkerRPC(context.Context, *dexpb.InvokeWorkerRPCRequest) (
		*dexpb.InvokeWorkerRPCResponse,
		error,
	)
}

type resultOnlyWorkerAdapter struct {
	dexpb.UnimplementedWorkerServiceServer
	handler any
}

func (adapter *resultOnlyWorkerAdapter) InvokeWaitForMethod(
	req *dexpb.InvokeWaitForMethodRequest,
	stream grpc.ServerStreamingServer[dexpb.InvokeWaitForMethodOutput],
) error {
	handler, ok := adapter.handler.(resultOnlyWaitForWorker)
	if !ok {
		return status.Error(codes.Unimplemented, "WaitFor is not implemented")
	}
	response, err := handler.InvokeWaitForMethod(stream.Context(), req)
	if err != nil {
		return err
	}
	return stream.Send(&dexpb.InvokeWaitForMethodOutput{
		Output: &dexpb.InvokeWaitForMethodOutput_Result{Result: response},
	})
}

func (adapter *resultOnlyWorkerAdapter) InvokeExecuteMethod(
	req *dexpb.InvokeExecuteMethodRequest,
	stream grpc.ServerStreamingServer[dexpb.InvokeExecuteMethodOutput],
) error {
	handler, ok := adapter.handler.(resultOnlyExecuteWorker)
	if !ok {
		return status.Error(codes.Unimplemented, "Execute is not implemented")
	}
	response, err := handler.InvokeExecuteMethod(stream.Context(), req)
	if err != nil {
		return err
	}
	return stream.Send(&dexpb.InvokeExecuteMethodOutput{
		Output: &dexpb.InvokeExecuteMethodOutput_Result{Result: response},
	})
}

func (adapter *resultOnlyWorkerAdapter) InvokeWorkerRPC(
	ctx context.Context,
	req *dexpb.InvokeWorkerRPCRequest,
) (*dexpb.InvokeWorkerRPCResponse, error) {
	handler, ok := adapter.handler.(resultOnlyRPCWorker)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "Worker RPC is not implemented")
	}
	return handler.InvokeWorkerRPC(ctx, req)
}

func startWorker(t *testing.T, handler any) *dexpb.WorkerTarget {
	t.Helper()
	if streamingHandler, ok := handler.(dexpb.WorkerServiceServer); ok {
		return startStreamingWorker(t, streamingHandler)
	}
	_, hasWaitFor := handler.(resultOnlyWaitForWorker)
	_, hasExecute := handler.(resultOnlyExecuteWorker)
	_, hasRPC := handler.(resultOnlyRPCWorker)
	require.True(t, hasWaitFor || hasExecute || hasRPC,
		"worker handler must implement streaming or result-only methods")
	return startStreamingWorker(t, &resultOnlyWorkerAdapter{handler: handler})
}

func startStreamingWorker(
	t *testing.T,
	handler dexpb.WorkerServiceServer,
	options ...grpc.ServerOption,
) *dexpb.WorkerTarget {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// Match Api.EffectiveGrpcMaxMessageBytes; bare grpc.NewServer defaults to 4MiB.
	workerOptions := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(config.DefaultGrpcMaxMessageBytes),
		grpc.MaxSendMsgSize(config.DefaultGrpcMaxMessageBytes),
	}
	server := grpc.NewServer(append(workerOptions, options...)...)
	dexpb.RegisterWorkerServiceServer(server, handler)
	serveError := make(chan error, 1)
	go func() {
		serveError <- server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.GracefulStop()
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			require.NoError(t, err)
		}
		if err := <-serveError; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			require.NoError(t, err)
		}
	})
	return &dexpb.WorkerTarget{Address: listener.Addr().String()}
}

func withWorkerTarget(
	options *dexpb.FlowStartOptions,
	workerTarget *dexpb.WorkerTarget,
) *dexpb.FlowStartOptions {
	if options == nil {
		options = &dexpb.FlowStartOptions{}
	}
	if options.FlowConfigOverride == nil {
		options.FlowConfigOverride = &dexpb.FlowConfig{}
	}
	options.FlowConfigOverride.WorkerTarget = workerTarget
	return options
}

// startDexService returns clients, starting API and interpreter unless an external Dex address is configured.
func startDexService(t *testing.T, testConfig DexServiceTestConfig) *integRuntime {
	t.Helper()
	if *dexServerAddress != "" {
		return connectToExternalDexService(t, testConfig)
	}
	return startInProcessDexService(t, testConfig)
}

func startInProcessDexService(t *testing.T, testConfig DexServiceTestConfig) *integRuntime {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	cfg := createTestConfig(t, testConfig)
	cfg.Interpreter.InterpreterActivityConfig.InternalServiceTarget = listener.Addr().String()
	var internalListener net.Listener
	if cfg.Tenancy.IsMultiTenant() {
		internalListener, err = net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		cfg.Tenancy.Internal.Listen = internalListener.Addr().String()
		cfg.Interpreter.InterpreterActivityConfig.InternalServiceTarget = ""
		require.NoError(t, cfg.ValidateTenancy())
	}
	workerPool, err := workerclient.NewWorkerClientPool(&cfg)
	require.NoError(t, err)
	t.Cleanup(workerPool.Close)
	logger, err := loggerimpl.NewDevelopment()
	require.NoError(t, err)
	s3Client, err := bootstrap.CreateS3Client(context.Background(), &cfg)
	require.NoError(t, err)
	attributeStore, err := attributestore.NewManager(context.Background(), &cfg.AttributeStore, logger)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, attributeStore.Close()) })
	streamStore, err := streamstore.New(&cfg.StreamStore, logger)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, streamStore.Close()) })

	var worker interpreterWorker
	var unifiedClient uclient.UnifiedClient
	var store blobstore.BlobStore
	switch testConfig.BackendType {
	case service.BackendTypeTemporal:
		dataConverter := dexconverter.NewTemporalDataConverter()
		if testConfig.MemoEncryption {
			dataConverter = encryptionDataConverter
		}
		metricsHandler := testConfig.TemporalMetricsHandler
		if metricsHandler == nil {
			metricsHandler = client.MetricsNopHandler
		}
		temporalClient := createTemporalClient(t, dataConverter, metricsHandler)
		store, err = blobstore.NewBlobStore(
			s3Client,
			testNamespace,
			&cfg.BlobStore,
			logger,
			client.MetricsNopHandler,
		)
		require.NoError(t, err)
		unifiedClient = temporalapi.NewTemporalClient(
			temporalClient,
			testNamespace,
			dataConverter,
			testConfig.MemoEncryption,
			cfg.Api.QueryWorkflowFailedRetryPolicy,
			nil,
		)
		worker = temporal.NewInterpreterWorker(
			&cfg,
			temporalClient,
			service.TaskQueue,
			dataConverter,
			unifiedClient,
			store,
			attributeStore,
			workerPool,
			streamStore,
			metricsHandler,
		)
	case service.BackendTypeCadence:
		domain := testConfig.CadenceDomain
		if domain == "" {
			domain = bootstrap.DefaultCadenceDomain
		}
		serviceClient, adminClient, closeServiceClient, err := bootstrap.BuildCadenceServiceClient(
			bootstrap.DefaultCadenceHostPort,
		)
		require.NoError(t, err)
		dataConverter := dexconverter.NewCadenceDataConverter()
		cadenceClient, err := bootstrap.BuildCadenceClient(
			serviceClient,
			domain,
			dataConverter,
		)
		require.NoError(t, err)
		store, err = blobstore.NewBlobStore(
			s3Client,
			domain,
			&cfg.BlobStore,
			logger,
			client.MetricsNopHandler,
		)
		require.NoError(t, err)
		unifiedClient = cadenceapi.NewCadenceClient(
			domain,
			cadenceClient,
			serviceClient,
			adminClient,
			cfg.Interpreter.Cadence.AdminSecurityToken,
			dataConverter,
			closeServiceClient,
			cfg.Api.QueryWorkflowFailedRetryPolicy,
		)
		worker = cadence.NewInterpreterWorker(
			&cfg,
			serviceClient,
			domain,
			service.TaskQueue,
			closeServiceClient,
			dataConverter,
			unifiedClient,
			store,
			attributeStore,
			workerPool,
			streamStore,
			client.MetricsNopHandler,
		)
	default:
		require.FailNow(t, "unsupported backend", testConfig.BackendType)
	}
	if store != nil {
		t.Cleanup(func() { require.NoError(t, store.Close()) })
	}
	attributeIndexes := map[string]dexpb.IndexType{
		service.SearchAttributeDexWorkflowType: dexpb.IndexType_INDEX_TYPE_KEYWORD,
		service.SearchAttributeDexParentFlowID: dexpb.IndexType_INDEX_TYPE_KEYWORD,
		service.SearchAttributeActiveStepTypes: dexpb.IndexType_INDEX_TYPE_KEYWORD_ARRAY,
	}
	if cfg.Tenancy.IsMultiTenant() {
		attributeIndexes[tenancy.SearchAttributeDexTenantID] = dexpb.IndexType_INDEX_TYPE_KEYWORD
	}
	for name, indexType := range localDexDevTestAttributeIndexes {
		attributeIndexes[name] = indexType
	}
	syncCtx, cancelSync := context.WithTimeout(
		context.Background(),
		cfg.Interpreter.EffectiveAttributeIndexSyncTimeout(),
	)
	// The operator registers indexes outside Dex in multi-tenant mode; the test plays that role.
	operatorInterpreterConfig := cfg.Interpreter
	operatorInterpreterConfig.AttributeIndexesManagedExternally = false
	err = indexsync.New(&operatorInterpreterConfig, unifiedClient, logger).Sync(syncCtx, attributeIndexes)
	cancelSync()
	require.NoError(t, err)
	startInterpreter(t, worker)
	internalDumpCapture := &internalDumpHeaderCapture{}
	runtime := &integRuntime{
		defaultFlowConfig:   cfg.Interpreter.DefaultWorkflowConfig,
		internalDumpCapture: internalDumpCapture,
	}
	previousDumpObserver := api.DumpFlowForContinueAsNewHeaderObserver
	api.DumpFlowForContinueAsNewHeaderObserver = func(ctx context.Context) {
		if incomingMetadata, ok := metadata.FromIncomingContext(ctx); ok {
			internalDumpCapture.append(incomingMetadata.Copy())
		}
	}
	t.Cleanup(func() {
		api.DumpFlowForContinueAsNewHeaderObserver = previousDumpObserver
	})
	startApiServer(
		t,
		listener,
		internalListener,
		&cfg,
		unifiedClient,
		logger,
		store,
		attributeStore,
		streamStore,
		worker.Close,
		workerPool,
		newInternalDumpHeaderCaptureInterceptor(internalDumpCapture),
	)

	runtime.PublicAddress = listener.Addr().String()
	if internalListener != nil {
		runtime.InternalAddress = internalListener.Addr().String()
	}

	globalBlobStore = store
	runtime.FlowClient = dexpb.NewFlowServiceClient(newDexClientConnectionWithCredential(
		t,
		runtime.PublicAddress,
		testConfig.Credential,
		cfg.Api.EffectiveGrpcMaxMessageBytes(),
	))
	runtime.UnifiedClient = unifiedClient
	runtime.BlobStore = store
	return runtime
}

func (runtime *integRuntime) requireInternalDumpHeaders(
	t *testing.T,
	headerKey string,
	headerValue string,
) {
	t.Helper()
	captured := runtime.internalDumpCapture.snapshot()

	require.NotEmpty(t, captured, "expected at least one DumpFlowForContinueAsNew call")
	found := false
	for _, capturedMetadata := range captured {
		values := capturedMetadata.Get(headerKey)
		if len(values) > 0 && values[0] == headerValue {
			found = true
			break
		}
	}
	require.True(
		t,
		found,
		"DumpFlowForContinueAsNew metadata missing %q=%q, captured: %v",
		headerKey,
		headerValue,
		captured,
	)
}

// globalBlobStore is set by startDexService for S3 cleanup tests that need the store.
var globalBlobStore blobstore.BlobStore

func startInterpreter(t *testing.T, worker interpreterWorker) {
	t.Helper()
	if *disableStickyCache {
		require.NoError(t, worker.StartWithStickyCacheDisabledForTest())
		return
	}
	require.NoError(t, worker.Start())
}

// newFlowClientWithCredential dials FlowService; a non-empty credential is sent as a bearer token on every call.
func newFlowClientWithCredential(t *testing.T, address string, credential string) dexpb.FlowServiceClient {
	t.Helper()
	connection := newDexClientConnectionWithCredential(t, address, credential, config.DefaultGrpcMaxMessageBytes)
	return dexpb.NewFlowServiceClient(connection)
}

func newDexClientConnectionWithCredential(
	t *testing.T,
	address string,
	credential string,
	maxMessageBytes int,
) *grpc.ClientConn {
	t.Helper()
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMessageBytes),
			grpc.MaxCallSendMsgSize(maxMessageBytes),
		),
	}
	if credential != "" {
		options = append(options, grpc.WithPerRPCCredentials(bearerCredential(credential)))
	}
	connection, err := grpc.NewClient(address, options...)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, connection.Close())
	})
	return connection
}

type bearerCredential string

func (credential bearerCredential) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + string(credential)}, nil
}

func (credential bearerCredential) RequireTransportSecurity() bool {
	return false
}

func startApiServer(
	t *testing.T,
	listener net.Listener,
	internalListener net.Listener,
	cfg *config.Config,
	unifiedClient uclient.UnifiedClient,
	logger log.Logger,
	store blobstore.BlobStore,
	attributeStore *attributestore.Manager,
	streamStore *streamstore.Store,
	closeInterpreter func(),
	workerPool *workerclient.WorkerClientPool,
	extraUnaryInterceptors ...grpc.UnaryServerInterceptor,
) {
	t.Helper()

	server, err := api.NewServer(
		&cfg.Api,
		&cfg.Tenancy,
		&cfg.BlobStore,
		&cfg.Interpreter,
		unifiedClient,
		logger,
		store,
		attributeStore,
		streamStore,
		func(context.Context) error { return nil },
		workerPool,
		extraUnaryInterceptors...,
	)
	require.NoError(t, err)
	listeners := []net.Listener{listener}
	serveError := make(chan error, 2)
	go func() {
		serveError <- server.Serve(listener)
	}()
	if internalListener != nil {
		listeners = append(listeners, internalListener)
		go func() {
			serveError <- server.ServeInternal(internalListener)
		}()
	}
	t.Cleanup(func() {
		closeInterpreter()
		server.GracefulStop(2 * time.Second)
		for _, serverListener := range listeners {
			if err := serverListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				require.NoError(t, err)
			}
			if err := <-serveError; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				require.NoError(t, err)
			}
		}
	})
}

func createTemporalClient(
	t *testing.T,
	dataConverter converter.DataConverter,
	metricsHandler client.MetricsHandler,
) client.Client {
	t.Helper()
	temporalClient, err := client.Dial(client.Options{
		HostPort:       *temporalHostPort,
		Namespace:      testNamespace,
		DataConverter:  dataConverter,
		MetricsHandler: metricsHandler,
	})
	require.NoError(t, err)
	return temporalClient
}

func grpcServiceErrorResponse(t *testing.T, err error) *dexpb.ServiceErrorResponse {
	t.Helper()
	require.Error(t, err)
	statusError, ok := status.FromError(err)
	require.True(t, ok)
	for _, detail := range statusError.Details() {
		if response, ok := detail.(*dexpb.ServiceErrorResponse); ok {
			return response
		}
	}
	require.FailNow(t, "gRPC error has no ServiceErrorResponse details", err)
	return nil
}

func smallWaitForFastTest() {
	duration := time.Millisecond * time.Duration(*repeatInterval)
	if *repeatIntegTest == 0 {
		duration = time.Millisecond
	}
	time.Sleep(duration)
}

func minimumContinueAsNewAsyncDurabilityConfig() *dexpb.FlowConfig {
	config := asyncDurabilityConfig()
	config.ContinueAsNewThreshold = ptr.Any(int32(1))
	return config
}

func minimumContinueAsNewSyncDurabilityConfig() *dexpb.FlowConfig {
	config := syncDurabilityConfig()
	config.ContinueAsNewThreshold = ptr.Any(int32(1))
	return config
}

func asyncDurabilityConfig() *dexpb.FlowConfig {
	return &dexpb.FlowConfig{
		StepDurability: ptr.Any(dexpb.StepDurability_STEP_DURABILITY_ASYNC),
	}
}

func syncDurabilityConfig() *dexpb.FlowConfig {
	return &dexpb.FlowConfig{
		StepDurability: ptr.Any(dexpb.StepDurability_STEP_DURABILITY_SYNC),
	}
}

func copyFlowConfigForMutation(flowConfig *dexpb.FlowConfig) *dexpb.FlowConfig {
	return &dexpb.FlowConfig{
		ActiveStepSearchMode:         flowConfig.ActiveStepSearchMode,
		ContinueAsNewThreshold:       flowConfig.ContinueAsNewThreshold,
		ContinueAsNewPageSizeInBytes: flowConfig.ContinueAsNewPageSizeInBytes,
		StepDurability:               flowConfig.StepDurability,
		WorkerTarget:                 flowConfig.WorkerTarget,
	}
}

func encodedObjectValue(encoding string, payload []byte) *dexpb.Value {
	return &dexpb.Value{
		Kind: &dexpb.Value_ObjValue{
			ObjValue: &dexpb.EncodedObject{
				Encoding: encoding,
				Payload:  payload,
			},
		},
	}
}

func getBackendTypes() []service.BackendType {
	backends := []service.BackendType{}
	if *temporalIntegTest {
		backends = append(backends, service.BackendTypeTemporal)
	}
	if *cadenceIntegTest {
		backends = append(backends, service.BackendTypeCadence)
	}
	return backends
}

func jsonObjValue(payload any) *dexpb.Value {
	bytes, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return &dexpb.Value{
		Kind: &dexpb.Value_ObjValue{
			ObjValue: &dexpb.EncodedObject{
				Encoding: "json",
				Payload:  bytes,
			},
		},
	}
}

func stringValue(value string) *dexpb.Value {
	return &dexpb.Value{Kind: &dexpb.Value_StringValue{StringValue: value}}
}

func intValue(value int64) *dexpb.Value {
	return &dexpb.Value{Kind: &dexpb.Value_IntValue{IntValue: value}}
}

func boolValue(value bool) *dexpb.Value {
	return &dexpb.Value{Kind: &dexpb.Value_BoolValue{BoolValue: value}}
}

func doubleValue(value float64) *dexpb.Value {
	return &dexpb.Value{Kind: &dexpb.Value_DoubleValue{DoubleValue: value}}
}

func nullValue() *dexpb.Value {
	return &dexpb.Value{
		Kind: &dexpb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE},
	}
}

func objJSONValue(payload string) *dexpb.Value {
	return &dexpb.Value{
		Kind: &dexpb.Value_ObjValue{
			ObjValue: &dexpb.EncodedObject{
				Encoding: "json",
				Payload:  []byte(payload),
			},
		},
	}
}

func indexedKeywordAttribute(key, value string) *dexpb.AttributeWrite {
	return &dexpb.AttributeWrite{
		Key:   key,
		Value: stringValue(value),
		IndexConfig: &dexpb.IndexConfig{
			Enable: true,
			Type:   dexpb.IndexType_INDEX_TYPE_KEYWORD,
		},
	}
}

func indexedTextAttribute(key, value string) *dexpb.AttributeWrite {
	return &dexpb.AttributeWrite{
		Key:   key,
		Value: stringValue(value),
		IndexConfig: &dexpb.IndexConfig{
			Enable: true,
			Type:   dexpb.IndexType_INDEX_TYPE_TEXT,
		},
	}
}

func indexedIntAttribute(key string, value int64) *dexpb.AttributeWrite {
	return &dexpb.AttributeWrite{
		Key:   key,
		Value: intValue(value),
		IndexConfig: &dexpb.IndexConfig{
			Enable: true,
			Type:   dexpb.IndexType_INDEX_TYPE_INT,
		},
	}
}

func indexedDoubleAttribute(key string, value float64) *dexpb.AttributeWrite {
	return &dexpb.AttributeWrite{
		Key:   key,
		Value: doubleValue(value),
		IndexConfig: &dexpb.IndexConfig{
			Enable: true,
			Type:   dexpb.IndexType_INDEX_TYPE_DOUBLE,
		},
	}
}

func indexedBoolAttribute(key string, value bool) *dexpb.AttributeWrite {
	return &dexpb.AttributeWrite{
		Key:   key,
		Value: boolValue(value),
		IndexConfig: &dexpb.IndexConfig{
			Enable: true,
			Type:   dexpb.IndexType_INDEX_TYPE_BOOL,
		},
	}
}

func indexedDatetimeAttribute(key, value string) *dexpb.AttributeWrite {
	return &dexpb.AttributeWrite{
		Key:   key,
		Value: stringValue(value),
		IndexConfig: &dexpb.IndexConfig{
			Enable: true,
			Type:   dexpb.IndexType_INDEX_TYPE_DATETIME,
		},
	}
}

func indexedKeywordArrayAttribute(key string, values ...string) *dexpb.AttributeWrite {
	return &dexpb.AttributeWrite{
		Key:   key,
		Value: jsonObjValue(values),
		IndexConfig: &dexpb.IndexConfig{
			Enable: true,
			Type:   dexpb.IndexType_INDEX_TYPE_KEYWORD_ARRAY,
		},
	}
}

func dataObjectAttribute(key, jsonPayload string) *dexpb.AttributeWrite {
	return &dexpb.AttributeWrite{
		Key:   key,
		Value: objJSONValue(jsonPayload),
	}
}

func assertSearchFlows(
	t *testing.T,
	flowClient dexpb.FlowServiceClient,
	query string,
	expectedCount int,
	includeContinuedAsNew bool,
) {
	t.Helper()
	assertions := require.New(t)
	ctx := context.Background()

	if expectedCount == 0 {
		searchResp, err := flowClient.SearchFlows(ctx, &dexpb.SearchFlowsRequest{
			IncludeContinuedAsNew: includeContinuedAsNew,
			Query:                 query,
			PageSize:              2,
		})
		require.NoError(t, err)
		assertions.Empty(searchResp.GetFlowRuns(), "expected zero results for query %v", query)
		assertions.Empty(searchResp.GetNextPageToken())
		return
	}

	var nextPageToken string
	currentCount := 0
	for currentCount < expectedCount {
		searchResp, err := flowClient.SearchFlows(ctx, &dexpb.SearchFlowsRequest{
			IncludeContinuedAsNew: includeContinuedAsNew,
			Query:                 query,
			PageSize:              2,
			NextPageToken:         nextPageToken,
		})
		require.NoError(t, err)

		for _, flowRun := range searchResp.GetFlowRuns() {
			assertions.NotEmpty(flowRun.GetIndexedAttributes())
		}
		currentCount += len(searchResp.GetFlowRuns())
		if currentCount < expectedCount {
			assertions.Equal(2, len(searchResp.GetFlowRuns()))
			assertions.NotEmpty(t, searchResp.GetNextPageToken())
			nextPageToken = searchResp.GetNextPageToken()
		} else if currentCount == expectedCount {
			if searchResp.GetNextPageToken() != "" {
				nextPageToken = searchResp.GetNextPageToken()
				searchResp, err = flowClient.SearchFlows(ctx, &dexpb.SearchFlowsRequest{
					IncludeContinuedAsNew: includeContinuedAsNew,
					Query:                 query,
					PageSize:              2,
					NextPageToken:         nextPageToken,
				})
				require.NoError(t, err)
				assertions.Empty(t, searchResp.GetFlowRuns())
				assertions.Empty(t, searchResp.GetNextPageToken())
			}
		} else {
			assertions.FailNow(
				fmt.Sprintf(
					"currentCount %v is greater than expectedCount %v, for query %v",
					currentCount,
					expectedCount,
					query,
				),
			)
		}
	}
}
