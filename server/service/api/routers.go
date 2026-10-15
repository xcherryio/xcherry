// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package api

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/auth"
	uclient "github.com/superdurable/dex/service/client"
	"github.com/superdurable/dex/service/common/attributestore"
	"github.com/superdurable/dex/service/common/blobstore"
	"github.com/superdurable/dex/service/common/log"
	"github.com/superdurable/dex/service/common/log/tag"
	"github.com/superdurable/dex/service/common/pulldispatch"
	"github.com/superdurable/dex/service/common/streamstore"
	"github.com/superdurable/dex/service/common/workerclient"
	"github.com/superdurable/dex/service/matching"
	"github.com/superdurable/dex/service/wake"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// maxCredentialHeaderListBytes bounds request metadata before credential parsing.
const maxCredentialHeaderListBytes = 32 << 10

// Server hosts FlowService, InternalService, and grpc.health.
// Authentication serves InternalService on a separate listener.
type Server struct {
	cfg                *config.ApiConfig
	grpcServer         *grpc.Server
	internalGrpcServer *grpc.Server
	healthSrv          *health.Server
	handler            *handler
	pullClient         *workerclient.InternalServiceClient
	logger             log.Logger
	readyCheck         func(context.Context) error
}

type pollWorkerTaskService struct {
	dexpb.UnimplementedWorkerTaskServiceServer
	engine *matching.Engine
}

func (s *pollWorkerTaskService) PollWorkerTask(stream dexpb.WorkerTaskService_PollWorkerTaskServer) error {
	return s.engine.ProxyPollWorkerTask(stream)
}

func NewServer(
	apiCfg *config.ApiConfig,
	blobStoreCfg *config.BlobStoreConfig,
	interpreterCfg *config.Interpreter,
	client uclient.UnifiedClient,
	logger log.Logger,
	store blobstore.BlobStore,
	attributeStore *attributestore.Manager,
	streamStore *streamstore.Store,
	readyCheck func(context.Context) error,
	workerPool *workerclient.WorkerClientPool,
	extraUnaryInterceptors ...grpc.UnaryServerInterceptor,
) (*Server, error) {
	return NewServerWithAccess(apiCfg, &config.AuthConfig{}, &config.InternalConfig{}, &config.WakeConfig{}, blobStoreCfg, interpreterCfg,
		client, logger, store, attributeStore, streamStore, readyCheck, workerPool, extraUnaryInterceptors...)
}

// NewServerWithAccess builds the public and optional private gRPC listeners.
func NewServerWithAccess(
	apiCfg *config.ApiConfig,
	authCfg *config.AuthConfig,
	internalCfg *config.InternalConfig,
	wakeCfg *config.WakeConfig,
	blobStoreCfg *config.BlobStoreConfig,
	interpreterCfg *config.Interpreter,
	client uclient.UnifiedClient,
	logger log.Logger,
	store blobstore.BlobStore,
	attributeStore *attributestore.Manager,
	streamStore *streamstore.Store,
	readyCheck func(context.Context) error,
	workerPool *workerclient.WorkerClientPool,
	extraUnaryInterceptors ...grpc.UnaryServerInterceptor,
) (*Server, error) {
	return NewServerWithMatching(apiCfg, authCfg, internalCfg, wakeCfg, blobStoreCfg, interpreterCfg,
		client, logger, store, attributeStore, streamStore, readyCheck, workerPool, true, nil, nil, extraUnaryInterceptors...)
}

// NewServerWithMatching builds gRPC listeners with pull Worker routing.
func NewServerWithMatching(
	apiCfg *config.ApiConfig,
	authCfg *config.AuthConfig,
	internalCfg *config.InternalConfig,
	wakeCfg *config.WakeConfig,
	blobStoreCfg *config.BlobStoreConfig,
	interpreterCfg *config.Interpreter,
	client uclient.UnifiedClient,
	logger log.Logger,
	store blobstore.BlobStore,
	attributeStore *attributestore.Manager,
	streamStore *streamstore.Store,
	readyCheck func(context.Context) error,
	workerPool *workerclient.WorkerClientPool,
	publicEnabled bool,
	internalToken *auth.InternalTokenFile,
	matchingEngine *matching.Engine,
	extraUnaryInterceptors ...grpc.UnaryServerInterceptor,
) (*Server, error) {
	return NewServerWithMatchingAccess(apiCfg, authCfg, internalCfg, wakeCfg, nil, blobStoreCfg, interpreterCfg,
		client, logger, store, attributeStore, streamStore, readyCheck, workerPool,
		publicEnabled, internalToken, matchingEngine, nil, extraUnaryInterceptors...)
}

// NewServerWithMatchingAccess configures pull dispatch waits alongside API access.
func NewServerWithMatchingAccess(
	apiCfg *config.ApiConfig,
	authCfg *config.AuthConfig,
	internalCfg *config.InternalConfig,
	wakeCfg *config.WakeConfig,
	matchingCfg *config.MatchingConfig,
	blobStoreCfg *config.BlobStoreConfig,
	interpreterCfg *config.Interpreter,
	client uclient.UnifiedClient,
	logger log.Logger,
	store blobstore.BlobStore,
	attributeStore *attributestore.Manager,
	streamStore *streamstore.Store,
	readyCheck func(context.Context) error,
	workerPool *workerclient.WorkerClientPool,
	publicEnabled bool,
	internalToken *auth.InternalTokenFile,
	matchingEngine *matching.Engine,
	pullDispatcher pulldispatch.Dispatcher,
	extraUnaryInterceptors ...grpc.UnaryServerInterceptor,
) (*Server, error) {
	if apiCfg == nil {
		panic("apiCfg must not be nil")
	}
	if authCfg == nil || internalCfg == nil || wakeCfg == nil {
		panic("access config sections must not be nil")
	}
	if authCfg.IsEnabled() && (internalCfg.Listen == "" || internalCfg.TokenFile == "") {
		return nil, fmt.Errorf("internal.listen and internal.tokenFile are required when auth is enabled")
	}
	hasInternalListener := internalCfg.Listen != "" && internalCfg.TokenFile != ""
	if client == nil {
		panic("client must not be nil")
	}
	if logger == nil {
		panic("logger must not be nil")
	}
	if blobStoreCfg == nil {
		panic("blobStoreCfg must not be nil")
	}
	if blobStoreCfg.EffectiveEnabled() && store == nil {
		panic("store must not be nil when blob storage is enabled")
	}
	if interpreterCfg == nil {
		panic("interpreterCfg must not be nil")
	}
	if attributeStore == nil {
		panic("attributeStore must not be nil")
	}
	if streamStore == nil {
		panic("streamStore must not be nil")
	}
	if workerPool == nil {
		panic("workerPool must not be nil")
	}
	if readyCheck == nil {
		readyCheck = func(context.Context) error { return nil }
	}

	generalInternalToken := internalToken
	if generalInternalToken == nil && internalCfg.TokenFile != "" {
		var err error
		generalInternalToken, err = auth.NewInternalTokenFile(internalCfg, logger)
		if err != nil {
			return nil, fmt.Errorf("internal.tokenFile: %w", err)
		}
	}
	var generalWakeToken interface{ Token() string }
	if generalInternalToken != nil {
		generalWakeToken = generalInternalToken
	}
	wakeNotifier := wake.NewNotifier(wakeCfg, generalWakeToken, logger)
	var pullClient *workerclient.InternalServiceClient
	if pullDispatcher == nil {
		internalClientConfig := &config.Config{Api: *apiCfg, Auth: *authCfg, Internal: *internalCfg, Interpreter: *interpreterCfg}
		var err error
		pullClient, err = workerclient.NewInternalServiceClient(
			internalClientConfig.GetInternalServiceTargetWithDefault(), internalClientConfig, generalWakeToken,
		)
		if err != nil {
			return nil, fmt.Errorf("create matching client: %w", err)
		}
		pullDispatcher = pulldispatch.NewDispatcher(pullClient)
	}
	handler := newHandler(
		apiCfg,
		blobStoreCfg,
		interpreterCfg,
		client,
		logger,
		store,
		attributeStore,
		streamStore,
		workerPool,
		wakeNotifier,
		pullDispatcher,
		matchingCfg,
	)
	maxMsg := apiCfg.EffectiveGrpcMaxMessageBytes()
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	healthSrv.SetServingStatus(dexpb.FlowService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_NOT_SERVING)
	healthSrv.SetServingStatus(dexpb.InternalService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_NOT_SERVING)
	healthSrv.SetServingStatus(dexpb.MatchingService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_NOT_SERVING)
	healthSrv.SetServingStatus(dexpb.WorkerTaskService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_NOT_SERVING)

	serverOptions := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(maxMsg),
		grpc.MaxSendMsgSize(maxMsg),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: false}),
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 20 * time.Second, Timeout: 10 * time.Second}),
	}
	server := &Server{
		cfg:        apiCfg,
		healthSrv:  healthSrv,
		handler:    handler,
		pullClient: pullClient,
		logger:     logger,
		readyCheck: readyCheck,
	}
	if publicEnabled && !authCfg.IsEnabled() {
		server.grpcServer = grpc.NewServer(append(
			serverOptions,
			grpc.ChainUnaryInterceptor(unaryInterceptorChain(nil, extraUnaryInterceptors, logger)...),
		)...)
		dexpb.RegisterFlowServiceServer(server.grpcServer, handler)
		dexpb.RegisterInternalServiceServer(server.grpcServer, handler)
		if matchingEngine != nil {
			dexpb.RegisterWorkerTaskServiceServer(server.grpcServer, &pollWorkerTaskService{engine: matchingEngine})
		}
		healthpb.RegisterHealthServer(server.grpcServer, healthSrv)
	}
	if publicEnabled && authCfg.IsEnabled() {
		authenticator, err := auth.NewAuthenticator(authCfg, logger)
		if err != nil {
			if pullClient != nil {
				pullClient.Close()
			}
			return nil, err
		}
		// Recovery wraps authentication so a panic there is contained; metadata stays small before any parsing.
		serverOptions = append(serverOptions, grpc.MaxHeaderListSize(maxCredentialHeaderListBytes))
		publicInterceptors := auth.NewPublicServiceInterceptors(authenticator)
		server.grpcServer = grpc.NewServer(append(serverOptions,
			grpc.ChainUnaryInterceptor(unaryInterceptorChain([]grpc.UnaryServerInterceptor{publicInterceptors.Unary}, extraUnaryInterceptors, logger)...),
			grpc.ChainStreamInterceptor(publicInterceptors.Stream))...)
		dexpb.RegisterFlowServiceServer(server.grpcServer, handler)
		if matchingEngine != nil {
			dexpb.RegisterWorkerTaskServiceServer(server.grpcServer, &pollWorkerTaskService{engine: matchingEngine})
		}
		healthpb.RegisterHealthServer(server.grpcServer, healthSrv)
	}
	if !hasInternalListener && matchingEngine == nil {
		return server, nil
	}
	if generalInternalToken != nil {
		internalInterceptors := auth.NewInternalServiceInterceptor(generalInternalToken, []string{dexpb.InternalService_ServiceDesc.ServiceName, dexpb.MatchingService_ServiceDesc.ServiceName})
		server.internalGrpcServer = grpc.NewServer(append(serverOptions,
			grpc.ChainUnaryInterceptor(unaryInterceptorChain([]grpc.UnaryServerInterceptor{internalInterceptors.Unary}, extraUnaryInterceptors, logger)...),
			grpc.ChainStreamInterceptor(internalInterceptors.Stream))...)
	} else {
		server.internalGrpcServer = grpc.NewServer(append(serverOptions,
			grpc.ChainUnaryInterceptor(unaryInterceptorChain(nil, extraUnaryInterceptors, logger)...))...)
	}
	dexpb.RegisterInternalServiceServer(server.internalGrpcServer, handler)
	if matchingEngine != nil {
		dexpb.RegisterMatchingServiceServer(server.internalGrpcServer, matchingEngine)
	}
	healthpb.RegisterHealthServer(server.internalGrpcServer, healthSrv)
	return server, nil
}

// HasInternalListener reports whether InternalService must be served by ServeInternal.
func (s *Server) HasInternalListener() bool {
	return s.internalGrpcServer != nil
}

// ServeInternal hosts the private gRPC services on listener.
func (s *Server) ServeInternal(listener net.Listener) error {
	if s.internalGrpcServer == nil {
		panic("ServeInternal requires an internal listener")
	}
	s.logger.Info("InternalService gRPC listening", tag.Address(listener.Addr().String()))
	if err := s.readyCheck(context.Background()); err != nil {
		s.logger.Error("initial readiness check failed", tag.Error(err))
	} else {
		s.setServing(true)
	}
	return s.internalGrpcServer.Serve(listener)
}

// Run listens on the configured port.
func (s *Server) Run() error {
	port := s.cfg.Port
	if port == 0 {
		port = config.DefaultApiPort
	}
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	return s.Serve(lis)
}

// Serve hosts the gRPC services on listener.
func (s *Server) Serve(listener net.Listener) error {
	if s.grpcServer == nil {
		panic("Serve requires the API role")
	}
	s.logger.Info("FlowService gRPC listening", tag.Address(listener.Addr().String()))

	if err := s.readyCheck(context.Background()); err != nil {
		s.logger.Error("initial readiness check failed", tag.Error(err))
	} else {
		s.setServing(true)
	}

	return s.grpcServer.Serve(listener)
}

// GracefulStop stops accepting requests before closing dependencies.
func (s *Server) GracefulStop(timeout time.Duration) {
	s.StopServing(timeout)
	s.Close()
}

func (s *Server) StopServing(timeout time.Duration) {
	s.setServing(false)
	done := make(chan struct{})
	go func() {
		if s.grpcServer != nil {
			s.grpcServer.GracefulStop()
		}
		if s.internalGrpcServer != nil {
			s.internalGrpcServer.GracefulStop()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		if s.grpcServer != nil {
			s.grpcServer.Stop()
		}
		if s.internalGrpcServer != nil {
			s.internalGrpcServer.Stop()
		}
	}
}

func (s *Server) Close() {
	s.handler.close()
	if s.pullClient != nil {
		s.pullClient.Close()
	}
}

func (s *Server) setServing(serving bool) {
	st := healthpb.HealthCheckResponse_NOT_SERVING
	if serving {
		st = healthpb.HealthCheckResponse_SERVING
	}
	s.healthSrv.SetServingStatus("", st)
	s.healthSrv.SetServingStatus(dexpb.FlowService_ServiceDesc.ServiceName, st)
	s.healthSrv.SetServingStatus(dexpb.InternalService_ServiceDesc.ServiceName, st)
	s.healthSrv.SetServingStatus(dexpb.MatchingService_ServiceDesc.ServiceName, st)
	s.healthSrv.SetServingStatus(dexpb.WorkerTaskService_ServiceDesc.ServiceName, st)
}

// unaryInterceptorChain returns a new chain: recovery outermost, then authorization, extra interceptors and logging.
func unaryInterceptorChain(
	authorization []grpc.UnaryServerInterceptor,
	extra []grpc.UnaryServerInterceptor,
	logger log.Logger,
) []grpc.UnaryServerInterceptor {
	chain := []grpc.UnaryServerInterceptor{unaryRecover(logger)}
	chain = append(chain, authorization...)
	chain = append(chain, extra...)
	return append(chain, unaryLog(logger))
}

func unaryRecover(logger log.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				logger.Error(
					"gRPC handler panic",
					tag.Panic(fmt.Sprintf("%v", recovered)),
					tag.OperationName(info.FullMethod),
				)
				err = status.Error(codes.Internal, "internal panic")
			}
		}()
		return handler(ctx, req)
	}
}

func unaryLog(logger log.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		logger.Debug(
			"gRPC call",
			tag.OperationName(info.FullMethod),
			tag.Elapsed(time.Since(start)),
			tag.Error(err),
		)
		return resp, err
	}
}
