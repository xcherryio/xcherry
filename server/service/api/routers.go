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
	uclient "github.com/superdurable/dex/service/client"
	"github.com/superdurable/dex/service/common/attributestore"
	"github.com/superdurable/dex/service/common/blobstore"
	"github.com/superdurable/dex/service/common/log"
	"github.com/superdurable/dex/service/common/log/tag"
	"github.com/superdurable/dex/service/common/streamstore"
	"github.com/superdurable/dex/service/common/workerclient"
	"github.com/superdurable/dex/service/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// maxTenantHeaderListBytes bounds request metadata in multi-tenant mode, leaving room for an 8 KiB credential.
const maxTenantHeaderListBytes = 32 << 10

// Server hosts FlowService, InternalService, and grpc.health.
// In multi-tenant mode InternalService moves to a separate internal gRPC server.
type Server struct {
	cfg                *config.ApiConfig
	grpcServer         *grpc.Server
	internalGrpcServer *grpc.Server
	healthSrv          *health.Server
	handler            *handler
	logger             log.Logger
	readyCheck         func(context.Context) error
}

func NewServer(
	apiCfg *config.ApiConfig,
	tenancyCfg *config.TenancyConfig,
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
	if apiCfg == nil {
		panic("apiCfg must not be nil")
	}
	if tenancyCfg == nil {
		panic("tenancyCfg must not be nil")
	}
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
	)
	maxMsg := apiCfg.EffectiveGrpcMaxMessageBytes()
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	healthSrv.SetServingStatus(dexpb.FlowService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_NOT_SERVING)
	healthSrv.SetServingStatus(dexpb.InternalService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_NOT_SERVING)

	serverOptions := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(maxMsg),
		grpc.MaxSendMsgSize(maxMsg),
	}
	server := &Server{
		cfg:        apiCfg,
		healthSrv:  healthSrv,
		handler:    handler,
		logger:     logger,
		readyCheck: readyCheck,
	}
	if !tenancyCfg.IsMultiTenant() {
		server.grpcServer = grpc.NewServer(append(
			serverOptions,
			grpc.ChainUnaryInterceptor(unaryInterceptorChain(nil, extraUnaryInterceptors, logger)...),
		)...)
		dexpb.RegisterFlowServiceServer(server.grpcServer, handler)
		dexpb.RegisterInternalServiceServer(server.grpcServer, handler)
		healthpb.RegisterHealthServer(server.grpcServer, healthSrv)
		return server, nil
	}

	authenticator, err := tenancy.NewAuthenticator(tenancyCfg, logger)
	if err != nil {
		return nil, err
	}
	internalToken, err := tenancy.NewInternalTokenFile(tenancyCfg.Internal.TokenFile, logger)
	if err != nil {
		return nil, fmt.Errorf("tenancy.internal.tokenFile: %w", err)
	}
	// Recovery wraps authentication so a panic there is contained; metadata stays small before any parsing.
	serverOptions = append(serverOptions, grpc.MaxHeaderListSize(maxTenantHeaderListBytes))
	publicInterceptors := tenancy.NewPublicServiceInterceptors(authenticator)
	server.grpcServer = grpc.NewServer(append(
		serverOptions,
		grpc.ChainUnaryInterceptor(unaryInterceptorChain(
			[]grpc.UnaryServerInterceptor{publicInterceptors.Unary},
			extraUnaryInterceptors,
			logger,
		)...),
		grpc.ChainStreamInterceptor(publicInterceptors.Stream),
	)...)
	dexpb.RegisterFlowServiceServer(server.grpcServer, handler)
	healthpb.RegisterHealthServer(server.grpcServer, healthSrv)

	server.internalGrpcServer = grpc.NewServer(append(
		serverOptions,
		grpc.ChainUnaryInterceptor(unaryInterceptorChain(
			[]grpc.UnaryServerInterceptor{tenancy.NewInternalServiceInterceptor(internalToken).Unary},
			extraUnaryInterceptors,
			logger,
		)...),
	)...)
	dexpb.RegisterInternalServiceServer(server.internalGrpcServer, handler)
	healthpb.RegisterHealthServer(server.internalGrpcServer, healthSrv)
	return server, nil
}

// HasInternalListener reports whether InternalService must be served by ServeInternal.
func (s *Server) HasInternalListener() bool {
	return s.internalGrpcServer != nil
}

// ServeInternal hosts InternalService on listener. Only multi-tenant mode has an internal server.
func (s *Server) ServeInternal(listener net.Listener) error {
	if s.internalGrpcServer == nil {
		panic("ServeInternal requires multi-tenant mode")
	}
	s.logger.Info("InternalService gRPC listening", tag.Address(listener.Addr().String()))
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
		s.grpcServer.GracefulStop()
		if s.internalGrpcServer != nil {
			s.internalGrpcServer.GracefulStop()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		s.grpcServer.Stop()
		if s.internalGrpcServer != nil {
			s.internalGrpcServer.Stop()
		}
	}
}

func (s *Server) Close() {
	s.handler.close()
}

func (s *Server) setServing(serving bool) {
	st := healthpb.HealthCheckResponse_NOT_SERVING
	if serving {
		st = healthpb.HealthCheckResponse_SERVING
	}
	s.healthSrv.SetServingStatus("", st)
	s.healthSrv.SetServingStatus(dexpb.FlowService_ServiceDesc.ServiceName, st)
	s.healthSrv.SetServingStatus(dexpb.InternalService_ServiceDesc.ServiceName, st)
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
