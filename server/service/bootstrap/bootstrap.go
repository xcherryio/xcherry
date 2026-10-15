// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/api"
	"github.com/superdurable/dex/service/auth"
	uclient "github.com/superdurable/dex/service/client"
	"github.com/superdurable/dex/service/common/attributestore"
	"github.com/superdurable/dex/service/common/blobstore"
	"github.com/superdurable/dex/service/common/log"
	"github.com/superdurable/dex/service/common/log/loggerimpl"
	"github.com/superdurable/dex/service/common/log/tag"
	"github.com/superdurable/dex/service/common/pulldispatch"
	"github.com/superdurable/dex/service/common/streamstore"
	"github.com/superdurable/dex/service/common/workerclient"
	"github.com/superdurable/dex/service/indexsync"
	"github.com/superdurable/dex/service/matching"
	"github.com/superdurable/dex/service/membership"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const DefaultShutdownTimeout = 10 * time.Second

type Services struct {
	// API defaults false and enables the Dex gRPC API.
	API bool
	// Interpreter defaults false and enables the workflow interpreter.
	Interpreter bool
	// Matching defaults false and owns pull Worker queues when enabled.
	Matching bool
}

type Options struct {
	// Services has no default; at least one service must be enabled.
	Services Services
	// APIListener defaults nil and uses the configured API port.
	APIListener net.Listener
	// InternalListener defaults nil and uses internal.listen when configured.
	InternalListener net.Listener
	// ShutdownTimeout defaults to 10 seconds for graceful API shutdown.
	ShutdownTimeout time.Duration
}

type Runtime struct {
	cfg               *config.Config
	options           *Options
	apiServer         *api.Server
	worker            interpreterWorker
	workerPool        *workerclient.WorkerClientPool
	attributeStore    *attributestore.Manager
	blobStore         blobstore.BlobStore
	logger            log.Logger
	zapLogger         *zap.Logger
	metricsCloser     io.Closer
	serveError        chan error
	indexSynchronizer *indexsync.Synchronizer
	unifiedClient     uclient.UnifiedClient
	streamStore       *streamstore.Store
	membership        *membership.Monitor
	matching          *matching.Engine
	matchingClients   *matchingClientPool
	pullDispatcher    pulldispatch.Dispatcher
	internalListener  net.Listener
	internalToken     *auth.InternalTokenFile
	shutdownOnce      sync.Once
}

type interpreterWorker interface {
	Start() error
	Close()
}

func New(cfg *config.Config, options *Options) (*Runtime, error) {
	if cfg == nil {
		panic("runtime config must not be nil")
	}
	if options == nil {
		panic("runtime options must not be nil")
	}
	if !options.Services.API && !options.Services.Interpreter && !options.Services.Matching {
		return nil, fmt.Errorf("at least one Dex service is required")
	}
	if cfg.Interpreter.Temporal == nil && cfg.Interpreter.Cadence == nil {
		return nil, fmt.Errorf("Temporal or Cadence config is required")
	}
	if cfg.Interpreter.Temporal != nil && cfg.Interpreter.Cadence != nil {
		return nil, fmt.Errorf("Temporal and Cadence configs are mutually exclusive")
	}
	if err := cfg.ValidateGeneralAccess(); err != nil {
		return nil, err
	}

	zapLogger, err := cfg.Log.NewZapLogger()
	if err != nil {
		return nil, fmt.Errorf("create logger: %w", err)
	}
	logger := loggerimpl.NewLogger(zapLogger)
	workerPool, err := workerclient.NewWorkerClientPool(cfg)
	if err != nil {
		return nil, fmt.Errorf("create WorkerService client pool: %w", err)
	}

	runtime := &Runtime{
		cfg:        cfg,
		options:    options,
		workerPool: workerPool,
		logger:     logger,
		zapLogger:  zapLogger,
		serveError: make(chan error, 2),
	}
	streamStore, streamStoreErr := streamstore.New(
		&cfg.StreamStore,
		logger.WithTags(tag.Service("stream-store")),
	)
	if streamStoreErr != nil {
		workerPool.Close()
		return nil, fmt.Errorf("initialize Stream Store: %w", streamStoreErr)
	}
	runtime.streamStore = streamStore
	attributeStore, err := attributestore.NewManager(context.Background(), &cfg.AttributeStore, logger)
	if err != nil {
		if runtime.streamStore != nil {
			if closeErr := runtime.streamStore.Close(); closeErr != nil {
				logger.Error("close Stream Store", tag.Error(closeErr))
			}
		}
		workerPool.Close()
		return nil, fmt.Errorf("initialize Attribute Stores: %w", err)
	}
	runtime.attributeStore = attributeStore
	if err := runtime.createServices(); err != nil {
		runtime.shutdown()
		return nil, err
	}
	return runtime, nil
}

func (r *Runtime) createServices() error {
	if err := r.createMembershipAndMatching(); err != nil {
		return err
	}
	var (
		client uclient.UnifiedClient
		store  blobstore.BlobStore
		err    error
	)
	if r.cfg.Interpreter.Temporal != nil {
		client, store, r.worker, err = r.createTemporalServices()
	} else {
		client, store, r.worker, err = r.createCadenceServices()
	}
	if err != nil {
		return err
	}
	r.blobStore = store
	r.unifiedClient = client
	r.indexSynchronizer = indexsync.New(&r.cfg.Interpreter, client, r.logger)
	if r.options.Services.API || r.options.Services.Matching || r.internalListener != nil {
		serviceName := "api"
		if !r.options.Services.API {
			serviceName = "interpreter"
		}
		if !r.options.Services.API && r.options.Services.Matching {
			serviceName = "matching"
		}
		r.apiServer, err = api.NewServerWithMatchingAccess(
			&r.cfg.Api,
			&r.cfg.Auth,
			&r.cfg.Internal,
			&r.cfg.Worker.Wake,
			r.cfg.Matching,
			&r.cfg.BlobStore,
			&r.cfg.Interpreter,
			client,
			r.logger.WithTags(tag.Service(serviceName)),
			store,
			r.attributeStore,
			r.streamStore,
			nil,
			r.workerPool,
			r.options.Services.API,
			r.internalToken,
			r.matching,
			r.pullDispatcher,
		)
		if err != nil {
			return fmt.Errorf("create Dex API: %w", err)
		}
	}
	return nil
}

func (r *Runtime) createMembershipAndMatching() error {
	if r.options.Services.Matching && !r.options.Services.API && r.cfg.Internal.Listen == "" {
		return fmt.Errorf("internal.listen is required for a matching-only process")
	}
	if r.options.Services.Matching && !r.options.Services.API && r.cfg.Internal.TokenFile == "" {
		return fmt.Errorf("internal.tokenFile is required for a matching-only process")
	}
	if r.options.InternalListener != nil {
		r.internalListener = r.options.InternalListener
	} else if r.cfg.Internal.Listen != "" && r.cfg.Internal.TokenFile != "" {
		listener, err := net.Listen("tcp", r.cfg.Internal.Listen)
		if err != nil {
			return fmt.Errorf("listen on internal.listen: %w", err)
		}
		r.internalListener = listener
	} else if r.options.Services.API || r.options.Services.Matching {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return fmt.Errorf("listen on local matching service: %w", err)
		}
		r.internalListener = listener
	}
	roles := make([]membership.Role, 0, 3)
	if r.options.Services.API {
		roles = append(roles, membership.RoleAPI)
	}
	if r.options.Services.Interpreter {
		roles = append(roles, membership.RoleInterpreter)
	}
	if r.options.Services.Matching {
		roles = append(roles, membership.RoleMatching)
	}
	rpcAddress := ""
	if r.internalListener != nil {
		var err error
		rpcAddress, err = membership.AdvertisedRPCAddress(r.cfg.Membership, r.internalListener.Addr().String())
		if err != nil {
			return err
		}
	}
	monitor, err := membership.NewMonitor(r.cfg.Membership, roles, rpcAddress, nil, r.zapLogger)
	if err != nil {
		return err
	}
	r.membership = monitor
	var token *auth.InternalTokenFile
	if r.cfg.Internal.TokenFile != "" {
		token, err = auth.NewInternalTokenFile(&r.cfg.Internal, r.logger)
		if err != nil {
			return err
		}
	}
	r.internalToken = token
	var matchingToken interface{ Token() string }
	if token != nil {
		matchingToken = token
	}
	r.matchingClients = newMatchingClientPool(matchingToken, r.cfg.Api.EffectiveGrpcMaxMessageBytes(), monitor, r.zapLogger)
	r.pullDispatcher = pulldispatch.NewRoutedDispatcher(monitor, r.matchingClients.MatchingClient)
	if r.options.Services.API || r.options.Services.Matching {
		matchingCfg := r.cfg.Matching
		if matchingCfg == nil {
			matchingCfg = &config.MatchingConfig{}
		}
		r.matching, err = matching.NewEngine(matchingCfg, r.zapLogger, monitor, r.matchingClients.MatchingClient)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Runtime) Run(ctx context.Context) error {
	dexIndexes := map[string]dexpb.IndexType{
		service.SearchAttributeDexWorkflowType: dexpb.IndexType_INDEX_TYPE_KEYWORD,
		service.SearchAttributeDexParentFlowID: dexpb.IndexType_INDEX_TYPE_KEYWORD,
		service.SearchAttributeActiveStepTypes: dexpb.IndexType_INDEX_TYPE_KEYWORD_ARRAY,
	}
	if err := r.indexSynchronizer.Sync(ctx, dexIndexes); err != nil {
		r.shutdown()
		return fmt.Errorf("sync Dex attribute indexes: %w", err)
	}
	if err := r.membership.Start(ctx); err != nil {
		r.shutdown()
		return fmt.Errorf("start membership: %w", err)
	}
	if r.apiServer != nil && r.apiServer.HasInternalListener() {
		go func() { r.serveError <- r.apiServer.ServeInternal(r.internalListener) }()
	}
	if r.worker != nil {
		if err := r.worker.Start(); err != nil {
			r.shutdown()
			return err
		}
	}
	if r.apiServer != nil && r.options.Services.API {
		go func() {
			if r.options.APIListener != nil {
				r.serveError <- r.apiServer.Serve(r.options.APIListener)
				return
			}
			r.serveError <- r.apiServer.Run()
		}()
	}

	if r.apiServer == nil {
		<-ctx.Done()
		r.shutdown()
		return nil
	}

	select {
	case <-ctx.Done():
		r.shutdown()
		err := <-r.serveError
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			return fmt.Errorf("serve Dex API: %w", err)
		}
		return nil
	case err := <-r.serveError:
		r.shutdown()
		if err == nil || errors.Is(err, grpc.ErrServerStopped) {
			return fmt.Errorf("Dex API stopped unexpectedly")
		}
		return fmt.Errorf("serve Dex API: %w", err)
	}
}

func (r *Runtime) Close() {
	r.shutdown()
}

func (r *Runtime) shutdown() {
	r.shutdownOnce.Do(func() {
		if r.membership != nil {
			r.membership.Stop()
		}
		if r.matching != nil {
			r.matching.Close()
		}
		if r.apiServer != nil {
			timeout := r.options.ShutdownTimeout
			if timeout <= 0 {
				timeout = DefaultShutdownTimeout
			}
			r.apiServer.StopServing(timeout)
		}
		if r.worker != nil {
			r.worker.Close()
		}
		if r.apiServer != nil {
			r.apiServer.Close()
		}
		if r.internalListener != nil {
			if err := r.internalListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				r.logger.Error("close internal listener", tag.Error(err))
			}
		}
		if r.matchingClients != nil {
			if err := r.matchingClients.Close(); err != nil {
				r.logger.Error("close matching clients", tag.Error(err))
			}
		}
		if r.attributeStore != nil {
			if err := r.attributeStore.Close(); err != nil {
				r.logger.Error("close Attribute Stores", tag.Error(err))
			}
		}
		if r.blobStore != nil {
			if err := r.blobStore.Close(); err != nil {
				r.logger.Error("close blob store", tag.Error(err))
			}
		}
		if r.streamStore != nil {
			if err := r.streamStore.Close(); err != nil {
				r.logger.Error("close Stream Store", tag.Error(err))
			}
		}
		r.workerPool.Close()
		if r.metricsCloser != nil {
			if err := r.metricsCloser.Close(); err != nil {
				r.logger.Error("close metrics scope", tag.Error(err))
			}
		}
	})
}

// InternalAddress returns the local MatchingService listener address.
func (r *Runtime) InternalAddress() string {
	if r.internalListener == nil {
		return ""
	}
	return r.internalListener.Addr().String()
}

// MembershipAddress returns this process's advertised ring address after Run starts.
func (r *Runtime) MembershipAddress() string {
	if r.membership == nil {
		return ""
	}
	return r.membership.Address()
}
