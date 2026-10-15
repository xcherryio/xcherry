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
	uclient "github.com/superdurable/dex/service/client"
	"github.com/superdurable/dex/service/common/attributestore"
	"github.com/superdurable/dex/service/common/blobstore"
	"github.com/superdurable/dex/service/common/log"
	"github.com/superdurable/dex/service/common/log/loggerimpl"
	"github.com/superdurable/dex/service/common/log/tag"
	"github.com/superdurable/dex/service/common/streamstore"
	"github.com/superdurable/dex/service/common/workerclient"
	"github.com/superdurable/dex/service/indexsync"
	"github.com/superdurable/dex/service/tenancy"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const DefaultShutdownTimeout = 10 * time.Second

type Services struct {
	// API defaults false and enables the Dex gRPC API.
	API bool
	// Interpreter defaults false and enables the workflow interpreter.
	Interpreter bool
}

type Options struct {
	// Services has no default; at least one service must be enabled.
	Services Services
	// APIListener defaults nil and uses the configured API port.
	APIListener net.Listener
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
	if !options.Services.API && !options.Services.Interpreter {
		return nil, fmt.Errorf("at least one Dex service is required")
	}
	if cfg.Interpreter.Temporal == nil && cfg.Interpreter.Cadence == nil {
		return nil, fmt.Errorf("Temporal or Cadence config is required")
	}
	if cfg.Interpreter.Temporal != nil && cfg.Interpreter.Cadence != nil {
		return nil, fmt.Errorf("Temporal and Cadence configs are mutually exclusive")
	}
	if err := cfg.ValidateTenancy(); err != nil {
		return nil, err
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
		&cfg.Tenancy,
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
	if r.options.Services.API {
		r.apiServer, err = api.NewServerWithAccess(
			&r.cfg.Api,
			&r.cfg.Tenancy,
			&r.cfg.Auth,
			&r.cfg.Internal,
			&r.cfg.Worker.Wake,
			&r.cfg.BlobStore,
			&r.cfg.Interpreter,
			client,
			r.logger.WithTags(tag.Service("api")),
			store,
			r.attributeStore,
			r.streamStore,
			nil,
			r.workerPool,
		)
		if err != nil {
			return fmt.Errorf("create Dex API: %w", err)
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
	if r.cfg.Tenancy.IsMultiTenant() {
		dexIndexes[tenancy.SearchAttributeDexTenantID] = dexpb.IndexType_INDEX_TYPE_KEYWORD
	}
	if err := r.indexSynchronizer.Sync(ctx, dexIndexes); err != nil {
		r.shutdown()
		return fmt.Errorf("sync Dex attribute indexes: %w", err)
	}
	if r.cfg.Tenancy.IsMultiTenant() {
		if err := tenancy.VerifyNamespaceHasOnlyTenantFlows(ctx, r.unifiedClient); err != nil {
			r.shutdown()
			return err
		}
	}
	if r.worker != nil {
		if err := r.worker.Start(); err != nil {
			r.shutdown()
			return err
		}
	}
	if r.apiServer != nil && r.apiServer.HasInternalListener() {
		internalListen := r.cfg.Internal.Listen
		if r.cfg.Tenancy.IsMultiTenant() {
			internalListen = r.cfg.Tenancy.Internal.Listen
		}
		internalListener, err := net.Listen("tcp", internalListen)
		if err != nil {
			r.shutdown()
			return fmt.Errorf("listen on internal.listen: %w", err)
		}
		go func() {
			r.serveError <- r.apiServer.ServeInternal(internalListener)
		}()
	}
	if r.apiServer != nil {
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
