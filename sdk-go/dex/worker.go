// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package dex

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/superdurable/dex/blob-cache-go/blobcache"
	"github.com/superdurable/dex/sdk-go/gen/dexpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	defaultWorkerBindAddress         = ":8803"
	defaultFlowServiceTarget         = "localhost:8801"
	defaultAttributeIndexSyncTimeout = 2 * time.Minute
)

// WorkerOptions configures a listening or outbound-polling Worker.
//
// Zero values select local plaintext defaults. In listening mode, BindAddress controls the
// socket, while WorkerTarget is the address Dex advertises to running Flows; these
// commonly differ in containers or headless service deployments.
type WorkerOptions struct {
	// Pull enables outbound task polling. Nil keeps the listening WorkerService mode.
	// Configure FlowServiceAddress and Credential for the server this Worker contacts.
	// BindAddress and WorkerTarget must be empty in pull mode; NewWorker rejects either setting.
	Pull *PullWorkerOptions
	// BindAddress is the plaintext WorkerService listener for Dex; it may differ from WorkerTarget. Default: ":8803".
	// It uses host:port syntax.
	BindAddress string
	// WorkerTarget is advertised to Dex and may differ from BindAddress. Default Address derives from BindAddress; Headless defaults false.
	WorkerTarget WorkerTarget
	// FlowServiceAddress is the plaintext Dex endpoint used for startup synchronization and blob hydration. Default: "localhost:8801".
	// Startup synchronization registers Attribute indexes.
	FlowServiceAddress string
	// AttributeIndexSyncTimeout bounds index registration at startup. Default: two minutes.
	// Negative durations are invalid.
	AttributeIndexSyncTimeout time.Duration
	// Logger defaults to the shared BlobCache logger.
	// It receives concurrent structured Worker logs and falls back to slog.Default.
	Logger Logger
	// Credential is sent as a bearer credential with the Worker's FlowService calls: startup index
	// verification and blob hydration. Default: nil sends none. A Dex server with authentication enabled requires it.
	Credential Credential
	// GrpcMaxMessageBytes limits the gRPC messages the Worker receives, in bytes: Step and RPC
	// invocations on the WorkerService listener and FlowService responses such as each LoadBlobs
	// response used to hydrate values. Zero uses DefaultGrpcMaxMessageBytes (32 MiB) and keeps
	// gRPC's unlimited send limit. A positive value also limits the messages the Worker sends,
	// including Step and RPC results. Negative values are invalid. The Dex server applies its own
	// api.grpcMaxMessageBytes, and the smaller limit wins.
	// Keep it above the largest application value plus headroom for the message envelope.
	GrpcMaxMessageBytes int
}

// PullWorkerOptions configures outbound polling of Dex task queues.
//
// Use it with WorkerOptions.Pull and advertise worker.WorkerTarget() to a Client.
// MaxConcurrentTasks defaults to runtime.NumCPU; IdleStreamsPerQueue defaults to one.
// StopDrainTimeout defaults to 30 seconds.
// Each stream runs one task at a time. Negative values and an invalid AppWorkerID
// cause NewWorker to return an error.
//
//	worker, err := dex.NewWorker(registry, cache, dex.WorkerOptions{
//		Pull: &dex.PullWorkerOptions{MaxConcurrentTasks: 8},
//	})
//	if err != nil { return err }
//	defer worker.Stop(context.Background())
type PullWorkerOptions struct {
	// MaxConcurrentTasks bounds simultaneous handlers. Zero uses runtime.NumCPU().
	MaxConcurrentTasks int
	// IdleStreamsPerQueue sets waiting streams for each Flow type and the sticky queue. Zero uses one.
	IdleStreamsPerQueue int
	// AppWorkerID overrides the generated process identity. Empty generates identity plus eight base32 characters.
	// Set a new value after each process restart. It must not equal a registered Flow type.
	// Valid values contain 1-63 ASCII letters, digits, dots, underscores, or hyphens.
	AppWorkerID string
	// StopDrainTimeout bounds Stop after it begins draining. Zero uses 30 seconds; negative values are invalid.
	StopDrainTimeout time.Duration
}

// Worker hosts registered Flows through a listening service or outbound pull streams.
//
// A Worker is one-shot: construct it, call Start once, then call Stop during
// shutdown. Start blocks while serving. Step and RPC handlers may execute
// concurrently, so application handler state must provide its own synchronization.
//
//	worker, err := dex.NewWorker(registry, cache, dex.WorkerOptions{})
//	if err != nil {
//		return err
//	}
//	go func() { serveErr <- worker.Start() }()
//	defer worker.Stop(context.Background())
type Worker struct {
	registry                  *Registry
	bindAddress               string
	workerTarget              WorkerTarget
	grpcServer                *grpc.Server
	flowConn                  *grpc.ClientConn
	flowService               dexpb.FlowServiceClient
	attributeIndexSyncTimeout time.Duration
	sdkVersion                string
	negotiatedProtocolVersion uint32
	logger                    Logger
	pull                      *pullWorker

	lifecycleMu sync.Mutex
	state       workerState
	done        chan struct{}
	finishOnce  sync.Once
}

type workerState uint8

const (
	workerCreated workerState = iota
	workerRunning
	workerStopping
	workerStopped
)

// NewWorker constructs a one-shot Worker from shared dependencies.
//
// registry supplies definitions and cache hydrates large values. It panics when
// registry or cache is nil. It validates listener, advertised target,
// FlowService address, synchronization timeout, and GrpcMaxMessageBytes, then creates the private gRPC
// server and FlowService connection without starting a listener. The returned
// Worker owns that connection; Start or Stop closes it.
func NewWorker(
	registry *Registry,
	cache *blobcache.Cache,
	options WorkerOptions,
) (*Worker, error) {
	if registry == nil {
		panic("dex.NewWorker requires Registry")
	}
	if cache == nil {
		panic("dex.NewWorker requires BlobCache")
	}
	bindAddress, target, flowServiceAddress, syncTimeout, err := resolveWorkerOptions(options)
	if err != nil {
		return nil, err
	}
	messageLimits, err := resolveGrpcMessageLimits(options.GrpcMaxMessageBytes)
	if err != nil {
		return nil, err
	}
	dialOptions := messageLimits.flowServiceDialOptions(options.Credential)
	if options.Pull != nil {
		dialOptions = append(dialOptions, grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time: 30 * time.Second, Timeout: 10 * time.Second, PermitWithoutStream: false,
		}))
	}
	flowConn, err := grpc.NewClient(flowServiceAddress, dialOptions...)
	if err != nil {
		return nil, fmt.Errorf("dex: create FlowService client: %w", err)
	}

	logger := resolveLogger(options.Logger, cache.Logger())
	grpcServer := grpc.NewServer(messageLimits.workerServiceServerOptions()...)
	flowService := dexpb.NewFlowServiceClient(flowConn)
	service := newWorkerService(
		registry,
		newValueHydrator(
			flowService,
			cache,
			logger,
			flowServiceAddress,
		),
		logger,
	)
	dexpb.RegisterWorkerServiceServer(grpcServer, service)
	var pull *pullWorker
	if options.Pull != nil {
		pull, err = newPullWorker(registry, service, flowConn, *options.Pull, currentGoSDKVersion())
		if err != nil {
			if closeErr := flowConn.Close(); closeErr != nil {
				logger.Error("close Worker FlowService connection", "error", closeErr)
			}
			return nil, err
		}
	}
	return &Worker{
		registry:                  registry,
		bindAddress:               bindAddress,
		workerTarget:              target,
		grpcServer:                grpcServer,
		flowConn:                  flowConn,
		flowService:               flowService,
		attributeIndexSyncTimeout: syncTimeout,
		sdkVersion:                currentGoSDKVersion(),
		logger:                    logger,
		pull:                      pull,
		state:                     workerCreated,
		done:                      make(chan struct{}),
	}, nil
}

func resolveWorkerOptions(
	options WorkerOptions,
) (string, WorkerTarget, string, time.Duration, error) {
	bindAddress := strings.TrimSpace(options.BindAddress)
	if options.Pull == nil && options.WorkerTarget.Pull {
		return "", WorkerTarget{}, "", 0, fmt.Errorf("dex: WorkerTarget.Pull requires WorkerOptions.Pull")
	}
	if options.Pull != nil {
		if bindAddress != "" || options.WorkerTarget.Address != "" || options.WorkerTarget.Headless {
			return "", WorkerTarget{}, "", 0, fmt.Errorf("dex: Pull Worker cannot configure BindAddress or listening WorkerTarget")
		}
	}
	if bindAddress == "" {
		bindAddress = defaultWorkerBindAddress
	}
	var target WorkerTarget
	if options.Pull != nil {
		target.Pull = true
	} else {
		bindHost, bindPort, err := validateWorkerBindAddress(bindAddress)
		if err != nil {
			return "", WorkerTarget{}, "", 0, err
		}
		target, err = resolveAdvertisedWorkerTarget(bindHost, bindPort, options.WorkerTarget)
		if err != nil {
			return "", WorkerTarget{}, "", 0, err
		}
	}
	flowServiceAddress := strings.TrimSpace(options.FlowServiceAddress)
	if flowServiceAddress == "" {
		flowServiceAddress = defaultFlowServiceTarget
	}
	if err := validatePlaintextTarget(flowServiceAddress, false); err != nil {
		return "", WorkerTarget{}, "", 0, fmt.Errorf(
			"dex: invalid FlowService address: %w",
			err,
		)
	}
	syncTimeout := options.AttributeIndexSyncTimeout
	if syncTimeout < 0 {
		return "", WorkerTarget{}, "", 0, fmt.Errorf(
			"dex: Attribute index sync timeout must be positive",
		)
	}
	if syncTimeout == 0 {
		syncTimeout = defaultAttributeIndexSyncTimeout
	}
	return bindAddress, target, flowServiceAddress, syncTimeout, nil
}

func validateWorkerTarget(target WorkerTarget) error {
	if target.Pull {
		if target.Address != "" || target.Headless {
			return fmt.Errorf("pull mode cannot be combined with address or headless mode")
		}
		return nil
	}
	return validatePlaintextTarget(target.Address, target.Headless)
}

func validateWorkerBindAddress(address string) (string, string, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", "", fmt.Errorf("dex: invalid Worker bind address %q: %w", address, err)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return "", "", fmt.Errorf("dex: invalid Worker bind address %q: port must be 1-65535", address)
	}
	return host, port, nil
}

func resolveAdvertisedWorkerTarget(
	bindHost string,
	bindPort string,
	target WorkerTarget,
) (WorkerTarget, error) {
	target.Address = strings.TrimSpace(target.Address)
	if target.Address == "" {
		targetHost := bindHost
		if targetHost == "" || targetHost == "0.0.0.0" || targetHost == "::" {
			targetHost = "localhost"
		}
		target.Address = net.JoinHostPort(targetHost, bindPort)
	}
	if err := validatePlaintextTarget(target.Address, target.Headless); err != nil {
		return WorkerTarget{}, fmt.Errorf("dex: invalid Worker target: %w", err)
	}
	return target, nil
}

func validatePlaintextTarget(address string, requireHostPort bool) error {
	trimmed := strings.TrimSpace(address)
	if trimmed != address {
		return fmt.Errorf("target address must not contain surrounding whitespace")
	}
	lower := strings.ToLower(trimmed)
	if lower == "" {
		return fmt.Errorf("target address must not be empty")
	}
	if strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://") {
		return fmt.Errorf("target %q must use plaintext gRPC", address)
	}
	if requireHostPort {
		_, port, err := net.SplitHostPort(address)
		if err != nil {
			return fmt.Errorf("headless target %q must use host:port: %w", address, err)
		}
		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 1 || portNumber > 65535 {
			return fmt.Errorf("headless target %q must use port 1-65535", address)
		}
	}
	return nil
}

// Start serves Worker tasks and blocks until Stop or a serve failure.
//
// Start may be called exactly once. Before serving, it negotiates a Server
// protocol, synchronizes Attribute indexes, and waits up to
// WorkerOptions.AttributeIndexSyncTimeout for each startup request.
// It returns nil after a normal Stop, or an error for synchronization, listener,
// serving, or invalid lifecycle state failures. Call Start on a dedicated goroutine
// when the application must continue doing other work.
// Poll stream failures are retried while the Worker runs; registration failures
// are logged with a longer retry delay.
func (worker *Worker) Start() error {
	worker.lifecycleMu.Lock()
	if worker.state != workerCreated {
		state := worker.state
		worker.lifecycleMu.Unlock()
		return fmt.Errorf("dex: Worker cannot start from state %s", state)
	}
	serverInfoCtx, cancelServerInfo := context.WithTimeout(
		context.Background(),
		worker.attributeIndexSyncTimeout,
	)
	serverInfo, err := worker.flowService.GetServerInfo(serverInfoCtx, &emptypb.Empty{})
	cancelServerInfo()
	if err != nil {
		worker.state = workerStopped
		worker.lifecycleMu.Unlock()
		worker.finish()
		return serverProtocolRequestError(worker.sdkVersion, err)
	}
	worker.negotiatedProtocolVersion, err = negotiateServerProtocol(serverInfo, worker.sdkVersion)
	if err != nil {
		worker.state = workerStopped
		worker.lifecycleMu.Unlock()
		worker.finish()
		return err
	}
	syncCtx, cancelSync := context.WithTimeout(context.Background(), worker.attributeIndexSyncTimeout)
	_, err = worker.flowService.SyncAttributeIndexes(syncCtx, &dexpb.SyncAttributeIndexRequest{
		AttributeIndexes: worker.registry.attributeIndexes,
	})
	cancelSync()
	if err != nil {
		worker.state = workerStopped
		worker.lifecycleMu.Unlock()
		worker.finish()
		return fmt.Errorf("dex: sync attribute indexes: %w", err)
	}
	if worker.pull != nil {
		worker.state = workerRunning
		worker.lifecycleMu.Unlock()
		worker.pull.run()
		worker.lifecycleMu.Lock()
		worker.state = workerStopped
		worker.lifecycleMu.Unlock()
		worker.finish()
		return nil
	}
	listener, err := net.Listen("tcp", worker.bindAddress)
	if err != nil {
		worker.state = workerStopped
		worker.lifecycleMu.Unlock()
		worker.finish()
		return fmt.Errorf("dex: listen on %q: %w", worker.bindAddress, err)
	}
	worker.state = workerRunning
	worker.lifecycleMu.Unlock()

	serveErr := worker.grpcServer.Serve(listener)
	if errors.Is(serveErr, grpc.ErrServerStopped) {
		serveErr = nil
	}
	worker.lifecycleMu.Lock()
	worker.state = workerStopped
	worker.lifecycleMu.Unlock()
	worker.finish()
	if serveErr != nil {
		return fmt.Errorf("dex: serve WorkerService: %w", serveErr)
	}
	return nil
}

// Stop drains handlers until ctx expires, then force-stops the Worker.
//
// Stop accepts calls before Start and repeated calls after shutdown. A nil context is
// invalid. If ctx expires while handlers are draining, Stop force-closes the gRPC
// server or connection, waits for cleanup, and returns ctx.Err. Pull mode also
// limits the drain with PullWorkerOptions.StopDrainTimeout. Stop closes the Worker's internal
// FlowService connection before returning. A forced pull Stop waits for handlers to return;
// a handler ignoring ctx can block Stop. Push mode does not wait after forcing its gRPC server closed.
func (worker *Worker) Stop(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("dex: Worker Stop context must not be nil")
	}

	worker.lifecycleMu.Lock()
	switch worker.state {
	case workerCreated:
		worker.state = workerStopped
		worker.lifecycleMu.Unlock()
		worker.grpcServer.Stop()
		worker.finish()
		return nil
	case workerRunning:
		worker.state = workerStopping
		if worker.pull != nil {
			worker.pull.stop()
		} else {
			go worker.grpcServer.GracefulStop()
		}
	case workerStopping:
	case workerStopped:
		worker.lifecycleMu.Unlock()
		return nil
	}
	done := worker.done
	worker.lifecycleMu.Unlock()
	if worker.pull != nil {
		var cancelDrain context.CancelFunc
		ctx, cancelDrain = context.WithTimeout(ctx, worker.pull.stopDrainTimeout)
		defer cancelDrain()
	}

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		if worker.pull != nil {
			worker.pull.cancel()
		} else {
			worker.grpcServer.Stop()
		}
		<-done
		return ctx.Err()
	}
}

func (worker *Worker) finish() {
	worker.finishOnce.Do(func() {
		if worker.pull != nil {
			worker.pull.cancel()
		}
		if err := worker.flowConn.Close(); err != nil {
			worker.logger.Error("close Worker FlowService connection", "error", err)
		}
		close(worker.done)
	})
}

// WorkerTarget returns a copy suitable for FlowConfig.WorkerTarget.
//
// The result is also suitable for ClientOptions.WorkerTarget. Mutating it does not
// reconfigure the Worker.
func (worker *Worker) WorkerTarget() *WorkerTarget {
	target := worker.workerTarget
	return &target
}

func (state workerState) String() string {
	switch state {
	case workerCreated:
		return "created"
	case workerRunning:
		return "running"
	case workerStopping:
		return "stopping"
	case workerStopped:
		return "stopped"
	default:
		return "unknown"
	}
}
