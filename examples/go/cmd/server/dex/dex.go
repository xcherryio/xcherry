// Copyright (c) 2022-2026 Super Durable, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package dex

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/superdurable/dex/blob-cache-go/blobcache"
	"github.com/superdurable/dex/examples/go/patterns/cron"
	drainexternal "github.com/superdurable/dex/examples/go/patterns/drain-channels/external-publishing"
	draininternal "github.com/superdurable/dex/examples/go/patterns/drain-channels/internal-drain"
	"github.com/superdurable/dex/examples/go/patterns/entity-store"
	hashpartitionedattributemap "github.com/superdurable/dex/examples/go/patterns/hash-partitioned-attribute-map"
	inactivenesstrackertimer "github.com/superdurable/dex/examples/go/patterns/inactiveness-tracker-timer"
	"github.com/superdurable/dex/examples/go/patterns/interruptible"
	"github.com/superdurable/dex/examples/go/patterns/intervention"
	"github.com/superdurable/dex/examples/go/patterns/parallel"
	parallelsubflows "github.com/superdurable/dex/examples/go/patterns/parallel-subflows"
	patternspolling "github.com/superdurable/dex/examples/go/patterns/polling"
	"github.com/superdurable/dex/examples/go/patterns/recovery"
	"github.com/superdurable/dex/examples/go/patterns/reminders"
	sequentiallychunkedattributemap "github.com/superdurable/dex/examples/go/patterns/sequentially-chunked-attribute-map"
	"github.com/superdurable/dex/examples/go/patterns/timeout"
	"github.com/superdurable/dex/examples/go/patterns/wait-for-step-completion"
	primitiveattribute "github.com/superdurable/dex/examples/go/primitives/attribute"
	primitivechannel "github.com/superdurable/dex/examples/go/primitives/channel"
	primitiveclientapis "github.com/superdurable/dex/examples/go/primitives/client-apis"
	primitivecustomretry "github.com/superdurable/dex/examples/go/primitives/custom-retry"
	primitivedurability "github.com/superdurable/dex/examples/go/primitives/durability"
	primitiveflow "github.com/superdurable/dex/examples/go/primitives/flow"
	primitiveheartbeat "github.com/superdurable/dex/examples/go/primitives/heartbeat"
	primitiveoptionsoverride "github.com/superdurable/dex/examples/go/primitives/options-override"
	primitiveproceedonwaitfailure "github.com/superdurable/dex/examples/go/primitives/proceed-on-wait-failure"
	primitiverpc "github.com/superdurable/dex/examples/go/primitives/rpc"
	primitivestep "github.com/superdurable/dex/examples/go/primitives/step"
	primitivestepdecision "github.com/superdurable/dex/examples/go/primitives/step-decision"
	primitivestream "github.com/superdurable/dex/examples/go/primitives/stream"
	primitivesubflow "github.com/superdurable/dex/examples/go/primitives/subflow"
	primitivetimer "github.com/superdurable/dex/examples/go/primitives/timer"
	primitivewaittypes "github.com/superdurable/dex/examples/go/primitives/wait-types"
	customerrefund "github.com/superdurable/dex/examples/go/products/customer-refund"
	"github.com/superdurable/dex/examples/go/products/engagement"
	"github.com/superdurable/dex/examples/go/products/job-post"
	"github.com/superdurable/dex/examples/go/products/microservices"
	"github.com/superdurable/dex/examples/go/products/money-transfer"
	"github.com/superdurable/dex/examples/go/products/order-processing"
	"github.com/superdurable/dex/examples/go/products/signup"
	"github.com/superdurable/dex/examples/go/products/subscription"
	"github.com/superdurable/dex/examples/go/registry"
	"github.com/superdurable/dex/examples/go/server/httputil"
	"github.com/superdurable/dex/examples/go/shared/service"
	"github.com/superdurable/dex/examples/go/shared/workermode"
	sdk "github.com/superdurable/dex/sdk-go/dex"
)

type sampleServer struct {
	client        *sdk.Client
	worker        *sdk.Worker
	cache         *blobcache.Cache
	httpServer    *http.Server
	workerAddress string
	workerMode    workermode.WorkerMode
	workerResult  chan error
	httpResult    chan error
}

func Run(ctx context.Context) error {
	server, err := newSampleServer()
	if err != nil {
		return err
	}
	return server.Run(ctx)
}

func newSampleServer() (*sampleServer, error) {
	cache, err := blobcache.New(&blobcache.Config{
		Dir:      environmentOr("DEX_BLOB_CACHE_DIR", filepath.Join(os.TempDir(), "dex-go-examples-blobs")),
		MaxBytes: 1 << 30,
	})
	if err != nil {
		return nil, fmt.Errorf("create blob cache: %w", err)
	}
	var client *sdk.Client
	flows := registry.New(service.NewMyService(), func() *sdk.Client { return client })
	flowRegistry, err := sdk.NewRegistry(flows)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("register example flows: %w", err), cache.Close())
	}
	workerMode, err := workermode.ReadWorkerModeFromEnvironment()
	if err != nil {
		return nil, errors.Join(err, cache.Close())
	}
	workerOptions := newWorkerOptions(workerMode)
	worker, err := sdk.NewWorker(flowRegistry, cache, workerOptions)
	if err != nil {
		return nil, errors.Join(err, cache.Close())
	}
	client, err = sdk.NewClient(flowRegistry, cache, sdk.ClientOptions{
		FlowServiceAddress: os.Getenv("DEX_FLOW_SERVICE_ADDRESS"),
		WorkerTarget:       worker.WorkerTarget(),
	})
	if err != nil {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return nil, errors.Join(err, worker.Stop(stopCtx), cache.Close())
	}
	server := &sampleServer{
		client:        client,
		worker:        worker,
		cache:         cache,
		workerAddress: workerOptions.BindAddress,
		workerMode:    workerMode,
		workerResult:  make(chan error, 1),
		httpResult:    make(chan error, 1),
	}
	server.httpServer = &http.Server{
		Addr:    environmentOr("DEX_EXAMPLES_HTTP_ADDRESS", "127.0.0.1:8080"),
		Handler: server.router(),
	}
	return server, nil
}

func newWorkerOptions(workerMode workermode.WorkerMode) sdk.WorkerOptions {
	if workerMode.IsPull() {
		return sdk.WorkerOptions{
			FlowServiceAddress: os.Getenv("DEX_FLOW_SERVICE_ADDRESS"),
			Pull:               workermode.NewExamplePullWorkerOptions(),
		}
	}
	workerOptions := sdk.WorkerOptions{
		BindAddress:        environmentOr("DEX_WORKER_BIND_ADDRESS", "127.0.0.1:8803"),
		FlowServiceAddress: os.Getenv("DEX_FLOW_SERVICE_ADDRESS"),
	}
	if target := os.Getenv("DEX_WORKER_TARGET"); target != "" {
		workerOptions.WorkerTarget.Address = target
	}
	return workerOptions
}

func environmentOr(name string, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func (server *sampleServer) router() http.Handler {
	return NewRouter(server.client)
}

// NewRouter builds the Go examples HTTP API.
func NewRouter(client *sdk.Client) http.Handler {
	router := gin.Default()
	router.Use(httputil.AllowCORS())
	subscription.RegisterRoutes(router, client, registry.Subscription)
	engagement.RegisterRoutes(router, client, registry.Engagement)
	microservices.RegisterRoutes(router, client, registry.Microservices)
	moneytransfer.RegisterRoutes(router, client, registry.MoneyTransfer)
	orderprocessing.RegisterRoutes(router, client, registry.OrderProcessing)
	signup.RegisterRoutes(router, client, registry.UserOnboarding)
	jobpost.RegisterRoutes(router, client, registry.JobPosting)
	customerrefund.RegisterRoutes(router, client, registry.CustomerRefund, registry.AgenticRefund)
	patternspolling.RegisterRoutes(router, client, registry.Polling, registry.PollingJobs)
	interruptible.RegisterRoutes(router, client, registry.Interruptible)
	reminders.RegisterRoutes(router, client, registry.Reminder)
	entitystore.RegisterRoutes(router, client, registry.UserProfile)
	intervention.RegisterRoutes(router, client, registry.ManualRecovery)
	inactivenesstrackertimer.RegisterRoutes(router, client, registry.InactivenessTracker)
	parallel.RegisterRoutes(router, client, registry.StaticParallel, registry.DynamicParallel, registry.AwaitParallel, registry.FirstWinParallel)
	parallelsubflows.RegisterRoutes(router, client, registry.BasicSubFlows, registry.WaitForHalfSubFlows, registry.LongLiveSubFlows, registry.ShortLiveSubFlows, registry.SubmitSubFlowRequest)
	recovery.RegisterRoutes(router, client, registry.FailureRecovery)
	draininternal.RegisterRoutes(router, client, registry.DrainInternal)
	drainexternal.RegisterRoutes(router, client, registry.DrainExternal)
	waitforstepcompletion.RegisterRoutes(router, client, registry.WaitForStepCompletion)
	timeout.RegisterRoutes(router, client, registry.GracefulTimeout)
	sequentiallychunkedattributemap.RegisterRoutes(router, client, registry.ChunkedSubscriber)
	hashpartitionedattributemap.RegisterRoutes(router, client, registry.CustomerDirectory)
	primitiveflow.RegisterRoutes(router, client, registry.ExampleFlow)
	primitivestep.RegisterRoutes(router, client, registry.Step, registry.StepRetry)
	primitivecustomretry.RegisterRoutes(router, client, registry.CustomRetry)
	primitivedurability.RegisterRoutes(router, client, registry.Durability)
	primitiveheartbeat.RegisterRoutes(router, client, registry.Heartbeat)
	primitiveoptionsoverride.RegisterRoutes(router, client, registry.OptionsOverride)
	primitiveproceedonwaitfailure.RegisterRoutes(router, client, registry.ProceedOnWaitFailure)
	primitivestepdecision.RegisterRoutes(router, client, registry.StepDecision)
	primitivewaittypes.RegisterRoutes(router, client, registry.WaitTypes)
	primitiveattribute.RegisterRoutes(router, client, registry.Attribute)
	primitivechannel.RegisterRoutes(router, client, registry.Channel)
	primitivestream.RegisterRoutes(router, client, registry.Stream)
	primitivetimer.RegisterRoutes(router, client, registry.Timer)
	primitiverpc.RegisterRoutes(router, client, registry.Rpc)
	primitivesubflow.RegisterRoutes(router, client, registry.SubFlowParent)
	primitiveclientapis.RegisterRoutes(router, client, registry.ClientApis)
	return router
}

func (server *sampleServer) Run(ctx context.Context) error {
	go func() {
		server.workerResult <- server.worker.Start()
	}()
	if err := server.waitForWorker(ctx); err != nil {
		return errors.Join(err, server.close())
	}
	startCronSchedule(server.client)
	go func() {
		err := server.httpServer.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		server.httpResult <- err
	}()

	var runErr error
	select {
	case <-ctx.Done():
	case err := <-server.workerResult:
		runErr = err
	case err := <-server.httpResult:
		runErr = err
	}
	return errors.Join(runErr, server.close())
}

func (server *sampleServer) waitForWorker(ctx context.Context) error {
	if server.workerMode.IsPull() {
		registrationCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		return server.worker.WaitPullRegistered(registrationCtx)
	}
	address, err := localWorkerAddress(server.workerAddress)
	if err != nil {
		return err
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		connection, dialErr := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if dialErr == nil {
			if closeErr := connection.Close(); closeErr != nil {
				return fmt.Errorf("close Worker readiness connection: %w", closeErr)
			}
			return nil
		}
		select {
		case workerErr := <-server.workerResult:
			if workerErr == nil {
				return errors.New("Worker stopped before becoming ready")
			}
			return workerErr
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func localWorkerAddress(bindAddress string) (string, error) {
	host, port, err := net.SplitHostPort(bindAddress)
	if err != nil {
		return "", fmt.Errorf("parse Worker bind address: %w", err)
	}
	switch host {
	case "", "0.0.0.0":
		host = "127.0.0.1"
	case "::":
		host = "::1"
	}
	return net.JoinHostPort(host, port), nil
}

func (server *sampleServer) close() error {
	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	httpErr := server.httpServer.Shutdown(stopCtx)
	workerErr := server.worker.Stop(stopCtx)
	clientErr := server.client.Close()
	cacheErr := server.cache.Close()
	return errors.Join(httpErr, workerErr, clientErr, cacheErr)
}

func startCronSchedule(client *sdk.Client) {
	timeout := 24 * time.Hour
	_, err := client.StartFlow(
		context.Background(),
		registry.CronSchedule,
		"cron-schedule-sample",
		cron.CronScheduleInput{
			Interval: cron.Interval{Value: 1, Unit: cron.Hour},
			RunCount: 10,
		},
		sdk.StartFlowOptions{
			Timeout: &timeout,
		},
	)
	if err != nil {
		var duplicate *sdk.FlowAlreadyStartedError
		if errors.As(err, &duplicate) {
			return
		}
		panic(fmt.Errorf("start cron schedule sample: %w", err))
	}
}
