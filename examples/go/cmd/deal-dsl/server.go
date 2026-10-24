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

package main

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
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/superdurable/dex/blob-cache-go/blobcache"
	"github.com/superdurable/dex/examples/go/products/deal-dsl"
	"github.com/superdurable/dex/examples/go/server/httputil"
	"github.com/superdurable/dex/examples/go/shared/workermode"
	sdk "github.com/superdurable/dex/sdk-go/dex"
)

const defaultDealDSLPostgresURL = "postgres://deal_dsl:deal_dsl@127.0.0.1:15432/deal_dsl?sslmode=disable"

type dealDSLServer struct {
	client         *sdk.Client
	worker         *sdk.Worker
	cache          *blobcache.Cache
	database       *pgxpool.Pool
	dealFlow       *dealdsl.DealDSLFlow
	dealRepository dealdsl.Repository
	httpServer     *http.Server
	workerAddress  string
	workerMode     workermode.WorkerMode
	workerResult   chan error
	httpResult     chan error
}

func run(ctx context.Context) error {
	server, err := newDealDSLServer(ctx)
	if err != nil {
		return err
	}
	return server.serve(ctx)
}

func newDealDSLServer(ctx context.Context) (*dealDSLServer, error) {
	cache, err := blobcache.New(&blobcache.Config{
		Dir:      environmentOr("DEX_BLOB_CACHE_DIR", filepath.Join(os.TempDir(), "dex-deal-dsl-blobs")),
		MaxBytes: 1 << 30,
	})
	if err != nil {
		return nil, fmt.Errorf("create blob cache: %w", err)
	}
	database, dealRepository, err := newDealDSLRepository(ctx)
	if err != nil {
		return nil, errors.Join(err, cache.Close())
	}
	dealFlow := dealdsl.NewDealDSLFlow(cache.Logger())
	flowRegistry, err := sdk.NewRegistry([]sdk.Flow{dealFlow})
	if err != nil {
		database.Close()
		return nil, errors.Join(fmt.Errorf("register Deal DSL flow: %w", err), cache.Close())
	}
	workerMode, err := workermode.ReadWorkerModeFromEnvironment()
	if err != nil {
		database.Close()
		return nil, errors.Join(err, cache.Close())
	}
	workerOptions := newDealDSLWorkerOptions(workerMode)
	worker, err := sdk.NewWorker(flowRegistry, cache, workerOptions)
	if err != nil {
		database.Close()
		return nil, errors.Join(err, cache.Close())
	}
	client, err := sdk.NewClient(flowRegistry, cache, sdk.ClientOptions{
		FlowServiceAddress: os.Getenv("DEX_FLOW_SERVICE_ADDRESS"),
		WorkerTarget:       worker.WorkerTarget(),
	})
	if err != nil {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		database.Close()
		return nil, errors.Join(err, worker.Stop(stopCtx), cache.Close())
	}
	server := &dealDSLServer{
		client:         client,
		worker:         worker,
		cache:          cache,
		database:       database,
		dealFlow:       dealFlow,
		dealRepository: dealRepository,
		workerAddress:  workerOptions.BindAddress,
		workerMode:     workerMode,
		workerResult:   make(chan error, 1),
		httpResult:     make(chan error, 1),
	}
	server.httpServer = &http.Server{
		Addr:    environmentOr("DEX_EXAMPLES_HTTP_ADDRESS", "127.0.0.1:8080"),
		Handler: server.router(),
	}
	return server, nil
}

func newDealDSLRepository(
	ctx context.Context,
) (*pgxpool.Pool, *dealdsl.PostgresRepository, error) {
	database, err := pgxpool.New(
		ctx,
		environmentOr("DEAL_DSL_POSTGRES_URL", defaultDealDSLPostgresURL),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("configure Deal DSL PostgreSQL: %w", err)
	}
	if err := database.Ping(ctx); err != nil {
		database.Close()
		return nil, nil, fmt.Errorf("connect to Deal DSL PostgreSQL: %w", err)
	}
	repository := dealdsl.NewPostgresRepository(database)
	if err := repository.EnsureSchema(ctx); err != nil {
		database.Close()
		return nil, nil, err
	}
	return database, repository, nil
}

func (server *dealDSLServer) router() http.Handler {
	router := gin.Default()
	router.Use(httputil.AllowCORS())
	dealdsl.RegisterRoutes(router, server.client, server.dealFlow, server.dealRepository)
	return router
}

func (server *dealDSLServer) serve(ctx context.Context) error {
	go func() {
		server.workerResult <- server.worker.Start()
	}()
	if err := server.waitForWorker(ctx); err != nil {
		return errors.Join(err, server.close())
	}
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

func (server *dealDSLServer) waitForWorker(ctx context.Context) error {
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

func (server *dealDSLServer) close() error {
	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	httpErr := server.httpServer.Shutdown(stopCtx)
	workerErr := server.worker.Stop(stopCtx)
	clientErr := server.client.Close()
	cacheErr := server.cache.Close()
	server.database.Close()
	return errors.Join(httpErr, workerErr, clientErr, cacheErr)
}

func newDealDSLWorkerOptions(workerMode workermode.WorkerMode) sdk.WorkerOptions {
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
