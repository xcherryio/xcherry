// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package dev

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"time"

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	dexweb "github.com/superdurable/dex/web"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type zdProcess = childProcess

type zdRunResources struct {
	ctx             context.Context
	shutdownTimeout time.Duration
	listeners       *ownedListeners
	logs            *serverLogDir
	process         *zdProcess
	connection      *grpc.ClientConn
	web             *dexweb.Server
}

func (resources *zdRunResources) closeInto(runErr *error) {
	if resources.web != nil {
		shutdownCtx, cancelShutdown := context.WithTimeout(context.WithoutCancel(resources.ctx), resources.shutdownTimeout)
		webErr := resources.web.Shutdown(shutdownCtx)
		cancelShutdown()
		if webErr != nil && !errors.Is(webErr, context.Canceled) {
			*runErr = errors.Join(*runErr, fmt.Errorf("stop Dex Web: %w", webErr))
		}
	}
	if resources.connection != nil {
		*runErr = errors.Join(*runErr, resources.connection.Close())
	}
	if resources.process != nil {
		*runErr = errors.Join(*runErr, resources.process.Stop(resources.shutdownTimeout))
	}
	if resources.logs != nil {
		shouldRemove := resources.logs.isEphemeral && *runErr == nil
		if resources.logs.isEphemeral && *runErr != nil {
			*runErr = fmt.Errorf("%w (server log folder %s)", *runErr, resources.logs.directory)
		}
		*runErr = errors.Join(*runErr, resources.logs.Close(shouldRemove))
	}
	if resources.listeners != nil {
		*runErr = errors.Join(*runErr, resources.listeners.Close())
	}
}

func startZDProcess(cfg *Config, dataDirectory string, dexAddress string, logs io.Writer) (*zdProcess, error) {
	arguments := []string{"dev", "--data-dir", dataDirectory, "--listen", dexAddress}
	if cfg.isExplicit("server-log-folder") {
		arguments = append(arguments, "--log-dir", cfg.LogDirectory)
	}
	command := exec.Command(cfg.ZDBinary, arguments...)
	return startChildProcess(command, logs, "dex-server-zd", true)
}

func waitForZD(ctx context.Context, address string, process *zdProcess) (*grpc.ClientConn, error) {
	connection, err := grpc.NewClient(address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(config.DefaultGrpcMaxMessageBytes),
			grpc.MaxCallSendMsgSize(config.DefaultGrpcMaxMessageBytes),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create dex-server-zd client: %w", err)
	}
	healthClient := healthpb.NewHealthClient(connection)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	services := []string{"", dexpb.FlowService_ServiceDesc.ServiceName, dexpb.WorkerTaskService_ServiceDesc.ServiceName}
	var lastErr error
	var lastStatusErr error
	for {
		allServing := true
		for _, service := range services {
			checkCtx, cancel := context.WithTimeout(ctx, time.Second)
			response, checkErr := healthClient.Check(checkCtx, &healthpb.HealthCheckRequest{Service: service})
			cancel()
			if checkErr != nil || response.GetStatus() != healthpb.HealthCheckResponse_SERVING {
				allServing = false
				if checkErr != nil {
					lastErr = fmt.Errorf("%q health check: %w", service, checkErr)
				} else {
					lastErr = fmt.Errorf("%q health status is %s", service, response.GetStatus())
					lastStatusErr = lastErr
				}
				break
			}
		}
		if allServing {
			return connection, nil
		}
		select {
		case <-ctx.Done():
			return nil, errors.Join(fmt.Errorf("wait for dex-server-zd at %s: %w", address, errors.Join(ctx.Err(), lastStatusErr, lastErr)), connection.Close())
		case <-process.done:
			if process.Err() == nil {
				return nil, errors.Join(errors.New("dex-server-zd exited before readiness"), connection.Close())
			}
			return nil, errors.Join(fmt.Errorf("dex-server-zd exited before readiness: %w", process.Err()), connection.Close())
		case <-ticker.C:
		}
	}
}
