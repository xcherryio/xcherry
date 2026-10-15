// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package allinone

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/superdurable/dex/service/bootstrap"
	_ "github.com/superdurable/xtemporal/common/persistence/sql/sqlplugin/postgresql"
	"github.com/superdurable/xtemporal/temporal"
)

func Run(ctx context.Context, cfg *Config) error {
	if cfg == nil {
		return fmt.Errorf("config is required")
	}
	if err := initializeDatabase(ctx, cfg.Database); err != nil {
		return fmt.Errorf("initialize Temporal databases: %w", err)
	}
	temporalServer, err := temporal.NewServer(
		temporal.ForServices(temporal.DefaultServices),
		temporal.WithConfig(cfg.Temporal),
	)
	if err != nil {
		return fmt.Errorf("create Temporal server: %w", err)
	}
	if err := temporalServer.Start(); err != nil {
		return fmt.Errorf("start Temporal server: %w", err)
	}
	slog.Info("Temporal services started", "frontend", cfg.FrontendTarget)

	startupCtx, cancelStartup := context.WithTimeout(ctx, cfg.StartupTimeout)
	defer cancelStartup()
	if err := ensureNamespace(startupCtx, cfg.FrontendTarget, cfg.Namespace, cfg.Retention); err != nil {
		return errors.Join(fmt.Errorf("initialize Temporal namespace: %w", err), temporalServer.Stop())
	}

	var dexRuntime *bootstrap.Runtime
	if err := withDatabaseLock(startupCtx, cfg.Database, s3LockID, func() error {
		var err error
		dexRuntime, err = createDexRuntime(startupCtx, cfg)
		return err
	}); err != nil {
		return errors.Join(fmt.Errorf("create Dex runtime: %w", err), temporalServer.Stop())
	}
	cancelStartup()
	slog.Info("Dex services started", "port", cfg.Dex.Api.Port, "namespace", cfg.Namespace)
	runErr := dexRuntime.Run(ctx)
	stopErr := temporalServer.Stop()
	if runErr != nil {
		runErr = fmt.Errorf("run Dex server: %w", runErr)
	}
	if stopErr != nil {
		stopErr = fmt.Errorf("stop Temporal server: %w", stopErr)
	}
	return errors.Join(runErr, stopErr)
}

func createDexRuntime(ctx context.Context, cfg *Config) (*bootstrap.Runtime, error) {
	var lastErr error
	for ctx.Err() == nil {
		runtime, err := bootstrap.New(cfg.Dex, &bootstrap.Options{
			Services: bootstrap.Services{API: true, Interpreter: true, Matching: true},
		})
		if err == nil {
			return runtime, nil
		}
		lastErr = err
		if err := waitRetry(ctx); err != nil {
			break
		}
	}
	return nil, fmt.Errorf("Dex dependencies did not become ready: %w", lastErr)
}
