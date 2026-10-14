// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/superdurable/dex/all-in-one/internal/allinone"
)

var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

func main() {
	if err := run(); err != nil {
		slog.Error("dex-server-all-in-one stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "healthcheck":
			return allinone.Healthcheck(context.Background())
		case "version":
			fmt.Printf("dex-server-all-in-one %s (%s, %s)\n", version, commit, date)
			return nil
		case "serve":
		default:
			return fmt.Errorf("unknown command %q", os.Args[1])
		}
	}

	cfg, err := allinone.LoadConfig()
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer cancel()
	slog.Info("starting dex-server-all-in-one", "version", version, "commit", commit)
	return allinone.Run(ctx, cfg)
}
