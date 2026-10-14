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
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func Healthcheck(parent context.Context) error {
	target := envDefault("DEX_HEALTH_TARGET", fmt.Sprintf("127.0.0.1:%d", envIntOrDefault("DEX_API_PORT", defaultDexPort)))
	timeout, err := envDuration("HEALTHCHECK_TIMEOUT", 3*time.Second)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("connect to Dex health endpoint: %w", err)
	}
	defer conn.Close()
	response, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		return fmt.Errorf("check Dex health: %w", err)
	}
	if response.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("Dex health status is %s", response.GetStatus())
	}
	_, _ = fmt.Fprintln(os.Stdout, "SERVING")
	return nil
}
