// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package grpctarget

import (
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/superdurable/dex/gen/dexpb"
)

// NormalizeWorkerTarget validates a pull target or a plaintext gRPC target and applies host rewrites.
func NormalizeWorkerTarget(target *dexpb.WorkerTarget) (*dexpb.WorkerTarget, error) {
	if target == nil {
		return nil, fmt.Errorf("worker_target is required")
	}
	if target.GetIsPullMode() {
		if target.GetAddress() != "" {
			return nil, fmt.Errorf("pull worker_target cannot specify address")
		}
		if target.GetIsHeadlessAddress() {
			return nil, fmt.Errorf("pull worker_target cannot use headless address")
		}
		return &dexpb.WorkerTarget{IsPullMode: true}, nil
	}
	address, err := NormalizeAddress(target.GetAddress())
	if err != nil {
		return nil, err
	}
	if target.GetIsHeadlessAddress() {
		if _, _, err := net.SplitHostPort(address); err != nil {
			return nil, fmt.Errorf("headless worker_target %q must use host:port: %w", address, err)
		}
	}
	return &dexpb.WorkerTarget{
		Address:           address,
		IsHeadlessAddress: target.GetIsHeadlessAddress(),
	}, nil
}

// NormalizeAddress validates a plaintext gRPC address and applies optional host rewrites.
func NormalizeAddress(address string) (string, error) {
	address = strings.TrimSpace(address)
	if address == "" {
		return "", fmt.Errorf("worker_target is empty")
	}
	lower := strings.ToLower(address)
	if strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://") {
		return "", fmt.Errorf("HTTP(S) worker_target %q rejected; use a plaintext gRPC target (host:port)", address)
	}

	autofixHost := os.Getenv("AUTO_FIX_WORKER_URL")
	if autofixHost != "" {
		address = strings.Replace(address, "localhost", autofixHost, 1)
		address = strings.Replace(address, "127.0.0.1", autofixHost, 1)
	}
	autofixPortEnv := os.Getenv("AUTO_FIX_WORKER_PORT_FROM_ENV")
	if autofixPortEnv != "" {
		envVal := os.Getenv(autofixPortEnv)
		address = strings.Replace(address, "$"+autofixPortEnv+"$", envVal, 1)
	}
	return address, nil
}
