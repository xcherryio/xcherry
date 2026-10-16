// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package credential

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/credentials"
)

const fileCheckInterval = time.Second

type bearerCredential struct {
	path string

	mu        sync.Mutex
	token     string
	modTime   time.Time
	size      int64
	checkedAt time.Time
	loadErr   error
}

// File reads a bearer token and checks for rotation at most once per second.
func File(path string) (credentials.PerRPCCredentials, error) {
	if path == "" {
		return nil, fmt.Errorf("bearer credential file path is empty")
	}
	credential := &bearerCredential{path: path}
	credential.refreshLocked()
	if credential.loadErr != nil {
		return nil, credential.loadErr
	}
	return credential, nil
}

// Static holds a bearer token for every RPC.
func Static(token string) (credentials.PerRPCCredentials, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return nil, fmt.Errorf("bearer credential is empty")
	}
	return &bearerCredential{token: token}, nil
}

func (c *bearerCredential) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.path != "" && time.Since(c.checkedAt) >= fileCheckInterval {
		c.refreshLocked()
	}
	if c.loadErr != nil {
		return nil, c.loadErr
	}
	return map[string]string{"authorization": "Bearer " + c.token}, nil
}

func (*bearerCredential) RequireTransportSecurity() bool { return false }

func (c *bearerCredential) refreshLocked() {
	c.checkedAt = time.Now()
	info, err := os.Stat(c.path)
	if err != nil {
		c.token, c.loadErr = "", fmt.Errorf("read bearer credential file %s: %w", c.path, err)
		return
	}
	if c.loadErr == nil && c.token != "" && info.ModTime().Equal(c.modTime) && info.Size() == c.size {
		return
	}
	data, err := os.ReadFile(c.path)
	if err != nil {
		c.token, c.loadErr = "", fmt.Errorf("read bearer credential file %s: %w", c.path, err)
		return
	}
	token := string(bytes.TrimSpace(data))
	if token == "" {
		c.token, c.loadErr = "", fmt.Errorf("bearer credential file %s is empty", c.path)
		return
	}
	c.token, c.loadErr, c.modTime, c.size = token, nil, info.ModTime(), info.Size()
}
