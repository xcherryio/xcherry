// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package dex

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
)

// credentialFileCheckInterval bounds how often a call may stat the credential file.
const credentialFileCheckInterval = time.Second

// Credential supplies the bearer credential that a Client or Worker sends with every FlowService call.
//
// A Dex server with authentication enabled requires a credential on every public API call. Use FileCredential for a token that another
// process, such as a Kubernetes projected service account token, writes and rotates.
//
//	credential, err := dex.FileCredential("/var/run/secrets/dex/token")
//	if err != nil {
//		return err
//	}
//	client, err := dex.NewClient(registry, cache, dex.ClientOptions{Credential: credential})
//
// The credential is sent over the same plaintext connection as every other call. Protect the
// network path between the application and Dex accordingly.
type Credential interface {
	// Token returns the current bearer credential. It is called before every FlowService call,
	// may be called concurrently, and must return quickly.
	//
	// ctx is the context of the call. An error fails the call without contacting Dex.
	Token(ctx context.Context) (string, error)
}

// FileCredential returns a Credential that reads a bearer token from path.
//
// The file is read at construction and reread whenever its modification time or size changes,
// checked at most once per second, so a rotated token takes effect without a restart.
// Surrounding whitespace is ignored. A file that becomes empty or unreadable fails the calls
// that need it until it is restored, rather than sending a stale token.
//
// path is the token file. It returns an error when the file cannot be read or holds no token.
func FileCredential(path string) (Credential, error) {
	credential := &fileCredential{path: path}
	if _, err := credential.load(); err != nil {
		return nil, err
	}
	return credential, nil
}

type fileCredential struct {
	path string

	mu        sync.Mutex
	token     string
	modTime   time.Time
	size      int64
	checkedAt time.Time
	loadErr   error
}

func (c *fileCredential) Token(context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if time.Since(c.checkedAt) >= credentialFileCheckInterval {
		c.refreshLocked()
	}
	return c.token, c.loadErr
}

func (c *fileCredential) load() (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.refreshLocked()
	return c.token, c.loadErr
}

func (c *fileCredential) refreshLocked() {
	c.checkedAt = time.Now()
	info, err := os.Stat(c.path)
	if err != nil {
		c.token, c.loadErr = "", fmt.Errorf("dex: read credential file: %w", err)
		return
	}
	if c.loadErr == nil && c.token != "" && info.ModTime().Equal(c.modTime) && info.Size() == c.size {
		return
	}
	data, err := os.ReadFile(c.path)
	if err != nil {
		c.token, c.loadErr = "", fmt.Errorf("dex: read credential file: %w", err)
		return
	}
	token := string(bytes.TrimSpace(data))
	if token == "" {
		c.token, c.loadErr = "", fmt.Errorf("dex: credential file %s is empty", c.path)
		return
	}
	c.token, c.loadErr, c.modTime, c.size = token, nil, info.ModTime(), info.Size()
}

// credentialDialOptions returns the dial option that attaches credential, or none without one.
func credentialDialOptions(credential Credential) []grpc.DialOption {
	if credential == nil {
		return nil
	}
	return []grpc.DialOption{grpc.WithPerRPCCredentials(perRPCCredential{credential: credential})}
}

type perRPCCredential struct {
	credential Credential
}

func (c perRPCCredential) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	token, err := c.credential.Token(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]string{"authorization": "Bearer " + token}, nil
}

// RequireTransportSecurity is false because Dex serves plaintext gRPC.
func (perRPCCredential) RequireTransportSecurity() bool {
	return false
}
