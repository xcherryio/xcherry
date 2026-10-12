// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package tenancy

import (
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"strings"

	"github.com/superdurable/dex/service/common/log"
)

// MinInternalTokenLength is the shortest accepted internal token.
const MinInternalTokenLength = 32

// InternalTokenFile holds the InternalService bearer tokens, one per line, and rereads its file when it changes.
type InternalTokenFile struct {
	file *watchedFile[internalTokens]
}

type internalTokens struct {
	first   string
	digests [][sha256.Size]byte
}

// NewInternalTokenFile reads the token file, which must hold at least one token at startup.
func NewInternalTokenFile(path string, logger log.Logger) (*InternalTokenFile, error) {
	file, err := newWatchedFile(path, parseInternalTokens, logger)
	if err != nil {
		return nil, err
	}
	if file.Content().first == "" {
		return nil, fmt.Errorf("%s holds no token", path)
	}
	return &InternalTokenFile{file: file}, nil
}

// Token returns the token that callers send: the first line. It is empty when the file holds none.
func (f *InternalTokenFile) Token() string {
	return f.file.Content().first
}

// Accepts reports whether credential equals any listed token, comparing SHA-256 digests in constant time.
func (f *InternalTokenFile) Accepts(credential string) bool {
	digest := sha256.Sum256([]byte(credential))
	isAccepted := 0
	for _, accepted := range f.file.Content().digests {
		isAccepted |= subtle.ConstantTimeCompare(digest[:], accepted[:])
	}
	return isAccepted == 1
}

func parseInternalTokens(data []byte) (internalTokens, error) {
	var tokens internalTokens
	for _, line := range strings.Split(string(data), "\n") {
		token := strings.TrimSpace(line)
		if token == "" {
			continue
		}
		if len(token) < MinInternalTokenLength {
			return internalTokens{}, fmt.Errorf("internal tokens must be at least %d characters", MinInternalTokenLength)
		}
		if tokens.first == "" {
			tokens.first = token
		}
		tokens.digests = append(tokens.digests, sha256.Sum256([]byte(token)))
	}
	return tokens, nil
}
