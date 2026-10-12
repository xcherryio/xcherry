// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package tenancy

import (
	"bytes"
	"errors"

	"github.com/superdurable/dex/service/common/log"
)

var errEmptyInternalToken = errors.New("internal token file is empty")

// InternalTokenFile holds the InternalService bearer token and rereads its file when it changes.
type InternalTokenFile struct {
	file *watchedFile[[]byte]
}

// NewInternalTokenFile reads the token file; surrounding whitespace is ignored and an empty token is refused.
func NewInternalTokenFile(path string, logger log.Logger) (*InternalTokenFile, error) {
	file, err := newWatchedFile(path, parseInternalToken, logger)
	if err != nil {
		return nil, err
	}
	return &InternalTokenFile{file: file}, nil
}

func (f *InternalTokenFile) Token() []byte {
	return f.file.Content()
}

func parseInternalToken(data []byte) ([]byte, error) {
	token := bytes.TrimSpace(data)
	if len(token) == 0 {
		return nil, errEmptyInternalToken
	}
	return token, nil
}
