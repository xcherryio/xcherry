// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package credential

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFileCredentialRotationAndFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte(" first \n"), 0o600))
	perRPCCredential, err := File(path)
	require.NoError(t, err)
	metadata, err := perRPCCredential.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	require.Equal(t, "Bearer first", metadata["authorization"])

	require.NoError(t, os.WriteFile(path, []byte("second-token"), 0o600))
	require.Eventually(t, func() bool {
		metadata, metadataErr := perRPCCredential.GetRequestMetadata(context.Background())
		return metadataErr == nil && metadata["authorization"] == "Bearer second-token"
	}, 2*time.Second, 20*time.Millisecond)

	require.NoError(t, os.WriteFile(path, []byte(" \n"), 0o600))
	require.Eventually(t, func() bool {
		_, metadataErr := perRPCCredential.GetRequestMetadata(context.Background())
		return metadataErr != nil && metadataErr.Error() == "bearer credential file "+path+" is empty"
	}, 2*time.Second, 20*time.Millisecond)

	require.NoError(t, os.Remove(path))
	require.Eventually(t, func() bool {
		_, metadataErr := perRPCCredential.GetRequestMetadata(context.Background())
		return errors.Is(metadataErr, os.ErrNotExist)
	}, 2*time.Second, 20*time.Millisecond)
}

func TestFileCredentialRejectsMissingAndEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	_, err := File(path)
	require.ErrorContains(t, err, "read bearer credential file")
	require.NoError(t, os.WriteFile(path, []byte("\n"), 0o600))
	_, err = File(path)
	require.ErrorContains(t, err, "is empty")
}
