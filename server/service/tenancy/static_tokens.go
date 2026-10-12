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
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/service/common/log"
	"gopkg.in/yaml.v3"
)

type staticTokenFileContent struct {
	Tokens []staticTokenFileEntry `yaml:"tokens"`
}

type staticTokenFileEntry struct {
	SHA256 string            `yaml:"sha256"`
	Tenant string            `yaml:"tenant"`
	Claims map[string]string `yaml:"claims"`
}

type staticTokenPrincipals map[[sha256.Size]byte]*Principal

// staticTokenAuthenticator maps SHA-256 hashes of static tokens to tenant principals.
type staticTokenAuthenticator struct {
	file *watchedFile[staticTokenPrincipals]
}

func newStaticTokenAuthenticator(path string, logger log.Logger) (*staticTokenAuthenticator, error) {
	file, err := newWatchedFile(path, parseStaticTokenFile, logger)
	if err != nil {
		return nil, err
	}
	return &staticTokenAuthenticator{file: file}, nil
}

// authenticate returns the tenant principal of a credential, or false when the file does not list its hash.
func (a *staticTokenAuthenticator) authenticate(credentialHash [sha256.Size]byte) (*Principal, bool) {
	principal, ok := a.file.Content()[credentialHash]
	return principal, ok
}

// parseStaticTokenFile treats an empty file as no tokens.
func parseStaticTokenFile(data []byte) (staticTokenPrincipals, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return staticTokenPrincipals{}, nil
	}
	var content staticTokenFileContent
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&content); err != nil {
		return nil, err
	}
	principals := make(staticTokenPrincipals, len(content.Tokens))
	for i, entry := range content.Tokens {
		hash, err := decodeSHA256(entry.SHA256)
		if err != nil {
			return nil, fmt.Errorf("tokens[%d]: %w", i, err)
		}
		if !config.TenantIDPattern.MatchString(entry.Tenant) {
			return nil, fmt.Errorf("tokens[%d]: tenant must match %s", i, config.TenantIDPattern)
		}
		if _, exists := principals[hash]; exists {
			return nil, fmt.Errorf("tokens[%d]: duplicate sha256", i)
		}
		principals[hash] = newTenantPrincipal(entry.Tenant, entry.Claims)
	}
	return principals, nil
}

func decodeSHA256(encoded string) ([sha256.Size]byte, error) {
	var hash [sha256.Size]byte
	decoded, err := hex.DecodeString(encoded)
	if err != nil || len(decoded) != sha256.Size {
		return hash, fmt.Errorf("sha256 must be %d hexadecimal characters", 2*sha256.Size)
	}
	copy(hash[:], decoded)
	return hash, nil
}

func newTenantPrincipal(tenantID string, claims map[string]string) *Principal {
	principalClaims := map[string]string{config.TenantIDCaptureName: tenantID}
	for name, value := range claims {
		if name != config.TenantIDCaptureName {
			principalClaims[name] = value
		}
	}
	return &Principal{Kind: PrincipalKindTenant, TenantID: tenantID, Claims: principalClaims}
}
