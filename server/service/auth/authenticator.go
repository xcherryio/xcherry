// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package auth

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/go-jose/go-jose/v3/jwt"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/service/common/log"
	"gopkg.in/yaml.v3"
)

// Principal identifies an authenticated public API caller.
type Principal struct{}

type principalContextKey struct{}

// PrincipalFromContext returns the authenticated caller, when present.
func PrincipalFromContext(ctx context.Context) (*Principal, bool) {
	principal, ok := ctx.Value(principalContextKey{}).(*Principal)
	return principal, ok
}

type credentialAuthenticator struct {
	jwt          *jwtAuthenticator
	staticTokens *watchedFile[map[[sha256.Size]byte]struct{}]
}

// Authenticator verifies public API bearer credentials.
type Authenticator struct{ entries []credentialAuthenticator }

// NewAuthenticator loads the configured static token files and JWT verifiers.
func NewAuthenticator(cfg *config.AuthConfig, logger log.Logger) (*Authenticator, error) {
	if cfg == nil || logger == nil {
		panic("auth config and logger must not be nil")
	}
	authenticator := &Authenticator{}
	for index := range cfg.Authenticators {
		entry := &cfg.Authenticators[index]
		switch entry.Kind {
		case config.AuthenticatorKindJWT:
			verifier, err := newJWTAuthenticator(entry, logger)
			if err != nil {
				return nil, fmt.Errorf("auth.authenticators[%d]: %w", index, err)
			}
			authenticator.entries = append(authenticator.entries, credentialAuthenticator{jwt: verifier})
		case config.AuthenticatorKindStaticTokens:
			file, err := newWatchedFile(entry.File, parseStaticTokens, logger)
			if err != nil {
				return nil, fmt.Errorf("auth.authenticators[%d]: %w", index, err)
			}
			authenticator.entries = append(authenticator.entries, credentialAuthenticator{staticTokens: file})
		default:
			return nil, fmt.Errorf("auth.authenticators[%d]: unsupported kind %q", index, entry.Kind)
		}
	}
	return authenticator, nil
}

// Authenticate verifies credential and returns its caller principal.
func (a *Authenticator) Authenticate(ctx context.Context, credential string) (*Principal, error) {
	credentialHash := sha256.Sum256([]byte(credential))
	token, issuer := parseUnverifiedJWT(credential)
	for _, entry := range a.entries {
		if entry.staticTokens != nil {
			for hash := range entry.staticTokens.Content() {
				if subtle.ConstantTimeCompare(hash[:], credentialHash[:]) == 1 {
					return &Principal{}, nil
				}
			}
			continue
		}
		if token == nil || entry.jwt.cfg.Issuer != issuer {
			continue
		}
		if err := entry.jwt.verify(ctx, token); err != nil {
			return nil, err
		}
		return &Principal{}, nil
	}
	return nil, fmt.Errorf("credential is not recognized")
}

func parseUnverifiedJWT(credential string) (*jwt.JSONWebToken, string) {
	if strings.Count(credential, ".") != 2 {
		return nil, ""
	}
	token, err := jwt.ParseSigned(credential)
	if err != nil {
		return nil, ""
	}
	var claims jwt.Claims
	if err := token.UnsafeClaimsWithoutVerification(&claims); err != nil {
		return nil, ""
	}
	return token, claims.Issuer
}

type staticTokenFile struct {
	Tokens []struct {
		SHA256 string `yaml:"sha256"`
	} `yaml:"tokens"`
}

func parseStaticTokens(data []byte) (map[[sha256.Size]byte]struct{}, error) {
	hashes := map[[sha256.Size]byte]struct{}{}
	if len(bytes.TrimSpace(data)) == 0 {
		return hashes, nil
	}
	var content staticTokenFile
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&content); err != nil {
		return nil, err
	}
	for index, entry := range content.Tokens {
		decoded, err := hex.DecodeString(entry.SHA256)
		if err != nil || len(decoded) != sha256.Size {
			return nil, fmt.Errorf("tokens[%d].sha256 must be 64 hexadecimal characters", index)
		}
		var hash [sha256.Size]byte
		copy(hash[:], decoded)
		if _, exists := hashes[hash]; exists {
			return nil, fmt.Errorf("tokens[%d]: duplicate sha256", index)
		}
		hashes[hash] = struct{}{}
	}
	return hashes, nil
}
