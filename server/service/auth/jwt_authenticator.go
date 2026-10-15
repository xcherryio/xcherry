// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package auth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-jose/go-jose/v3"
	"github.com/go-jose/go-jose/v3/jwt"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/service/common/log"
	"github.com/superdurable/dex/service/common/log/tag"
	"golang.org/x/sync/singleflight"
)

const (
	keySetRefreshInterval        = time.Minute
	unknownKeyIDRefreshInterval  = 10 * time.Second
	keySetFetchTimeout           = 5 * time.Second
	maxKeySetResponseBytes       = 1 << 20
	jwtClockSkewLeeway           = 30 * time.Second
	openIDConfigurationWellKnown = "/.well-known/openid-configuration"
)

var allowedJWTAlgorithms = []string{
	string(jose.RS256), string(jose.RS384), string(jose.RS512),
	string(jose.PS256), string(jose.PS384), string(jose.PS512),
	string(jose.ES256), string(jose.ES384), string(jose.ES512),
	string(jose.EdDSA),
}

var errUntrustedJWT = errors.New("JWT is not valid")

// jwtAuthenticator verifies JWTs of one issuer locally against a cached JSON Web Key Set.
type jwtAuthenticator struct {
	cfg    *config.AuthenticatorConfig
	keySet *cachedKeySet
}

func newJWTAuthenticator(cfg *config.AuthenticatorConfig, logger log.Logger) (*jwtAuthenticator, error) {
	keySet, err := newCachedKeySet(cfg, logger)
	if err != nil {
		return nil, err
	}
	return &jwtAuthenticator{cfg: cfg, keySet: keySet}, nil
}

// verify checks the signature, issuer, audience and validity period.
func (a *jwtAuthenticator) verify(ctx context.Context, token *jwt.JSONWebToken) error {
	if len(token.Headers) != 1 || !slices.Contains(allowedJWTAlgorithms, token.Headers[0].Algorithm) {
		return errUntrustedJWT
	}
	key, err := a.keySet.key(ctx, token.Headers[0].KeyID)
	if err != nil {
		return err
	}
	if key.Algorithm != "" && key.Algorithm != token.Headers[0].Algorithm {
		return errUntrustedJWT
	}
	var registered jwt.Claims
	if err := token.Claims(key.Key, &registered); err != nil {
		return errUntrustedJWT
	}
	if registered.Issuer != a.cfg.Issuer || registered.Expiry == nil {
		return errUntrustedJWT
	}
	if !slices.ContainsFunc(a.cfg.Audiences, registered.Audience.Contains) {
		return errUntrustedJWT
	}
	if err := registered.ValidateWithLeeway(jwt.Expected{Time: time.Now()}, jwtClockSkewLeeway); err != nil {
		return errUntrustedJWT
	}
	return nil
}

// cachedKeySet bounds refreshes and shares concurrent fetches.
type cachedKeySet struct {
	cfg        *config.AuthenticatorConfig
	httpClient *http.Client
	logger     log.Logger
	fetches    singleflight.Group

	mu               sync.RWMutex
	keys             jose.JSONWebKeySet
	refreshAttemptAt time.Time
}

func newCachedKeySet(cfg *config.AuthenticatorConfig, logger log.Logger) (*cachedKeySet, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if cfg.CAFile != "" {
		bundle, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("caFile: %w", err)
		}
		roots, err := x509.SystemCertPool()
		if err != nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(bundle) {
			return nil, fmt.Errorf("caFile %s holds no PEM certificate", cfg.CAFile)
		}
		transport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	}
	return &cachedKeySet{
		cfg:        cfg,
		httpClient: &http.Client{Timeout: keySetFetchTimeout, Transport: transport},
		logger:     logger,
	}, nil
}

func (s *cachedKeySet) key(ctx context.Context, keyID string) (jose.JSONWebKey, error) {
	if s.isRefreshDue(keyID) {
		s.refresh(ctx)
	}
	s.mu.RLock()
	keys := s.keys.Key(keyID)
	s.mu.RUnlock()
	if len(keys) != 1 || !keys[0].IsPublic() {
		return jose.JSONWebKey{}, errUntrustedJWT
	}
	return keys[0], nil
}

func (s *cachedKeySet) isRefreshDue(keyID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sinceRefresh := time.Since(s.refreshAttemptAt)
	if sinceRefresh >= keySetRefreshInterval {
		return true
	}
	return len(s.keys.Key(keyID)) == 0 && sinceRefresh >= unknownKeyIDRefreshInterval
}

// refresh keeps the previous keys when the fetch fails, and records the attempt to bound the fetch rate.
func (s *cachedKeySet) refresh(ctx context.Context) {
	result := s.fetches.DoChan("keys", func() (interface{}, error) {
		s.mu.Lock()
		s.refreshAttemptAt = time.Now()
		s.mu.Unlock()
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), keySetFetchTimeout)
		defer cancel()
		keys, err := s.fetch(fetchCtx)
		if err != nil {
			s.logger.Error("refresh JWT key set", tag.Address(s.cfg.Issuer), tag.Error(err))
			return nil, err
		}
		s.mu.Lock()
		s.keys = keys
		s.mu.Unlock()
		return nil, nil
	})
	select {
	case <-result:
	case <-ctx.Done():
	}
}

func (s *cachedKeySet) fetch(ctx context.Context) (jose.JSONWebKeySet, error) {
	var keys jose.JSONWebKeySet
	jwksURL := s.cfg.JWKSURL
	if jwksURL == "" {
		var discovery struct {
			Issuer  string `json:"issuer"`
			JWKSURI string `json:"jwks_uri"`
		}
		if err := s.getJSON(ctx, strings.TrimSuffix(s.cfg.Issuer, "/")+openIDConfigurationWellKnown, &discovery); err != nil {
			return keys, err
		}
		if discovery.Issuer != s.cfg.Issuer {
			return keys, fmt.Errorf("OIDC discovery document names issuer %q", discovery.Issuer)
		}
		if err := config.ValidateIssuerURL("jwks_uri", discovery.JWKSURI); err != nil {
			return keys, fmt.Errorf("OIDC discovery document: %w", err)
		}
		jwksURL = discovery.JWKSURI
	}
	err := s.getJSON(ctx, jwksURL, &keys)
	return keys, err
}

func (s *cachedKeySet) getJSON(ctx context.Context, url string, out any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	response, err := s.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := response.Body.Close(); closeErr != nil {
			s.logger.Error("close JWT key set response", tag.Error(closeErr))
		}
	}()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: HTTP %d", url, response.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(response.Body, maxKeySetResponseBytes)).Decode(out)
}
