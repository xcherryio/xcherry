// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package tenancy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/go-jose/go-jose/v3"
	"github.com/go-jose/go-jose/v3/jwt"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/service/common/log"
	"github.com/superdurable/dex/service/common/log/tag"
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
	issuer         string
	audiences      []string
	subjectPattern *regexp.Regexp
	claimTemplates map[string]*template.Template
	keySet         *cachedKeySet
}

func newJWTAuthenticator(cfg *config.TenantAuthenticatorConfig, logger log.Logger) (*jwtAuthenticator, error) {
	subjectPattern, err := config.CompileTenantSubjectPattern(cfg.SubjectPattern)
	if err != nil {
		return nil, err
	}
	claimTemplates := make(map[string]*template.Template, len(cfg.Claims))
	for name, claimTemplate := range cfg.Claims {
		parsed, err := template.New(name).Option("missingkey=error").Parse(claimTemplate)
		if err != nil {
			return nil, fmt.Errorf("claims.%s: %w", name, err)
		}
		claimTemplates[name] = parsed
	}
	return &jwtAuthenticator{
		issuer:         cfg.Issuer,
		audiences:      cfg.Audiences,
		subjectPattern: subjectPattern,
		claimTemplates: claimTemplates,
		keySet:         newCachedKeySet(cfg.Issuer, cfg.JWKSURL, logger),
	}, nil
}

// verify checks the signature, issuer, audience and validity period, and returns the subject and all claims.
func (a *jwtAuthenticator) verify(ctx context.Context, token *jwt.JSONWebToken) (string, map[string]any, error) {
	if len(token.Headers) != 1 || !slices.Contains(allowedJWTAlgorithms, token.Headers[0].Algorithm) {
		return "", nil, errUntrustedJWT
	}
	key, err := a.keySet.key(ctx, token.Headers[0].KeyID)
	if err != nil {
		return "", nil, err
	}
	if key.Algorithm != "" && key.Algorithm != token.Headers[0].Algorithm {
		return "", nil, errUntrustedJWT
	}
	var registered jwt.Claims
	var allClaims map[string]any
	if err := token.Claims(key.Key, &registered, &allClaims); err != nil {
		return "", nil, errUntrustedJWT
	}
	if registered.Issuer != a.issuer || registered.Subject == "" || registered.Expiry == nil {
		return "", nil, errUntrustedJWT
	}
	if !slices.ContainsFunc(a.audiences, registered.Audience.Contains) {
		return "", nil, errUntrustedJWT
	}
	if err := registered.ValidateWithLeeway(jwt.Expected{Time: time.Now()}, jwtClockSkewLeeway); err != nil {
		return "", nil, errUntrustedJWT
	}
	return registered.Subject, allClaims, nil
}

// tenantPrincipal derives the tenant fail-closed: the whole subject must match and every configured claim must agree.
func (a *jwtAuthenticator) tenantPrincipal(subject string, allClaims map[string]any) (*Principal, error) {
	match := a.subjectPattern.FindStringSubmatch(subject)
	if match == nil {
		return nil, errUntrustedJWT
	}
	captures := map[string]string{}
	for i, name := range a.subjectPattern.SubexpNames() {
		if name != "" {
			captures[name] = match[i]
		}
	}
	tenantID := captures[config.TenantIDCaptureName]
	if !config.TenantIDPattern.MatchString(tenantID) {
		return nil, errUntrustedJWT
	}
	principalClaims := map[string]string{}
	for name, value := range captures {
		principalClaims[name] = value
	}
	for name, claimTemplate := range a.claimTemplates {
		var expected strings.Builder
		if err := claimTemplate.Execute(&expected, captures); err != nil {
			return nil, errUntrustedJWT
		}
		actual, ok := allClaims[name].(string)
		if !ok || actual != expected.String() {
			return nil, errUntrustedJWT
		}
		principalClaims[name] = actual
	}
	return &Principal{Kind: PrincipalKindTenant, TenantID: tenantID, Claims: principalClaims}, nil
}

// cachedKeySet fetches a JSON Web Key Set at most once a minute, and once per ten seconds for an unknown key ID.
type cachedKeySet struct {
	issuer     string
	jwksURL    string
	httpClient *http.Client
	logger     log.Logger

	mu          sync.Mutex
	keys        jose.JSONWebKeySet
	refreshedAt time.Time
}

func newCachedKeySet(issuer string, jwksURL string, logger log.Logger) *cachedKeySet {
	return &cachedKeySet{
		issuer:     issuer,
		jwksURL:    jwksURL,
		httpClient: &http.Client{Timeout: keySetFetchTimeout},
		logger:     logger,
	}
}

func (s *cachedKeySet) key(ctx context.Context, keyID string) (jose.JSONWebKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sinceRefresh := time.Since(s.refreshedAt)
	if sinceRefresh >= keySetRefreshInterval {
		s.refresh(ctx)
	} else if len(s.keys.Key(keyID)) == 0 && sinceRefresh >= unknownKeyIDRefreshInterval {
		s.refresh(ctx)
	}
	keys := s.keys.Key(keyID)
	if len(keys) != 1 || !keys[0].IsPublic() {
		return jose.JSONWebKey{}, errUntrustedJWT
	}
	return keys[0], nil
}

// refresh keeps the previous keys when the fetch fails, and records the attempt to bound the fetch rate.
func (s *cachedKeySet) refresh(ctx context.Context) {
	s.refreshedAt = time.Now()
	fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), keySetFetchTimeout)
	defer cancel()
	keys, err := s.fetch(fetchCtx)
	if err != nil {
		s.logger.Error("refresh JWT key set", tag.Address(s.issuer), tag.Error(err))
		return
	}
	s.keys = keys
}

func (s *cachedKeySet) fetch(ctx context.Context) (jose.JSONWebKeySet, error) {
	var keys jose.JSONWebKeySet
	jwksURL := s.jwksURL
	if jwksURL == "" {
		var discovery struct {
			JWKSURI string `json:"jwks_uri"`
		}
		if err := s.getJSON(ctx, strings.TrimSuffix(s.issuer, "/")+openIDConfigurationWellKnown, &discovery); err != nil {
			return keys, err
		}
		if discovery.JWKSURI == "" {
			return keys, fmt.Errorf("OIDC discovery document has no jwks_uri")
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
