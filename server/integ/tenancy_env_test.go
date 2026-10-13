// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package integ

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v3"
	"github.com/go-jose/go-jose/v3/jwt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/service"
)

const (
	tenancyTestKeyID     = "tenancy-test-key"
	tenancyTestAudience  = "dex"
	tenancyOperatorSub   = "system:serviceaccount:platform:controller"
	tenancySubjectFormat = "system:serviceaccount:apps-%s:app"
)

// tenancyTestEnvironment is two JWT issuers, a static token file and an internal token for one multi-tenant server.
// The first issuer is configured with its key set URL, the second through OIDC discovery.
type tenancyTestEnvironment struct {
	Config            *config.TenancyConfig
	Issuer            string
	SecondIssuer      *testJWTIssuer
	InternalToken     string
	InternalTokenFile string
	OperatorToken     string
	StaticTokenFile   string
	issuer            *testJWTIssuer
	signingKey        *rsa.PrivateKey
	staticTokenLines  []string
}

// testJWTIssuer serves an OIDC discovery document and a key set that tests can extend.
type testJWTIssuer struct {
	URL  string
	mu   sync.Mutex
	keys map[string]*rsa.PrivateKey
}

func newTestJWTIssuer(t *testing.T) *testJWTIssuer {
	t.Helper()
	issuer := &testJWTIssuer{keys: map[string]*rsa.PrivateKey{}}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if request.URL.Path == "/.well-known/openid-configuration" {
			require.NoError(t, json.NewEncoder(writer).Encode(map[string]string{
				"issuer": issuer.URL, "jwks_uri": issuer.URL + "/keys",
			}))
			return
		}
		require.NoError(t, json.NewEncoder(writer).Encode(issuer.keySet()))
	}))
	t.Cleanup(server.Close)
	issuer.URL = server.URL
	issuer.AddKey(t, tenancyTestKeyID)
	return issuer
}

// AddKey publishes a new signing key under keyID and returns it.
func (issuer *testJWTIssuer) AddKey(t *testing.T, keyID string) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	issuer.keys[keyID] = key
	return key
}

func (issuer *testJWTIssuer) key(keyID string) *rsa.PrivateKey {
	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	return issuer.keys[keyID]
}

func (issuer *testJWTIssuer) keySet() jose.JSONWebKeySet {
	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	keySet := jose.JSONWebKeySet{}
	for keyID, key := range issuer.keys {
		keySet.Keys = append(keySet.Keys, jose.JSONWebKey{
			Key: &key.PublicKey, KeyID: keyID, Algorithm: string(jose.RS256), Use: "sig",
		})
	}
	return keySet
}

func newTenancyTestEnvironment(t *testing.T) *tenancyTestEnvironment {
	t.Helper()
	issuer := newTestJWTIssuer(t)
	secondIssuer := newTestJWTIssuer(t)
	directory := t.TempDir()
	environment := &tenancyTestEnvironment{
		Issuer:            issuer.URL,
		SecondIssuer:      secondIssuer,
		InternalToken:     "internal-" + uuid.NewString(),
		InternalTokenFile: filepath.Join(directory, "internal-token"),
		OperatorToken:     "operator-" + uuid.NewString(),
		StaticTokenFile:   filepath.Join(directory, "tenant-tokens.yaml"),
		issuer:            issuer,
		signingKey:        issuer.key(tenancyTestKeyID),
	}
	require.NoError(t, os.WriteFile(environment.InternalTokenFile, []byte(environment.InternalToken+"\n"), 0o600))
	require.NoError(t, os.WriteFile(environment.StaticTokenFile, []byte("tokens: []\n"), 0o600))
	environment.Config = &config.TenancyConfig{
		Mode: config.TenancyModeMulti,
		Authenticators: []config.TenantAuthenticatorConfig{
			{
				Kind:           config.TenantAuthenticatorKindJWT,
				Issuer:         issuer.URL,
				Audiences:      []string{tenancyTestAudience},
				JWKSURL:        issuer.URL + "/keys",
				SubjectPattern: `^system:serviceaccount:apps-(?P<tenant>[a-z0-9]{1,32}):app$`,
				Claims:         map[string]string{"namespace": "apps-{{ .tenant }}"},
			},
			{
				Kind:           config.TenantAuthenticatorKindJWT,
				Issuer:         secondIssuer.URL,
				Audiences:      []string{tenancyTestAudience},
				SubjectPattern: `^second:(?P<tenant>[a-z0-9]{1,32})$`,
			},
			{
				Kind: config.TenantAuthenticatorKindStaticTokens,
				File: environment.StaticTokenFile,
			},
		},
		Operators: []config.TenantOperatorConfig{
			{Kind: config.TenantOperatorKindJWT, Issuer: issuer.URL, Subject: tenancyOperatorSub},
			{Kind: config.TenantOperatorKindStaticToken, SHA256: sha256Hex(environment.OperatorToken)},
		},
		Internal: config.TenancyInternalConfig{TokenFile: environment.InternalTokenFile},
		// The repository's integration Temporal sets limit.maxIDLength to 255.
		MaxStoredFlowIDBytes: 255,
		WorkerTarget:         config.TenantWorkerTargetConfig{Allowed: `127\.0\.0\.1:[0-9]+`},
		// Tests poll freely; TestTenantRateLimitsTemporal sets tight buckets itself.
		Limits: config.TenantLimitsConfig{
			StartFlowsPerSecond: config.TenantRateLimitConfig{Rate: 1000, Burst: 1000},
			WritesPerSecond:     config.TenantRateLimitConfig{Rate: 1000, Burst: 1000},
			SearchesPerSecond:   config.TenantRateLimitConfig{Rate: 1000, Burst: 1000},
		},
	}
	return environment
}

// startTenantDexService starts a multi-tenant server whose runtime FlowClient authenticates as tenantID.
func (environment *tenancyTestEnvironment) startTenantDexService(
	t *testing.T,
	tenantID string,
	configure ...func(*DexServiceTestConfig),
) *integRuntime {
	t.Helper()
	testConfig := DexServiceTestConfig{
		BackendType: service.BackendTypeTemporal,
		Tenancy:     environment.Config,
		Credential:  environment.TenantJWT(t, tenantID),
	}
	for _, apply := range configure {
		apply(&testConfig)
	}
	return startDexService(t, testConfig)
}

// TenantJWT returns a valid one-hour token for tenantID.
func (environment *tenancyTestEnvironment) TenantJWT(t *testing.T, tenantID string) string {
	t.Helper()
	return environment.SignJWT(t, environment.signingKey, tenancyJWTClaims{
		Claims: jwt.Claims{
			Issuer:   environment.Issuer,
			Subject:  fmt.Sprintf(tenancySubjectFormat, tenantID),
			Audience: jwt.Audience{tenancyTestAudience},
			Expiry:   jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
		Namespace: "apps-" + tenantID,
	})
}

type tenancyJWTClaims struct {
	jwt.Claims
	Namespace string `json:"namespace,omitempty"`
}

func (environment *tenancyTestEnvironment) SignJWT(
	t *testing.T,
	signingKey *rsa.PrivateKey,
	claims tenancyJWTClaims,
) string {
	t.Helper()
	return signTestJWT(t, signingKey, tenancyTestKeyID, claims)
}

func signTestJWT(t *testing.T, signingKey *rsa.PrivateKey, keyID string, claims any) string {
	t.Helper()
	signer, err := jose.NewSigner(
		jose.SigningKey{
			Algorithm: jose.RS256,
			Key:       jose.JSONWebKey{Key: signingKey, KeyID: keyID},
		},
		(&jose.SignerOptions{}).WithType("JWT"),
	)
	require.NoError(t, err)
	token, err := jwt.Signed(signer).Claims(claims).CompactSerialize()
	require.NoError(t, err)
	return token
}

// AddStaticToken appends a token for tenantID to the static token file and returns it.
func (environment *tenancyTestEnvironment) AddStaticToken(t *testing.T, tenantID string) string {
	t.Helper()
	return environment.AddStaticTokenWithClaims(t, tenantID, map[string]string{"namespace": "apps-" + tenantID})
}

// AddStaticTokenWithClaims appends a token for tenantID with the given claims and returns it.
func (environment *tenancyTestEnvironment) AddStaticTokenWithClaims(
	t *testing.T,
	tenantID string,
	claims map[string]string,
) string {
	t.Helper()
	token := "static-" + uuid.NewString()
	encodedClaims, err := json.Marshal(claims)
	require.NoError(t, err)
	environment.staticTokenLines = append(
		environment.staticTokenLines,
		fmt.Sprintf("  - sha256: %s\n    tenant: %s\n    claims: %s\n", sha256Hex(token), tenantID, encodedClaims),
	)
	environment.WriteStaticTokens(t)
	return token
}

// WriteStaticTokens rewrites the static token file from the tokens added so far.
func (environment *tenancyTestEnvironment) WriteStaticTokens(t *testing.T) {
	t.Helper()
	content := "tokens:\n"
	for _, line := range environment.staticTokenLines {
		content += line
	}
	require.NoError(t, os.WriteFile(environment.StaticTokenFile, []byte(content), 0o600))
}

func sha256Hex(token string) string {
	hash := sha256.Sum256([]byte(token))
	return hex.EncodeToString(hash[:])
}
