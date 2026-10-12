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

// tenancyTestEnvironment is a JWT issuer, static token file and internal token for one multi-tenant server.
type tenancyTestEnvironment struct {
	Config           *config.TenancyConfig
	Issuer           string
	InternalToken    string
	OperatorToken    string
	StaticTokenFile  string
	signingKey       *rsa.PrivateKey
	staticTokenLines []string
}

func newTenancyTestEnvironment(t *testing.T) *tenancyTestEnvironment {
	t.Helper()
	signingKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	keySet := jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{
		Key:       &signingKey.PublicKey,
		KeyID:     tenancyTestKeyID,
		Algorithm: string(jose.RS256),
		Use:       "sig",
	}}}
	issuerServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(writer).Encode(keySet))
	}))
	t.Cleanup(issuerServer.Close)

	directory := t.TempDir()
	environment := &tenancyTestEnvironment{
		Issuer:          issuerServer.URL,
		InternalToken:   "internal-" + uuid.NewString(),
		OperatorToken:   "operator-" + uuid.NewString(),
		StaticTokenFile: filepath.Join(directory, "tenant-tokens.yaml"),
		signingKey:      signingKey,
	}
	internalTokenFile := filepath.Join(directory, "internal-token")
	require.NoError(t, os.WriteFile(internalTokenFile, []byte(environment.InternalToken+"\n"), 0o600))
	require.NoError(t, os.WriteFile(environment.StaticTokenFile, []byte("tokens: []\n"), 0o600))
	environment.Config = &config.TenancyConfig{
		Mode: config.TenancyModeMulti,
		Authenticators: []config.TenantAuthenticatorConfig{
			{
				Kind:           config.TenantAuthenticatorKindJWT,
				Issuer:         issuerServer.URL,
				Audiences:      []string{tenancyTestAudience},
				JWKSURL:        issuerServer.URL + "/keys",
				SubjectPattern: `^system:serviceaccount:apps-(?P<tenant>[a-z0-9]{1,32}):app$`,
				Claims:         map[string]string{"namespace": "apps-{{ .tenant }}"},
			},
			{
				Kind: config.TenantAuthenticatorKindStaticTokens,
				File: environment.StaticTokenFile,
			},
		},
		Operators: []config.TenantOperatorConfig{
			{Kind: config.TenantOperatorKindJWT, Subject: tenancyOperatorSub},
			{Kind: config.TenantOperatorKindStaticToken, SHA256: sha256Hex(environment.OperatorToken)},
		},
		Internal: config.TenancyInternalConfig{TokenFile: internalTokenFile},
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
	signer, err := jose.NewSigner(
		jose.SigningKey{
			Algorithm: jose.RS256,
			Key:       jose.JSONWebKey{Key: signingKey, KeyID: tenancyTestKeyID},
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
	token := "static-" + uuid.NewString()
	environment.staticTokenLines = append(
		environment.staticTokenLines,
		fmt.Sprintf("  - sha256: %s\n    tenant: %s\n    claims: {namespace: apps-%s}\n", sha256Hex(token), tenantID, tenantID),
	)
	content := "tokens:\n"
	for _, line := range environment.staticTokenLines {
		content += line
	}
	require.NoError(t, os.WriteFile(environment.StaticTokenFile, []byte(content), 0o600))
	return token
}

func sha256Hex(token string) string {
	hash := sha256.Sum256([]byte(token))
	return hex.EncodeToString(hash[:])
}
