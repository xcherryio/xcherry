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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/go-jose/go-jose/v3"
	"github.com/go-jose/go-jose/v3/jwt"
	"github.com/stretchr/testify/require"
)

const authenticationTestKeyID = "authentication-test-key"

// testJWTIssuer serves an OIDC discovery document and a key set that tests can extend.
type testJWTIssuer struct {
	URL  string
	test *testing.T
	mu   sync.Mutex
	keys map[string]*rsa.PrivateKey
}

func newTestJWTIssuer(t *testing.T) *testJWTIssuer {
	t.Helper()
	issuer := &testJWTIssuer{test: t, keys: map[string]*rsa.PrivateKey{}}
	server := httptest.NewServer(issuer)
	t.Cleanup(server.Close)
	issuer.URL = server.URL
	issuer.AddKey(t, authenticationTestKeyID)
	return issuer
}

func (issuer *testJWTIssuer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Content-Type", "application/json")
	if request.URL.Path == "/.well-known/openid-configuration" {
		require.NoError(issuer.test, json.NewEncoder(writer).Encode(map[string]string{
			"issuer": issuer.URL, "jwks_uri": issuer.URL + "/keys",
		}))
		return
	}
	require.NoError(issuer.test, json.NewEncoder(writer).Encode(issuer.keySet()))
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
