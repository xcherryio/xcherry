// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package web

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestConnectorOAuthManifestDecodesLiteralsAndSettingReferences(t *testing.T) {
	tests := []struct {
		name    string
		oauth2  string
		want    connectorManifestOAuth2
		wantErr string
	}{
		{
			name: "literals",
			oauth2: `{"authorizationEndpoint":"https://idp.example/auth","tokenEndpoint":"https://idp.example/token",` +
				`"scopes":["read","write"],"credentialDerivations":[{"credential":"email","endpoint":"https://idp.example/userinfo","source":"email"}],"pkce":true}`,
			want: connectorManifestOAuth2{
				AuthorizationEndpoint: "https://idp.example/auth", TokenEndpoint: "https://idp.example/token",
				Scopes: []string{"read", "write"}, PKCE: true,
				CredentialDerivations: []connectorOAuthCredentialDerivation{{Credential: "email", Endpoint: "https://idp.example/userinfo", Source: "email"}},
			},
		},
		{
			name: "references with OIDC metadata that Dex Web ignores",
			oauth2: `{"protocol":"oidc","authorizationEndpoint":{"fromSetting":"authUrl"},"tokenEndpoint":{"fromSetting":"tokenUrl"},` +
				`"scopes":{"fromSetting":"scopeList"},"credentialDerivations":[{"credential":"email","endpoint":{"fromSetting":"userInfoUrl"},"source":"email"}],` +
				`"oidc":{"issuer":{"fromSetting":"issuer"},"discoveryEndpoint":{"fromSetting":"discoveryUrl"},"userInfoEndpoint":{"fromSetting":"userInfoUrl"},"nonceRequired":true},"pkce":true}`,
			want: connectorManifestOAuth2{
				AuthorizationEndpointSetting: "authUrl", TokenEndpointSetting: "tokenUrl", ScopesSetting: "scopeList", PKCE: true,
				CredentialDerivations: []connectorOAuthCredentialDerivation{{Credential: "email", EndpointSetting: "userInfoUrl", Source: "email"}},
			},
		},
		{
			name:   "a reference beside a literal and no scopes",
			oauth2: `{"authorizationEndpoint":"https://idp.example/auth","tokenEndpoint":{"fromSetting":"tokenUrl"},"pkce":false}`,
			want:   connectorManifestOAuth2{AuthorizationEndpoint: "https://idp.example/auth", TokenEndpointSetting: "tokenUrl"},
		},
		{
			name:   "token request options as literals",
			oauth2: `{"authorizationEndpoint":"https://idp.example/auth","tokenEndpoint":"https://idp.example/token","tokenRequestEncoding":"json","tokenEndpointAuthMethod":"client_secret_basic","pkce":false}`,
			want: connectorManifestOAuth2{
				AuthorizationEndpoint: "https://idp.example/auth", TokenEndpoint: "https://idp.example/token",
				TokenRequestEncoding: "json", TokenEndpointAuthMethod: "client_secret_basic",
			},
		},
		{
			name:   "token request options as references",
			oauth2: `{"authorizationEndpoint":"","tokenEndpoint":"","tokenRequestEncoding":{"fromSetting":"encoding"},"tokenEndpointAuthMethod":{"fromSetting":"clientAuthentication"},"pkce":false}`,
			want:   connectorManifestOAuth2{TokenRequestEncodingSetting: "encoding", TokenEndpointAuthMethodSetting: "clientAuthentication"},
		},
		{name: "empty token option reference", oauth2: `{"tokenEndpointAuthMethod":{"fromSetting":""}}`, wantErr: "tokenEndpointAuthMethod: a setting reference must be exactly"},
		{name: "token option with another key", oauth2: `{"tokenRequestEncoding":{"fromSetting":"a","values":{}}}`, wantErr: "tokenRequestEncoding: a setting reference must be exactly"},
		{name: "token option of another type", oauth2: `{"tokenEndpointAuthMethod":["client_secret_post"]}`, wantErr: "tokenEndpointAuthMethod: tokenEndpointAuthMethod must be a string"},
		{name: "empty reference", oauth2: `{"tokenEndpoint":{"fromSetting":""}}`, wantErr: "tokenEndpoint: a setting reference must be exactly"},
		{name: "reference with another key", oauth2: `{"tokenEndpoint":{"fromSetting":"a","default":"https://x"}}`, wantErr: "tokenEndpoint: a setting reference must be exactly"},
		{name: "reference without a name", oauth2: `{"authorizationEndpoint":{}}`, wantErr: "authorizationEndpoint: a setting reference must be exactly"},
		{name: "endpoint of another type", oauth2: `{"authorizationEndpoint":5}`, wantErr: "authorizationEndpoint: an OAuth endpoint must be"},
		{name: "scopes of another type", oauth2: `{"scopes":"read write"}`, wantErr: "scopes: OAuth scopes must be"},
		{name: "derivation endpoint of another type", oauth2: `{"credentialDerivations":[{"endpoint":["x"]}]}`, wantErr: "endpoint: an OAuth endpoint must be"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var got connectorManifestOAuth2
			err := json.Unmarshal([]byte(test.oauth2), &got)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("decode error = %v, want %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("decoded = %+v, want %+v", got, test.want)
			}
			encoded, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			var original, reencoded map[string]any
			if err := json.Unmarshal([]byte(test.oauth2), &original); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(encoded, &reencoded); err != nil {
				t.Fatal(err)
			}
			// Dex Web does not keep protocol or oidc, so the comparison drops what it never read.
			delete(original, "protocol")
			delete(original, "oidc")
			if !reflect.DeepEqual(original, reencoded) {
				t.Fatalf("re-encoded manifest = %s, want %s", encoded, test.oauth2)
			}
		})
	}
}
