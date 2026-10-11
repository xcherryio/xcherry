// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package web

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/superdurable/dex/web/api"
)

func TestConnectorOAuthSettingsOfASavedConnectionAreUsedForAuthorizationExchangeAndDerivation(t *testing.T) {
	provider := newConnectorOAuthSettingsTestProvider(t, `{"access_token":"provider-access-token","refresh_token":"provider-refresh-token","scope":"openid profile","expires_in":60}`)
	setup := newConnectorOAuthSettingsTestSetup(t)

	staleStart := startConnectorOAuthSettingsTest(t, setup, provider.configuration(map[string]string{"authorizationEndpoint": `"` + provider.URL + `/stale"`}))
	if staleStart.Code != http.StatusOK {
		t.Fatalf("first OAuth start = %d: %s", staleStart.Code, staleStart.Body.String())
	}
	startRecorder := startConnectorOAuthSettingsTest(t, setup, provider.configuration(nil))
	if startRecorder.Code != http.StatusOK {
		t.Fatalf("OAuth start = %d: %s", startRecorder.Code, startRecorder.Body.String())
	}
	authorizationURL := connectorOAuthSettingsTestAuthorizationURL(t, startRecorder)
	if got := authorizationURL.Scheme + "://" + authorizationURL.Host + authorizationURL.Path; got != provider.URL+"/authorize" ||
		authorizationURL.Query().Get("tenant") != "acme" ||
		authorizationURL.Query().Get("scope") != "openid profile" ||
		authorizationURL.Query().Get("code_challenge_method") != "S256" {
		t.Fatalf("authorization URL = %s", authorizationURL)
	}
	if stalePath := connectorOAuthSettingsTestAuthorizationURL(t, staleStart).Path; stalePath != "/stale" {
		t.Fatalf("the first start path = %q, want its own settings", stalePath)
	}

	callback := httptest.NewRecorder()
	setup.handleOAuthCallback(callback, httptest.NewRequest(http.MethodGet,
		"/api/v2/connector-oauth/callback?state="+url.QueryEscape(authorizationURL.Query().Get("state"))+"&code=authorization-code", nil))
	if callback.Code != http.StatusSeeOther {
		t.Fatalf("OAuth callback = %d: %s", callback.Code, callback.Body.String())
	}
	if redeemed := provider.redeemedCodes(); len(redeemed) != 1 || redeemed[0] != "authorization-code" {
		t.Fatalf("token endpoint requests = %v", redeemed)
	}
	if form := provider.lastTokenRequest(t).form; form.Get("client_id") != "oauth-client-id" ||
		form.Get("client_secret") != "oauth-client-secret" || form.Get("code_verifier") == "" {
		t.Fatalf("token request form = %v", form)
	}
	if bearer := provider.userInfoAuthorization(); bearer != "Bearer provider-access-token" {
		t.Fatalf("UserInfo authorization = %q", bearer)
	}
	connection, found, err := setup.store.get("oauth2-http", "keycloak")
	if err != nil || !found {
		t.Fatalf("saved connection found = %v, err = %v", found, err)
	}
	for name, want := range map[string]string{
		"access_token": `"provider-access-token"`, "refresh_token": `"provider-refresh-token"`,
		"oauth_client_id": `"oauth-client-id"`, "primary_email": `"owner@example.com"`,
	} {
		if string(connection.Credentials[name]) != want {
			t.Fatalf("saved credential %s = %s, want %s", name, connection.Credentials[name], want)
		}
	}
	for name, want := range map[string]string{
		"tokenEndpoint": `"` + provider.URL + `/token"`, "scopes": `["openid","profile"]`,
	} {
		var compacted bytes.Buffer
		if err := json.Compact(&compacted, connection.Configuration[name]); err != nil || compacted.String() != want {
			t.Fatalf("saved setting %s = %s, want %s", name, connection.Configuration[name], want)
		}
	}
}

func TestConnectorOAuthCallbackRequiresTheScopesEnteredInSettings(t *testing.T) {
	provider := newConnectorOAuthSettingsTestProvider(t, `{"access_token":"provider-access-token","scope":"openid"}`)
	setup := newConnectorOAuthSettingsTestSetup(t)
	startRecorder := startConnectorOAuthSettingsTest(t, setup, provider.configuration(nil))
	if startRecorder.Code != http.StatusOK {
		t.Fatalf("OAuth start = %d: %s", startRecorder.Code, startRecorder.Body.String())
	}
	state := connectorOAuthSettingsTestAuthorizationURL(t, startRecorder).Query().Get("state")
	callback := httptest.NewRecorder()
	setup.handleOAuthCallback(callback, httptest.NewRequest(http.MethodGet,
		"/api/v2/connector-oauth/callback?state="+url.QueryEscape(state)+"&code=authorization-code", nil))
	if callback.Code != http.StatusBadRequest || !strings.Contains(callback.Body.String(), "CONNECTOR_OAUTH_SCOPE_INSUFFICIENT") {
		t.Fatalf("OAuth callback = %d: %s", callback.Code, callback.Body.String())
	}
	if _, found, err := setup.store.get("oauth2-http", "keycloak"); err != nil || found {
		t.Fatalf("a grant without every requested scope was saved: found = %v, err = %v", found, err)
	}
}

func TestConnectorOAuthStartRejectsMissingAndInvalidSettingsWithoutLeavingForTheProvider(t *testing.T) {
	provider := newConnectorOAuthSettingsTestProvider(t, `{"access_token":"provider-access-token"}`)
	setup := newConnectorOAuthSettingsTestSetup(t)
	tests := []struct {
		name    string
		setting string
		value   string
		want    string
	}{
		{"missing authorization endpoint", "authorizationEndpoint", "", `setting "authorizationEndpoint" is missing or blank`},
		{"null token endpoint", "tokenEndpoint", `null`, `setting "tokenEndpoint" is missing or blank`},
		{"blank token endpoint", "tokenEndpoint", `"   "`, `setting "tokenEndpoint" is missing or blank`},
		{"endpoint that is not a string", "tokenEndpoint", `5`, `setting "tokenEndpoint" must be a URL string`},
		{"relative endpoint", "authorizationEndpoint", `"/authorize"`, `setting "authorizationEndpoint" is invalid`},
		{"endpoint with another scheme", "tokenEndpoint", `"ftp://idp.example/token"`, `setting "tokenEndpoint" is invalid`},
		{"endpoint without a host", "tokenEndpoint", `"http:///token"`, `setting "tokenEndpoint" is invalid`},
		{"endpoint with user information", "tokenEndpoint", `"http://admin:hunter2@idp.example/token"`, `setting "tokenEndpoint" is invalid`},
		{"endpoint with a fragment", "authorizationEndpoint", `"https://idp.example/authorize#top"`, `setting "authorizationEndpoint" is invalid`},
		{"endpoint with a space", "authorizationEndpoint", `"https://idp.example/author ize"`, `setting "authorizationEndpoint" is invalid`},
		{"invalid UserInfo endpoint", "userInfoEndpoint", `"idp.example/userinfo"`, `setting "userInfoEndpoint" is invalid`},
		{"missing scopes", "scopes", "", `setting "scopes" is missing or blank`},
		{"null scopes", "scopes", `null`, `setting "scopes" is missing or blank`},
		{"empty scopes", "scopes", `[]`, `setting "scopes" is missing or blank`},
		{"scopes that are not a list", "scopes", `"openid profile"`, `setting "scopes" must be a list of strings`},
		{"blank scope", "scopes", `["openid",""]`, `setting "scopes" must list scopes that are not blank`},
		{"scope with a space", "scopes", `["openid profile"]`, `setting "scopes" must list scopes that are not blank`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := startConnectorOAuthSettingsTest(t, setup, provider.configuration(map[string]string{test.setting: test.value}))
			var body struct {
				Code  string `json:"code"`
				Error string `json:"error"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if recorder.Code != http.StatusBadRequest || body.Code != "CONNECTOR_OAUTH_SETTING_INVALID" || !strings.Contains(body.Error, test.want) {
				t.Fatalf("OAuth start = %d %+v, want 400 CONNECTOR_OAUTH_SETTING_INVALID containing %q", recorder.Code, body, test.want)
			}
			if strings.Contains(recorder.Body.String(), "hunter2") || strings.Contains(recorder.Body.String(), "idp.example") {
				t.Fatalf("the error repeats a setting value: %s", recorder.Body.String())
			}
		})
	}
	if requests := provider.redeemedCodes(); len(requests) != 0 {
		t.Fatalf("the provider received %d token requests", len(requests))
	}
}

func TestConnectorOAuthTokenEndpointAuthMethodComesFromTheSettingOrItsDefault(t *testing.T) {
	for _, test := range []struct {
		name      string
		value     string
		wantBasic bool
	}{
		{"missing value uses the field default", "", false},
		{"null value uses the field default", `null`, false},
		{"blank value uses the field default", `""`, false},
		{"client_secret_post", `"client_secret_post"`, false},
		{"client_secret_basic", `"client_secret_basic"`, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := newConnectorOAuthSettingsTestProvider(t, `{"access_token":"provider-access-token","refresh_token":"provider-refresh-token","scope":"openid profile"}`)
			setup := newConnectorOAuthSettingsTestSetup(t)
			start := startConnectorOAuthSettingsTest(t, setup, provider.configuration(map[string]string{"tokenEndpointAuthMethod": test.value}))
			if start.Code != http.StatusOK {
				t.Fatalf("OAuth start = %d: %s", start.Code, start.Body.String())
			}
			callback := httptest.NewRecorder()
			setup.handleOAuthCallback(callback, httptest.NewRequest(http.MethodGet, "/api/v2/connector-oauth/callback?state="+
				url.QueryEscape(connectorOAuthSettingsTestAuthorizationURL(t, start).Query().Get("state"))+"&code=authorization-code", nil))
			if callback.Code != http.StatusSeeOther {
				t.Fatalf("OAuth callback = %d: %s", callback.Code, callback.Body.String())
			}
			request := provider.lastTokenRequest(t)
			hasClientInForm := request.form.Get("client_id") != "" || request.form.Get("client_secret") != ""
			if request.hasBasic != test.wantBasic || request.basicUser != map[bool]string{true: "oauth-client-id", false: ""}[test.wantBasic] || hasClientInForm == test.wantBasic {
				t.Fatalf("token request basic = %v user %q, client in form = %v, want basic = %v", request.hasBasic, request.basicUser, hasClientInForm, test.wantBasic)
			}
		})
	}
}

func TestConnectorOAuthStartRejectsUnsupportedTokenRequestOptionsFromSettings(t *testing.T) {
	provider := newConnectorOAuthSettingsTestProvider(t, `{"access_token":"provider-access-token"}`)
	setup := newConnectorOAuthSettingsTestSetup(t)
	for _, test := range []struct{ name, value, want string }{
		{"unsupported method", `"private_key_jwt"`, `must be client_secret_post or client_secret_basic`},
		{"method with another case", `"CLIENT_SECRET_POST"`, `must be client_secret_post or client_secret_basic`},
		{"method with a space", `" client_secret_basic"`, `must be client_secret_post or client_secret_basic`},
		{"method that is not a string", `5`, `must be a string`},
		{"method list", `["client_secret_basic"]`, `must be a string`},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorder := startConnectorOAuthSettingsTest(t, setup, provider.configuration(map[string]string{"tokenEndpointAuthMethod": test.value}))
			var body struct {
				Code  string `json:"code"`
				Error string `json:"error"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if recorder.Code != http.StatusBadRequest || body.Code != "CONNECTOR_OAUTH_SETTING_INVALID" ||
				!strings.Contains(body.Error, `setting "tokenEndpointAuthMethod" `+test.want) {
				t.Fatalf("OAuth start = %d %+v", recorder.Code, body)
			}
			for _, echoed := range []string{"private_key_jwt", "CLIENT_SECRET_POST"} {
				if strings.Contains(body.Error, echoed) {
					t.Fatalf("the error repeats the setting value: %s", body.Error)
				}
			}
		})
	}
	if codes := provider.redeemedCodes(); len(codes) != 0 {
		t.Fatalf("the provider received %d token requests", len(codes))
	}
}

func TestConnectorOAuthTokenRequestEncodingComesFromTheSetting(t *testing.T) {
	provider := newConnectorOAuthSettingsTestProvider(t, `{"access_token":"provider-access-token"}`)
	identity := connectorOAuthSettingsTestIdentity()
	setup := connectorTestSetup(t, t.TempDir(), connectorTestDefinitionProvider(t, []connectorDefinitionIdentity{identity}))
	installConnectorTestReleaseWithAuth(t, setup, identity, "oauth2-http", connectorManifestAuth{
		Type:   "oauth2",
		Fields: []connectorManifestField{{Name: "access_token", Type: "secretString", Required: true}},
		OAuth2: &connectorManifestOAuth2{
			AuthorizationEndpointSetting: "authorizationEndpoint", TokenEndpointSetting: "tokenEndpoint",
			TokenRequestEncodingSetting: "tokenRequestEncoding",
		},
	},
		connectorManifestField{Name: "authorizationEndpoint", Type: "url", Required: true},
		connectorManifestField{Name: "tokenEndpoint", Type: "url", Required: true},
		connectorManifestField{Name: "tokenRequestEncoding", Type: "enum", Enum: []string{"form", "json"}, Default: "form"},
	)
	for _, test := range []struct {
		value           string
		wantContentType string
		wantStatus      int
	}{
		{"", "application/x-www-form-urlencoded", http.StatusOK},
		{`"json"`, "application/json", http.StatusOK},
		{`"xml"`, "", http.StatusBadRequest},
	} {
		configuration := `{"authorizationEndpoint":"` + provider.URL + `/authorize","tokenEndpoint":"` + provider.URL + `/token"`
		if test.value != "" {
			configuration += `,"tokenRequestEncoding":` + test.value
		}
		configuration += `}`
		start := postConnectorOAuthSettingsTestStart(t, setup, `{"clientId":"oauth-client-id","clientSecret":"","configuration":`+configuration+`}`)
		if start.Code != test.wantStatus {
			t.Fatalf("OAuth start for %q = %d: %s", test.value, start.Code, start.Body.String())
		}
		if test.wantStatus != http.StatusOK {
			if !strings.Contains(start.Body.String(), `setting \"tokenRequestEncoding\" must be form or json`) || strings.Contains(start.Body.String(), "xml") {
				t.Fatalf("OAuth start error = %s", start.Body.String())
			}
			continue
		}
		callback := httptest.NewRecorder()
		setup.handleOAuthCallback(callback, httptest.NewRequest(http.MethodGet, "/api/v2/connector-oauth/callback?state="+
			url.QueryEscape(connectorOAuthSettingsTestAuthorizationURL(t, start).Query().Get("state"))+"&code=authorization-code", nil))
		if callback.Code != http.StatusSeeOther {
			t.Fatalf("OAuth callback = %d: %s", callback.Code, callback.Body.String())
		}
		if got := provider.lastTokenRequest(t).contentType; got != test.wantContentType {
			t.Fatalf("token request content type for %q = %q, want %q", test.value, got, test.wantContentType)
		}
	}
}

func TestConnectorOAuthLiteralEndpointsStayHTTPSOnly(t *testing.T) {
	provider := newConnectorOAuthSettingsTestProvider(t, `{"access_token":"provider-access-token"}`)
	t.Run("authorization endpoint", func(t *testing.T) {
		setup := newConnectorOAuthLiteralTestSetup(t, connectorManifestOAuth2{
			AuthorizationEndpoint: provider.URL + "/authorize", TokenEndpoint: "https://idp.example/token",
		})
		recorder := postConnectorOAuthSettingsTestStart(t, setup, `{"clientId":"oauth-client-id","configuration":{}}`)
		if recorder.Code != http.StatusBadGateway || !strings.Contains(recorder.Body.String(), "CONNECTOR_RELEASE_INVALID") {
			t.Fatalf("OAuth start = %d: %s", recorder.Code, recorder.Body.String())
		}
	})
	t.Run("token endpoint", func(t *testing.T) {
		setup := newConnectorOAuthLiteralTestSetup(t, connectorManifestOAuth2{
			AuthorizationEndpoint: "https://idp.example/authorize", TokenEndpoint: provider.URL + "/token",
		})
		recorder := postConnectorOAuthSettingsTestStart(t, setup, `{"clientId":"oauth-client-id","configuration":{}}`)
		if recorder.Code != http.StatusOK {
			t.Fatalf("OAuth start = %d: %s", recorder.Code, recorder.Body.String())
		}
		callback := httptest.NewRecorder()
		setup.handleOAuthCallback(callback, httptest.NewRequest(http.MethodGet, "/api/v2/connector-oauth/callback?state="+
			url.QueryEscape(connectorOAuthSettingsTestAuthorizationURL(t, recorder).Query().Get("state"))+"&code=authorization-code", nil))
		if callback.Code != http.StatusBadGateway || !strings.Contains(callback.Body.String(), "CONNECTOR_OAUTH_TOKEN_EXCHANGE_FAILED") {
			t.Fatalf("OAuth callback = %d: %s", callback.Code, callback.Body.String())
		}
	})
	if codes := provider.redeemedCodes(); len(codes) != 0 {
		t.Fatalf("the provider received %d token requests over plain HTTP", len(codes))
	}
}

func TestConnectorOAuthResolvesMethodSettingsAndDefaults(t *testing.T) {
	provider := newConnectorOAuthSettingsTestProvider(t, `{"access_token":"provider-access-token"}`)
	identity := connectorOAuthSettingsTestIdentity()
	setup := connectorTestSetup(t, t.TempDir(), connectorTestDefinitionProvider(t, []connectorDefinitionIdentity{identity}))
	installConnectorTestReleaseWithAuth(t, setup, identity, "oauth2-http", connectorManifestAuth{
		DefaultMethod: "server",
		Methods: []connectorManifestAuthMethod{{
			ID: "server", Type: "oauth2",
			Fields:        []connectorManifestField{{Name: "access_token", Type: "secretString", Required: true}},
			Configuration: &connectorManifestAuthMethodConfiguration{Fields: []connectorManifestField{{Name: "tokenEndpoint", Type: "url", Required: true}}},
			OAuth2: &connectorManifestOAuth2{
				AuthorizationEndpointSetting: "authorizationEndpoint", TokenEndpointSetting: "tokenEndpoint", ScopesSetting: "scopes",
			},
		}},
	},
		connectorManifestField{Name: "authorizationEndpoint", Type: "url", Required: true, Default: provider.URL + "/default-authorize"},
		connectorManifestField{Name: "scopes", Type: "stringList", Required: true, Default: []any{"read"}},
	)
	body := `{"authMethodId":"server","clientId":"oauth-client-id","clientSecret":"oauth-client-secret","configuration":{"tokenEndpoint":"` + provider.URL + `/token"}}`
	recorder := postConnectorOAuthSettingsTestStart(t, setup, body)
	if recorder.Code != http.StatusOK {
		t.Fatalf("OAuth start = %d: %s", recorder.Code, recorder.Body.String())
	}
	authorizationURL := connectorOAuthSettingsTestAuthorizationURL(t, recorder)
	if authorizationURL.Path != "/default-authorize" || authorizationURL.Query().Get("scope") != "read" {
		t.Fatalf("authorization URL = %s", authorizationURL)
	}
	missingMethodSetting := `{"authMethodId":"server","clientId":"oauth-client-id","clientSecret":"oauth-client-secret","configuration":{}}`
	recorder = postConnectorOAuthSettingsTestStart(t, setup, missingMethodSetting)
	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), `setting \"tokenEndpoint\" is missing or blank`) {
		t.Fatalf("OAuth start without the method setting = %d: %s", recorder.Code, recorder.Body.String())
	}
}

// connectorOAuthSettingsTestTokenResponse is a grant that satisfies the generic connector's mappings and scopes.
const connectorOAuthSettingsTestTokenResponse = `{"access_token":"provider-access-token","refresh_token":"provider-refresh-token","scope":"openid profile","expires_in":60}`

// connectorOAuthSettingsTestProvider is a plain-HTTP authorization server, like a realm on a private network.
type connectorOAuthSettingsTestProvider struct {
	*httptest.Server
	mu            sync.Mutex
	tokenRequests []connectorOAuthSettingsTestTokenRequest
	userInfoToken string
}

type connectorOAuthSettingsTestTokenRequest struct {
	form        url.Values
	contentType string
	basicUser   string
	hasBasic    bool
}

func newConnectorOAuthSettingsTestProvider(t *testing.T, tokenResponse string) *connectorOAuthSettingsTestProvider {
	t.Helper()
	provider := &connectorOAuthSettingsTestProvider{}
	provider.Server = httptest.NewServer(provider.handler(t, tokenResponse))
	t.Cleanup(provider.Close)
	return provider
}

func newConnectorOAuthSettingsTestTLSProvider(t *testing.T, tokenResponse string) *connectorOAuthSettingsTestProvider {
	t.Helper()
	provider := &connectorOAuthSettingsTestProvider{}
	provider.Server = httptest.NewTLSServer(provider.handler(t, tokenResponse))
	t.Cleanup(provider.Close)
	return provider
}

func (provider *connectorOAuthSettingsTestProvider) handler(t *testing.T, tokenResponse string) http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		provider.mu.Lock()
		defer provider.mu.Unlock()
		response.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/token":
			tokenRequest := connectorOAuthSettingsTestTokenRequest{contentType: request.Header.Get("Content-Type")}
			tokenRequest.basicUser, _, tokenRequest.hasBasic = request.BasicAuth()
			if tokenRequest.contentType == "application/json" {
				parameters := map[string]string{}
				if err := json.NewDecoder(request.Body).Decode(&parameters); err != nil {
					t.Error(err)
				}
				tokenRequest.form = url.Values{}
				for name, value := range parameters {
					tokenRequest.form.Set(name, value)
				}
			} else {
				if err := request.ParseForm(); err != nil {
					t.Error(err)
				}
				tokenRequest.form = request.PostForm
			}
			provider.tokenRequests = append(provider.tokenRequests, tokenRequest)
			if _, err := response.Write([]byte(tokenResponse)); err != nil {
				t.Error(err)
			}
		case "/userinfo":
			provider.userInfoToken = request.Header.Get("Authorization")
			if _, err := response.Write([]byte(`{"email":"owner@example.com","email_verified":true}`)); err != nil {
				t.Error(err)
			}
		default:
			http.NotFound(response, request)
		}
	}
}

func (provider *connectorOAuthSettingsTestProvider) redeemedCodes() []string {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	var codes []string
	for _, tokenRequest := range provider.tokenRequests {
		codes = append(codes, tokenRequest.form.Get("code"))
	}
	return codes
}

func (provider *connectorOAuthSettingsTestProvider) lastTokenRequest(t *testing.T) connectorOAuthSettingsTestTokenRequest {
	t.Helper()
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if len(provider.tokenRequests) == 0 {
		t.Fatal("the provider received no token request")
	}
	return provider.tokenRequests[len(provider.tokenRequests)-1]
}

func (provider *connectorOAuthSettingsTestProvider) userInfoAuthorization() string {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return provider.userInfoToken
}

// configuration returns the connection settings as JSON. An override replaces a value; an empty override omits the setting.
func (provider *connectorOAuthSettingsTestProvider) configuration(overrides map[string]string) string {
	settings := map[string]string{
		"authorizationEndpoint": `"` + provider.URL + `/authorize?tenant=acme"`,
		"tokenEndpoint":         `"` + provider.URL + `/token"`,
		"userInfoEndpoint":      `"` + provider.URL + `/userinfo"`,
		"scopes":                `["openid","profile"]`,
	}
	for name, value := range overrides {
		settings[name] = value
	}
	var members []string
	for name, value := range settings {
		if value != "" {
			members = append(members, `"`+name+`":`+value)
		}
	}
	return `{` + strings.Join(members, ",") + `}`
}

func connectorOAuthSettingsTestIdentity() connectorDefinitionIdentity {
	return connectorDefinitionIdentity{
		ConnectorID: "oauth2-http", OperationID: "sendRequest", OperationKind: "mutation", ConnectionName: "keycloak",
		ModulePath:    "github.com/superdurable/dex-connectors-library/connectors/superdurable/oauth2-http",
		ModuleVersion: "v0.1.0", ConfigurationEnabled: true,
	}
}

// newConnectorOAuthSettingsTestSetup installs a release shaped like the generic OAuth 2.0 HTTP connector.
func newConnectorOAuthSettingsTestSetup(t *testing.T) *connectorSetup {
	t.Helper()
	identity := connectorOAuthSettingsTestIdentity()
	setup := connectorTestSetup(t, t.TempDir(), connectorTestDefinitionProvider(t, []connectorDefinitionIdentity{identity}))
	installConnectorTestReleaseWithAuth(t, setup, identity, "oauth2-http", connectorManifestAuth{
		Type: "oauth2", ConnectionKind: "oauth2-http",
		Fields: []connectorManifestField{
			{Name: "oauth_client_id", Type: "string", Required: true},
			{Name: "oauth_client_secret", Type: "secretString", Required: true},
			{Name: "access_token", Type: "secretString", Required: true},
			{Name: "refresh_token", Type: "secretString"},
			{Name: "primary_email", Type: "string", Required: true},
		},
		OAuth2: &connectorManifestOAuth2{
			AuthorizationEndpointSetting: "authorizationEndpoint", TokenEndpointSetting: "tokenEndpoint", ScopesSetting: "scopes",
			ClientIDCredential: "oauth_client_id", ClientSecretCredential: "oauth_client_secret",
			CredentialMappings: []connectorOAuthCredentialMapping{
				{Credential: "access_token", Source: "access_token"}, {Credential: "refresh_token", Source: "refresh_token"},
			},
			CredentialDerivations: []connectorOAuthCredentialDerivation{{
				Credential: "primary_email", EndpointSetting: "userInfoEndpoint", Source: "email", VerifiedBy: "email_verified",
			}},
			TokenEndpointAuthMethodSetting: "tokenEndpointAuthMethod",
			PKCE:                           true,
		},
	},
		connectorManifestField{Name: "authorizationEndpoint", Type: "url", Required: true},
		connectorManifestField{Name: "tokenEndpoint", Type: "url", Required: true},
		connectorManifestField{Name: "userInfoEndpoint", Type: "url", Required: true},
		connectorManifestField{Name: "scopes", Type: "stringList", Required: true},
		connectorManifestField{
			Name: "tokenEndpointAuthMethod", Type: "enum", Enum: []string{"client_secret_post", "client_secret_basic"}, Default: "client_secret_post",
		},
	)
	return setup
}

// newConnectorOAuthLiteralTestSetup installs a release whose OAuth endpoints are literals, as every release was before references.
func newConnectorOAuthLiteralTestSetup(t *testing.T, oauth2 connectorManifestOAuth2) *connectorSetup {
	t.Helper()
	identity := connectorOAuthSettingsTestIdentity()
	setup := connectorTestSetup(t, t.TempDir(), connectorTestDefinitionProvider(t, []connectorDefinitionIdentity{identity}))
	installConnectorTestReleaseWithAuth(t, setup, identity, "oauth2-http", connectorManifestAuth{
		Type: "oauth2", Fields: []connectorManifestField{{Name: "access_token", Type: "secretString", Required: true}}, OAuth2: &oauth2,
	})
	return setup
}

func startConnectorOAuthSettingsTest(t *testing.T, setup *connectorSetup, configuration string) *httptest.ResponseRecorder {
	t.Helper()
	return postConnectorOAuthSettingsTestStart(t, setup,
		`{"clientId":"oauth-client-id","clientSecret":"oauth-client-secret","configuration":`+configuration+`,"credentialValues":{},"credentialSecrets":{}}`)
}

func postConnectorOAuthSettingsTestStart(t *testing.T, setup *connectorSetup, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/api/v2/connector-connections/oauth2-http/keycloak/oauth/start", strings.NewReader(body))
	request.SetPathValue("connectorId", "oauth2-http")
	request.SetPathValue("connectionName", "keycloak")
	request.Host = "127.0.0.1:8802"
	request.Header.Set("Origin", "http://127.0.0.1:8802")
	request.Header.Set(connectorCSRFHeader, setup.csrfToken)
	request.Header.Set(api.V2DefinitionRevisionHeader, "sha256:test")
	recorder := httptest.NewRecorder()
	setup.handleOAuthStart(recorder, request)
	return recorder
}

// callbackConnectorOAuthSettingsTest redeems an authorization code for the session that start opened.
func callbackConnectorOAuthSettingsTest(t *testing.T, setup *connectorSetup, start *httptest.ResponseRecorder) *httptest.ResponseRecorder {
	t.Helper()
	callback := httptest.NewRecorder()
	setup.handleOAuthCallback(callback, httptest.NewRequest(http.MethodGet, "/api/v2/connector-oauth/callback?state="+
		url.QueryEscape(connectorOAuthSettingsTestAuthorizationURL(t, start).Query().Get("state"))+"&code=authorization-code", nil))
	return callback
}

func connectorOAuthSettingsTestAuthorizationURL(t *testing.T, recorder *httptest.ResponseRecorder) *url.URL {
	t.Helper()
	var body struct {
		AuthorizationURL string `json:"authorizationUrl"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	authorizationURL, err := url.Parse(body.AuthorizationURL)
	if err != nil {
		t.Fatal(err)
	}
	return authorizationURL
}
