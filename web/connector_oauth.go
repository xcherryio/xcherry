// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package web

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/superdurable/dex/web/api"
)

// An empty manifest value selects the form encoding and the client_secret_post RFC 7591 method.
const (
	connectorOAuthFormTokenRequestEncoding    = "form"
	connectorOAuthJSONTokenRequestEncoding    = "json"
	connectorOAuthClientSecretPostAuthMethod  = "client_secret_post"
	connectorOAuthClientSecretBasicAuthMethod = "client_secret_basic"
)

var (
	connectorOAuthTokenRequestEncodings    = []string{connectorOAuthFormTokenRequestEncoding, connectorOAuthJSONTokenRequestEncoding}
	connectorOAuthTokenEndpointAuthMethods = []string{connectorOAuthClientSecretPostAuthMethod, connectorOAuthClientSecretBasicAuthMethod}
)

type connectorOAuthStartRequest struct {
	AuthMethodID      string                     `json:"authMethodId"`
	ClientID          string                     `json:"clientId"`
	ClientSecret      string                     `json:"clientSecret"`
	Configuration     map[string]json.RawMessage `json:"configuration"`
	CredentialValues  map[string]json.RawMessage `json:"credentialValues"`
	CredentialSecrets map[string]string          `json:"credentialSecrets"`
}

type connectorOAuthSession struct {
	identity           connectorDefinitionIdentity
	release            connectorRelease
	authMethod         connectorManifestAuthMethod
	authMethodID       string
	definitionRevision string
	clientID           string
	clientSecret       string
	codeVerifier       string
	redirectURI        string
	configuration      map[string]json.RawMessage
	credentialValues   map[string]json.RawMessage
	credentialSecrets  map[string]string
	expiresAt          time.Time
}

type connectorOAuthTokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int64  `json:"expires_in"`
	Error       string `json:"error"`
	raw         map[string]any
}

func (setup *connectorSetup) handleOAuthStart(response http.ResponseWriter, request *http.Request) {
	snapshot, identity, ok := setup.authorizeConnectionWrite(response, request)
	if !ok {
		return
	}
	request.Body = http.MaxBytesReader(response, request.Body, 1<<20)
	var body connectorOAuthStartRequest
	if err := decodeStrictConnectorJSONReader(request.Body, &body); err != nil ||
		strings.TrimSpace(body.ClientID) == "" {
		api.WriteCodedError(response, http.StatusBadRequest, "CONNECTOR_OAUTH_REQUEST_INVALID", "Connector OAuth request is invalid")
		return
	}
	resolved, err := setup.releases.resolve(request.Context(), identity)
	if err != nil {
		api.WriteCodedError(response, http.StatusBadGateway, "CONNECTOR_RELEASE_UNAVAILABLE", "Connector release metadata is unavailable")
		return
	}
	authMethod, found := resolved.release.Manifest.Spec.Auth.method(body.AuthMethodID)
	if !found || authMethod.Type != "oauth2" || authMethod.OAuth2 == nil {
		api.WriteCodedError(response, http.StatusBadRequest, "CONNECTOR_OAUTH_UNSUPPORTED", "Connector release does not support local OAuth")
		return
	}
	oauth := authMethod.OAuth2
	if connectorOAuthClientSecretRequired(authMethod) && strings.TrimSpace(body.ClientSecret) == "" {
		api.WriteCodedError(response, http.StatusBadRequest, "CONNECTOR_OAUTH_REQUEST_INVALID", "Connector OAuth configuration is invalid")
		return
	}
	resolvedOAuth, err := oauth.resolveSettings(
		connectorConfigurationFieldsForMethods(resolved.release.Manifest.Spec.Configuration.Fields, []connectorManifestAuthMethod{authMethod}),
		body.Configuration,
	)
	if err != nil {
		api.WriteCodedError(response, http.StatusBadRequest, "CONNECTOR_OAUTH_SETTING_INVALID", err.Error())
		return
	}
	oauth = &resolvedOAuth
	authMethod.OAuth2 = oauth
	if err := validateManifestValueMaps(resolved.release.Manifest, authMethod, body); err != nil {
		api.WriteCodedError(response, http.StatusBadRequest, "CONNECTOR_OAUTH_REQUEST_INVALID", "Connector OAuth configuration is invalid")
		return
	}
	if body.Configuration == nil {
		body.Configuration = map[string]json.RawMessage{}
	}
	if body.CredentialValues == nil {
		body.CredentialValues = map[string]json.RawMessage{}
	}
	if body.CredentialSecrets == nil {
		body.CredentialSecrets = map[string]string{}
	}
	authorizationURL, err := parseConnectorOAuthEndpoint(oauth.AuthorizationEndpoint, oauth.AuthorizationEndpointSetting != "")
	if err != nil {
		api.WriteCodedError(response, http.StatusBadGateway, "CONNECTOR_RELEASE_INVALID", "Connector OAuth endpoint is invalid")
		return
	}
	if err := validateConnectorOAuthTokenRequestDeclarations(*oauth); err != nil {
		api.WriteCodedError(response, http.StatusBadGateway, "CONNECTOR_RELEASE_INVALID", "Connector OAuth token request declarations are invalid")
		return
	}
	redirectURI := connectorOAuthRedirectURI(request)

	state, err := randomConnectorToken(32)
	if err != nil {
		api.WriteCodedError(response, http.StatusInternalServerError, "CONNECTOR_OAUTH_START_FAILED", "Connector OAuth could not start")
		return
	}
	verifier, err := randomConnectorToken(32)
	if err != nil {
		api.WriteCodedError(response, http.StatusInternalServerError, "CONNECTOR_OAUTH_START_FAILED", "Connector OAuth could not start")
		return
	}
	expiresAt := time.Now().Add(10 * time.Minute)
	setup.oauthSessionsMu.Lock()
	setup.deleteExpiredOAuthSessions(time.Now())
	setup.oauthSessions[state] = connectorOAuthSession{
		identity: identity, release: resolved.release, definitionRevision: snapshot.DefinitionRevision,
		authMethod: authMethod, authMethodID: authMethod.ID,
		clientID: body.ClientID, clientSecret: body.ClientSecret, codeVerifier: verifier, redirectURI: redirectURI,
		configuration: body.Configuration, credentialValues: body.CredentialValues,
		credentialSecrets: body.CredentialSecrets, expiresAt: expiresAt,
	}
	setup.oauthSessionsMu.Unlock()
	query := authorizationURL.Query()
	for name, value := range oauth.AuthorizationParameters {
		if strings.TrimSpace(name) == "" || strings.TrimSpace(value) == "" || isReservedConnectorOAuthAuthorizationParameter(name) {
			api.WriteCodedError(response, http.StatusBadGateway, "CONNECTOR_RELEASE_INVALID", "Connector OAuth authorization parameters are invalid")
			return
		}
		query.Set(name, value)
	}
	query.Set("client_id", body.ClientID)
	query.Set("redirect_uri", redirectURI)
	query.Set("response_type", "code")
	if len(oauth.Scopes) > 0 {
		query.Set("scope", strings.Join(oauth.Scopes, " "))
	}
	if len(oauth.UserScopes) > 0 {
		query.Set("user_scope", strings.Join(oauth.UserScopes, " "))
	}
	query.Set("state", state)
	if oauth.PKCE {
		challenge := sha256.Sum256([]byte(verifier))
		query.Set("code_challenge", base64.RawURLEncoding.EncodeToString(challenge[:]))
		query.Set("code_challenge_method", "S256")
	}
	authorizationURL.RawQuery = query.Encode()
	writeWebJSON(response, http.StatusOK, map[string]any{
		"authorizationUrl": authorizationURL.String(), "expiresAt": expiresAt,
	})
}

func isReservedConnectorOAuthAuthorizationParameter(name string) bool {
	switch name {
	case "client_id", "redirect_uri", "response_type", "scope", "state", "code_challenge", "code_challenge_method":
		return true
	default:
		return false
	}
}

func connectorOAuthClientSecretRequired(authMethod connectorManifestAuthMethod) bool {
	if authMethod.OAuth2 == nil || authMethod.OAuth2.ClientSecretCredential == "" {
		return false
	}
	for _, field := range authMethod.Fields {
		if field.Name == authMethod.OAuth2.ClientSecretCredential {
			return field.Required
		}
	}
	return false
}

func (setup *connectorSetup) handleOAuthCallback(response http.ResponseWriter, request *http.Request) {
	state := request.URL.Query().Get("state")
	code := request.URL.Query().Get("code")
	providerError := request.URL.Query().Get("error")
	if state == "" || (code == "" && providerError == "") {
		api.WriteCodedError(response, http.StatusBadRequest, "CONNECTOR_OAUTH_CALLBACK_INVALID", "Connector OAuth callback is invalid")
		return
	}

	if providerError != "" {
		api.WriteCodedError(response, http.StatusBadRequest, "CONNECTOR_OAUTH_CALLBACK_INVALID", "Connector OAuth callback is invalid")
		return
	}
	setup.oauthSessionsMu.Lock()
	setup.deleteExpiredOAuthSessions(time.Now())
	session, found := setup.oauthSessions[state]
	delete(setup.oauthSessions, state)
	setup.oauthSessionsMu.Unlock()
	if !found {
		api.WriteCodedError(response, http.StatusBadRequest, "CONNECTOR_OAUTH_SESSION_INVALID", "Connector OAuth session is missing, expired, or already used")
		return
	}
	snapshot, err := setup.flowDefinitions.Load(request.Context())
	if err != nil || snapshot.DefinitionRevision != session.definitionRevision {
		api.WriteCodedError(response, http.StatusConflict, "FLOW_DEFINITION_REVISION_CONFLICT", "Flow Definition changed during Connector OAuth")
		return
	}
	currentIdentity, found, conflict, err := connectorDefinitionForKey(
		snapshot.Response,
		session.identity.ConnectorID,
		session.identity.ConnectionName,
		setup.releases.overrideIdentities(),
	)
	if err != nil || !found || conflict || currentIdentity.ModulePath != session.identity.ModulePath ||
		currentIdentity.ModuleVersion != session.identity.ModuleVersion {
		api.WriteCodedError(response, http.StatusConflict, "FLOW_DEFINITION_REVISION_CONFLICT", "Connector definition changed during OAuth")
		return
	}
	token, err := setup.exchangeConnectorOAuthToken(request.Context(), session, code)
	if err != nil {
		api.WriteCodedError(response, http.StatusBadGateway, "CONNECTOR_OAUTH_TOKEN_EXCHANGE_FAILED", "Connector OAuth token exchange failed")
		return
	}
	if !hasRequiredConnectorScopes(token.raw, session.authMethod.OAuth2.Scopes) {
		api.WriteCodedError(response, http.StatusBadRequest, "CONNECTOR_OAUTH_SCOPE_INSUFFICIENT", "Connector OAuth grant is missing required scopes")
		return
	}
	if len(session.authMethod.OAuth2.UserScopes) > 0 &&
		!hasRequiredConnectorUserScopes(token.raw, session.authMethod.OAuth2.UserScopes) {
		api.WriteCodedError(response, http.StatusBadRequest, "CONNECTOR_OAUTH_SCOPE_INSUFFICIENT", "Connector OAuth user grant is missing required scopes")
		return
	}
	credentials := make(map[string]json.RawMessage, 1+len(session.credentialValues)+len(session.credentialSecrets))
	if session.authMethodID != "" {
		encodedAuthMethodID, marshalErr := json.Marshal(session.authMethodID)
		if marshalErr != nil {
			api.WriteCodedError(response, http.StatusInternalServerError, "CONNECTOR_OAUTH_WRITE_FAILED", "Connector OAuth credentials could not be saved")
			return
		}
		credentials["auth_method"] = encodedAuthMethodID
	}
	if clientIDCredential := session.authMethod.OAuth2.ClientIDCredential; clientIDCredential != "" {
		encodedClientID, marshalErr := json.Marshal(session.clientID)
		if marshalErr != nil {
			api.WriteCodedError(response, http.StatusInternalServerError, "CONNECTOR_OAUTH_WRITE_FAILED", "Connector OAuth credentials could not be saved")
			return
		}
		credentials[clientIDCredential] = encodedClientID
	}
	if clientSecretCredential := session.authMethod.OAuth2.ClientSecretCredential; clientSecretCredential != "" {
		encodedClientSecret, marshalErr := json.Marshal(session.clientSecret)
		if marshalErr != nil {
			api.WriteCodedError(response, http.StatusInternalServerError, "CONNECTOR_OAUTH_WRITE_FAILED", "Connector OAuth credentials could not be saved")
			return
		}
		credentials[clientSecretCredential] = encodedClientSecret
	}
	for name, value := range session.credentialValues {
		credentials[name] = value
	}
	for name, value := range session.credentialSecrets {
		encoded, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			api.WriteCodedError(response, http.StatusInternalServerError, "CONNECTOR_OAUTH_WRITE_FAILED", "Connector OAuth credentials could not be saved")
			return
		}
		credentials[name] = encoded
	}
	mappings := session.authMethod.OAuth2.CredentialMappings
	if len(mappings) == 0 {
		mappings = []connectorOAuthCredentialMapping{{Credential: "access_token", Source: "access_token"}}
	}
	for _, mapping := range mappings {
		value, found := connectorOAuthResponseValue(token.raw, mapping.Source)
		if !found {
			api.WriteCodedError(response, http.StatusBadGateway, "CONNECTOR_OAUTH_TOKEN_EXCHANGE_FAILED", "Connector OAuth token response is missing a mapped credential")
			return
		}
		encoded, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			api.WriteCodedError(response, http.StatusInternalServerError, "CONNECTOR_OAUTH_WRITE_FAILED", "Connector OAuth credentials could not be saved")
			return
		}
		credentials[mapping.Credential] = encoded
	}
	derivedCredentials, err := setup.deriveConnectorOAuthCredentials(request.Context(), token, session.authMethod.OAuth2.CredentialDerivations)
	if err != nil {
		api.WriteCodedError(response, http.StatusBadGateway, "CONNECTOR_OAUTH_IDENTITY_DERIVATION_FAILED", "Connector OAuth identity could not be verified")
		return
	}
	for name, value := range derivedCredentials {
		credentials[name] = value
	}
	var expiresAt *time.Time
	if token.ExpiresIn > 0 {
		value := time.Now().Add(time.Duration(token.ExpiresIn) * time.Second).UTC()
		expiresAt = &value
	}
	connection := localConnectorConnection{
		ConnectorID: session.identity.ConnectorID, AuthMethodID: session.authMethodID, ModulePath: session.identity.ModulePath,
		ModuleVersion: session.identity.ModuleVersion, Provider: session.release.Manifest.Spec.Provider,
		ConnectionName: session.identity.ConnectionName, Configuration: session.configuration,
		Credentials: credentials, CredentialExpiresAt: expiresAt,
	}
	if err := setup.store.put(connection, nil); err != nil {
		writeConnectorConfigurationStoreError(response, err, "CONNECTOR_OAUTH_WRITE_FAILED", "Connector OAuth credentials could not be saved")
		return
	}
	location := "/v2/connectors" +
		"?oauth=success&connectorId=" + url.QueryEscape(connection.ConnectorID) +
		"&connectionName=" + url.QueryEscape(connection.ConnectionName)
	http.Redirect(response, request, location, http.StatusSeeOther)
}

func (setup *connectorSetup) deriveConnectorOAuthCredentials(
	ctx context.Context,
	token connectorOAuthTokenResponse,
	derivations []connectorOAuthCredentialDerivation,
) (map[string]json.RawMessage, error) {
	credentials := make(map[string]json.RawMessage, len(derivations))
	responses := make(map[string]map[string]any)
	for _, derivation := range derivations {
		endpointURL, err := parseConnectorOAuthEndpoint(derivation.Endpoint, derivation.EndpointSetting != "")
		if err != nil {
			return nil, fmt.Errorf("Connector OAuth credential derivation endpoint is invalid")
		}
		providerResponse, found := responses[derivation.Endpoint]
		if !found {
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpointURL.String(), nil)
			if err != nil {
				return nil, err
			}
			request.Header.Set("Accept", "application/json")
			request.Header.Set("Authorization", "Bearer "+token.AccessToken)
			response, err := setup.releases.httpClient.Do(request)
			if err != nil {
				return nil, err
			}
			contents, readErr := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
			closeErr := response.Body.Close()
			if readErr != nil {
				return nil, readErr
			}
			if closeErr != nil {
				return nil, closeErr
			}
			if response.StatusCode != http.StatusOK || len(contents) > 1<<20 {
				return nil, fmt.Errorf("Connector OAuth identity provider returned HTTP %d", response.StatusCode)
			}
			if err := json.Unmarshal(contents, &providerResponse); err != nil {
				return nil, err
			}
			responses[derivation.Endpoint] = providerResponse
		}
		if derivation.VerifiedBy != "" {
			verified, found := connectorOAuthResponsePath(providerResponse, derivation.VerifiedBy)
			if !found || verified != true {
				return nil, fmt.Errorf("Connector OAuth identity claim %q is not verified", derivation.Source)
			}
		}
		value, found := connectorOAuthResponseValue(providerResponse, derivation.Source)
		if !found {
			return nil, fmt.Errorf("Connector OAuth identity response is missing %q", derivation.Source)
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		credentials[derivation.Credential] = encoded
	}
	return credentials, nil
}

func (setup *connectorSetup) exchangeConnectorOAuthToken(
	ctx context.Context,
	session connectorOAuthSession,
	code string,
) (connectorOAuthTokenResponse, error) {
	oauth := session.authMethod.OAuth2
	tokenURL, err := parseConnectorOAuthEndpoint(oauth.TokenEndpoint, oauth.TokenEndpointSetting != "")
	if err != nil {
		return connectorOAuthTokenResponse{}, fmt.Errorf("Connector OAuth token endpoint is invalid")
	}
	if err := validateConnectorOAuthTokenRequestDeclarations(*oauth); err != nil {
		return connectorOAuthTokenResponse{}, err
	}
	usesClientSecretBasic := oauth.TokenEndpointAuthMethod == connectorOAuthClientSecretBasicAuthMethod
	form := url.Values{
		"grant_type":   {"authorization_code"},
		"code":         {code},
		"redirect_uri": {session.redirectURI},
	}
	if !usesClientSecretBasic {
		form.Set("client_id", session.clientID)
		form.Set("client_secret", session.clientSecret)
	}
	if oauth.PKCE {
		form.Set("code_verifier", session.codeVerifier)
	}
	body, contentType, err := encodeConnectorOAuthTokenRequest(form, oauth.TokenRequestEncoding)
	if err != nil {
		return connectorOAuthTokenResponse{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL.String(), strings.NewReader(body))
	if err != nil {
		return connectorOAuthTokenResponse{}, err
	}
	request.Header.Set("Content-Type", contentType)
	request.Header.Set("Accept", "application/json")
	if usesClientSecretBasic {
		// Zoom, Calendly, and Notion document base64(id:secret) without RFC 6749 form-encoding.
		request.SetBasicAuth(session.clientID, session.clientSecret)
	}
	response, err := setup.releases.httpClient.Do(request)
	if err != nil {
		return connectorOAuthTokenResponse{}, err
	}
	defer response.Body.Close()
	contents, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil {
		return connectorOAuthTokenResponse{}, err
	}
	if response.StatusCode < 200 || response.StatusCode > 299 || len(contents) > 1<<20 {
		return connectorOAuthTokenResponse{}, fmt.Errorf("Connector OAuth provider returned HTTP %d", response.StatusCode)
	}
	var token connectorOAuthTokenResponse
	if err := json.Unmarshal(contents, &token); err != nil {
		return connectorOAuthTokenResponse{}, err
	}
	if err := json.Unmarshal(contents, &token.raw); err != nil {
		return connectorOAuthTokenResponse{}, err
	}
	if token.Error != "" {
		return connectorOAuthTokenResponse{}, fmt.Errorf("Connector OAuth provider rejected token exchange")
	}
	return token, nil
}

func validateConnectorOAuthTokenRequestDeclarations(oauth connectorManifestOAuth2) error {
	if oauth.TokenRequestEncoding != "" && !slices.Contains(connectorOAuthTokenRequestEncodings, oauth.TokenRequestEncoding) {
		return fmt.Errorf("Connector OAuth token request encoding %q is not supported", oauth.TokenRequestEncoding)
	}
	if oauth.TokenEndpointAuthMethod != "" && !slices.Contains(connectorOAuthTokenEndpointAuthMethods, oauth.TokenEndpointAuthMethod) {
		return fmt.Errorf("Connector OAuth token endpoint auth method %q is not supported", oauth.TokenEndpointAuthMethod)
	}
	return nil
}

// encodeConnectorOAuthTokenRequest returns the request body and its Content-Type. The json encoding
// sends one JSON object whose values are the single-valued form parameters.
func encodeConnectorOAuthTokenRequest(form url.Values, encoding string) (string, string, error) {
	if encoding != connectorOAuthJSONTokenRequestEncoding {
		return form.Encode(), "application/x-www-form-urlencoded", nil
	}
	parameters := make(map[string]string, len(form))
	for name := range form {
		parameters[name] = form.Get(name)
	}
	encoded, err := json.Marshal(parameters)
	if err != nil {
		return "", "", err
	}
	return string(encoded), "application/json", nil
}

func validateManifestValueMaps(manifest connectorReleaseManifest, authMethod connectorManifestAuthMethod, request connectorOAuthStartRequest) error {
	if request.Configuration == nil {
		request.Configuration = map[string]json.RawMessage{}
	}
	if request.CredentialValues == nil {
		request.CredentialValues = map[string]json.RawMessage{}
	}
	if request.CredentialSecrets == nil {
		request.CredentialSecrets = map[string]string{}
	}
	configurationFields := connectorConfigurationFieldsForMethods(
		manifest.Spec.Configuration.Fields, []connectorManifestAuthMethod{authMethod},
	)
	if err := validateConnectorFieldValues(configurationFields, request.Configuration, nil, nil); err != nil {
		return err
	}
	mappedCredentials := make(map[string]bool)
	if authMethod.OAuth2 != nil {
		if authMethod.OAuth2.ClientIDCredential != "" {
			mappedCredentials[authMethod.OAuth2.ClientIDCredential] = true
		}
		if authMethod.OAuth2.ClientSecretCredential != "" {
			mappedCredentials[authMethod.OAuth2.ClientSecretCredential] = true
		}
		for _, mapping := range authMethod.OAuth2.CredentialMappings {
			mappedCredentials[mapping.Credential] = true
		}
		if len(authMethod.OAuth2.CredentialMappings) == 0 {
			mappedCredentials["access_token"] = true
		}
		for _, derivation := range authMethod.OAuth2.CredentialDerivations {
			mappedCredentials[derivation.Credential] = true
		}
	}
	return validateConnectorFieldValues(authMethod.Fields, request.CredentialValues, request.CredentialSecrets, mappedCredentials)
}

func validateConnectorFieldValues(
	fields []connectorManifestField,
	values map[string]json.RawMessage,
	secrets map[string]string,
	mappedCredentials map[string]bool,
) error {
	known := make(map[string]connectorManifestField, len(fields))
	for _, field := range fields {
		known[field.Name] = field
	}
	for name := range values {
		field, found := known[name]
		if !found || field.Type == "secretString" || mappedCredentials[name] {
			return fmt.Errorf("field %q is not a host-supplied non-secret field", name)
		}
	}
	for name := range secrets {
		field, found := known[name]
		if !found || field.Type != "secretString" || mappedCredentials[name] {
			return fmt.Errorf("field %q is not a host-supplied secret field", name)
		}
	}
	for _, field := range fields {
		if !field.Required || field.Default != nil {
			continue
		}
		if mappedCredentials[field.Name] {
			continue
		}
		if field.Type == "secretString" {
			if strings.TrimSpace(secrets[field.Name]) == "" {
				return fmt.Errorf("required secret field %q is missing", field.Name)
			}
		} else if len(values[field.Name]) == 0 {
			return fmt.Errorf("required field %q is missing", field.Name)
		}
	}
	return nil
}

func connectorOAuthResponseValue(response map[string]any, path string) (string, bool) {
	current, found := connectorOAuthResponsePath(response, path)
	if !found {
		return "", false
	}
	value, ok := current.(string)
	return value, ok && strings.TrimSpace(value) != ""
}

func connectorOAuthResponsePath(response map[string]any, path string) (any, bool) {
	var current any = response
	for _, segment := range strings.Split(path, ".") {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[segment]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

// keptFieldNames satisfy a required field without a request value; the store copies them.
func validateRawConnectorFields(
	fields []connectorManifestField,
	values map[string]json.RawMessage,
	allowSecrets bool,
	keptFieldNames map[string]bool,
) error {
	if values == nil {
		values = map[string]json.RawMessage{}
	}
	known := make(map[string]connectorManifestField, len(fields))
	for _, field := range fields {
		known[field.Name] = field
	}
	for name, value := range values {
		field, found := known[name]
		if !found || (field.Type == "secretString" && !allowSecrets) {
			return fmt.Errorf("field %q is not allowed", name)
		}
		var decoded any
		if err := json.Unmarshal(value, &decoded); err != nil {
			return fmt.Errorf("field %q is invalid", name)
		}
		if (field.Type == "string" || field.Type == "url" || field.Type == "duration" ||
			field.Type == "enum" || field.Type == "secretString") && decoded != nil {
			text, ok := decoded.(string)
			if !ok || (field.Required && strings.TrimSpace(text) == "") {
				return fmt.Errorf("field %q must be a non-empty string", name)
			}
		}
	}
	for _, field := range fields {
		if field.Required && field.Default == nil && len(values[field.Name]) == 0 && !keptFieldNames[field.Name] {
			return fmt.Errorf("required field %q is missing", field.Name)
		}
	}
	return nil
}

// Slack reports the user grant in authed_user. Without declared scopes, only a user token proves it.
func hasRequiredConnectorUserScopes(response map[string]any, required []string) bool {
	userGrant, isObject := response["authed_user"].(map[string]any)
	if !isObject {
		return false
	}
	if _, isDeclared := declaredConnectorOAuthScopes(userGrant); !isDeclared {
		if _, hasUserToken := connectorOAuthResponseValue(userGrant, "access_token"); !hasUserToken {
			return false
		}
	}
	return hasRequiredConnectorScopes(userGrant, required)
}

// A grant that declares no scopes grants the requested ones (RFC 6749 section 5.1).
func hasRequiredConnectorScopes(grant map[string]any, required []string) bool {
	grantedScopes, isDeclared := declaredConnectorOAuthScopes(grant)
	if !isDeclared {
		return true
	}
	for _, scope := range required {
		if !grantedScopes[scope] && !isConnectorOAuthScopeProvenByIssuedToken(grant, scope) {
			return false
		}
	}
	return true
}

// Microsoft omits offline_access and openid from scope; the refresh and ID tokens they request prove them.
func isConnectorOAuthScopeProvenByIssuedToken(grant map[string]any, scope string) bool {
	var issuedTokenField string
	switch scope {
	case "offline_access":
		issuedTokenField = "refresh_token"
	case "openid":
		issuedTokenField = "id_token"
	default:
		return false
	}
	_, isIssued := connectorOAuthResponseValue(grant, issuedTokenField)
	return isIssued
}

// declaredConnectorOAuthScopes returns the scopes named by a grant's scope string or, when scope
// is absent or null, by its scopes string array (HubSpot). It returns false when the grant
// declares neither. A declared value of another JSON type names no scopes.
func declaredConnectorOAuthScopes(grant map[string]any) (map[string]bool, bool) {
	grantedScopes := make(map[string]bool)
	if scope := grant["scope"]; scope != nil {
		if text, isText := scope.(string); isText {
			for _, name := range strings.FieldsFunc(text, func(character rune) bool {
				return character == ' ' || character == ','
			}) {
				grantedScopes[name] = true
			}
		}
		return grantedScopes, true
	}
	scopes := grant["scopes"]
	if scopes == nil {
		return nil, false
	}
	if names, isArray := scopes.([]any); isArray {
		for _, name := range names {
			if text, isText := name.(string); isText {
				grantedScopes[text] = true
			}
		}
	}
	return grantedScopes, true
}

func connectorOAuthRedirectURI(request *http.Request) string {
	scheme := "http"
	if request.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + request.Host + "/api/v2/connector-oauth/callback"
}

func (setup *connectorSetup) deleteExpiredOAuthSessions(now time.Time) {
	for state, session := range setup.oauthSessions {
		if !now.Before(session.expiresAt) {
			delete(setup.oauthSessions, state)
		}
	}
}
