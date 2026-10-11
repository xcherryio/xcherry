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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"unicode"
)

// parseConnectorOAuthEndpoint requires https for a manifest literal. A setting may also supply http.
func parseConnectorOAuthEndpoint(value string, isFromSetting bool) (*url.URL, error) {
	target, err := url.Parse(value)
	if err != nil {
		return nil, fmt.Errorf("URL cannot be parsed")
	}
	if !isFromSetting {
		if target.Scheme != "https" || target.Host == "" {
			return nil, fmt.Errorf("URL must be absolute HTTPS with a host")
		}
		return target, nil
	}
	hasAllowedScheme := target.Scheme == "https" || target.Scheme == "http"
	hasForbiddenPart := target.User != nil || strings.Contains(value, "#") || strings.ContainsFunc(value, unicode.IsSpace)
	if !hasAllowedScheme || target.Hostname() == "" || hasForbiddenPart {
		return nil, fmt.Errorf("URL must be absolute HTTP or HTTPS with a host, without user information or a fragment")
	}
	return target, nil
}

// resolveSettings returns a copy in which each setting reference holds its configuration value.
func (oauth connectorManifestOAuth2) resolveSettings(
	fields []connectorManifestField,
	configuration map[string]json.RawMessage,
) (connectorManifestOAuth2, error) {
	settings := connectorOAuthSettings{fields: fields, configuration: configuration}
	resolved := oauth
	var err error
	if resolved.AuthorizationEndpoint, err = settings.resolveEndpoint(oauth.AuthorizationEndpointSetting, oauth.AuthorizationEndpoint); err != nil {
		return connectorManifestOAuth2{}, err
	}
	if resolved.TokenEndpoint, err = settings.resolveEndpoint(oauth.TokenEndpointSetting, oauth.TokenEndpoint); err != nil {
		return connectorManifestOAuth2{}, err
	}
	if resolved.Scopes, err = settings.resolveScopes(oauth.ScopesSetting, oauth.Scopes); err != nil {
		return connectorManifestOAuth2{}, err
	}
	if resolved.TokenRequestEncoding, err = settings.resolveOption(oauth.TokenRequestEncodingSetting, oauth.TokenRequestEncoding, connectorOAuthTokenRequestEncodings); err != nil {
		return connectorManifestOAuth2{}, err
	}
	if resolved.TokenEndpointAuthMethod, err = settings.resolveOption(oauth.TokenEndpointAuthMethodSetting, oauth.TokenEndpointAuthMethod, connectorOAuthTokenEndpointAuthMethods); err != nil {
		return connectorManifestOAuth2{}, err
	}
	resolved.CredentialDerivations = append([]connectorOAuthCredentialDerivation(nil), oauth.CredentialDerivations...)
	for index, derivation := range resolved.CredentialDerivations {
		if resolved.CredentialDerivations[index].Endpoint, err = settings.resolveEndpoint(derivation.EndpointSetting, derivation.Endpoint); err != nil {
			return connectorManifestOAuth2{}, err
		}
	}
	return resolved, nil
}

// connectorOAuthSettings reads the configuration values that a connection owner entered.
type connectorOAuthSettings struct {
	fields        []connectorManifestField
	configuration map[string]json.RawMessage
}

// value returns a setting's saved JSON, or the field default when the value is missing or null.
func (settings connectorOAuthSettings) value(name string) (json.RawMessage, error) {
	if raw := settings.configuration[name]; len(raw) > 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return raw, nil
	}
	return settings.defaultValue(name)
}

func (settings connectorOAuthSettings) defaultValue(name string) (json.RawMessage, error) {
	for _, field := range settings.fields {
		if field.Name != name || field.Default == nil {
			continue
		}
		encoded, err := json.Marshal(field.Default)
		if err != nil {
			return nil, fmt.Errorf("Connector OAuth setting %q has an invalid default", name)
		}
		return encoded, nil
	}
	return nil, nil
}

func (settings connectorOAuthSettings) resolveEndpoint(name string, literal string) (string, error) {
	if name == "" {
		return literal, nil
	}
	raw, err := settings.value(name)
	if err != nil {
		return "", err
	}
	var value string
	if raw != nil && json.Unmarshal(raw, &value) != nil {
		return "", fmt.Errorf("Connector OAuth setting %q must be a URL string", name)
	}
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("Connector OAuth setting %q is missing or blank", name)
	}
	if _, err := parseConnectorOAuthEndpoint(value, true); err != nil {
		return "", fmt.Errorf("Connector OAuth setting %q is invalid: %w", name, err)
	}
	return value, nil
}

func (settings connectorOAuthSettings) resolveScopes(name string, literal []string) ([]string, error) {
	if name == "" {
		return literal, nil
	}
	raw, err := settings.value(name)
	if err != nil {
		return nil, err
	}
	var values []string
	if raw != nil && json.Unmarshal(raw, &values) != nil {
		return nil, fmt.Errorf("Connector OAuth setting %q must be a list of strings", name)
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("Connector OAuth setting %q is missing or blank", name)
	}
	for _, scope := range values {
		if scope == "" || strings.ContainsFunc(scope, unicode.IsSpace) {
			return nil, fmt.Errorf("Connector OAuth setting %q must list scopes that are not blank and contain no spaces", name)
		}
	}
	return values, nil
}

// resolveOption resolves a token option. A blank value takes the field default.
func (settings connectorOAuthSettings) resolveOption(name string, literal string, supportedValues []string) (string, error) {
	if name == "" {
		return literal, nil
	}
	var value string
	for _, raw := range []func(string) (json.RawMessage, error){settings.value, settings.defaultValue} {
		encoded, err := raw(name)
		if err != nil {
			return "", err
		}
		if encoded != nil && json.Unmarshal(encoded, &value) != nil {
			return "", fmt.Errorf("Connector OAuth setting %q must be a string", name)
		}
		if value != "" {
			break
		}
	}
	if value != "" && !slices.Contains(supportedValues, value) {
		return "", fmt.Errorf("Connector OAuth setting %q must be %s", name, strings.Join(supportedValues, " or "))
	}
	return value, nil
}

// connectorOAuthSettingError reports a setting that Dex Web will not use. It names the setting, never its value.
type connectorOAuthSettingError struct{ message string }

func (err *connectorOAuthSettingError) Error() string { return err.message }

func newConnectorOAuthRefusedEndpointError(settingName string, refusal *connectorAddressRefusal) error {
	return &connectorOAuthSettingError{message: fmt.Sprintf(
		"Connector OAuth setting %q resolves to an address Dex Web will not connect to: %s", settingName, refusal.reason)}
}

// newConnectorOAuthEndpointRequestError names the setting when the address policy refused the request.
func newConnectorOAuthEndpointRequestError(settingName string, err error) error {
	if refusal := (*connectorAddressRefusal)(nil); settingName != "" && errors.As(err, &refusal) {
		return newConnectorOAuthRefusedEndpointError(settingName, refusal)
	}
	return err
}

// endpointHTTPClient returns the client for an endpoint. A setting-supplied endpoint gets the address policy.
func (setup *connectorSetup) endpointHTTPClient(settingName string) *http.Client {
	if settingName == "" {
		return setup.releases.httpClient
	}
	return setup.settingHTTPClient
}

// checkSettingEndpointAddresses refuses a forbidden token or identity address before the browser leaves.
func (setup *connectorSetup) checkSettingEndpointAddresses(ctx context.Context, oauth connectorManifestOAuth2) error {
	endpoints := []struct{ settingName, endpoint string }{{oauth.TokenEndpointSetting, oauth.TokenEndpoint}}
	for _, derivation := range oauth.CredentialDerivations {
		endpoints = append(endpoints, struct{ settingName, endpoint string }{derivation.EndpointSetting, derivation.Endpoint})
	}
	for _, endpoint := range endpoints {
		if endpoint.settingName == "" {
			continue
		}
		target, err := parseConnectorOAuthEndpoint(endpoint.endpoint, true)
		if err != nil {
			return fmt.Errorf("Connector OAuth setting %q is invalid: %w", endpoint.settingName, err)
		}
		_, err = setup.endpointPolicy.checkedAddresses(ctx, target.Hostname(), target.Scheme == "http")
		// An unresolvable host is not a refusal; the code exchange reports it.
		if refusal := (*connectorAddressRefusal)(nil); errors.As(err, &refusal) {
			return newConnectorOAuthRefusedEndpointError(endpoint.settingName, refusal)
		}
	}
	return nil
}
