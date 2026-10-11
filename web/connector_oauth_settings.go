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
	"fmt"
	"net/url"
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
