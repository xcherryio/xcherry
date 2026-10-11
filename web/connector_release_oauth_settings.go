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
)

// decodeConnectorOAuthSettingReference reads {"fromSetting": name}. Any other JSON value is a literal.
func decodeConnectorOAuthSettingReference(raw json.RawMessage) (settingName string, isReference bool, err error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return "", false, nil
	}
	var reference struct {
		FromSetting string `json:"fromSetting"`
	}
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&reference); err != nil || reference.FromSetting == "" {
		return "", true, fmt.Errorf(`a setting reference must be exactly {"fromSetting": <configuration field name>}`)
	}
	return reference.FromSetting, true, nil
}

func decodeConnectorOAuthEndpoint(raw json.RawMessage) (literal string, settingName string, err error) {
	return decodeConnectorOAuthText(raw, "an OAuth endpoint must be a URL string")
}

// decodeConnectorOAuthText reads a literal string or a setting reference. Its requirement words the error.
func decodeConnectorOAuthText(raw json.RawMessage, requirement string) (literal string, settingName string, err error) {
	settingName, isReference, err := decodeConnectorOAuthSettingReference(raw)
	if err != nil || isReference {
		return "", settingName, err
	}
	if len(raw) == 0 {
		return "", "", nil
	}
	if err := json.Unmarshal(raw, &literal); err != nil {
		return "", "", fmt.Errorf(`%s or a {"fromSetting"} object`, requirement)
	}
	return literal, "", nil
}

func decodeConnectorOAuthScopes(raw json.RawMessage) (literal []string, settingName string, err error) {
	settingName, isReference, err := decodeConnectorOAuthSettingReference(raw)
	if err != nil || isReference {
		return nil, settingName, err
	}
	if len(raw) == 0 {
		return nil, "", nil
	}
	if err := json.Unmarshal(raw, &literal); err != nil {
		return nil, "", fmt.Errorf(`OAuth scopes must be an array of strings or a {"fromSetting"} object`)
	}
	return literal, "", nil
}

// UnmarshalJSON reads each endpoint, the scopes and the token request options as a literal or a setting reference.
func (oauth *connectorManifestOAuth2) UnmarshalJSON(contents []byte) error {
	type declarations connectorManifestOAuth2
	wire := struct {
		*declarations
		AuthorizationEndpoint json.RawMessage `json:"authorizationEndpoint"`
		TokenEndpoint         json.RawMessage `json:"tokenEndpoint"`
		Scopes                json.RawMessage `json:"scopes"`
		TokenRequestEncoding  json.RawMessage `json:"tokenRequestEncoding"`
		TokenEndpointAuth     json.RawMessage `json:"tokenEndpointAuthMethod"`
	}{declarations: (*declarations)(oauth)}
	if err := json.Unmarshal(contents, &wire); err != nil {
		return err
	}
	var err error
	if oauth.AuthorizationEndpoint, oauth.AuthorizationEndpointSetting, err = decodeConnectorOAuthEndpoint(wire.AuthorizationEndpoint); err != nil {
		return fmt.Errorf("authorizationEndpoint: %w", err)
	}
	if oauth.TokenEndpoint, oauth.TokenEndpointSetting, err = decodeConnectorOAuthEndpoint(wire.TokenEndpoint); err != nil {
		return fmt.Errorf("tokenEndpoint: %w", err)
	}
	if oauth.Scopes, oauth.ScopesSetting, err = decodeConnectorOAuthScopes(wire.Scopes); err != nil {
		return fmt.Errorf("scopes: %w", err)
	}
	if oauth.TokenRequestEncoding, oauth.TokenRequestEncodingSetting, err = decodeConnectorOAuthText(wire.TokenRequestEncoding, "tokenRequestEncoding must be a string"); err != nil {
		return fmt.Errorf("tokenRequestEncoding: %w", err)
	}
	if oauth.TokenEndpointAuthMethod, oauth.TokenEndpointAuthMethodSetting, err = decodeConnectorOAuthText(wire.TokenEndpointAuth, "tokenEndpointAuthMethod must be a string"); err != nil {
		return fmt.Errorf("tokenEndpointAuthMethod: %w", err)
	}
	return nil
}

// MarshalJSON writes a literal as before and a setting reference as {"fromSetting": name}.
func (oauth connectorManifestOAuth2) MarshalJSON() ([]byte, error) {
	type declarations connectorManifestOAuth2
	wire := struct {
		declarations
		AuthorizationEndpoint any `json:"authorizationEndpoint"`
		TokenEndpoint         any `json:"tokenEndpoint"`
		Scopes                any `json:"scopes,omitempty"`
		TokenRequestEncoding  any `json:"tokenRequestEncoding,omitempty"`
		TokenEndpointAuth     any `json:"tokenEndpointAuthMethod,omitempty"`
	}{
		declarations:          declarations(oauth),
		AuthorizationEndpoint: connectorOAuthEndpointWireValue(oauth.AuthorizationEndpoint, oauth.AuthorizationEndpointSetting),
		TokenEndpoint:         connectorOAuthEndpointWireValue(oauth.TokenEndpoint, oauth.TokenEndpointSetting),
		TokenRequestEncoding:  connectorOAuthOptionWireValue(oauth.TokenRequestEncoding, oauth.TokenRequestEncodingSetting),
		TokenEndpointAuth:     connectorOAuthOptionWireValue(oauth.TokenEndpointAuthMethod, oauth.TokenEndpointAuthMethodSetting),
	}
	if oauth.ScopesSetting != "" {
		wire.Scopes = connectorOAuthSettingReference{FromSetting: oauth.ScopesSetting}
	} else if len(oauth.Scopes) > 0 {
		wire.Scopes = oauth.Scopes
	}
	return json.Marshal(wire)
}

// UnmarshalJSON reads the endpoint as a literal or a setting reference.
func (derivation *connectorOAuthCredentialDerivation) UnmarshalJSON(contents []byte) error {
	type declarations connectorOAuthCredentialDerivation
	wire := struct {
		*declarations
		Endpoint json.RawMessage `json:"endpoint"`
	}{declarations: (*declarations)(derivation)}
	if err := json.Unmarshal(contents, &wire); err != nil {
		return err
	}
	var err error
	if derivation.Endpoint, derivation.EndpointSetting, err = decodeConnectorOAuthEndpoint(wire.Endpoint); err != nil {
		return fmt.Errorf("endpoint: %w", err)
	}
	return nil
}

// MarshalJSON writes a literal as before and a setting reference as {"fromSetting": name}.
func (derivation connectorOAuthCredentialDerivation) MarshalJSON() ([]byte, error) {
	type declarations connectorOAuthCredentialDerivation
	return json.Marshal(struct {
		declarations
		Endpoint any `json:"endpoint"`
	}{
		declarations: declarations(derivation),
		Endpoint:     connectorOAuthEndpointWireValue(derivation.Endpoint, derivation.EndpointSetting),
	})
}

type connectorOAuthSettingReference struct {
	FromSetting string `json:"fromSetting"`
}

func connectorOAuthEndpointWireValue(literal string, settingName string) any {
	if settingName != "" {
		return connectorOAuthSettingReference{FromSetting: settingName}
	}
	return literal
}

// connectorOAuthOptionWireValue is like connectorOAuthEndpointWireValue, but omits an option that is empty.
func connectorOAuthOptionWireValue(literal string, settingName string) any {
	if settingName == "" && literal == "" {
		return nil
	}
	return connectorOAuthEndpointWireValue(literal, settingName)
}
