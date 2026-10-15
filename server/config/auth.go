// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package config

import (
	"fmt"
	"net"
	"net/url"
	"time"
)

// AuthenticatorKind selects the public credential format.
type AuthenticatorKind string

const (
	// AuthenticatorKindJWT verifies signed JSON Web Tokens.
	AuthenticatorKindJWT AuthenticatorKind = "jwt"
	// AuthenticatorKindStaticTokens verifies hashed bearer tokens.
	AuthenticatorKindStaticTokens AuthenticatorKind = "staticTokens"
)

// AuthConfig authenticates callers of the public gRPC API.
type AuthConfig struct {
	// Authenticators are tried in order. Default empty disables public API authentication. Immutable after startup.
	Authenticators []AuthenticatorConfig `yaml:"authenticators"`
}

// IsEnabled reports whether the public API requires credentials.
func (c AuthConfig) IsEnabled() bool { return len(c.Authenticators) != 0 }

// AuthenticatorConfig verifies either JWTs or static bearer tokens.
type AuthenticatorConfig struct {
	// Kind selects jwt or staticTokens. Default empty is invalid for a configured authenticator. Immutable after startup.
	Kind AuthenticatorKind `yaml:"kind"`
	// Issuer is the exact JWT issuer URL. Default empty is invalid for jwt. HTTPS is required except on loopback. Immutable after startup.
	Issuer string `yaml:"issuer"`
	// Audiences lists accepted JWT audiences. Default empty is invalid for jwt; a token needs at least one match. Immutable after startup.
	Audiences []string `yaml:"audiences"`
	// JWKSURL is the key set URL. Default empty discovers it through the issuer's OIDC configuration.
	// HTTPS is required except on loopback; keys refresh at most once a minute, with a bounded unknown-key refresh.
	// The URL is immutable after startup.
	JWKSURL string `yaml:"jwksURL"`
	// CAFile adds a PEM certificate bundle to system roots for issuer HTTPS endpoints. Default empty uses system roots. Immutable after startup.
	CAFile string `yaml:"caFile"`
	// File contains YAML tokens entries with SHA-256 hashes and no plaintext tokens. Default empty is invalid for staticTokens.
	// Changed files are reloaded; malformed updates retain the last valid content, while empty or missing files revoke all tokens.
	// The file path is immutable after startup.
	File string `yaml:"file"`
}

// InternalConfig protects the server's private InternalService listener.
type InternalConfig struct {
	// Listen is the plaintext gRPC host:port bind address serving InternalService to interpreter Workers.
	// Default empty disables the separate listener when auth is off; it is required with auth.
	// Interpreter Workers dial this address unless interpreter.interpreterActivityConfig.internalServiceTarget overrides it.
	// Bind to loopback or restrict access and encrypt the network path because bearer tokens travel in clear text.
	// The bind address is immutable after startup.
	Listen string `yaml:"listen"`
	// TokenFile contains one bearer token per line, each at least 32 characters. Default empty disables the token when auth is off.
	// It is required with auth. Interpreter Workers send the first line and the listener accepts every line for rotation.
	// Servers and interpreter Workers must read the same file; an empty or deleted file refuses callers after reload.
	// The file path is immutable after startup.
	TokenFile string `yaml:"tokenFile"`
}

// WakeConfig requests that an unavailable Worker be started.
type WakeConfig struct {
	// URL receives HTTP POST messages with target and kind. Default empty disables the hook.
	// Unavailable dispatches send kind wake at most once per target per 10 seconds; successful dispatches send kind active
	// at most once per target per minute. The hook is best effort. The internal token is sent when configured.
	// The URL is immutable after startup.
	URL string `yaml:"url"`
	// Budget holds dispatches to unavailable Workers before their retry policy applies. Default 120s.
	// Holding retries the connection every two seconds without counting a new attempt.
	// Immutable after startup.
	Budget time.Duration `yaml:"budget"`
}

// EffectiveBudget returns the configured budget or its default.
func (c WakeConfig) EffectiveBudget() time.Duration {
	if c.Budget <= 0 {
		return 120 * time.Second
	}
	return c.Budget
}

// ValidateGeneralAccess checks public authentication, internal listener, and Worker wake settings.
func (c Config) ValidateGeneralAccess() error {
	if c.Auth.IsEnabled() {
		if c.Internal.Listen == "" || c.Internal.TokenFile == "" {
			return fmt.Errorf("internal.listen and internal.tokenFile are required when auth is enabled")
		}
	}
	issuers := map[string]bool{}
	for index, authenticator := range c.Auth.Authenticators {
		switch authenticator.Kind {
		case AuthenticatorKindJWT:
			if err := ValidateIssuerURL("issuer", authenticator.Issuer); err != nil {
				return fmt.Errorf("auth.authenticators[%d]: %w", index, err)
			}
			if authenticator.JWKSURL != "" {
				if err := ValidateIssuerURL("jwksURL", authenticator.JWKSURL); err != nil {
					return fmt.Errorf("auth.authenticators[%d]: %w", index, err)
				}
			}
			if len(authenticator.Audiences) == 0 {
				return fmt.Errorf("auth.authenticators[%d].audiences must not be empty", index)
			}
			if issuers[authenticator.Issuer] {
				return fmt.Errorf("auth.authenticators[%d].issuer is configured twice", index)
			}
			issuers[authenticator.Issuer] = true
		case AuthenticatorKindStaticTokens:
			if authenticator.File == "" {
				return fmt.Errorf("auth.authenticators[%d].file is required", index)
			}
		default:
			return fmt.Errorf("auth.authenticators[%d].kind must be jwt or staticTokens", index)
		}
	}
	if c.Worker.Wake.URL != "" {
		parsed, err := url.ParseRequestURI(c.Worker.Wake.URL)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return fmt.Errorf("worker.wake.url must be an HTTP URL")
		}
	}
	if c.Worker.Wake.Budget < 0 {
		return fmt.Errorf("worker.wake.budget must not be negative")
	}
	return nil
}

// ValidateIssuerURL requires https, or http for a loopback host.
func ValidateIssuerURL(name string, rawURL string) error {
	if rawURL == "" {
		return fmt.Errorf("%s is required", name)
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Host == "" {
		return fmt.Errorf("%s must be an absolute URL", name)
	}
	if parsed.Scheme == "https" {
		return nil
	}
	if parsed.Scheme == "http" && isLoopbackHost(parsed.Hostname()) {
		return nil
	}
	return fmt.Errorf("%s must use https unless its host is loopback", name)
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	address := net.ParseIP(host)
	return address != nil && address.IsLoopback()
}
