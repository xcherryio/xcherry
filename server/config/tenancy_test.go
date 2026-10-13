// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompileTenantSubjectPatternRefusesWideTenantCaptures(t *testing.T) {
	accepted := []string{
		`^system:serviceaccount:apps-(?P<tenant>[a-z0-9]{1,32}):app$`,
		`apps-(?P<tenant>[a-z][a-z0-9]{0,31})`,
		`(?P<tenant>acme|globex)/.*`,
	}
	for _, pattern := range accepted {
		_, err := CompileTenantSubjectPattern(pattern)
		require.NoError(t, err, pattern)
	}
	refused := []string{
		``,
		`apps-(?P<tenant>[a-z0-9]+)`,
		`apps-(?P<tenant>[a-z0-9]{1,33})`,
		`apps-(?P<tenant>[a-zA-Z0-9]{1,32})`,
		`apps-(?P<tenant>.{1,32})`,
		`apps-(?P<tenant>[a-z0-9-]{1,32})`,
		`apps-(?P<tenant>[a-z0-9]{0,32})`,
		`(?i)apps-(?P<tenant>acme)`,
		`apps-([a-z0-9]{1,32})`,
		`(?P<tenant>[a-z]{1,8})-(?P<tenant>[a-z]{1,8})`,
		`apps-(?P<tenant>[a-z]{1,8})x`,
		`apps-(?P<tenant>[a-z]{1,8})[a-z]`,
		`apps-((?P<tenant>[a-z]{1,8})):app`,
	}
	for _, pattern := range refused {
		_, err := CompileTenantSubjectPattern(pattern)
		require.Error(t, err, pattern)
	}

	compiled, err := CompileTenantSubjectPattern(`apps-(?P<tenant>[a-z]{1,8})`)
	require.NoError(t, err)
	require.True(t, compiled.MatchString("apps-acme"))
	require.False(t, compiled.MatchString("prefix:apps-acme"), "the pattern must match the whole subject")
	require.False(t, compiled.MatchString("apps-acme:suffix"), "the pattern must match the whole subject")
}

func TestValidateTenancyRefusesUnsafeMultiTenantServers(t *testing.T) {
	validConfig := func() Config {
		return Config{
			Interpreter: Interpreter{
				Temporal:                          &TemporalConfig{},
				AttributeIndexesManagedExternally: true,
			},
			Tenancy: TenancyConfig{
				Mode: TenancyModeMulti,
				Authenticators: []TenantAuthenticatorConfig{{
					Kind: TenantAuthenticatorKindStaticTokens,
					File: "/etc/dex/tenant-tokens.yaml",
				}},
				Internal:     TenancyInternalConfig{Listen: "127.0.0.1:8803", TokenFile: "/etc/dex/internal-token"},
				WorkerTarget: TenantWorkerTargetConfig{Allowed: `^worker\.{{ quote .tenant }}:9000$`},
				IsIncompleteMultiTenantModeAllowedForTest: true,
			},
		}
	}
	valid := validConfig()
	require.NoError(t, valid.ValidateTenancy())
	for _, allowed := range []string{`{{ .tenant | quote }}:9000`, `literal-only:9000`} {
		quotedPipe := validConfig()
		quotedPipe.Tenancy.WorkerTarget.Allowed = allowed
		require.NoError(t, quotedPipe.ValidateTenancy(), allowed)
	}
	require.NoError(t, Config{}.ValidateTenancy(), "single-tenant mode needs no tenancy settings")

	refusals := map[string]func(*Config){
		"Cadence backend": func(cfg *Config) {
			cfg.Interpreter.Temporal = nil
			cfg.Interpreter.Cadence = &CadenceConfig{}
		},
		"indexes managed by Dex":   func(cfg *Config) { cfg.Interpreter.AttributeIndexesManagedExternally = false },
		"Attribute Store":          func(cfg *Config) { cfg.AttributeStore.Stores = map[string]AttributeStoreConfigEntry{"rows": {}} },
		"no authenticator":         func(cfg *Config) { cfg.Tenancy.Authenticators = nil },
		"no internal listener":     func(cfg *Config) { cfg.Tenancy.Internal.Listen = "" },
		"no internal token":        func(cfg *Config) { cfg.Tenancy.Internal.TokenFile = "" },
		"no allowed worker target": func(cfg *Config) { cfg.Tenancy.WorkerTarget.Allowed = "" },
		"unquoted claim in allowed": func(cfg *Config) {
			cfg.Tenancy.WorkerTarget.Allowed = `^worker\.{{ .tenant }}:9000$`
		},
		"partly quoted claims in allowed": func(cfg *Config) {
			cfg.Tenancy.WorkerTarget.Allowed = `{{ quote .tenant }}\.{{ .namespace }}`
		},
		"conditional in allowed": func(cfg *Config) {
			cfg.Tenancy.WorkerTarget.Allowed = `{{ if .tenant }}{{ quote .tenant }}{{ end }}`
		},
		"other function in allowed": func(cfg *Config) {
			cfg.Tenancy.WorkerTarget.Allowed = `{{ printf "%s" .tenant }}`
		},
		"unknown mode": func(cfg *Config) { cfg.Tenancy.Mode = "shared" },
		"multi mode while under development": func(cfg *Config) {
			cfg.Tenancy.IsIncompleteMultiTenantModeAllowedForTest = false
		},
		"jwt operator without its issuer": func(cfg *Config) {
			cfg.Tenancy.Operators = []TenantOperatorConfig{{
				Kind: TenantOperatorKindJWT, Issuer: "https://issuer.example", Subject: "operator",
			}}
		},
		"duplicate jwt issuers": func(cfg *Config) {
			jwtAuthenticator := TenantAuthenticatorConfig{
				Kind:           TenantAuthenticatorKindJWT,
				Issuer:         "https://issuer.example",
				Audiences:      []string{"dex"},
				SubjectPattern: `^apps-(?P<tenant>[a-z]{1,8})$`,
			}
			cfg.Tenancy.Authenticators = []TenantAuthenticatorConfig{jwtAuthenticator, jwtAuthenticator}
		},
		"plaintext remote issuer": func(cfg *Config) {
			cfg.Tenancy.Authenticators = []TenantAuthenticatorConfig{{
				Kind:           TenantAuthenticatorKindJWT,
				Issuer:         "http://issuer.example",
				Audiences:      []string{"dex"},
				SubjectPattern: `^apps-(?P<tenant>[a-z]{1,8})$`,
			}}
		},
		"wide tenant capture": func(cfg *Config) {
			cfg.Tenancy.Authenticators = []TenantAuthenticatorConfig{{
				Kind:           TenantAuthenticatorKindJWT,
				Issuer:         "https://issuer.example",
				Audiences:      []string{"dex"},
				SubjectPattern: `^apps-(?P<tenant>.+)$`,
			}}
		},
	}
	for name, mutate := range refusals {
		cfg := validConfig()
		mutate(&cfg)
		require.Error(t, cfg.ValidateTenancy(), name)
	}
}
