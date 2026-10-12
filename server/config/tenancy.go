// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package config

import (
	"encoding/hex"
	"fmt"
	"regexp"
	"regexp/syntax"
	"text/template"
	"unicode"
)

const (
	// TenancyModeSingle serves one namespace for every caller without authentication. It is the default.
	TenancyModeSingle TenancyMode = "single"
	// TenancyModeMulti serves many tenants in one namespace and authenticates every FlowService call.
	TenancyModeMulti TenancyMode = "multi"
)

const (
	// TenantAuthenticatorKindJWT verifies signed JSON Web Tokens against a cached key set.
	TenantAuthenticatorKindJWT TenantAuthenticatorKind = "jwt"
	// TenantAuthenticatorKindStaticTokens maps SHA-256 token hashes from a file to tenants.
	TenantAuthenticatorKindStaticTokens TenantAuthenticatorKind = "staticTokens"
)

const (
	// TenantOperatorKindJWT identifies an operator by the exact subject of a verified JWT.
	TenantOperatorKindJWT TenantOperatorKind = "jwt"
	// TenantOperatorKindStaticToken identifies an operator by the SHA-256 hash of a static token.
	TenantOperatorKindStaticToken TenantOperatorKind = "staticToken"
)

const (
	// TenantIDCaptureName is the subjectPattern capture group that yields the tenant ID.
	TenantIDCaptureName = "tenant"
	// MaxTenantIDLength is the longest tenant ID. Tenant IDs match ^[a-z0-9]{1,32}$.
	MaxTenantIDLength = 32
)

// TenantIDPattern is the exact form of every tenant ID.
var TenantIDPattern = regexp.MustCompile(`^[a-z0-9]{1,32}$`)

type (
	TenancyMode string

	TenantAuthenticatorKind string

	TenantOperatorKind string

	// TenancyConfig configures multi-tenant namespaces.
	// The zero value is single-tenant mode, which behaves exactly as a server without this section.
	TenancyConfig struct {
		// Mode is single (default when empty) or multi. In multi mode every FlowService call must carry
		// "authorization: Bearer <credential>" metadata, and the server derives the caller's tenant from it.
		// Multi mode refuses to start with the Cadence backend, without interpreter.attributeIndexesManagedExternally,
		// or with any configured Attribute Store. Immutable after startup.
		Mode TenancyMode `yaml:"mode"`
		// Authenticators resolve tenant credentials. Default empty. Multi mode requires at least one.
		// They are tried in order; the first authenticator that recognizes a credential decides it.
		Authenticators []TenantAuthenticatorConfig `yaml:"authenticators"`
		// Operators are the principals allowed on TenantAdminService and nowhere else. Default empty.
		// An operator credential is never a tenant credential, even when it would also match a tenant authenticator.
		Operators []TenantOperatorConfig `yaml:"operators"`
		// Internal configures the listener that serves InternalService to the server's own interpreter Workers.
		// Required in multi mode, where InternalService is never served on the public API listener.
		Internal TenancyInternalConfig `yaml:"internal"`
	}

	TenantAuthenticatorConfig struct {
		// Kind is jwt or staticTokens. Required.
		Kind TenantAuthenticatorKind `yaml:"kind"`
		// Issuer is the exact "iss" claim a JWT must carry. Required for jwt.
		// When JWKSURL is empty, the key set URL is discovered from <issuer>/.well-known/openid-configuration.
		Issuer string `yaml:"issuer"`
		// Audiences lists accepted "aud" values. Required for jwt; a token must name at least one of them.
		Audiences []string `yaml:"audiences"`
		// JWKSURL is the JSON Web Key Set URL. Default empty uses OIDC discovery from Issuer.
		// Keys are cached and refreshed at most once a minute; a token with an unknown key ID triggers one bounded refresh.
		// No request makes a remote call otherwise.
		JWKSURL string `yaml:"jwksURL"`
		// SubjectPattern is a regular expression that must match the whole "sub" claim. Required for jwt.
		// Its named group "tenant" is the tenant ID and must be able to match only [a-z0-9]{1,32},
		// for example '^system:serviceaccount:apps-(?P<tenant>[a-z0-9]{1,32}):app$'.
		// A subject that does not match is refused: the tenant is derived fail-closed.
		// Other named groups become principal claims available to templates.
		SubjectPattern string `yaml:"subjectPattern"`
		// Claims maps a JWT claim name to a Go template rendered with the subject pattern's named groups.
		// The token's claim must be a string equal to the rendered value, for example {namespace: 'apps-{{ .tenant }}'}.
		// Each verified claim also becomes a principal claim. Default empty.
		Claims map[string]string `yaml:"claims"`
		// File is the static token file for staticTokens. Required for staticTokens.
		// It is YAML of the form "tokens: [{sha256: <hex>, tenant: <id>, claims: {<name>: <value>}}]".
		// The file is reread when its modification time changes. To rotate a token, add the new hash before removing the old one.
		File string `yaml:"file"`
	}

	TenantOperatorConfig struct {
		// Kind is jwt or staticToken. Required.
		Kind TenantOperatorKind `yaml:"kind"`
		// Subject is the exact "sub" claim of an operator JWT verified by any jwt authenticator. Required for jwt.
		Subject string `yaml:"subject"`
		// SHA256 is the lowercase hexadecimal SHA-256 hash of an operator static token. Required for staticToken.
		SHA256 string `yaml:"sha256"`
	}

	TenancyInternalConfig struct {
		// Listen is the host:port bind address of the InternalService gRPC listener, for example 127.0.0.1:8803.
		// Only the server's own interpreter Workers connect to it. Required in multi mode.
		// Interpreter Workers dial interpreter.interpreterActivityConfig.internalServiceTarget when set, otherwise this address.
		Listen string `yaml:"listen"`
		// TokenFile holds the bearer token that InternalService callers must present. Required in multi mode.
		// Servers and interpreter Workers sharing a namespace must read the same token. The file is reread when it changes.
		TokenFile string `yaml:"tokenFile"`
	}
)

// IsMultiTenant reports whether multi-tenant mode is enabled.
func (c TenancyConfig) IsMultiTenant() bool {
	return c.Mode == TenancyModeMulti
}

// ValidateTenancy refuses tenancy settings that cannot be served safely.
func (c Config) ValidateTenancy() error {
	switch c.Tenancy.Mode {
	case "", TenancyModeSingle:
		return nil
	case TenancyModeMulti:
	default:
		return fmt.Errorf("tenancy.mode must be single or multi, got %q", c.Tenancy.Mode)
	}
	if c.Interpreter.Cadence != nil {
		return fmt.Errorf("tenancy.mode multi does not support the Cadence backend")
	}
	if !c.Interpreter.AttributeIndexesManagedExternally {
		return fmt.Errorf("tenancy.mode multi requires interpreter.attributeIndexesManagedExternally")
	}
	if len(c.AttributeStore.Stores) > 0 {
		return fmt.Errorf("tenancy.mode multi does not support Attribute Stores yet")
	}
	return c.Tenancy.validateMultiTenant()
}

func (c TenancyConfig) validateMultiTenant() error {
	if len(c.Authenticators) == 0 {
		return fmt.Errorf("tenancy.authenticators must not be empty in multi mode")
	}
	hasJWTAuthenticator := false
	for i, authenticator := range c.Authenticators {
		if err := authenticator.validate(); err != nil {
			return fmt.Errorf("tenancy.authenticators[%d]: %w", i, err)
		}
		hasJWTAuthenticator = hasJWTAuthenticator || authenticator.Kind == TenantAuthenticatorKindJWT
	}
	for i, operator := range c.Operators {
		if err := operator.validate(hasJWTAuthenticator); err != nil {
			return fmt.Errorf("tenancy.operators[%d]: %w", i, err)
		}
	}
	if c.Internal.Listen == "" {
		return fmt.Errorf("tenancy.internal.listen is required in multi mode")
	}
	if c.Internal.TokenFile == "" {
		return fmt.Errorf("tenancy.internal.tokenFile is required in multi mode")
	}
	return nil
}

func (c TenantAuthenticatorConfig) validate() error {
	switch c.Kind {
	case TenantAuthenticatorKindJWT:
		if c.Issuer == "" {
			return fmt.Errorf("issuer is required")
		}
		if len(c.Audiences) == 0 {
			return fmt.Errorf("audiences must not be empty")
		}
		if _, err := CompileTenantSubjectPattern(c.SubjectPattern); err != nil {
			return err
		}
		for name, claimTemplate := range c.Claims {
			if _, err := template.New(name).Option("missingkey=error").Parse(claimTemplate); err != nil {
				return fmt.Errorf("claims.%s: %w", name, err)
			}
		}
		return nil
	case TenantAuthenticatorKindStaticTokens:
		if c.File == "" {
			return fmt.Errorf("file is required")
		}
		return nil
	default:
		return fmt.Errorf("kind must be jwt or staticTokens, got %q", c.Kind)
	}
}

func (c TenantOperatorConfig) validate(hasJWTAuthenticator bool) error {
	switch c.Kind {
	case TenantOperatorKindJWT:
		if c.Subject == "" {
			return fmt.Errorf("subject is required")
		}
		if !hasJWTAuthenticator {
			return fmt.Errorf("a jwt operator requires a jwt authenticator to verify its token")
		}
		return nil
	case TenantOperatorKindStaticToken:
		decoded, err := hex.DecodeString(c.SHA256)
		if err != nil || len(decoded) != 32 {
			return fmt.Errorf("sha256 must be 64 hexadecimal characters")
		}
		return nil
	default:
		return fmt.Errorf("kind must be jwt or staticToken, got %q", c.Kind)
	}
}

// CompileTenantSubjectPattern compiles a subjectPattern so that it matches only whole subjects.
// It returns an error unless the pattern has exactly one "tenant" group that can match only [a-z0-9]{1,32}.
func CompileTenantSubjectPattern(pattern string) (*regexp.Regexp, error) {
	if pattern == "" {
		return nil, fmt.Errorf("subjectPattern is required")
	}
	parsed, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return nil, fmt.Errorf("subjectPattern: %w", err)
	}
	tenantGroups := findNamedCaptures(parsed, TenantIDCaptureName)
	if len(tenantGroups) != 1 {
		return nil, fmt.Errorf("subjectPattern must contain exactly one (?P<%s>...) group", TenantIDCaptureName)
	}
	minLength, maxLength, isTenantAlphabet := tenantCaptureLengthBounds(tenantGroups[0].Sub[0])
	if !isTenantAlphabet || minLength < 1 || maxLength > MaxTenantIDLength {
		return nil, fmt.Errorf(
			"subjectPattern group %q must match only [a-z0-9]{1,%d}",
			TenantIDCaptureName,
			MaxTenantIDLength,
		)
	}
	return regexp.Compile(`^(?:` + pattern + `)$`)
}

func findNamedCaptures(expression *syntax.Regexp, name string) []*syntax.Regexp {
	var found []*syntax.Regexp
	if expression.Op == syntax.OpCapture && expression.Name == name {
		found = append(found, expression)
	}
	for _, sub := range expression.Sub {
		found = append(found, findNamedCaptures(sub, name)...)
	}
	return found
}

// tenantCaptureLengthBounds returns the shortest and longest match of expression and whether every rune it can match
// lies in [a-z0-9]. Unbounded repetition and any other construct report false.
func tenantCaptureLengthBounds(expression *syntax.Regexp) (int, int, bool) {
	switch expression.Op {
	case syntax.OpEmptyMatch:
		return 0, 0, true
	case syntax.OpLiteral:
		for _, literalRune := range expression.Rune {
			if !isTenantRune(literalRune) {
				return 0, 0, false
			}
			if expression.Flags&syntax.FoldCase != 0 && unicode.IsLetter(literalRune) {
				return 0, 0, false
			}
		}
		return len(expression.Rune), len(expression.Rune), true
	case syntax.OpCharClass:
		for i := 0; i+1 < len(expression.Rune); i += 2 {
			if !isTenantRuneRange(expression.Rune[i], expression.Rune[i+1]) {
				return 0, 0, false
			}
		}
		return 1, 1, len(expression.Rune) > 0
	case syntax.OpCapture:
		return tenantCaptureLengthBounds(expression.Sub[0])
	case syntax.OpConcat:
		minTotal, maxTotal := 0, 0
		for _, sub := range expression.Sub {
			minLength, maxLength, ok := tenantCaptureLengthBounds(sub)
			if !ok {
				return 0, 0, false
			}
			minTotal += minLength
			maxTotal += maxLength
		}
		return minTotal, maxTotal, true
	case syntax.OpAlternate:
		minAll, maxAll := -1, 0
		for _, sub := range expression.Sub {
			minLength, maxLength, ok := tenantCaptureLengthBounds(sub)
			if !ok {
				return 0, 0, false
			}
			if minAll < 0 || minLength < minAll {
				minAll = minLength
			}
			if maxLength > maxAll {
				maxAll = maxLength
			}
		}
		return minAll, maxAll, minAll >= 0
	case syntax.OpQuest:
		_, maxLength, ok := tenantCaptureLengthBounds(expression.Sub[0])
		return 0, maxLength, ok
	case syntax.OpRepeat:
		if expression.Max < 0 {
			return 0, 0, false
		}
		minLength, maxLength, ok := tenantCaptureLengthBounds(expression.Sub[0])
		return minLength * expression.Min, maxLength * expression.Max, ok
	default:
		return 0, 0, false
	}
}

func isTenantRuneRange(low rune, high rune) bool {
	return (low >= 'a' && high <= 'z') || (low >= '0' && high <= '9')
}

func isTenantRune(candidate rune) bool {
	return isTenantRuneRange(candidate, candidate)
}
