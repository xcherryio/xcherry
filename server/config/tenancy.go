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
	"net"
	"net/url"
	"regexp"
	"regexp/syntax"
	"text/template"
	"unicode"
)

const (
	// TenancyModeSingle serves one namespace for every caller without authentication. It is the default.
	TenancyModeSingle TenancyMode = "single"
	// TenancyModeMulti will serve many tenants in one namespace. It is under development: the server refuses it at
	// startup until tenant isolation of Flow identities, visibility and Worker dispatch is complete.
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
		// MaxStoredFlowIDBytes is the backend's identifier limit, which a tenant's stored Flow ID
		// (the Flow ID plus the "T:<tenant>:" prefix of up to 36 bytes) must respect. Default 1000, Temporal's
		// default limit.maxIDLength and the Temporal Cloud limit. Set it to the backend's value when it is lower.
		// A StartFlow over the limit is INVALID_ARGUMENT; a SubFlow over it fails its Step before any start.
		MaxStoredFlowIDBytes int `yaml:"maxStoredFlowIDBytes"`
		// IsIncompleteMultiTenantModeAllowedForTest lets integration tests start multi mode while it is under
		// development. It cannot be set from a configuration file. Default false.
		IsIncompleteMultiTenantModeAllowedForTest bool `yaml:"-"`
	}

	TenantAuthenticatorConfig struct {
		// Kind is jwt or staticTokens. Required.
		Kind TenantAuthenticatorKind `yaml:"kind"`
		// Issuer is the exact "iss" claim a JWT must carry. Required for jwt and unique among jwt authenticators.
		// It must be an https URL; http is accepted only for a loopback host.
		// When JWKSURL is empty, the key set URL is discovered from <issuer>/.well-known/openid-configuration,
		// whose "issuer" must equal Issuer and whose "jwks_uri" must also be https (or loopback http).
		Issuer string `yaml:"issuer"`
		// Audiences lists accepted "aud" values. Required for jwt; a token must name at least one of them.
		Audiences []string `yaml:"audiences"`
		// JWKSURL is the JSON Web Key Set URL. Default empty uses OIDC discovery from Issuer.
		// Keys are cached and refreshed at most once a minute; a token with an unknown key ID triggers one bounded refresh.
		// No request makes a remote call otherwise.
		// It must be an https URL; http is accepted only for a loopback host.
		JWKSURL string `yaml:"jwksURL"`
		// CAFile is a PEM bundle trusted for the issuer's https endpoints in addition to the system roots,
		// for example a cluster's service account issuer CA. Default empty uses the system roots only.
		CAFile string `yaml:"caFile"`
		// SubjectPattern is a regular expression that must match the whole "sub" claim. Required for jwt.
		// Its named group "tenant" is the tenant ID and must be able to match only [a-z0-9]{1,32},
		// for example '^system:serviceaccount:apps-(?P<tenant>[a-z0-9]{1,32}):app$'.
		// The group must be a top-level part of the pattern and be followed by the end of the pattern or by a literal
		// that does not start with [a-z0-9], so that no subject can be read as two different tenants.
		// A subject that does not match is refused: the tenant is derived fail-closed.
		// Other named groups become principal claims available to templates.
		SubjectPattern string `yaml:"subjectPattern"`
		// Claims maps a JWT claim name to a Go template rendered with the subject pattern's named groups.
		// The token's claim must be a string equal to the rendered value, for example {namespace: 'apps-{{ .tenant }}'}.
		// Each verified claim also becomes a principal claim. Default empty.
		Claims map[string]string `yaml:"claims"`
		// File is the static token file for staticTokens. Required for staticTokens.
		// It is YAML of the form "tokens: [{sha256: <hex>, tenant: <id>, claims: {<name>: <value>}}]".
		// Tokens must carry at least 128 bits of entropy, for example 32 random hexadecimal characters.
		// The file is reread when its modification time or size changes. An empty or deleted file means no tokens.
		// Malformed content is logged and the previous tokens stay in effect until the file is fixed.
		// The file must exist and be valid at startup. To rotate a token, add the new hash before removing the old one.
		File string `yaml:"file"`
	}

	TenantOperatorConfig struct {
		// Kind is jwt or staticToken. Required.
		Kind TenantOperatorKind `yaml:"kind"`
		// Issuer selects the jwt authenticator that verifies an operator JWT. Required for jwt.
		// It must equal the issuer of a configured jwt authenticator.
		Issuer string `yaml:"issuer"`
		// Subject is the exact "sub" claim of an operator JWT from Issuer. Required for jwt.
		Subject string `yaml:"subject"`
		// SHA256 is the lowercase hexadecimal SHA-256 hash of an operator static token. Required for staticToken.
		SHA256 string `yaml:"sha256"`
	}

	TenancyInternalConfig struct {
		// Listen is the host:port bind address of the InternalService gRPC listener, for example 127.0.0.1:8803.
		// Only the server's own interpreter Workers connect to it. Required in multi mode.
		// Interpreter Workers dial interpreter.interpreterActivityConfig.internalServiceTarget when set, otherwise this address.
		// The listener serves plaintext gRPC. Bind it to loopback, or restrict it to the server's own Pods with a
		// network policy and encrypt the network path, because the token is sent in clear text.
		Listen string `yaml:"listen"`
		// TokenFile holds the bearer tokens that InternalService callers may present, one per line. Required in multi mode.
		// Each token must be at least 32 characters. Interpreter Workers send the first line; every line is accepted,
		// so a token is rotated by adding the new token as the first line, rolling out, then removing the old line.
		// Servers and interpreter Workers sharing a namespace must read the same file. The file is reread when it
		// changes; an empty or deleted file refuses every caller.
		TokenFile string `yaml:"tokenFile"`
	}
)

// DefaultMaxStoredFlowIDBytes is Temporal's default identifier limit.
const DefaultMaxStoredFlowIDBytes = 1000

// EffectiveMaxStoredFlowIDBytes returns MaxStoredFlowIDBytes or its default.
func (c TenancyConfig) EffectiveMaxStoredFlowIDBytes() int {
	if c.MaxStoredFlowIDBytes <= 0 {
		return DefaultMaxStoredFlowIDBytes
	}
	return c.MaxStoredFlowIDBytes
}

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
	if !c.Tenancy.IsIncompleteMultiTenantModeAllowedForTest {
		return fmt.Errorf("tenancy.mode multi is under development and not available yet")
	}
	return c.Tenancy.validateMultiTenant()
}

func (c TenancyConfig) validateMultiTenant() error {
	if len(c.Authenticators) == 0 {
		return fmt.Errorf("tenancy.authenticators must not be empty in multi mode")
	}
	jwtIssuers := map[string]bool{}
	for i, authenticator := range c.Authenticators {
		if err := authenticator.validate(); err != nil {
			return fmt.Errorf("tenancy.authenticators[%d]: %w", i, err)
		}
		if authenticator.Kind != TenantAuthenticatorKindJWT {
			continue
		}
		if jwtIssuers[authenticator.Issuer] {
			return fmt.Errorf("tenancy.authenticators[%d]: issuer %q is configured twice", i, authenticator.Issuer)
		}
		jwtIssuers[authenticator.Issuer] = true
	}
	for i, operator := range c.Operators {
		if err := operator.validate(jwtIssuers); err != nil {
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
		if err := ValidateIssuerURL("issuer", c.Issuer); err != nil {
			return err
		}
		if c.JWKSURL != "" {
			if err := ValidateIssuerURL("jwksURL", c.JWKSURL); err != nil {
				return err
			}
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

func (c TenantOperatorConfig) validate(jwtIssuers map[string]bool) error {
	switch c.Kind {
	case TenantOperatorKindJWT:
		if c.Subject == "" {
			return fmt.Errorf("subject is required")
		}
		if !jwtIssuers[c.Issuer] {
			return fmt.Errorf("issuer %q is not the issuer of a jwt authenticator", c.Issuer)
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
	if !isTenantCaptureDelimited(parsed, tenantGroups[0]) {
		return nil, fmt.Errorf(
			"subjectPattern group %q must be followed by the end of the pattern or a literal not starting with [a-z0-9]",
			TenantIDCaptureName,
		)
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

// isTenantCaptureDelimited reports whether capture is a top-level part of pattern followed by its end or by a literal
// whose first rune cannot continue a tenant ID.
func isTenantCaptureDelimited(pattern *syntax.Regexp, capture *syntax.Regexp) bool {
	if pattern == capture {
		return true
	}
	if pattern.Op != syntax.OpConcat {
		return false
	}
	for i, part := range pattern.Sub {
		if part != capture {
			continue
		}
		if i+1 == len(pattern.Sub) {
			return true
		}
		next := pattern.Sub[i+1]
		switch next.Op {
		case syntax.OpEndText, syntax.OpEndLine:
			return true
		case syntax.OpLiteral:
			return !isTenantRune(unicode.ToLower(next.Rune[0]))
		}
		return false
	}
	return false
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
