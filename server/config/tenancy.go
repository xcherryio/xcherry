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
	"slices"
	"text/template"
	"text/template/parse"
	"time"
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
		// MaxStoredFlowIDBytes is the backend's identifier limit, which a tenant's stored Flow ID
		// (the Flow ID plus the "T:<tenant>:" prefix of up to 36 bytes) must respect. Default 1000, Temporal's
		// default limit.maxIDLength and the Temporal Cloud limit. Set it to the backend's value when it is lower.
		// A StartFlow over the limit is INVALID_ARGUMENT; a SubFlow over it fails its Step before any start.
		MaxStoredFlowIDBytes int `yaml:"maxStoredFlowIDBytes"`
		// WorkerTarget decides which Worker addresses each tenant's Flows may dispatch to. Required in multi mode.
		WorkerTarget TenantWorkerTargetConfig `yaml:"workerTarget"`
		// Wake optionally asks an operator endpoint to start a tenant's idle Worker. Default empty disables it.
		Wake TenantWakeConfig `yaml:"wake"`
		// Limits bound what one tenant can use of a shared server.
		Limits TenantLimitsConfig `yaml:"limits"`
		// AttributeStores lists the Attribute Stores a tenant's Flow may select. Default empty allows every
		// configured store, which multi mode requires to be tenant-scoped.
		AttributeStores []string `yaml:"attributeStores"`
		// Fairness sets the engine fairness key of every workflow and activity task to the tenant ID, so the
		// namespace's task queue serves tenants round-robin. Default false. The namespace must enable task queue
		// fairness (Temporal Cloud namespace setting, or matching.enableFairness on a self-hosted server).
		Fairness bool `yaml:"fairness"`
	}

	TenantWorkerTargetConfig struct {
		// Default is a Go template rendered with the principal's claims and used as the Worker address when a
		// start or SubFlow requests none, for example 'worker.{{ .namespace }}.svc.cluster.local:9000'.
		// Default empty: a Flow must name a target.
		Default string `yaml:"default"`
		// Allowed is a Go template rendered with the principal's claims into a regular expression that must match
		// the whole requested Worker address, for example '^worker\.{{ .namespace }}\.svc\.cluster\.local:9000$'.
		// Every claim must pass through the template function quote, which escapes it for use in the expression,
		// for example '^worker\.{{ quote .namespace }}:9000$'; the server refuses to start otherwise, so a claim
		// value with regular expression metacharacters can never widen the pattern. Required in multi mode.
		// The persisted target of every dispatch is matched again, so a configuration change takes effect for
		// running Flows. Set it to exactly one stable endpoint per tenant for the strictest isolation.
		Allowed string `yaml:"allowed"`
	}

	TenantWakeConfig struct {
		// URL receives a POST {"tenant", "target", "kind"} with the internal token as bearer credential:
		// kind "wake" when a dispatch finds its Worker unavailable, at most once per target per 10 seconds, and
		// kind "active" after a successful dispatch, at most once per target per minute. Default empty disables it.
		// The hook is best effort; a failed POST is logged.
		URL string `yaml:"url"`
		// Budget is how long a dispatch to an unavailable Worker is held, redialing every 2 seconds, before the
		// attempt fails into the Step's retry policy. Holding does not count an attempt. Default 120s.
		Budget time.Duration `yaml:"budget"`
	}

	TenantLimitsConfig struct {
		// MaxConcurrentDispatchesPerTenant bounds one tenant's concurrent WaitFor, Execute and Worker RPC
		// dispatches per server process. A dispatch over the cap waits for a slot without counting an attempt.
		// Default 32.
		MaxConcurrentDispatchesPerTenant int `yaml:"maxConcurrentDispatchesPerTenant"`
		// MaxStreamCapacityBytes caps the Stream capacity that a tenant's write may request. Default 1 MiB.
		MaxStreamCapacityBytes int64 `yaml:"maxStreamCapacityBytes"`
		// MaxStreamBytesPerTenant caps one tenant's buffered Stream bytes across all its Streams. Default 8 MiB.
		// The memory backend enforces it per server process; the redis backend enforces it across every server
		// sharing the Redis, with a per-tenant total key that trimming decreases.
		MaxStreamBytesPerTenant int64 `yaml:"maxStreamBytesPerTenant"`
		// StartFlowsPerSecond is a tenant's StartFlow token bucket per server process. Default 20 per second, burst 100.
		StartFlowsPerSecond TenantRateLimitConfig `yaml:"startFlowsPerSecond"`
		// WritesPerSecond is a tenant's bucket for InvokeRPC, PublishToChannel, DeleteChannelMessage, SetAttributes
		// and WriteStream per server process. Default 100 per second, burst 500.
		WritesPerSecond TenantRateLimitConfig `yaml:"writesPerSecond"`
		// SearchesPerSecond is a tenant's SearchFlows bucket per server process. Default 1 per second, burst 5.
		// Temporal Cloud allows 30 visibility calls per second per namespace, shared by every tenant.
		SearchesPerSecond TenantRateLimitConfig `yaml:"searchesPerSecond"`
	}

	// TenantRateLimitConfig is one token bucket. With N server replicas the effective rate is N times the setting.
	// A call over the bucket is RESOURCE_EXHAUSTED.
	TenantRateLimitConfig struct {
		// Rate is the sustained calls per second. Zero uses the bucket's default.
		Rate float64 `yaml:"rate"`
		// Burst is the bucket size. Zero uses the bucket's default.
		Burst int `yaml:"burst"`
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
	for name, store := range c.AttributeStore.Stores {
		if store.Type != AttributeStoreTypePostgres && store.Type != AttributeStoreTypeMySQL {
			return fmt.Errorf("tenancy.mode multi supports only postgres and mysql Attribute Stores; %q is %s", name, store.Type)
		}
		if store.TenantColumn == "" {
			return fmt.Errorf("tenancy.mode multi requires attributeStore.stores.%s.tenantColumn", name)
		}
	}
	for _, name := range c.Tenancy.AttributeStores {
		if _, found := c.AttributeStore.Stores[name]; !found {
			return fmt.Errorf("tenancy.attributeStores names unknown Attribute Store %q", name)
		}
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
	if err := c.WorkerTarget.validate(); err != nil {
		return err
	}
	if c.Wake.URL != "" {
		if _, err := url.ParseRequestURI(c.Wake.URL); err != nil {
			return fmt.Errorf("tenancy.wake.url: %w", err)
		}
	}
	if c.Wake.Budget < 0 || c.Limits.MaxConcurrentDispatchesPerTenant < 0 {
		return fmt.Errorf("tenancy.wake.budget and tenancy.limits must not be negative")
	}
	return nil
}

func (c TenantWorkerTargetConfig) validate() error {
	if c.Allowed == "" {
		return fmt.Errorf("tenancy.workerTarget.allowed is required in multi mode")
	}
	for name, workerTargetTemplate := range map[string]string{"default": c.Default, "allowed": c.Allowed} {
		if _, err := NewTenantClaimTemplate(name, workerTargetTemplate); err != nil {
			return fmt.Errorf("tenancy.workerTarget.%s: %w", name, err)
		}
	}
	allowed, err := NewTenantClaimTemplate("allowed", c.Allowed)
	if err != nil {
		return err
	}
	if err := requireQuotedClaims(allowed.Tree.Root); err != nil {
		return fmt.Errorf("tenancy.workerTarget.allowed: %w", err)
	}
	return nil
}

// requireQuotedClaims refuses an allowed template unless every action is a claim passed through quote, such as
// {{ quote .namespace }} or {{ .namespace | quote }}, so a claim value can never widen the pattern.
func requireQuotedClaims(node parse.Node) error {
	switch typed := node.(type) {
	case *parse.ListNode:
		for _, child := range typed.Nodes {
			if err := requireQuotedClaims(child); err != nil {
				return err
			}
		}
		return nil
	case *parse.TextNode:
		return nil
	case *parse.ActionNode:
		if isQuotedClaimPipe(typed.Pipe) {
			return nil
		}
		return fmt.Errorf("%s must be a claim passed through quote, such as {{ quote .tenant }}", typed)
	default:
		return fmt.Errorf("%s is not allowed; use only text and quoted claims", node)
	}
}

func isQuotedClaimPipe(pipe *parse.PipeNode) bool {
	if pipe == nil || len(pipe.Decl) > 0 {
		return false
	}
	var arguments []parse.Node
	switch len(pipe.Cmds) {
	case 1:
		arguments = pipe.Cmds[0].Args
	case 2:
		if len(pipe.Cmds[0].Args) != 1 || len(pipe.Cmds[1].Args) != 1 {
			return false
		}
		arguments = []parse.Node{pipe.Cmds[1].Args[0], pipe.Cmds[0].Args[0]}
	default:
		return false
	}
	if len(arguments) != 2 {
		return false
	}
	function, isIdentifier := arguments[0].(*parse.IdentifierNode)
	field, isField := arguments[1].(*parse.FieldNode)
	return isIdentifier && function.Ident == "quote" && isField && len(field.Ident) == 1
}

// NewTenantClaimTemplate parses a template rendered with principal claims; a missing claim fails rendering.
func NewTenantClaimTemplate(name string, text string) (*template.Template, error) {
	return template.New(name).
		Option("missingkey=error").
		Funcs(template.FuncMap{"quote": regexp.QuoteMeta}).
		Parse(text)
}

// DefaultTenantWakeBudget is how long a dispatch to an unavailable Worker is held by default.
const DefaultTenantWakeBudget = 120 * time.Second

// DefaultMaxConcurrentDispatchesPerTenant is the default per-tenant dispatch cap per process.
const DefaultMaxConcurrentDispatchesPerTenant = 32

// EffectiveBudget returns Budget or its default.
func (c TenantWakeConfig) EffectiveBudget() time.Duration {
	if c.Budget <= 0 {
		return DefaultTenantWakeBudget
	}
	return c.Budget
}

// EffectiveTenantRateLimit returns limit with zero fields replaced by the given defaults.
func EffectiveTenantRateLimit(limit TenantRateLimitConfig, defaultRate float64, defaultBurst int) TenantRateLimitConfig {
	if limit.Rate <= 0 {
		limit.Rate = defaultRate
	}
	if limit.Burst <= 0 {
		limit.Burst = defaultBurst
	}
	return limit
}

// EffectiveMaxStreamCapacityBytes returns MaxStreamCapacityBytes or its 1 MiB default.
func (c TenantLimitsConfig) EffectiveMaxStreamCapacityBytes() int64 {
	if c.MaxStreamCapacityBytes <= 0 {
		return 1 << 20
	}
	return c.MaxStreamCapacityBytes
}

// EffectiveMaxStreamBytesPerTenant returns MaxStreamBytesPerTenant or its 8 MiB default.
func (c TenantLimitsConfig) EffectiveMaxStreamBytesPerTenant() int64 {
	if c.MaxStreamBytesPerTenant <= 0 {
		return 8 << 20
	}
	return c.MaxStreamBytesPerTenant
}

// EffectiveMaxConcurrentDispatchesPerTenant returns the cap or its default.
func (c TenantLimitsConfig) EffectiveMaxConcurrentDispatchesPerTenant() int {
	if c.MaxConcurrentDispatchesPerTenant <= 0 {
		return DefaultMaxConcurrentDispatchesPerTenant
	}
	return c.MaxConcurrentDispatchesPerTenant
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

// IsAttributeStoreAllowed reports whether a tenant's Flow may select the named Attribute Store.
func (c TenancyConfig) IsAttributeStoreAllowed(name string) bool {
	return len(c.AttributeStores) == 0 || slices.Contains(c.AttributeStores, name)
}
