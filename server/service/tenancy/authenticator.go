// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package tenancy

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"strings"

	"github.com/go-jose/go-jose/v3/jwt"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/service/common/log"
)

var errUnknownCredential = errors.New("credential is not recognized")

// Authenticator resolves the principal of a bearer credential without a remote call per request.
type Authenticator struct {
	tenantAuthenticators []tenantCredentialAuthenticator
	jwtAuthenticators    []*jwtAuthenticator
	operatorSubjects     map[operatorSubject]struct{}
	operatorTokenHashes  [][sha256.Size]byte
}

// tenantCredentialAuthenticator is one configured tenant authenticator, in configuration order.
// operatorSubject binds an operator JWT subject to the issuer that must have signed it.
type operatorSubject struct {
	issuer  string
	subject string
}

type tenantCredentialAuthenticator struct {
	jwt         *jwtAuthenticator
	staticToken *staticTokenAuthenticator
}

func NewAuthenticator(cfg *config.TenancyConfig, logger log.Logger) (*Authenticator, error) {
	if cfg == nil {
		panic("tenancy config must not be nil")
	}
	authenticator := &Authenticator{operatorSubjects: map[operatorSubject]struct{}{}}
	for i := range cfg.Authenticators {
		entry := &cfg.Authenticators[i]
		switch entry.Kind {
		case config.TenantAuthenticatorKindJWT:
			verifier, err := newJWTAuthenticator(entry, logger)
			if err != nil {
				return nil, fmt.Errorf("tenancy.authenticators[%d]: %w", i, err)
			}
			authenticator.jwtAuthenticators = append(authenticator.jwtAuthenticators, verifier)
			authenticator.tenantAuthenticators = append(
				authenticator.tenantAuthenticators,
				tenantCredentialAuthenticator{jwt: verifier},
			)
		case config.TenantAuthenticatorKindStaticTokens:
			staticTokens, err := newStaticTokenAuthenticator(entry.File, logger)
			if err != nil {
				return nil, fmt.Errorf("tenancy.authenticators[%d]: %w", i, err)
			}
			authenticator.tenantAuthenticators = append(
				authenticator.tenantAuthenticators,
				tenantCredentialAuthenticator{staticToken: staticTokens},
			)
		default:
			return nil, fmt.Errorf("tenancy.authenticators[%d]: unsupported kind %q", i, entry.Kind)
		}
	}
	for i, operator := range cfg.Operators {
		switch operator.Kind {
		case config.TenantOperatorKindJWT:
			authenticator.operatorSubjects[operatorSubject{issuer: operator.Issuer, subject: operator.Subject}] = struct{}{}
		case config.TenantOperatorKindStaticToken:
			hash, err := decodeSHA256(operator.SHA256)
			if err != nil {
				return nil, fmt.Errorf("tenancy.operators[%d]: %w", i, err)
			}
			authenticator.operatorTokenHashes = append(authenticator.operatorTokenHashes, hash)
		default:
			return nil, fmt.Errorf("tenancy.operators[%d]: unsupported kind %q", i, operator.Kind)
		}
	}
	return authenticator, nil
}

// Authenticate returns the tenant or operator principal of credential, or an error when no authenticator accepts it.
func (a *Authenticator) Authenticate(ctx context.Context, credential string) (*Principal, error) {
	credentialHash := sha256.Sum256([]byte(credential))
	for _, operatorHash := range a.operatorTokenHashes {
		if subtle.ConstantTimeCompare(credentialHash[:], operatorHash[:]) == 1 {
			return &Principal{Kind: PrincipalKindOperator}, nil
		}
	}
	token, issuer := parseUnverifiedJWT(credential)
	if token != nil {
		if principal, err := a.authenticateOperatorJWT(ctx, token, issuer); principal != nil || err != nil {
			return principal, err
		}
	}
	for _, tenantAuthenticator := range a.tenantAuthenticators {
		if tenantAuthenticator.staticToken != nil {
			if principal, ok := tenantAuthenticator.staticToken.authenticate(credentialHash); ok {
				return principal, nil
			}
			continue
		}
		if token == nil || tenantAuthenticator.jwt.issuer != issuer {
			continue
		}
		subject, allClaims, err := tenantAuthenticator.jwt.verify(ctx, token)
		if err != nil {
			return nil, err
		}
		return tenantAuthenticator.jwt.tenantPrincipal(subject, allClaims)
	}
	return nil, errUnknownCredential
}

// authenticateOperatorJWT returns an operator principal for a verified operator subject, or nil when it is not one.
func (a *Authenticator) authenticateOperatorJWT(
	ctx context.Context,
	token *jwt.JSONWebToken,
	issuer string,
) (*Principal, error) {
	if len(a.operatorSubjects) == 0 {
		return nil, nil
	}
	for _, verifier := range a.jwtAuthenticators {
		if verifier.issuer != issuer {
			continue
		}
		subject, _, err := verifier.verify(ctx, token)
		if err != nil {
			return nil, err
		}
		if _, isOperator := a.operatorSubjects[operatorSubject{issuer: issuer, subject: subject}]; isOperator {
			return &Principal{Kind: PrincipalKindOperator}, nil
		}
		return nil, nil
	}
	return nil, nil
}

// parseUnverifiedJWT returns the token and its unverified issuer, used only to select a verifier.
func parseUnverifiedJWT(credential string) (*jwt.JSONWebToken, string) {
	if strings.Count(credential, ".") != 2 {
		return nil, ""
	}
	token, err := jwt.ParseSigned(credential)
	if err != nil {
		return nil, ""
	}
	var claims jwt.Claims
	if err := token.UnsafeClaimsWithoutVerification(&claims); err != nil {
		return nil, ""
	}
	return token, claims.Issuer
}
