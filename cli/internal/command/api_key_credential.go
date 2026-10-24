// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package command

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	apiKeyExchangeTimeout          = 10 * time.Second
	apiKeyRefreshRetryInterval     = 5 * time.Second
	maxAPIKeyExchangeResponseBytes = 1 << 20
	maxAPIKeyExchangeErrorRunes    = 200
	// Capping the lifetime keeps expiry arithmetic far from time.Duration overflow.
	maxAPIKeyTokenLifetimeSeconds = math.MaxInt32
)

// apiKeyCredential follows the SDK API key contract; one mutex serializes exchanges because dexcli rarely calls concurrently.
type apiKeyCredential struct {
	tokenURL   *url.URL
	serverID   string
	apiKey     string
	httpClient *http.Client

	mu                   sync.Mutex
	token                string
	refreshAt            time.Time
	expiresAt            time.Time
	nextRefreshAttemptAt time.Time
}

func newAPIKeyCredential(tokenURL string, serverID string, apiKey string) (*apiKeyCredential, error) {
	parsedTokenURL, err := parseAPIKeyTokenURL(tokenURL)
	if err != nil {
		return nil, err
	}
	if serverID == "" {
		return nil, errors.New("API key server ID is empty")
	}
	if apiKey == "" {
		return nil, errors.New("API key is empty")
	}
	return &apiKeyCredential{
		tokenURL:   parsedTokenURL,
		serverID:   serverID,
		apiKey:     apiKey,
		httpClient: &http.Client{CheckRedirect: refuseAPIKeyExchangeRedirect},
	}, nil
}

func parseAPIKeyTokenURL(tokenURL string) (*url.URL, error) {
	if tokenURL == "" {
		return nil, errors.New("token URL is empty")
	}
	parsedTokenURL, err := url.Parse(tokenURL)
	if err != nil {
		return nil, fmt.Errorf("token URL is invalid: %w", err)
	}
	if parsedTokenURL.Host == "" {
		return nil, fmt.Errorf("token URL %s must be an absolute URL with a host", parsedTokenURL.Redacted())
	}
	switch parsedTokenURL.Scheme {
	case "https":
		return parsedTokenURL, nil
	case "http":
		if isLoopbackHost(parsedTokenURL.Hostname()) {
			return parsedTokenURL, nil
		}
	}
	return nil, fmt.Errorf(
		"token URL %s must use https; http is allowed only for localhost, 127.0.0.0/8, and ::1",
		parsedTokenURL.Redacted(),
	)
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// refuseAPIKeyExchangeRedirect keeps the API key from reaching any URL other than the configured one.
func refuseAPIKeyExchangeRedirect(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}

func (c *apiKeyCredential) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	hasValidToken := c.token != "" && now.Before(c.expiresAt)
	if hasValidToken && (now.Before(c.refreshAt) || now.Before(c.nextRefreshAttemptAt)) {
		return map[string]string{"authorization": "Bearer " + c.token}, nil
	}
	token, lifetime, err := c.exchange(ctx)
	if err != nil {
		if !hasValidToken {
			return nil, err
		}
		c.nextRefreshAttemptAt = time.Now().Add(apiKeyRefreshRetryInterval)
		return map[string]string{"authorization": "Bearer " + c.token}, nil
	}
	c.token = token
	c.refreshAt = now.Add(lifetime - lifetime/5)
	c.expiresAt = now.Add(lifetime)
	c.nextRefreshAttemptAt = time.Time{}
	return map[string]string{"authorization": "Bearer " + token}, nil
}

func (*apiKeyCredential) RequireTransportSecurity() bool { return false }

// exchange returns the access token and its lifetime, or an *apiKeyExchangeError.
func (c *apiKeyCredential) exchange(ctx context.Context) (string, time.Duration, error) {
	exchangeCtx, cancel := context.WithTimeout(ctx, apiKeyExchangeTimeout)
	defer cancel()
	requestBody, err := json.Marshal(apiKeyExchangeRequest{ServerID: c.serverID, APIKey: c.apiKey})
	if err != nil {
		return "", 0, c.newExchangeError(0, "encode the exchange request", err)
	}
	request, err := http.NewRequestWithContext(exchangeCtx, http.MethodPost, c.tokenURL.String(), bytes.NewReader(requestBody))
	if err != nil {
		return "", 0, c.newExchangeError(0, "create the exchange request", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	response, err := c.httpClient.Do(request)
	if err != nil {
		return "", 0, c.newTransportError(exchangeCtx, err)
	}
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, maxAPIKeyExchangeResponseBytes+1))
	if closeErr := response.Body.Close(); closeErr != nil && readErr == nil {
		readErr = closeErr
	}
	if readErr != nil {
		return "", 0, c.newTransportError(exchangeCtx, readErr)
	}
	return c.parseExchangeResponse(response.StatusCode, responseBody)
}

type apiKeyExchangeRequest struct {
	ServerID string `json:"server_id"`
	APIKey   string `json:"api_key"`
}

func (c *apiKeyCredential) parseExchangeResponse(statusCode int, responseBody []byte) (string, time.Duration, error) {
	if len(responseBody) > maxAPIKeyExchangeResponseBytes {
		return "", 0, c.newExchangeError(statusCode, "response body exceeds 1 MiB", nil)
	}
	var fields map[string]json.RawMessage
	isObject := json.Unmarshal(responseBody, &fields) == nil && fields != nil
	exchangeError := c.newExchangeError(statusCode, "", nil)
	if isObject {
		exchangeError.errorCode = truncatedJSONStringField(fields, "error")
		exchangeError.errorDescription = truncatedJSONStringField(fields, "error_description")
	}
	switch {
	case statusCode != http.StatusOK:
		return "", 0, exchangeError
	case !isObject:
		exchangeError.problem = "response is not a JSON object"
		return "", 0, exchangeError
	}
	accessToken, hasAccessToken := jsonStringField(fields, "access_token")
	if !hasAccessToken || accessToken == "" {
		exchangeError.problem = "response has no non-empty access_token string"
		return "", 0, exchangeError
	}
	tokenType, hasTokenType := jsonStringField(fields, "token_type")
	if !hasTokenType || !strings.EqualFold(tokenType, "Bearer") {
		exchangeError.problem = "response token_type is not Bearer"
		return "", 0, exchangeError
	}
	expiresInSeconds, err := strconv.ParseInt(string(bytes.TrimSpace(fields["expires_in"])), 10, 64)
	if errors.Is(err, strconv.ErrRange) && expiresInSeconds > 0 {
		err = nil
	}
	if err != nil || expiresInSeconds <= 0 {
		exchangeError.problem = "response expires_in is not a positive integer"
		return "", 0, exchangeError
	}
	expiresInSeconds = min(expiresInSeconds, maxAPIKeyTokenLifetimeSeconds)
	return accessToken, time.Duration(expiresInSeconds) * time.Second, nil
}

func jsonStringField(fields map[string]json.RawMessage, name string) (string, bool) {
	raw, hasField := fields[name]
	if !hasField {
		return "", false
	}
	var value string
	if !bytes.HasPrefix(bytes.TrimSpace(raw), []byte(`"`)) || json.Unmarshal(raw, &value) != nil {
		return "", false
	}
	return value, true
}

func truncatedJSONStringField(fields map[string]json.RawMessage, name string) string {
	value, _ := jsonStringField(fields, name)
	runes := []rune(value)
	if len(runes) > maxAPIKeyExchangeErrorRunes {
		return string(runes[:maxAPIKeyExchangeErrorRunes])
	}
	return value
}

func (c *apiKeyCredential) newTransportError(exchangeCtx context.Context, err error) *apiKeyExchangeError {
	if errors.Is(exchangeCtx.Err(), context.DeadlineExceeded) {
		return c.newExchangeError(0, "timed out", err)
	}
	return c.newExchangeError(0, "request failed", err)
}

func (c *apiKeyCredential) newExchangeError(statusCode int, problem string, cause error) *apiKeyExchangeError {
	return &apiKeyExchangeError{tokenURL: c.tokenURL.Redacted(), statusCode: statusCode, problem: problem, cause: cause}
}

// String identifies the credential by token URL and server ID; it never prints a secret.
func (c *apiKeyCredential) String() string {
	return fmt.Sprintf("APIKeyCredential{tokenURL: %s, serverID: %q}", c.tokenURL.Redacted(), c.serverID)
}

// GoString keeps %#v from printing the API key or an access token.
func (c *apiKeyCredential) GoString() string {
	return c.String()
}

// apiKeyExchangeError describes a failed exchange without the API key, any token, or the raw response body.
type apiKeyExchangeError struct {
	tokenURL         string
	statusCode       int
	problem          string
	errorCode        string
	errorDescription string
	cause            error
}

func (e *apiKeyExchangeError) Error() string {
	var message strings.Builder
	fmt.Fprintf(&message, "API key exchange with %s failed", e.tokenURL)
	if e.statusCode != 0 {
		fmt.Fprintf(&message, " with HTTP %d", e.statusCode)
	}
	if e.problem != "" {
		fmt.Fprintf(&message, ": %s", e.problem)
	}
	if e.errorCode != "" {
		fmt.Fprintf(&message, "; error %q", e.errorCode)
	}
	if e.errorDescription != "" {
		fmt.Fprintf(&message, "; error_description %q", e.errorDescription)
	}
	if e.cause != nil {
		fmt.Fprintf(&message, ": %v", e.cause)
	}
	return message.String()
}

func (e *apiKeyExchangeError) Unwrap() error {
	return e.cause
}
