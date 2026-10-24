// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package dex

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
	defaultAPIKeyExchangeTimeout   = 10 * time.Second
	apiKeyRefreshRetryInterval     = 5 * time.Second
	maxAPIKeyExchangeResponseBytes = 1 << 20
	maxAPIKeyExchangeErrorRunes    = 200
	// Capping the lifetime keeps expiry arithmetic far from time.Duration overflow.
	maxAPIKeyTokenLifetimeSeconds = math.MaxInt32
)

// APIKeyCredentialOption configures an APIKeyCredential.
//
// Create options with WithAPIKeyExchangeTimeout and WithAPIKeyHTTPClient, and pass
// them to APIKeyCredential. Options apply in order, so a later option of the same
// kind replaces an earlier one. See APIKeyCredential for a complete example.
type APIKeyCredentialOption interface {
	applyAPIKeyCredential(*apiKeyCredentialSettings)
}

// WithAPIKeyExchangeTimeout bounds each token exchange of an APIKeyCredential.
//
// The timeout covers connecting to the token endpoint, sending the request, and
// reading the complete response. The default is 10 seconds. A Dex call that has no
// valid cached token waits at most this long for the exchange, and less when its
// own context ends first. The timeout applies through the request context, so an
// HTTP client supplied with WithAPIKeyHTTPClient keeps its own Timeout as an
// additional limit.
//
//	credential, err := dex.APIKeyCredential(tokenURL, serverID, apiKey,
//		dex.WithAPIKeyExchangeTimeout(3*time.Second))
//
// timeout must be positive. APIKeyCredential returns an error for a zero or
// negative timeout.
func WithAPIKeyExchangeTimeout(timeout time.Duration) APIKeyCredentialOption {
	return apiKeyExchangeTimeoutOption(timeout)
}

// WithAPIKeyHTTPClient sets the HTTP client that an APIKeyCredential uses for token exchanges.
//
// Use it to route exchanges through a proxy, to trust a private certificate
// authority, or to reach an in-process token endpoint in tests. By default the
// credential uses a client built on http.DefaultTransport, which honors the
// standard proxy environment variables. The credential copies the client and
// replaces its CheckRedirect so that it never follows a redirect. A redirect
// response fails the exchange instead, so the API key is sent only to the
// configured token URL. The credential never closes the client or its transport;
// the caller keeps ownership of both.
//
//	httpClient := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}}
//	credential, err := dex.APIKeyCredential(tokenURL, serverID, apiKey,
//		dex.WithAPIKeyHTTPClient(httpClient))
//
// httpClient must not be nil. APIKeyCredential returns an error for a nil client.
func WithAPIKeyHTTPClient(httpClient *http.Client) APIKeyCredentialOption {
	return apiKeyHTTPClientOption{httpClient: httpClient}
}

// APIKeyCredential returns a Credential that exchanges a long-lived API key for short-lived access tokens.
//
// Use it when a Dex server with JWT authentication issues access tokens through a
// token endpoint and the application holds an API key. Use FileCredential instead
// when another process writes and rotates the access token in a file. Pass the
// returned Credential in ClientOptions.Credential or WorkerOptions.Credential.
//
//	apiKey := os.Getenv("DEX_API_KEY")
//	credential, err := dex.APIKeyCredential(
//		"https://auth.example.com/oauth/token",
//		"orders-production",
//		apiKey,
//	)
//	if err != nil {
//		return err
//	}
//	client, err := dex.NewClient(registry, cache, dex.ClientOptions{Credential: credential})
//	if err != nil {
//		return err
//	}
//	defer client.Close()
//
// # Exchange
//
// An exchange sends POST to the token URL with Content-Type and Accept set to
// application/json. The body is {"server_id":"<server ID>","api_key":"<API key>"}.
// A successful response is HTTP 200 with a JSON object that holds a non-empty
// access_token string, a token_type equal to Bearer in any letter case, and a
// positive integer expires_in in seconds. Unknown response fields are ignored.
// Construction validates its arguments without contacting the token endpoint. The
// first Token call performs the first exchange.
//
// # Caching and refresh
//
// The credential caches the access token. Its expiry is measured with the
// monotonic clock from the moment the exchange request started, plus expires_in.
// After 80% of that lifetime, the next Token call starts a refresh on a separate
// goroutine and still returns the cached token immediately. At most one exchange
// is in flight per credential, and concurrent callers that need a token share its
// result. When a refresh fails while the cached token is still valid, the
// credential keeps serving the cached token and retries the refresh at most once
// every 5 seconds. After the token expires, a Dex call waits for the in-flight
// exchange, bounded by the exchange timeout and by the call's context. The call
// fails with the exchange error when no token arrives.
//
// # Rejected tokens
//
// When Dex rejects a call with gRPC UNAUTHENTICATED, the SDK discards the token
// that the call used, but only while it is still the cached token. A rejection of
// an older token therefore never discards a newer one. A Client or Worker retries
// a rejected unary call exactly once with a fresh token. The retry is safe because
// Dex rejects an unauthenticated call before executing it. A second rejection
// fails the call with the UNAUTHENTICATED error. A rejected pull Worker poll stream
// only discards the token, and the Worker reconnects through its usual stream
// reconnection, which waits about a minute after an UNAUTHENTICATED rejection.
// FileCredential and application Credential implementations do not get this retry.
//
// # Security
//
// The token URL must use https. Plain http is allowed only for loopback hosts:
// localhost, 127.0.0.0/8, and ::1. The credential never follows redirects. It
// never includes the API key or an access token in an error, in log output, or
// in its formatted value. Format verbs such as %v, %+v, and %#v print only the
// token URL and the server ID. Errors include the token-endpoint error and
// error_description string fields, truncated to 200 characters, but never the
// raw response body. The Dex connection itself is plaintext gRPC, so protect the
// network path between the application and Dex.
//
// # Concurrency and lifecycle
//
// The credential is safe for concurrent use and may be shared by several Clients
// and Workers. It starts no long-lived goroutine. Each exchange runs on its own
// goroutine that ends within the exchange timeout. The default HTTP client uses
// the shared http.DefaultTransport. There is nothing to close, and dropping the
// credential leaves no running work beyond an exchange already in flight.
//
// # Errors
//
// APIKeyCredential returns an error when tokenURL, serverID, or apiKey is empty,
// when tokenURL does not parse as an absolute URL, when tokenURL uses a scheme
// other than https or uses http with a non-loopback host, when the exchange
// timeout is not positive, or when the HTTP client option is nil.
//
// A failed exchange fails the Dex call that needed a token with an error that
// names the token URL. When the endpoint responded, the error also names the HTTP
// status and includes the error and error_description string fields when present.
// Exchange errors cover non-200 statuses including redirects, responses that are
// not a JSON object, missing or invalid access_token, token_type, or expires_in
// fields, transport failures, and timeouts. Transport failures and timeouts wrap
// the underlying error, so errors.Is(err, context.DeadlineExceeded) reports a
// timeout. A failed refresh does not fail any call while the cached token is
// still valid.
//
// tokenURL is the token endpoint. serverID identifies the Dex server that the
// endpoint scopes the access token to; the application supplies it and the SDK
// never derives it from a host name. apiKey is the secret API key. options holds
// optional settings. The returned Credential is safe for concurrent use.
func APIKeyCredential(
	tokenURL string,
	serverID string,
	apiKey string,
	options ...APIKeyCredentialOption,
) (Credential, error) {
	parsedTokenURL, err := parseAPIKeyTokenURL(tokenURL)
	if err != nil {
		return nil, err
	}
	if serverID == "" {
		return nil, errors.New("dex: API key credential server ID must not be empty")
	}
	if apiKey == "" {
		return nil, errors.New("dex: API key credential API key must not be empty")
	}
	settings := apiKeyCredentialSettings{exchangeTimeout: defaultAPIKeyExchangeTimeout}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("dex: API key credential option must not be nil")
		}
		option.applyAPIKeyCredential(&settings)
	}
	if settings.exchangeTimeout <= 0 {
		return nil, fmt.Errorf("dex: API key exchange timeout must be positive, got %s", settings.exchangeTimeout)
	}
	httpClient := &http.Client{}
	if settings.hasHTTPClient {
		if settings.httpClient == nil {
			return nil, errors.New("dex: API key credential HTTP client must not be nil")
		}
		copiedHTTPClient := *settings.httpClient
		httpClient = &copiedHTTPClient
	}
	httpClient.CheckRedirect = refuseAPIKeyExchangeRedirect
	return &apiKeyCredential{
		tokenURL:        parsedTokenURL,
		serverID:        serverID,
		apiKey:          apiKey,
		exchangeTimeout: settings.exchangeTimeout,
		httpClient:      httpClient,
		now:             time.Now,
	}, nil
}

type apiKeyCredentialSettings struct {
	exchangeTimeout time.Duration
	httpClient      *http.Client
	hasHTTPClient   bool
}

type apiKeyExchangeTimeoutOption time.Duration

func (option apiKeyExchangeTimeoutOption) applyAPIKeyCredential(settings *apiKeyCredentialSettings) {
	settings.exchangeTimeout = time.Duration(option)
}

type apiKeyHTTPClientOption struct {
	httpClient *http.Client
}

func (option apiKeyHTTPClientOption) applyAPIKeyCredential(settings *apiKeyCredentialSettings) {
	settings.httpClient = option.httpClient
	settings.hasHTTPClient = true
}

func parseAPIKeyTokenURL(tokenURL string) (*url.URL, error) {
	if tokenURL == "" {
		return nil, errors.New("dex: API key credential token URL must not be empty")
	}
	parsedTokenURL, err := url.Parse(tokenURL)
	if err != nil {
		return nil, fmt.Errorf("dex: API key credential token URL is invalid: %w", err)
	}
	if parsedTokenURL.Host == "" {
		return nil, fmt.Errorf("dex: API key credential token URL %s must be an absolute URL with a host", parsedTokenURL.Redacted())
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
		"dex: API key credential token URL %s must use https; http is allowed only for localhost, 127.0.0.0/8, and ::1",
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

func refuseAPIKeyExchangeRedirect(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}

// apiKeyCredential caches one access token and runs at most one exchange at a time.
type apiKeyCredential struct {
	tokenURL        *url.URL
	serverID        string
	apiKey          string
	exchangeTimeout time.Duration
	httpClient      *http.Client
	// now must return monotonic readings; tests replace it before the first Token call.
	now func() time.Time

	mu                   sync.Mutex
	token                string
	refreshAt            time.Time
	expiresAt            time.Time
	nextRefreshAttemptAt time.Time
	inFlightExchange     *apiKeyExchange
}

// apiKeyExchange is one token exchange; done closes after token and err are set.
type apiKeyExchange struct {
	done  chan struct{}
	token string
	err   error
}

func (credential *apiKeyCredential) Token(ctx context.Context) (string, error) {
	credential.mu.Lock()
	now := credential.now()
	if credential.token != "" && now.Before(credential.expiresAt) {
		token := credential.token
		if !now.Before(credential.refreshAt) && !now.Before(credential.nextRefreshAttemptAt) {
			credential.startExchangeLocked()
		}
		credential.mu.Unlock()
		return token, nil
	}
	exchange := credential.startExchangeLocked()
	credential.mu.Unlock()
	select {
	case <-exchange.done:
		if exchange.err != nil {
			return "", exchange.err
		}
		return exchange.token, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (credential *apiKeyCredential) discardRejectedToken(token string) {
	credential.mu.Lock()
	defer credential.mu.Unlock()
	if token == "" || token != credential.token {
		return
	}
	credential.token = ""
	credential.refreshAt = time.Time{}
	credential.expiresAt = time.Time{}
}

// startExchangeLocked returns the in-flight exchange, starting one when none is running.
func (credential *apiKeyCredential) startExchangeLocked() *apiKeyExchange {
	if credential.inFlightExchange != nil {
		return credential.inFlightExchange
	}
	exchange := &apiKeyExchange{done: make(chan struct{})}
	credential.inFlightExchange = exchange
	go credential.runExchange(exchange)
	return exchange
}

func (credential *apiKeyCredential) runExchange(exchange *apiKeyExchange) {
	// The exchange is shared by every waiting caller, so no single call's context may cancel it.
	ctx, cancel := context.WithTimeout(context.Background(), credential.exchangeTimeout)
	defer cancel()
	startedAt := credential.now()
	token, lifetime, err := credential.exchangeAPIKey(ctx)
	credential.mu.Lock()
	defer credential.mu.Unlock()
	credential.inFlightExchange = nil
	if err != nil {
		credential.nextRefreshAttemptAt = credential.now().Add(apiKeyRefreshRetryInterval)
	} else {
		credential.token = token
		credential.refreshAt = startedAt.Add(lifetime - lifetime/5)
		credential.expiresAt = startedAt.Add(lifetime)
		credential.nextRefreshAttemptAt = time.Time{}
	}
	exchange.token, exchange.err = token, err
	close(exchange.done)
}

// exchangeAPIKey returns the access token and its lifetime, or an *apiKeyExchangeError.
func (credential *apiKeyCredential) exchangeAPIKey(ctx context.Context) (string, time.Duration, error) {
	requestBody, err := json.Marshal(apiKeyExchangeRequest{ServerID: credential.serverID, APIKey: credential.apiKey})
	if err != nil {
		return "", 0, credential.newExchangeError(0, "encode the exchange request", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, credential.tokenURL.String(), bytes.NewReader(requestBody))
	if err != nil {
		return "", 0, credential.newExchangeError(0, "create the exchange request", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	response, err := credential.httpClient.Do(request)
	if err != nil {
		return "", 0, credential.newTransportError(ctx, err)
	}
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, maxAPIKeyExchangeResponseBytes+1))
	if closeErr := response.Body.Close(); closeErr != nil && readErr == nil {
		readErr = closeErr
	}
	if readErr != nil {
		return "", 0, credential.newTransportError(ctx, readErr)
	}
	return credential.parseExchangeResponse(response.StatusCode, responseBody)
}

type apiKeyExchangeRequest struct {
	ServerID string `json:"server_id"`
	APIKey   string `json:"api_key"`
}

func (credential *apiKeyCredential) parseExchangeResponse(statusCode int, responseBody []byte) (string, time.Duration, error) {
	if len(responseBody) > maxAPIKeyExchangeResponseBytes {
		return "", 0, credential.newExchangeError(statusCode, "response body exceeds 1 MiB", nil)
	}
	var fields map[string]json.RawMessage
	isObject := json.Unmarshal(responseBody, &fields) == nil && fields != nil
	exchangeError := credential.newExchangeError(statusCode, "", nil)
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

func (credential *apiKeyCredential) newTransportError(ctx context.Context, err error) *apiKeyExchangeError {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return credential.newExchangeError(0, fmt.Sprintf("timed out after %s", credential.exchangeTimeout), err)
	}
	return credential.newExchangeError(0, "request failed", err)
}

func (credential *apiKeyCredential) newExchangeError(statusCode int, problem string, cause error) *apiKeyExchangeError {
	return &apiKeyExchangeError{
		tokenURL:   credential.tokenURL.Redacted(),
		statusCode: statusCode,
		problem:    problem,
		cause:      cause,
	}
}

// String identifies the credential by token URL and server ID; it never prints a secret.
func (credential *apiKeyCredential) String() string {
	return fmt.Sprintf("dex.APIKeyCredential{tokenURL: %s, serverID: %q}", credential.tokenURL.Redacted(), credential.serverID)
}

// GoString keeps %#v from printing the API key or an access token.
func (credential *apiKeyCredential) GoString() string {
	return credential.String()
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

func (exchangeError *apiKeyExchangeError) Error() string {
	var message strings.Builder
	fmt.Fprintf(&message, "dex: API key exchange with %s failed", exchangeError.tokenURL)
	if exchangeError.statusCode != 0 {
		fmt.Fprintf(&message, " with HTTP %d", exchangeError.statusCode)
	}
	if exchangeError.problem != "" {
		fmt.Fprintf(&message, ": %s", exchangeError.problem)
	}
	if exchangeError.errorCode != "" {
		fmt.Fprintf(&message, "; error %q", exchangeError.errorCode)
	}
	if exchangeError.errorDescription != "" {
		fmt.Fprintf(&message, "; error_description %q", exchangeError.errorDescription)
	}
	if exchangeError.cause != nil {
		fmt.Fprintf(&message, ": %v", exchangeError.cause)
	}
	return message.String()
}

func (exchangeError *apiKeyExchangeError) Unwrap() error {
	return exchangeError.cause
}
