// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package dex

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	testAPIKey      = "api-key-secret-value"
	testAPIServerID = "server-1"
)

// apiKeyTokenEndpoint is an in-process token endpoint that records every exchange request.
type apiKeyTokenEndpoint struct {
	server  *httptest.Server
	respond func(requestNumber int, writer http.ResponseWriter, request *http.Request)

	mu       sync.Mutex
	requests []recordedAPIKeyExchange
}

type recordedAPIKeyExchange struct {
	method      string
	path        string
	contentType string
	accept      string
	body        string
	receivedAt  time.Time
}

func startAPIKeyTokenEndpoint(
	t *testing.T,
	respond func(requestNumber int, writer http.ResponseWriter, request *http.Request),
) *apiKeyTokenEndpoint {
	t.Helper()
	endpoint := &apiKeyTokenEndpoint{respond: respond}
	endpoint.server = httptest.NewServer(http.HandlerFunc(endpoint.serveHTTP))
	t.Cleanup(endpoint.server.Close)
	return endpoint
}

func (endpoint *apiKeyTokenEndpoint) serveHTTP(writer http.ResponseWriter, request *http.Request) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		http.Error(writer, "read body", http.StatusBadRequest)
		return
	}
	endpoint.mu.Lock()
	endpoint.requests = append(endpoint.requests, recordedAPIKeyExchange{
		method:      request.Method,
		path:        request.URL.Path,
		contentType: request.Header.Get("Content-Type"),
		accept:      request.Header.Get("Accept"),
		body:        string(body),
		receivedAt:  time.Now(),
	})
	requestNumber := len(endpoint.requests)
	endpoint.mu.Unlock()
	endpoint.respond(requestNumber, writer, request)
}

func (endpoint *apiKeyTokenEndpoint) tokenURL() string {
	return endpoint.server.URL + "/token"
}

func (endpoint *apiKeyTokenEndpoint) exchangeCount() int {
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	return len(endpoint.requests)
}

func (endpoint *apiKeyTokenEndpoint) recordedExchanges() []recordedAPIKeyExchange {
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	return append([]recordedAPIKeyExchange(nil), endpoint.requests...)
}

func writeAPIKeyExchangeResponse(writer http.ResponseWriter, statusCode int, body string) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(statusCode)
	_, err := io.WriteString(writer, body)
	if err != nil {
		panic(fmt.Sprintf("write token endpoint response: %v", err))
	}
}

func writeAPIKeyAccessToken(writer http.ResponseWriter, accessToken string, expiresInSeconds int) {
	writeAPIKeyExchangeResponse(writer, http.StatusOK, fmt.Sprintf(
		`{"access_token":%q,"token_type":"Bearer","expires_in":%d}`, accessToken, expiresInSeconds,
	))
}

// apiKeyTestClock is a manually advanced clock that keeps the monotonic reading of its start.
type apiKeyTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func newAPIKeyTestClock() *apiKeyTestClock {
	return &apiKeyTestClock{now: time.Now()}
}

func (clock *apiKeyTestClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *apiKeyTestClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = clock.now.Add(duration)
}

func newTestAPIKeyCredential(
	t *testing.T,
	tokenURL string,
	clock *apiKeyTestClock,
	options ...APIKeyCredentialOption,
) *apiKeyCredential {
	t.Helper()
	credential, err := APIKeyCredential(tokenURL, testAPIServerID, testAPIKey, options...)
	require.NoError(t, err)
	apiKeyCredential, ok := credential.(*apiKeyCredential)
	require.True(t, ok)
	if clock != nil {
		apiKeyCredential.now = clock.Now
	}
	return apiKeyCredential
}

func waitForAPIKeyExchangeIdle(t *testing.T, credential *apiKeyCredential) {
	t.Helper()
	require.Eventually(t, func() bool {
		credential.mu.Lock()
		defer credential.mu.Unlock()
		return credential.inFlightExchange == nil
	}, 5*time.Second, time.Millisecond)
}

func TestAPIKeyCredentialSendsExchangeRequest(t *testing.T) {
	endpoint := startAPIKeyTokenEndpoint(t, func(_ int, writer http.ResponseWriter, _ *http.Request) {
		writeAPIKeyAccessToken(writer, "access-token-a", 3600)
	})
	credential := newTestAPIKeyCredential(t, endpoint.tokenURL(), nil)
	require.Zero(t, endpoint.exchangeCount(), "construction does not contact the token endpoint")

	token, err := credential.Token(context.Background())
	require.NoError(t, err)
	require.Equal(t, "access-token-a", token)
	token, err = credential.Token(context.Background())
	require.NoError(t, err)
	require.Equal(t, "access-token-a", token)

	exchanges := endpoint.recordedExchanges()
	require.Len(t, exchanges, 1, "a cached token needs no second exchange")
	require.Equal(t, http.MethodPost, exchanges[0].method)
	require.Equal(t, "/token", exchanges[0].path)
	require.Equal(t, "application/json", exchanges[0].contentType)
	require.Equal(t, "application/json", exchanges[0].accept)
	require.Equal(t, `{"server_id":"server-1","api_key":"api-key-secret-value"}`, exchanges[0].body)
}

func TestAPIKeyCredentialValidatesConstruction(t *testing.T) {
	for _, tokenURL := range []string{
		"https://auth.example.com/token",
		"HTTPS://auth.example.com/token",
		"http://localhost:8080/token",
		"http://LOCALHOST/token",
		"http://127.0.0.1/token",
		"http://127.1.2.3:9000/token",
		"http://[::1]:8080/token",
	} {
		_, err := APIKeyCredential(tokenURL, testAPIServerID, testAPIKey)
		require.NoError(t, err, tokenURL)
	}

	for _, invalid := range []struct {
		tokenURL string
		serverID string
		apiKey   string
		options  []APIKeyCredentialOption
		message  string
	}{
		{tokenURL: "", serverID: testAPIServerID, apiKey: testAPIKey, message: "token URL must not be empty"},
		{tokenURL: "https://auth.example.com/token", serverID: "", apiKey: testAPIKey, message: "server ID must not be empty"},
		{tokenURL: "https://auth.example.com/token", serverID: testAPIServerID, apiKey: "", message: "API key must not be empty"},
		{tokenURL: "https://auth example.com/token", serverID: testAPIServerID, apiKey: testAPIKey, message: "token URL is invalid"},
		{tokenURL: "/token", serverID: testAPIServerID, apiKey: testAPIKey, message: "absolute URL with a host"},
		{tokenURL: "auth.example.com/token", serverID: testAPIServerID, apiKey: testAPIKey, message: "absolute URL with a host"},
		{tokenURL: "ftp://auth.example.com/token", serverID: testAPIServerID, apiKey: testAPIKey, message: "must use https"},
		{tokenURL: "http://auth.example.com/token", serverID: testAPIServerID, apiKey: testAPIKey, message: "must use https"},
		{tokenURL: "http://10.0.0.1/token", serverID: testAPIServerID, apiKey: testAPIKey, message: "must use https"},
		{tokenURL: "http://localhost.example.com/token", serverID: testAPIServerID, apiKey: testAPIKey, message: "must use https"},
		{
			tokenURL: "https://auth.example.com/token", serverID: testAPIServerID, apiKey: testAPIKey,
			options: []APIKeyCredentialOption{WithAPIKeyExchangeTimeout(0)}, message: "exchange timeout must be positive",
		},
		{
			tokenURL: "https://auth.example.com/token", serverID: testAPIServerID, apiKey: testAPIKey,
			options: []APIKeyCredentialOption{WithAPIKeyExchangeTimeout(-time.Second)}, message: "exchange timeout must be positive",
		},
		{
			tokenURL: "https://auth.example.com/token", serverID: testAPIServerID, apiKey: testAPIKey,
			options: []APIKeyCredentialOption{WithAPIKeyHTTPClient(nil)}, message: "HTTP client must not be nil",
		},
		{
			tokenURL: "https://auth.example.com/token", serverID: testAPIServerID, apiKey: testAPIKey,
			options: []APIKeyCredentialOption{nil}, message: "option must not be nil",
		},
	} {
		credential, err := APIKeyCredential(invalid.tokenURL, invalid.serverID, invalid.apiKey, invalid.options...)
		require.Nil(t, credential, invalid.message)
		require.ErrorContains(t, err, invalid.message)
		require.NotContains(t, err.Error(), testAPIKey)
	}
}

func TestAPIKeyCredentialUsesInjectedHTTPClientWithoutFollowingRedirects(t *testing.T) {
	redirectTarget := startAPIKeyTokenEndpoint(t, func(_ int, writer http.ResponseWriter, _ *http.Request) {
		writeAPIKeyAccessToken(writer, "redirected-token", 3600)
	})
	endpoint := startAPIKeyTokenEndpoint(t, func(_ int, writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, redirectTarget.tokenURL(), http.StatusTemporaryRedirect)
	})
	transport := &countingRoundTripper{next: http.DefaultTransport}
	httpClient := &http.Client{Transport: transport}
	credential := newTestAPIKeyCredential(t, endpoint.tokenURL(), nil, WithAPIKeyHTTPClient(httpClient))

	_, err := credential.Token(context.Background())
	require.ErrorContains(t, err, "with HTTP 307")
	require.Zero(t, redirectTarget.exchangeCount(), "the API key is never sent to a redirect target")
	require.Equal(t, 1, transport.roundTrips())
	require.Nil(t, httpClient.CheckRedirect, "the caller's client is not modified")
}

type countingRoundTripper struct {
	next http.RoundTripper

	mu    sync.Mutex
	count int
}

func (roundTripper *countingRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	roundTripper.mu.Lock()
	roundTripper.count++
	roundTripper.mu.Unlock()
	return roundTripper.next.RoundTrip(request)
}

func (roundTripper *countingRoundTripper) roundTrips() int {
	roundTripper.mu.Lock()
	defer roundTripper.mu.Unlock()
	return roundTripper.count
}

func TestAPIKeyCredentialValidatesExchangeResponses(t *testing.T) {
	for _, accepted := range []struct {
		name string
		body string
	}{
		{name: "lowercase token type", body: `{"access_token":"token-ok","token_type":"bearer","expires_in":60}`},
		{name: "uppercase token type", body: `{"access_token":"token-ok","token_type":"BEARER","expires_in":60}`},
		{name: "unknown fields", body: `{"access_token":"token-ok","token_type":"Bearer","expires_in":60,"scope":"flows","extra":{"a":[1]}}`},
		{name: "huge lifetime", body: `{"access_token":"token-ok","token_type":"Bearer","expires_in":99999999999999999999}`},
	} {
		t.Run(accepted.name, func(t *testing.T) {
			endpoint := startAPIKeyTokenEndpoint(t, func(_ int, writer http.ResponseWriter, _ *http.Request) {
				writeAPIKeyExchangeResponse(writer, http.StatusOK, accepted.body)
			})
			token, err := newTestAPIKeyCredential(t, endpoint.tokenURL(), nil).Token(context.Background())
			require.NoError(t, err)
			require.Equal(t, "token-ok", token)
		})
	}

	for _, rejected := range []struct {
		name       string
		statusCode int
		body       string
		message    string
	}{
		{name: "server error without JSON", statusCode: http.StatusInternalServerError, body: "raw-body-marker", message: "with HTTP 500"},
		{name: "unauthorized", statusCode: http.StatusUnauthorized, body: `{"detail":"raw-body-marker"}`, message: "with HTTP 401"},
		{name: "created", statusCode: http.StatusCreated, body: `{"access_token":"raw-body-marker","token_type":"Bearer","expires_in":60}`, message: "with HTTP 201"},
		{name: "malformed JSON", statusCode: http.StatusOK, body: `{"access_token": raw-body-marker`, message: "with HTTP 200: response is not a JSON object"},
		{name: "array", statusCode: http.StatusOK, body: `["raw-body-marker"]`, message: "response is not a JSON object"},
		{name: "null", statusCode: http.StatusOK, body: `null`, message: "response is not a JSON object"},
		{name: "missing access token", statusCode: http.StatusOK, body: `{"token_type":"Bearer","expires_in":60,"x":"raw-body-marker"}`, message: "no non-empty access_token string"},
		{name: "empty access token", statusCode: http.StatusOK, body: `{"access_token":"","token_type":"Bearer","expires_in":60}`, message: "no non-empty access_token string"},
		{name: "numeric access token", statusCode: http.StatusOK, body: `{"access_token":42,"token_type":"Bearer","expires_in":60}`, message: "no non-empty access_token string"},
		{name: "null access token", statusCode: http.StatusOK, body: `{"access_token":null,"token_type":"Bearer","expires_in":60}`, message: "no non-empty access_token string"},
		{name: "missing token type", statusCode: http.StatusOK, body: `{"access_token":"raw-body-marker","expires_in":60}`, message: "token_type is not Bearer"},
		{name: "other token type", statusCode: http.StatusOK, body: `{"access_token":"raw-body-marker","token_type":"mac","expires_in":60}`, message: "token_type is not Bearer"},
		{name: "missing lifetime", statusCode: http.StatusOK, body: `{"access_token":"raw-body-marker","token_type":"Bearer"}`, message: "expires_in is not a positive integer"},
		{name: "zero lifetime", statusCode: http.StatusOK, body: `{"access_token":"raw-body-marker","token_type":"Bearer","expires_in":0}`, message: "expires_in is not a positive integer"},
		{name: "negative lifetime", statusCode: http.StatusOK, body: `{"access_token":"raw-body-marker","token_type":"Bearer","expires_in":-5}`, message: "expires_in is not a positive integer"},
		{name: "string lifetime", statusCode: http.StatusOK, body: `{"access_token":"raw-body-marker","token_type":"Bearer","expires_in":"60"}`, message: "expires_in is not a positive integer"},
		{name: "fractional lifetime", statusCode: http.StatusOK, body: `{"access_token":"raw-body-marker","token_type":"Bearer","expires_in":1.5}`, message: "expires_in is not a positive integer"},
		{name: "exponent lifetime", statusCode: http.StatusOK, body: `{"access_token":"raw-body-marker","token_type":"Bearer","expires_in":6e1}`, message: "expires_in is not a positive integer"},
		{name: "oversized body", statusCode: http.StatusOK, body: `{"x":"` + strings.Repeat("a", maxAPIKeyExchangeResponseBytes) + `"}`, message: "response body exceeds 1 MiB"},
	} {
		t.Run(rejected.name, func(t *testing.T) {
			endpoint := startAPIKeyTokenEndpoint(t, func(_ int, writer http.ResponseWriter, _ *http.Request) {
				writeAPIKeyExchangeResponse(writer, rejected.statusCode, rejected.body)
			})
			_, err := newTestAPIKeyCredential(t, endpoint.tokenURL(), nil).Token(context.Background())
			require.ErrorContains(t, err, "dex: API key exchange with "+endpoint.tokenURL()+" failed")
			require.ErrorContains(t, err, rejected.message)
			require.NotContains(t, err.Error(), "raw-body-marker", "errors never include the raw response body")
		})
	}
}

func TestAPIKeyCredentialReportsTokenEndpointErrorFields(t *testing.T) {
	longDescription := strings.Repeat("é", maxAPIKeyExchangeErrorRunes+50)
	endpoint := startAPIKeyTokenEndpoint(t, func(requestNumber int, writer http.ResponseWriter, _ *http.Request) {
		switch requestNumber {
		case 1:
			writeAPIKeyExchangeResponse(writer, http.StatusUnauthorized,
				`{"error":"invalid_client","error_description":"API key revoked","detail":"raw-body-marker"}`)
		case 2:
			writeAPIKeyExchangeResponse(writer, http.StatusBadRequest,
				fmt.Sprintf(`{"error":"invalid_request","error_description":%q}`, longDescription))
		default:
			writeAPIKeyExchangeResponse(writer, http.StatusForbidden, `{"error":42,"error_description":{"text":"raw-body-marker"}}`)
		}
	})
	credential := newTestAPIKeyCredential(t, endpoint.tokenURL(), nil)

	_, err := credential.Token(context.Background())
	require.EqualError(t, err, "dex: API key exchange with "+endpoint.tokenURL()+
		` failed with HTTP 401; error "invalid_client"; error_description "API key revoked"`)

	_, err = credential.Token(context.Background())
	require.ErrorContains(t, err, `with HTTP 400; error "invalid_request"; error_description "`+
		strings.Repeat("é", maxAPIKeyExchangeErrorRunes)+`"`)
	require.NotContains(t, err.Error(), strings.Repeat("é", maxAPIKeyExchangeErrorRunes+1))

	_, err = credential.Token(context.Background())
	require.EqualError(t, err, "dex: API key exchange with "+endpoint.tokenURL()+" failed with HTTP 403",
		"non-string error fields are omitted")
}

func TestAPIKeyCredentialReportsTransportFailuresAndTimeouts(t *testing.T) {
	closedEndpoint := startAPIKeyTokenEndpoint(t, func(_ int, writer http.ResponseWriter, _ *http.Request) {
		writeAPIKeyAccessToken(writer, "unreachable", 60)
	})
	closedTokenURL := closedEndpoint.tokenURL()
	closedEndpoint.server.Close()
	_, err := newTestAPIKeyCredential(t, closedTokenURL, nil).Token(context.Background())
	require.ErrorContains(t, err, "dex: API key exchange with "+closedTokenURL+" failed: request failed")
	require.NotContains(t, err.Error(), "with HTTP")

	slowEndpoint := startAPIKeyTokenEndpoint(t, func(_ int, writer http.ResponseWriter, request *http.Request) {
		select {
		case <-request.Context().Done():
		case <-time.After(10 * time.Second):
		}
		writeAPIKeyAccessToken(writer, "too-late", 60)
	})
	credential := newTestAPIKeyCredential(t, slowEndpoint.tokenURL(), nil, WithAPIKeyExchangeTimeout(100*time.Millisecond))
	startedAt := time.Now()
	_, err = credential.Token(context.Background())
	require.Less(t, time.Since(startedAt), 5*time.Second)
	require.ErrorContains(t, err, "dex: API key exchange with "+slowEndpoint.tokenURL()+" failed: timed out after 100ms")
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestAPIKeyCredentialNeverFormatsSecrets(t *testing.T) {
	const accessToken = "access-token-secret-value"
	endpoint := startAPIKeyTokenEndpoint(t, func(requestNumber int, writer http.ResponseWriter, request *http.Request) {
		switch requestNumber {
		case 1:
			writeAPIKeyAccessToken(writer, accessToken, 3600)
		case 2:
			writeAPIKeyExchangeResponse(writer, http.StatusUnauthorized,
				`{"error":"invalid_client","error_description":"bad key","access_token":"`+accessToken+`"}`)
		case 3:
			writeAPIKeyExchangeResponse(writer, http.StatusOK,
				`{"access_token":"`+accessToken+`","token_type":"mac","expires_in":60}`)
		case 4:
			writeAPIKeyExchangeResponse(writer, http.StatusOK, `{"access_token":"`+accessToken+`"`)
		default:
			<-request.Context().Done()
		}
	})
	credential := newTestAPIKeyCredential(t, endpoint.tokenURL(), nil, WithAPIKeyExchangeTimeout(100*time.Millisecond))
	formatted := []string{}
	formatValue := func(value any) {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
			formatted = append(formatted, fmt.Sprintf(verb, value))
		}
	}
	formatValue(credential)
	require.Contains(t, formatted[0], endpoint.tokenURL())
	require.Contains(t, formatted[0], testAPIServerID)

	token, err := credential.Token(context.Background())
	require.NoError(t, err)
	require.Equal(t, accessToken, token)
	formatValue(credential)
	var asCredential Credential = credential
	formatValue(asCredential)

	exchangeErrors := []error{}
	for range 4 {
		credential.discardRejectedToken(accessToken)
		_, err := credential.Token(context.Background())
		require.Error(t, err)
		exchangeErrors = append(exchangeErrors, err)
	}
	closedEndpoint := startAPIKeyTokenEndpoint(t, func(int, http.ResponseWriter, *http.Request) {})
	closedTokenURL := closedEndpoint.tokenURL()
	closedEndpoint.server.Close()
	_, err = newTestAPIKeyCredential(t, closedTokenURL, nil).Token(context.Background())
	require.Error(t, err)
	exchangeErrors = append(exchangeErrors, err)
	_, err = APIKeyCredential("http://example.com/token", testAPIServerID, testAPIKey)
	require.Error(t, err)
	exchangeErrors = append(exchangeErrors, err)
	_, err = APIKeyCredential("https://example.com/token", testAPIServerID, testAPIKey, WithAPIKeyHTTPClient(nil))
	require.Error(t, err)
	exchangeErrors = append(exchangeErrors, err)

	require.ErrorContains(t, exchangeErrors[0], "with HTTP 401")
	require.ErrorContains(t, exchangeErrors[1], "token_type is not Bearer")
	require.ErrorContains(t, exchangeErrors[2], "response is not a JSON object")
	require.ErrorContains(t, exchangeErrors[3], "timed out")
	for _, exchangeError := range exchangeErrors {
		formatValue(exchangeError)
	}
	for _, text := range formatted {
		require.NotContains(t, text, testAPIKey)
		require.NotContains(t, text, accessToken)
	}
}

func TestAPIKeyCredentialRefreshesAfter80PercentOfLifetime(t *testing.T) {
	refreshStarted := make(chan struct{})
	endpoint := startAPIKeyTokenEndpoint(t, func(requestNumber int, writer http.ResponseWriter, _ *http.Request) {
		if requestNumber == 1 {
			writeAPIKeyAccessToken(writer, "access-token-a", 2)
			return
		}
		close(refreshStarted)
		time.Sleep(300 * time.Millisecond)
		writeAPIKeyAccessToken(writer, "access-token-b", 2)
	})
	credential := newTestAPIKeyCredential(t, endpoint.tokenURL(), nil)
	firstCallAt := time.Now()
	token, err := credential.Token(context.Background())
	require.NoError(t, err)
	require.Equal(t, "access-token-a", token)

	var observed []string
	var slowestCall time.Duration
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for len(observed) == 0 || observed[len(observed)-1] != "access-token-b" {
		require.Less(t, time.Since(firstCallAt), 5*time.Second, "the refreshed token never arrived")
		<-poll.C
		callStartedAt := time.Now()
		token, err := credential.Token(context.Background())
		slowestCall = max(slowestCall, time.Since(callStartedAt))
		require.NoError(t, err)
		observed = append(observed, token)
	}

	<-refreshStarted
	exchanges := endpoint.recordedExchanges()
	require.Len(t, exchanges, 2)
	refreshDelay := exchanges[1].receivedAt.Sub(firstCallAt)
	require.GreaterOrEqual(t, refreshDelay, 1600*time.Millisecond, "refresh starts after 80%% of the lifetime")
	require.Less(t, refreshDelay, 2*time.Second, "refresh starts before the token expires")
	require.Less(t, slowestCall, 200*time.Millisecond, "a refresh never blocks callers holding a valid token")
	firstRefreshed := len(observed) - 1
	for _, token := range observed[:firstRefreshed] {
		require.Equal(t, "access-token-a", token)
	}
}

func TestAPIKeyCredentialSharesOneExchangeAcrossConcurrentCallers(t *testing.T) {
	releaseExchange := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	endpoint := startAPIKeyTokenEndpoint(t, func(requestNumber int, writer http.ResponseWriter, _ *http.Request) {
		<-releaseExchange[requestNumber-1]
		writeAPIKeyAccessToken(writer, fmt.Sprintf("access-token-%d", requestNumber), 100)
	})
	clock := newAPIKeyTestClock()
	credential := newTestAPIKeyCredential(t, endpoint.tokenURL(), clock)
	callConcurrently := func(callers int) []string {
		tokens := make([]string, callers)
		tokenErrors := make([]error, callers)
		var callersDone sync.WaitGroup
		for caller := range callers {
			callersDone.Add(1)
			go func() {
				defer callersDone.Done()
				tokens[caller], tokenErrors[caller] = credential.Token(context.Background())
			}()
		}
		require.Eventually(t, func() bool { return endpoint.exchangeCount() > 0 }, 5*time.Second, time.Millisecond)
		close(releaseExchange[endpoint.exchangeCount()-1])
		callersDone.Wait()
		for _, err := range tokenErrors {
			require.NoError(t, err)
		}
		return tokens
	}

	for _, token := range callConcurrently(32) {
		require.Equal(t, "access-token-1", token)
	}
	require.Equal(t, 1, endpoint.exchangeCount(), "the first exchange is single-flight")

	clock.Advance(85 * time.Second)
	refreshTokens := make([]string, 32)
	refreshErrors := make([]error, 32)
	var refreshCallers sync.WaitGroup
	for caller := range refreshTokens {
		refreshCallers.Add(1)
		go func() {
			defer refreshCallers.Done()
			refreshTokens[caller], refreshErrors[caller] = credential.Token(context.Background())
		}()
	}
	refreshCallers.Wait()
	for caller := range refreshTokens {
		require.NoError(t, refreshErrors[caller])
		require.Equal(t, "access-token-1", refreshTokens[caller], "callers keep the valid token while the refresh is blocked")
	}
	require.Eventually(t, func() bool { return endpoint.exchangeCount() == 2 }, 5*time.Second, time.Millisecond)
	close(releaseExchange[1])
	waitForAPIKeyExchangeIdle(t, credential)
	require.Equal(t, 2, endpoint.exchangeCount(), "a refresh is single-flight")
	token, err := credential.Token(context.Background())
	require.NoError(t, err)
	require.Equal(t, "access-token-2", token)

	clock.Advance(101 * time.Second)
	canceledCtx, cancel := context.WithCancel(context.Background())
	canceledResult := make(chan error, 1)
	go func() {
		_, err := credential.Token(canceledCtx)
		canceledResult <- err
	}()
	require.Eventually(t, func() bool { return endpoint.exchangeCount() == 3 }, 5*time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-canceledResult, context.Canceled, "a waiting caller honors its own context")
	for _, token := range callConcurrently(32) {
		require.Equal(t, "access-token-3", token, "a canceled caller does not cancel the shared exchange")
	}
	require.Equal(t, 3, endpoint.exchangeCount())
}

func TestAPIKeyCredentialServesCachedTokenWhileRefreshFails(t *testing.T) {
	endpoint := startAPIKeyTokenEndpoint(t, func(requestNumber int, writer http.ResponseWriter, _ *http.Request) {
		if requestNumber == 1 {
			writeAPIKeyAccessToken(writer, "access-token-a", 100)
			return
		}
		writeAPIKeyExchangeResponse(writer, http.StatusServiceUnavailable, `{"error":"temporarily_unavailable"}`)
	})
	clock := newAPIKeyTestClock()
	credential := newTestAPIKeyCredential(t, endpoint.tokenURL(), clock)
	requireCachedToken := func() {
		t.Helper()
		token, err := credential.Token(context.Background())
		require.NoError(t, err)
		require.Equal(t, "access-token-a", token)
		waitForAPIKeyExchangeIdle(t, credential)
	}
	requireCachedToken()
	require.Equal(t, 1, endpoint.exchangeCount())

	clock.Advance(79 * time.Second)
	requireCachedToken()
	require.Equal(t, 1, endpoint.exchangeCount(), "no refresh before 80% of the lifetime")

	clock.Advance(time.Second)
	requireCachedToken()
	require.Equal(t, 2, endpoint.exchangeCount(), "the first refresh fails")
	requireCachedToken()
	clock.Advance(4 * time.Second)
	requireCachedToken()
	require.Equal(t, 2, endpoint.exchangeCount(), "a failed refresh is retried at most once every 5 seconds")

	clock.Advance(time.Second)
	requireCachedToken()
	require.Equal(t, 3, endpoint.exchangeCount(), "the refresh is retried after 5 seconds")

	clock.Advance(15 * time.Second)
	_, err := credential.Token(context.Background())
	require.EqualError(t, err, "dex: API key exchange with "+endpoint.tokenURL()+
		` failed with HTTP 503; error "temporarily_unavailable"`)
	require.Equal(t, 4, endpoint.exchangeCount(), "an expired token waits for a fresh exchange")
}

func TestAPIKeyCredentialFailsAfterExpiryWithinExchangeTimeout(t *testing.T) {
	endpoint := startAPIKeyTokenEndpoint(t, func(requestNumber int, writer http.ResponseWriter, request *http.Request) {
		if requestNumber == 1 {
			writeAPIKeyAccessToken(writer, "access-token-a", 10)
			return
		}
		<-request.Context().Done()
	})
	clock := newAPIKeyTestClock()
	credential := newTestAPIKeyCredential(t, endpoint.tokenURL(), clock, WithAPIKeyExchangeTimeout(200*time.Millisecond))
	token, err := credential.Token(context.Background())
	require.NoError(t, err)
	require.Equal(t, "access-token-a", token)

	clock.Advance(10 * time.Second)
	startedAt := time.Now()
	_, err = credential.Token(context.Background())
	require.Less(t, time.Since(startedAt), 5*time.Second)
	require.ErrorContains(t, err, "timed out after 200ms")
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestAPIKeyCredentialDiscardsOnlyTheCurrentRejectedToken(t *testing.T) {
	endpoint := startAPIKeyTokenEndpoint(t, func(requestNumber int, writer http.ResponseWriter, _ *http.Request) {
		writeAPIKeyAccessToken(writer, fmt.Sprintf("access-token-%d", requestNumber), 3600)
	})
	credential := newTestAPIKeyCredential(t, endpoint.tokenURL(), nil)
	requireToken := func(expected string) {
		t.Helper()
		token, err := credential.Token(context.Background())
		require.NoError(t, err)
		require.Equal(t, expected, token)
	}
	requireToken("access-token-1")

	credential.discardRejectedToken("access-token-other")
	credential.discardRejectedToken("")
	requireToken("access-token-1")
	require.Equal(t, 1, endpoint.exchangeCount())

	credential.discardRejectedToken("access-token-1")
	requireToken("access-token-2")
	require.Equal(t, 2, endpoint.exchangeCount())

	credential.discardRejectedToken("access-token-1")
	requireToken("access-token-2")
	require.Equal(t, 2, endpoint.exchangeCount(), "a rejected older token never discards a newer one")
}

func TestAPIKeyCredentialOptionsApplyInOrder(t *testing.T) {
	credential := newTestAPIKeyCredential(t, "https://auth.example.com/token", nil,
		WithAPIKeyExchangeTimeout(time.Second), WithAPIKeyExchangeTimeout(3*time.Second))
	require.Equal(t, 3*time.Second, credential.exchangeTimeout)
	defaultCredential := newTestAPIKeyCredential(t, "https://auth.example.com/token", nil)
	require.Equal(t, 10*time.Second, defaultCredential.exchangeTimeout)
	require.True(t, errors.Is(http.ErrUseLastResponse, defaultCredential.httpClient.CheckRedirect(nil, nil)))
}
