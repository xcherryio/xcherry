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
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/gen/dexpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

type credentialTestService struct {
	dexpb.UnimplementedFlowServiceServer
	mu      sync.Mutex
	headers []string
}

func (*credentialTestService) HealthCheck(context.Context, *emptypb.Empty) (*dexpb.HealthInfo, error) {
	return &dexpb.HealthInfo{}, nil
}

func (s *credentialTestService) recordHeader(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	requestMetadata, _ := metadata.FromIncomingContext(ctx)
	s.mu.Lock()
	s.headers = append(s.headers, requestMetadata.Get("authorization")...)
	s.mu.Unlock()
	return handler(ctx, request)
}

func (s *credentialTestService) recordedHeaders() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.headers...)
}

func startCredentialTestService(t *testing.T) (*credentialTestService, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	service := &credentialTestService{}
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(service.recordHeader))
	dexpb.RegisterFlowServiceServer(grpcServer, service)
	serveFinished := make(chan error, 1)
	go func() { serveFinished <- grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.GracefulStop()
		serveErr := <-serveFinished
		if serveErr != nil {
			require.ErrorIs(t, serveErr, grpc.ErrServerStopped)
		}
	})
	return service, listener.Addr().String()
}

func TestCredentialPrecedenceAndHeader(t *testing.T) {
	service, address := startCredentialTestService(t)
	fileFromEnvironment := filepath.Join(t.TempDir(), "env-token")
	fileFromFlag := filepath.Join(t.TempDir(), "flag-token")
	require.NoError(t, os.WriteFile(fileFromEnvironment, []byte(" environment \n"), 0o600))
	require.NoError(t, os.WriteFile(fileFromFlag, []byte(" flag \n"), 0o600))
	t.Setenv("DEX_CREDENTIAL", "raw-token")
	t.Setenv("DEX_CREDENTIAL_FILE", fileFromEnvironment)

	for _, arguments := range [][]string{
		{"health", "--server", address, "--credential-file", fileFromFlag},
		{"health", "--server", address},
	} {
		app := NewApp(bytes.NewReader(nil), &bytes.Buffer{}, &bytes.Buffer{})
		require.NoError(t, app.Execute(context.Background(), arguments))
	}
	require.NoError(t, os.Unsetenv("DEX_CREDENTIAL_FILE"))
	app := NewApp(bytes.NewReader(nil), &bytes.Buffer{}, &bytes.Buffer{})
	require.NoError(t, app.Execute(context.Background(), []string{"health", "--server", address}))
	require.Equal(t, []string{"Bearer flag", "Bearer environment", "Bearer raw-token"}, service.recordedHeaders())
}

func TestCredentialFileRotationAndEmptyFileStopBeforeRPC(t *testing.T) {
	service, address := startCredentialTestService(t)
	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte("first"), 0o600))
	client, err := newFlowService(options{server: address, credentialFile: path})
	require.NoError(t, err)
	defer func() { require.NoError(t, client.connection.Close()) }()
	_, err = client.service.HealthCheck(context.Background(), emptyRequest())
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, []byte("second-value"), 0o600))
	require.Eventually(t, func() bool {
		_, callErr := client.service.HealthCheck(context.Background(), emptyRequest())
		return callErr == nil && len(service.recordedHeaders()) > 1 && service.recordedHeaders()[len(service.recordedHeaders())-1] == "Bearer second-value"
	}, 2*time.Second, 20*time.Millisecond)
	require.NoError(t, os.WriteFile(path, []byte(" \n"), 0o600))
	require.Eventually(t, func() bool {
		_, callErr := client.service.HealthCheck(context.Background(), emptyRequest())
		return callErr != nil
	}, 2*time.Second, 20*time.Millisecond)
	beforeEmpty := len(service.recordedHeaders())
	_, err = client.service.HealthCheck(context.Background(), emptyRequest())
	require.Error(t, err)
	_, err = newFlowService(options{server: address, credentialFile: path})
	require.ErrorContains(t, err, "is empty")
	require.Len(t, service.recordedHeaders(), beforeEmpty)
}

func TestEmptyEnvironmentCredentialsAreUnset(t *testing.T) {
	service, address := startCredentialTestService(t)
	t.Setenv("DEX_CREDENTIAL", "")
	app := NewApp(bytes.NewReader(nil), &bytes.Buffer{}, &bytes.Buffer{})
	require.NoError(t, app.Execute(context.Background(), []string{"health", "--server", address}))
	require.Empty(t, service.recordedHeaders())

	t.Setenv("DEX_CREDENTIAL_FILE", "")
	app = NewApp(bytes.NewReader(nil), &bytes.Buffer{}, &bytes.Buffer{})
	require.NoError(t, app.Execute(context.Background(), []string{"health", "--server", address}))
	require.Empty(t, service.recordedHeaders())
	t.Setenv("DEX_CREDENTIAL", "raw-token")
	app = NewApp(bytes.NewReader(nil), &bytes.Buffer{}, &bytes.Buffer{})
	require.NoError(t, app.Execute(context.Background(), []string{"health", "--server", address}))
	require.Equal(t, []string{"Bearer raw-token"}, service.recordedHeaders())

	app = NewApp(bytes.NewReader(nil), &bytes.Buffer{}, &bytes.Buffer{})
	err := app.Execute(context.Background(), []string{"health", "--server", address, "--credential-file", ""})
	require.ErrorContains(t, err, "bearer credential file path is empty")
	require.Equal(t, []string{"Bearer raw-token"}, service.recordedHeaders())
}

func TestCredentialFileFlagHelpHasNoNilPanic(t *testing.T) {
	var output bytes.Buffer
	flags := newFlagSet("summary", &output)
	addCommonFlags(flags, &options{})
	flags.PrintDefaults()
	require.Contains(t, output.String(), "-credential-file")
	require.Contains(t, output.String(), "-api-key-file")
	require.Contains(t, output.String(), "-token-url")
	require.Contains(t, output.String(), "-server-id")
	require.NotContains(t, output.String(), "panic")
}

const (
	cliTestAPIKey   = "cli-api-key-secret"
	cliTestServerID = "server-1"
)

// cliTokenEndpoint is an in-process API key token endpoint on loopback that records each exchange body.
type cliTokenEndpoint struct {
	server  *httptest.Server
	respond func(requestNumber int, writer http.ResponseWriter)

	mu     sync.Mutex
	bodies []string
}

func startCLITokenEndpoint(t *testing.T, respond func(requestNumber int, writer http.ResponseWriter)) *cliTokenEndpoint {
	t.Helper()
	endpoint := &cliTokenEndpoint{respond: respond}
	endpoint.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil || request.Method != http.MethodPost || request.Header.Get("Content-Type") != "application/json" ||
			request.Header.Get("Accept") != "application/json" {
			http.Error(writer, "unexpected exchange request", http.StatusBadRequest)
			return
		}
		endpoint.mu.Lock()
		endpoint.bodies = append(endpoint.bodies, string(body))
		requestNumber := len(endpoint.bodies)
		endpoint.mu.Unlock()
		endpoint.respond(requestNumber, writer)
	}))
	t.Cleanup(endpoint.server.Close)
	return endpoint
}

func (endpoint *cliTokenEndpoint) tokenURL() string {
	return endpoint.server.URL + "/token"
}

func (endpoint *cliTokenEndpoint) exchangeBodies() []string {
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	return append([]string(nil), endpoint.bodies...)
}

func writeCLITokenResponse(writer http.ResponseWriter, statusCode int, body string) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(statusCode)
	if _, err := io.WriteString(writer, body); err != nil {
		panic(fmt.Sprintf("write token endpoint response: %v", err))
	}
}

func writeCLIAccessToken(writer http.ResponseWriter, accessToken string, expiresInSeconds int) {
	writeCLITokenResponse(writer, http.StatusOK, fmt.Sprintf(
		`{"access_token":%q,"token_type":"Bearer","expires_in":%d}`, accessToken, expiresInSeconds,
	))
}

func executeCLI(arguments ...string) error {
	return NewApp(bytes.NewReader(nil), &bytes.Buffer{}, &bytes.Buffer{}).Execute(context.Background(), arguments)
}

func clearCLICredentialEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range []string{"DEX_CREDENTIAL", "DEX_CREDENTIAL_FILE", "DEX_API_KEY", "DEX_TOKEN_URL", "DEX_SERVER_ID"} {
		t.Setenv(name, "")
	}
}

func TestAPIKeyFileExchangesAccessTokenForEveryCall(t *testing.T) {
	clearCLICredentialEnvironment(t)
	service, address := startCredentialTestService(t)
	endpoint := startCLITokenEndpoint(t, func(requestNumber int, writer http.ResponseWriter) {
		writeCLIAccessToken(writer, fmt.Sprintf("cli-access-token-%d", requestNumber), 3600)
	})
	apiKeyPath := filepath.Join(t.TempDir(), "api-key")
	require.NoError(t, os.WriteFile(apiKeyPath, []byte(" "+cliTestAPIKey+" \n"), 0o600))

	require.NoError(t, executeCLI("health", "--server", address, "--api-key-file", apiKeyPath,
		"--token-url", endpoint.tokenURL(), "--server-id", cliTestServerID))
	require.NoError(t, executeCLI("--api-key-file", apiKeyPath, "--token-url", endpoint.tokenURL(),
		"api", "call", "HealthCheck", "--server", address, "--server-id", cliTestServerID))
	require.Equal(t, []string{"Bearer cli-access-token-1", "Bearer cli-access-token-2"}, service.recordedHeaders())
	require.Equal(t, []string{
		`{"server_id":"server-1","api_key":"cli-api-key-secret"}`,
		`{"server_id":"server-1","api_key":"cli-api-key-secret"}`,
	}, endpoint.exchangeBodies())
}

func TestAPIKeyEnvironmentAndFilePrecedence(t *testing.T) {
	clearCLICredentialEnvironment(t)
	service, address := startCredentialTestService(t)
	endpoint := startCLITokenEndpoint(t, func(requestNumber int, writer http.ResponseWriter) {
		writeCLIAccessToken(writer, fmt.Sprintf("cli-access-token-%d", requestNumber), 3600)
	})
	t.Setenv("DEX_API_KEY", " environment-api-key ")
	t.Setenv("DEX_TOKEN_URL", endpoint.tokenURL())
	t.Setenv("DEX_SERVER_ID", "environment-server")
	require.NoError(t, executeCLI("api", "call", "HealthCheck", "--server", address))

	apiKeyPath := filepath.Join(t.TempDir(), "api-key")
	require.NoError(t, os.WriteFile(apiKeyPath, []byte("file-api-key"), 0o600))
	require.NoError(t, executeCLI("health", "--server", address, "--api-key-file", apiKeyPath, "--server-id", "flag-server"))
	require.Equal(t, []string{
		`{"server_id":"environment-server","api_key":"environment-api-key"}`,
		`{"server_id":"flag-server","api_key":"file-api-key"}`,
	}, endpoint.exchangeBodies(), "--api-key-file overrides DEX_API_KEY and flags override environment settings")
	require.Equal(t, []string{"Bearer cli-access-token-1", "Bearer cli-access-token-2"}, service.recordedHeaders())
}

func TestAPIKeySettingsRejectInvalidCombinations(t *testing.T) {
	service, address := startCredentialTestService(t)
	endpoint := startCLITokenEndpoint(t, func(_ int, writer http.ResponseWriter) {
		writeCLIAccessToken(writer, "unused-token", 3600)
	})
	apiKeyPath := filepath.Join(t.TempDir(), "api-key")
	require.NoError(t, os.WriteFile(apiKeyPath, []byte(cliTestAPIKey), 0o600))
	tokenPath := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenPath, []byte("bearer-token"), 0o600))
	apiKeyFlags := []string{"--api-key-file", apiKeyPath, "--token-url", endpoint.tokenURL(), "--server-id", cliTestServerID}

	for _, invalid := range []struct {
		name        string
		environment map[string]string
		arguments   []string
		message     string
	}{
		{
			name:      "API key file with credential file",
			arguments: append([]string{"health", "--server", address, "--credential-file", tokenPath}, apiKeyFlags...),
			message:   "--api-key-file and DEX_API_KEY cannot be combined with --credential-file, DEX_CREDENTIAL_FILE, or DEX_CREDENTIAL",
		},
		{
			name:        "API key file with credential environment through api",
			environment: map[string]string{"DEX_CREDENTIAL": "raw-token"},
			arguments:   append([]string{"api", "call", "HealthCheck", "--server", address}, apiKeyFlags...),
			message:     "cannot be combined with --credential-file, DEX_CREDENTIAL_FILE, or DEX_CREDENTIAL",
		},
		{
			name:        "API key environment with credential file environment through flow",
			environment: map[string]string{"DEX_API_KEY": cliTestAPIKey, "DEX_CREDENTIAL_FILE": tokenPath, "DEX_TOKEN_URL": endpoint.tokenURL(), "DEX_SERVER_ID": cliTestServerID},
			arguments:   []string{"flow", "summary", "order-1", "--server", address},
			message:     "cannot be combined with --credential-file, DEX_CREDENTIAL_FILE, or DEX_CREDENTIAL",
		},
		{
			name:      "missing token URL",
			arguments: []string{"health", "--server", address, "--api-key-file", apiKeyPath, "--server-id", cliTestServerID},
			message:   "an API key requires --token-url or DEX_TOKEN_URL",
		},
		{
			name:      "missing server ID",
			arguments: []string{"health", "--server", address, "--api-key-file", apiKeyPath, "--token-url", endpoint.tokenURL()},
			message:   "an API key requires --server-id or DEX_SERVER_ID",
		},
		{
			name:        "token URL without API key",
			environment: map[string]string{"DEX_TOKEN_URL": endpoint.tokenURL()},
			arguments:   []string{"health", "--server", address},
			message:     "--token-url and --server-id (DEX_TOKEN_URL, DEX_SERVER_ID) require an API key",
		},
		{
			name:      "server ID without API key",
			arguments: []string{"health", "--server", address, "--server-id", cliTestServerID},
			message:   "require an API key from --api-key-file or DEX_API_KEY",
		},
		{
			name: "plain HTTP token URL on a remote host",
			arguments: []string{"health", "--server", address, "--api-key-file", apiKeyPath,
				"--token-url", "http://auth.example.com/token", "--server-id", cliTestServerID},
			message: "must use https; http is allowed only for localhost, 127.0.0.0/8, and ::1",
		},
	} {
		t.Run(invalid.name, func(t *testing.T) {
			clearCLICredentialEnvironment(t)
			for name, value := range invalid.environment {
				t.Setenv(name, value)
			}
			err := executeCLI(invalid.arguments...)
			require.ErrorContains(t, err, invalid.message)
			require.Equal(t, 2, ExitCode(err), "credential misconfiguration is a usage error")
			require.NotContains(t, err.Error(), cliTestAPIKey)
		})
	}
	require.Empty(t, service.recordedHeaders(), "invalid settings fail before any FlowService call")
	require.Empty(t, endpoint.exchangeBodies(), "invalid settings fail before any token exchange")
}

func TestAPIKeyExchangeFailureStopsBeforeRPCWithoutSecrets(t *testing.T) {
	clearCLICredentialEnvironment(t)
	service, address := startCredentialTestService(t)
	endpoint := startCLITokenEndpoint(t, func(_ int, writer http.ResponseWriter) {
		writeCLITokenResponse(writer, http.StatusUnauthorized,
			`{"error":"invalid_client","error_description":"API key revoked","detail":"raw-body-marker"}`)
	})
	t.Setenv("DEX_API_KEY", cliTestAPIKey)
	err := executeCLI("health", "--server", address, "--token-url", endpoint.tokenURL(), "--server-id", cliTestServerID)
	require.ErrorContains(t, err, "API key exchange with "+endpoint.tokenURL()+
		` failed with HTTP 401; error "invalid_client"; error_description "API key revoked"`)
	require.NotContains(t, err.Error(), cliTestAPIKey)
	require.NotContains(t, err.Error(), "raw-body-marker")
	require.Empty(t, service.recordedHeaders())

	apiKeyPath := filepath.Join(t.TempDir(), "api-key")
	require.NoError(t, os.WriteFile(apiKeyPath, []byte(" \n"), 0o600))
	err = executeCLI("health", "--server", address, "--api-key-file", apiKeyPath,
		"--token-url", endpoint.tokenURL(), "--server-id", cliTestServerID)
	require.ErrorContains(t, err, "API key file "+apiKeyPath+" is empty")
	err = executeCLI("health", "--server", address, "--api-key-file", "",
		"--token-url", endpoint.tokenURL(), "--server-id", cliTestServerID)
	require.ErrorContains(t, err, "API key file path is empty")
	require.Len(t, endpoint.exchangeBodies(), 1)
}

func TestAPIKeyCredentialCachesRefreshesAndHidesSecrets(t *testing.T) {
	service, address := startCredentialTestService(t)
	endpoint := startCLITokenEndpoint(t, func(requestNumber int, writer http.ResponseWriter) {
		switch requestNumber {
		case 1:
			writeCLIAccessToken(writer, "cli-access-token-1", 2)
		case 2:
			writeCLITokenResponse(writer, http.StatusOK, `{"access_token":"cli-access-token-2","token_type":"mac","expires_in":60}`)
		default:
			writeCLIAccessToken(writer, fmt.Sprintf("cli-access-token-%d", requestNumber), 2)
		}
	})
	perRPCCredential, err := newAPIKeyCredential(endpoint.tokenURL(), cliTestServerID, cliTestAPIKey)
	require.NoError(t, err)
	client, err := newFlowService(options{
		server: address, apiKey: cliTestAPIKey, hasAPIKey: true, tokenURL: endpoint.tokenURL(), serverID: cliTestServerID,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, client.connection.Close()) }()

	firstCallAt := time.Now()
	_, err = client.service.HealthCheck(context.Background(), emptyRequest())
	require.NoError(t, err)
	_, err = client.service.HealthCheck(context.Background(), emptyRequest())
	require.NoError(t, err)
	require.Equal(t, []string{"Bearer cli-access-token-1", "Bearer cli-access-token-1"}, service.recordedHeaders())
	require.Len(t, endpoint.exchangeBodies(), 1, "the access token is cached")

	require.Eventually(t, func() bool {
		_, callErr := client.service.HealthCheck(context.Background(), emptyRequest())
		headers := service.recordedHeaders()
		return callErr == nil && headers[len(headers)-1] == "Bearer cli-access-token-3"
	}, 5*time.Second, 50*time.Millisecond)
	require.GreaterOrEqual(t, time.Since(firstCallAt), 1600*time.Millisecond, "refresh waits for 80%% of the lifetime")
	require.Len(t, endpoint.exchangeBodies(), 3, "a failed refresh keeps the valid token and retries later")
	for _, header := range service.recordedHeaders() {
		require.Contains(t, []string{"Bearer cli-access-token-1", "Bearer cli-access-token-3"}, header)
	}

	_, err = perRPCCredential.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	_, constructionErr := newAPIKeyCredential("http://example.com/token", cliTestServerID, cliTestAPIKey)
	require.Error(t, constructionErr)
	failingEndpoint := startCLITokenEndpoint(t, func(_ int, writer http.ResponseWriter) {
		writeCLITokenResponse(writer, http.StatusOK, `{"access_token":"cli-access-token-secret","token_type":"mac","expires_in":60}`)
	})
	failingCredential, err := newAPIKeyCredential(failingEndpoint.tokenURL(), cliTestServerID, cliTestAPIKey)
	require.NoError(t, err)
	_, exchangeErr := failingCredential.GetRequestMetadata(context.Background())
	require.ErrorContains(t, exchangeErr, "token_type is not Bearer")
	for _, value := range []any{perRPCCredential, failingCredential, constructionErr, exchangeErr} {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
			formatted := fmt.Sprintf(verb, value)
			require.NotContains(t, formatted, cliTestAPIKey)
			require.NotContains(t, formatted, "cli-access-token")
		}
	}
}
