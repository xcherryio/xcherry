// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//go:build integration

package dex

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/urfave/cli"
	"google.golang.org/grpc"
)

type flowService struct {
	dexpb.UnimplementedFlowServiceServer
}

func (f *flowService) SearchFlows(
	context.Context,
	*dexpb.SearchFlowsRequest,
) (*dexpb.SearchFlowsResponse, error) {
	return &dexpb.SearchFlowsResponse{}, nil
}

func TestWebOnlyStartsWithoutWorkflowBackend(t *testing.T) {
	grpcListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	dexpb.RegisterFlowServiceServer(grpcServer, &flowService{})
	grpcExit := make(chan error, 1)
	go func() {
		grpcExit <- grpcServer.Serve(grpcListener)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		serveErr := <-grpcExit
		if serveErr != nil {
			require.ErrorIs(t, serveErr, grpc.ErrServerStopped)
		}
	})

	webPort := reservePort(t)
	configPath := filepath.Join(t.TempDir(), "web-only.yaml")
	configBody := fmt.Sprintf(
		"web:\n  bindAddress: 127.0.0.1\n  port: %d\n  flowServiceTarget: %s\n",
		webPort,
		grpcListener.Addr().String(),
	)
	require.NoError(t, os.WriteFile(configPath, []byte(configBody), 0o600))

	ctx, cancel := context.WithCancel(context.Background())
	serverExit := make(chan error, 1)
	go func() {
		serverExit <- BuildCLI(ctx).Run([]string{
			"dex-server",
			"--config", configPath,
			"start",
			"--services", "web",
		})
	}()

	httpClient := &http.Client{Timeout: time.Second}
	webAddress := "http://127.0.0.1:" + strconv.Itoa(webPort)
	require.Eventually(t, func() bool {
		response, requestErr := httpClient.Get(webAddress + "/healthz")
		if requestErr != nil {
			return false
		}
		defer response.Body.Close()
		return response.StatusCode == http.StatusOK
	}, 5*time.Second, 25*time.Millisecond)

	response, err := httpClient.Get(webAddress)
	require.NoError(t, err)
	indexBody, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Contains(t, string(indexBody), `<div id="root"></div>`)

	response, err = httpClient.Post(
		webAddress+"/api/flows/search",
		"application/json",
		bytes.NewBufferString(`{"pageSize":1}`),
	)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, http.StatusOK, response.StatusCode)

	cancel()
	require.NoError(t, <-serverExit)
	releasedListener, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(webPort)))
	require.NoError(t, err)
	require.NoError(t, releasedListener.Close())
}

func TestWebOnlyStartsBeforeFlowService(t *testing.T) {
	webPort := reservePort(t)
	flowServicePort := reservePort(t)
	configPath := filepath.Join(t.TempDir(), "web-before-api.yaml")
	configBody := fmt.Sprintf(
		"web:\n  bindAddress: 127.0.0.1\n  port: %d\n  flowServiceTarget: 127.0.0.1:%d\n",
		webPort,
		flowServicePort,
	)
	require.NoError(t, os.WriteFile(configPath, []byte(configBody), 0o600))

	ctx, cancel := context.WithCancel(context.Background())
	serverExit := make(chan error, 1)
	go func() {
		serverExit <- BuildCLI(ctx).Run([]string{
			"dex-server",
			"--config", configPath,
			"start",
			"--services=web",
		})
	}()

	httpClient := &http.Client{Timeout: time.Second}
	webAddress := "http://127.0.0.1:" + strconv.Itoa(webPort)
	require.Eventually(t, func() bool {
		response, requestErr := httpClient.Get(webAddress + "/healthz")
		if requestErr != nil {
			return false
		}
		defer response.Body.Close()
		return response.StatusCode == http.StatusOK
	}, 5*time.Second, 25*time.Millisecond)

	cancel()
	require.NoError(t, <-serverExit)
}

func TestWebConfigDefaultsAndOverrides(t *testing.T) {
	defaultConfig := config.Config{}
	require.Equal(t, config.DefaultWebBindAddress, defaultConfig.Web.EffectiveBindAddress())
	require.Equal(t, config.DefaultWebPort, defaultConfig.Web.EffectivePort())
	require.Equal(t, "localhost:8801", defaultConfig.GetWebFlowServiceTargetWithDefault())

	configPath := filepath.Join(t.TempDir(), "web.yaml")
	configBody := `
api:
  port: 9801
web:
  bindAddress: 127.0.0.1
  port: 9802
  flowServiceTarget: dex-api.example:8801
  flowRenderingDirectory: /var/lib/dex/flow-definitions
`
	require.NoError(t, os.WriteFile(configPath, []byte(configBody), 0o600))
	cfg, err := config.NewConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", cfg.Web.EffectiveBindAddress())
	require.Equal(t, 9802, cfg.Web.EffectivePort())
	require.Equal(t, "dex-api.example:8801", cfg.GetWebFlowServiceTargetWithDefault())
	require.Equal(t, "/var/lib/dex/flow-definitions", cfg.Web.FlowRenderingDirectory)

	cfg.Web.FlowServiceTarget = ""
	require.Equal(t, "localhost:9801", cfg.GetWebFlowServiceTargetWithDefault())
}

func TestServiceSelection(t *testing.T) {
	testCases := []struct {
		name        string
		arguments   []string
		expected    serviceSelection
		errorString string
	}{
		{
			name: "default",
			expected: serviceSelection{
				isWebEnabled:         true,
				isAPIEnabled:         true,
				isInterpreterEnabled: true,
			},
		},
		{name: "web", arguments: []string{"--services", "web"}, expected: serviceSelection{isWebEnabled: true}},
		{name: "api", arguments: []string{"--services", "api"}, expected: serviceSelection{isAPIEnabled: true}},
		{name: "interpreter", arguments: []string{"--services", "interpreter"}, expected: serviceSelection{isInterpreterEnabled: true}},
		{
			name:      "web and api",
			arguments: []string{"--services", "web, api"},
			expected:  serviceSelection{isWebEnabled: true, isAPIEnabled: true},
		},
		{
			name:      "web and interpreter",
			arguments: []string{"--services", "web, interpreter"},
			expected:  serviceSelection{isWebEnabled: true, isInterpreterEnabled: true},
		},
		{
			name:      "api and interpreter",
			arguments: []string{"--services", "api, interpreter"},
			expected:  serviceSelection{isAPIEnabled: true, isInterpreterEnabled: true},
		},
		{
			name:      "all explicit",
			arguments: []string{"--services", "web,api,interpreter"},
			expected: serviceSelection{
				isWebEnabled:         true,
				isAPIEnabled:         true,
				isInterpreterEnabled: true,
			},
		},
		{name: "empty", arguments: []string{"--services", ""}, errorString: "no services specified for starting"},
		{name: "unknown", arguments: []string{"--services", "scheduler"}, errorString: `invalid service "scheduler"`},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			cliContext := serviceCLIContext(t, testCase.arguments)
			selection, err := getServices(cliContext)
			if testCase.errorString != "" {
				require.EqualError(t, err, testCase.errorString)
				return
			}
			require.NoError(t, err)
			require.Equal(t, testCase.expected, selection)
		})
	}
}

func reservePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	return port
}

func serviceCLIContext(t *testing.T, arguments []string) *cli.Context {
	t.Helper()
	app := BuildCLI(context.Background())
	flagSet := flag.NewFlagSet("start", flag.ContinueOnError)
	for _, commandFlag := range app.Commands[0].Flags {
		commandFlag.Apply(flagSet)
	}
	require.NoError(t, flagSet.Parse(arguments))
	return cli.NewContext(app, flagSet, nil)
}
