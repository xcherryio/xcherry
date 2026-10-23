// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package dev

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestZDEngineFlags(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "dex-server-zd")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DEX_SERVER_ZD_BINARY", binary)
	config, err := parseConfig([]string{"--engine", "zd"}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if config.Engine != "zd" || config.ZDBinary != binary {
		t.Fatalf("zd config = %+v", config)
	}
	if config.ZDStartupTimeout != defaultZDStartupTimeout {
		t.Fatalf("zd startup timeout = %s", config.ZDStartupTimeout)
	}
	config.StateDirectory = t.TempDir()
	config.DexPort = 9012
	if expected := filepath.Join(config.StateDirectory, "zd", "9012"); config.zdDataDirectory() != expected {
		t.Fatalf("default zd data directory = %q, want %q", config.zdDataDirectory(), expected)
	}
	overrideDirectory := filepath.Join(t.TempDir(), "isolated")
	config, err = parseConfig([]string{"--engine", "zd", "--zd-data-dir", overrideDirectory}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if config.ZDDataDirectory != overrideDirectory || config.zdDataDirectory() != overrideDirectory {
		t.Fatalf("zd data directory override = %q", config.ZDDataDirectory)
	}
	config, err = parseConfig(nil, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if config.Engine != "temporal" {
		t.Fatalf("default engine = %q", config.Engine)
	}
	for _, flagName := range []string{
		"external-temporal-address", "external-temporal-namespace", "sqlite-db-filename",
		"verbose-engine-log", "attribute-store-config", "blob-store-dir",
	} {
		value := "value"
		if flagName == "external-temporal-address" {
			value = "127.0.0.1:7233"
		}
		arguments := []string{"--engine", "zd", "--" + flagName, value}
		if flagName == "verbose-engine-log" {
			arguments = []string{"--engine", "zd", "--verbose-engine-log"}
		}
		_, err := parseConfig(arguments, &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), "--"+flagName+" cannot be used with --engine zd") {
			t.Fatalf("--%s: expected zd incompatibility, got %v", flagName, err)
		}
	}
	for _, arguments := range [][]string{{"--engine", "unknown"}, {"--zd-binary", binary}, {"--zd-data-dir", overrideDirectory}, {"--engine", "zd", "--zd-data-dir", ""}} {
		if _, err := parseConfig(arguments, &bytes.Buffer{}); err == nil {
			t.Fatalf("accepted invalid arguments %v", arguments)
		}
	}
	t.Setenv("DEX_SERVER_ZD_BINARY", filepath.Join(t.TempDir(), "missing"))
	if _, err := parseConfig([]string{"--engine", "zd"}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "was not found") {
		t.Fatalf("expected missing binary error, got %v", err)
	}
}

func TestZDSupervisorStartsWebAndStopsChild(t *testing.T) {
	binary := buildFakeZDBinary(t)
	testDirectory := t.TempDir()
	argumentsFile := filepath.Join(testDirectory, "arguments")
	stoppedFile := filepath.Join(testDirectory, "stopped")
	t.Setenv("DEX_ZD_TEST_ARGUMENTS", argumentsFile)
	t.Setenv("DEX_ZD_TEST_STOPPED", stoppedFile)
	dexPort, webPort := freeZDTestPorts(t)
	dataDirectory := filepath.Join(testDirectory, "isolated-zd")
	config, err := parseConfig([]string{
		"--engine", "zd", "--zd-binary", binary,
		"--zd-data-dir", dataDirectory,
		"--dex-port", strconv.Itoa(dexPort),
		"--web-port", strconv.Itoa(webPort),
		"--open=false", "--server-log-folder", filepath.Join(testDirectory, "logs"),
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	config.StateDirectory = filepath.Join(testDirectory, "state")
	output := &zdTestBuffer{}
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- newSupervisor(config, output, output).Run(runCtx) }()
	waitForZDTestReady(t, output, finished)
	expected := []string{"dev", "--data-dir", dataDirectory, "--listen", net.JoinHostPort(config.BindAddress, strconv.Itoa(config.DexPort)), "--log-dir", config.LogDirectory}
	arguments, err := os.ReadFile(argumentsFile)
	if err != nil {
		t.Fatal(err)
	}
	if actual := strings.Split(strings.TrimSpace(string(arguments)), "\n"); !reflect.DeepEqual(actual, expected) {
		t.Fatalf("child arguments = %q, want %q", actual, expected)
	}
	if err := checkHTTP(context.Background(), "http://"+net.JoinHostPort(config.BindAddress, strconv.Itoa(config.WebPort))+"/healthz"); err != nil {
		t.Fatalf("Dex Web health: %v", err)
	}
	if strings.Contains(output.String(), "Local DB:") || strings.Contains(output.String(), "Blob store:") {
		t.Fatalf("zd readiness included Temporal state: %s", output.String())
	}
	cancel()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("zd supervisor did not stop")
	}
	if _, err := os.Stat(stoppedFile); err != nil {
		t.Fatalf("child did not receive SIGTERM: %v", err)
	}
}

func TestZDSupervisorFailsWhenChildExitsBeforeReadiness(t *testing.T) {
	binary := buildFakeZDBinary(t)
	t.Setenv("DEX_ZD_TEST_EXIT_EARLY", "1")
	dexPort, webPort := freeZDTestPorts(t)
	config, err := parseConfig([]string{
		"--engine", "zd", "--zd-binary", binary,
		"--dex-port", strconv.Itoa(dexPort),
		"--web-port", strconv.Itoa(webPort),
		"--open=false",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	config.StateDirectory = t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = newSupervisor(config, &bytes.Buffer{}, &bytes.Buffer{}).Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "exited before readiness") {
		t.Fatalf("expected early child exit, got %v", err)
	}
	if config.LogDirectory != "" || strings.Contains(err.Error(), "server log folder") {
		t.Fatalf("zd without --server-log-folder reported an empty log folder: %v", err)
	}
}

func TestZDProcessKillsUnresponsiveChild(t *testing.T) {
	binary := buildFakeZDBinary(t)
	t.Setenv("DEX_ZD_TEST_IGNORE_SIGTERM", "1")
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(freeZDTestPort(t)))
	process, err := startZDProcess(&Config{ZDBinary: binary}, t.TempDir(), address, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if stopErr := process.Stop(time.Second); stopErr != nil {
			t.Error(stopErr)
		}
	})
	readyCtx, cancelReady := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelReady()
	connection, err := waitForZD(readyCtx, address, process)
	if err != nil {
		t.Fatal(err)
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := process.Stop(100 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > 3*time.Second {
		t.Fatal("unresponsive child shutdown took too long")
	}
	var exitError *exec.ExitError
	if !errors.As(process.Err(), &exitError) {
		t.Fatalf("expected killed child exit, got %v", process.Err())
	}
	status, ok := exitError.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("expected SIGKILL after timeout, got %v", exitError.Sys())
	}
}

func TestZDProcessReportsDirtyShutdown(t *testing.T) {
	binary := buildFakeZDBinary(t)
	t.Setenv("DEX_ZD_TEST_EXIT_ON_TERM", "1")
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(freeZDTestPort(t)))
	process, err := startZDProcess(&Config{ZDBinary: binary}, t.TempDir(), address, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if stopErr := process.Stop(time.Second); stopErr != nil {
			t.Error(stopErr)
		}
	})
	readyCtx, cancelReady := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelReady()
	connection, err := waitForZD(readyCtx, address, process)
	if err != nil {
		t.Fatal(err)
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	if err := process.Stop(time.Second); err == nil || !strings.Contains(err.Error(), "exit status 23") {
		t.Fatalf("expected dirty shutdown error, got %v", err)
	}
}

func TestZDSupervisorReportsHealthTimeout(t *testing.T) {
	binary := buildFakeZDBinary(t)
	t.Setenv("DEX_ZD_TEST_NOT_SERVING", "1")
	dexPort, webPort := freeZDTestPorts(t)
	config, err := parseConfig([]string{
		"--engine", "zd", "--zd-binary", binary,
		"--dex-port", strconv.Itoa(dexPort),
		"--web-port", strconv.Itoa(webPort),
		"--open=false",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	config.StateDirectory = t.TempDir()
	config.ZDStartupTimeout = 3 * time.Second
	output := &bytes.Buffer{}
	err = newSupervisor(config, output, output).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "deadline exceeded") || !strings.Contains(err.Error(), "dex.WorkerTaskService") || !strings.Contains(err.Error(), "NOT_SERVING") {
		t.Fatalf("expected health status and timeout, got %v\n%s", err, output.String())
	}
}

func buildFakeZDBinary(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "main.go")
	if err := os.WriteFile(sourcePath, []byte(fakeZDServerSource), 0o600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(directory, "dex-server-zd")
	command := exec.Command("go", "build", "-o", binary, sourcePath)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build fake zd server: %v\n%s", err, output)
	}
	return binary
}

func freeZDTestPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

func freeZDTestPorts(t *testing.T) (int, int) {
	t.Helper()
	firstListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	secondListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		closeErr := firstListener.Close()
		t.Fatal(errors.Join(err, closeErr))
	}
	firstPort := firstListener.Addr().(*net.TCPAddr).Port
	secondPort := secondListener.Addr().(*net.TCPAddr).Port
	if err := firstListener.Close(); err != nil {
		t.Fatal(err)
	}
	if err := secondListener.Close(); err != nil {
		t.Fatal(err)
	}
	return firstPort, secondPort
}

func waitForZDTestReady(t *testing.T, output *zdTestBuffer, finished <-chan error) {
	t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	for {
		if strings.Contains(output.String(), "Dex development environment is ready") {
			return
		}
		select {
		case err := <-finished:
			t.Fatalf("zd supervisor exited before readiness: %v\n%s", err, output.String())
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("zd supervisor did not become ready: %s", output.String())
		}
	}
}

type zdTestBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *zdTestBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(data)
}

func (b *zdTestBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

const fakeZDServerSource = `package main

import (
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	if path := os.Getenv("DEX_ZD_TEST_ARGUMENTS"); path != "" {
		if err := os.WriteFile(path, []byte(strings.Join(os.Args[1:], "\n")), 0o600); err != nil { panic(err) }
	}
	if os.Getenv("DEX_ZD_TEST_EXIT_EARLY") == "1" { os.Exit(17) }
	var address string
	for index := 2; index < len(os.Args)-1; index += 2 {
		if os.Args[index] == "--listen" { address = os.Args[index+1] }
	}
	listener, err := net.Listen("tcp", address)
	if err != nil { panic(err) }
	server := grpc.NewServer()
	healthServer := health.NewServer()
	for _, service := range []string{"", "dex.FlowService", "dex.WorkerTaskService"} {
		status := healthpb.HealthCheckResponse_SERVING
		if service == "dex.WorkerTaskService" && os.Getenv("DEX_ZD_TEST_NOT_SERVING") == "1" {
			status = healthpb.HealthCheckResponse_NOT_SERVING
		}
		healthServer.SetServingStatus(service, status)
	}
	healthpb.RegisterHealthServer(server, healthServer)
	signals := make(chan os.Signal, 1)
	if os.Getenv("DEX_ZD_TEST_IGNORE_SIGTERM") == "1" {
		signal.Ignore(syscall.SIGTERM)
		signal.Notify(signals, syscall.SIGINT)
	} else {
		signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	}
	go func() {
		<-signals
		if os.Getenv("DEX_ZD_TEST_EXIT_ON_TERM") == "1" { os.Exit(23) }
		if path := os.Getenv("DEX_ZD_TEST_STOPPED"); path != "" {
			if err := os.WriteFile(path, []byte("stopped"), 0o600); err != nil { panic(err) }
		}
		server.GracefulStop()
	}()
	if err := server.Serve(listener); err != nil { panic(err) }
}
`
