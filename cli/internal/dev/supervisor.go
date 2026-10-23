// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package dev

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/superdurable/dex/cli/internal/update"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/bootstrap"
	"github.com/superdurable/dex/service/common/ptr"
	dexweb "github.com/superdurable/dex/web"
	"github.com/superdurable/dex/web/assets"
	temporalclient "go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type supervisor struct {
	cfg    *Config
	stdout io.Writer
	stderr io.Writer
}

type ownedListeners struct {
	dex        net.Listener
	web        net.Listener
	temporal   net.Listener
	temporalUI net.Listener
}

type componentExit struct {
	name string
	err  error
}

func newSupervisor(cfg *Config, stdout io.Writer, stderr io.Writer) *supervisor {
	if cfg == nil {
		panic("dev config must not be nil")
	}
	if stdout == nil || stderr == nil {
		panic("dev output writers must not be nil")
	}
	return &supervisor{cfg: cfg, stdout: stdout, stderr: stderr}
}

func (s *supervisor) Run(ctx context.Context) (runErr error) {
	if s.cfg.Engine == "zd" {
		return s.runZD(ctx)
	}
	listeners, err := reserveOwnedListeners(s.cfg)
	if err != nil {
		return err
	}
	defer func() {
		runErr = errors.Join(runErr, listeners.Close())
	}()
	blobStoreDirectory, err := s.prepareBlobStoreDirectory()
	if err != nil {
		return err
	}
	logDir, err := openServerLogDir(s.cfg)
	if err != nil {
		return err
	}
	defer func() {
		shouldRemove := logDir.isEphemeral && runErr == nil
		if logDir.isEphemeral && runErr != nil {
			runErr = fmt.Errorf("%w (server log folder %s)", runErr, logDir.directory)
		}
		runErr = errors.Join(runErr, logDir.Close(shouldRemove))
	}()

	startupCtx, cancelStartup := context.WithTimeout(ctx, s.cfg.StartupTimeout)
	defer cancelStartup()
	var temporal *temporalProcess
	var temporalClient temporalclient.Client
	if s.cfg.ExternalTemporalAddress == "" {
		if err := listeners.releaseTemporalPorts(); err != nil {
			return err
		}
		temporal, temporalClient, err = s.startLocalTemporal(startupCtx, logDir.engineLog)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		defer func() {
			runErr = errors.Join(runErr, temporal.Stop(s.cfg.ShutdownTimeout))
		}()
	} else {
		temporalClient, err = waitForTemporal(startupCtx, s.cfg, nil)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
	if temporal != nil {
		temporalWebURL := "http://" + net.JoinHostPort(s.cfg.BindAddress, strconv.Itoa(s.cfg.TemporalUIPort))
		if err := waitForHTTP(startupCtx, temporalWebURL, temporal); err != nil {
			temporalClient.Close()
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("wait for internal workflow UI: %w", err)
		}
	}
	temporalClient.Close()

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	componentExits := make(chan componentExit, 2)
	dexRuntime, err := s.startDexRuntime(runCtx, listeners.dex, componentExits, blobStoreDirectory)
	if err != nil {
		return err
	}

	dexConnection, err := waitForDex(startupCtx, listeners.dex.Addr().String(), componentExits)
	if err != nil {
		cancelRun()
		dexRuntime.Close()
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	defer func() {
		runErr = errors.Join(runErr, dexConnection.Close())
	}()

	webServer, err := dexweb.NewServer(
		&dexweb.Config{
			BindAddress:               s.cfg.BindAddress,
			Port:                      s.cfg.WebPort,
			FlowRenderingDirectory:    s.cfg.FlowRenderingDirectory,
			ConnectorSetupEnabled:     true,
			ConnectorConfigDirectory:  s.cfg.ConnectorConfigDirectory,
			ConnectorReleaseOverrides: s.cfg.ConnectorReleaseOverrides,
		},
		dexpb.NewFlowServiceClient(dexConnection),
		assets.Files,
	)
	if err != nil {
		cancelRun()
		dexRuntime.Close()
		return err
	}
	go func() {
		componentExits <- componentExit{name: "Dex Web", err: webServer.Serve(listeners.web)}
	}()
	webURL := "http://" + listeners.web.Addr().String()
	if err := waitForComponentHTTP(startupCtx, webURL+"/healthz", componentExits); err != nil {
		if ctx.Err() != nil {
			return s.shutdown(runCtx, cancelRun, webServer, dexRuntime, nil)
		}
		return s.shutdown(runCtx, cancelRun, webServer, dexRuntime, err)
	}

	s.printReady(webURL, listeners.dex.Addr().String(), blobStoreDirectory)
	s.printUpdateNotice(runCtx)
	if s.cfg.OpenBrowser {
		if err := openBrowser(webURL); err != nil {
			return s.shutdown(runCtx, cancelRun, webServer, dexRuntime, err)
		}
	}

	select {
	case <-ctx.Done():
		runErr = nil
	case exited := <-componentExits:
		runErr = formatComponentExit(exited, "stopped unexpectedly")
	case <-temporalDone(temporal):
		if temporal.Err() == nil {
			runErr = fmt.Errorf("workflow backend stopped unexpectedly")
		} else {
			runErr = fmt.Errorf("workflow backend stopped unexpectedly: %w", temporal.Err())
		}
	}
	return s.shutdown(runCtx, cancelRun, webServer, dexRuntime, runErr)
}

func (s *supervisor) runZD(ctx context.Context) (runErr error) {
	listeners, err := reserveOwnedListeners(s.cfg)
	if err != nil {
		return err
	}
	resources := &zdRunResources{ctx: ctx, shutdownTimeout: s.cfg.ShutdownTimeout, listeners: listeners}
	defer resources.closeInto(&runErr)
	if s.cfg.isExplicit("server-log-folder") {
		logDir, err := openServerLogDir(s.cfg)
		if err != nil {
			return err
		}
		resources.logs = logDir
	}
	if s.cfg.StateDirectory == "" {
		return fmt.Errorf("Dex state directory is required")
	}
	dataDirectory := s.cfg.zdDataDirectory()
	if err := os.MkdirAll(dataDirectory, 0o700); err != nil {
		return fmt.Errorf("create zd state directory: %w", err)
	}
	dexAddress := listeners.dex.Addr().String()
	if err := closeListener(listeners.dex); err != nil {
		return err
	}
	listeners.dex = nil
	process, err := startZDProcess(s.cfg, dataDirectory, dexAddress, s.stderr)
	if err != nil {
		return err
	}
	resources.process = process

	startupCtx, cancelStartup := context.WithTimeout(ctx, s.cfg.ZDStartupTimeout)
	defer cancelStartup()
	dexConnection, err := waitForZD(startupCtx, dexAddress, process)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	resources.connection = dexConnection

	webServer, err := dexweb.NewServer(
		&dexweb.Config{
			BindAddress:               s.cfg.BindAddress,
			Port:                      s.cfg.WebPort,
			FlowRenderingDirectory:    s.cfg.FlowRenderingDirectory,
			ConnectorSetupEnabled:     true,
			ConnectorConfigDirectory:  s.cfg.ConnectorConfigDirectory,
			ConnectorReleaseOverrides: s.cfg.ConnectorReleaseOverrides,
		},
		dexpb.NewFlowServiceClient(dexConnection),
		assets.Files,
	)
	if err != nil {
		return err
	}
	resources.web = webServer
	componentExits := make(chan componentExit, 2)
	go func() { componentExits <- componentExit{name: "Dex Web", err: webServer.Serve(listeners.web)} }()
	go func() {
		<-process.done
		componentExits <- componentExit{name: "Dex Server", err: process.Err()}
	}()
	webURL := "http://" + listeners.web.Addr().String()
	if err := waitForComponentHTTP(startupCtx, webURL+"/healthz", componentExits); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	select {
	case exited := <-componentExits:
		return formatComponentExit(exited, "stopped before readiness")
	default:
	}
	s.printReady(webURL, dexAddress, dataDirectory)
	s.printUpdateNotice(ctx)
	if s.cfg.OpenBrowser {
		if err := openBrowser(webURL); err != nil {
			return err
		}
	}
	select {
	case <-ctx.Done():
		return nil
	case exited := <-componentExits:
		return formatComponentExit(exited, "stopped unexpectedly")
	}
}

func formatComponentExit(exited componentExit, phase string) error {
	if exited.err == nil {
		return fmt.Errorf("%s %s", exited.name, phase)
	}
	return fmt.Errorf("%s %s: %w", exited.name, phase, exited.err)
}

func (s *supervisor) startLocalTemporal(ctx context.Context, logs io.Writer) (*temporalProcess, temporalclient.Client, error) {
	const maxAttempts = 5
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			if err := rebindTemporalPorts(s.cfg); err != nil {
				return nil, nil, err
			}
			if err := assignSQLiteDBFilename(s.cfg); err != nil {
				return nil, nil, err
			}
		}
		if logs != nil {
			if err := s.cfg.writeTemporalStartupRecord(logs); err != nil {
				return nil, nil, fmt.Errorf("write Temporal log header: %w", err)
			}
		}
		process, err := startTemporalProcess(s.cfg, logs)
		if err != nil {
			return nil, nil, err
		}
		client, err := waitForTemporal(ctx, s.cfg, process)
		if err == nil {
			return process, client, nil
		}
		exited := false
		select {
		case <-process.Done():
			exited = true
		default:
		}
		lastErr = errors.Join(err, process.Stop(s.cfg.ShutdownTimeout))
		if ctx.Err() != nil || !exited {
			return nil, nil, lastErr
		}
	}
	return nil, nil, lastErr
}

func (s *supervisor) prepareBlobStoreDirectory() (string, error) {
	directory := s.cfg.BlobStoreDirectory
	if s.cfg.SQLiteDBFilename != "" && s.cfg.blobStoreDirectoryDefault {
		directory = s.cfg.adjacentBlobStoreDirectory()
	}
	directory, err := filepath.Abs(directory)
	if err != nil {
		return "", fmt.Errorf("resolve blob store directory: %w", err)
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return "", fmt.Errorf("create blob store directory: %w", err)
	}
	return directory, nil
}

func (s *supervisor) startDexRuntime(
	ctx context.Context,
	listener net.Listener,
	exits chan<- componentExit,
	blobStoreDirectory string,
) (*bootstrap.Runtime, error) {
	attributeStoreConfig, err := s.cfg.loadAttributeStoreConfig()
	if err != nil {
		return nil, err
	}
	dexConfig := &config.Config{
		Log: config.Logger{
			Level:      "info",
			Encoding:   "console",
			OutputFile: s.cfg.dexServerLogPath(),
		},
		Api: config.ApiConfig{Port: s.cfg.DexPort},
		StreamStore: config.StreamStoreConfig{
			Backend: config.StreamStoreBackendMemory,
		},
		BlobStore: config.BlobStoreConfig{
			Enabled:                ptr.Any(true),
			LazyLoading:            ptr.Any(true),
			ThresholdInBytes:       config.DefaultBlobStoreThresholdInBytes,
			HistoryRetentionInDays: 1,
			SupportedStorages: []config.BlobStoreConfigEntry{{
				Status:         config.StorageStatusActive,
				StorageId:      "dexcli-local",
				StorageType:    config.StorageTypeLocal,
				LocalDirectory: blobStoreDirectory,
				CleanupStrategy: config.CleanupStrategy{
					CleanupStrategyType:    config.CleanupStrategyTypeAfterAllRunsDeleted,
					CleanupFrequencyInDays: 1,
				},
			}},
		},
		Interpreter: config.Interpreter{
			Temporal: &config.TemporalConfig{
				HostPort:  s.cfg.temporalAddress(),
				Namespace: s.cfg.TemporalNamespace,
			},
			InterpreterActivityConfig: config.InterpreterActivityConfig{
				InternalServiceTarget: listener.Addr().String(),
			},
		},
		AttributeStore: attributeStoreConfig,
	}
	dexRuntime, err := bootstrap.New(dexConfig, &bootstrap.Options{
		Services:        bootstrap.Services{API: true, Interpreter: true, Matching: true},
		APIListener:     listener,
		ShutdownTimeout: s.cfg.ShutdownTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("create Dex Server: %w", err)
	}
	go func() {
		exits <- componentExit{name: "Dex Server", err: dexRuntime.Run(ctx)}
	}()
	return dexRuntime, nil
}

func (s *supervisor) shutdown(
	ctx context.Context,
	cancel context.CancelFunc,
	webServer *dexweb.Server,
	dexRuntime *bootstrap.Runtime,
	runErr error,
) error {
	shutdownCtx, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), s.cfg.ShutdownTimeout)
	defer cancelShutdown()
	webErr := webServer.Shutdown(shutdownCtx)
	cancel()
	dexRuntime.Close()
	if webErr != nil && !errors.Is(webErr, context.Canceled) {
		runErr = errors.Join(runErr, fmt.Errorf("stop Dex Web: %w", webErr))
	}
	return runErr
}

func (s *supervisor) printReady(webURL string, dexAddress string, blobStoreDirectory string) {
	fmt.Fprintln(s.stdout)
	fmt.Fprintln(s.stdout, "Dex development environment is ready")
	fmt.Fprintln(s.stdout)
	fmt.Fprintf(s.stdout, "Dex Web:       %s\n", webURL)
	fmt.Fprintf(s.stdout, "Dex Server:    %s\n", dexAddress)
	if s.cfg.SQLiteDBFilename != "" {
		fmt.Fprintf(s.stdout, "Local DB:      %s\n", s.cfg.SQLiteDBFilename)
	}
	if s.cfg.Engine == "zd" {
		fmt.Fprintf(s.stdout, "State dir:     %s\n", blobStoreDirectory)
	} else {
		fmt.Fprintf(s.stdout, "Blob store:    %s\n", blobStoreDirectory)
	}
	if s.cfg.LogDirectory != "" {
		fmt.Fprintf(s.stdout, "Server log folder: %s\n", s.cfg.LogDirectory)
	}
	fmt.Fprintln(s.stdout)
	fmt.Fprintln(s.stdout, "Press Ctrl+C to stop.")
}

func (s *supervisor) printUpdateNotice(ctx context.Context) {
	go func() {
		update.PrintNotice(ctx, s.stderr, s.cfg.version)
	}()
}

func reserveOwnedListeners(cfg *Config) (*ownedListeners, error) {
	allocated := make(map[string]string)
	dexListener, err := bindServicePort("Dex Server", cfg.isExplicit("dex-port"), cfg, &cfg.DexPort, allocated)
	if err != nil {
		return nil, err
	}
	listeners := &ownedListeners{dex: dexListener}
	webListener, err := bindServicePort("Dex Web", cfg.isExplicit("web-port"), cfg, &cfg.WebPort, allocated)
	if err != nil {
		return nil, errors.Join(err, listeners.Close())
	}
	listeners.web = webListener
	if cfg.Engine == "zd" || cfg.ExternalTemporalAddress != "" {
		return listeners, nil
	}
	temporalListener, err := bindServicePort("Temporal", false, cfg, &cfg.TemporalPort, allocated)
	if err != nil {
		return nil, errors.Join(err, listeners.Close())
	}
	listeners.temporal = temporalListener
	uiListener, err := bindServicePort("Temporal Web", false, cfg, &cfg.TemporalUIPort, allocated)
	if err != nil {
		return nil, errors.Join(err, listeners.Close())
	}
	listeners.temporalUI = uiListener
	if err := assignSQLiteDBFilename(cfg); err != nil {
		return nil, errors.Join(err, listeners.Close())
	}
	return listeners, nil
}

func rebindTemporalPorts(cfg *Config) error {
	if cfg.TemporalPort < 65535 {
		cfg.TemporalPort++
	}
	if cfg.TemporalUIPort < 65535 {
		cfg.TemporalUIPort++
	}
	allocated := map[string]string{
		net.JoinHostPort(cfg.BindAddress, strconv.Itoa(cfg.DexPort)): "Dex Server",
		net.JoinHostPort(cfg.BindAddress, strconv.Itoa(cfg.WebPort)): "Dex Web",
	}
	temporalListener, err := bindServicePort("Temporal", false, cfg, &cfg.TemporalPort, allocated)
	if err != nil {
		return err
	}
	uiListener, err := bindServicePort("Temporal Web", false, cfg, &cfg.TemporalUIPort, allocated)
	if err != nil {
		return errors.Join(err, closeListener(temporalListener))
	}
	return errors.Join(closeListener(temporalListener), closeListener(uiListener))
}

func bindServicePort(
	name string,
	explicit bool,
	cfg *Config,
	port *int,
	allocated map[string]string,
) (net.Listener, error) {
	for candidate := *port; candidate <= 65535; candidate++ {
		address := net.JoinHostPort(cfg.BindAddress, strconv.Itoa(candidate))
		if owner, exists := allocated[address]; exists {
			if explicit {
				return nil, fmt.Errorf("%s and %s cannot both use %s", owner, name, address)
			}
			continue
		}
		if isAddressReachable(address) {
			if explicit {
				return nil, fmt.Errorf("cannot start %s: %s is already in use", name, address)
			}
			continue
		}
		listener, err := net.Listen("tcp", address)
		if err != nil {
			if explicit {
				return nil, fmt.Errorf("cannot start %s: %s is already in use: %w", name, address, err)
			}
			continue
		}
		*port = candidate
		allocated[address] = name
		return listener, nil
	}
	return nil, fmt.Errorf("cannot start %s: no available port from %d", name, *port)
}

func assignSQLiteDBFilename(cfg *Config) error {
	if cfg.ExternalTemporalAddress != "" || cfg.isExplicit("sqlite-db-filename") {
		return nil
	}
	if cfg.StateDirectory == "" {
		return fmt.Errorf("Dex state directory is required")
	}
	filename := cfg.autoSQLiteDBFilename()
	if err := os.MkdirAll(filepath.Dir(filename), 0o700); err != nil {
		return fmt.Errorf("create Temporal database directory: %w", err)
	}
	cfg.SQLiteDBFilename = filename
	return nil
}

func isAddressReachable(address string) bool {
	connection, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
	if err != nil {
		return false
	}
	// Closing the probe does not change that the port accepted a connection.
	_ = connection.Close()
	return true
}

func closeListener(listener net.Listener) error {
	if listener == nil {
		return nil
	}
	if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	return nil
}

func (l *ownedListeners) releaseTemporalPorts() error {
	err := errors.Join(closeListener(l.temporal), closeListener(l.temporalUI))
	l.temporal = nil
	l.temporalUI = nil
	return err
}

func (l *ownedListeners) Close() error {
	return errors.Join(l.releaseTemporalPorts(), closeListener(l.web), closeListener(l.dex))
}

func waitForTemporal(
	ctx context.Context,
	cfg *Config,
	process *temporalProcess,
) (temporalclient.Client, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		temporalClient, err := temporalclient.Dial(temporalclient.Options{
			HostPort:  cfg.temporalAddress(),
			Namespace: cfg.TemporalNamespace,
			ConnectionOptions: temporalclient.ConnectionOptions{
				GetSystemInfoTimeout: time.Second,
			},
		})
		if err == nil {
			checkCtx, cancel := context.WithTimeout(ctx, time.Second)
			_, err = temporalClient.CheckHealth(checkCtx, &temporalclient.CheckHealthRequest{})
			cancel()
			if err == nil {
				return temporalClient, nil
			}
			temporalClient.Close()
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for workflow backend: %w", ctx.Err())
		case <-temporalDone(process):
			return nil, fmt.Errorf("workflow backend exited before readiness: %w", process.Err())
		case <-ticker.C:
		}
	}
}

func waitForDex(
	ctx context.Context,
	address string,
	exits <-chan componentExit,
) (*grpc.ClientConn, error) {
	connection, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(config.DefaultGrpcMaxMessageBytes),
			grpc.MaxCallSendMsgSize(config.DefaultGrpcMaxMessageBytes),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create Dex Server client: %w", err)
	}
	healthClient := healthpb.NewHealthClient(connection)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		checkCtx, cancel := context.WithTimeout(ctx, time.Second)
		response, checkErr := healthClient.Check(checkCtx, &healthpb.HealthCheckRequest{
			Service: dexpb.FlowService_ServiceDesc.ServiceName,
		})
		cancel()
		if checkErr == nil && response.GetStatus() == healthpb.HealthCheckResponse_SERVING {
			return connection, nil
		}
		lastErr = checkErr
		select {
		case <-ctx.Done():
			closeErr := connection.Close()
			return nil, errors.Join(fmt.Errorf("wait for Dex Server at %s: %w", address, errors.Join(ctx.Err(), lastErr)), closeErr)
		case exited := <-exits:
			closeErr := connection.Close()
			return nil, errors.Join(fmt.Errorf("%s exited before readiness: %w", exited.name, exited.err), closeErr)
		case <-ticker.C:
		}
	}
}

func waitForHTTP(ctx context.Context, url string, process *temporalProcess) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := checkHTTP(ctx, url); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for internal workflow UI: %w", ctx.Err())
		case <-temporalDone(process):
			return fmt.Errorf("workflow backend exited before UI readiness: %w", process.Err())
		case <-ticker.C:
		}
	}
}

func waitForComponentHTTP(ctx context.Context, url string, exits <-chan componentExit) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		lastErr = checkHTTP(ctx, url)
		if lastErr == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), lastErr)
		case exited := <-exits:
			return fmt.Errorf("%s exited before readiness: %w", exited.name, exited.err)
		case <-ticker.C:
		}
	}
}

func checkHTTP(ctx context.Context, url string) error {
	requestCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	_, readErr := io.Copy(io.Discard, response.Body)
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil {
		return errors.Join(readErr, closeErr)
	}
	if response.StatusCode < 200 || response.StatusCode >= 400 {
		return fmt.Errorf("%s returned HTTP %d", url, response.StatusCode)
	}
	return nil
}

func temporalDone(process *temporalProcess) <-chan struct{} {
	if process == nil {
		return nil
	}
	return process.Done()
}

func openBrowser(url string) error {
	var command *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		command = exec.Command("open", url)
	case "linux":
		command = exec.Command("xdg-open", url)
	default:
		return fmt.Errorf("--open is not supported on %s", runtime.GOOS)
	}
	if err := command.Start(); err != nil {
		return fmt.Errorf("open Dex Web: %w", err)
	}
	if err := command.Process.Release(); err != nil {
		return fmt.Errorf("release browser process: %w", err)
	}
	return nil
}
