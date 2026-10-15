// Copyright (c) 2021 Cadence workflow OSS organization
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package dex

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/bootstrap"
	dexweb "github.com/superdurable/dex/web"
	"github.com/superdurable/dex/web/assets"
	"github.com/urfave/cli"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	serviceWeb         = "web"
	serviceAPI         = "api"
	serviceInterpreter = "interpreter"
)

type serviceSelection struct {
	isWebEnabled         bool
	isAPIEnabled         bool
	isInterpreterEnabled bool
}

type componentExit struct {
	name string
	err  error
}

type application struct {
	dexRuntime     *bootstrap.Runtime
	webServer      *dexweb.Server
	webConnection  *grpc.ClientConn
	componentCount int
}

// BuildCLI is the main entry point for the dex server
func BuildCLI(ctx context.Context) *cli.App {
	if ctx == nil {
		panic("Dex Server context must not be nil")
	}
	app := cli.NewApp()
	app.Name = "dex service"
	app.Usage = "dex service"
	app.Version = service.DexServerVersion
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "config, c",
			Value: "config/development.yaml",
			Usage: "config path is a path relative to root, or an absolute path",
		},
	}
	app.Commands = []cli.Command{
		{
			Name:    "start",
			Aliases: []string{""},
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "services",
					Value: strings.Join([]string{serviceWeb, serviceAPI, serviceInterpreter}, ", "),
					Usage: "start Dex components: web, api, interpreter",
				},
			},
			Usage: "start Dex Server",
			Action: func(cliContext *cli.Context) error {
				return start(ctx, cliContext)
			},
		},
	}
	return app
}

func start(ctx context.Context, cliContext *cli.Context) error {
	cfg, err := config.NewConfig(cliContext.GlobalString("config"))
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	services, err := getServices(cliContext)
	if err != nil {
		return err
	}
	serverApplication, err := newApplication(cfg, services)
	if err != nil {
		return err
	}
	return serverApplication.Run(ctx)
}

func newApplication(cfg *config.Config, services serviceSelection) (*application, error) {
	if cfg == nil {
		panic("Dex Server config must not be nil")
	}
	serverApplication := &application{}
	if services.isWebEnabled {
		connection, err := grpc.NewClient(
			cfg.GetWebFlowServiceTargetWithDefault(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(cfg.Api.EffectiveGrpcMaxMessageBytes()),
				grpc.MaxCallSendMsgSize(cfg.Api.EffectiveGrpcMaxMessageBytes()),
			),
		)
		if err != nil {
			return nil, fmt.Errorf("create Dex Web FlowService client: %w", err)
		}
		webConfig := &dexweb.Config{
			BindAddress:                     cfg.Web.EffectiveBindAddress(),
			Port:                            cfg.Web.EffectivePort(),
			FlowRenderingDirectory:          cfg.Web.FlowRenderingDirectory,
			IsStartFlowWorkerTargetHeadless: cfg.Web.IsStartFlowWorkerTargetHeadless,
		}
		webServer, err := dexweb.NewServer(
			webConfig,
			dexpb.NewFlowServiceClient(connection),
			assets.Files,
		)
		if err != nil {
			if closeErr := connection.Close(); closeErr != nil {
				return nil, errors.Join(err, fmt.Errorf("close Dex Web FlowService connection: %w", closeErr))
			}
			return nil, err
		}
		serverApplication.webConnection = connection
		serverApplication.webServer = webServer
		serverApplication.componentCount++
	}
	if services.isAPIEnabled || services.isInterpreterEnabled {
		dexRuntime, err := bootstrap.New(cfg, &bootstrap.Options{
			Services: bootstrap.Services{
				API:         services.isAPIEnabled,
				Interpreter: services.isInterpreterEnabled,
			},
		})
		if err != nil {
			return nil, errors.Join(err, serverApplication.closeWebConnection())
		}
		serverApplication.dexRuntime = dexRuntime
		serverApplication.componentCount++
	}
	return serverApplication, nil
}

func (a *application) Run(ctx context.Context) error {
	if ctx == nil {
		panic("Dex Server context must not be nil")
	}
	runCtx, cancelRun := context.WithCancel(ctx)
	componentExits := make(chan componentExit, a.componentCount)
	if a.dexRuntime != nil {
		go func() {
			componentExits <- componentExit{name: "Dex API/Interpreter", err: a.dexRuntime.Run(runCtx)}
		}()
	}
	if a.webServer != nil {
		go func() {
			componentExits <- componentExit{name: "Dex Web", err: a.webServer.Run()}
		}()
	}

	select {
	case <-ctx.Done():
		return a.shutdown(runCtx, cancelRun, componentExits, a.componentCount, nil)
	case exited := <-componentExits:
		runErr := unexpectedComponentExit(exited)
		return a.shutdown(runCtx, cancelRun, componentExits, a.componentCount-1, runErr)
	}
}

func (a *application) shutdown(
	runCtx context.Context,
	cancelRun context.CancelFunc,
	componentExits <-chan componentExit,
	remainingComponents int,
	runErr error,
) error {
	shutdownCtx, cancelShutdown := context.WithTimeout(context.WithoutCancel(runCtx), bootstrap.DefaultShutdownTimeout)
	defer cancelShutdown()
	if a.webServer != nil {
		if err := a.webServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			runErr = errors.Join(runErr, fmt.Errorf("stop Dex Web: %w", err))
		}
	}
	cancelRun()
	for remainingComponents > 0 {
		select {
		case exited := <-componentExits:
			remainingComponents--
			if !isExpectedShutdown(exited.err) {
				runErr = errors.Join(runErr, fmt.Errorf("stop %s: %w", exited.name, exited.err))
			}
		case <-shutdownCtx.Done():
			if a.dexRuntime != nil {
				a.dexRuntime.Close()
			}
			return errors.Join(
				runErr,
				a.closeWebConnection(),
				fmt.Errorf("stop Dex Server components: %w", shutdownCtx.Err()),
			)
		}
	}
	return errors.Join(runErr, a.closeWebConnection())
}

func (a *application) closeWebConnection() error {
	if a.webConnection == nil {
		return nil
	}
	err := a.webConnection.Close()
	a.webConnection = nil
	if err != nil {
		return fmt.Errorf("close Dex Web FlowService connection: %w", err)
	}
	return nil
}

func unexpectedComponentExit(exited componentExit) error {
	if exited.err == nil || isExpectedShutdown(exited.err) {
		return fmt.Errorf("%s stopped unexpectedly", exited.name)
	}
	return fmt.Errorf("%s stopped unexpectedly: %w", exited.name, exited.err)
}

func isExpectedShutdown(err error) bool {
	return err == nil || errors.Is(err, http.ErrServerClosed) || errors.Is(err, grpc.ErrServerStopped)
}

func getServices(cliContext *cli.Context) (serviceSelection, error) {
	value := strings.TrimSpace(cliContext.String("services"))
	if value == "" {
		return serviceSelection{}, fmt.Errorf("no services specified for starting")
	}
	var services serviceSelection
	for _, token := range strings.Split(value, ",") {
		switch strings.TrimSpace(token) {
		case serviceWeb:
			services.isWebEnabled = true
		case serviceAPI:
			services.isAPIEnabled = true
		case serviceInterpreter:
			services.isInterpreterEnabled = true
		default:
			return serviceSelection{}, fmt.Errorf("invalid service %q", token)
		}
	}
	return services, nil
}
