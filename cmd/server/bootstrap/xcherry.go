// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package bootstrap

import (
	"context"
	"fmt"
	"github.com/xcherryio/xcherry/persistence/visibility"
	rawLog "log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/urfave/cli/v2"
	"github.com/xcherryio/xcherry/common/log"
	"github.com/xcherryio/xcherry/common/log/tag"
	"github.com/xcherryio/xcherry/config"
	"github.com/xcherryio/xcherry/persistence/process"
	"github.com/xcherryio/xcherry/service/api"
	"github.com/xcherryio/xcherry/service/async"
	"go.uber.org/multierr"
)

const ApiServiceName = "api"
const AsyncServiceName = "async"

const FlagConfig = "config"
const FlagService = "service"

func StartXCherryServerCli(c *cli.Context) {
	// register interrupt signal for graceful shutdown
	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	configPath := c.String("config")
	services := getServices(c)

	cfg, err := config.NewConfig(configPath)
	if err != nil {
		rawLog.Fatalf("Unable to load config for path %v because of error %v", configPath, err)
	}
	shutdownFunc := StartXCherryServer(rootCtx, cfg, services)
	// wait for os signals
	<-rootCtx.Done()

	ctx, cancF := context.WithTimeout(context.Background(), time.Second*10)
	defer cancF()
	err = shutdownFunc(ctx)
	if err != nil {
		fmt.Println("shutdown error:", err)
	}
}

type GracefulShutdown func(ctx context.Context) error

func StartXCherryServer(rootCtx context.Context, cfg *config.Config, services map[string]bool) GracefulShutdown {
	if len(services) == 0 {
		services = map[string]bool{ApiServiceName: true, AsyncServiceName: true}
	}

	zapLogger, err := cfg.Log.NewZapLogger()
	if err != nil {
		rawLog.Fatalf("Unable to create a new zap logger %v", err)
	}
	logger := log.NewLogger(zapLogger)
	logger.Info("config is loaded", tag.Value(cfg.String()))
	err = cfg.ValidateAndSetDefaults()
	if err != nil {
		logger.Fatal("config is invalid", tag.Error(err))
	}

	processStore, err := process.NewSQLProcessStore(*cfg.Database.ProcessStoreConfig, logger)
	if err != nil {
		logger.Fatal("error on persistence setup", tag.Error(err))
	}

	visibilityStore, err := visibility.NewSqlVisibilityStore(*cfg.Database.VisibilityStoreConfig, logger)
	if err != nil {
		logger.Fatal("error on visibility setup", tag.Error(err))
	}

	var apiServer api.Server
	if services[ApiServiceName] {
		apiServer = api.NewDefaultAPIServerWithGin(
			rootCtx, *cfg, processStore, logger.WithTags(tag.Service(ApiServiceName)))
		err = apiServer.Start()
		if err != nil {
			logger.Fatal("Failed to start api server", tag.Error(err))
		}
	}

	var asyncServer async.Server
	if services[AsyncServiceName] {
		asyncServer := async.NewDefaultAPIServerWithGin(
			rootCtx, *cfg, processStore, visibilityStore, logger.WithTags(tag.Service(AsyncServiceName)))
		err = asyncServer.Start()
		if err != nil {
			logger.Fatal("Failed to start async server", tag.Error(err))
		}
	}

	return func(ctx context.Context) error {
		// graceful shutdown
		var errs error
		// first stop api server
		if apiServer != nil {
			err := apiServer.Stop(ctx)
			if err != nil {
				errs = multierr.Append(errs, err)
			}
		}
		if asyncServer != nil {
			err := asyncServer.Stop(ctx)
			if err != nil {
				errs = multierr.Append(errs, err)
			}
		}
		// stop processStore and visibilityStore
		err := processStore.Close()
		if err != nil {
			errs = multierr.Append(errs, err)
		}
		err = visibilityStore.Close()
		if err != nil {
			errs = multierr.Append(errs, err)
		}
		return errs
	}
}

func getServices(c *cli.Context) map[string]bool {
	val := strings.TrimSpace(c.String(FlagService))
	tokens := strings.Split(val, ",")

	if len(tokens) == 0 {
		rawLog.Fatal("No services specified for starting")
	}

	services := map[string]bool{}
	for _, token := range tokens {
		t := strings.TrimSpace(token)
		services[t] = true
	}

	return services
}
