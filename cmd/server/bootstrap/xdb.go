// Apache License 2.0

// Copyright (c) XDBLab organization

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.    

package bootstrap

import (
	"context"
	"fmt"
	"github.com/urfave/cli/v2"
	log2 "github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/persistence/sql"
	"github.com/xdblab/xdb/service/api"
	"github.com/xdblab/xdb/service/async"
	"go.uber.org/multierr"
	rawLog "log"
	"os"
	"os/signal"
	"strings"
	"time"
)

const ApiServiceName = "api"
const AsyncServiceName = "async"

const FlagConfig = "config"
const FlagService = "service"

func StartXdbServerCli(c *cli.Context) {
	// register interrupt signal for graceful shutdown
	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	configPath := c.String("config")
	services := getServices(c)

	cfg, err := config.NewConfig(configPath)
	if err != nil {
		rawLog.Fatalf("Unable to load config for path %v because of error %v", configPath, err)
	}
	shutdownFunc := StartXdbServer(rootCtx, cfg, services)
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

func StartXdbServer(rootCtx context.Context, cfg *config.Config, services map[string]bool) GracefulShutdown {
	if len(services) == 0 {
		services = map[string]bool{ApiServiceName: true, AsyncServiceName: true}
	}

	zapLogger, err := cfg.Log.NewZapLogger()
	if err != nil {
		rawLog.Fatalf("Unable to create a new zap logger %v", err)
	}
	logger := log2.NewLogger(zapLogger)
	logger.Info("config is loaded", tag.Value(cfg.String()))
	err = cfg.ValidateAndSetDefaults()
	if err != nil {
		logger.Fatal("config is invalid", tag.Error(err))
	}

	sqlStore, err := sql.NewSQLProcessStore(*cfg.Database.SQL, logger)
	if err != nil {
		logger.Fatal("error on persistence setup", tag.Error(err))
	}

	var apiServer api.Server
	if services[ApiServiceName] {
		apiServer = api.NewDefaultAPIServerWithGin(rootCtx, *cfg, sqlStore, logger.WithTags(tag.Service(ApiServiceName)))
		err = apiServer.Start()
		if err != nil {
			logger.Fatal("Failed to start api server", tag.Error(err))
		}
	}

	var asyncServer async.Server
	if services[AsyncServiceName] {
		asyncServer := async.NewDefaultAPIServerWithGin(rootCtx, *cfg, sqlStore, logger.WithTags(tag.Service(AsyncServiceName)))
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
		// stop sqlStore
		err := sqlStore.Close()
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
