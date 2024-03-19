// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package integTests

import (
	"context"
	"flag"
	"fmt"
	"github.com/xcherryio/sdk-go/xc"
	"github.com/xcherryio/xcherry/cmd/server/bootstrap"
	"github.com/xcherryio/xcherry/config"
	"github.com/xcherryio/xcherry/extensions"
	"github.com/xcherryio/xcherry/extensions/postgres"
	"github.com/xcherryio/xcherry/extensions/postgres/postgrestool"
	"testing"
	"time"

	"github.com/xcherryio/sdk-go/integTests/worker"
)

func TestMain(m *testing.M) {
	flag.Parse()
	testDBName := fmt.Sprintf("test%v", time.Now().UnixNano())
	fmt.Printf("start running integ test, "+
		"testDBName: %v, useLocalServer:%v, createServerWithPostgres: %v \n",
		testDBName, *useLocalServer, *createServerWithPostgres)

	worker.StartGinWorker(workerService)

	var resultCode int
	var shutdownFunc bootstrap.GracefulShutdown
	rootCtx, rootCtxCancelFunc := context.WithCancel(context.Background())

	if !*useLocalServer {
		if *createServerWithPostgres {
			sqlConfig := &config.SQL{
				ConnectAddr:     fmt.Sprintf("%v:%v", postgrestool.DefaultEndpoint, postgrestool.DefaultPort),
				User:            postgrestool.DefaultUserName,
				Password:        postgrestool.DefaultPassword,
				DBExtensionName: postgres.ExtensionName,
				DatabaseName:    testDBName,
			}
			err := extensions.CreateDatabase(*sqlConfig, testDBName)
			if err != nil {
				panic(err)
			}
			defer func() {
				err := extensions.DropDatabase(*sqlConfig, testDBName)
				if err != nil {
					fmt.Println("failed to drop database ", testDBName, err)
				} else {
					fmt.Println("testing database is deleted")
				}
			}()
			err = extensions.SetupSchema(sqlConfig, "../"+postgrestool.DefaultSchemaFilePath)
			if err != nil {
				panic(err)
			}
			err = extensions.SetupSchema(sqlConfig, "../"+postgrestool.SampleTablesSchemaFilePath)
			if err != nil {
				panic(err)
			}

			cfg := config.Config{
				Log: config.Logger{
					Level: "debug",
				},
				ApiService: &config.ApiServiceConfig{
					HttpServer: config.HttpServerConfig{
						Address:      ":" + xc.DefaultServerPort,
						ReadTimeout:  5 * time.Second,
						WriteTimeout: 60 * time.Second,
					},
					AsyncAddresses: []string{
						"http://0.0.0.0:8701",
					},
				},
				Database: &config.DatabaseConfig{
					ProcessStoreConfig:    sqlConfig,
					VisibilityStoreConfig: sqlConfig,
				},
				AsyncService: &config.AsyncServiceConfig{
					Mode: config.AsyncServiceModeStandalone,
					InternalHttpServer: config.HttpServerConfig{
						Address: "0.0.0.0:8701",
					},
				},
			}

			shutdownFunc = bootstrap.StartXCherryServer(rootCtx, &cfg, nil)
		}
	}

	// looks like this wait can fix some flaky failure
	// where API call is made before Gin server is ready
	time.Sleep(time.Millisecond * 100)

	resultCode = m.Run()
	fmt.Println("finished running integ test with status code", resultCode)
	rootCtxCancelFunc()
	if shutdownFunc != nil {
		_ = shutdownFunc(rootCtx)
	}
}
