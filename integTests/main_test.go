package integTests

import (
	"context"
	"flag"
	"fmt"
	"github.com/xdblab/xdb-golang-sdk/xdb"
	"github.com/xdblab/xdb/cmd/server/bootstrap"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/extensions/postgres"
	"github.com/xdblab/xdb/extensions/postgres/postgrestool"
	"testing"
	"time"

	"github.com/xdblab/xdb-golang-sdk/integTests/worker"
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
			err = extensions.SetupSchema(sqlConfig, "../"+postgrestool.DefaultSchemaFilePath)
			if err != nil {
				panic(err)
			}

			cfg := config.Config{
				ApiService: config.ApiServiceConfig{
					HttpServer: config.HttpServerConfig{
						Address:      ":" + xdb.DefaultServerPort,
						ReadTimeout:  5 * time.Second,
						WriteTimeout: 60 * time.Second,
					},
				},
				Database: config.DatabaseConfig{
					SQL: sqlConfig,
				},
				AsyncService: config.AsyncServiceConfig{
					Mode: config.AsyncServiceModeStandalone,
					InternalHttpServer: config.HttpServerConfig{
						Address: "0.0.0.0:8701",
					},
				},
			}

			shutdownFunc = bootstrap.StartXdbServer(rootCtx, &cfg, nil)
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
