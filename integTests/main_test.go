package integTests

import (
	"context"
	"flag"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
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
	fmt.Printf("start running integ test, using, postgres:%v \n", *postgresIntegTest)

	worker.StartGinWorker(workerService)

	var resultCode int

	var sqlConfig *config.SQL
	if *postgresIntegTest {
		sqlConfig = &config.SQL{
			ConnectAddr:     fmt.Sprintf("%v:%v", postgrestool.DefaultEndpoint, postgrestool.DefaultPort),
			User:            postgrestool.DefaultUserName,
			Password:        postgrestool.DefaultPassword,
			DBExtensionName: postgres.ExtensionName,
			DatabaseName:    postgrestool.DefaultDatabaseName,
		}
		err := extensions.CreateDatabase(*sqlConfig, postgrestool.DefaultDatabaseName)
		if err != nil {
			fmt.Println("ignore error for creating database", err)
		}
		err = extensions.SetupSchema(sqlConfig, "../"+postgrestool.DefaultSchemaFilePath)
		if err != nil {
			fmt.Println("ignore error for setup database", err)
			//panic(err)
		}
		defer func() {
			if *keepDatabaseForDebugWhenTestFails && resultCode != 0 {
				return
			}
			// TODO clean up data in the testing database
		}()
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
			MessageQueue: config.MessageQueueConfig{
				Pulsar: &config.PulsarMQConfig{
					PulsarClientOptions: pulsar.ClientOptions{
						URL: "pulsar://localhost:6650",
					},
					CDCTopicsPrefix:             "public/default/dbserver1.public.",
					DefaultCDCTopicSubscription: "default-shared",
				},
			},
		},
	}

	rootCtx, rootCtxCancelFunc := context.WithCancel(context.Background())
	shutdownFunc := bootstrap.StartXdbServer(rootCtx, &cfg, nil)
	// looks like this wait can fix some flaky failure
	// where API call is made before Gin server is ready
	time.Sleep(time.Millisecond * 100)

	resultCode = m.Run()
	fmt.Println("finished running integ test with status code", resultCode)
	rootCtxCancelFunc() // this will request shutdown the server
	err := shutdownFunc(rootCtx)
	if err != nil {
		fmt.Println("error when closing processOrm")
	}

}
