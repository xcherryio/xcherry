package integTests

import (
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
		} else {
			fmt.Println("wait for 5 seconds so that topics for new tables are created")
			time.Sleep(time.Second * 5)
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
			Address: ":" + xdb.DefaultServerPort,
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
	stopF := bootstrap.StartXdbServer(&cfg, nil)
	// TODO not sure this can fix some flaky failure on Github CI
	// wait for server to be ready ...
	time.Sleep(time.Millisecond * 100)

	resultCode = m.Run()
	fmt.Println("finished running integ test with status code", resultCode)
	err := stopF()
	if err != nil {
		fmt.Println("error when closing processOrm")
	}

}
