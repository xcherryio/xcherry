package integTests

import (
	"flag"
	"fmt"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/extensions/postgres"
	"github.com/xdblab/xdb/extensions/postgres/postgrestool"
	"testing"
	"time"
)

var testDatabaseName = fmt.Sprintf("tst%v", time.Now().UnixNano())

func TestMain(m *testing.M) {
	flag.Parse()
	fmt.Printf("start running integ test, using database %v, postgres:%v \n", testDatabaseName, *postgresIntegTest)
	var resultCode int

	if *postgresIntegTest {
		sqlConfig := &config.SQL{
			ConnectAddr:     fmt.Sprintf("%v:%v", postgrestool.DefaultEndpoint, postgrestool.DefaultPort),
			User:            postgrestool.DefaultUserName,
			Password:        postgrestool.DefaultPassword,
			DBExtensionName: postgres.ExtensionName,
			DatabaseName:    testDatabaseName,
		}
		err := extensions.CreateDatabase(*sqlConfig, testDatabaseName)
		if err != nil {
			panic(err)
		}
		err = extensions.SetupSchema(sqlConfig, "../"+postgrestool.DefaultSchemaFilePath)
		if err != nil {
			panic(err)
		}
		defer func() {
			if *keepDatabaseForDebugWhenTestFails && resultCode != 0 {
				return
			}
			err := extensions.DropDatabase(*sqlConfig, testDatabaseName)
			if err != nil {
				panic(err)
			}
			fmt.Println("test database deleted:", testDatabaseName)
		}()
		fmt.Println("test database created:", testDatabaseName)
	}

	resultCode = m.Run()
	fmt.Println("finished running integ test with status code", resultCode)
}
