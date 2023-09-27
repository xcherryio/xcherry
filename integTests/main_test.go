package integTests

import (
	"flag"
	"fmt"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/extensions/postgres"
	"testing"
	"time"
)

var testDatabaseName = fmt.Sprintf("tst%v", time.Now().UnixNano())

func TestMain(m *testing.M) {
	fmt.Println("start running integ test")

	flag.Parse()
	if *postgresIntegTest {
		sqlConfig := &config.SQL{
			ConnectAddr:     "0.0.0.0:5432",
			User:            "xdb",
			Password:        "xdbxdb",
			DBExtensionName: postgres.ExtensionName,
		}
		err := extensions.CreateDatabase(sqlConfig, testDatabaseName)
		if err != nil {
			panic(err)
		}
		defer func() {
			err := extensions.DropDatabase(sqlConfig, testDatabaseName)
			if err != nil {
				panic(err)
			}
			fmt.Println("test database deleted:", testDatabaseName)
		}()
		fmt.Println("test database created:", testDatabaseName)
	}

	code := m.Run()
	fmt.Println("finished running integ test with status code", code)
}
