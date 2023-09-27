package integTests

import (
	"flag"
	"fmt"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/extensions"
	"testing"
	"time"
)

var postgresIntegTest = flag.Bool("postgres", false, "run integ test against Postgres")

var testDatabaseName = fmt.Sprintf("tst-%v", time.Now().Nanosecond())

func TestMain(m *testing.M) {
	flag.Parse()
	if *postgresIntegTest {
		err := extensions.CreateDatabase(&config.SQL{
			User:            "xdb",
			Password:        "xdbxdb",
			DBExtensionName: testDatabaseName,
		}, testDatabaseName)
		if err != nil {
			panic(err)
		}
		fmt.Println("test database created:", testDatabaseName)
	}
}
