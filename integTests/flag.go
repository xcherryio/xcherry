package integTests

import (
	"flag"
)

var postgresIntegTest = flag.Bool("postgres", false, "run integ test against Postgres")
var keepDatabaseForDebugWhenTestFails = flag.Bool("keepDatabaseForDebugWhenTestFails", false, "set to true so that the testing database is not deleted after test fails")
