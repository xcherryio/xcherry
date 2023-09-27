package integTests

import (
	"flag"
)

var postgresIntegTest = flag.Bool("postgres", false, "run integ test against Postgres")
