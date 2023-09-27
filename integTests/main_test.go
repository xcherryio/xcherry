package integTests

import (
	"flag"
	"testing"
)

var postgresIntegTest = flag.Bool("postgres", false, "run integ test against Postgres")

func TestMain(m *testing.M) {
	flag.Parse()
	if *postgresIntegTest {
		
	}
}
