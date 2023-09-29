package integTests

import (
	"github.com/xdblab/xdb-golang-sdk/integTests/basic"
	"testing"
)

func TestStartBasicProcessPostgres(t *testing.T) {
	if !*postgresIntegTest {
		t.Skip()
	}

	basic.TestStartIOProcess(t, client)
}