package integTests

import (
	"fmt"
	"github.com/xdblab/xdb-golang-sdk/integTests/basic"
	"testing"
	"time"
)

func TestStartBasicProcessPostgres(t *testing.T) {
	if !*postgresIntegTest {
		t.Skip()
	}

	basic.TestStartIOProcess(t, client)

	fmt.Println("waiting for messages")
	time.Sleep(time.Second * 10)
}
