package integTests

import (
	"github.com/xdblab/xdb-golang-sdk/integTests/basic"
	"testing"
	"time"
)

func TestStartBasicProcess(t *testing.T) {
	basic.TestStartIOProcess(t, client)
	time.Sleep(time.Second * 500)
}
