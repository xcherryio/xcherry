package integTests

import (
	"fmt"
	"github.com/xdblab/xdb-golang-sdk/integTests/basic"
	"testing"
	"time"
)

func TestStartBasicProcess(t *testing.T) {
	basic.TestStartIOProcess(t, client)

	fmt.Println("waiting for messages")
	time.Sleep(time.Second * 10)
}
