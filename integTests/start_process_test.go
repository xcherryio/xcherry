package integTests

import (
	"github.com/xdblab/xdb-golang-sdk/integTests/basic"
	"testing"
)

func TestStartBasicProcess(t *testing.T) {
	basic.TestStartIOProcess(t, client)
}
