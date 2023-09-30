package integTests

import (
	"fmt"
	"testing"
	"time"

	"github.com/xdblab/xdb-golang-sdk/integTests/worker"
)

func TestMain(m *testing.M) {
	worker.StartGinWorker(workerService)

	// looks like this wait can fix some flaky failure
	// where API call is made before Gin server is ready
	time.Sleep(time.Millisecond * 100)

	resultCode := m.Run()
	fmt.Println("finished running integ test with status code", resultCode)
}
