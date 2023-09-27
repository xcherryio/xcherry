package persistence

import "github.com/xdblab/xdb-apis/goapi/xdbapi"

type ProcessORM interface {
	StartProcess(request xdbapi.ProcessExecutionStartRequest) (xdbapi.ProcessExecutionStartResponse, error)
}

type ProcessMQ interface {
	// TODO
}
