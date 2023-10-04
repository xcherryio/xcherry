package persistence

import "github.com/xdblab/xdb-apis/goapi/xdbapi"

type (
	StartProcessRequest struct {
		Request xdbapi.ProcessExecutionStartRequest
	}

	StartProcessResponse struct {
		ProcessExecutionId string
		AlreadyStarted     bool
	}

	DescribeLatestProcessRequest struct {
		Namespace string
		ProcessId string
	}

	DescribeLatestProcessResponse struct {
		Response  *xdbapi.ProcessExecutionDescribeResponse
		NotExists bool
	}
)
