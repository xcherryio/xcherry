// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: BUSL-1.1

package data_models

import (
	"encoding/json"
	"github.com/xcherryio/apis/goapi/xcapi"
)

type ProcessExecutionInfoJson struct {
	ProcessType       string                     `json:"processType"`
	WorkerURL         string                     `json:"workerURL"`
	AppDatabaseConfig *InternalAppDatabaseConfig `json:"appDatabaseConfig"`
}

func FromStartRequestToProcessInfoBytes(req xcapi.ProcessExecutionStartRequest) ([]byte, error) {
	info := ProcessExecutionInfoJson{
		ProcessType:       req.GetProcessType(),
		WorkerURL:         req.GetWorkerUrl(),
		AppDatabaseConfig: getInternalAppDatabaseConfig(req),
	}
	return json.Marshal(info)
}

func BytesToProcessExecutionInfo(bytes []byte) (ProcessExecutionInfoJson, error) {
	var info ProcessExecutionInfoJson
	err := json.Unmarshal(bytes, &info)
	return info, err
}
