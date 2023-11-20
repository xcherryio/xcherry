// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

package data_models

import (
	"encoding/json"

	"github.com/xdblab/xdb-apis/goapi/xdbapi"
)

type ProcessExecutionInfoJson struct {
	ProcessType           string                         `json:"processType"`
	WorkerURL             string                         `json:"workerURL"`
	GlobalAttributeConfig *InternalGlobalAttributeConfig `json:"globalAttributeConfig"`
}

func FromStartRequestToProcessInfoBytes(req xdbapi.ProcessExecutionStartRequest) ([]byte, error) {
	info := ProcessExecutionInfoJson{
		ProcessType:           req.GetProcessType(),
		WorkerURL:             req.GetWorkerUrl(),
		GlobalAttributeConfig: getInternalGlobalAttributeConfig(req),
	}
	return json.Marshal(info)
}

func BytesToProcessExecutionInfo(bytes []byte) (ProcessExecutionInfoJson, error) {
	var info ProcessExecutionInfoJson
	err := json.Unmarshal(bytes, &info)
	return info, err
}
