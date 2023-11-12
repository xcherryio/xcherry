// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

package data_models

import (
	"encoding/json"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/persistence"
)

type AsyncStateExecutionInfoJson struct {
	Namespace                   string                                     `json:"namespace"`
	ProcessId                   string                                     `json:"processId"`
	ProcessType                 string                                     `json:"processType"`
	WorkerURL                   string                                     `json:"workerURL"`
	StateConfig                 *xdbapi.AsyncStateConfig                   `json:"stateConfig"`
	RecoverFromStateExecutionId *string                                    `json:"recoverFromStateExecutionId,omitempty"`
	RecoverFromApi              *xdbapi.StateApiType                       `json:"recoverFromApi,omitempty"`
	GlobalAttributeConfig       *persistence.InternalGlobalAttributeConfig `json:"globalAttributeConfig"`
}

func FromStartRequestToStateInfoBytes(req xdbapi.ProcessExecutionStartRequest) ([]byte, error) {

	return json.Marshal(AsyncStateExecutionInfoJson{
		Namespace:             req.Namespace,
		ProcessId:             req.ProcessId,
		ProcessType:           req.GetProcessType(),
		WorkerURL:             req.GetWorkerUrl(),
		StateConfig:           req.StartStateConfig,
		GlobalAttributeConfig: getInternalGlobalAttributeConfig(req),
	})
}

func FromAsyncStateExecutionInfoToBytesForNextState(
	info AsyncStateExecutionInfoJson,
	nextStateConfig *xdbapi.AsyncStateConfig,
) ([]byte, error) {
	info.StateConfig = nextStateConfig
	return json.Marshal(info)
}

func FromAsyncStateExecutionInfoToBytesForStateRecovery(
	info AsyncStateExecutionInfoJson,
	stateExeId string,
	api xdbapi.StateApiType,
) ([]byte, error) {
	info.RecoverFromStateExecutionId = &stateExeId
	info.RecoverFromApi = &api
	// TODO we need to clean up for the next state execution otherwise it will be carried over forever
	return json.Marshal(info)
}

func BytesToAsyncStateExecutionInfo(bytes []byte) (AsyncStateExecutionInfoJson, error) {
	var info AsyncStateExecutionInfoJson
	err := json.Unmarshal(bytes, &info)
	return info, err
}
