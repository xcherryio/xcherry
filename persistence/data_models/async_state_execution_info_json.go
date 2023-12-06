// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: BUSL-1.1

package data_models

import (
	"encoding/json"
	"github.com/xcherryio/apis/goapi/xcapi"
)

type AsyncStateExecutionInfoJson struct {
	Namespace                   string                     `json:"namespace"`
	ProcessId                   string                     `json:"processId"`
	ProcessType                 string                     `json:"processType"`
	WorkerURL                   string                     `json:"workerURL"`
	StateConfig                 *xcapi.AsyncStateConfig    `json:"stateConfig"`
	RecoverFromStateExecutionId *string                    `json:"recoverFromStateExecutionId,omitempty"`
	RecoverFromApi              *xcapi.WorkerApiType       `json:"recoverFromApi,omitempty"`
	AppDatabaseConfig           *InternalAppDatabaseConfig `json:"appDatabaseConfig"`
}

func FromStartRequestToStateInfoBytes(req xcapi.ProcessExecutionStartRequest) ([]byte, error) {
	infoJson := AsyncStateExecutionInfoJson{
		Namespace:         req.Namespace,
		ProcessId:         req.ProcessId,
		ProcessType:       req.GetProcessType(),
		WorkerURL:         req.GetWorkerUrl(),
		StateConfig:       req.StartStateConfig,
		AppDatabaseConfig: getInternalAppDatabaseConfig(req),
	}

	return infoJson.ToBytes()
}

func (j *AsyncStateExecutionInfoJson) ToBytes() ([]byte, error) {
	return json.Marshal(j)
}

func FromAsyncStateExecutionInfoToBytesForStateRecovery(
	info AsyncStateExecutionInfoJson,
	stateExeId string,
	api xcapi.WorkerApiType,
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
