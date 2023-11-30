// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: BUSL-1.1

package data_models

import (
	"encoding/json"
	"github.com/xcherryio/apis/goapi/xcapi"
)

type AsyncStateExecutionInfoJson struct {
	Namespace                   string                         `json:"namespace"`
	ProcessId                   string                         `json:"processId"`
	ProcessType                 string                         `json:"processType"`
	WorkerURL                   string                         `json:"workerURL"`
	StateConfig                 *xcapi.AsyncStateConfig        `json:"stateConfig"`
	RecoverFromStateExecutionId *string                        `json:"recoverFromStateExecutionId,omitempty"`
	RecoverFromApi              *xcapi.StateApiType            `json:"recoverFromApi,omitempty"`
	GlobalAttributeConfig       *InternalGlobalAttributeConfig `json:"globalAttributeConfig"`
	LocalAttributeConfig        *InternalLocalAttributeConfig  `json:"localAttributeConfig"`
}

func FromStartRequestToStateInfoBytes(req xcapi.ProcessExecutionStartRequest) ([]byte, error) {
	infoJson := AsyncStateExecutionInfoJson{
		Namespace:             req.Namespace,
		ProcessId:             req.ProcessId,
		ProcessType:           req.GetProcessType(),
		WorkerURL:             req.GetWorkerUrl(),
		StateConfig:           req.StartStateConfig,
		GlobalAttributeConfig: getInternalGlobalAttributeConfig(req),
		LocalAttributeConfig:  getInternalLocalAttributeConfig(req),
	}

	return infoJson.ToBytes()
}

func FromAsyncStateExecutionInfoToBytesForNextState(
	info AsyncStateExecutionInfoJson,
	nextStateConfig *xcapi.AsyncStateConfig,
) ([]byte, error) {
	info.StateConfig = nextStateConfig
	return json.Marshal(info)
}

func (j *AsyncStateExecutionInfoJson) ToBytes() ([]byte, error) {
	return json.Marshal(j)
}

func FromAsyncStateExecutionInfoToBytesForStateRecovery(
	info AsyncStateExecutionInfoJson,
	stateExeId string,
	api xcapi.StateApiType,
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
