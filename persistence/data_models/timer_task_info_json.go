// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

import (
	"encoding/json"
)

type TimerTaskInfoJson struct {
	WorkerTaskBackoffInfo *WorkerTaskBackoffInfoJson `json:"workerTaskBackoffInfo"`
	WorkerTaskType        *ImmediateTaskType         `json:"workerTaskType"`
	TimerCommandIndex     int                        `json:"timerCommandIndex"`
}

func (s *TimerTaskInfoJson) ToBytes() ([]byte, error) {
	return json.Marshal(s)
}

func BytesToTimerTaskInfo(bytes []byte) (TimerTaskInfoJson, error) {
	var obj TimerTaskInfoJson
	err := json.Unmarshal(bytes, &obj)
	return obj, err
}

func CreateTimerTaskInfoBytes(backoff *WorkerTaskBackoffInfoJson, taskType *ImmediateTaskType) ([]byte, error) {
	obj := TimerTaskInfoJson{
		WorkerTaskBackoffInfo: backoff,
		WorkerTaskType:        taskType,
	}
	return obj.ToBytes()
}
