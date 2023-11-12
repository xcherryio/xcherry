// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

package data_models

import (
	"encoding/json"
	"github.com/xdblab/xdb/persistence"
)

type TimerTaskInfoJson struct {
	WorkerTaskBackoffInfo *WorkerTaskBackoffInfoJson     `json:"workerTaskBackoffInfo"`
	WorkerTaskType        *persistence.ImmediateTaskType `json:"workerTaskType"`
	TimerCommandIndex     int                            `json:"timerCommandIndex"`
}

func (s *TimerTaskInfoJson) ToBytes() ([]byte, error) {
	return json.Marshal(s)
}

func BytesToTimerTaskInfo(bytes []byte) (TimerTaskInfoJson, error) {
	var obj TimerTaskInfoJson
	err := json.Unmarshal(bytes, &obj)
	return obj, err
}

func CreateTimerTaskInfoBytes(backoff *WorkerTaskBackoffInfoJson, taskType *persistence.ImmediateTaskType) ([]byte, error) {
	obj := TimerTaskInfoJson{
		WorkerTaskBackoffInfo: backoff,
		WorkerTaskType:        taskType,
	}
	return obj.ToBytes()
}
