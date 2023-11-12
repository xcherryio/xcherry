// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

package persistence

import (
	"encoding/json"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
)

type CommandResultsJson struct {
	// if value is true, the timer was fired. Otherwise, the timer was skipped.
	TimerResults      map[int]bool                             `json:"timerResults"`
	LocalQueueResults map[int][]xdbapi.LocalQueueMessageResult `json:"localQueueResults"`
}

func NewCommandResultsJson() CommandResultsJson {
	return CommandResultsJson{
		TimerResults:      map[int]bool{},
		LocalQueueResults: map[int][]xdbapi.LocalQueueMessageResult{},
	}
}

func FromCommandResultsJsonToBytes(result CommandResultsJson) ([]byte, error) {
	return json.Marshal(result)
}

func BytesToCommandResultsJson(bytes []byte) (CommandResultsJson, error) {
	var result CommandResultsJson
	err := json.Unmarshal(bytes, &result)
	return result, err
}
