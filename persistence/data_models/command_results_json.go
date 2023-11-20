// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: BUSL-1.1

package data_models

import (
	"encoding/json"
)

type CommandResultsJson struct {
	// if value is true, the timer was fired. Otherwise, the timer was skipped.
	TimerResults      map[int]bool                            `json:"timerResults"`
	LocalQueueResults map[int][]xcapi.LocalQueueMessageResult `json:"localQueueResults"`
}

func NewCommandResultsJson() CommandResultsJson {
	return CommandResultsJson{
		TimerResults:      map[int]bool{},
		LocalQueueResults: map[int][]xcapi.LocalQueueMessageResult{},
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
