// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

import "encoding/json"

type PaginationToken struct {
	LastProcessExecutionId string `json:"lastProcessExecutionId"`
	LastStartTime          int64  `json:"lastStartTime"`
}

func NewPaginationToken(lastProcessExecutionId string, lastStartTime int64) *PaginationToken {
	return &PaginationToken{
		LastProcessExecutionId: lastProcessExecutionId,
		LastStartTime:          lastStartTime,
	}
}

func (pt *PaginationToken) String() (string, error) {
	b, err := json.Marshal(pt)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func ParsePaginationTokenFromString(token string) (*PaginationToken, error) {
	var pt PaginationToken
	err := json.Unmarshal([]byte(token), &pt)
	if err != nil {
		return nil, err
	}
	return &pt, err
}
