// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

import (
	"fmt"
	"strconv"
	"strings"
)

type StateExecutionId struct {
	StateId         string
	StateIdSequence int32
}

func (s StateExecutionId) GetStateExecutionId() string {
	return fmt.Sprintf("%v-%v", s.StateId, s.StateIdSequence)
}

func NewStateExecutionIdFromString(s string) (*StateExecutionId, error) {
	lastHyphenIndex := strings.LastIndex(s, "-")
	if lastHyphenIndex == -1 {
		return nil, fmt.Errorf("invalid format: %s", s)
	}

	stateId := s[:lastHyphenIndex]
	stateIdSequence, err := strconv.ParseInt(s[lastHyphenIndex+1:], 10, 32)
	if err != nil {
		return nil, err
	}

	return &StateExecutionId{StateId: stateId, StateIdSequence: int32(stateIdSequence)}, nil
}
