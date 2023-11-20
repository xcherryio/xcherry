// Copyright 2023 XDBLab organization

// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

package decision

import (
	"fmt"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
)

func ValidateDecision(decision xdbapi.StateDecision) error {
	if decision.HasThreadCloseDecision() && len(decision.GetNextStates()) > 0 {
		return fmt.Errorf("cannot have both thread decision and next states")
	}
	return nil
}
