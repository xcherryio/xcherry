// Copyright 2023 XDBLab organization

// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

package utils

import (
	"fmt"
	"net/http"

	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/common/log/tag"
)

func CheckDecision(decision xdbapi.StateDecision) error {
	if decision.HasThreadCloseDecision() && len(decision.GetNextStates()) > 0 {
		return fmt.Errorf("cannot have both thread decision and next states")
	}
	return nil
}

func CheckHttpResponseAndError(err error, httpResp *http.Response, logger log.Logger) bool {
	status := 0
	if httpResp != nil {
		status = httpResp.StatusCode
	}
	logger.Debug("check http response and error", tag.Error(err), tag.StatusCode(status))

	if err != nil || (httpResp != nil && httpResp.StatusCode != http.StatusOK) {
		return true
	}
	return false
}
