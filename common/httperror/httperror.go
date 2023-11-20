// Copyright 2023 xCherryIO organization

// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: BUSL-1.1

package httperror

import (
	"github.com/xcherryio/xcherry/common/log"
	"github.com/xcherryio/xcherry/common/log/tag"
	"net/http"
)

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
