// Copyright 2023 XDBLab organization

// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

package httperror

import (
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/common/log/tag"
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
