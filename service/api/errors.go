// Copyright (c) XDBLab
// SPDX-License-Identifier: BUSL-1.1

package api

import "github.com/xdblab/xdb-apis/goapi/xdbapi"

type ErrorWithStatus struct {
	StatusCode int
	Error      xdbapi.ApiErrorResponse
}

func NewErrorWithStatus(code int, details string) *ErrorWithStatus {
	return &ErrorWithStatus{
		StatusCode: code,
		Error: xdbapi.ApiErrorResponse{
			Detail: &details,
		},
	}
}

func NewErrorResponseWithStatus(code int, error xdbapi.ApiErrorResponse) *ErrorWithStatus {
	return &ErrorWithStatus{
		StatusCode: code,
		Error:      error,
	}
}
