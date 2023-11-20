// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: BUSL-1.1

package api

type ErrorWithStatus struct {
	StatusCode int
	Error      xcapi.ApiErrorResponse
}

func NewErrorWithStatus(code int, details string) *ErrorWithStatus {
	return &ErrorWithStatus{
		StatusCode: code,
		Error: xcapi.ApiErrorResponse{
			Detail: &details,
		},
	}
}

func NewErrorResponseWithStatus(code int, error xcapi.ApiErrorResponse) *ErrorWithStatus {
	return &ErrorWithStatus{
		StatusCode: code,
		Error:      error,
	}
}
