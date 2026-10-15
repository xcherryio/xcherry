// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package interfaces

import (
	"github.com/superdurable/dex/gen/dexpb"
)

func IsPullStepActivityInput(input interface{}) bool {
	switch activityInput := input.(type) {
	case *dexpb.InvokeWaitForMethodActivityInput:
		return activityInput.GetWorkerTarget().GetIsPullMode()
	case *dexpb.InvokeExecuteMethodActivityInput:
		return activityInput.GetWorkerTarget().GetIsPullMode()
	default:
		return false
	}
}

func IsStepActivityInput(input interface{}) bool {
	switch input.(type) {
	case *dexpb.InvokeWaitForMethodActivityInput:
		return true
	case *dexpb.InvokeExecuteMethodActivityInput:
		return true
	default:
		return false
	}
}

func StepActivityInputWithAttemptContext(
	input interface{},
	previousAttempts int32,
	firstAttemptTimestamp int64,
) interface{} {
	switch activityInput := input.(type) {
	case *dexpb.InvokeWaitForMethodActivityInput:
		return waitForActivityInputWithAttemptContext(
			activityInput,
			previousAttempts,
			firstAttemptTimestamp,
		)
	case *dexpb.InvokeExecuteMethodActivityInput:
		return executeActivityInputWithAttemptContext(
			activityInput,
			previousAttempts,
			firstAttemptTimestamp,
		)
	default:
		panic("step activity input required")
	}
}

func waitForActivityInputWithAttemptContext(
	input *dexpb.InvokeWaitForMethodActivityInput,
	previousAttempts int32,
	firstAttemptTimestamp int64,
) *dexpb.InvokeWaitForMethodActivityInput {
	request := input.GetRequest()
	if request == nil {
		panic("step activity request required")
	}
	fallbackInput := *input
	fallbackRequest := *request
	fallbackRequest.Context = stepActivityContextForFallback(request.GetContext(), previousAttempts, firstAttemptTimestamp)
	fallbackInput.Request = &fallbackRequest
	return &fallbackInput
}

func executeActivityInputWithAttemptContext(
	input *dexpb.InvokeExecuteMethodActivityInput,
	previousAttempts int32,
	firstAttemptTimestamp int64,
) *dexpb.InvokeExecuteMethodActivityInput {
	request := input.GetRequest()
	if request == nil {
		panic("step activity request required")
	}
	fallbackInput := *input
	fallbackRequest := *request
	fallbackRequest.Context = stepActivityContextForFallback(request.GetContext(), previousAttempts, firstAttemptTimestamp)
	fallbackInput.Request = &fallbackRequest
	return &fallbackInput
}

func stepActivityContextForFallback(
	workerContext *dexpb.Context,
	previousAttempts int32,
	firstAttemptTimestamp int64,
) *dexpb.Context {
	if workerContext == nil {
		panic("step activity Context required")
	}
	fallbackContext := *workerContext
	fallbackContext.FirstAttemptTimestamp = firstAttemptTimestamp
	fallbackContext.Attempt = previousAttempts
	return &fallbackContext
}
