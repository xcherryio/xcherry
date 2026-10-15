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
	"google.golang.org/protobuf/proto"
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
	if input.GetRequest() == nil {
		panic("step activity request required")
	}
	fallbackInput := proto.Clone(input).(*dexpb.InvokeWaitForMethodActivityInput)
	fallbackInput.Request.Context = stepActivityContextForFallback(
		fallbackInput.Request.GetContext(), previousAttempts, firstAttemptTimestamp,
	)
	return fallbackInput
}

func executeActivityInputWithAttemptContext(
	input *dexpb.InvokeExecuteMethodActivityInput,
	previousAttempts int32,
	firstAttemptTimestamp int64,
) *dexpb.InvokeExecuteMethodActivityInput {
	if input.GetRequest() == nil {
		panic("step activity request required")
	}
	fallbackInput := proto.Clone(input).(*dexpb.InvokeExecuteMethodActivityInput)
	fallbackInput.Request.Context = stepActivityContextForFallback(
		fallbackInput.Request.GetContext(), previousAttempts, firstAttemptTimestamp,
	)
	return fallbackInput
}

func stepActivityContextForFallback(
	workerContext *dexpb.Context,
	previousAttempts int32,
	firstAttemptTimestamp int64,
) *dexpb.Context {
	if workerContext == nil {
		panic("step activity Context required")
	}
	workerContext.FirstAttemptTimestamp = firstAttemptTimestamp
	workerContext.Attempt = previousAttempts
	return workerContext
}
