// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package interfaces

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/gen/dexpb"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestStepActivityInputWithAttemptContextPreservesDispatchFields(t *testing.T) {
	for _, input := range []interface{}{
		&dexpb.InvokeWaitForMethodActivityInput{
			Request:      &dexpb.InvokeWaitForMethodRequest{Context: &dexpb.Context{FlowId: "flow", Attempt: 1}},
			DexTaskQueue: "worker", DispatchWait: durationpb.New(10 * time.Second), StickyWait: durationpb.New(5 * time.Second),
		},
		&dexpb.InvokeExecuteMethodActivityInput{
			Request:      &dexpb.InvokeExecuteMethodRequest{Context: &dexpb.Context{FlowId: "flow", Attempt: 1}},
			DexTaskQueue: "worker", DispatchWait: durationpb.New(10 * time.Second), StickyWait: durationpb.New(5 * time.Second),
		},
	} {
		fallback := StepActivityInputWithAttemptContext(input, 2, 123)
		switch got := fallback.(type) {
		case *dexpb.InvokeWaitForMethodActivityInput:
			require.Equal(t, "worker", got.GetDexTaskQueue())
			require.Equal(t, 10*time.Second, got.GetDispatchWait().AsDuration())
			require.Equal(t, 5*time.Second, got.GetStickyWait().AsDuration())
			require.Equal(t, int32(2), got.GetRequest().GetContext().GetAttempt())
			require.Equal(t, int64(123), got.GetRequest().GetContext().GetFirstAttemptTimestamp())
			require.Equal(t, int32(1), input.(*dexpb.InvokeWaitForMethodActivityInput).GetRequest().GetContext().GetAttempt())
		case *dexpb.InvokeExecuteMethodActivityInput:
			require.Equal(t, "worker", got.GetDexTaskQueue())
			require.Equal(t, 10*time.Second, got.GetDispatchWait().AsDuration())
			require.Equal(t, 5*time.Second, got.GetStickyWait().AsDuration())
			require.Equal(t, int32(2), got.GetRequest().GetContext().GetAttempt())
			require.Equal(t, int64(123), got.GetRequest().GetContext().GetFirstAttemptTimestamp())
			require.Equal(t, int32(1), input.(*dexpb.InvokeExecuteMethodActivityInput).GetRequest().GetContext().GetAttempt())
		}
	}
}
