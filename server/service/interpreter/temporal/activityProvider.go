// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package temporal

import (
	"context"
	"time"

	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/interpreter/interfaces"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

type activityProvider struct{}

func (a *activityProvider) GetLogger(ctx context.Context) interfaces.UnifiedLogger {
	return activity.GetLogger(ctx)
}

func (a *activityProvider) NewSubFlowStartRejection(detail string) error {
	return temporal.NewApplicationError("", interfaces.SubFlowStartRejectionErrorType, detail)
}

func (a *activityProvider) NewActivityError(
	errType dexpb.FlowErrorType,
	activityError *dexpb.InternalActivityError,
	retryAfterSeconds int32,
) error {
	return temporal.NewApplicationErrorWithOptions(
		"",
		errType.String(),
		temporal.ApplicationErrorOptions{
			Details:        []interface{}{activityError},
			NextRetryDelay: time.Duration(retryAfterSeconds) * time.Second,
		},
	)
}

func (a *activityProvider) NewNonRetryableActivityError(
	errType dexpb.FlowErrorType,
	activityError *dexpb.InternalActivityError,
) error {
	return temporal.NewNonRetryableApplicationError("", errType.String(), nil, activityError)
}

func (a *activityProvider) NewLocalActivityError(
	errType dexpb.FlowErrorType,
	failure *dexpb.InternalLocalStepActivityFailure,
	retryAfterSeconds int32,
) error {
	return temporal.NewApplicationErrorWithOptions(
		"",
		errType.String(),
		temporal.ApplicationErrorOptions{
			Details:        []interface{}{failure},
			NextRetryDelay: time.Duration(retryAfterSeconds) * time.Second,
		},
	)
}

func (a *activityProvider) GetActivityInfo(ctx context.Context) interfaces.ActivityInfo {
	info := activity.GetInfo(ctx)
	return interfaces.ActivityInfo{
		ScheduledTime:    info.ScheduledTime,
		ActivityID:       info.ActivityID,
		Attempt:          info.Attempt,
		IsLocalActivity:  info.IsLocalActivity,
		HeartbeatTimeout: info.HeartbeatTimeout,
		WorkflowExecution: interfaces.WorkflowExecution{
			ID:    info.WorkflowExecution.ID,
			RunID: info.WorkflowExecution.RunID,
		},
	}
}

func (a *activityProvider) GetHeartbeatValue(ctx context.Context) (*dexpb.Value, error) {
	if !activity.HasHeartbeatDetails(ctx) {
		return nil, nil
	}
	value := &dexpb.Value{}
	if err := activity.GetHeartbeatDetails(ctx, value); err != nil {
		return nil, err
	}
	return value, nil
}

func (a *activityProvider) RecordHeartbeat(ctx context.Context, details ...interface{}) {
	activity.RecordHeartbeat(ctx, details...)
}
