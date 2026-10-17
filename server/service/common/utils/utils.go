// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package utils

import (
	"context"
	"net/http"
	"time"

	"github.com/superdurable/dex/gen/dexpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultMaxApiTimeoutSeconds = 60
)

// ShallowCopyMessage copies every populated top-level field, sharing nested values with message.
func ShallowCopyMessage[T proto.Message](message T) T {
	source := message.ProtoReflect()
	copied := source.New()
	source.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		copied.Set(field, value)
		return true
	})
	return copied.Interface().(T)
}

func IsNullValue(value *dexpb.Value) bool {
	if value == nil {
		return true
	}
	_, ok := value.GetKind().(*dexpb.Value_NullValue)
	return ok
}

func MergeStringSlice(first, second []string) []string {
	exists := map[string]bool{}
	var out []string
	for _, k := range first {
		if !exists[k] {
			exists[k] = true
			out = append(out, k)
		}
	}
	for _, k := range second {
		if !exists[k] {
			exists[k] = true
			out = append(out, k)
		}
	}
	return out
}

func MergeMap(first map[string]interface{}, second map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(first))
	for k, v := range first {
		out[k] = v
	}

	for k, v := range second {
		out[k] = v
	}
	return out
}

func TrimRpcTimeoutSeconds(ctx context.Context, req *dexpb.InvokeRPCRequest) int32 {
	secondsRemaining := int32(defaultMaxApiTimeoutSeconds)
	ddl, ok := ctx.Deadline()
	if ok {
		timeRemaining := ddl.Sub(time.Now())
		if int32(timeRemaining.Seconds()) < secondsRemaining {
			secondsRemaining = int32(timeRemaining.Seconds())
		}
	}
	if req != nil && req.GetTimeoutSeconds() > 0 && req.GetTimeoutSeconds() < secondsRemaining {
		secondsRemaining = req.GetTimeoutSeconds()
	}
	return secondsRemaining
}

func TrimContextByTimeoutWithCappedDDL(parent context.Context, reqWaitSeconds *int32, configuredMaxSeconds int64) (context.Context, context.CancelFunc) {
	maxWaitSeconds := configuredMaxSeconds
	if maxWaitSeconds == 0 {
		maxWaitSeconds = defaultMaxApiTimeoutSeconds
	}

	if reqWaitSeconds != nil && *reqWaitSeconds > 0 && int64(*reqWaitSeconds) < maxWaitSeconds {
		maxWaitSeconds = int64(*reqWaitSeconds)
	}

	// Preserve sub-second precision so short waits are not truncated near second boundaries.
	maxWaitDeadline := time.Now().Add(time.Duration(maxWaitSeconds) * time.Second)

	// then capped by context
	ddl, ok := parent.Deadline()
	if ok && ddl.Before(maxWaitDeadline) {
		maxWaitDeadline = ddl
	}

	return context.WithDeadline(parent, maxWaitDeadline)
}

func CheckHttpError(err error, httpResp *http.Response) bool {
	if err != nil || (httpResp != nil && httpResp.StatusCode != http.StatusOK) {
		return true
	}
	return false
}

func ToNanoSeconds(e *timestamppb.Timestamp) int64 {
	return e.GetSeconds()*1000*1000*1000 + int64(e.GetNanos())
}
