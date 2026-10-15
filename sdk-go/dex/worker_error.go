// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package dex

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/superdurable/dex/sdk-go/gen/dexpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxWorkerErrorDetailBytes = 1024
	maxWorkerErrorTypeBytes   = 256
	maxWorkerStackTraceBytes  = 4 * 1024
)

var (
	errorDetailTruncationMarker = []byte("\n... error detail truncated by Dex Go SDK ...")
	errorTypeTruncationMarker   = []byte("\n... error type truncated by Dex Go SDK ...")
	stackTraceTruncationMarker  = []byte("\n... stack trace truncated by Dex Go SDK ...")
)

type workerFailure struct {
	code  codes.Code
	cause error
}

func newWorkerFailure(code codes.Code, cause error) error {
	return &workerFailure{code: code, cause: cause}
}

func (failure *workerFailure) Error() string {
	return failure.cause.Error()
}

func (failure *workerFailure) Unwrap() error {
	return failure.cause
}

func finishWorkerCall(logger Logger, recovered any, err error) error {
	if recovered != nil {
		logger.Error(
			"Worker handler panic",
			"panic", recovered,
			"stack", string(debug.Stack()),
		)
		panicErr := fmt.Errorf("panic: %v", recovered)
		return workerStatusError(
			logger,
			codes.Internal,
			panicErr,
			fmt.Sprintf("%T", recovered),
			stackTraceFromPanic(recovered),
			nil,
			panicErr.Error(),
		)
	}
	if err == nil {
		return nil
	}

	reported, retryAfter := reportedWorkerFailure(err)
	code, classified := classifyWorkerError(reported)
	detail := classified.Error()
	if rpcStatus, ok := status.FromError(classified); ok {
		detail = rpcStatus.Message()
	}
	return workerStatusError(
		logger,
		code,
		classified,
		workerErrorType(reported),
		stackTraceFromError(classified),
		retryAfter,
		detail,
	)
}

func workerErrorType(reported error) string {
	var failure *workerFailure
	if errors.As(reported, &failure) {
		return fmt.Sprintf("%T", failure.cause)
	}
	return fmt.Sprintf("%T", reported)
}

func reportedWorkerFailure(err error) (error, *RetryAfterError) {
	var retryAfter *RetryAfterError
	if errors.As(err, &retryAfter) {
		return retryAfter.Cause, retryAfter
	}
	return err, nil
}

func classifyWorkerError(err error) (codes.Code, error) {
	var failure *workerFailure
	if errors.As(err, &failure) {
		return failure.code, failure.cause
	}
	if errors.Is(err, context.Canceled) {
		return codes.Canceled, err
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return codes.DeadlineExceeded, err
	}
	if rpcStatus, ok := status.FromError(err); ok {
		return rpcStatus.Code(), err
	}
	return codes.Unknown, err
}

func workerStatusError(
	logger Logger,
	code codes.Code,
	reported error,
	errorType string,
	stackTrace string,
	retryAfter *RetryAfterError,
	detail string,
) error {
	workerError := &dexpb.WorkerErrorResponse{
		Detail:     truncateWorkerFailureField(detail, maxWorkerErrorDetailBytes, errorDetailTruncationMarker),
		ErrorType:  truncateWorkerFailureField(errorType, maxWorkerErrorTypeBytes, errorTypeTruncationMarker),
		StackTrace: truncateWorkerFailureField(stackTrace, maxWorkerStackTraceBytes, stackTraceTruncationMarker),
		GrpcStatus: int32(code),
	}
	if retryAfter != nil {
		workerError.RetryAfterSeconds = int32(retryAfter.After / time.Second)
	}
	rpcStatus := status.New(code, workerError.GetDetail())
	withDetails, err := rpcStatus.WithDetails(workerError)
	if err != nil {
		logger.Error("attach Worker error details", "error", err)
		return rpcStatus.Err()
	}
	return withDetails.Err()
}

func stackTraceFromError(cause error) string {
	var withStack *errorWithStack
	if errors.As(cause, &withStack) {
		return withStack.stackTrace()
	}
	// Plain errors have no origin stack. Do not capture the Worker wrap site;
	// that frame is not useful for diagnosing application failures.
	return "no application stack captured; wrap with dex.ErrorWithStack(err) at the failure site"
}

func stackTraceFromPanic(recovered any) string {
	return fmt.Sprintf("panic: %v\n%s", recovered, debug.Stack())
}

func truncateWorkerFailureField(value string, maximumBytes int, truncationMarker []byte) string {
	encoded := []byte(value)
	if len(encoded) <= maximumBytes {
		return value
	}
	prefixLength := maximumBytes - len(truncationMarker)
	for prefixLength > 0 && encoded[prefixLength]&0xc0 == 0x80 {
		prefixLength--
	}
	return string(encoded[:prefixLength]) + string(truncationMarker)
}
