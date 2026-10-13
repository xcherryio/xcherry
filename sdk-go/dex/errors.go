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
	"errors"
	"fmt"

	"github.com/superdurable/dex/sdk-go/gen/dexpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	errInvalidInvocationContext = errors.New("dex: invalid invocation context")
)

// AttributeMapNotLoadedError reports an AttributeMap entry read omitted by invocation options.
type AttributeMapNotLoadedError struct {
	// Name is the AttributeMap name or its physical instance name.
	Name string
}

// Error describes the missing AttributeMap load.
func (e *AttributeMapNotLoadedError) Error() string {
	return fmt.Sprintf("dex: AttributeMap %q was not loaded for this invocation", e.Name)
}

// ChannelMessagesNotLoadedError reports pending messages omitted by invocation options.
type ChannelMessagesNotLoadedError struct {
	// Name is the Channel or ChannelMap physical name.
	Name string
}

// Error describes the missing pending-message load.
func (e *ChannelMessagesNotLoadedError) Error() string {
	return fmt.Sprintf("dex: Channel messages for %q were not loaded for this invocation", e.Name)
}

// AttributeNotFoundError reports a missing invocation attribute.
type AttributeNotFoundError struct {
	// AttributeName is the stable Attribute or AttributeMap name.
	AttributeName string
	// Instance is the missing map instance, or empty for a single Attribute.
	Instance string
}

// FlowDefinitionError reports an invalid or unregistered Flow definition.
type FlowDefinitionError struct {
	// FlowType identifies the invalid or unregistered Flow when known.
	FlowType string
	// Definition identifies the invalid Step, RPC, Attribute, or Channel when known.
	Definition string
	// Err preserves the developer-actionable contract violation.
	Err error
}

// Error formats the Flow definition and its underlying contract violation.
func (e *FlowDefinitionError) Error() string {
	if e == nil {
		return "<nil>"
	}
	prefix := "dex: flow definition"
	if e.FlowType != "" {
		prefix = fmt.Sprintf("dex: flow %q definition", e.FlowType)
	}
	if e.Definition != "" {
		prefix += " " + e.Definition
	}
	if e.Err == nil {
		return prefix
	}
	return prefix + ": " + e.Err.Error()
}

// Unwrap returns the underlying definition violation.
func (e *FlowDefinitionError) Unwrap() error {
	return e.Err
}

// InvalidStepResultError reports an invalid Worker handler result.
type InvalidStepResultError struct {
	// FlowType identifies the containing Flow.
	FlowType string
	// StepType identifies the Step, or empty for an RPC.
	StepType string
	// Method is WaitFor, Execute, or the RPC name.
	Method string
	// Err preserves the violated result contract.
	Err error
}

// Error formats the handler location and invalid-result detail.
func (e *InvalidStepResultError) Error() string {
	if e == nil {
		return "<nil>"
	}
	prefix := fmt.Sprintf("dex: flow %q", e.FlowType)
	if e.StepType != "" {
		prefix += fmt.Sprintf(" step %q", e.StepType)
	}
	if e.Method != "" {
		prefix += " " + e.Method
	}
	if e.Err == nil {
		return prefix + " returned an invalid result"
	}
	return prefix + ": " + e.Err.Error()
}

// Unwrap returns the underlying result-contract violation.
func (e *InvalidStepResultError) Unwrap() error {
	return e.Err
}

// ValueMappingError reports a Dex value conversion failure.
type ValueMappingError struct {
	// Operation describes whether encoding, decoding, indexing, or hydration failed.
	Operation string
	// Err preserves the serializer or type-conversion failure.
	Err error
}

// Error formats the value operation and underlying mapping failure.
func (e *ValueMappingError) Error() string {
	if e == nil {
		return "<nil>"
	}
	prefix := "dex: map value"
	if e.Operation != "" {
		prefix = "dex: " + e.Operation + " value"
	}
	if e.Err == nil {
		return prefix
	}
	return prefix + ": " + e.Err.Error()
}

// Unwrap returns the underlying value-mapping failure.
func (e *ValueMappingError) Unwrap() error {
	return e.Err
}

// Error identifies the missing Attribute or Attribute-map instance.
func (e *AttributeNotFoundError) Error() string {
	if e.Instance == "" {
		return fmt.Sprintf("dex: attribute %q not found", e.AttributeName)
	}
	return fmt.Sprintf(
		"dex: attribute map %q instance %q not found",
		e.AttributeName,
		e.Instance,
	)
}

// BlobMessageTooLargeError reports a blob value that alone exceeds a gRPC message size limit.
//
// Dex stores large Attribute, Channel, and RPC values as blobs and the SDK hydrates
// them with LoadBlobs. The SDK first requests every uncached blob in one LoadBlobs
// call. When that response exceeds a gRPC message size limit, the SDK splits the
// request in half and retries each half, so the batch size never needs tuning. This
// error means a single blob still does not fit in one response.
//
// The limit that rejected the response is either the SDK's GrpcMaxMessageBytes on
// ClientOptions or WorkerOptions, or the Dex server's api.grpcMaxMessageBytes. Err
// keeps gRPC's RESOURCE_EXHAUSTED status, whose message states the response size and
// the limit. Raise the smaller limit above the value size plus envelope headroom, or
// split the value across several Attributes or AttributeMap instances.
//
// A Client call returns this error wrapped in a ServiceError. A Worker reports it as an
// INTERNAL Step or RPC failure whose detail contains this message.
//
//	var tooLarge *dex.BlobMessageTooLargeError
//	if errors.As(err, &tooLarge) {
//		log.Printf("Flow %s blob %s exceeds the gRPC limit: %v",
//			tooLarge.FlowID, tooLarge.BlobID, tooLarge.Err)
//	}
type BlobMessageTooLargeError struct {
	// FlowID is the Flow ID that owns the blob.
	FlowID string
	// BlobID is the Dex blob ID of the value that did not fit.
	BlobID string
	// Err is the gRPC RESOURCE_EXHAUSTED status returned for the single-blob LoadBlobs call.
	Err error
}

// Error identifies the Flow ID, blob ID, and gRPC size failure.
func (e *BlobMessageTooLargeError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf(
		"dex: blob %q of Flow ID %q exceeds the gRPC message size limit: %v",
		e.BlobID,
		e.FlowID,
		e.Err,
	)
}

// Unwrap returns the gRPC RESOURCE_EXHAUSTED status.
func (e *BlobMessageTooLargeError) Unwrap() error {
	return e.Err
}

// ServiceError reports a FlowService failure.
type ServiceError struct {
	// Op is the SDK operation that failed.
	Op string
	// FlowID is the targeted Flow ID, or empty for service-wide operations.
	FlowID string
	// Code is the outer gRPC status code.
	Code codes.Code
	// SubStatus is Dex's specific error classification.
	SubStatus ErrorSubStatus
	// Detail is the most specific server or transport message available.
	Detail string
	cause  error
}

// Error formats operation, Flow identity, gRPC code, and detail.
func (e *ServiceError) Error() string {
	if e == nil {
		return "<nil>"
	}
	prefix := "dex"
	if e.Op != "" {
		prefix += ": " + e.Op
	}
	if e.FlowID != "" {
		prefix += fmt.Sprintf(" flow %q", e.FlowID)
	}
	if e.Detail == "" {
		return fmt.Sprintf("%s: %s", prefix, e.Code)
	}
	return fmt.Sprintf("%s: %s: %s", prefix, e.Code, e.Detail)
}

// Unwrap returns the original gRPC transport failure.
func (e *ServiceError) Unwrap() error {
	return e.cause
}

// FlowAlreadyStartedError reports a duplicate Flow start.
type FlowAlreadyStartedError struct {
	// ServiceError contains the failed StartFlow metadata.
	*ServiceError
}

// Unwrap returns the shared service failure.
func (e *FlowAlreadyStartedError) Unwrap() error {
	return e.ServiceError
}

// FlowNotFoundError reports a missing Flow for an existing-Flow operation.
type FlowNotFoundError struct {
	// ServiceError contains the failed existing-Flow operation metadata.
	*ServiceError
}

// Unwrap returns the shared service failure.
func (e *FlowNotFoundError) Unwrap() error {
	return e.ServiceError
}

// FlowNotActiveOrNotFoundError reports a missing Flow or an operation that cannot use a closed Flow.
// InvokeRPC also returns this error when a query-only RPC finds no readable execution.
// A query-only RPC without locks, transactionality, durable effects, or forced Update routing can
// read a retained terminal execution. At that confirmed read boundary, treat this error as not found.
// For mutations and active-only RPCs, this error does not distinguish missing from closed Flows
// or prove that the requested work succeeded. Other service and Worker failures remain distinct.
// Use errors.As to match the error and access its ServiceError metadata:
//
//	var unavailable *dex.FlowNotActiveOrNotFoundError
//	if errors.As(err, &unavailable) {
//		return unavailable
//	}
type FlowNotActiveOrNotFoundError struct {
	// ServiceError contains the failed operation's metadata and original transport cause.
	*ServiceError
}

// Unwrap returns the shared service failure.
func (e *FlowNotActiveOrNotFoundError) Unwrap() error {
	return e.ServiceError
}

// WorkerInvocationError reports a WorkerService or user-handler failure.
type WorkerInvocationError struct {
	// ServiceError contains the outer Dex service failure.
	*ServiceError
	// Worker preserves the original Worker-side status, type, and detail.
	Worker *WorkerError
}

// Unwrap returns the outer service failure.
func (e *WorkerInvocationError) Unwrap() error {
	return e.ServiceError
}

// RPCLockConflictError reports concurrent RPC lock contention.
type RPCLockConflictError struct {
	// ServiceError contains the aborted RPC metadata.
	*ServiceError
}

// Unwrap returns the shared service failure.
func (e *RPCLockConflictError) Unwrap() error {
	return e.ServiceError
}

// LongPollTimeoutError reports an expired server long poll.
type LongPollTimeoutError struct {
	// ServiceError contains the timed-out wait metadata.
	*ServiceError
}

// RequestTimeoutError reports that a caller-visible request budget expired.
type RequestTimeoutError struct {
	// ServiceError contains the timed-out wait metadata.
	*ServiceError
}

// ChannelMessageNotFoundError reports a pending message ID that no longer exists.
type ChannelMessageNotFoundError struct {
	// ServiceError contains the failed deletion metadata.
	*ServiceError
}

// Unwrap returns the shared service failure.
func (e *ChannelMessageNotFoundError) Unwrap() error {
	return e.ServiceError
}

// Unwrap returns the shared service failure.
func (e *LongPollTimeoutError) Unwrap() error {
	return e.ServiceError
}

// Unwrap returns the shared service failure.
func (e *RequestTimeoutError) Unwrap() error {
	return e.ServiceError
}

// WorkerError preserves the original WorkerService failure.
type WorkerError struct {
	// Code is the original Worker gRPC status.
	Code codes.Code
	// Type is the original Worker error or exception type.
	Type string
	// Detail is the original Worker-provided message.
	Detail string
}

// ErrorSubStatus is Dex's diagnostic service error classification.
type ErrorSubStatus uint8

const (
	// ErrorSubStatusUncategorized means Dex returned no specific status.
	ErrorSubStatusUncategorized ErrorSubStatus = iota + 1
	// ErrorSubStatusFlowAlreadyStarted identifies a Flow ID reuse conflict.
	ErrorSubStatusFlowAlreadyStarted
	// ErrorSubStatusFlowNotFound identifies an unknown or inactive Flow.
	ErrorSubStatusFlowNotFound
	// ErrorSubStatusWorkerAPI identifies a failed Step or RPC invocation.
	ErrorSubStatusWorkerAPI
	// ErrorSubStatusLongPollTimeout identifies an active wait whose deadline elapsed.
	ErrorSubStatusLongPollTimeout
	// ErrorSubStatusChannelMessageNotFound identifies a pending message that no longer exists.
	ErrorSubStatusChannelMessageNotFound
	// ErrorSubStatusRequestTimeout identifies an expired caller-visible request budget.
	ErrorSubStatusRequestTimeout
)

type flowTargetRequirement uint8

const (
	flowTargetNone flowTargetRequirement = iota
	flowTargetExisting
	flowTargetActive
)

func translateRPCError(
	err error,
	op string,
	flowID string,
	requirement flowTargetRequirement,
) error {
	if err == nil {
		return nil
	}
	rpcStatus, ok := status.FromError(err)
	if !ok {
		return err
	}
	serviceError := &ServiceError{
		Op:        op,
		FlowID:    flowID,
		Code:      rpcStatus.Code(),
		SubStatus: ErrorSubStatusUncategorized,
		Detail:    rpcStatus.Message(),
		cause:     err,
	}
	var response *dexpb.ServiceErrorResponse
	for _, detail := range rpcStatus.Details() {
		if malformed, isMalformed := detail.(error); isMalformed {
			serviceError.Detail = fmt.Sprintf(
				"Dex returned malformed error details: %v",
				malformed,
			)
			return serviceError
		}
		var isDexError bool
		response, isDexError = detail.(*dexpb.ServiceErrorResponse)
		if isDexError {
			break
		}
	}
	if response == nil {
		return serviceError
	}
	serviceError.SubStatus = mapErrorSubStatus(response.SubStatus)
	if response.Detail != "" {
		serviceError.Detail = response.Detail
	}
	switch serviceError.SubStatus {
	case ErrorSubStatusFlowAlreadyStarted:
		return &FlowAlreadyStartedError{ServiceError: serviceError}
	case ErrorSubStatusFlowNotFound:
		switch requirement {
		case flowTargetExisting:
			return &FlowNotFoundError{ServiceError: serviceError}
		case flowTargetActive:
			return &FlowNotActiveOrNotFoundError{ServiceError: serviceError}
		default:
			return serviceError
		}
	case ErrorSubStatusWorkerAPI:
		if serviceError.Code == codes.Aborted {
			return &RPCLockConflictError{ServiceError: serviceError}
		}
		return &WorkerInvocationError{
			ServiceError: serviceError,
			Worker:       mapWorkerError(response),
		}
	case ErrorSubStatusLongPollTimeout:
		return &LongPollTimeoutError{ServiceError: serviceError}
	case ErrorSubStatusRequestTimeout:
		return &RequestTimeoutError{ServiceError: serviceError}
	case ErrorSubStatusChannelMessageNotFound:
		return &ChannelMessageNotFoundError{ServiceError: serviceError}
	default:
		return serviceError
	}
}

func mapWorkerError(response *dexpb.ServiceErrorResponse) *WorkerError {
	if response.OriginalWorkerErrorDetail == "" &&
		response.OriginalWorkerErrorType == "" &&
		response.OriginalWorkerErrorStatus == 0 {
		return nil
	}
	return &WorkerError{
		Code:   codes.Code(response.OriginalWorkerErrorStatus),
		Type:   response.OriginalWorkerErrorType,
		Detail: response.OriginalWorkerErrorDetail,
	}
}

func mapErrorSubStatus(subStatus dexpb.ErrorSubStatus) ErrorSubStatus {
	switch subStatus {
	case dexpb.ErrorSubStatus_ERROR_SUB_STATUS_FLOW_ALREADY_STARTED:
		return ErrorSubStatusFlowAlreadyStarted
	case dexpb.ErrorSubStatus_ERROR_SUB_STATUS_FLOW_NOT_EXISTS:
		return ErrorSubStatusFlowNotFound
	case dexpb.ErrorSubStatus_ERROR_SUB_STATUS_WORKER_API_ERROR:
		return ErrorSubStatusWorkerAPI
	case dexpb.ErrorSubStatus_ERROR_SUB_STATUS_LONG_POLL_TIME_OUT:
		return ErrorSubStatusLongPollTimeout
	case dexpb.ErrorSubStatus_ERROR_SUB_STATUS_CHANNEL_MESSAGE_NOT_FOUND:
		return ErrorSubStatusChannelMessageNotFound
	case dexpb.ErrorSubStatus_ERROR_SUB_STATUS_REQUEST_TIMEOUT:
		return ErrorSubStatusRequestTimeout
	default:
		return ErrorSubStatusUncategorized
	}
}

func newFlowDefinitionError(
	flowType string,
	definition string,
	err error,
) error {
	var existing *FlowDefinitionError
	if errors.As(err, &existing) {
		return err
	}
	return &FlowDefinitionError{
		FlowType:   flowType,
		Definition: definition,
		Err:        err,
	}
}

func wrapValueMappingError(operation string, err error) error {
	if err == nil {
		return nil
	}
	var existing *ValueMappingError
	if errors.As(err, &existing) {
		return err
	}
	return &ValueMappingError{Operation: operation, Err: err}
}
