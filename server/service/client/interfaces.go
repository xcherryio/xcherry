// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package uclient

import (
	"context"
	"errors"
	"time"

	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
)

// ErrQueryRequestTimeout reports a QueryWorkflow read that did not complete within its request budget.
var ErrQueryRequestTimeout = errors.New("flow read did not complete in time and can be retried")

type AttributeIndexClient interface {
	ListAttributeIndexes(context.Context) (map[string]dexpb.IndexType, error)
	AddAttributeIndexes(context.Context, map[string]dexpb.IndexType) error
	NormalizeAttributeIndexType(dexpb.IndexType) dexpb.IndexType
}

type UnifiedClient interface {
	AttributeIndexClient
	Close()
	errorHandler
	StartInterpreterWorkflow(
		ctx context.Context, options StartWorkflowOptions, args ...interface{},
	) (runId string, err error)
	StartBlobStoreCleanupWorkflow(
		ctx context.Context, taskQueue, workflowID, cronSchedule, storeId string,
	) error
	SignalWorkflow(ctx context.Context, workflowID string, runID string, signalName string, arg interface{}) error
	CancelWorkflow(ctx context.Context, workflowID string, runID string) error
	TerminateWorkflow(ctx context.Context, workflowID string, runID string, reason string) error
	ListWorkflow(ctx context.Context, request *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error)
	QueryWorkflow(
		ctx context.Context, valuePtr interface{}, workflowID string, runID string, queryType string,
		args ...interface{},
	) error
	DescribeWorkflowExecution(
		ctx context.Context, workflowID, runID string, indexedAttrTypes map[string]dexpb.IndexType,
	) (*DescribeWorkflowExecutionResponse, error)
	GetWorkflowHistory(
		ctx context.Context,
		request *GetWorkflowHistoryRequest,
	) (*WorkflowHistory, error)
	WaitForWorkflowHistoryEvent(
		ctx context.Context,
		workflowID string,
		runID string,
		nextInternalEventID int64,
	) (*WorkflowHistory, error)
	GetWorkflowResult(
		ctx context.Context, valuePtr interface{}, workflowID string, runID string,
	) (status dexpb.FlowStatus, err error)
	SynchronousUpdateWorkflow(
		ctx context.Context, valuePtr interface{}, workflowID, runID, updateID, updateType string, input interface{},
	) error
	ResetWorkflow(ctx context.Context, request *dexpb.ResetFlowRequest) (runId string, err error)
	GetBackendType() (backendType service.BackendType)
	GetApiService() interface{}
}

type errorHandler interface {
	// Returns false if err is not an update application error.
	GetIfUpdateError(err error, detail *string) (dexpb.UpdateErrorType, bool)
	// Returns false if err is not a flow application error. resp must be non-nil; filled on success.
	GetIfFlowError(err error, resp *dexpb.ServiceErrorResponse) (dexpb.FlowErrorType, bool)
	IsWorkflowAlreadyStartedError(error) bool
	GetRunIdFromWorkflowAlreadyStartedError(error) (string, bool)
	IsNotFoundError(error) bool
	IsUnknownUpdateError(error, string) bool
	IsRequestTimeoutError(error) bool
	IsWorkflowTimeoutError(error) bool
}

type StartWorkflowOptions struct {
	ID            string
	TaskQueue     string
	IdReusePolicy *dexpb.IdReusePolicy
	RetryPolicy   *dexpb.FlowRetryPolicy
	// SearchAttributes are Temporal/Cadence indexed fields (already encoded as backend values).
	SearchAttributes   map[string]interface{}
	Memo               map[string]interface{}
	WorkflowStartDelay *time.Duration
}

type ListWorkflowExecutionsRequest struct {
	PageSize      int32
	Query         string
	NextPageToken []byte
}

type ListWorkflowExecutionsResponse struct {
	Executions    []*dexpb.SearchFlowsResponseEntry
	NextPageToken []byte
}

type DescribeWorkflowExecutionResponse struct {
	Status               dexpb.FlowStatus
	RunId                string
	FirstRunId           string
	IndexedAttributes    map[string]*dexpb.Value
	Memos                map[string]*dexpb.Value
	FlowStartedTimestamp int64
	StartTime            time.Time
	CloseTime            *time.Time
	PendingStepFailures  map[string]*dexpb.StepMethodFailure
}

type GetWorkflowHistoryRequest struct {
	WorkflowID           string
	RunID                string
	StartInternalEventID int64
	EstimatePageSize     int32
	NextPageToken        []byte
}

type WorkflowHistory struct {
	Events                   []*dexpb.FlowHistoryEvent
	NextPageToken            []byte
	NextInternalEventID      int64
	EventAvailable           bool
	AvailableInternalEventID int64
}
