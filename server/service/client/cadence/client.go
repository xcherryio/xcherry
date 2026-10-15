// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package cadence

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/service/interpreter/cadence"

	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/common/ptr"
	"github.com/superdurable/dex/service/common/retry"

	"github.com/google/uuid"
	"github.com/superdurable/dex/gen/dexpb"
	uclient "github.com/superdurable/dex/service/client"
	historybuilder "github.com/superdurable/dex/service/client/history"
	"github.com/superdurable/dex/service/common/blobstore"
	serviceerrors "github.com/superdurable/dex/service/common/errors"
	"github.com/superdurable/dex/service/common/index"
	cadenceadmin "github.com/uber/cadence-idl/go/proto/admin/v1"
	cadenceapi "github.com/uber/cadence-idl/go/proto/api/v1"
	realcadence "go.uber.org/cadence"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/encoded"
	"go.uber.org/cadence/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const cadenceCanceledLocalActivityReason = "cadenceInternal:Canceled"

type cadenceClient struct {
	domain                         string
	cClient                        client.Client
	closeFunc                      func()
	serviceClient                  workflowserviceclient.Interface
	adminClient                    cadenceadmin.AdminAPIYARPCClient
	adminSecurityToken             string
	converter                      encoded.DataConverter
	queryWorkflowFailedRetryPolicy *config.RetryPolicy
	it                             *cadence.InterpreterWorker
}

type localActivityMarkerData struct {
	ActivityType string `json:"activityType,omitempty"`
	ErrReason    string `json:"errReason,omitempty"`
	ErrJSON      string `json:"errJson,omitempty"`
	ResultJSON   string `json:"resultJson,omitempty"`
	Attempt      int32  `json:"attempt,omitempty"`
}

func (t *cadenceClient) IsWorkflowAlreadyStartedError(err error) bool {
	var workflowExecutionAlreadyStartedError *shared.WorkflowExecutionAlreadyStartedError
	ok := errors.As(err, &workflowExecutionAlreadyStartedError)
	return ok
}

func (t *cadenceClient) GetRunIdFromWorkflowAlreadyStartedError(err error) (string, bool) {
	var res *shared.WorkflowExecutionAlreadyStartedError
	ok := errors.As(err, &res)
	runId := ""
	if ok {
		runId = *res.RunId
	}
	return runId, ok
}

func (t *cadenceClient) IsNotFoundError(err error) bool {
	var entityNotExistsError *shared.EntityNotExistsError
	if errors.As(err, &entityNotExistsError) {
		return true
	}
	var workflowExecutionAlreadyCompletedError *shared.WorkflowExecutionAlreadyCompletedError
	return errors.As(err, &workflowExecutionAlreadyCompletedError)
}

func (t *cadenceClient) IsUnknownUpdateError(error, string) bool {
	return false
}

func (t *cadenceClient) isQueryFailedError(err error) bool {
	var serviceError *shared.QueryFailedError
	ok := errors.As(err, &serviceError)
	return ok
}

func (t *cadenceClient) IsWorkflowTimeoutError(err error) bool {
	return realcadence.IsTimeoutError(err)
}

func (t *cadenceClient) IsRequestTimeoutError(err error) bool {
	return errors.Is(err, context.DeadlineExceeded)
}

func (t *cadenceClient) GetIfUpdateError(err error, detail *string) (dexpb.UpdateErrorType, bool) {
	typeName := t.extractAppErrType(err)
	value, ok := dexpb.UpdateErrorType_value[typeName]
	if !ok {
		return 0, false
	}
	var decoded string
	if decodeErr := t.decodeAppErrDetails(err, &decoded); decodeErr == nil && detail != nil {
		*detail = decoded
	}
	return dexpb.UpdateErrorType(value), true
}

func (t *cadenceClient) GetIfFlowError(err error, resp *dexpb.ServiceErrorResponse) (dexpb.FlowErrorType, bool) {
	typeName := t.extractAppErrType(err)
	value, ok := dexpb.FlowErrorType_value[typeName]
	if !ok {
		return 0, false
	}
	if resp == nil {
		panic("resp required")
	}
	flowErrorType := dexpb.FlowErrorType(value)
	flowError := &dexpb.InternalFlowError{}
	if decodeErr := t.decodeAppErrDetails(err, flowError); decodeErr != nil {
		flowError.Failure = &dexpb.InternalFlowError_ServerDetail{ServerDetail: err.Error()}
	}
	*resp = *serviceerrors.ServiceErrorResponseFromFlowError(flowErrorType, flowError)
	return flowErrorType, true
}

func (t *cadenceClient) extractAppErrType(err error) string {
	var cErr *realcadence.CustomError
	if errors.As(err, &cErr) {
		return cErr.Reason()
	}
	return ""
}

func (t *cadenceClient) decodeAppErrDetails(err error, detailsPtr interface{}) error {
	var cErr *realcadence.CustomError
	if !errors.As(err, &cErr) {
		return fmt.Errorf("not an application error")
	}
	if !cErr.HasDetails() {
		return fmt.Errorf("application error doesn't have details")
	}
	return cErr.Details(detailsPtr)
}

func NewCadenceClient(
	domain string, cClient client.Client, serviceClient workflowserviceclient.Interface,
	adminClient cadenceadmin.AdminAPIYARPCClient, adminSecurityToken string,
	converter encoded.DataConverter, closeFunc func(), retryPolicy *config.RetryPolicy,
) uclient.UnifiedClient {
	if adminClient == nil {
		panic("Cadence admin client must not be nil")
	}
	return &cadenceClient{
		domain:                         domain,
		cClient:                        cClient,
		closeFunc:                      closeFunc,
		serviceClient:                  serviceClient,
		adminClient:                    adminClient,
		adminSecurityToken:             adminSecurityToken,
		converter:                      converter,
		queryWorkflowFailedRetryPolicy: retryPolicy,
	}
}

func (t *cadenceClient) Close() {
	t.closeFunc()
}

func (t *cadenceClient) ListAttributeIndexes(ctx context.Context) (map[string]dexpb.IndexType, error) {
	response, err := t.cClient.GetSearchAttributes(ctx)
	if err != nil {
		return nil, err
	}
	indexes := make(map[string]dexpb.IndexType, len(response.GetKeys()))
	for name, indexType := range response.GetKeys() {
		indexes[name] = mapCadenceIndexedValueType(indexType)
	}
	return indexes, nil
}

func (t *cadenceClient) AddAttributeIndexes(ctx context.Context, indexes map[string]dexpb.IndexType) error {
	searchAttributes := make(map[string]cadenceapi.IndexedValueType, len(indexes))
	for name, indexType := range indexes {
		searchAttributes[name] = mapToCadenceIndexedValueType(indexType)
	}
	_, err := t.adminClient.AddSearchAttribute(ctx, &cadenceadmin.AddSearchAttributeRequest{
		SearchAttribute: searchAttributes,
		SecurityToken:   t.adminSecurityToken,
	})
	return err
}

func (t *cadenceClient) NormalizeAttributeIndexType(indexType dexpb.IndexType) dexpb.IndexType {
	if indexType == dexpb.IndexType_INDEX_TYPE_KEYWORD_ARRAY {
		return dexpb.IndexType_INDEX_TYPE_KEYWORD
	}
	return indexType
}

func mapCadenceIndexedValueType(indexType shared.IndexedValueType) dexpb.IndexType {
	switch indexType {
	case shared.IndexedValueTypeString:
		return dexpb.IndexType_INDEX_TYPE_TEXT
	case shared.IndexedValueTypeKeyword:
		return dexpb.IndexType_INDEX_TYPE_KEYWORD
	case shared.IndexedValueTypeInt:
		return dexpb.IndexType_INDEX_TYPE_INT
	case shared.IndexedValueTypeDouble:
		return dexpb.IndexType_INDEX_TYPE_DOUBLE
	case shared.IndexedValueTypeBool:
		return dexpb.IndexType_INDEX_TYPE_BOOL
	case shared.IndexedValueTypeDatetime:
		return dexpb.IndexType_INDEX_TYPE_DATETIME
	default:
		return dexpb.IndexType_INDEX_TYPE_UNSPECIFIED
	}
}

func mapToCadenceIndexedValueType(indexType dexpb.IndexType) cadenceapi.IndexedValueType {
	switch indexType {
	case dexpb.IndexType_INDEX_TYPE_TEXT:
		return cadenceapi.IndexedValueType_INDEXED_VALUE_TYPE_STRING
	case dexpb.IndexType_INDEX_TYPE_KEYWORD, dexpb.IndexType_INDEX_TYPE_KEYWORD_ARRAY:
		return cadenceapi.IndexedValueType_INDEXED_VALUE_TYPE_KEYWORD
	case dexpb.IndexType_INDEX_TYPE_INT:
		return cadenceapi.IndexedValueType_INDEXED_VALUE_TYPE_INT
	case dexpb.IndexType_INDEX_TYPE_DOUBLE:
		return cadenceapi.IndexedValueType_INDEXED_VALUE_TYPE_DOUBLE
	case dexpb.IndexType_INDEX_TYPE_BOOL:
		return cadenceapi.IndexedValueType_INDEXED_VALUE_TYPE_BOOL
	case dexpb.IndexType_INDEX_TYPE_DATETIME:
		return cadenceapi.IndexedValueType_INDEXED_VALUE_TYPE_DATETIME
	default:
		return cadenceapi.IndexedValueType_INDEXED_VALUE_TYPE_INVALID
	}
}

func (t *cadenceClient) StartInterpreterWorkflow(
	ctx context.Context, options uclient.StartWorkflowOptions, args ...interface{},
) (runId string, err error) {
	workflowOptions := client.StartWorkflowOptions{
		ID:                           options.ID,
		TaskList:                     options.TaskQueue,
		ExecutionStartToCloseTimeout: time.Duration(math.MaxInt32) * time.Second,
		SearchAttributes:             options.SearchAttributes,
		Memo:                         options.Memo,
	}

	if options.IdReusePolicy != nil {
		workflowIdReusePolicy, err := mapToCadenceWorkflowIdReusePolicy(*options.IdReusePolicy)
		if err != nil {
			return "", err
		}

		workflowOptions.WorkflowIDReusePolicy = *workflowIdReusePolicy
	}

	if options.RetryPolicy != nil {
		workflowOptions.RetryPolicy = mapToCadenceRetryPolicy(options.RetryPolicy)
	}

	if options.WorkflowStartDelay != nil {
		workflowOptions.DelayStart = *options.WorkflowStartDelay
	}

	run, err := t.cClient.StartWorkflow(ctx, workflowOptions, t.it.Engine, args...)
	if err != nil {
		return "", err
	}
	return run.RunID, nil
}

func (t *cadenceClient) StartBlobStoreCleanupWorkflow(
	ctx context.Context, taskQueue, workflowID, cronSchedule, storeId string,
) error {

	workflowOptions := client.StartWorkflowOptions{
		ID:                           workflowID,
		TaskList:                     taskQueue,
		ExecutionStartToCloseTimeout: time.Hour * 24 * 365,
		CronSchedule:                 cronSchedule,
	}

	_, err := t.cClient.StartWorkflow(ctx, workflowOptions, t.it.BlobStoreCleanup, storeId)

	return err
}

func (t *cadenceClient) SignalWorkflow(
	ctx context.Context, workflowID string, runID string, signalName string, arg interface{},
) error {
	return t.cClient.SignalWorkflow(ctx, workflowID, runID, signalName, arg)
}

func (t *cadenceClient) CancelWorkflow(ctx context.Context, workflowID string, runID string) error {
	return t.cClient.CancelWorkflow(ctx, workflowID, runID)
}

func (t *cadenceClient) TerminateWorkflow(ctx context.Context, workflowID string, runID string, reason string) error {
	var reasonStr string
	if reason == "" {
		reasonStr = "Force termiantion from user"
	} else {
		reasonStr = reason
	}

	return t.cClient.TerminateWorkflow(ctx, workflowID, runID, reasonStr, nil)
}

func (t *cadenceClient) ListWorkflow(
	ctx context.Context, request *uclient.ListWorkflowExecutionsRequest,
) (*uclient.ListWorkflowExecutionsResponse, error) {
	query := mapToCadenceVisibilityQuery(request.Query)
	listReq := &shared.ListWorkflowExecutionsRequest{
		PageSize:      &request.PageSize,
		Query:         &query,
		NextPageToken: request.NextPageToken,
	}
	resp, err := t.cClient.ListWorkflow(ctx, listReq)
	if err != nil {
		return nil, err
	}
	var executions []*dexpb.SearchFlowsResponseEntry
	for _, exe := range resp.GetExecutions() {
		searchAttributes := index.MapCadenceSearchAttributeFieldsToKVs(exe.GetSearchAttributes())
		status, err := mapToDexWorkflowStatus(exe.CloseStatus)
		if err != nil {
			return nil, err
		}
		executions = append(executions, &dexpb.SearchFlowsResponseEntry{
			FlowId:            *exe.Execution.WorkflowId,
			RunId:             *exe.Execution.RunId,
			IndexedAttributes: searchAttributes,
			FlowType:          stringSearchAttribute(searchAttributes, service.SearchAttributeDexWorkflowType),
			FlowStatus:        status,
			StartTime:         cadenceTimestamp(exe.StartTime),
			CloseTime:         cadenceTimestamp(exe.CloseTime),
		})
	}
	return &uclient.ListWorkflowExecutionsResponse{
		Executions:    executions,
		NextPageToken: resp.NextPageToken,
	}, nil
}

func mapToCadenceVisibilityQuery(query string) string {
	return strings.ReplaceAll(
		query,
		`ExecutionStatus != "ContinuedAsNew"`,
		`(CloseTime = missing OR CloseStatus = "COMPLETED" OR CloseStatus = "FAILED" OR `+
			`CloseStatus = "CANCELED" OR CloseStatus = "TERMINATED" OR CloseStatus = "TIMED_OUT")`,
	)
}

func (t *cadenceClient) QueryWorkflow(
	ctx context.Context, valuePtr interface{}, workflowID string, runID string, queryType string, args ...interface{},
) error {
	var qres encoded.Value
	var err error

	retryBackoff := retry.NewQueryWorkflowBackoff(t.queryWorkflowFailedRetryPolicy)
	// Only QueryFailed error causes retry; all other errors make the loop to finish immediately
	for {
		qres, err = t.cClient.QueryWorkflow(ctx, workflowID, runID, queryType, args...)
		if err == nil {
			break
		}
		if !t.isQueryFailedError(err) {
			return err
		}
		shouldRetry, retryErr := retryBackoff.WaitForNextAttempt(ctx)
		if retryErr != nil {
			return retryErr
		}
		if !shouldRetry {
			return err
		}
	}
	return qres.Get(valuePtr)
}

func queryWorkflowWithStrongConsistency(
	t *cadenceClient, ctx context.Context, workflowID string, runID string, queryType string, args []interface{},
) (encoded.Value, error) {
	queryWorkflowWithOptionsRequest := &client.QueryWorkflowWithOptionsRequest{
		WorkflowID:            workflowID,
		RunID:                 runID,
		QueryType:             queryType,
		Args:                  args,
		QueryConsistencyLevel: ptr.Any(shared.QueryConsistencyLevelStrong),
	}
	result, err := t.cClient.QueryWorkflowWithOptions(ctx, queryWorkflowWithOptionsRequest)
	if err != nil {
		return nil, err
	}
	return result.QueryResult, nil
}

func (t *cadenceClient) DescribeWorkflowExecution(
	ctx context.Context, workflowID, runID string, indexedAttrTypes map[string]dexpb.IndexType,
) (*uclient.DescribeWorkflowExecutionResponse, error) {
	resp, err := t.cClient.DescribeWorkflowExecution(ctx, workflowID, runID)
	if err != nil {
		return nil, err
	}
	status, err := mapToDexWorkflowStatus(resp.GetWorkflowExecutionInfo().CloseStatus)
	if err != nil {
		return nil, err
	}
	indexedAttributes, err := index.MapCadenceSearchAttributeFieldsToAttrValues(resp.GetWorkflowExecutionInfo().GetSearchAttributes(), indexedAttrTypes)
	if err != nil {
		return nil, err
	}

	memo, err := t.decodeMemo(resp.GetWorkflowExecutionInfo().GetMemo())
	if err != nil {
		return nil, err
	}
	info := resp.GetWorkflowExecutionInfo()
	startTime := time.Unix(0, info.GetStartTime())
	var closeTime *time.Time
	if info.CloseTime != nil {
		closeTime = ptr.Any(time.Unix(0, info.GetCloseTime()))
	}
	pendingStepFailures, err := t.pendingStepFailures(resp.GetPendingActivities())
	if err != nil {
		return nil, err
	}

	return &uclient.DescribeWorkflowExecutionResponse{
		RunId:               info.GetExecution().GetRunId(),
		FirstRunId:          "", // Cadence does not provide FirstRunId
		Status:              status,
		IndexedAttributes:   indexedAttributes,
		Memos:               memo,
		StartTime:           startTime,
		CloseTime:           closeTime,
		PendingStepFailures: pendingStepFailures,
	}, nil
}

func (t *cadenceClient) pendingStepFailures(
	pendingActivities []*shared.PendingActivityInfo,
) (map[string]*dexpb.StepMethodFailure, error) {
	failures := make(map[string]*dexpb.StepMethodFailure)
	for _, pendingActivity := range pendingActivities {
		stepExecutionID, ok := service.StepExecutionIDFromActivityID(pendingActivity.GetActivityID())
		if !ok || pendingActivity.GetLastFailureReason() == "" {
			continue
		}
		failure, err := t.cadenceStepFailure(
			pendingActivity.GetLastFailureReason(),
			pendingActivity.GetLastFailureDetails(),
		)
		if err != nil {
			return nil, err
		}
		failure.Attempt = pendingActivity.GetAttempt()
		failures[stepExecutionID] = failure
	}
	return failures, nil
}

func (t *cadenceClient) GetWorkflowHistory(
	ctx context.Context,
	request *uclient.GetWorkflowHistoryRequest,
) (*uclient.WorkflowHistory, error) {
	response, err := t.getCadenceHistoryPage(ctx, request)
	if err != nil {
		return nil, err
	}
	rawEvents := response.GetHistory().GetEvents()
	startInternalEventID := request.StartInternalEventID
	if startInternalEventID == 0 {
		startInternalEventID = 1
		if len(rawEvents) > 0 {
			startInternalEventID = rawEvents[0].GetEventId()
		}
	}
	nextInternalEventID := startInternalEventID
	if len(rawEvents) > 0 {
		nextInternalEventID = rawEvents[len(rawEvents)-1].GetEventId() + 1
	}
	events, err := t.buildCadenceHistoryEvents(
		ctx,
		request.WorkflowID,
		request.RunID,
		startInternalEventID,
		nextInternalEventID,
	)
	if err != nil {
		return nil, err
	}
	return &uclient.WorkflowHistory{
		Events:              events,
		NextPageToken:       response.GetNextPageToken(),
		NextInternalEventID: nextInternalEventID,
	}, nil
}

func (t *cadenceClient) getCadenceHistoryPage(
	ctx context.Context,
	request *uclient.GetWorkflowHistoryRequest,
) (*shared.GetWorkflowExecutionHistoryResponse, error) {
	nextPageToken := request.NextPageToken
	for {
		filterType := shared.HistoryEventFilterTypeAllEvent
		response, err := t.serviceClient.GetWorkflowExecutionHistory(
			ctx,
			&shared.GetWorkflowExecutionHistoryRequest{
				Domain: &t.domain,
				Execution: &shared.WorkflowExecution{
					WorkflowId: &request.WorkflowID,
					RunId:      &request.RunID,
				},
				MaximumPageSize:        ptr.Any(request.EstimatePageSize),
				NextPageToken:          nextPageToken,
				HistoryEventFilterType: &filterType,
			},
		)
		if err != nil {
			return nil, err
		}
		rawEvents := response.GetHistory().GetEvents()
		reachedStart := len(rawEvents) > 0 &&
			rawEvents[len(rawEvents)-1].GetEventId() >= request.StartInternalEventID
		if len(request.NextPageToken) > 0 ||
			request.StartInternalEventID <= 1 ||
			reachedStart ||
			len(response.GetNextPageToken()) == 0 {
			return response, nil
		}
		nextPageToken = response.GetNextPageToken()
	}
}

func (t *cadenceClient) buildCadenceHistoryEvents(
	ctx context.Context,
	workflowID string,
	runID string,
	startInternalEventID int64,
	nextInternalEventID int64,
) ([]*dexpb.FlowHistoryEvent, error) {
	iterator := t.cClient.GetWorkflowHistory(
		ctx,
		workflowID,
		runID,
		false,
		shared.HistoryEventFilterTypeAllEvent,
	)
	builder := historybuilder.NewBuilder(workflowID, runID)
	scheduledTypes := map[int64]string{}
	scheduledEventIDsByActivityID := map[string]int64{}
	localFallbackCounts := map[string]int{}
	for iterator.HasNext() {
		event, err := iterator.Next()
		if err != nil {
			return nil, err
		}
		if err := t.addCadenceHistoryEvent(
			builder,
			scheduledTypes,
			scheduledEventIDsByActivityID,
			localFallbackCounts,
			event,
		); err != nil {
			return nil, err
		}
	}
	return builder.EventsInRange(startInternalEventID, nextInternalEventID)
}

func (t *cadenceClient) WaitForWorkflowHistoryEvent(
	ctx context.Context,
	workflowID string,
	runID string,
	nextInternalEventID int64,
) (*uclient.WorkflowHistory, error) {
	iterator := t.cClient.GetWorkflowHistory(
		ctx,
		workflowID,
		runID,
		true,
		shared.HistoryEventFilterTypeAllEvent,
	)
	for iterator.HasNext() {
		event, err := iterator.Next()
		if err != nil {
			return nil, err
		}
		if event.GetEventId() >= nextInternalEventID {
			return &uclient.WorkflowHistory{
				EventAvailable:           true,
				AvailableInternalEventID: event.GetEventId(),
			}, nil
		}
	}
	return &uclient.WorkflowHistory{}, nil
}

func (t *cadenceClient) addCadenceHistoryEvent(
	builder *historybuilder.Builder,
	scheduledTypes map[int64]string,
	scheduledEventIDsByActivityID map[string]int64,
	localFallbackCounts map[string]int,
	event *shared.HistoryEvent,
) error {
	eventTime := time.Unix(0, event.GetTimestamp())
	switch event.GetEventType() {
	case shared.EventTypeWorkflowExecutionStarted:
		attributes := event.GetWorkflowExecutionStartedEventAttributes()
		var input dexpb.InterpreterWorkflowInput
		if err := t.converter.FromData(
			attributes.GetInput(),
			&input,
		); err != nil {
			return err
		}
		builder.RecordStart(event.GetEventId(), eventTime, &input)
	case shared.EventTypeDecisionTaskFailed:
		attributes := event.GetDecisionTaskFailedEventAttributes()
		if attributes.GetCause() == shared.DecisionTaskFailedCauseResetWorkflow {
			builder.RecordTimeTravelFork(event.GetEventId(), eventTime, attributes.GetBaseRunId())
		}
	case shared.EventTypeActivityTaskScheduled:
		attributes := event.GetActivityTaskScheduledEventAttributes()
		scheduledEventIDsByActivityID[attributes.GetActivityId()] = event.GetEventId()
		return t.recordCadenceScheduledActivity(
			builder,
			scheduledTypes,
			localFallbackCounts,
			event,
		)
	case shared.EventTypeActivityTaskStarted:
		attributes := event.GetActivityTaskStartedEventAttributes()
		var lastFailure *dexpb.StepMethodFailure
		if attributes.GetLastFailureReason() != "" {
			var err error
			lastFailure, err = t.cadenceStepFailure(
				attributes.GetLastFailureReason(),
				attributes.GetLastFailureDetails(),
			)
			if err != nil {
				return err
			}
		}
		builder.RecordActivityStarted(
			eventTime,
			attributes.GetScheduledEventId(),
			attributes.GetAttempt()+1,
			lastFailure,
		)
	case shared.EventTypeActivityTaskCompleted:
		return t.recordCadenceCompletedActivity(builder, scheduledTypes, event)
	case shared.EventTypeActivityTaskFailed:
		attributes := event.GetActivityTaskFailedEventAttributes()
		if !isStepActivity(scheduledTypes[attributes.GetScheduledEventId()]) {
			return nil
		}
		failure, err := t.cadenceStepFailure(
			attributes.GetReason(),
			attributes.GetDetails(),
		)
		if err != nil {
			return err
		}
		return builder.RecordActivityFailed(
			event.GetEventId(),
			eventTime,
			attributes.GetScheduledEventId(),
			failure,
		)
	case shared.EventTypeActivityTaskTimedOut:
		attributes := event.GetActivityTaskTimedOutEventAttributes()
		if !isStepActivity(scheduledTypes[attributes.GetScheduledEventId()]) {
			return nil
		}
		return builder.RecordActivityFailed(
			event.GetEventId(),
			eventTime,
			attributes.GetScheduledEventId(),
			&dexpb.StepMethodFailure{
				BackendError: attributes.GetTimeoutType().String(),
			},
		)
	case shared.EventTypeActivityTaskCancelRequested:
		attributes := event.GetActivityTaskCancelRequestedEventAttributes()
		scheduledEventID, ok := scheduledEventIDsByActivityID[attributes.GetActivityId()]
		if !ok {
			return fmt.Errorf("canceled activity %q is missing its schedule event", attributes.GetActivityId())
		}
		if isStepActivity(scheduledTypes[scheduledEventID]) {
			builder.RecordActivityCanceled(scheduledEventID)
		}
	case shared.EventTypeActivityTaskCanceled:
		attributes := event.GetActivityTaskCanceledEventAttributes()
		if isStepActivity(scheduledTypes[attributes.GetScheduledEventId()]) {
			builder.RecordActivityCanceled(attributes.GetScheduledEventId())
		}
	case shared.EventTypeMarkerRecorded:
		return t.recordCadenceLocalActivity(builder, localFallbackCounts, event)
	case shared.EventTypeWorkflowExecutionSignaled:
		attributes := event.GetWorkflowExecutionSignaledEventAttributes()
		if attributes.GetSignalName() != service.ExecuteRpcSignalChannelName {
			return nil
		}
		var request dexpb.ExecuteRpcSignalRequest
		if err := t.converter.FromData(attributes.GetInput(), &request); err != nil {
			return err
		}
		builder.RecordSignal(event.GetEventId(), eventTime, &request)
	case shared.EventTypeWorkflowExecutionCompleted:
		var output dexpb.InterpreterWorkflowOutput
		attributes := event.GetWorkflowExecutionCompletedEventAttributes()
		if len(attributes.GetResult()) > 0 {
			if err := t.converter.FromData(attributes.GetResult(), &output); err != nil {
				return err
			}
		}
		builder.RecordClose(event.GetEventId(), eventTime, &dexpb.FlowClosedHistoryEvent{
			FlowStatus: dexpb.FlowStatus_FLOW_STATUS_COMPLETED,
			Results:    output.GetStepCompletionOutputs(),
		})
	case shared.EventTypeWorkflowExecutionFailed:
		attributes := event.GetWorkflowExecutionFailedEventAttributes()
		errorType, errorMessage := t.cadenceFlowClosedFailure(
			attributes.GetReason(),
			attributes.GetDetails(),
		)
		builder.RecordClose(event.GetEventId(), eventTime, &dexpb.FlowClosedHistoryEvent{
			FlowStatus:   dexpb.FlowStatus_FLOW_STATUS_FAILED,
			ErrorType:    errorType,
			ErrorMessage: errorMessage,
		})
	case shared.EventTypeWorkflowExecutionTimedOut:
		builder.RecordClose(event.GetEventId(), eventTime, &dexpb.FlowClosedHistoryEvent{
			FlowStatus: dexpb.FlowStatus_FLOW_STATUS_SERVER_SIDE_TIMEOUT_INTERNAL_ONLY,
		})
	case shared.EventTypeWorkflowExecutionTerminated:
		attributes := event.GetWorkflowExecutionTerminatedEventAttributes()
		builder.RecordClose(event.GetEventId(), eventTime, &dexpb.FlowClosedHistoryEvent{
			FlowStatus:   dexpb.FlowStatus_FLOW_STATUS_TERMINATED,
			ErrorMessage: attributes.GetReason(),
		})
	case shared.EventTypeWorkflowExecutionCanceled:
		builder.RecordClose(event.GetEventId(), eventTime, &dexpb.FlowClosedHistoryEvent{
			FlowStatus: dexpb.FlowStatus_FLOW_STATUS_CANCELED,
		})
	case shared.EventTypeWorkflowExecutionContinuedAsNew:
		attributes := event.GetWorkflowExecutionContinuedAsNewEventAttributes()
		builder.RecordClose(event.GetEventId(), eventTime, &dexpb.FlowClosedHistoryEvent{
			FlowStatus:       dexpb.FlowStatus_FLOW_STATUS_CONTINUED_AS_NEW,
			ContinuedToRunId: attributes.GetNewExecutionRunId(),
		})
	}
	return nil
}

func (t *cadenceClient) recordCadenceScheduledActivity(
	builder *historybuilder.Builder,
	scheduledTypes map[int64]string,
	localFallbackCounts map[string]int,
	event *shared.HistoryEvent,
) error {
	attributes := event.GetActivityTaskScheduledEventAttributes()
	activityType := attributes.GetActivityType().GetName()
	durability := dexpb.StepDurability_STEP_DURABILITY_SYNC
	method := activityMethod(activityType)
	if localFallbackCounts[method] > 0 {
		durability = dexpb.StepDurability_STEP_DURABILITY_ASYNC
		localFallbackCounts[method]--
		if localFallbackCounts[method] == 0 {
			delete(localFallbackCounts, method)
		}
	}
	switch {
	case activityType == service.WaitForMethodActivityType:
		var input dexpb.InvokeWaitForMethodActivityInput
		var localInput *dexpb.InternalLocalActivityInput
		if err := t.converter.FromData(attributes.GetInput(), &input, &localInput); err != nil {
			return err
		}
		builder.RecordWaitScheduled(
			event.GetEventId(),
			time.Unix(0, event.GetTimestamp()),
			&input,
			durability,
			cadenceStepMethodOptions(attributes),
		)
	case activityType == service.ExecuteMethodActivityType:
		var input dexpb.InvokeExecuteMethodActivityInput
		var localInput *dexpb.InternalLocalActivityInput
		if err := t.converter.FromData(attributes.GetInput(), &input, &localInput); err != nil {
			return err
		}
		builder.RecordExecuteScheduled(
			event.GetEventId(),
			time.Unix(0, event.GetTimestamp()),
			&input,
			durability,
			cadenceStepMethodOptions(attributes),
		)
	case strings.Contains(activityType, "DumpFlowForContinueAsNew"):
	default:
		return nil
	}
	scheduledTypes[event.GetEventId()] = activityType
	return nil
}

func cadenceStepMethodOptions(
	attributes *shared.ActivityTaskScheduledEventAttributes,
) *dexpb.StepMethodOptions {
	options := &dexpb.StepMethodOptions{
		TimeoutSeconds:          attributes.GetStartToCloseTimeoutSeconds(),
		HeartbeatTimeoutSeconds: attributes.GetHeartbeatTimeoutSeconds(),
	}
	policy := attributes.GetRetryPolicy()
	if policy == nil {
		return options
	}
	totalDuration := policy.GetExpirationIntervalInSeconds()
	if totalDuration == int32((365*24*time.Hour)/time.Second) {
		totalDuration = 0
	}
	options.RetryPolicy = &dexpb.RetryPolicy{
		InitialIntervalSeconds: policy.GetInitialIntervalInSeconds(),
		BackoffCoefficient:     float32(policy.GetBackoffCoefficient()),
		MaximumIntervalSeconds: policy.GetMaximumIntervalInSeconds(),
		MaximumAttempts:        policy.GetMaximumAttempts(),
		TotalDurationSeconds:   totalDuration,
	}
	return options
}

func (t *cadenceClient) recordCadenceCompletedActivity(
	builder *historybuilder.Builder,
	scheduledTypes map[int64]string,
	event *shared.HistoryEvent,
) error {
	attributes := event.GetActivityTaskCompletedEventAttributes()
	activityType := scheduledTypes[attributes.GetScheduledEventId()]
	switch {
	case activityType == service.WaitForMethodActivityType:
		var output dexpb.InvokeWaitForMethodActivityOutput
		if err := t.converter.FromData(attributes.GetResult(), &output); err != nil {
			return err
		}
		return builder.RecordActivityCompleted(
			event.GetEventId(),
			time.Unix(0, event.GetTimestamp()),
			attributes.GetScheduledEventId(),
			&output,
			nil,
		)
	case activityType == service.ExecuteMethodActivityType:
		var output dexpb.InvokeExecuteMethodActivityOutput
		if err := t.converter.FromData(attributes.GetResult(), &output); err != nil {
			return err
		}
		return builder.RecordActivityCompleted(
			event.GetEventId(),
			time.Unix(0, event.GetTimestamp()),
			attributes.GetScheduledEventId(),
			nil,
			&output,
		)
	case strings.Contains(activityType, "DumpFlowForContinueAsNew"):
		var output dexpb.DumpFlowForContinueAsNewActivityOutput
		if err := t.converter.FromData(attributes.GetResult(), &output); err != nil {
			return err
		}
		builder.RecordContinueDump(&output)
	}
	return nil
}

func (t *cadenceClient) recordCadenceLocalActivity(
	builder *historybuilder.Builder,
	localFallbackCounts map[string]int,
	event *shared.HistoryEvent,
) error {
	attributes := event.GetMarkerRecordedEventAttributes()
	if attributes.GetMarkerName() != "LocalActivity" {
		return nil
	}
	var marker localActivityMarkerData
	if err := t.converter.FromData(attributes.GetDetails(), &marker); err != nil {
		return err
	}
	if marker.ResultJSON == "" {
		if marker.ErrReason == cadenceCanceledLocalActivityReason {
			return nil
		}
		localFallbackCounts[activityMethod(marker.ActivityType)]++
		if marker.ErrReason == "" {
			return nil
		}
		failure, metadata, err := t.cadenceLocalStepFailure(
			marker.ErrReason,
			[]byte(marker.ErrJSON),
		)
		if err != nil {
			return err
		}
		if metadata == nil {
			return nil
		}
		switch {
		case marker.ActivityType == service.WaitForMethodActivityType:
			builder.RecordLocalActivityFailed(
				event.GetEventId(),
				time.Unix(0, event.GetTimestamp()),
				blobstore.StepEventInputMethodWaitFor,
				failure,
				metadata,
			)
		case marker.ActivityType == service.ExecuteMethodActivityType:
			builder.RecordLocalActivityFailed(
				event.GetEventId(),
				time.Unix(0, event.GetTimestamp()),
				blobstore.StepEventInputMethodExecute,
				failure,
				metadata,
			)
		}
		return nil
	}
	result := []byte(marker.ResultJSON)
	switch {
	case marker.ActivityType == service.WaitForMethodActivityType:
		var output dexpb.InvokeWaitForMethodActivityOutput
		if err := t.converter.FromData(result, &output); err != nil {
			return err
		}
		builder.RecordLocalWaitCompleted(
			event.GetEventId(),
			time.Unix(0, event.GetTimestamp()),
			&output,
			marker.Attempt+1,
		)
	case marker.ActivityType == service.ExecuteMethodActivityType:
		var output dexpb.InvokeExecuteMethodActivityOutput
		if err := t.converter.FromData(result, &output); err != nil {
			return err
		}
		builder.RecordLocalExecuteCompleted(
			event.GetEventId(),
			time.Unix(0, event.GetTimestamp()),
			&output,
			marker.Attempt+1,
		)
	case strings.Contains(marker.ActivityType, "DumpFlowForContinueAsNew"):
		var output dexpb.DumpFlowForContinueAsNewActivityOutput
		if err := t.converter.FromData(result, &output); err != nil {
			return err
		}
		builder.RecordContinueDump(&output)
	}
	return nil
}

func (t *cadenceClient) cadenceStepFailure(
	reason string,
	detailsData []byte,
) (*dexpb.StepMethodFailure, error) {
	failure := &dexpb.StepMethodFailure{
		BackendError: reason,
	}
	if len(detailsData) == 0 {
		return failure, nil
	}
	activityError := &dexpb.InternalActivityError{}
	if err := t.converter.FromData(detailsData, activityError); err != nil {
		return nil, fmt.Errorf("decode step failure details: %w", err)
	}
	failure.Details = serviceerrors.ServiceErrorResponseFromActivityError(
		cadenceFlowErrorType(reason),
		activityError,
	)
	return failure, nil
}

func (t *cadenceClient) cadenceLocalStepFailure(
	reason string,
	detailsData []byte,
) (*dexpb.StepMethodFailure, *dexpb.InternalLocalStepActivityFailure, error) {
	if len(detailsData) == 0 {
		return &dexpb.StepMethodFailure{BackendError: reason}, nil, nil
	}
	var metadata *dexpb.InternalLocalStepActivityFailure
	if err := t.converter.FromData(detailsData, &metadata); err != nil {
		return nil, nil, fmt.Errorf("decode local step failure details: %w", err)
	}
	if metadata == nil || metadata.GetActivityError() == nil {
		return nil, nil, fmt.Errorf("local step failure details are incomplete")
	}
	return &dexpb.StepMethodFailure{
		BackendError: reason,
		Details: serviceerrors.ServiceErrorResponseFromActivityError(
			cadenceFlowErrorType(reason),
			metadata.GetActivityError(),
		),
	}, metadata, nil
}

func (t *cadenceClient) cadenceFlowClosedFailure(
	reason string,
	detailsData []byte,
) (dexpb.FlowErrorType, string) {
	errorType := cadenceFlowErrorType(reason)
	if len(detailsData) == 0 {
		return errorType, cadenceFlowClosedFallbackMessage(reason)
	}
	flowError := &dexpb.InternalFlowError{}
	if err := t.converter.FromData(detailsData, flowError); err != nil {
		return errorType, cadenceFlowClosedFallbackMessage(reason)
	}
	errorMessage := serviceerrors.ServiceErrorResponseDetail(
		serviceerrors.ServiceErrorResponseFromFlowError(errorType, flowError),
	)
	if errorMessage != "" {
		return errorType, errorMessage
	}
	return errorType, cadenceFlowClosedFallbackMessage(reason)
}

func cadenceFlowClosedFallbackMessage(reason string) string {
	if _, ok := dexpb.FlowErrorType_value[reason]; ok {
		return ""
	}
	return reason
}

func cadenceFlowErrorType(reason string) dexpb.FlowErrorType {
	value, ok := dexpb.FlowErrorType_value[reason]
	if !ok {
		return dexpb.FlowErrorType_FLOW_ERROR_TYPE_UNSPECIFIED
	}
	return dexpb.FlowErrorType(value)
}

func activityMethod(activityType string) string {
	switch {
	case activityType == service.WaitForMethodActivityType:
		return "wait-for"
	case activityType == service.ExecuteMethodActivityType:
		return "execute"
	default:
		return activityType
	}
}

func isStepActivity(activityType string) bool {
	return activityType == service.WaitForMethodActivityType ||
		activityType == service.ExecuteMethodActivityType
}

func stringSearchAttribute(attributes []*dexpb.KV, key string) string {
	for _, attribute := range attributes {
		if attribute.GetKey() == key {
			return attribute.GetValue().GetStringValue()
		}
	}
	return ""
}

func cadenceTimestamp(timestamp *int64) *timestamppb.Timestamp {
	if timestamp == nil {
		return nil
	}
	return timestamppb.New(time.Unix(0, *timestamp))
}

func (t *cadenceClient) decodeMemo(memo *shared.Memo) (map[string]*dexpb.Value, error) {
	if memo == nil || len(memo.GetFields()) == 0 {
		return nil, nil
	}

	out := map[string]*dexpb.Value{}
	for k, payload := range memo.GetFields() {
		var value dexpb.EncodedObject
		err := t.converter.FromData(payload, &value)
		if err != nil {
			return nil, err
		}
		out[k] = &dexpb.Value{Kind: &dexpb.Value_ObjValue{ObjValue: &value}}
	}
	return out, nil
}

func mapToCadenceWorkflowIdReusePolicy(idReusePolicy dexpb.IdReusePolicy) (*client.WorkflowIDReusePolicy, error) {
	var res client.WorkflowIDReusePolicy
	switch idReusePolicy {
	case dexpb.IdReusePolicy_ID_REUSE_POLICY_ALLOW_IF_NO_RUNNING:
		res = client.WorkflowIDReusePolicyAllowDuplicate
		return &res, nil
	case dexpb.IdReusePolicy_ID_REUSE_POLICY_ALLOW_IF_PREVIOUS_EXISTS_ABNORMALLY:
		res = client.WorkflowIDReusePolicyAllowDuplicateFailedOnly
		return &res, nil
	case dexpb.IdReusePolicy_ID_REUSE_POLICY_DISALLOW_REUSE:
		res = client.WorkflowIDReusePolicyRejectDuplicate
		return &res, nil
	case dexpb.IdReusePolicy_ID_REUSE_POLICY_ALLOW_TERMINATE_IF_RUNNING:
		res = client.WorkflowIDReusePolicyTerminateIfRunning
		return &res, nil
	default:
		return nil, fmt.Errorf("unsupported workflow id reuse policy %s", idReusePolicy)
	}
}

// mapToCadenceRetryPolicy fills unset (zero-value) fields with the same
// defaults dex has always used for flow retries.
func mapToCadenceRetryPolicy(policy *dexpb.FlowRetryPolicy) *workflow.RetryPolicy {
	if policy == nil {
		return nil
	}

	initialIntervalSeconds := policy.GetInitialIntervalSeconds()
	if initialIntervalSeconds <= 0 {
		initialIntervalSeconds = 1
	}
	backoffCoefficient := policy.GetBackoffCoefficient()
	if backoffCoefficient <= 0 {
		backoffCoefficient = 2
	}
	maximumIntervalSeconds := policy.GetMaximumIntervalSeconds()
	if maximumIntervalSeconds <= 0 {
		maximumIntervalSeconds = 100
	}

	return &workflow.RetryPolicy{
		InitialInterval:    time.Second * time.Duration(initialIntervalSeconds),
		MaximumInterval:    time.Second * time.Duration(maximumIntervalSeconds),
		MaximumAttempts:    policy.GetMaximumAttempts(),
		BackoffCoefficient: float64(backoffCoefficient),
	}
}

func mapToDexWorkflowStatus(status *shared.WorkflowExecutionCloseStatus) (dexpb.FlowStatus, error) {
	if status == nil {
		return dexpb.FlowStatus_FLOW_STATUS_RUNNING, nil
	}

	switch *status {
	case shared.WorkflowExecutionCloseStatusCanceled:
		return dexpb.FlowStatus_FLOW_STATUS_CANCELED, nil
	case shared.WorkflowExecutionCloseStatusContinuedAsNew:
		return dexpb.FlowStatus_FLOW_STATUS_CONTINUED_AS_NEW, nil
	case shared.WorkflowExecutionCloseStatusFailed:
		return dexpb.FlowStatus_FLOW_STATUS_FAILED, nil
	case shared.WorkflowExecutionCloseStatusTimedOut:
		return dexpb.FlowStatus_FLOW_STATUS_SERVER_SIDE_TIMEOUT_INTERNAL_ONLY, nil
	case shared.WorkflowExecutionCloseStatusTerminated:
		return dexpb.FlowStatus_FLOW_STATUS_TERMINATED, nil
	case shared.WorkflowExecutionCloseStatusCompleted:
		return dexpb.FlowStatus_FLOW_STATUS_COMPLETED, nil
	default:
		return dexpb.FlowStatus_FLOW_STATUS_UNSPECIFIED, fmt.Errorf("not supported status %s", status)
	}
}

func (t *cadenceClient) GetWorkflowResult(
	ctx context.Context, valuePtr interface{}, workflowID string, runID string,
) (status dexpb.FlowStatus, err error) {
	workflowRun := t.cClient.GetWorkflow(ctx, workflowID, runID)
	err = workflowRun.Get(ctx, valuePtr)
	switch {
	case err == nil:
		status = dexpb.FlowStatus_FLOW_STATUS_COMPLETED
	case realcadence.IsCanceledError(err):
		status = dexpb.FlowStatus_FLOW_STATUS_CANCELED
	case realcadence.IsTimeoutError(err):
		status = dexpb.FlowStatus_FLOW_STATUS_SERVER_SIDE_TIMEOUT_INTERNAL_ONLY
	case realcadence.IsTerminatedError(err):
		status = dexpb.FlowStatus_FLOW_STATUS_TERMINATED
	case client.IsWorkflowError(err):
		status = dexpb.FlowStatus_FLOW_STATUS_FAILED
	}
	return
}

func (t *cadenceClient) SynchronousUpdateWorkflow(
	ctx context.Context, valuePtr interface{}, workflowID, runID, updateID, updateType string, input interface{},
) error {
	return fmt.Errorf("not supported in Cadence")
}

func (t *cadenceClient) ResetWorkflow(
	ctx context.Context, request *dexpb.ResetFlowRequest,
) (newRunId string, err error) {

	reqRunId := request.GetRunId()
	if reqRunId == "" {
		// set default runId to current
		resp, err := t.cClient.DescribeWorkflowExecution(ctx, request.GetFlowId(), "")
		if err != nil {
			return "", err
		}
		reqRunId = resp.GetWorkflowExecutionInfo().GetExecution().GetRunId()
	}

	// TODO not sure why Cadence reset API requires this for GetWorkflowExecutionHistory API....
	ctx, cancelFn := context.WithTimeout(ctx, time.Second*120)
	defer cancelFn()

	resetType := request.GetResetType()
	resetBaseRunID, decisionFinishID, err := getResetIDsByType(ctx, resetType, t.domain, request.GetFlowId(),
		reqRunId, t.serviceClient, t.converter, request.GetHistoryEventTime(), request.GetStepType(), request.GetStepExecutionId(), request.GetStepMethod())

	if err != nil {
		return "", err
	}

	requestId := uuid.New().String()
	resetReq := &shared.ResetWorkflowExecutionRequest{
		Domain: &t.domain,
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: &request.FlowId,
			RunId:      &resetBaseRunID,
		},
		Reason:                &request.Reason,
		DecisionFinishEventId: ptr.Any(decisionFinishID),
		RequestId:             &requestId,
		SkipSignalReapply:     ptr.Any(request.GetSkipWritesReapply()),
	}
	resp, err := t.serviceClient.ResetWorkflowExecution(ctx, resetReq)
	if err != nil {
		return "", err
	}
	return resp.GetRunId(), nil
}

func (t *cadenceClient) GetBackendType() (backendType service.BackendType) {
	return service.BackendTypeCadence
}

func (t *cadenceClient) GetApiService() interface{} {
	return t.cClient
}
