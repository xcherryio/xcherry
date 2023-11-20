// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: BUSL-1.1

package sql

import (
	"context"
	"github.com/xcherryio/xcherry/common/uuid"
	"math"

	"github.com/xcherryio/xcherry/common/ptr"
	"github.com/xcherryio/xcherry/extensions"
	"github.com/xcherryio/xcherry/persistence/data_models"
)

func createGetTimerTaskResponse(
	shardId int32, dbTimerTasks []extensions.TimerTaskRow, reqPageSize *int32,
) (*data_models.GetTimerTasksResponse, error) {
	var tasks []data_models.TimerTask
	for _, t := range dbTimerTasks {
		info, err := data_models.BytesToTimerTaskInfo(t.Info)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, data_models.TimerTask{
			ShardId:              shardId,
			FireTimestampSeconds: t.FireTimeUnixSeconds,
			TaskSequence:         ptr.Any(t.TaskSequence),

			TaskType:           t.TaskType,
			ProcessExecutionId: t.ProcessExecutionId,
			StateExecutionId: data_models.StateExecutionId{
				StateId:         t.StateId,
				StateIdSequence: t.StateIdSequence,
			},
			TimerTaskInfo: info,
		})
	}
	resp := &data_models.GetTimerTasksResponse{
		Tasks: tasks,
	}
	if len(dbTimerTasks) > 0 {
		firstTask := dbTimerTasks[0]
		lastTask := dbTimerTasks[len(dbTimerTasks)-1]
		resp.MinFireTimestampSecondsInclusive = firstTask.FireTimeUnixSeconds
		resp.MaxFireTimestampSecondsInclusive = lastTask.FireTimeUnixSeconds

		resp.MinSequenceInclusive = math.MaxInt64
		resp.MaxSequenceInclusive = math.MinInt64
		for _, t := range dbTimerTasks {
			if t.TaskSequence < resp.MinSequenceInclusive {
				resp.MinSequenceInclusive = t.TaskSequence
			}
			if t.TaskSequence > resp.MaxSequenceInclusive {
				resp.MaxSequenceInclusive = t.TaskSequence
			}
		}
	}
	if reqPageSize != nil {
		if len(dbTimerTasks) == int(*reqPageSize) {
			resp.FullPage = true
		}
	}
	return resp, nil
}

type (
	HandleStateDecisionRequest struct {
		Namespace                  string
		ProcessId                  string
		ProcessType                string
		ProcessExecutionId         uuid.UUID
		StateDecision              xcapi.StateDecision
		GlobalAttributeTableConfig *data_models.InternalGlobalAttributeConfig
		WorkerUrl                  string

		// for ProcessExecutionRowForUpdate
		ProcessExecutionRowStateExecutionSequenceMaps *data_models.StateExecutionSequenceMapsJson
		ProcessExecutionRowWaitToComplete             bool
		ProcessExecutionRowStatus                     data_models.ProcessExecutionStatus

		TaskShardId int32
	}

	HandleStateDecisionResponse struct {
		HasNewImmediateTask bool

		// for ProcessExecutionRowForUpdate to update
		ProcessExecutionRowNewStateExecutionSequenceMaps *data_models.StateExecutionSequenceMapsJson
		ProcessExecutionRowNewWaitToComplete             bool
		ProcessExecutionRowNewStatus                     data_models.ProcessExecutionStatus
	}
)

func (p sqlProcessStoreImpl) handleStateDecision(ctx context.Context, tx extensions.SQLTransaction,
	request HandleStateDecisionRequest) (*HandleStateDecisionResponse, error) {
	hasNewImmediateTask := false

	// these fields will be updated and returned back in response for ProcessExecutionRowForUpdate
	sequenceMaps := request.ProcessExecutionRowStateExecutionSequenceMaps
	procExecWaitToComplete := request.ProcessExecutionRowWaitToComplete
	procExecStatus := request.ProcessExecutionRowStatus

	if len(request.StateDecision.GetNextStates()) > 0 {
		hasNewImmediateTask = true

		for _, next := range request.StateDecision.GetNextStates() {
			stateIdSeq := sequenceMaps.StartNewStateExecution(next.StateId)

			stateInputBytes, err := data_models.FromEncodedObjectIntoBytes(next.StateInput)
			if err != nil {
				return nil, err
			}

			stateInfo := data_models.AsyncStateExecutionInfoJson{
				Namespace:             request.Namespace,
				ProcessId:             request.ProcessId,
				ProcessType:           request.ProcessType,
				WorkerURL:             request.WorkerUrl,
				StateConfig:           next.StateConfig,
				GlobalAttributeConfig: request.GlobalAttributeTableConfig,
			}

			stateInfoBytes, err := stateInfo.ToBytes()
			if err != nil {
				return nil, err
			}

			err = insertAsyncStateExecution(ctx, tx, request.ProcessExecutionId, next.StateId, stateIdSeq, next.StateConfig, stateInputBytes, stateInfoBytes)
			if err != nil {
				return nil, err
			}

			err = insertImmediateTask(ctx, tx, request.ProcessExecutionId, next.StateId, stateIdSeq, next.StateConfig, request.TaskShardId)
			if err != nil {
				return nil, err
			}
		}
	}

	// If the process was previously configured to gracefully complete and there are no states running,
	// then gracefully complete the process regardless of the thread close type set in this state.
	// Otherwise, handle the thread close type set in this state.

	toGracefullyComplete := procExecWaitToComplete && len(sequenceMaps.PendingExecutionMap) == 0

	toAbortRunningAsyncStates := false

	threadDecision := request.StateDecision.GetThreadCloseDecision()
	if !toGracefullyComplete && request.StateDecision.HasThreadCloseDecision() {
		switch threadDecision.GetCloseType() {
		case xcapi.GRACEFUL_COMPLETE_PROCESS:
			procExecWaitToComplete = true
			toGracefullyComplete = len(sequenceMaps.PendingExecutionMap) == 0
		case xcapi.FORCE_COMPLETE_PROCESS:
			toAbortRunningAsyncStates = len(sequenceMaps.PendingExecutionMap) > 0

			procExecStatus = data_models.ProcessExecutionStatusCompleted
			sequenceMaps.PendingExecutionMap = map[string]map[int]bool{}
		case xcapi.FORCE_FAIL_PROCESS:
			toAbortRunningAsyncStates = len(sequenceMaps.PendingExecutionMap) > 0

			procExecStatus = data_models.ProcessExecutionStatusFailed
			sequenceMaps.PendingExecutionMap = map[string]map[int]bool{}
		case xcapi.DEAD_END:
			// do nothing
		}
	}

	if toGracefullyComplete {
		procExecStatus = data_models.ProcessExecutionStatusCompleted
	}

	if toAbortRunningAsyncStates {
		// handle xcherry_sys_async_state_executions
		// find all related rows with the processExecutionId, and
		// modify the wait_until/execute status from running to aborted
		err := tx.BatchUpdateAsyncStateExecutionsToAbortRunning(ctx, request.ProcessExecutionId)
		if err != nil {
			return nil, err
		}
	}

	return &HandleStateDecisionResponse{
		HasNewImmediateTask: hasNewImmediateTask,
		ProcessExecutionRowNewStateExecutionSequenceMaps: sequenceMaps,
		ProcessExecutionRowNewWaitToComplete:             procExecWaitToComplete,
		ProcessExecutionRowNewStatus:                     procExecStatus,
	}, nil
}
