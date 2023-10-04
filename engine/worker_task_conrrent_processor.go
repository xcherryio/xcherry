package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/common/ptr"
	"github.com/xdblab/xdb/common/urlautofix"
	"github.com/xdblab/xdb/engine/persistence"
	"github.com/xdblab/xdb/extensions"
	"io/ioutil"
	"net/http"
	"time"
)

// TODO refactor to be more modularized

func startWorkerTaskConcurrentProcessor(
	ctx context.Context, concurrency int,
	taskToProcessChan chan extensions.WorkerTaskRow,
	taskCompletionChan chan<- extensions.WorkerTaskRow,
	dbSession extensions.SQLDBSession, logger log.Logger, notify LocalNotifyNewWorkerTask) {
	for i := 0; i < concurrency; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case task, ok := <-taskToProcessChan:
					if !ok {
						return
					}
					err := processWorkerTask(ctx, task, dbSession, logger, notify)
					if err != nil {
						// put it back to the queue for retry
						// Note that if the error is because of invoking worker APIs,
						// processWorkerTask will use timer queue for backoff retry instead(To be implemented)
						taskToProcessChan <- task
					} else {
						taskCompletionChan <- task
					}

				}
			}
		}()
	}
}

func processWorkerTask(ctx context.Context, task extensions.WorkerTaskRow, session extensions.SQLDBSession,
	logger log.Logger, notify LocalNotifyNewWorkerTask) error {

	logger.Info("execute worker task", tag.ID(tag.AnyToStr(task.TaskSequence)))

	stateRow, err := session.SelectAsyncStateExecutionForUpdate(
		ctx, extensions.AsyncStateExecutionSelectFilter{
			ProcessExecutionId: task.ProcessExecutionId,
			StateId:            task.StateId,
			StateIdSequence:    task.StateIdSequence,
		})
	if err != nil {
		return err
	}
	var info extensions.AsyncStateExecutionInfoJson
	err = json.Unmarshal(stateRow.Info, &info)
	if err != nil {
		return err
	}
	var input xdbapi.EncodedObject
	err = json.Unmarshal(stateRow.Input, &input)
	if err != nil {
		return err
	}

	iwfWorkerBaseUrl := urlautofix.FixWorkerUrl(info.WorkerURL)
	apiClient := xdbapi.NewAPIClient(&xdbapi.Configuration{
		Servers: []xdbapi.ServerConfiguration{
			{
				URL: iwfWorkerBaseUrl,
			},
		},
	})

	if stateRow.WaitUntilStatus == extensions.StateExecutionStatusRunning {
		return processWaitUntilTask(ctx, task, info, stateRow, input, apiClient, session, logger, notify)
	} else if stateRow.ExecuteStatus == extensions.StateExecutionStatusRunning {
		return processExecuteTask(ctx, task, info, stateRow, input, apiClient, session, logger, notify)
	} else {
		logger.Warn("noop for worker task ",
			tag.ID(tag.AnyToStr(task.TaskSequence)),
			tag.Value(fmt.Sprintf("waitUntilStatus %v, executeStatus %v",
				stateRow.WaitUntilStatus, stateRow.ExecuteStatus)))
		return nil
	}
}

func processWaitUntilTask(ctx context.Context, task extensions.WorkerTaskRow, info extensions.AsyncStateExecutionInfoJson,
	stateRow *extensions.AsyncStateExecutionRow, input xdbapi.EncodedObject,
	apiClient *xdbapi.APIClient, session extensions.SQLDBSession, logger log.Logger, notifyNewTask LocalNotifyNewWorkerTask,
) (retErr error) {
	hasNewWorkerTask := false
	// TODO fix using backoff retry when worker API returns error by pushing the task into timer queue
	attempt := int32(1)

	req := apiClient.DefaultAPI.ApiV1XdbWorkerAsyncStateWaitUntilPost(ctx)
	resp, httpResp, err := req.AsyncStateWaitUntilRequest(
		xdbapi.AsyncStateWaitUntilRequest{
			Context: &xdbapi.Context{
				ProcessId:          info.ProcessId,
				ProcessExecutionId: task.ProcessExecutionIdString,
				Attempt:            ptr.Any(attempt),
				StateExecutionId:   ptr.Any(getAsyncStateExecutionId(task.StateId, task.StateIdSequence)),
				// TODO more fields
			},
			ProcessType: ptr.Any(info.ProcessType),
			StateId:     ptr.Any(task.StateId),
			StateInput: &xdbapi.EncodedObject{
				Encoding: input.Encoding,
				Data:     input.Data,
			},
		},
	).Execute()
	if checkHttpError(err, httpResp) {
		err := composeHttpError(err, httpResp)
		logger.Info("worker API return error", tag.Error(err))
		// TODO instead of returning error, we should do backoff retry by pushing this task into timer queue
		return err
	}
	txn, retErr := session.StartTransaction(ctx)
	if retErr != nil {
		return retErr
	}
	defer func() {
		if retErr == nil {
			retErr = txn.Commit()
		} else {
			err2 := txn.Rollback()
			logger.Error("failed at rolling back transaction", tag.Error(err2))
		}
		if retErr == nil && hasNewWorkerTask {
			notifyNewTask(time.Now())
		}
	}()

	stateRow.WaitUntilStatus = extensions.StateExecutionStatusCompleted
	if resp.CommandRequest.GetWaitingType() != xdbapi.EMPTY_COMMAND {
		// TODO set command request from resp
		return fmt.Errorf("not supported command type %v", resp.CommandRequest.GetWaitingType())
	} else {
		stateRow.ExecuteStatus = extensions.StateExecutionStatusRunning
	}
	retErr = txn.UpdateAsyncStateExecution(ctx, *stateRow)
	if retErr != nil {
		if session.IsConditionalUpdateFailure(retErr) {
			logger.Warn("UpdateAsyncStateExecution failed at conditional update")
		}
		return retErr
	}
	retErr = txn.InsertWorkerTask(ctx, extensions.WorkerTaskRowForInsert{
		ShardId:            task.ShardId,
		TaskType:           extensions.WorkerTaskTypeExecute,
		ProcessExecutionId: task.ProcessExecutionId,
		StateId:            task.StateId,
		StateIdSequence:    task.StateIdSequence,
	})
	hasNewWorkerTask = true
	return retErr
}

func processExecuteTask(ctx context.Context, task extensions.WorkerTaskRow, info extensions.AsyncStateExecutionInfoJson,
	stateRow *extensions.AsyncStateExecutionRow, input xdbapi.EncodedObject,
	apiClient *xdbapi.APIClient, session extensions.SQLDBSession, logger log.Logger, notifyNewTask LocalNotifyNewWorkerTask,
) (retErr error) {
	hasNewWorkerTask := false
	// TODO fix using backoff retry when worker API returns error by pushing the task into timer queue
	attempt := int32(1)

	req := apiClient.DefaultAPI.ApiV1XdbWorkerAsyncStateExecutePost(ctx)
	resp, httpResp, err := req.AsyncStateExecuteRequest(
		xdbapi.AsyncStateExecuteRequest{
			Context: &xdbapi.Context{
				ProcessId:          info.ProcessId,
				ProcessExecutionId: task.ProcessExecutionIdString,
				Attempt:            ptr.Any(attempt),
				StateExecutionId:   ptr.Any(getAsyncStateExecutionId(task.StateId, task.StateIdSequence)),
				// TODO more fields
			},
			ProcessType: ptr.Any(info.ProcessType),
			StateId:     ptr.Any(task.StateId),
			StateInput: &xdbapi.EncodedObject{
				Encoding: input.Encoding,
				Data:     input.Data,
			},
		},
	).Execute()
	if checkHttpError(err, httpResp) {
		err := composeHttpError(err, httpResp)
		logger.Info("worker API return error", tag.Error(err))
		// TODO instead of returning error, we should do backoff retry by pushing this task into timer queue
		return err
	}
	err = checkDecision(resp.StateDecision)
	if err != nil {
		return err
	}

	txn, retErr := session.StartTransaction(ctx)
	if retErr != nil {
		return retErr
	}
	defer func() {
		if retErr == nil {
			retErr = txn.Commit()
		} else {
			err2 := txn.Rollback()
			logger.Error("failed at rolling back transaction", tag.Error(err2))
		}
		if hasNewWorkerTask && retErr == nil {
			notifyNewTask(time.Now())
		}
	}()

	stateRow.ExecuteStatus = extensions.StateExecutionStatusCompleted
	retErr = txn.UpdateAsyncStateExecution(ctx, *stateRow)
	if retErr != nil {
		if session.IsConditionalUpdateFailure(retErr) {
			logger.Warn("UpdateAsyncStateExecution failed at conditional update")
		}
		return retErr
	}

	if resp.StateDecision == nil {
		return nil
	}

	if resp.StateDecision.HasThreadCloseDecision() {
		threadDecision := resp.StateDecision.GetThreadCloseDecision()
		if threadDecision.GetCloseType() == xdbapi.DEAD_END {
			return nil
		}
	}

	// at this point, it's either going to next states or closing the process
	prcRow, retErr := txn.SelectProcessExecutionForUpdate(ctx, task.ProcessExecutionId)
	if retErr != nil {
		return retErr
	}
	var stateIdSequence extensions.StateExecutionIdSequenceJson
	retErr = json.Unmarshal(prcRow.StateIdSequence, &stateIdSequence)
	if retErr != nil {
		return retErr
	}

	if len(resp.StateDecision.GetNextStates()) > 0 {
		nextStates := resp.StateDecision.GetNextStates()
		// go to next states
		for _, next := range nextStates {
			stateIdSequence.SequenceMap[next.StateId]++
			stateIdSeq := stateIdSequence.SequenceMap[next.StateId]

			stateInput, retErr := json.Marshal(next.StateInput)
			if retErr != nil {
				return retErr
			}

			stateRow := extensions.AsyncStateExecutionRow{
				ProcessExecutionId: task.ProcessExecutionId,
				StateId:            task.StateId,
				StateIdSequence:    int32(stateIdSeq),
				PreviousVersion:    1,
				Input:              stateInput,
				Info:               stateRow.Info, // reuse the info from last execution as it won't change
			}

			if next.StateConfig.GetSkipWaitUntil() {
				stateRow.WaitUntilStatus = extensions.StateExecutionStatusSkipped
				stateRow.ExecuteStatus = extensions.StateExecutionStatusRunning
			} else {
				stateRow.WaitUntilStatus = extensions.StateExecutionStatusRunning
				stateRow.ExecuteStatus = extensions.StateExecutionStatusUndefined
			}

			retErr = txn.InsertAsyncStateExecution(ctx, stateRow)
			if retErr != nil {
				return retErr
			}

			workerTaskRow := extensions.WorkerTaskRowForInsert{
				ShardId:            persistence.DefaultShardId,
				ProcessExecutionId: task.ProcessExecutionId,
				StateId:            next.StateId,
				StateIdSequence:    int32(stateIdSeq),
			}
			if next.StateConfig.GetSkipWaitUntil() {
				workerTaskRow.TaskType = extensions.WorkerTaskTypeExecute
			} else {
				workerTaskRow.TaskType = extensions.WorkerTaskTypeWaitUntil
			}

			retErr = txn.InsertWorkerTask(ctx, workerTaskRow)
			if retErr != nil {
				return retErr
			}
		}
		hasNewWorkerTask = true
		// finally update process execution row
		seqJ, retErr := json.Marshal(stateIdSequence)
		if retErr != nil {
			return retErr
		}
		prcRow.StateIdSequence = seqJ
		return txn.UpdateProcessExecution(ctx, *prcRow)
	} else {
		threadDecision := resp.StateDecision.GetThreadCloseDecision()
		// close the thread
		if threadDecision.GetCloseType() != xdbapi.FORCE_COMPLETE_PROCESS {
			return fmt.Errorf("cannot support close type: %v", threadDecision.GetCloseType())
		}

		// update process execution row
		prcRow.Status = extensions.ProcessExecutionStatusCompleted
		return txn.UpdateProcessExecution(ctx, *prcRow)
	}
}

func checkDecision(decision *xdbapi.StateDecision) error {
	if decision == nil {
		return nil
	}
	if decision.HasThreadCloseDecision() && len(decision.GetNextStates()) > 0 {
		return fmt.Errorf("cannot have both thread decision and next states")
	}
	return nil
}
func getAsyncStateExecutionId(stateId string, sequence int32) string {
	return fmt.Sprintf("%v-%v", stateId, sequence)
}

func checkHttpError(err error, httpResp *http.Response) bool {
	if err != nil || (httpResp != nil && httpResp.StatusCode != http.StatusOK) {
		return true
	}
	return false
}

func composeHttpError(err error, httpResp *http.Response) error {
	responseBody := "None"
	var statusCode int
	if httpResp != nil {
		body, err := ioutil.ReadAll(httpResp.Body)
		if err != nil {
			responseBody = "cannot read body from http response"
		} else {
			responseBody = string(body)
		}
		statusCode = httpResp.StatusCode
	}
	return fmt.Errorf("statusCode: %v, responseBody: %v, errMsg: %v", statusCode, responseBody, err)
}
