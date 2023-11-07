// Copyright (c) XDBLab
// SPDX-License-Identifier: BUSL-1.1

package sql

import (
	"context"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"

	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/persistence"
)

func (p sqlProcessStoreImpl) PrepareStateExecution(
	ctx context.Context, request persistence.PrepareStateExecutionRequest,
) (*persistence.PrepareStateExecutionResponse, error) {
	stateRow, err := p.session.SelectAsyncStateExecution(
		ctx, extensions.AsyncStateExecutionSelectFilter{
			ProcessExecutionId: request.ProcessExecutionId,
			StateId:            request.StateId,
			StateIdSequence:    request.StateIdSequence,
		})
	if err != nil {
		return nil, err
	}

	info, err := persistence.BytesToAsyncStateExecutionInfo(stateRow.Info)
	if err != nil {
		return nil, err
	}

	input, err := persistence.BytesToEncodedObject(stateRow.Input)
	if err != nil {
		return nil, err
	}

	commandResultsJson, err := persistence.BytesToCommandResultsJson(stateRow.WaitUntilCommandResults)
	if err != nil {
		return nil, err
	}

	commandRequest, err := persistence.BytesToCommandRequest(stateRow.WaitUntilCommands)
	if err != nil {
		return nil, err
	}

	commandResults := p.prepareWaitUntilCommandResults(commandResultsJson, commandRequest)

	return &persistence.PrepareStateExecutionResponse{
		Status:                  stateRow.Status,
		WaitUntilCommandResults: commandResults,
		PreviousVersion:         stateRow.PreviousVersion,
		Info:                    info,
		Input:                   input,
	}, nil
}

func (p sqlProcessStoreImpl) prepareWaitUntilCommandResults(
	commandResultsJson persistence.CommandResultsJson, commandRequest xdbapi.CommandRequest,
) xdbapi.CommandResults {
	commandResults := xdbapi.CommandResults{}

	for idx := range commandRequest.TimerCommands {
		timerResult := xdbapi.TimerResult{
			Status: xdbapi.WAITING_COMMAND,
		}

		fired, ok := commandResultsJson.TimerResults[idx]
		if ok {
			if fired {
				timerResult.Status = xdbapi.COMPLETED_COMMAND
			} else {
				timerResult.Status = xdbapi.SKIPPED_COMMAND
			}
		}

		commandResults.TimerResults = append(commandResults.TimerResults, timerResult)
	}

	for idx, localQueueCommand := range commandRequest.LocalQueueCommands {
		localQueueResult := xdbapi.LocalQueueResult{
			Status:    xdbapi.WAITING_COMMAND,
			QueueName: localQueueCommand.GetQueueName(),
			Messages:  nil,
		}

		result, ok := commandResultsJson.LocalQueueResults[idx]
		if ok {
			localQueueResult.Status = xdbapi.COMPLETED_COMMAND
			localQueueResult.Messages = result
		}

		commandResults.LocalQueueResults = append(commandResults.LocalQueueResults, localQueueResult)
	}

	return commandResults
}
