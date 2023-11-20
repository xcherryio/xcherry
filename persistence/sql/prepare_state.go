// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: BUSL-1.1

package sql

import (
	"context"
	"github.com/xcherryio/xcherry/persistence/data_models"

	"github.com/xcherryio/xcherry/extensions"
)

func (p sqlProcessStoreImpl) PrepareStateExecution(
	ctx context.Context, request data_models.PrepareStateExecutionRequest,
) (*data_models.PrepareStateExecutionResponse, error) {
	stateRow, err := p.session.SelectAsyncStateExecution(
		ctx, extensions.AsyncStateExecutionSelectFilter{
			ProcessExecutionId: request.ProcessExecutionId,
			StateId:            request.StateId,
			StateIdSequence:    request.StateIdSequence,
		})
	if err != nil {
		return nil, err
	}

	info, err := data_models.BytesToAsyncStateExecutionInfo(stateRow.Info)
	if err != nil {
		return nil, err
	}

	input, err := data_models.BytesToEncodedObject(stateRow.Input)
	if err != nil {
		return nil, err
	}

	commandResultsJson, err := data_models.BytesToCommandResultsJson(stateRow.WaitUntilCommandResults)
	if err != nil {
		return nil, err
	}

	commandRequest, err := data_models.BytesToCommandRequest(stateRow.WaitUntilCommands)
	if err != nil {
		return nil, err
	}

	commandResults := p.prepareWaitUntilCommandResults(commandResultsJson, commandRequest)

	return &data_models.PrepareStateExecutionResponse{
		Status:                  stateRow.Status,
		WaitUntilCommandResults: commandResults,
		PreviousVersion:         stateRow.PreviousVersion,
		Info:                    info,
		Input:                   input,
	}, nil
}

func (p sqlProcessStoreImpl) prepareWaitUntilCommandResults(
	commandResultsJson data_models.CommandResultsJson, commandRequest xcapi.CommandRequest,
) xcapi.CommandResults {
	commandResults := xcapi.CommandResults{}

	for idx := range commandRequest.TimerCommands {
		timerResult := xcapi.TimerResult{
			Status: xcapi.WAITING_COMMAND,
		}

		fired, ok := commandResultsJson.TimerResults[idx]
		if ok {
			if fired {
				timerResult.Status = xcapi.COMPLETED_COMMAND
			} else {
				timerResult.Status = xcapi.SKIPPED_COMMAND
			}
		}

		commandResults.TimerResults = append(commandResults.TimerResults, timerResult)
	}

	for idx, localQueueCommand := range commandRequest.LocalQueueCommands {
		localQueueResult := xcapi.LocalQueueResult{
			Status:    xcapi.WAITING_COMMAND,
			QueueName: localQueueCommand.GetQueueName(),
			Messages:  nil,
		}

		result, ok := commandResultsJson.LocalQueueResults[idx]
		if ok {
			localQueueResult.Status = xcapi.COMPLETED_COMMAND
			localQueueResult.Messages = result
		}

		commandResults.LocalQueueResults = append(commandResults.LocalQueueResults, localQueueResult)
	}

	return commandResults
}
