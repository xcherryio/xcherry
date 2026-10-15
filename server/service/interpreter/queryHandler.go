// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package interpreter

import (
	"fmt"

	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/common/rpc"
	"github.com/superdurable/dex/service/interpreter/config"
	"github.com/superdurable/dex/service/interpreter/interfaces"
)

func SetQueryHandlers(
	ctx interfaces.UnifiedContext,
	provider interfaces.WorkflowProvider,
	timerProcessor interfaces.TimerProcessor,
	persistenceManager *PersistenceManager,
	channelStore *ChannelStore,
	continueAsNewer *ContinueAsNewer,
	stepExecutionCounter *StepExecutionCounter,
	flowConfiger *config.FlowConfiger,
	basicInfo service.BasicInfo,
) error {
	err := provider.SetQueryHandler(ctx, service.GetAttributesWorkflowQueryType, func(req *dexpb.GetAttributesQueryRequest) (*dexpb.GetAttributesQueryResponse, error) {
		if req == nil {
			return nil, fmt.Errorf("GetAttributes query requires a request")
		}
		return persistenceManager.GetAttributes(req), nil
	})
	if err != nil {
		return err
	}
	err = provider.SetQueryHandler(
		ctx,
		service.GetChannelMessagesWorkflowQueryType,
		func(req *dexpb.GetChannelMessagesRequest) (*dexpb.GetChannelMessagesResponse, error) {
			if req == nil || req.GetChannelName() == "" {
				return nil, fmt.Errorf("GetChannelMessages query requires a channel name")
			}
			return &dexpb.GetChannelMessagesResponse{
				Messages: channelStore.GetMessages(req.GetChannelName()),
			}, nil
		},
	)
	if err != nil {
		return err
	}
	err = continueAsNewer.SetQueryHandlersForContinueAsNew(ctx)
	if err != nil {
		return err
	}
	err = provider.SetQueryHandler(ctx, service.DebugDumpQueryType, func() (*dexpb.DebugDumpResponse, error) {
		return &dexpb.DebugDumpResponse{
			Config:                     flowConfiger.Get(),
			Snapshot:                   continueAsNewer.GetSnapshot(),
			FiringTimersUnixTimestamps: timerProcessor.GetTimerStartedUnixTimestamps(),
			ActiveStepExecutions:       continueAsNewer.GetActiveStepExecutionStates(),
		}, nil
	})
	if err != nil {
		return err
	}
	err = provider.SetQueryHandler(
		ctx,
		service.IsStepExecutionCompletedQueryType,
		func(stepType string, stepExecutionNumber int32) (bool, error) {
			if stepType == "" || stepExecutionNumber <= 0 {
				return false, fmt.Errorf("step type and positive execution number are required")
			}
			return stepExecutionCounter.IsStepExecutionCompleted(
				stepType,
				stepExecutionNumber,
			), nil
		},
	)
	if err != nil {
		return err
	}
	err = provider.SetQueryHandler(ctx, service.PrepareRpcQueryType, func(req *dexpb.PrepareRpcQueryRequest) (*dexpb.PrepareRpcQueryResponse, error) {
		if req == nil {
			return nil, fmt.Errorf("PrepareRpc query requires a request")
		}
		selection, err := rpc.ValidateAndSortSelections(
			req.GetLoadAttributeMapInstances(),
			req.GetLoadChannelNames(),
			req.GetLoadChannelMapInstances(),
		)
		if err != nil {
			return nil, err
		}
		info := provider.GetWorkflowInfo(ctx)
		return &dexpb.PrepareRpcQueryResponse{
			DexTaskQueue:                continueAsNewer.DexTaskQueue(),
			Attributes:                  persistenceManager.GetSelectedAttributes(selection.AttributeMapInstances),
			RunId:                       info.WorkflowExecution.RunID,
			FlowStartedTimestamp:        info.WorkflowStartTime.Unix(),
			FlowType:                    basicInfo.FlowType,
			WorkerTarget:                flowConfiger.GetWorkerTarget(),
			ChannelInfos:                channelStore.GetInfos(),
			LoadedChannelMessages:       channelStore.GetLoadedMessages(selection.ChannelNames, selection.ChannelMapInstances),
			LoadedAttributeMapInstances: selection.AttributeMapInstances,
			LoadedChannelNames:          selection.ChannelNames,
			LoadedChannelMapInstances:   selection.ChannelMapInstances,
		}, nil
	})
	if err != nil {
		return err
	}
	err = provider.SetQueryHandler(ctx, service.GetCurrentTimerInfosQueryType, func() (*dexpb.GetCurrentTimerInfosQueryResponse, error) {
		timerInfos := make(map[string]*dexpb.TimerInfoList, len(timerProcessor.GetTimerInfos()))
		for stepExecutionId, infos := range timerProcessor.GetTimerInfos() {
			timerInfos[stepExecutionId] = &dexpb.TimerInfoList{Timers: infos}
		}
		return &dexpb.GetCurrentTimerInfosQueryResponse{
			StepExecutionCurrentTimerInfos: timerInfos,
		}, nil
	})
	if err != nil {
		return err
	}
	return nil
}
