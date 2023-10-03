package engine

import (
	"context"
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/common/urlautofix"
	"github.com/xdblab/xdb/extensions"
)

func startWorkerTaskConcurrentProcessor(
	ctx context.Context,
	concurrency int,
	taskToProcessChan <-chan extensions.WorkerTaskRow,
	taskCompletionChan chan<- extensions.WorkerTaskRow,
	dbSession extensions.SQLDBSession, logger log.Logger,
) {
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
					processWorkerTask(ctx, task, dbSession, logger)
					taskCompletionChan <- task
				}
			}
		}()
	}
}

func processWorkerTask(
	ctx context.Context, task extensions.WorkerTaskRow,
	session extensions.SQLDBSession, logger log.Logger) {
	// TODO
	
	logger.Info("execute worker task")
	iwfWorkerBaseUrl := urlautofix.FixWorkerUrl(input.IwfWorkerUrl)

	apiClient := iwfidl.NewAPIClient(&iwfidl.Configuration{
		Servers: []iwfidl.ServerConfiguration{
			{
				URL: iwfWorkerBaseUrl,
			},
		},
	})

	attempt := provider.GetActivityInfo(ctx).Attempt
	scheduledTs := provider.GetActivityInfo(ctx).ScheduledTime.Unix()
	input.Request.Context.Attempt = &attempt
	input.Request.Context.FirstAttemptTimestamp = &scheduledTs

	req := apiClient.DefaultApi.ApiV1WorkflowStateStartPost(ctx)
	resp, httpResp, err := req.WorkflowStateStartRequest(input.Request).Execute()
	printDebugMsg(logger, err, iwfWorkerBaseUrl)
	if checkHttpError(err, httpResp) {
		return nil, composeHttpError(provider, err, httpResp, string(iwfidl.STATE_API_FAIL_MAX_OUT_RETRY_ERROR_TYPE))
	}

	if err := checkCommandRequestFromWaitUntilResponse(resp); err != nil {
		return nil, composeStartApiRespError(provider, err, resp)
	}

}
