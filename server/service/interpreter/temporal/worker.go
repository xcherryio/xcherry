// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package temporal

import (
	"context"
	"fmt"

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
	uclient "github.com/superdurable/dex/service/client"
	"github.com/superdurable/dex/service/common/attributestore"
	"github.com/superdurable/dex/service/common/blobstore"
	"github.com/superdurable/dex/service/common/event"
	"github.com/superdurable/dex/service/common/pulldispatch"
	"github.com/superdurable/dex/service/common/streamstore"
	"github.com/superdurable/dex/service/common/workerclient"
	"github.com/superdurable/dex/service/interpreter"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

type InterpreterWorker struct {
	temporalClient client.Client
	worker         worker.Worker
	taskQueue      string
	internalClient *workerclient.InternalServiceClient
	unifiedClient  uclient.UnifiedClient
	activities     *interpreter.Activities
	workflow       *interpreter.Interpreter
	cfg            *config.Config
	dataConverter  converter.DataConverter
}

func NewInterpreterWorker(
	cfg *config.Config,
	temporalClient client.Client,
	taskQueue string,
	dataConverter converter.DataConverter,
	unifiedClient uclient.UnifiedClient,
	store blobstore.BlobStore,
	attributeStore *attributestore.Manager,
	workerPool *workerclient.WorkerClientPool,
	streamStore *streamstore.Store,
	metrics client.MetricsHandler,
) *InterpreterWorker {
	if cfg == nil {
		panic("Temporal InterpreterWorker requires non-nil config sections")
	}
	if temporalClient == nil || dataConverter == nil || unifiedClient == nil ||
		workerPool == nil || streamStore == nil || metrics == nil || taskQueue == "" {
		panic("Temporal InterpreterWorker requires non-nil dependencies and task queue")
	}
	internal := interpreter.NewInternalServiceClient(cfg)
	eventHandler := event.Handle
	activities := interpreter.NewActivities(
		&activityProvider{},
		workerPool,
		internal,
		unifiedClient,
		store,
		attributeStore,
		eventHandler,
		cfg,
		streamStore,
		metrics,
		pulldispatch.NewDispatcher(internal),
	)
	workflowInterpreter := interpreter.NewInterpreter(cfg, activities)

	return &InterpreterWorker{
		temporalClient: temporalClient,
		taskQueue:      taskQueue,
		internalClient: internal,
		unifiedClient:  unifiedClient,
		activities:     activities,
		workflow:       workflowInterpreter,
		cfg:            cfg,
		dataConverter:  dataConverter,
	}
}

// NewWorkerForReplay builds a worker that can register Engine for Temporal history replay.
// Activity results come from history; Activities is never invoked for real.
func NewWorkerForReplay() *InterpreterWorker {
	return &InterpreterWorker{
		workflow: interpreter.NewInterpreter(&config.Config{}, &interpreter.Activities{}),
	}
}

func (iw *InterpreterWorker) Close() {
	if iw.worker != nil {
		iw.worker.Stop()
	}
	if iw.internalClient != nil {
		iw.internalClient.Close()
	}
	iw.temporalClient.Close()
}

// StartWithStickyCacheDisabledForTest can harm performance; should not be used in production environment
func (iw *InterpreterWorker) StartWithStickyCacheDisabledForTest() error {
	return iw.start(true)
}

func (iw *InterpreterWorker) Start() error {
	return iw.start(false)
}

func (iw *InterpreterWorker) start(disableStickyCache bool) error {
	var options worker.Options

	if iw.cfg.Interpreter.Temporal.WorkerOptions != nil {
		options = *iw.cfg.Interpreter.Temporal.WorkerOptions
	}

	// override default
	if options.MaxConcurrentActivityTaskPollers == 0 {
		options.MaxConcurrentActivityTaskPollers = 10
	}

	// override default
	if options.MaxConcurrentWorkflowTaskPollers == 0 {
		// TODO: Keep this at 10; smaller values break continue-as-new persistence tests, likely due to Temporal SDK parallelism.
		options.MaxConcurrentWorkflowTaskPollers = 10
	}

	// When DisableStickyCache is true it can harm performance; should not be used in production environment
	if disableStickyCache {
		worker.SetStickyWorkflowCacheSize(0)
		fmt.Println("Temporal worker: Sticky cache disabled")
	}

	iw.worker = worker.New(iw.temporalClient, iw.taskQueue, options)
	worker.EnableVerboseLogging(iw.cfg.Interpreter.VerboseDebug)

	iw.worker.RegisterWorkflowWithOptions(iw.Engine, workflow.RegisterOptions{
		FlowTypeProvider: interpreterWorkflowFlowType,
	})
	iw.worker.RegisterWorkflow(iw.BlobStoreCleanup)
	iw.worker.RegisterActivityWithOptions(iw.activities.IWaitForM, activity.RegisterOptions{
		Name:             service.WaitForMethodActivityType,
		FlowTypeProvider: waitForMethodFlowType,
		StepTypeProvider: waitForMethodStepType,
	})
	iw.worker.RegisterActivityWithOptions(iw.activities.IExecuteM, activity.RegisterOptions{
		Name:             service.ExecuteMethodActivityType,
		FlowTypeProvider: executeMethodFlowType,
		StepTypeProvider: executeMethodStepType,
	})
	iw.worker.RegisterActivity(iw.activities.DumpFlowForContinueAsNew)
	iw.worker.RegisterActivityWithOptions(iw.activities.IWRPC, activity.RegisterOptions{
		Name:             service.WorkerRPCActivityType,
		FlowTypeProvider: invokeWorkerRPCFlowType,
		RPCNameProvider:  invokeWorkerRPCName,
	})
	iw.worker.RegisterActivityWithOptions(iw.activities.StartSubFlow, activity.RegisterOptions{
		SubFlowTypeProvider: startSubFlowType,
	})
	iw.worker.RegisterActivity(iw.activities.ReportSubFlowCompletion)
	iw.worker.RegisterActivity(iw.activities.CleanupBlobsAfterAllRunsDeleted)
	iw.worker.RegisterActivity(iw.activities.SyncAttributeBatch)

	err := iw.worker.Start()
	if err != nil {
		return fmt.Errorf("start Temporal interpreter worker: %w", err)
	}

	if iw.cfg.BlobStore.EffectiveEnabled() {
		for _, storeCfg := range iw.cfg.BlobStore.SupportedStorages {
			cronSchedule, scheduleErr := storeCfg.CleanupStrategy.CronSchedule()
			if scheduleErr != nil {
				return fmt.Errorf("invalid blobstore cleanup strategy: %w", scheduleErr)
			}
			if cronSchedule != "" {
				err = iw.unifiedClient.StartBlobStoreCleanupWorkflow(
					context.Background(), iw.taskQueue,
					"blobstore-cleanup-"+storeCfg.StorageId,
					cronSchedule,
					storeCfg.StorageId)
				if err != nil {
					if iw.unifiedClient.IsWorkflowAlreadyStartedError(err) {
						continue
					}
					return fmt.Errorf("start blobstore cleanup workflow: %w", err)
				}
			}
		}
	}
	return nil
}

func interpreterWorkflowFlowType(input any) string {
	return input.(*dexpb.InterpreterWorkflowInput).GetFlowType()
}

func waitForMethodFlowType(input any) string {
	return input.(*dexpb.InvokeWaitForMethodActivityInput).GetRequest().GetFlowType()
}

func waitForMethodStepType(input any) string {
	return input.(*dexpb.InvokeWaitForMethodActivityInput).GetRequest().GetStepType()
}

func executeMethodFlowType(input any) string {
	return input.(*dexpb.InvokeExecuteMethodActivityInput).GetRequest().GetFlowType()
}

func executeMethodStepType(input any) string {
	return input.(*dexpb.InvokeExecuteMethodActivityInput).GetRequest().GetStepType()
}

func invokeWorkerRPCFlowType(input any) string {
	return input.(*dexpb.InvokeWorkerRPCActivityInput).GetRpcPrep().GetFlowType()
}

func invokeWorkerRPCName(input any) string {
	return input.(*dexpb.InvokeWorkerRPCActivityInput).GetRequest().GetRpcName()
}

func startSubFlowType(input any) string {
	return input.(*dexpb.StartSubFlowActivityInput).GetCondition().GetSubFlowType()
}
