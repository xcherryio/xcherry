package engine

import (
	"context"
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/extensions"
	"math/rand"
	"time"
)

type workerTaskQueueSQLImpl struct {
	shardId   int32
	dbSession extensions.SQLDBSession
	logger    log.Logger
	rootCtx   context.Context
	cfg       config.Config

	pollTimer                 TimerGate
	commitTimer               TimerGate
	taskToProcessChan         chan extensions.WorkerTaskRow
	taskCompletionChan        chan extensions.WorkerTaskRow
	currentReadCursor         int64
	pendingTaskSequenceToPage map[int64]*workerTaskPage
	completedPages            []*workerTaskPage
}

type workerTaskPage struct {
	minTaskSequence int64
	maxTaskSequence int64
	pendingCount    int
}

func NewWorkerTaskProcessorSQLImpl(
	rootCtx context.Context, shardId int32, cfg config.Config, logger log.Logger,
) (TaskQueue, error) {
	qCfg := cfg.AsyncService.WorkerTaskQueue

	session, err := extensions.NewSQLSession(cfg.Database.SQL)
	return &workerTaskQueueSQLImpl{
		shardId:   shardId,
		dbSession: session,
		logger:    logger,
		rootCtx:   rootCtx,
		cfg:       cfg,

		pollTimer:                 NewLocalTimerGate(logger),
		commitTimer:               NewLocalTimerGate(logger),
		taskToProcessChan:         make(chan extensions.WorkerTaskRow, qCfg.ProcessorBufferSize),
		taskCompletionChan:        make(chan extensions.WorkerTaskRow, qCfg.ProcessorBufferSize),
		currentReadCursor:         0,
		pendingTaskSequenceToPage: make(map[int64]*workerTaskPage),
	}, err
}

func (w *workerTaskQueueSQLImpl) Stop(ctx context.Context) error {
	// a final attempt to commit the completed page
	return w.commitCompletedPages(ctx)
}

func (w *workerTaskQueueSQLImpl) TriggerPolling(pollTime time.Time) {
	w.pollTimer.Update(pollTime)
}

func (w *workerTaskQueueSQLImpl) Start() error {
	qCfg := w.cfg.AsyncService.WorkerTaskQueue

	w.pollTimer.Update(time.Now()) // fire immediately to make the first poll for the first page

	startWorkerTaskConcurrentProcessor(
		w.rootCtx, qCfg.ProcessorConcurrency,
		w.taskToProcessChan, w.taskCompletionChan,
		w.dbSession, w.logger)

	for {
		select {
		case <-w.pollTimer.FireChan():
			w.pollAndDispatch()
			jitter := time.Duration(rand.Int63n(int64(qCfg.IntervalJitter)))
			w.pollTimer.Update(time.Now().Add(qCfg.MaxPollInterval).Add(jitter))
		case <-w.commitTimer.FireChan():
			_ = w.commitCompletedPages(w.rootCtx)
			jitter := time.Duration(rand.Int63n(int64(qCfg.IntervalJitter)))
			w.commitTimer.Update(time.Now().Add(qCfg.CommitInterval).Add(jitter))
		case task, ok := <-w.taskCompletionChan:
			if ok {
				w.receiveCompletedTask(task)
			}
		case <-w.rootCtx.Done():
			w.logger.Info("processor is being closed")
			return nil
		}
	}
}

func (w *workerTaskQueueSQLImpl) pollAndDispatch() {
	qCfg := w.cfg.AsyncService.WorkerTaskQueue

	workerTasks, err := w.dbSession.BatchSelectWorkerTasks(
		w.rootCtx, w.shardId, w.currentReadCursor, qCfg.PollPageSize)

	if err != nil {
		w.logger.Error("failed at polling worker task", tag.Error(err))
		// TODO maybe schedule an earlier next time?
	} else {
		if len(workerTasks) > 0 {
			firstTask := workerTasks[0]
			lastTask := workerTasks[len(workerTasks)-1]
			w.currentReadCursor = lastTask.TaskSequence + 1

			page := &workerTaskPage{
				minTaskSequence: firstTask.TaskSequence,
				maxTaskSequence: lastTask.TaskSequence,
				pendingCount:    len(workerTasks),
			}

			for _, task := range workerTasks {
				w.taskToProcessChan <- task
				w.pendingTaskSequenceToPage[task.TaskSequence] = page
			}
		}
	}
}

func (w *workerTaskQueueSQLImpl) commitCompletedPages(ctx context.Context) error {
	if len(w.completedPages) > 0 {
		// TODO optimize by combining into single query (as long as it won't exceed certain limit)
		for idx, page := range w.completedPages {
			err := w.dbSession.BatchDeleteWorkerTask(ctx, extensions.WorkerTaskRangeDeleteFilter{
				ShardId:                  w.shardId,
				MinTaskSequenceInclusive: page.minTaskSequence,
				MaxTaskSequenceInclusive: page.maxTaskSequence,
			})
			if err != nil {
				w.logger.Error("failed at deleting completed worker tasks", tag.Error(err))
				// fix the completed pages -- current page to the end
				// return and wait for next time
				w.completedPages = w.completedPages[idx:]
				return err
			}
		}
		// reset to empty
		w.completedPages = nil
	}
	return nil
}

func (w *workerTaskQueueSQLImpl) receiveCompletedTask(task extensions.WorkerTaskRow) {
	page := w.pendingTaskSequenceToPage[task.TaskSequence]
	delete(w.pendingTaskSequenceToPage, task.TaskSequence)

	page.pendingCount--
	if page.pendingCount == 0 {
		w.completedPages = append(w.completedPages, page)
	}
}
