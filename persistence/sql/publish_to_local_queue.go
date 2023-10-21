package sql

import (
	"context"

	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/common/uuid"
	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/persistence"
)

func (p sqlProcessStoreImpl) PublishToLocalQueue(ctx context.Context, request persistence.PublishToLocalQueueRequest) (
	*persistence.PublishToLocalQueueResponse, error) {
	tx, err := p.session.StartTransaction(ctx, defaultTxOpts)
	if err != nil {
		return nil, err
	}

	resp, err := p.doPublishToLocalQueueTx(ctx, tx, request)

	if err != nil {
		err2 := tx.Rollback()
		if err2 != nil {
			p.logger.Error("error on rollback transaction", tag.Error(err2))
		}
	} else {
		err = tx.Commit()
		if err != nil {
			p.logger.Error("error on committing transaction", tag.Error(err))
			return nil, err
		}
	}

	return resp, err
}

func (p sqlProcessStoreImpl) doPublishToLocalQueueTx(
	ctx context.Context, tx extensions.SQLTransaction, request persistence.PublishToLocalQueueRequest,
) (*persistence.PublishToLocalQueueResponse, error) {
	curProcExecRow, err := p.session.SelectLatestProcessExecution(ctx, request.Namespace, request.ProcessId)
	if err != nil {
		if p.session.IsNotFoundError(err) {
			// early stop when there is no such process running
			return &persistence.PublishToLocalQueueResponse{
				NotExists: true,
			}, nil
		}
		return nil, err
	}

	for _, message := range request.Messages {
		dedupId := uuid.ParseUUID(message.GetDedupId())
		if dedupId == nil {
			dedupId = uuid.MustNewUUID()
		}

		// insert a row into xdb_sys_local_queue

		payload, err := persistence.FromEncodedObjectIntoBytes(message.Payload)
		if err != nil {
			return nil, err
		}

		err = tx.InsertLocalQueue(ctx, extensions.LocalQueueRow{
			ProcessExecutionId: curProcExecRow.ProcessExecutionId,
			QueueName:          message.GetQueueName(),
			DedupId:            dedupId,
			Payload:            payload,
		})
		if err != nil {
			return nil, err
		}

		// insert a row into xdb_sys_immediate_tasks

		taskInfoBytes, err := persistence.FromImmediateTaskInfoIntoBytes(
			persistence.ImmediateTaskInfoJson{
				LocalQueueMessageInfo: &persistence.LocalQueueMessageInfoJson{
					QueueName: message.GetQueueName(),
					DedupId:   dedupId,
				},
			})
		if err != nil {
			return nil, err
		}

		err = tx.InsertImmediateTask(ctx, extensions.ImmediateTaskRowForInsert{
			ShardId:  persistence.DefaultShardId,
			TaskType: persistence.ImmediateTaskTypeNewLocalQueueMessage,

			ProcessExecutionId: curProcExecRow.ProcessExecutionId,
			StateId:            "",
			StateIdSequence:    0,
			Info:               taskInfoBytes,
		})
		if err != nil {
			return nil, err
		}
	}

	return &persistence.PublishToLocalQueueResponse{
		ProcessExecutionId:  curProcExecRow.ProcessExecutionId,
		HasNewImmediateTask: len(request.Messages) > 0,
		NotExists:           false,
	}, nil
}
