// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

package data_models

import "github.com/xdblab/xdb/common/uuid"

type LocalQueueMessageInfoJson struct {
	QueueName string    `json:"queueName"`
	DedupId   uuid.UUID `json:"dedupId"`
}

type InternalLocalQueueMessage struct {
	DedupId string
	IsFull  bool // only false for now until we support including payload
}
