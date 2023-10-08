// Apache License 2.0

// Copyright (c) XDBLab organization

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.    

package persistence

import (
	"context"
)

// ProcessStore is for operating on the database for process execution
type (
	ProcessStore interface {
		Close() error

		StartProcess(ctx context.Context, request StartProcessRequest) (*StartProcessResponse, error)
		DescribeLatestProcess(ctx context.Context, request DescribeLatestProcessRequest) (*DescribeLatestProcessResponse, error)

		GetWorkerTasks(ctx context.Context, request GetWorkerTasksRequest) (*GetWorkerTasksResponse, error)
		DeleteWorkerTasks(ctx context.Context, request DeleteWorkerTasksRequest) error

		PrepareStateExecution(ctx context.Context, request PrepareStateExecutionRequest) (*PrepareStateExecutionResponse, error)
		CompleteWaitUntilExecution(ctx context.Context, request CompleteWaitUntilExecutionRequest) (*CompleteWaitUntilExecutionResponse, error)
		CompleteExecuteExecution(ctx context.Context, request CompleteExecuteExecutionRequest) (*CompleteExecuteExecutionResponse, error)
	}
)
