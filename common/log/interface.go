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

package log

import (
	"github.com/xdblab/xdb/common/log/tag"
)

// Logger is our abstraction for logging
// Usage examples:
//
//	 import "github.com/uber/cadence/common/log/tag"
//	 1) logger = logger.WithTags(
//	         tag.WorkflowNextEventID( 123),
//	         tag.WorkflowActionWorkflowStarted,
//	         tag.WorkflowDomainID("test-domain-id"))
//	    logger.Info("hello world")
//	 2) logger.Info("hello world",
//	         tag.WorkflowNextEventID( 123),
//	         tag.WorkflowActionWorkflowStarted,
//	         tag.WorkflowDomainID("test-domain-id"))
//		   )
//	 Note: msg should be static, it is not recommended to use fmt.Sprintf() for msg.
//	       Anything dynamic should be tagged.
type Logger interface {
	Debug(msg string, tags ...tag.Tag)
	Info(msg string, tags ...tag.Tag)
	Warn(msg string, tags ...tag.Tag)
	Error(msg string, tags ...tag.Tag)
	Fatal(msg string, tags ...tag.Tag)
	WithTags(tags ...tag.Tag) Logger
}
