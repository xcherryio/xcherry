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

package urlautofix

import (
	"os"
	"strings"
)

type FixWorkerUrlFunc func(url string) string

var workerUrlFixer FixWorkerUrlFunc = DefaultFixWorkerUrlFunc

func SetWorkerUrlFixer(fixer FixWorkerUrlFunc) {
	workerUrlFixer = fixer
}

func FixWorkerUrl(url string) string {
	return workerUrlFixer(url)
}

func DefaultFixWorkerUrlFunc(url string) string {
	autofixUrl := os.Getenv("AUTO_FIX_LOCALHOST_WORKER_URL")
	if autofixUrl != "" {
		url = strings.Replace(url, "localhost", autofixUrl, 1)
		url = strings.Replace(url, "127.0.0.1", autofixUrl, 1)
	}

	return url
}
