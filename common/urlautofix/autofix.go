// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

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
