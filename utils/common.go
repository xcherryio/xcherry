// Copyright 2023 xCherryIO organization

// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"math/rand"
)

func GetRandomShardId(shardCount int) int {
	return rand.Intn(shardCount)
}
