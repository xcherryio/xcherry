// Copyright 2023 xCherryIO organization

// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"math/rand"
)

const (
	DefaultAdvertiseAddress = "0:0"
)

func GetRandomShardId(shardCount int) int {
	return rand.Intn(shardCount)
}
