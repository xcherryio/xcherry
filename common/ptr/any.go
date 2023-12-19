// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package ptr

func Any[T any](obj T) *T {
	return &obj
}
