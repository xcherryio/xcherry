// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xcherryio/xcherry/persistence/process/sqltest"
)

func TestBasic(t *testing.T) {
	sqltest.CleanupEnv(assert.New(t), store)
	sqltest.SQLBasicTest(t, assert.New(t), store)
}

func TestGracefulComplete(t *testing.T) {
	sqltest.CleanupEnv(assert.New(t), store)
	sqltest.SQLGracefulCompleteTest(t, assert.New(t), store)
}

func TestForceFail(t *testing.T) {
	sqltest.CleanupEnv(assert.New(t), store)
	sqltest.SQLForceFailTest(t, assert.New(t), store)
}

func TestProcessIdReusePolicyDisallowReuse(t *testing.T) {
	sqltest.CleanupEnv(assert.New(t), store)
	sqltest.SQLProcessIdReusePolicyDisallowReuseTest(t, assert.New(t), store)
}

func TestProcessIdReusePolicyAllowIfNoRunning(t *testing.T) {
	sqltest.CleanupEnv(assert.New(t), store)
	sqltest.SQLProcessIdReusePolicyAllowIfNoRunning(t, assert.New(t), store)
}

func TestProcessIdReusePolicyTerminateIfRunning(t *testing.T) {
	sqltest.CleanupEnv(assert.New(t), store)
	sqltest.SQLProcessIdReusePolicyTerminateIfRunning(t, assert.New(t), store)
}

func TestProcessIdReusePolicyAllowIfPreviousExitAbnormally(t *testing.T) {
	sqltest.CleanupEnv(assert.New(t), store)
	sqltest.SQLProcessIdReusePolicyAllowIfPreviousExitAbnormally(t, assert.New(t), store)
}

func TestProcessIdReusePolicyDefault(t *testing.T) {
	sqltest.CleanupEnv(assert.New(t), store)
	sqltest.SQLProcessIdReusePolicyDefault(t, assert.New(t), store)
}

func TestBackoffTimer(t *testing.T) {
	sqltest.CleanupEnv(assert.New(t), store)
	sqltest.SQLBackoffTest(t, assert.New(t), store)
}

func TestStateFailureRecovery(t *testing.T) {
	sqltest.CleanupEnv(assert.New(t), store)
	sqltest.SQLStateFailureRecoveryTest(t, assert.New(t), store)
}

func TestAppDatabase(t *testing.T) {
	sqltest.CleanupEnv(assert.New(t), store)
	sqltest.SQLAppDatabaseTest(t, assert.New(t), store)
}
