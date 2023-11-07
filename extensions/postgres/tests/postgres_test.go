// Copyright (c) XDBLab
// SPDX-License-Identifier: BUSL-1.1

package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xdblab/xdb/persistence/sql/sqltest"
)

func TestBasic(t *testing.T) {
	sqltest.SQLBasicTest(t, assert.New(t), store)
}

func TestGracefulComplete(t *testing.T) {
	sqltest.SQLGracefulCompleteTest(t, assert.New(t), store)
}

func TestForceFail(t *testing.T) {
	sqltest.SQLForceFailTest(t, assert.New(t), store)
}

func TestProcessIdReusePolicyDisallowReuse(t *testing.T) {
	sqltest.SQLProcessIdReusePolicyDisallowReuseTest(t, assert.New(t), store)
}

func TestProcessIdReusePolicyAllowIfNoRunning(t *testing.T) {
	sqltest.SQLProcessIdReusePolicyAllowIfNoRunning(t, assert.New(t), store)
}

func TestProcessIdReusePolicyTerminateIfRunning(t *testing.T) {
	sqltest.SQLProcessIdReusePolicyTerminateIfRunning(t, assert.New(t), store)
}

func TestProcessIdReusePolicyAllowIfPreviousExitAbnormally(t *testing.T) {
	sqltest.SQLProcessIdReusePolicyAllowIfPreviousExitAbnormally(t, assert.New(t), store)
}

func TestProcessIdReusePolicyDefault(t *testing.T) {
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

func TestGlobalAttributes(t *testing.T) {
	sqltest.CleanupEnv(assert.New(t), store)
	sqltest.SQLGlobalAttributesTest(t, assert.New(t), store)
}
